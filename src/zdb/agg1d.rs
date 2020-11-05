use crate::{
  config::Config,
  polygon::{grouped::*, OHLCV},
  zdb::MarketDays
};
use chrono::{Datelike, NaiveDate, Utc};
use std::{
  cmp, process,
  sync::{Arc, Mutex},
  thread,
  time::Instant
};
use threadpool::ThreadPool;
use zdb::{
  calendar::ToNaiveDateTime,
  schema::{Column, ColumnType, PartitionBy, Schema},
  table::Table
};

fn download_agg1d_year(
  year: i32,
  thread_pool: &ThreadPool,
  agg1d: &mut Table,
  config: Arc<Config>
) {
  let from = NaiveDate::from_ymd(year, 1, 1);
  let to = cmp::min(
    NaiveDate::from_ymd(year + 1, 1, 1),
    Utc::now().naive_utc().date()
  );
  let candles = Arc::new(Mutex::new(Vec::<OHLCV>::new()));

  let market_days = MarketDays { from, to };

  for (i, day) in market_days.enumerate() {
    let candles_year = Arc::clone(&candles);
    let config = config.clone();
    thread_pool.execute(move || {
      // Have 2/3 sleep for 1-2s to avoid spamming at start
      thread::sleep(std::time::Duration::from_secs(i as u64 % 3));
      // Retry up to 10 times
      for j in 0..10 {
        match get_grouped(&day, &config) {
          Ok(mut candles) => {
            println!("{}: {} candles", day, candles.len());
            candles_year.lock().unwrap().append(&mut candles);
            return;
          }
          Err(e) => {
            eprintln!("{}: get_grouped retry {}: {}", day, j + 1, e.to_string());
          }
        }
        thread::sleep(std::time::Duration::from_secs(j));
      }
      eprintln!("{}: failure", &day);
      process::exit(1);
    });
  }
  thread_pool.join();

  println!("Sorting {}", year);
  // Sort by ts, symbol
  let mut candles = candles.lock().unwrap();
  candles.sort_unstable_by(|c1, c2| {
    if c1.ts == c2.ts {
      c1.symbol.cmp(&c2.symbol)
    } else {
      c1.ts.cmp(&c2.ts)
    }
  });
  let now = Instant::now();
  let num_candles = candles.len();
  println!("Writing {}", year);
  for c in candles.drain(..) {
    // Filter out crazy tickers
    // https://github.com/polygon-io/issues/issues/3
    if !c.symbol.chars().all(|c| c.is_ascii_graphic()) {
      let date = c.ts.to_naive_date_time().date();
      println!("{}: Bad symbol {}", date, c.symbol);
      continue;
    }
    agg1d.put_timestamp(c.ts);
    agg1d.put_symbol(c.symbol);
    agg1d.put_currency(c.open);
    agg1d.put_currency(c.high);
    agg1d.put_currency(c.low);
    agg1d.put_currency(c.close);
    agg1d.put_u64(c.volume);
    agg1d.put_currency(0.0);
    agg1d.write();
  }
  println!("Flushing {} took {}ms", year, now.elapsed().as_millis());
  agg1d.flush();

  println!("Year {}: {} candles", year, num_candles);
}

pub fn download_agg1d(thread_pool: &ThreadPool, config: Arc<Config>) {
  // Setup DB
  let schema = Schema::new("agg1d")
    .add_cols(vec![
      Column::new("sym", ColumnType::SYMBOL16),
      Column::new("open", ColumnType::CURRENCY),
      Column::new("high", ColumnType::CURRENCY),
      Column::new("low", ColumnType::CURRENCY),
      Column::new("close", ColumnType::CURRENCY),
      Column::new("volume", ColumnType::U64),
      Column::new("close_un", ColumnType::CURRENCY),
    ])
    .partition_by(PartitionBy::Year);

  let mut agg1d = Table::create_or_open(schema).expect("Could not open table");
  let from = match agg1d.get_last_ts() {
    Some(ts) => ts.to_naive_date_time().date().year() + 1,
    None => 2004
  };
  let to = Utc::now().date().year();
  for i in from..=to {
    download_agg1d_year(i, &thread_pool, &mut agg1d, config.clone());
  }
}
