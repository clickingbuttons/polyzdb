use crate::util::MarketDays;
use chrono::{Datelike, Duration, NaiveDate, Utc};
use polygon_io::{
  client::Client,
  core::Candle,
  core::grouped::{Locale, Market, GroupedParams}
};
use std::{
  cmp, process,
  sync::{Arc, Mutex},
  time::Instant
};
use threadpool::ThreadPool;
use zdb::{
  calendar::ToNaiveDateTime,
  schema::{Column, ColumnType, PartitionBy, Schema},
  table::Table
};
use std::sync::atomic::{AtomicUsize, Ordering};

fn download_agg1d_year(
  year: i32,
  thread_pool: &ThreadPool,
  agg1d: &mut Table,
  client: &Client
) {
  let now = Instant::now();
  let from = match agg1d.partition_meta.get(&year.to_string()) {
    Some(meta) => meta.to_ts.to_naive_date_time().date() + Duration::days(1),
    None => NaiveDate::from_ymd(year, 1, 1)
  };
  let to = cmp::min(
    NaiveDate::from_ymd(year + 1, 1, 1),
    Utc::now().naive_utc().date()
  );
  let market_days = (MarketDays { from, to }).collect::<Vec<_>>();
  if from >= to || market_days.len() == 0 {
    eprintln!("Already downloaded agg1d until {}!", from - Duration::days(1));
    return;
  }
  let candles = Arc::new(Mutex::new(Vec::<Candle>::new()));

  eprintln!("Downloading agg1d in {}..{}", from, to);
  let num_days = market_days.len();
  let counter = Arc::new(AtomicUsize::new(0));
  eprintln!("{:3} / {} days", 0, num_days);
  for day in market_days.into_iter() {
    let candles_year = Arc::clone(&candles);
    let mut client = client.clone();
    let grouped_params = GroupedParams::new().unadjusted(true).params;
    let counter = counter.clone();
    thread_pool.execute(move || {
      // Retry up to 10 times
      for j in 0..10 {
        match client.get_grouped(Locale::US, Market::Stocks, day, Some(&grouped_params)) {
          Ok(mut resp) => {
            candles_year.lock().unwrap().append(&mut resp.results);
            counter.fetch_add(1, Ordering::Relaxed);
            println!("\x1b[1A\x1b[K{:3} / {} days [{}, {}]", counter.load(Ordering::Relaxed), num_days, day, resp.results_count);
            return;
          }
          Err(e) => {
            eprintln!("{}: get_grouped retry {}: {}\n", day, j + 1, e.to_string());
            std::thread::sleep(std::time::Duration::from_secs(j + 1));
          }
        }
      }
      eprintln!("{}: failure", &day);
      process::exit(1);
    });
  }
  thread_pool.join();

  // Sort by ts, symbol
  let mut candles = candles.lock().unwrap();
  let num_candles = candles.len();
  eprintln!("{}: Sorting {} candles", year, num_candles);
  candles.sort_unstable_by(|c1, c2| {
    if c1.ts == c2.ts {
      c1.symbol.cmp(&c2.symbol)
    } else {
      c1.ts.cmp(&c2.ts)
    }
  });
  eprintln!("{}: Writing {} candles", year, num_candles);
  for c in candles.drain(..) {
    // Filter out crazy tickers
    // https://github.com/polygon-io/issues/issues/3
    if !c.symbol.chars().all(|c| c.is_ascii_graphic()) {
      let date = c.ts.to_naive_date_time().date();
      eprintln!("{}: Bad symbol {}", date, c.symbol);
      continue;
    }
    agg1d.put_timestamp(c.ts);
    agg1d.put_symbol(c.symbol);
    agg1d.put_f64(c.open);
    agg1d.put_f64(c.high);
    agg1d.put_f64(c.low);
    agg1d.put_f64(c.close);
    agg1d.put_u64(c.volume);
    agg1d.write();
  }
  eprintln!("{}: Flushing {} candles", year, num_candles);
  agg1d.flush();

  eprintln!("{}: done in {}s", year, now.elapsed().as_secs());
}

pub fn download_agg1d(thread_pool: &ThreadPool, client: &mut Client) {
  let now = Instant::now();
  // Setup DB
  let schema = Schema::new("agg1d")
    .add_cols(vec![
      Column::new("ts", ColumnType::Timestamp).with_resolution(24 * 60 * 60 * 1_000_000_000),
      Column::new("sym", ColumnType::Symbol16).with_sym_name("us_equities"),
      Column::new("open", ColumnType::F64),
      Column::new("high", ColumnType::F64),
      Column::new("low", ColumnType::F64),
      Column::new("close", ColumnType::F64),
      Column::new("volume", ColumnType::U64)
    ])
    .partition_by(PartitionBy::Year);

  let mut agg1d = Table::create_or_open(schema).expect("Could not open table");
  let from = 2004;
  let to = (Utc::now().date() - Duration::days(1)).year();
  eprintln!("Downloading agg1d");
  for i in (from..=to).rev() {
    if agg1d.partition_meta.get(&format!("{}", i)).is_none() || i == to {
      download_agg1d_year(i, &thread_pool, &mut agg1d, client);
    }
  }
  eprintln!("Downloaded agg1d in {}s", now.elapsed().as_secs());
}
