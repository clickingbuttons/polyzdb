use crate::util::MarketDays;
use chrono::{Datelike, Duration, NaiveDate, Utc, FixedOffset, TimeZone};
use polygon_io::{
  client::Client,
  reference::tickers::Ticker
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

fn download_tickers_year(
  year: i32,
  thread_pool: &ThreadPool,
  tickers: &mut Table,
  client: &mut Client
) {
  let now = Instant::now();
  let from = match tickers.partition_meta.get(&year.to_string()) {
    Some(meta) => meta.to_ts.to_naive_date_time().date() + Duration::days(1),
    None => NaiveDate::from_ymd(year, 1, 1)
  };
  let to = cmp::min(
    NaiveDate::from_ymd(year + 1, 1, 1),
    Utc::now().naive_utc().date()
  );
  let market_days = (MarketDays { from, to }).collect::<Vec<_>>();
  if from >= to || market_days.len() == 0 {
    eprintln!("Already downloaded tickers until {}!", from - Duration::days(1));
    return;
  }

  let tickers_year = Arc::new(Mutex::new(Vec::<Ticker>::new()));
  eprintln!("Downloading tickers in {}..{}", from, to);
  let market_days = (MarketDays { from, to }).collect::<Vec<_>>();
  let num_days = market_days.len();
  let counter = Arc::new(AtomicUsize::new(0));
  eprintln!("{:3} / {} days", 0, num_days);
  for day in market_days.into_iter() {
    let tickers_year = Arc::clone(&tickers_year);
    let mut client = client.clone();
    let counter = counter.clone();
    thread_pool.execute(move || {
      // Retry up to 10 times
      for j in 0..10 {
        match client.get_all_tickers(&day) {
          Ok(mut results) => {
            for ticker in results.iter_mut() {
              // Hijack this mostly useless field to put current date
              ticker.last_updated_utc = FixedOffset::east(0)
                .ymd(day.year(), day.month(), day.day())
                .and_hms(0, 0, 0);
              assert!(ticker.last_updated_utc.timestamp_nanos() == day.and_hms(0, 0, 0).timestamp_nanos());
            }
            tickers_year.lock().unwrap().append(&mut results);
            counter.fetch_add(1, Ordering::Relaxed);
            println!("\x1b[1A\x1b[K{:3} / {} days [{}]", counter.load(Ordering::Relaxed), num_days, day);
            return;
          }
          Err(e) => {
            eprintln!("{}: get_all_tickers retry {}: {}\n", day, j + 1, e.to_string());
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
  let mut tickers_year = tickers_year.lock().unwrap();
  let num_candles = tickers_year.len();
  eprintln!("{}: Sorting {} tickers", year, num_candles);
  tickers_year.sort_unstable_by(|c1, c2| {
    if c1.last_updated_utc == c2.last_updated_utc {
      c1.symbol.cmp(&c2.symbol)
    } else {
      c1.last_updated_utc.cmp(&c2.last_updated_utc)
    }
  });
  eprintln!("{}: Writing {} tickers", year, num_candles);
  for c in tickers_year.drain(..) {
    tickers.put_timestamp(c.last_updated_utc.timestamp_nanos());
    tickers.put_symbol(c.symbol);
    tickers.put_symbol(c.name);
    tickers.put_symbol(c.primary_exchange.unwrap_or(String::new()));
    tickers.put_symbol(c.r#type.unwrap_or(String::new()));
    assert!(c.currency_name.is_some());
    tickers.put_symbol(c.currency_name.unwrap_or(String::new()));
    tickers.put_symbol(c.cik.unwrap_or(String::new()));
    tickers.put_symbol(c.composite_figi.unwrap_or(String::new()));
    tickers.put_symbol(c.share_class_figi.unwrap_or(String::new()));
    tickers.write();
  }
  eprintln!("{}: Flushing {} candles", year, num_candles);
  tickers.flush();

  eprintln!("{}: done in {}s", year, now.elapsed().as_secs());
}

pub fn download_tickers(thread_pool: &ThreadPool, client: &mut Client) {
  let now = Instant::now();
  // Setup DB
  let schema = Schema::new("tickers")
    .add_cols(vec![
      Column::new("ts", ColumnType::Timestamp).with_resolution(24 * 60 * 60 * 1_000_000_000),
      Column::new("sym", ColumnType::Symbol16).with_sym_name("us_equities"),
      Column::new("name", ColumnType::Symbol16),
      Column::new("primary_exchange", ColumnType::Symbol8),
      Column::new("type", ColumnType::Symbol8),
      Column::new("currency_name", ColumnType::Symbol8),
      Column::new("cik", ColumnType::Symbol16),
      Column::new("composite_figi", ColumnType::Symbol16),
      Column::new("share_class_figi", ColumnType::Symbol16),
    ])
    .partition_by(PartitionBy::Year);

  let mut tickers = Table::create_or_open(schema).expect("Could not open table");
  let from = 2004;
  let to = (Utc::now().date() - Duration::days(1)).year();
  eprintln!("Downloading tickers");
  for i in (from..=to).rev() {
    if tickers.partition_meta.get(&format!("{}", i)).is_none() || i == to {
      download_tickers_year(i, &thread_pool, &mut tickers, client);
    }
  }
  eprintln!("Downloaded tickers in {}s", now.elapsed().as_secs());
}
