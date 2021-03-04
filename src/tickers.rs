use crate::util::MarketDays;
use chrono::{Datelike, Duration, NaiveDate, Utc, FixedOffset, TimeZone};
use polygon_io::{
  client::Client,
  reference::tickers::TickerVx
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
use ratelimit::Handle;
use std::sync::atomic::{AtomicUsize, Ordering};

fn download_tickers_year(
  year: i32,
  thread_pool: &ThreadPool,
  ratelimit: &mut Handle,
  tickers: &mut Table,
  client: Arc<Client>
) {
  let now = Instant::now();
  let from = cmp::max(
    tickers
      .get_last_ts()
      .unwrap_or(0)
      .to_naive_date_time()
      .date() + Duration::days(1),
    NaiveDate::from_ymd(year, 1, 1)
  );
  let to = cmp::min(
    NaiveDate::from_ymd(year + 1, 1, 1),
    Utc::now().naive_utc().date() - Duration::days(1)
  );
  if from >= to {
    eprintln!("Already downloaded!");
    return;
  }
  let tickers_year = Arc::new(Mutex::new(Vec::<TickerVx>::new()));

  eprintln!("Downloading tickers in {}..{}", from, to);
  let market_days = (MarketDays { from, to }).collect::<Vec<_>>();
  let num_days = market_days.len();
  let counter = Arc::new(AtomicUsize::new(0));
  eprintln!("{:3} / {} days", 0, num_days);
  for day in market_days.into_iter() {
    let tickers_year = Arc::clone(&tickers_year);
    let client = client.clone();
    let mut ratelimit = ratelimit.clone();
    let counter = counter.clone();
    thread_pool.execute(move || {
      // Retry up to 10 times
      for j in 0..10 {
        ratelimit.wait();
        match client.get_all_tickers_vx(day) {
          Ok(mut results) => {
            for ticker in results.iter_mut() {
              // Hijack this mostly useless field to put current date
              ticker.last_updated_utc = FixedOffset::east(0)
                .ymd(day.year(), day.month(), day.day())
                .and_hms(0, 0, 0);
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
    tickers.put_symbol(c.currency_name);
    tickers.put_symbol(c.cik.unwrap_or(String::new()));
    tickers.put_symbol(c.composite_figi.unwrap_or(String::new()));
    tickers.put_symbol(c.share_class_figi.unwrap_or(String::new()));
    tickers.write();
  }
  eprintln!("{}: Flushing {} candles", year, num_candles);
  tickers.flush();

  eprintln!("{}: done in {}s", year, now.elapsed().as_secs());
}

pub fn download_tickers(thread_pool: &ThreadPool, ratelimit: &mut Handle, client: Arc<Client>) {
  // Setup DB
  let schema = Schema::new("tickers")
    .add_cols(vec![
      Column::new("ts", ColumnType::Timestamp).with_resolution(24 * 60 * 60 * 1_000_000_000),
      Column::new("sym", ColumnType::Symbol16),
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
  let from = match tickers.get_last_ts() {
    Some(ts) => ts.to_naive_date_time().date().year(),
    None => 2004
  };
  let to = (Utc::now().date() - Duration::days(1)).year();
  for i in from..=to {
    download_tickers_year(i, &thread_pool, ratelimit, &mut tickers, client.clone());
  }
}
