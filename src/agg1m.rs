extern crate polygon_io;
use chrono::{Datelike, Duration, NaiveDate, Utc};
use polygon_io::{
  client::Client,
  core::Candle,
  core::aggs::AggsParams,
  core::aggs::Timespan
};
use std::{
  cmp,
  collections::HashSet,
  io::ErrorKind,
  process,
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

fn add_month(date: &NaiveDate) -> NaiveDate {
  let mut to_year = date.year();
  let mut to_month = date.month();
  if to_month == 12 {
    to_month = 1;
    to_year += 1;
  } else {
    to_month += 1;
  }

  NaiveDate::from_ymd(to_year, to_month, date.day())
}

fn download_agg1m_month(
  year: i32,
  month: u32,
  thread_pool: &ThreadPool,
  agg1d: &Table,
  agg1m: &mut Table,
  client: Arc<Client>
) {
  // The US equity market is open from 4:00-20:00 which is 960 minutes.
  // We could download up to 50k bars/request with &limit=50000, which is 52 days.
  // However, Polygon recommends downloading a month at a time.
  // Humans think in months much better than 52 day periods, so I'm inclined to
  // go with months for now.
  let now = Instant::now();
  let month_start = NaiveDate::from_ymd(year, month, 1);
  let from = cmp::max(
    agg1m
      .get_last_ts()
      .unwrap_or(0)
      .to_naive_date_time()
      .date()
      + Duration::days(1),
    month_start
  );
  let to = cmp::min(add_month(&month_start) - Duration::days(1), Utc::today().naive_utc());
  let month_format = format!("{}-{:02}", year, month);
  println!(
    "{}: Scanning agg1d for symbols in {}..{}",
    month_format, from, to
  );
  let mut symbols = HashSet::<String>::default();
  agg1d.scan(
    from.and_hms(0, 0, 0).timestamp_nanos(),
    to.and_hms(0, 0, 0).timestamp_nanos(),
    vec!["ts", "sym", "volume"],
    |row| {
      if row[2].get_u64() > 0 {
        symbols.insert(row[1].get_symbol().clone());
      }
    }
  );

  println!(
    "{}: Downloading candles for {} symbols",
    month_format,
    symbols.len()
  );
  let candles = Arc::new(Mutex::new(Vec::<Candle>::new()));
  for (i, sym) in symbols.iter().enumerate() {
    let month_format = month_format.clone();
    let sym = sym.clone();
    let candles_year = Arc::clone(&candles);
    let client = client.clone();
    let params = AggsParams::new().with_adjusted(false).with_limit(50_000).params;
    thread_pool.execute(move || {
      // Have 2/3 sleep for 1-2s to avoid spamming at start
      thread::sleep(std::time::Duration::from_secs(i as u64 % 3));
      // Retry up to 10 times
      for j in 0..10 {
        match client.get_aggs(&sym, 1, Timespan::Minute, from, to, Some(&params)) {
          Ok(mut resp) => {
            // println!("{} {:6}: {} candles", month_format, sym, candles.len());
            candles_year.lock().unwrap().append(&mut resp.results);
            return;
          }
          Err(e) => {
            match e.kind() {
              // Give up if there's no data. We'll get the ticks later.
              ErrorKind::UnexpectedEof => {
                eprintln!("{} {:6}: No data", month_format, sym);
                return;
              }
              _ => {
                eprintln!(
                  "{} {:6}: get_agg1m retry {}: {}",
                  month_format,
                  sym,
                  j + 1,
                  e.to_string()
                );
              }
            }
          }
        }
        thread::sleep(std::time::Duration::from_secs(j));
      }
      eprintln!("{} {:6}: failure", month_format, sym);
      process::exit(1);
    });
  }

  thread_pool.join();

  let mut candles = candles.lock().unwrap();
  let num_candles = candles.len();
  // Sort by ts, symbol
  println!("{}: Sorting {} candles", month_format, num_candles);
  candles.sort_unstable_by(|c1, c2| {
    if c1.ts == c2.ts {
      c1.symbol.cmp(&c2.symbol)
    } else {
      c1.ts.cmp(&c2.ts)
    }
  });

  println!("{}: Writing {} candles", month_format, num_candles);
  for c in candles.iter() {
    agg1m.put_timestamp(c.ts);
    agg1m.put_symbol(c.symbol.clone());
    agg1m.put_currency(c.open);
    agg1m.put_currency(c.high);
    agg1m.put_currency(c.low);
    agg1m.put_currency(c.close);
    agg1m.put_u64(c.volume);
    agg1m.put_currency(0.0);
    agg1m.write();
  }
  println!("{}: Flushing {} candles", month_format, num_candles);
  agg1m.flush();
  assert_eq!(agg1m.cur_partition_meta.row_count, num_candles);

  println!(
    "{}: downloaded in {}s",
    month_format,
    now.elapsed().as_secs()
  )
}

pub fn download_agg1m(thread_pool: &ThreadPool, client: Arc<Client>) {
  // Get existing symbols
  let agg1d =
    Table::open("agg1d").expect("Table agg1d must exist to load symbols to download in agg1m");
  // Setup DB
  let schema = Schema::new("agg1m")
    .add_cols(vec![
      Column::new("sym", ColumnType::SYMBOL16),
      Column::new("open", ColumnType::CURRENCY),
      Column::new("high", ColumnType::CURRENCY),
      Column::new("low", ColumnType::CURRENCY),
      Column::new("close", ColumnType::CURRENCY),
      Column::new("volume", ColumnType::U64),
      Column::new("close_un", ColumnType::CURRENCY),
    ])
    .partition_by(PartitionBy::Month);

  // let from = NaiveDate::from_ymd(2004, 01, 01);
  // let to   = NaiveDate::from_ymd(2004, 03, 01);
  // println!("{} {}", from, to);
  // agg1d.scan(
  //   from.and_hms(0, 0, 0).timestamp_nanos(),
  //   to.and_hms(0, 0, 0).timestamp_nanos(),
  //   vec!["ts", "sym"],
  //   |row| {
  //     // println!("{:?}", row);
  //     if row[1].get_symbol() == "RHT" {
  //       println!("{}", row[0].get_timestamp());
  //     }
  //   }
  // );
  let mut agg1m = Table::create_or_open(schema).expect("Could not open table");
  let from = match agg1m.get_last_ts() {
    Some(ts) => {
      let last_date = ts.to_naive_date_time().date();
      NaiveDate::from_ymd(last_date.year(), last_date.month(), 1)
    },
    None => NaiveDate::from_ymd(2004, 1, 1)
  };
  let to = Utc::now().naive_utc().date();
  let mut iter = from.clone();
  while iter < to {
    println!("Downloading {}-{:02}", iter.year(), iter.month());
    download_agg1m_month(
      iter.year(),
      iter.month(),
      &thread_pool,
      &agg1d,
      &mut agg1m,
      client.clone()
    );
    iter = add_month(&iter);
  }
}