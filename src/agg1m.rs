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

fn sub_month(date: &NaiveDate) -> NaiveDate {
  let mut to_year = date.year();
  let mut to_month = date.month();
  if to_month == 1 {
    to_month = 12;
    to_year -= 1;
  } else {
    to_month -= 1;
  }

  NaiveDate::from_ymd(to_year, to_month, date.day())
}

fn download_agg1m_month(
  year: i32,
  month: u32,
  thread_pool: &ThreadPool,
  ratelimit: &mut Handle,
  agg1d: &Table,
  agg1m: &mut Table,
  client: Arc<Client>
) {
  // The US equity market is open from 4:00-20:00 which is 960 minutes.
  // We could download up to 50k bars/request with &limit=50000, which is 52 days.
  // However, Polygon recommends downloading a month at a time.
  // Humans think in months better than 52 day periods, so I'm inclined to
  // go with months for now.
  let now = Instant::now();
  let month_format = format!("{}-{:02}", year, month);
  let from = match agg1m.partition_meta.get(&month_format) {
    Some(meta) => meta.to_ts.to_naive_date_time().date() + Duration::days(1),
    None => NaiveDate::from_ymd(year, month, 1)
  };
  let month_start = NaiveDate::from_ymd(year, month, 1);
  let to = cmp::min(add_month(&month_start) - Duration::days(1), Utc::today().naive_utc());
  if from >= to {
    eprintln!("Already downloaded agg1m until {}!", from - Duration::days(1));
    return;
  }
  eprintln!(
    "{}: Scanning agg1d for symbols in {}..{}",
    month_format, from, to
  );
  let mut symbols = HashSet::<String>::default();
  let partitions = agg1d.partition_iter(
    from.and_hms(0, 0, 0).timestamp_nanos(),
    to.and_hms(0, 0, 0).timestamp_nanos(),
    vec!["sym", "volume"],
  );
  for partition in partitions {
    let sym_indexes = partition[0].get_u16();
    let volumes = partition[1].get_u64();
    volumes.iter().zip(sym_indexes.iter()).for_each(|(v, sym_i)| {
      if *v > 0 {
        let sym = partition[0].symbols[*sym_i as usize - 1].clone();
        symbols.insert(sym);
      }
    });
  }
  if symbols.len() == 0{
    eprintln!("{}: no agg1d", month_format);
    return;
  }

  eprintln!(
    "{}: Downloading candles for {} symbols",
    month_format,
    symbols.len()
  );
  let candles = Arc::new(Mutex::new(Vec::<Candle>::new()));
  let counter = Arc::new(AtomicUsize::new(0));
  let num_syms = symbols.len();
  eprintln!("{:5} / {:5} symbols", 0, num_syms);
  for sym in symbols.iter() {
    let month_format = month_format.clone();
    let sym = sym.clone();
    let candles_year = Arc::clone(&candles);
    let client = client.clone();
    let mut ratelimit = ratelimit.clone();
    let params = AggsParams::new().unadjusted(true).limit(50_000).params;
    let counter = counter.clone();
    thread_pool.execute(move || {
      // Retry up to 50 times
      for j in 0..50 {
        ratelimit.wait();
        match client.get_aggs(&sym, 1, Timespan::Minute, from, to, Some(&params)) {
          Ok(mut resp) => {
            candles_year.lock().unwrap().append(&mut resp.results);
            counter.fetch_add(1, Ordering::Relaxed);
            println!("\x1b[1A\x1b[K{:5} / {:5} symbols [{}]", counter.load(Ordering::Relaxed), num_syms, sym);
            return;
          }
          Err(e) => {
            match e.kind() {
              // Give up if there's no data. We'll get the ticks later.
              ErrorKind::UnexpectedEof => {
                eprintln!("{} {:6}: No data\n", month_format, sym);
                return;
              }
              _ => {
                eprintln!(
                  "{} {:6}: get_agg1m retry {}: {}\n",
                  month_format,
                  sym,
                  j + 1,
                  e.to_string()
                );
                std::thread::sleep(std::time::Duration::from_secs(j + 1));
              }
            }
          }
        }
      }
      eprintln!("{} {:6}: failure", month_format, sym);
      process::exit(1);
    });
  }

  thread_pool.join();

  let mut candles = candles.lock().unwrap();
  let num_candles = candles.len();
  // Sort by ts, symbol
  eprintln!("{}: Sorting {} candles", month_format, num_candles);
  candles.sort_unstable_by(|c1, c2| {
    if c1.ts == c2.ts {
      c1.symbol.cmp(&c2.symbol)
    } else {
      c1.ts.cmp(&c2.ts)
    }
  });

  eprintln!("{}: Writing {} candles", month_format, num_candles);
  for c in candles.drain(..) {
    agg1m.put_timestamp(c.ts);
    agg1m.put_symbol(c.symbol);
    agg1m.put_f64(c.open);
    agg1m.put_f64(c.high);
    agg1m.put_f64(c.low);
    agg1m.put_f64(c.close);
    agg1m.put_u32(c.volume as u32);
    agg1m.write();
  }
  eprintln!("{}: Flushing {} candles", month_format, num_candles);
  agg1m.flush();

  eprintln!(
    "{}: downloaded in {}s",
    month_format,
    now.elapsed().as_secs()
  )
}

pub fn download_agg1m(thread_pool: &ThreadPool, ratelimit: &mut Handle, client: Arc<Client>, column_dirs: Vec<&str>) {
  // Get existing symbols
  let agg1d =
    Table::open("agg1d").expect("Table agg1d must exist to load symbols to download in agg1m");
  // Setup DB
  let schema = Schema::new("agg1m")
    .add_cols(vec![
      Column::new("ts", ColumnType::Timestamp).with_resolution(60 * 1_000_000_000),
      Column::new("sym", ColumnType::Symbol16).with_sym_name("us_equities"),
      Column::new("open", ColumnType::F64),
      Column::new("high", ColumnType::F64),
      Column::new("low", ColumnType::F64),
      Column::new("close", ColumnType::F64),
      Column::new("volume", ColumnType::U32)
    ])
    .partition_dirs(column_dirs)
    .partition_by(PartitionBy::Month);

  let mut agg1m = Table::create_or_open(schema).expect("Could not open table");
  let from = NaiveDate::from_ymd(2004, 1, 1);
  let today = Utc::now().naive_utc().date();
  let to = NaiveDate::from_ymd(today.year(), today.month(), 1);
  let mut iter = to.clone();
  while iter > from {
    let formatted = format!("{}-{:02}", iter.year(), iter.month());
    let is_today = iter.year() == today.year() && iter.month() == today.month();
    if agg1m.partition_meta.get(&formatted).is_none() || is_today {
      eprintln!("Downloading agg1m {}", formatted);
      download_agg1m_month(
        iter.year(),
        iter.month(),
        &thread_pool,
        ratelimit,
        &agg1d,
        &mut agg1m,
        client.clone()
      );
    }
    iter = sub_month(&iter);
  }
}
