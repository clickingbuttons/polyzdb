extern crate polygon_io;
use crate::util::MarketDays;
use chrono::{Duration, NaiveDate, Utc};
use polygon_io::{
  client::Client,
  equities::trades::{Trade, TradesParams}
};
use std::{
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

fn download_trades_day(
  date: NaiveDate,
  thread_pool: &ThreadPool,
  ratelimit: &mut Handle,
  agg1d: &Table,
  trades_table: &mut Table,
  client: Arc<Client>
) {
  let now = Instant::now();
  let from = date.clone();
  println!(
    "{}: Scanning agg1d for symbols",
    date
  );
  let mut symbols = HashSet::<String>::default();
  agg1d.scan(
    from.and_hms(0, 0, 0).timestamp_nanos(),
    from.and_hms(0, 0, 0).timestamp_nanos(),
    vec!["ts", "sym"],
    |row| {
      symbols.insert(row[1].get_symbol().clone());
    }
  );

  println!(
    "{}: Downloading trades for {} symbols",
    date,
    symbols.len()
  );
  let trades = Arc::new(Mutex::new(Vec::<Trade>::new()));
  let counter = Arc::new(AtomicUsize::new(0));
  let num_syms = symbols.len();
  println!("{:5} / {:5} symbols", 0, num_syms);
  for sym in symbols.iter() {
    let day_format = date.clone();
    let sym = sym.clone();
    let trades_day = Arc::clone(&trades);
    let client = client.clone();
    let mut ratelimit = ratelimit.clone();
    let params = TradesParams::new().with_limit(50_000).params;
    let counter = counter.clone();
    thread_pool.execute(move || {
      // Retry up to 50 times
      for j in 0..50 {
        ratelimit.wait();
        match client.get_trades(&sym, date, Some(&params)) {
          Ok(mut resp) => {
            // println!("{} {:6}: {} candles", month_format, sym, candles.len());
            trades_day.lock().unwrap().append(&mut resp.results);
            counter.fetch_add(1, Ordering::Relaxed);
            println!("\x1b[1A\x1b[K{:5} / {:5} symbols [{}]", counter.load(Ordering::Relaxed), num_syms, sym);
            return;
          }
          Err(e) => {
            match e.kind() {
              ErrorKind::UnexpectedEof => {
                eprintln!("{} {:6}: No data", day_format, sym);
                return;
              }
              _ => {
                eprintln!(
                  "{} {:6}: get_trades retry {}: {}",
                  day_format,
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
      eprintln!("{} {:6}: failure", day_format, sym);
      process::exit(1);
    });
  }

  thread_pool.join();

  let mut trades = trades.lock().unwrap();
  let num_trades = trades.len();
  // Sort by ts, symbol
  println!("{}: Sorting {} trades", date, num_trades);
  trades.sort_unstable_by(|c1, c2| {
    if c1.ts == c2.ts {
      c1.symbol.cmp(&c2.symbol)
    } else {
      c1.ts.cmp(&c2.ts)
    }
  });

  println!("{}: Writing {} trades", date, num_trades);
  for t in trades.iter() {
    trades_table.put_timestamp(t.ts);
    trades_table.put_symbol(t.symbol.clone());
    trades_table.put_u32(t.size);
    trades_table.put_currency(t.price);
    trades_table.put_u8(t.exchange);
    trades_table.put_u8(t.tape);
    trades_table.write();
  }
  println!("{}: Flushing {} trades", date, num_trades);
  trades_table.flush();
  assert_eq!(trades_table.cur_partition_meta.row_count, num_trades);
  assert_eq!(trades_table.partition_meta.get(&date.to_string()).unwrap().row_count, num_trades);

  println!(
    "{}: downloaded in {}s",
    date,
    now.elapsed().as_secs()
  )
}

pub fn download_trades(thread_pool: &ThreadPool, ratelimit: &mut Handle, client: Arc<Client>, data_dirs: Vec<&str>) {
  // Get existing symbols
  let agg1d =
    Table::open("agg1d").expect("Table agg1d must exist to load symbols to download in trades");
  // Setup DB
  let schema = Schema::new("trades")
    .add_cols(vec![
      Column::new("sym", ColumnType::SYMBOL16),
      Column::new("size", ColumnType::U32),
      Column::new("price", ColumnType::CURRENCY),
      Column::new("exchange", ColumnType::U8),
      Column::new("tape", ColumnType::U8),
    ])
    .data_dirs(data_dirs)
    .partition_by(PartitionBy::Day);

  let mut trades = Table::create_or_open(schema).expect("Could not open table");
  let from = match trades.get_last_ts() {
    Some(ts) => {
      ts.to_naive_date_time().date() + Duration::days(1)
    },
    None => NaiveDate::from_ymd(2004, 1, 1)
  };
  let to = Utc::now().naive_utc().date();
  println!("{} - {}", from, to);
  for day in (MarketDays { from, to }) {
    println!("Downloading {}", day);
    download_trades_day(
      day,
      &thread_pool,
      ratelimit,
      &agg1d,
      &mut trades,
      client.clone()
    );
  }
}
