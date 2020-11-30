extern crate polygon_io;
use crate::zdb::MarketDays;
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
  thread,
  time::Instant
};
use threadpool::ThreadPool;
use zdb::{
  calendar::ToNaiveDateTime,
  schema::{Column, ColumnType, PartitionBy, Schema},
  table::Table
};

fn download_trades_day(
  date: NaiveDate,
  thread_pool: &ThreadPool,
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
  for (i, sym) in symbols.iter().enumerate() {
    let day_format = date.clone();
    let sym = sym.clone();
    let trades_day = Arc::clone(&trades);
    let client = client.clone();
    let params = TradesParams::new().with_limit(50_000).params;
    thread_pool.execute(move || {
      // Have 2/3 sleep for 1-2s to avoid spamming at start
      thread::sleep(std::time::Duration::from_secs(i as u64 % 3));
      // Retry up to 10 times
      for j in 0..10 {
        match client.get_trades(&sym, date, Some(&params)) {
          Ok(mut resp) => {
            // println!("{} {:6}: {} candles", month_format, sym, candles.len());
            trades_day.lock().unwrap().append(&mut resp.results);
            return;
          }
          Err(e) => {
            match e.kind() {
              // Give up if there's no data. We'll get the ticks later.
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
              }
            }
          }
        }
        thread::sleep(std::time::Duration::from_secs(j));
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

  println!(
    "{}: downloaded in {}s",
    date,
    now.elapsed().as_secs()
  )
}

pub fn download_trades(thread_pool: &ThreadPool, client: Arc<Client>) {
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
    .partition_by(PartitionBy::Day);

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
  let mut trades = Table::create_or_open(schema).expect("Could not open table");
  let from = match trades.get_last_ts() {
    Some(ts) => {
      (ts.to_naive_date_time() + Duration::days(1)).date()
    },
    None => NaiveDate::from_ymd(2004, 1, 1)
  };
  let to = Utc::now().naive_utc().date();
  for day in (MarketDays { from, to }) {
    println!("Downloading {}", day);
    download_trades_day(
      day,
      &thread_pool,
      &agg1d,
      &mut trades,
      client.clone()
    );
  }
}
