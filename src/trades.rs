extern crate polygon_io;
use crate::util::MarketDays;
use chrono::{NaiveDate, Utc, Duration};
use polygon_io::{
  client::Client,
  equities::trades::Trade
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
  schema::{Column, ColumnType, PartitionBy, Schema},
  table::Table
};
use std::sync::atomic::{AtomicUsize, Ordering};

fn download_trades_day(
  date: NaiveDate,
  thread_pool: &ThreadPool,
  agg1d: &Table,
  trades_table: &mut Table,
  client: &mut Client
) {
  let now = Instant::now();
  let from = date.clone();
  eprintln!(
    "{}: Scanning agg1d for symbols",
    date
  );
  let mut symbols = HashSet::<String>::default();
  let partitions = agg1d.partition_iter(
    from.and_hms(0, 0, 0).timestamp_nanos(),
    from.and_hms(0, 0, 0).timestamp_nanos(),
    vec!["sym"]
  );
  for partition in partitions {
    for sym_index in partition[0].get_u16() {
      let sym = partition[0].symbols[*sym_index as usize - 1].clone();
      symbols.insert(sym);
    }
  }

  eprintln!(
    "{}: Downloading trades for {} symbols",
    date,
    symbols.len()
  );
  let trades = Arc::new(Mutex::new(Vec::<Trade>::new()));
  let counter = Arc::new(AtomicUsize::new(0));
  let num_syms = symbols.len();
  eprintln!("{:5} / {:5} symbols", 0, num_syms);
  for sym in symbols.iter() {
    let day_format = date.clone();
    let sym = sym.clone();
    let trades_day = Arc::clone(&trades);
    let mut client = client.clone();
    let counter = counter.clone();
    thread_pool.execute(move || {
      // Retry up to 50 times
      for j in 0..50 {
        match client.get_all_trades(&sym, date) {
          Ok(mut resp) => {
            // println!("{} {:6}: {} candles", month_format, sym, candles.len());
            trades_day.lock().unwrap().append(&mut resp);
            counter.fetch_add(1, Ordering::Relaxed);
            println!("\x1b[1A\x1b[K{:5} / {:5} symbols [{}]", counter.load(Ordering::Relaxed), num_syms, sym);
            return;
          }
          Err(e) => {
            match e.kind() {
              ErrorKind::UnexpectedEof => {
                eprintln!("{} {:6}: No data\n", day_format, sym);
                return;
              }
              _ => {
                eprintln!(
                  "{} {:6}: get_trades retry {}: {}\n",
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
  // Sort by seq_id which is also ts
  eprintln!("{}: Sorting {} trades", date, num_trades);
  trades.sort_unstable_by(|t1, t2| t1.seq_id.cmp(&t2.seq_id));

  eprintln!("{}: Writing {} trades", date, num_trades);
  for t in trades.drain(..) {
    trades_table.put_timestamp(t.ts);
    trades_table.put_i64(t.ts_participant.unwrap_or(0));
    trades_table.put_u64(t.id);
    trades_table.put_u64(t.seq_id);
    trades_table.put_symbol(t.symbol.clone());
    trades_table.put_u32(t.size);
    trades_table.put_f64(t.price);
    trades_table.put_u32(t.conditions);
    trades_table.put_u8(t.error);
    trades_table.put_u8(t.exchange);
    trades_table.put_u8(t.tape);
    trades_table.write();
  }
  eprintln!("{}: Flushing {} trades", date, num_trades);
  trades_table.flush();
  assert_eq!(trades_table.cur_partition_meta.row_count, num_trades);
  let num_rows_inserted = match trades_table.partition_meta.get(&date.to_string()) {
    Some(meta) => meta.row_count,
    None => 0
  };
  assert_eq!(num_rows_inserted, num_trades);

  eprintln!(
    "{}: downloaded in {}s",
    date,
    now.elapsed().as_secs()
  )
}

pub fn download_trades(thread_pool: &ThreadPool, client: &mut Client, partition_dirs: Vec<&str>) {
  // Get existing symbols
  let agg1d =
    Table::open("agg1d").expect("Table agg1d must exist to load symbols to download in trades");
  // Setup DB
  let schema = Schema::new("trades")
    .add_cols(vec![
      Column::new("ts", ColumnType::Timestamp),
      Column::new("ts_participant", ColumnType::I64),
      Column::new("id", ColumnType::U64),
      Column::new("seq_id", ColumnType::U64),
      Column::new("sym", ColumnType::Symbol16).with_sym_name("us_equities"),
      Column::new("size", ColumnType::U32),
      Column::new("price", ColumnType::F64),
      Column::new("cond", ColumnType::U32),
      Column::new("err", ColumnType::U8),
      Column::new("exchange", ColumnType::U8),
      Column::new("tape", ColumnType::U8),
    ])
    .partition_dirs(partition_dirs)
    .partition_by(PartitionBy::Day);

  let mut trades = Table::create_or_open(schema).expect("Could not open table");
  let from = NaiveDate::from_ymd(2004, 1, 1);
  let to = Utc::now().naive_utc().date();
  let market_days = (MarketDays { from, to }).collect::<Vec<NaiveDate>>();
  for day in market_days.into_iter().rev() {
    if trades.partition_meta.get(&format!("{}", day.format("%Y-%m-%d"))).is_none() {
      eprintln!("Downloading {}", day);
      download_trades_day(
        day,
        &thread_pool,
        &agg1d,
        &mut trades,
        client
      );
    } else if day == to - Duration::days(1) {
      eprintln!("Already downloaded trades for {}!", day);
    }
  }
}
