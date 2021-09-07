extern crate polygon_io;
use crate::util::MarketDays;
use chrono::{NaiveDate, Utc};
use polygon_io::{
  client::Client,
  equities::trades::Trade
};
use std::{
  io::ErrorKind,
  process,
  sync::{Arc, Mutex},
  time::Instant
};
use threadpool::ThreadPool;
use ratelimit::Handle;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;

fn lines_from_file<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

fn download_trades_day(
  date: NaiveDate,
  thread_pool: &ThreadPool,
  ratelimit: &mut Handle,
  client: Arc<Client>
) {
  let now = Instant::now();
  let symbols = lines_from_file("tickers.txt").unwrap().map(|l| l.unwrap()).collect::<Vec<String>>();

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
    let client = client.clone();
    let mut ratelimit = ratelimit.clone();
    let counter = counter.clone();
    thread_pool.execute(move || {
      // Retry up to 50 times
      for j in 0..50 {
        ratelimit.wait();
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
  // Sort by ts, symbol
  eprintln!("{}: Sorting {} trades", date, num_trades);
  trades.sort_unstable_by(|c1, c2| {
    if c1.ts == c2.ts {
      c1.symbol.cmp(&c2.symbol)
    } else {
      c1.ts.cmp(&c2.ts)
    }
  });

  eprintln!("{}: Writing {} trades", date, num_trades);
  eprintln!(
    "{}: downloaded in {}s",
    date,
    now.elapsed().as_secs()
  )
}

pub fn download_trades(thread_pool: &ThreadPool, ratelimit: &mut Handle, client: Arc<Client>) {
  let from = NaiveDate::from_ymd(2004, 1, 1);
  let to = Utc::now().naive_utc().date();
  for day in (MarketDays { from, to }) {
    eprintln!("Downloading {}", day);
    download_trades_day(
      day,
      &thread_pool,
      ratelimit,
      client.clone()
    );
  }
}
