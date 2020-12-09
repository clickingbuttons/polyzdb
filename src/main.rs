mod agg1d;
mod agg1m;
mod trades;
mod util;

use polygon_io::client::Client;
use std::{panic, process, sync::Arc};
use threadpool::ThreadPool;
use ratelimit;

use agg1d::download_agg1d;
use agg1m::download_agg1m;
use trades::download_trades;

fn main() {
  let config = Arc::new(Client::new());
  // Polygon starts throttling after 100 req/s
  let polygon_limit: usize = 100;
  let mut ratelimit = ratelimit::Builder::new()
    .capacity(1)
    .quantum(1)
    .frequency(polygon_limit as u32)
    .build();
  // Allow use in multiple threads
  let mut handle = ratelimit.make_handle();
  std::thread::spawn(move || { ratelimit.run(); });
  // Don't spawn too many threads
  let thread_pool = ThreadPool::new(polygon_limit);
  // Panic if thread panics
  let orig_hook = panic::take_hook();
  panic::set_hook(Box::new(move |panic_info| {
    orig_hook(panic_info);
    process::exit(1);
  }));

  println!("Using {} threads", polygon_limit);
  println!("Downloading agg1d index data");
  download_agg1d(&thread_pool, &mut handle, config.clone());
  println!("Downloading agg1m data");
  download_agg1m(&thread_pool, &mut handle, config.clone());
  println!("Downloading trade data");
  download_trades(&thread_pool, &mut handle, config.clone());
}

