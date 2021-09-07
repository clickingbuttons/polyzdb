mod trades;
mod util;
use polygon_io::client::Client;
use std::{panic, process, sync::Arc, thread};
use threadpool::ThreadPool;
use ratelimit;
use trades::download_trades;
use clap::{app_from_crate, crate_authors, crate_description, crate_version, crate_name, Arg};

fn main() {
  let matches = app_from_crate!()
    .arg(
      Arg::with_name("ratelimit")
        .help("Sets ratelimit for polygon_io client")
        .long("ratelimit")
        .takes_value(true)
        .default_value("100")
    )
    .arg(
      Arg::with_name("data-dir")
        .help("Adds a directory to save data to to schema of agg1m or trades")
        .long("data-dir")
        .takes_value(true)
        .multiple(true)
        .default_value("data")
    )
    .arg(
      Arg::with_name("agg1d")
        .help("Download agg1d data using grouped endpoint")
        .long("agg1d")
    )
    .arg(
      Arg::with_name("tickers")
        .help("Download tickers data using reference tickers vX endpoint")
        .long("tickers")
    )
    .arg(
      Arg::with_name("agg1m")
        .help("Download agg1m data using agg1d data as an index")
        .long("agg1m")
    )
    .arg(
      Arg::with_name("trades")
        .help("Download trade data using agg1d data as an index")
        .long("trades")
    )
    .get_matches();
  // Polygon starts throttling after 100 req/s
  let polygon_limit: usize = matches.value_of("ratelimit").unwrap().parse::<usize>().expect("Ratelimit must be an unsigned int");
  let mut ratelimit = ratelimit::Builder::new()
    .capacity(1)
    .quantum(1)
    .frequency(polygon_limit as u32)
    .build();
  let mut handle = ratelimit.make_handle();
  thread::spawn(move || { ratelimit.run(); });

  // Don't spawn too many threads
  let thread_pool = ThreadPool::new(polygon_limit);
  
  // Holds API key
  let client = Arc::new(Client::new());

  // Panic if thread panics
  let orig_hook = panic::take_hook();
  panic::set_hook(Box::new(move |panic_info| {
    orig_hook(panic_info);
    process::exit(1);
  }));

  eprintln!("Downloading trade data");
  download_trades(&thread_pool, &mut handle, client.clone());
}
