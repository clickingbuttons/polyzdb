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
use clap::{app_from_crate, crate_authors, crate_description, crate_version, crate_name, Arg};

fn main() {
  let matches = app_from_crate!()
    .arg(
      Arg::with_name("ratelimit")
        .help("Sets ratelimit for polygon_io client")
        .long("ratelimit")
        .alias("rl")
        .takes_value(true)
        .default_value("100")
    )
    .arg(
      Arg::with_name("data-dir")
        .help("Adds a directory to save data to to schema of agg1m or trades")
        .long("data-dir")
        .short("d")
        .takes_value(true)
        .multiple(true)
        .default_value("data")
    )
    .arg(
      Arg::with_name("agg1d")
        .help("Download agg1d data using grouped endpoint")
        .long("agg1d")
        .short("D")
    )
    .arg(
      Arg::with_name("agg1m")
        .help("Download agg1m data using agg1d data as an index")
        .long("agg1m")
        .short("m")
    )
    .arg(
      Arg::with_name("trades")
        .help("Download trade data using agg1d data as an index")
        .long("trades")
        .short("t")
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
  std::thread::spawn(move || { ratelimit.run(); });

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

  let download_all = !matches.is_present("agg1d") && !matches.is_present("agg1m") && !matches.is_present("trades");

  if download_all || matches.is_present("agg1d") {
    println!("Downloading agg1d index data using dirs [\"data\"]");
    download_agg1d(&thread_pool, &mut handle, client.clone());
  }

  let data_dirs = matches.values_of("data-dir").unwrap().into_iter().collect::<Vec<&str>>();
  if download_all || matches.is_present("agg1m") {
    println!("Downloading agg1m data using dirs {:?}", data_dirs);
    download_agg1m(&thread_pool, &mut handle, client.clone(), data_dirs.clone());
  }
  if download_all || matches.is_present("trades") {
    println!("Downloading trade data using dirs {:?}", data_dirs);
    download_trades(&thread_pool, &mut handle, client.clone(), data_dirs);
  }
}

