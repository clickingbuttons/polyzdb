extern crate serde_json;
extern crate ureq;

use crate::{
  config::Config,
  polygon::{get_results, OHLCV, POLYGON_BASE}
};
use chrono::NaiveDate;
use std::io::{self, Error, ErrorKind};

pub fn get_grouped(date: &NaiveDate, config: &Config) -> io::Result<Vec<OHLCV>> {
  let uri = format!(
    "{}/aggs/grouped/locale/US/market/STOCKS/{}?unadjusted=true&apiKey={}",
    POLYGON_BASE,
    date.format("%Y-%m-%d"),
    config.polygon_key
  );
  let mut candles = get_results(&uri)?;

  let ts = date.and_hms(0, 0, 0).timestamp_nanos();
  for row in candles.iter_mut() {
    row.ts = ts;
  }

  if candles.len() == 0 {
    return Err(Error::new(ErrorKind::UnexpectedEof, "Results is empty"));
  }

  Ok(candles)
}
