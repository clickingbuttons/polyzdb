extern crate serde_json;
extern crate ureq;

use crate::{
  config::Config,
  polygon::{get_results, OHLCV, POLYGON_BASE}
};
use chrono::{Duration, NaiveDate};
use std::io::{self, Error, ErrorKind};

pub fn get_agg1m(
  from: &NaiveDate,
  to: &NaiveDate,
  symbol: &str,
  config: &Config
) -> io::Result<Vec<OHLCV>> {
  let uri = format!(
    "{}/aggs/ticker/{}/range/1/minute/{}/{}?unadjusted=true&apiKey={}&limit=50000",
    POLYGON_BASE,
    symbol,
    from.format("%Y-%m-%d"),
    to.format("%Y-%m-%d"),
    config.polygon_key
  );
  let mut candles = get_results(&uri)?;

  for row in candles.iter_mut() {
    // Polygon returns GMT milliseconds for this endpoint
    row.ts =
      // Subtract 5h of milliseconds
      (row.ts - (5 * 60 * 60 * 1_000))
      // Convert to ns
      * 1_000_000;
  }

  if candles.len() == 0 {
    return Err(Error::new(ErrorKind::UnexpectedEof, "Results is empty"));
  }

  let mut min_ts = candles[0].ts;
  let mut max_ts = candles[0].ts;
  for candle in &candles {
    if candle.ts > max_ts {
      max_ts = candle.ts;
    }
    if candle.ts < min_ts {
      min_ts = candle.ts;
    }
  }
  let to = to.clone() + Duration::days(1);
  if min_ts < from.and_hms(0, 0, 0).timestamp_nanos() {
    return Err(Error::new(ErrorKind::BrokenPipe, format!("ts {} is too small", min_ts)));
  }
  if max_ts > to.and_hms(0, 0, 0).timestamp_nanos() {
    return Err(Error::new(ErrorKind::BrokenPipe, format!("ts {} is too big", max_ts)));
  }

  Ok(candles)
}
