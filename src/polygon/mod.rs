use std::io::{self, Error, ErrorKind};

use std::fmt;
use serde::{de,Deserialize};

pub mod aggs;
pub mod grouped;

pub const POLYGON_BASE: &str = "https://api.polygon.io/v2";

fn f64_to_u64<'de, D>(deserializer: D) -> Result<u64, D::Error>
  where
    D: de::Deserializer<'de>,
{
  struct JsonNumberVisitor;

  impl<'de> de::Visitor<'de> for JsonNumberVisitor {
    type Value = u64;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
      formatter.write_str("a u64")
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
      where
        E: de::Error,
    {
      Ok(value)
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
      where
        E: de::Error,
    {
      let res = value as u64;
      if value == res as f64 {
        Ok(value as u64)
      } else {
        Err(E::custom(format!("cannot convert {} to u64", value)))
      }
    }
  }
  deserializer.deserialize_any(JsonNumberVisitor)
}

// This is shared between these two structures:
// 1. Grouped:
// { o, h, l, c, v, t, T }
// 2. Aggs:
// { o, h, l, c, v, t }
#[derive(Debug, Deserialize)]
pub struct OHLCV {
  #[serde(rename(deserialize = "t"))]
  pub ts:     i64,
  #[serde(rename(deserialize = "T"), default)]
  pub symbol: String,
  #[serde(rename(deserialize = "o"))]
  pub open:   f32,
  #[serde(rename(deserialize = "h"))]
  pub high:   f32,
  #[serde(rename(deserialize = "l"))]
  pub low:    f32,
  #[serde(rename(deserialize = "c"))]
  pub close:  f32,
  #[serde(rename(deserialize = "v"), deserialize_with = "f64_to_u64")]
  pub volume: u64
}

#[derive(Deserialize)]
pub struct OHLCVResponse {
  pub results: Vec<OHLCV>
}

pub fn get_results(uri: &str) -> io::Result<Vec<OHLCV>> {
  let resp = ureq::get(&uri)
    .timeout_connect(10_000)
    .timeout_read(10_000)
    .call();

  let status = resp.status();
  if status != 200 {
    return Err(Error::new(
      ErrorKind::NotConnected,
      format!("Server returned {}", status)
    ));
  }
  // Parsing JSON is slow, but the bottleneck is usually the network.
  // Can use https://github.com/simd-lite/simd-json for 10-20% boost
  let json = resp.into_json_deserialize::<OHLCVResponse>()?;
  Ok(json.results)
}
