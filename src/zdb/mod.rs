pub mod agg1d;
pub mod agg1m;

use chrono::{Duration, NaiveDate};
use zdb::calendar::us_equity::is_market_open;

pub struct MarketDays {
  from: NaiveDate,
  to:   NaiveDate
}

impl Iterator for MarketDays {
  type Item = NaiveDate;

  fn next(&mut self) -> Option<NaiveDate> {
    self.from += Duration::days(1);
    while !is_market_open(&self.from) {
      self.from += Duration::days(1);
    }
    if self.from < self.to {
      return Some(self.from);
    }

    None
  }
}
