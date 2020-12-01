use chrono::{Duration, NaiveDate};
use zdb::calendar::us_equity::is_market_open;

pub struct MarketDays {
  pub from: NaiveDate,
  pub to:   NaiveDate
}

impl Iterator for MarketDays {
  type Item = NaiveDate;

  fn next(&mut self) -> Option<NaiveDate> {
    while !is_market_open(&self.from) {
      self.from += Duration::days(1);
    }
    let res = self.from.clone();
    if res < self.to {
      self.from += Duration::days(1);
      return Some(res);
    }

    None
  }
}
