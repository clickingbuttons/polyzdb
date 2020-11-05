use std::{env, fs, io};

pub struct Config {
  pub polygon_key: String
}

impl Config {
  pub fn open(path: &str) -> io::Result<Config> {
    match env::var("POLYGON_KEY") {
      Ok(polygon_key) => Ok(Config { polygon_key }),
      Err(_) => Ok(Config {
        polygon_key: fs::read_to_string(path)?
      })
    }
  }
}
