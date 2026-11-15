// `app` is the deprecated word: under deny(deprecated) the warning is an error, at the `app` key.
#![deny(deprecated)]
use ce_db::prelude::*;

#[derive(Record, Clone, Debug)]
#[db(app = "shop", collection = "parts", version = 1)]
pub struct Part {
    #[db(key)]
    pub id: String,
}

fn main() {}
