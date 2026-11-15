// `cell = LiveDta` names a type, so a typo is an unresolved name, not a store nobody owns.
use ce_db::prelude::*;

#[derive(Record, Clone, Debug)]
#[record(cell = LiveDta, collection = "stations", version = 1)]
pub struct Station {
    #[record(key)]
    pub id: String,
}

fn main() {}
