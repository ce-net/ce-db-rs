// The caller code of every case below, on the schema it was written for: it compiles.
use ce_db::prelude::*;

#[derive(Record, Clone, Debug)]
#[record(cell = "shop", collection = "parts", version = 2)]
pub struct Part {
    #[record(key)]
    pub id: Id,
    pub name: String,
    #[record(index)]
    pub bin: String,
    #[record(index, ordered)]
    pub qty: u32,
}

#[allow(dead_code)]
fn caller(db: &ce_db::Db) -> Result<(), DbError> {
    let parts = db.collection::<Part>()?;
    let id = parts.insert(&Part { id: Id::new(), name: "M3 screw".into(), bin: "A4".into(), qty: 500 })?;
    parts.update(&id, |p| p.qty -= 1)?;
    let _in_a4: Vec<Part> = parts.by_bin("A4").fetch()?;
    let _low: Vec<Part> = parts.by_qty(..20).fetch()?;
    Ok(())
}

fn main() {}
