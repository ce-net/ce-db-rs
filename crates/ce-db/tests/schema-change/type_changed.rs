// Schema change: `qty` became a String. The v2 caller no longer compiles: arithmetic on it and the numeric range
// query are type errors.
use ce_db::prelude::*;

#[derive(Record, Clone, Debug)]
#[db(app = "shop", collection = "parts", version = 3)]
pub struct Part {
    #[db(key)]
    pub id: Id,
    pub name: String,
    #[db(index)]
    pub bin: String,
    #[db(index, ordered)]
    pub qty: String,
}

#[allow(dead_code)]
fn caller(db: &ce_db::Db) -> Result<(), DbError> {
    let parts = db.collection::<Part>()?;
    let id = parts.insert(&Part { id: Id::new(), name: "M3 screw".into(), bin: "A4".into(), qty: 500 })?;
    parts.update(&id, |p| p.qty -= 1)?;
    let _low: Vec<Part> = parts.by_qty(..20).fetch()?;
    Ok(())
}

fn main() {}
