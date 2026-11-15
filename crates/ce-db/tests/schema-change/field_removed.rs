// Schema change: `bin` is gone from Part. The v2 caller no longer compiles: the struct has no field `bin`, and the
// collection has no `by_bin` (the index went with the field).
use ce_db::prelude::*;

#[derive(Record, Clone, Debug)]
#[record(cell = "shop", collection = "parts", version = 3)]
pub struct Part {
    #[record(key)]
    pub id: Id,
    pub name: String,
    #[record(index, ordered)]
    pub qty: u32,
}

#[allow(dead_code)]
fn caller(db: &ce_db::Db) -> Result<(), DbError> {
    let parts = db.collection::<Part>()?;
    let id = parts.insert(&Part { id: Id::new(), name: "M3 screw".into(), bin: "A4".into(), qty: 500 })?;
    parts.update(&id, |p| p.qty -= 1)?;
    let _in_a4: Vec<Part> = parts.by_bin("A4").fetch()?;
    Ok(())
}

fn main() {}
