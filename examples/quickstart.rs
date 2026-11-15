//! Define a schema, insert, query and watch. Run against a dev store:
//!   cargo run -p ce-db-dev --bin ce-db-dev-store -- --grant dev='state.quickstart.*' &
//!   CE_DB_STORE=127.0.0.1:7811 CE_DB_CAP=dev cargo run -p ce-db-examples --bin quickstart
//! Without CE_DB_STORE it opens this machine's state.store with the wallet grant for app `quickstart`.
use ce_db::prelude::*;

#[derive(Record, Clone, Debug)]
#[record(cell = "quickstart", collection = "tasks", version = 1)]
pub struct Task { #[record(key)] pub id: Id, pub title: String, #[record(index)] pub done: bool }

fn main() -> Result<(), DbError> {
    let tasks = ce_db::open("quickstart")?.collection::<Task>()?;   // registers the schema when it is missing
    let mut live = tasks.by_done(false).watch()?;                    // every open task now, Synced, then changes
    tasks.insert(&Task { id: Id::new(), title: "buy milk".into(), done: false })?;
    println!("open tasks: {}", tasks.by_done(false).count()?);
    while let Some(change) = live.next_timeout(std::time::Duration::from_millis(500)) {
        println!("{:?}", change?);                                    // Added(..) | Changed{..} | Removed(key) | Synced
    }
    Ok(())
}
