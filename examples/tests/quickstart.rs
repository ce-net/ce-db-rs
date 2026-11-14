//! The quickstart binary, run as a stranger would: a dev store, CE_DB_STORE and CE_DB_CAP, then `quickstart`.
use std::process::Command;

#[test]
fn quickstart_runs_against_a_dev_store() {
    let s = ce_db_dev::serve("127.0.0.1:0", ce_db_dev::Grants::all_of("dev", "quickstart")).unwrap();
    let run = || Command::new(env!("CARGO_BIN_EXE_quickstart")).env("CE_DB_STORE", s.addr.to_string()).env("CE_DB_CAP", "dev").output().unwrap();
    let out = run();
    let text = String::from_utf8_lossy(&out.stdout).to_string();
    assert!(out.status.success(), "{text}{}", String::from_utf8_lossy(&out.stderr));
    assert!(text.contains("open tasks: 1") && text.contains("Synced") && text.contains("Added(Task {") && text.contains("buy milk"), "{text}");
    // a second run sees the first run's task in the query, and as an Added before Synced
    let text = String::from_utf8_lossy(&run().stdout).to_string();
    assert!(text.contains("open tasks: 2"), "{text}");
    println!("{text}");
}
