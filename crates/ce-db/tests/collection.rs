//! The Rust surface against a dev store over a real ce-wire connection.
use ce_db::prelude::*;
use ce_db::{Change, Link};
use std::time::Duration;

/// A part in a bin.
#[derive(Record, Clone, Debug, PartialEq)]
#[db(app = "t-parts", collection = "parts", version = 2)]
pub struct Part {
    #[db(key)]
    pub id: Id,
    #[db(max = 120)]
    pub name: String,
    #[db(index)]
    pub bin: String,
    #[db(index, ordered)]
    pub qty: u32,
    #[db(since = 2)]
    pub supplier: Option<String>,
    pub serial: u64,
}

fn part(name: &str, bin: &str, qty: u32) -> Part {
    Part { id: Id::new(), name: name.into(), bin: bin.into(), qty, supplier: None, serial: u64::MAX }
}

fn db() -> (ce_db_dev::DevStore, ce_db::Db) {
    let s = ce_db_dev::serve("127.0.0.1:0", ce_db_dev::Grants::all_of("rw", "t-parts").with("ro", &["state.tparts.parts.read"])).unwrap();
    let db = ce_db::open_at("t-parts", &s.addr.to_string(), "rw").unwrap();
    (s, db)
}

#[test]
fn crud_index_and_errors() {
    let (s, db) = db();
    let parts = db.collection::<Part>().unwrap();
    let a = part("M3x8 screw", "A4", 500);
    let id = parts.insert(&a).unwrap();
    assert_eq!(parts.get(&id).unwrap(), Some(a.clone()));
    assert_eq!(parts.insert(&a).unwrap_err(), DbError::Conflict { held_version: 1 });
    parts.insert(&part("M4 nut", "A4", 12)).unwrap();
    parts.insert(&part("washer", "B1", 3)).unwrap();
    let in_a4 = parts.by_bin("A4").fetch().unwrap();
    assert_eq!(in_a4.len(), 2);
    let low: Vec<String> = parts.by_qty(..20).fetch().unwrap().into_iter().map(|p| p.name).collect();
    assert_eq!(low, vec!["washer", "M4 nut"]);
    let desc: Vec<u32> = parts.by_qty(..).desc().fetch().unwrap().into_iter().map(|p| p.qty).collect();
    assert_eq!(desc, vec![500, 12, 3]);
    let after = parts.update(&id, |p| p.qty -= 1).unwrap();
    assert_eq!(after.qty, 499);
    assert_eq!(parts.by_qty(499..=499).count().unwrap(), 1);
    assert_eq!(parts.update("nope", |p| p.qty = 0).unwrap_err(), DbError::NotFound { key: "nope".into() });
    parts.remove(&id).unwrap();
    assert_eq!(parts.get(&id).unwrap(), None);
    assert_eq!(parts.by_bin("A4").count().unwrap(), 1);
    // the stored row: u64 as a decimal string, None absent, _v, keys sorted
    let row = s.rows("t-parts", "parts").into_iter().find(|d| d.value.contains("washer")).unwrap();
    assert!(row.value.contains("\"serial\":\"18446744073709551615\"") && row.value.contains("\"_v\":2") && !row.value.contains("supplier"), "{}", row.value);
    // too large: refused before any call, nothing written
    let big = Part { name: "x".repeat(100), ..part("", "Z", 1) };
    let mut big2 = big.clone();
    big2.name = "y".repeat(121);
    assert!(matches!(parts.insert(&big2).unwrap_err(), DbError::Invalid { field, .. } if field == "name"));
    // read-only grant: the write is refused with the right it needs
    let ro = ce_db::open_at("t-parts", &s.addr.to_string(), "ro").unwrap().collection::<Part>().unwrap();
    assert_eq!(ro.insert(&part("p", "A", 1)).unwrap_err(), DbError::Refused { need: "state.tparts.parts.write".into() });
    let _ = big;
}

#[test]
fn schema_missing_is_typed_and_fast() {
    let s = ce_db_dev::serve("127.0.0.1:0", ce_db_dev::Grants::default().with("ro", &["state.tparts.parts.read"])).unwrap();
    let t = std::time::Instant::now();
    let e = ce_db::open_at("t-parts", &s.addr.to_string(), "ro").unwrap().collection::<Part>().err().unwrap();
    assert!(matches!(e, DbError::SchemaMissing { .. } | DbError::Refused { .. }), "{e:?}");
    assert!(t.elapsed() < Duration::from_secs(2));
    // nothing listening: Unavailable within the 2 s deadline, naming what was tried
    let free = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap();
    let t = std::time::Instant::now();
    let e = ce_db::open_at("t-parts", &free.to_string(), "rw").unwrap().collection::<Part>().err().unwrap();
    assert!(matches!(&e, DbError::Unavailable { tried } if !tried.is_empty()), "{e:?}");
    assert!(t.elapsed() < Duration::from_secs(3), "{:?}", t.elapsed());
}

#[test]
fn a_query_watch_sees_rows_enter_change_and_leave() {
    let (s, db) = db();
    let parts = db.collection::<Part>().unwrap();
    let mut w = parts.by_bin("A4").watch().unwrap();
    assert_eq!(w.next_timeout(Duration::from_secs(2)), Some(Ok(Change::Synced)));
    // a second writer (its own Db and connection), so the change comes through the store's Watch stream
    let other = ce_db::open_at("t-parts", &s.addr.to_string(), "rw").unwrap().collection::<Part>().unwrap();
    let a = part("bolt", "A4", 5);
    let t = std::time::Instant::now();
    other.insert(&a).unwrap();
    assert_eq!(w.next_timeout(Duration::from_secs(3)), Some(Ok(Change::Added(a.clone()))));
    assert!(t.elapsed() < Duration::from_secs(1), "live within a second: {:?}", t.elapsed());
    other.update(a.id.as_str(), |p| p.qty = 6).unwrap();
    match w.next_timeout(Duration::from_secs(3)) {
        Some(Ok(Change::Changed { old, new })) => assert_eq!((old.qty, new.qty), (5, 6)),
        o => panic!("{o:?}"),
    }
    other.update(a.id.as_str(), |p| p.bin = "B2".into()).unwrap();
    assert_eq!(w.next_timeout(Duration::from_secs(3)), Some(Ok(Change::Removed(a.id.to_string()))));
    other.insert(&part("other bin", "C", 1)).unwrap();
    assert_eq!(w.next_timeout(Duration::from_millis(500)), None, "a row outside the query is not an event");
    assert_eq!(w.link(), Link::Live);
}

#[test]
fn update_keeps_fields_this_program_does_not_know() {
    let (s, db) = db();
    let parts = db.collection::<Part>().unwrap();
    let a = part("gear", "A4", 2);
    parts.insert(&a).unwrap();
    // a newer writer added `colour`
    let c = ce_wire::blocking::Client::<state_store_api::StateStore>::connect(s.addr, vec![]).unwrap();
    let mut v: serde_json::Value = serde_json::from_str(&s.rows("t-parts", "parts")[0].value).unwrap();
    v["colour"] = "red".into();
    c.call::<state_store_api::Put>(&state_store_api::PutReq { at: state_store_api::Coll { app: "t-parts".into(), coll: "parts".into() }, key: a.id.to_string(), value: v.to_string(), if_version: None, insert_only: false, cap: "rw".into() }).unwrap();
    parts.update(a.id.as_str(), |p| p.qty = 9).unwrap();
    let stored = parts.get_stored(&a.id).unwrap().unwrap();
    assert_eq!(stored.value.qty, 9);
    assert_eq!(stored.extra.get("colour").and_then(|c| c.as_str()), Some("red"));
    assert_eq!(Part::meta().version, 2);
}

#[test]
fn pages_walk_without_duplicate_or_gap() {
    let (_s, db) = db();
    let parts = db.collection::<Part>().unwrap();
    for i in 0..1201 {
        parts.insert(&part(&format!("p{i}"), if i % 2 == 0 { "E" } else { "O" }, (i % 7) as u32)).unwrap();
    }
    let mut seen = std::collections::HashSet::new();
    let mut pages = 0;
    let mut after: Option<String> = None;
    loop {
        let q = parts.all();
        let q = match &after { Some(a) => q.after(a.clone()), None => q };
        let (rows, next) = q.fetch_page().unwrap();
        pages += 1;
        for r in rows {
            assert!(seen.insert(r.id.clone()), "duplicate");
        }
        match next {
            Some(n) => after = Some(n),
            None => break,
        }
    }
    assert_eq!((seen.len(), pages), (1201, 3));
    // an ordered index paged by 100 with equal values across page boundaries
    let mut got = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let q = parts.by_qty(..).limit(100);
        let q = match &after { Some(a) => q.after(a.clone()), None => q };
        let (rows, next) = q.fetch_page().unwrap();
        got.extend(rows.into_iter().map(|p| (p.qty, p.id)));
        match next {
            Some(n) => after = Some(n),
            None => break,
        }
    }
    assert_eq!(got.len(), 1201);
    let mut sorted = got.clone();
    sorted.sort();
    assert_eq!(got, sorted);
}
