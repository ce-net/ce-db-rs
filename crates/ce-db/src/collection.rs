//! `Collection<T>`: the six verbs on one typed collection, and `Query<T>`.
use crate::mirror::{Filter, Mirror};
use crate::Db;
use ce_db_core::*;
use state_store_api::*;
use std::marker::PhantomData;
use std::ops::Bound;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::Arc;
use std::time::Duration;

/// One event of a watch. `Synced` follows the rows that matched when the watch began.
#[derive(Clone, Debug, PartialEq)]
pub enum Change<T> {
    Added(T),
    Changed { old: T, new: T },
    Removed(String),
    Synced,
}

/// The state of a collection's live link, for a UI to show.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Link {
    Connecting,
    Live,
    Resyncing,
    Offline(String),
}

/// A row with what the store keeps beside the typed value.
#[derive(Clone, Debug, PartialEq)]
pub struct Stored<T> {
    pub key: String,
    pub value: T,
    pub version: u64,
    pub seq: u64,
    /// Fields this program does not know, written back untouched by `replace`.
    pub extra: Obj,
    /// The schema version of the row's writer (`_v`).
    pub writer_version: u32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Stats {
    /// Rows in the mirror (0 until an index query or watch opened it).
    pub rows: usize,
    /// Rows that do not decode into the type, with why.
    pub rejects: Vec<(String, String)>,
    pub head_seq: u64,
    pub watchers: usize,
    pub link: Link,
}

/// A typed collection. Cheap to clone.
pub struct Collection<T: Record> {
    db: Db,
    _t: PhantomData<fn() -> T>,
}

impl<T: Record> Clone for Collection<T> {
    fn clone(&self) -> Self {
        Collection { db: self.db.clone(), _t: PhantomData }
    }
}

fn at<T: Record>(db: &Db) -> Coll {
    Coll { app: db.inner.app.clone(), coll: T::meta().collection.into() }
}

fn stored<T: Record>(d: &Doc) -> Result<Stored<T>, DbError> {
    let dec = decode::<T>(&d.value)?;
    Ok(Stored { key: d.key.clone(), value: dec.value, version: d.version, seq: d.seq, extra: dec.extra, writer_version: dec.writer_version })
}

impl<T: Record> Collection<T> {
    pub(crate) fn new(db: Db) -> Self {
        Collection { db, _t: PhantomData }
    }

    fn coll(&self) -> &'static str {
        T::meta().collection
    }

    fn call<R>(&self, write: bool, f: impl Fn(&ce_wire::blocking::Client<StateStore>, &str) -> Result<R, ce_wire::WireError>) -> Result<R, DbError> {
        self.db.inner.store.call(self.coll(), write, f)
    }

    /// Our own write, into the mirror at once (if one is open).
    fn mirrored(&self, d: &Doc) {
        let open = self.db.inner.mirrors.lock().ok().is_some_and(|g| g.contains_key(self.coll()));
        if open {
            if let Ok(m) = self.db.mirror::<T>() {
                if let Ok(mut g) = m.lock() {
                    g.apply(d);
                }
            }
        }
    }

    /// Create the row; `Conflict` when the key exists. Returns the key.
    pub fn insert(&self, t: &T) -> Result<String, DbError> {
        let value = encode(t)?;
        let key = t.key();
        let d = self.call(true, |c, cap| c.call::<Put>(&PutReq { at: at::<T>(&self.db), key: key.clone(), value: value.clone(), if_version: Some(0), insert_only: false, cap: cap.into() }))?;
        self.mirrored(&d);
        Ok(key)
    }

    pub fn get(&self, key: impl AsRef<str>) -> Result<Option<T>, DbError> {
        Ok(self.get_stored(key)?.map(|s| s.value))
    }

    /// The row with its version, unknown fields and writer version.
    pub fn get_stored(&self, key: impl AsRef<str>) -> Result<Option<Stored<T>>, DbError> {
        let key = key.as_ref().to_string();
        let d = self.call(false, |c, cap| c.call::<Get>(&GetReq { at: at::<T>(&self.db), key: key.clone(), cap: cap.into() }))?;
        d.map(|d| stored::<T>(&d)).transpose()
    }

    /// Change fields of a row: `f` edits the current value, and only the fields it changed go to the store (Patch, op
    /// 7), so a field this program does not know is never erased. `NotFound` when there is no such row.
    pub fn update(&self, key: impl AsRef<str>, f: impl FnOnce(&mut T)) -> Result<T, DbError> {
        let key = key.as_ref().to_string();
        let cur = self.get_stored(&key)?.ok_or_else(|| DbError::NotFound { key: key.clone() })?;
        let mut next = cur.value.clone();
        f(&mut next);
        if next.key() != key {
            return Err(DbError::Invalid { field: T::meta().key_field().name.into(), why: "update cannot change the key".into() });
        }
        // the whole row after the patch must still fit
        encode_with(&next, &cur.extra)?;
        let mut fields = diff(&cur.value.encode_fields(), &next.encode_fields());
        if fields.is_empty() {
            return Ok(next);
        }
        fields.insert("_v".into(), serde_json::Value::from(T::meta().version));
        let text = canonical(&serde_json::Value::Object(fields));
        let d = self.call(true, |c, cap| c.call::<Patch>(&PatchReq { at: at::<T>(&self.db), key: key.clone(), fields: text.clone(), cap: cap.into() }))?;
        self.mirrored(&d);
        Ok(stored::<T>(&d)?.value)
    }

    /// Write the whole value (unknown fields of the stored row are lost; prefer `update`). Returns the new version.
    pub fn replace(&self, t: &T) -> Result<u64, DbError> {
        self.put(t, &Obj::new(), None)
    }

    /// Compare-and-set: write `s.value` (with its unknown fields) only over version `s.version`; else `Conflict`.
    pub fn replace_stored(&self, s: &Stored<T>) -> Result<u64, DbError> {
        self.put(&s.value, &s.extra, Some(s.version))
    }

    fn put(&self, t: &T, extra: &Obj, if_version: Option<u64>) -> Result<u64, DbError> {
        let value = encode_with(t, extra)?;
        let key = t.key();
        let d = self.call(true, |c, cap| c.call::<Put>(&PutReq { at: at::<T>(&self.db), key: key.clone(), value: value.clone(), if_version, insert_only: false, cap: cap.into() }))?;
        self.mirrored(&d);
        Ok(d.version)
    }

    pub fn remove(&self, key: impl AsRef<str>) -> Result<(), DbError> {
        let key = key.as_ref().to_string();
        self.call(true, |c, cap| c.call::<Delete>(&DeleteReq { at: at::<T>(&self.db), key: key.clone(), cap: cap.into() }))?;
        if self.db.inner.mirrors.lock().ok().is_some_and(|g| g.contains_key(self.coll())) {
            if let Ok(m) = self.db.mirror::<T>() {
                if let Ok(mut g) = m.lock() {
                    g.remove_local(&key);
                }
            }
        }
        Ok(())
    }

    /// Rows whose key starts with `p` (op 4 List).
    pub fn prefix(&self, p: impl Into<String>) -> Query<T> {
        Query::new(self.clone(), Kind::Prefix(p.into()))
    }

    /// Every row (op 4 List pages).
    pub fn all(&self) -> Query<T> {
        Query::new(self.clone(), Kind::Prefix(String::new()))
    }

    /// Every change to the collection.
    pub fn watch(&self) -> Result<Watch<T>, DbError> {
        self.all().watch()
    }

    /// Every change to one row.
    pub fn watch_key(&self, key: impl AsRef<str>) -> Result<Watch<T>, DbError> {
        let k = key.as_ref().to_string();
        Watch::open(self.db.mirror::<T>()?, Arc::new(move |key: &str, _: &T| key == k))
    }

    pub fn stats(&self) -> Stats {
        let open = self.db.inner.mirrors.lock().ok().is_some_and(|g| g.contains_key(self.coll()));
        if !open {
            return Stats { rows: 0, rejects: vec![], head_seq: 0, watchers: 0, link: Link::Connecting };
        }
        match self.db.mirror::<T>() {
            Ok(m) => {
                let w = m.watchers();
                match m.lock() {
                    Ok(g) => Stats { rows: g.rows.len(), rejects: g.rejects.iter().map(|(k, v)| (k.clone(), v.clone())).collect(), head_seq: g.head, watchers: w, link: g.link.clone() },
                    Err(_) => Stats { rows: 0, rejects: vec![], head_seq: 0, watchers: w, link: Link::Offline("poisoned".into()) },
                }
            }
            Err(e) => Stats { rows: 0, rejects: vec![], head_seq: 0, watchers: 0, link: Link::Offline(e.to_string()) },
        }
    }

    /// The live link of this collection's mirror.
    pub fn link(&self) -> Link {
        self.stats().link
    }
}

impl<T: Record> IndexQuery<T> for Collection<T> {
    type Q = Query<T>;
    fn index_eq(&self, index: &'static str, v: IndexVal) -> Query<T> {
        Query::new(self.clone(), Kind::Eq(index, v))
    }
    fn index_range(&self, index: &'static str, lo: Bound<IndexVal>, hi: Bound<IndexVal>) -> Query<T> {
        Query::new(self.clone(), Kind::Range(index, lo, hi))
    }
}

#[derive(Clone, Debug)]
enum Kind {
    Prefix(String),
    Eq(&'static str, IndexVal),
    Range(&'static str, Bound<IndexVal>, Bound<IndexVal>),
}

/// One index or one prefix (no ad-hoc filters: a query's cost is visible in the schema).
pub struct Query<T: Record> {
    c: Collection<T>,
    kind: Kind,
    limit: Option<usize>,
    after: Option<String>,
    desc: bool,
}

/// A page size when `fetch_page` is called without `limit`.
pub const PAGE: usize = 500;

fn in_range(v: &IndexVal, lo: &Bound<IndexVal>, hi: &Bound<IndexVal>) -> bool {
    let lo_ok = match lo {
        Bound::Included(l) => v >= l,
        Bound::Excluded(l) => v > l,
        Bound::Unbounded => true,
    };
    let hi_ok = match hi {
        Bound::Included(h) => v <= h,
        Bound::Excluded(h) => v < h,
        Bound::Unbounded => true,
    };
    lo_ok && hi_ok && *v != IndexVal::Null
}

fn hex(s: &str) -> String {
    s.bytes().map(|b| format!("{b:02x}")).collect()
}

fn unhex(s: &str) -> Option<String> {
    if s.len() % 2 != 0 {
        return None;
    }
    let b: Option<Vec<u8>> = (0..s.len()).step_by(2).map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok()).collect();
    String::from_utf8(b?).ok()
}

impl<T: Record> Query<T> {
    fn new(c: Collection<T>, kind: Kind) -> Self {
        Query { c, kind, limit: None, after: None, desc: false }
    }
    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }
    /// Continue after a cursor from `fetch_page`.
    pub fn after(mut self, cursor: impl Into<String>) -> Self {
        self.after = Some(cursor.into());
        self
    }
    /// Descending: by the index value (then key) for an index query, by key for a prefix.
    pub fn desc(mut self) -> Self {
        self.desc = true;
        self
    }

    fn filter(&self) -> Filter<T> {
        match self.kind.clone() {
            Kind::Prefix(p) => Arc::new(move |k: &str, _: &T| k.starts_with(&p)),
            Kind::Eq(f, v) => Arc::new(move |_: &str, t: &T| t.index_val(f) == v),
            Kind::Range(f, lo, hi) => Arc::new(move |_: &str, t: &T| in_range(&t.index_val(f), &lo, &hi)),
        }
    }

    /// Served from the store (op 4) or from the mirror.
    fn server_side(&self) -> bool {
        matches!(self.kind, Kind::Prefix(_)) && !self.desc
    }

    /// One page and the cursor of the next (None at the end).
    pub fn fetch_page(&self) -> Result<(Vec<T>, Option<String>), DbError> {
        let (rows, _, next) = self.page(self.limit.unwrap_or(PAGE), self.after.clone())?;
        Ok((rows, next))
    }

    /// Every matching row up to `limit` (all of them without one).
    pub fn fetch(&self) -> Result<Vec<T>, DbError> {
        Ok(self.fetch_with_rejects()?.0)
    }

    /// `fetch`, and the rows that matched but did not decode into `T` (key, why): never dropped silently.
    pub fn fetch_with_rejects(&self) -> Result<(Vec<T>, Vec<(String, String)>), DbError> {
        let want = self.limit.unwrap_or(usize::MAX);
        let mut out = Vec::new();
        let mut rejects = Vec::new();
        let mut after = self.after.clone();
        loop {
            let (rows, rej, next) = self.page((want - out.len()).min(PAGE), after)?;
            out.extend(rows);
            rejects.extend(rej);
            match next {
                Some(n) if out.len() < want => after = Some(n),
                _ => return Ok((out, rejects)),
            }
        }
    }

    pub fn count(&self) -> Result<usize, DbError> {
        if self.server_side() {
            return Ok(self.fetch()?.len());
        }
        let m = self.c.db.mirror::<T>()?;
        let g = m.synced()?;
        let f = self.filter();
        Ok(g.rows.iter().filter(|(k, r)| f(k, &r.value)).count())
    }

    /// Rows entering, changing inside and leaving the result, after the rows in it now and `Synced`.
    pub fn watch(&self) -> Result<Watch<T>, DbError> {
        Watch::open(self.c.db.mirror::<T>()?, self.filter())
    }

    fn page(&self, n: usize, after: Option<String>) -> Result<(Vec<T>, Vec<(String, String)>, Option<String>), DbError> {
        let n = n.max(1);
        if self.server_side() {
            let Kind::Prefix(p) = &self.kind else { unreachable!("server_side is a prefix") };
            let since = match after.as_deref() {
                None => 0,
                Some(c) => c.strip_prefix('s').and_then(|s| s.parse().ok()).ok_or_else(|| DbError::Invalid { field: "cursor".into(), why: "not a cursor of this query".into() })?,
            };
            let limit = n.min(MAX_PAGE as usize) as u32;
            let page = self.c.call(false, |c, cap| c.call::<List>(&ListReq { at: at::<T>(&self.c.db), prefix: p.clone(), since, limit, include_deleted: false, cap: cap.into() }))?;
            let mut rows = Vec::new();
            let mut rej = Vec::new();
            for d in &page.docs {
                match stored::<T>(d) {
                    Ok(s) => rows.push(s.value),
                    Err(e) => rej.push((d.key.clone(), e.to_string())),
                }
            }
            return Ok((rows, rej, page.next.map(|s| format!("s{s}"))));
        }
        let m = self.c.db.mirror::<T>()?;
        let g = m.synced()?;
        // (index value, key) order for an index, key order for a prefix
        let mut hits: Vec<(IndexVal, &String, &T)> = match &self.kind {
            Kind::Prefix(p) => g.rows.range(p.clone()..).take_while(|(k, _)| k.starts_with(p.as_str())).map(|(k, r)| (IndexVal::Null, k, &r.value)).collect(),
            Kind::Eq(f, v) => g.idx.get(f).into_iter().flatten().filter(|(iv, _)| iv == v).filter_map(|(iv, k)| g.rows.get(k).map(|r| (iv.clone(), k, &r.value))).collect(),
            Kind::Range(f, lo, hi) => g.idx.get(f).into_iter().flatten().filter(|(iv, _)| in_range(iv, lo, hi)).filter_map(|(iv, k)| g.rows.get(k).map(|r| (iv.clone(), k, &r.value))).collect(),
        };
        if self.desc {
            hits.reverse();
        }
        let start = match after.as_deref() {
            None => 0,
            Some(c) => {
                let text = c.strip_prefix('m').and_then(unhex).ok_or_else(|| DbError::Invalid { field: "cursor".into(), why: "not a cursor of this query".into() })?;
                let v: serde_json::Value = serde_json::from_str(&text).map_err(|_| DbError::Invalid { field: "cursor".into(), why: "not a cursor of this query".into() })?;
                let iv = v.get(0).and_then(IndexVal::from_json).unwrap_or(IndexVal::Null);
                let k = v.get(1).and_then(|k| k.as_str()).unwrap_or("").to_string();
                let pos = (iv, k);
                hits.iter().position(|(hv, hk, _)| {
                    let here = (hv.clone(), (*hk).clone());
                    if self.desc { here < pos } else { here > pos }
                }).unwrap_or(hits.len())
            }
        };
        let page: Vec<&(IndexVal, &String, &T)> = hits[start..].iter().take(n).collect();
        let next = if start + page.len() < hits.len() {
            page.last().map(|(v, k, _)| format!("m{}", hex(&canonical(&serde_json::json!([v.to_json(), k])))))
        } else {
            None
        };
        let rej = g.rejects.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        Ok((page.iter().map(|(_, _, t)| (*t).clone()).collect(), rej, next))
    }
}

/// A live watch: an iterator of changes. Dropping it stops the watch; the shared stream stays for other watchers.
pub struct Watch<T: Record> {
    m: Arc<Mirror<T>>,
    id: u64,
    rx: Receiver<Result<Change<T>, DbError>>,
}

impl<T: Record> Watch<T> {
    fn open(m: Arc<Mirror<T>>, f: Filter<T>) -> Result<Watch<T>, DbError> {
        let (id, rx) = m.subscribe(f)?;
        Ok(Watch { m, id, rx })
    }
    /// The next change within `d`, None when none came.
    pub fn next_timeout(&mut self, d: Duration) -> Option<Result<Change<T>, DbError>> {
        match self.rx.recv_timeout(d) {
            Ok(c) => Some(c),
            Err(RecvTimeoutError::Timeout) => None,
            Err(RecvTimeoutError::Disconnected) => Some(Err(DbError::Unavailable { tried: vec!["the watch ended".into()] })),
        }
    }
    pub fn link(&self) -> Link {
        self.m.lock().map(|g| g.link.clone()).unwrap_or(Link::Offline("poisoned".into()))
    }
    /// The link's transitions from now on (the current state first): `Live`, then `Offline(why)` when the store goes
    /// away, `Resyncing` while it lists again after reconnecting, `Live` once the stream follows again. For a UI badge.
    pub fn link_changes(&self) -> std::sync::mpsc::Receiver<Link> {
        self.m.link_changes()
    }
}

impl<T: Record> Iterator for Watch<T> {
    type Item = Result<Change<T>, DbError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.rx.recv().ok()
    }
}

impl<T: Record> Drop for Watch<T> {
    fn drop(&mut self) {
        self.m.unsubscribe(self.id);
    }
}
