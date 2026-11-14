//! The live mirror of one collection (DB-SDK-DESIGN.md 2.1, 4.1): List the collection once (tombstones included, so
//! a row deleted while the walk runs is seen deleted), then follow the Watch stream from the last seq. One stream per
//! collection per `Db`, fanned out to every watcher; index queries read the same rows, so a query and its live
//! updates never disagree. Own writes are applied at once from the store's answer (read your writes); the same seq
//! arriving again on the stream is skipped.
use crate::collection::{Change, Link};
use crate::store::{Reach, Store};
use ce_db_core::*;
use state_store_api::*;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};
use std::time::Duration;

pub(crate) struct Row<T> {
    pub seq: u64,
    pub version: u64,
    pub value: T,
    pub raw: Obj,
}

pub(crate) type Filter<T> = Arc<dyn Fn(&str, &T) -> bool + Send + Sync>;

struct Sub<T> {
    id: u64,
    filter: Filter<T>,
    tx: Sender<Result<Change<T>, DbError>>,
}

pub(crate) struct St<T> {
    pub rows: BTreeMap<String, Row<T>>,
    pub idx: HashMap<&'static str, BTreeSet<(IndexVal, String)>>,
    pub rejects: BTreeMap<String, String>,
    pub head: u64,
    pub synced: bool,
    pub link: Link,
    pub error: Option<DbError>,
    subs: Vec<Sub<T>>,
    link_subs: Vec<Sender<Link>>,
    next_id: u64,
    ep: usize,
}

pub(crate) struct Mirror<T> {
    st: Mutex<St<T>>,
    cv: Condvar,
    store: Arc<Store>,
}

const SYNC_WAIT: Duration = Duration::from_secs(10);
const BACKOFF_MIN: Duration = Duration::from_millis(200);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

fn decode_doc<T: Record>(d: &Doc) -> Result<(T, Obj), String> {
    let v: serde_json::Value = serde_json::from_str(&d.value).map_err(|e| format!("not JSON: {e}"))?;
    let serde_json::Value::Object(o) = v else { return Err("not an object".into()) };
    let t = T::decode_fields(&o).map_err(|e| e.to_string())?;
    Ok((t, o))
}

impl<T: Record> St<T> {
    /// Set the link and tell every link watcher, once per transition.
    pub fn set_link(&mut self, l: Link) {
        if self.link == l {
            return;
        }
        self.link = l.clone();
        self.link_subs.retain(|tx| tx.send(l.clone()).is_ok());
    }
    fn index_add(&mut self, key: &str, t: &T) {
        for (f, _) in T::meta().indexes() {
            self.idx.entry(f).or_default().insert((t.index_val(f), key.to_string()));
        }
    }
    fn index_del(&mut self, key: &str, t: &T) {
        for (f, _) in T::meta().indexes() {
            if let Some(s) = self.idx.get_mut(f) {
                s.remove(&(t.index_val(f), key.to_string()));
            }
        }
    }

    fn notify(&mut self, key: &str, old: Option<&T>, new: Option<&T>) {
        self.subs.retain(|s| {
            let was = old.is_some_and(|o| (s.filter)(key, o));
            let is = new.is_some_and(|n| (s.filter)(key, n));
            let ch = match (was, is, old, new) {
                (false, true, _, Some(n)) => Change::Added(n.clone()),
                (true, true, Some(o), Some(n)) => Change::Changed { old: o.clone(), new: n.clone() },
                (true, false, _, _) => Change::Removed(key.to_string()),
                _ => return true,
            };
            s.tx.send(Ok(ch)).is_ok()
        });
    }

    /// One document from the store: newer than what is held, or ignored.
    pub fn apply(&mut self, d: &Doc) {
        self.head = self.head.max(d.seq);
        let held = self.rows.get(&d.key).map(|r| r.seq);
        if held.is_some_and(|s| s >= d.seq) {
            return;
        }
        if d.deleted {
            self.rejects.remove(&d.key);
            if let Some(old) = self.rows.remove(&d.key) {
                self.index_del(&d.key, &old.value);
                self.notify(&d.key, Some(&old.value), None);
            }
            return;
        }
        match decode_doc::<T>(d) {
            Ok((t, raw)) => {
                self.rejects.remove(&d.key);
                let old = self.rows.remove(&d.key);
                if let Some(o) = &old {
                    self.index_del(&d.key, &o.value);
                }
                self.index_add(&d.key, &t);
                self.notify(&d.key, old.as_ref().map(|o| &o.value), Some(&t));
                self.rows.insert(d.key.clone(), Row { seq: d.seq, version: d.version, value: t, raw });
            }
            Err(why) => {
                // never dropped silently: counted, and its old typed value leaves the result
                if let Some(old) = self.rows.remove(&d.key) {
                    self.index_del(&d.key, &old.value);
                    self.notify(&d.key, Some(&old.value), None);
                }
                self.rejects.insert(d.key.clone(), why);
            }
        }
    }

    pub fn remove_local(&mut self, key: &str) {
        if let Some(old) = self.rows.remove(key) {
            self.index_del(key, &old.value);
            self.notify(key, Some(&old.value), None);
        }
    }
}

impl<T: Record> Mirror<T> {
    pub fn start(store: Arc<Store>) -> Arc<Mirror<T>> {
        let m = Arc::new(Mirror {
            st: Mutex::new(St { rows: BTreeMap::new(), idx: HashMap::new(), rejects: BTreeMap::new(), head: 0, synced: false, link: Link::Connecting, error: None, subs: Vec::new(), link_subs: Vec::new(), next_id: 1, ep: 0 }),
            cv: Condvar::new(),
            store,
        });
        let w = Arc::downgrade(&m);
        std::thread::Builder::new().name(format!("ce-db mirror {}", T::meta().collection)).spawn(move || run::<T>(w)).ok();
        m
    }

    pub fn lock(&self) -> Result<MutexGuard<'_, St<T>>, DbError> {
        self.st.lock().map_err(|_| DbError::Store("mirror lock poisoned".into()))
    }

    /// The rows, once the first List has been walked.
    pub fn synced(&self) -> Result<MutexGuard<'_, St<T>>, DbError> {
        let g = self.lock()?;
        let (g, to) = self.cv.wait_timeout_while(g, SYNC_WAIT, |s| !s.synced).map_err(|_| DbError::Store("mirror lock poisoned".into()))?;
        if let Some(e) = &g.error {
            return Err(e.clone());
        }
        if to.timed_out() && !g.synced {
            let why = match &g.link {
                Link::Offline(w) => w.clone(),
                l => format!("{l:?}"),
            };
            return Err(DbError::Unavailable { tried: vec![format!("the mirror of {} did not load within {} s: {why}", T::meta().collection, SYNC_WAIT.as_secs())] });
        }
        Ok(g)
    }

    /// A watcher: every row matching `filter` now (Added, in key order), then `Synced`, then each change.
    pub fn subscribe(&self, filter: Filter<T>) -> Result<(u64, Receiver<Result<Change<T>, DbError>>), DbError> {
        let mut g = self.synced()?;
        let (tx, rx) = std::sync::mpsc::channel();
        for (k, r) in g.rows.iter() {
            if filter(k, &r.value) {
                let _ = tx.send(Ok(Change::Added(r.value.clone())));
            }
        }
        let _ = tx.send(Ok(Change::Synced));
        let id = g.next_id;
        g.next_id += 1;
        g.subs.push(Sub { id, filter, tx });
        Ok((id, rx))
    }

    pub fn unsubscribe(&self, id: u64) {
        if let Ok(mut g) = self.st.lock() {
            g.subs.retain(|s| s.id != id);
        }
    }

    /// Every later link transition (Connecting, Live, Offline, Resyncing), starting with the current one.
    pub fn link_changes(&self) -> Receiver<Link> {
        let (tx, rx) = std::sync::mpsc::channel();
        if let Ok(mut g) = self.st.lock() {
            let _ = tx.send(g.link.clone());
            g.link_subs.push(tx);
        }
        rx
    }

    pub fn watchers(&self) -> usize {
        self.st.lock().map(|g| g.subs.len()).unwrap_or(0)
    }

    /// Walk the whole collection (tombstones included) and make the mirror equal to it, telling watchers the difference.
    fn resync(&self) -> Result<(), DbError> {
        let m = T::meta();
        let at = Coll { app: self.store.app.clone(), coll: m.collection.into() };
        let mut docs: BTreeMap<String, Doc> = BTreeMap::new();
        let mut since = 0;
        let mut head = 0;
        loop {
            let p = self.store.call(m.collection, false, |c, cap| {
                c.call::<List>(&ListReq { at: at.clone(), prefix: String::new(), since, limit: MAX_PAGE, include_deleted: true, cap: cap.into() })
            })?;
            for d in p.docs {
                head = head.max(d.seq);
                if d.deleted {
                    docs.remove(&d.key);
                } else {
                    docs.insert(d.key.clone(), d);
                }
            }
            if docs.len() > m.mirror_limit {
                return Err(DbError::NeedsServerIndex { rows: docs.len(), limit: m.mirror_limit });
            }
            match p.next {
                Some(n) if n > since => since = n,
                _ => break,
            }
        }
        let ep = self.store.current();
        let mut g = self.lock()?;
        let gone: Vec<String> = g.rows.keys().filter(|k| !docs.contains_key(*k)).cloned().collect();
        for k in gone {
            g.remove_local(&k);
        }
        // a different replica numbers its seqs differently: compare values, not seqs
        for d in docs.values() {
            if let Some(r) = g.rows.get(&d.key) {
                let same = serde_json::from_str::<serde_json::Value>(&d.value).ok().and_then(|v| v.as_object().cloned()).is_some_and(|o| o == r.raw);
                if same {
                    if let Some(r) = g.rows.get_mut(&d.key) {
                        r.seq = d.seq;
                        r.version = d.version;
                    }
                    continue;
                }
                if let Some(r) = g.rows.get_mut(&d.key) {
                    r.seq = 0;
                }
            }
            g.apply(d);
        }
        g.head = head;
        g.ep = ep;
        g.synced = true;
        g.error = None;
        drop(g);
        self.cv.notify_all();
        Ok(())
    }

    fn set_link(&self, l: Link) {
        if let Ok(mut g) = self.st.lock() {
            g.set_link(l);
        }
    }
}

enum End {
    Gone,
    Broke(String),
}

/// Follow the stream from the mirror's head until it breaks; Gone when every handle to the mirror is dropped.
async fn stream<T: Record>(w: &Weak<Mirror<T>>) -> End {
    let Some(m) = w.upgrade() else { return End::Gone };
    let (ep_i, since) = match m.lock() {
        Ok(g) => (g.ep, g.head),
        Err(e) => return End::Broke(e.to_string()),
    };
    let Some(ep) = m.store.eps.get(ep_i).cloned() else { return End::Broke("no replica".into()) };
    let at = Coll { app: m.store.app.clone(), coll: T::meta().collection.into() };
    drop(m);
    let token = match (ep.token)() {
        Ok(t) => t,
        Err(e) => return End::Broke(e),
    };
    let c = match &ep.reach {
        Reach::Addr(a) => ce_wire::Client::<StateStore>::connect(a.as_str(), token).await,
        Reach::ByName => ce_wire::Client::<StateStore>::by_name(token).await,
    };
    let c = match c {
        Ok(c) => c,
        Err(e) => return End::Broke(format!("{}: {e}", ep.label())),
    };
    let mut sub = match c.subscribe::<Watch>(&WatchReq { at, since, cap: ep.cap.clone() }) {
        Ok(s) => s,
        Err(e) => return End::Broke(format!("{e}")),
    };
    let mut live = false;
    loop {
        match tokio::time::timeout(Duration::from_secs(2), sub.next()).await {
            Err(_) => {
                let Some(m) = w.upgrade() else { return End::Gone };
                if !live {
                    live = true;
                    m.set_link(Link::Live);
                }
            }
            Ok(None) => return End::Broke("the store ended the stream".into()),
            Ok(Some(Err(e))) => return End::Broke(format!("{e}")),
            Ok(Some(Ok(d))) => {
                let Some(m) = w.upgrade() else { return End::Gone };
                let g = m.lock();
                if let Ok(mut g) = g {
                    g.apply(&d);
                    if !live {
                        live = true;
                        g.set_link(Link::Live);
                    }
                }
            }
        }
    }
}

fn run<T: Record>(w: Weak<Mirror<T>>) {
    let Ok(rt) = tokio::runtime::Builder::new_current_thread().enable_all().build() else {
        if let Some(m) = w.upgrade() {
            m.set_link(Link::Offline("no async runtime for the watch stream".into()));
        }
        return;
    };
    let mut backoff = BACKOFF_MIN;
    let mut need_list = true;
    loop {
        let Some(m) = w.upgrade() else { return };
        let moved = match m.lock() {
            Ok(g) => g.ep != m.store.current(),
            Err(_) => true,
        };
        if need_list || moved {
            match m.resync() {
                Ok(()) => need_list = false,
                Err(e @ DbError::NeedsServerIndex { .. }) => {
                    if let Ok(mut g) = m.lock() {
                        g.error = Some(e.clone());
                        g.synced = true;
                        g.set_link(Link::Offline(e.to_string()));
                    }
                    m.cv.notify_all();
                    return;
                }
                Err(e) => {
                    m.set_link(Link::Offline(e.to_string()));
                    drop(m);
                    std::thread::sleep(backoff);
                    backoff = (backoff * 2).min(BACKOFF_MAX);
                    if let Some(m) = w.upgrade() {
                        if m.lock().is_ok_and(|g| g.synced) {
                            m.set_link(Link::Resyncing);
                        }
                    }
                    continue;
                }
            }
        }
        drop(m);
        let t0 = std::time::Instant::now();
        match rt.block_on(stream::<T>(&w)) {
            End::Gone => return,
            End::Broke(why) => {
                let Some(m) = w.upgrade() else { return };
                m.set_link(Link::Offline(why));
                // List again before following the stream: the store may have restarted, lost or renumbered rows,
                // or be another replica. The walk emits only the difference from the mirror (no duplicate, no gap).
                need_list = true;
                if t0.elapsed() > Duration::from_secs(10) {
                    backoff = BACKOFF_MIN;
                }
                drop(m);
                std::thread::sleep(backoff);
                backoff = (backoff * 2).min(BACKOFF_MAX);
                if let Some(m) = w.upgrade() {
                    m.set_link(Link::Resyncing);
                }
            }
        }
    }
}
