//! ce-db for Rust: typed collections on state.store (repo:state-store-cell/docs/DB-SDK-DESIGN.md, slice 1).
//!
//! ```ignore
//! use parts_schema::{Part, PartIndexes};
//! let db = ce_db::open("parts-demo")?;                 // finds state.store, loads the grant
//! let parts = db.collection::<Part>()?;                // checks (or registers) the schema
//! let id = parts.insert(&Part::new("M3x8 screw", "A4", 500))?;
//! let one: Option<Part> = parts.get(&id)?;
//! let in_a4: Vec<Part> = parts.by_bin("A4").limit(50).fetch()?;
//! let low: Vec<Part> = parts.by_qty(..20).fetch()?;
//! for change in parts.by_bin("A4").watch()? { println!("{:?}", change?); }
//! ```
//!
//! Slice 1 runs on today's store with no server change: get by key is op 1, prefix and `all()` walk op 4 List, and
//! index queries and `watch` are served from one live in-memory mirror per collection (List once, then the Watch
//! stream), bounded by `mirror_limit` rows (default 5,000; above it index queries answer `NeedsServerIndex`).
pub use ce_db_core::*;

mod collection;
mod mirror;
mod store;

pub use collection::{Change, Collection, Link, Query, Stats, Stored, Watch};
pub use store::{Endpoint, Reach, Token};

use state_store_api::{check_name, kv, Schema, SchemaPut, SchemaPutReq, SchemaReq, StateStore};
use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// One app's view of state.store. Cheap to clone; collections opened from it share its connection.
#[derive(Clone)]
pub struct Db {
    pub(crate) inner: Arc<DbInner>,
}

pub(crate) struct DbInner {
    pub app: String,
    pub store: Arc<store::Store>,
    pub mirrors: Mutex<HashMap<String, Arc<dyn Any + Send + Sync>>>,
}

/// Open `app` on state.store.
///
/// `CE_DB_STORE=<host:port>` (and `CE_DB_CAP`): that store, with an empty Hello, which is how a dev store
/// (`ce-db-dev-store`) is used. Otherwise this machine's replica by name, then every replica net.find and
/// `~/.config/ce-state/replicas` name, each with the grant the wallet holds for it (`state.store:<app>@<node16>`);
/// the cap of the local one also from `STATE_STORE_CAP` or `~/.config/ce-state/<app>.cap`. No cap string in app code.
pub fn open(app: &str) -> Result<Db, DbError> {
    check_name("app", app).map_err(|why| DbError::Invalid { field: "app".into(), why })?;
    if let Ok(addr) = std::env::var("CE_DB_STORE") {
        let cap = std::env::var("CE_DB_CAP").or_else(|_| std::env::var("STATE_STORE_CAP")).unwrap_or_default();
        return open_at(app, addr.trim(), cap.trim());
    }
    let mut eps = Vec::new();
    let token: Token = Arc::new(|| ce_gate::self_proof::<StateStore>(3600).map_err(|e| e.to_string()));
    let f = format!("{}/.config/ce-state/{app}.cap", std::env::var("HOME").unwrap_or_default());
    let local_cap = std::env::var("STATE_STORE_CAP")
        .ok()
        .or_else(|| std::fs::read_to_string(&f).ok())
        .map(|c| c.trim().to_string())
        .filter(|c| !c.is_empty())
        .or_else(|| this_node16().and_then(|n| kv::wallet_cap(app, &n)));
    if let Some(cap) = local_cap {
        eps.push(Endpoint { reach: Reach::ByName, cap, token: token.clone() });
    }
    for (addr, cap, hello) in kv::found_replicas(app) {
        let t: Token = match hello {
            Some(h) => Arc::new(move || replica_hello(&h)),
            None => token.clone(),
        };
        eps.push(Endpoint { reach: Reach::Addr(addr), cap, token: t });
    }
    if eps.is_empty() {
        return Err(DbError::Refused { need: format!("a grant for {app}: STATE_STORE_CAP, {f}, or the wallet label state.store:{app}@<node16>") });
    }
    Ok(open_with(app, eps))
}

/// Open the collections of cell `C` (its contract type): `ce_db::open_cell::<LiveData>()` is `open(LiveData::NAME)`,
/// so the owning cell's name is never retyped.
pub fn open_cell<C: ce_wire::Api>() -> Result<Db, DbError> {
    open(C::NAME)
}

/// Open `app` on the store at `addr` with `cap`, Hello empty: a dev store, or a test.
pub fn open_at(app: &str, addr: &str, cap: &str) -> Result<Db, DbError> {
    check_name("app", app).map_err(|why| DbError::Invalid { field: "app".into(), why })?;
    Ok(open_with(app, vec![Endpoint::dev(addr, cap)]))
}

/// Open `app` on these replicas, tried in order.
pub fn open_with(app: &str, eps: Vec<Endpoint>) -> Db {
    Db { inner: Arc::new(DbInner { app: app.into(), store: Arc::new(store::Store::new(app, eps)), mirrors: Mutex::new(HashMap::new()) }) }
}

fn this_node16() -> Option<String> {
    let id = ce_gate::node_identity().ok()?.node_id();
    Some(id.iter().take(8).map(|b| format!("{b:02x}")).collect())
}

fn replica_hello(cap: &str) -> Result<Vec<u8>, String> {
    let chain = ce_gate::chain_from_hex(cap.trim())?;
    let me = ce_gate::node_identity().map_err(|e| e.to_string())?;
    Ok(ce_gate::proof::<StateStore>(&me, &chain))
}

impl Db {
    pub fn app(&self) -> &str {
        &self.inner.app
    }

    /// The typed collection of `T`. The first open per collection reads the registered schema (op 8): equal or newer,
    /// go on; missing or older, register this program's (op 9), which needs `state.<app>.admin`; without that right
    /// the answer is `SchemaMissing` or `SchemaBehind` at once.
    pub fn collection<T: Record>(&self) -> Result<Collection<T>, DbError> {
        let m = T::meta();
        if m.app != self.inner.app {
            return Err(DbError::Invalid { field: "app".into(), why: format!("{} belongs to app {}, this Db is {}", m.type_name, m.app, self.inner.app) });
        }
        self.ensure_schema(m)?;
        Ok(Collection::new(self.clone()))
    }

    fn ensure_schema(&self, m: &'static Meta) -> Result<(), DbError> {
        let app = self.inner.app.clone();
        let coll = m.collection;
        let reg = self.inner.store.call(coll, false, |c, cap| c.call::<Schema>(&SchemaReq { app: app.clone(), cap: cap.into() }))?;
        let mut doc: serde_json::Value = match reg.as_deref() {
            Some(t) => serde_json::from_str(t).map_err(|e| DbError::Store(format!("registered schema of {app} is not JSON: {e}")))?,
            None => serde_json::json!({ "app": app, "collections": {} }),
        };
        let want = m.schema_entry();
        let have = doc.get("collections").and_then(|c| c.get(coll)).cloned();
        let registered = have.as_ref().map(|h| h.get("version").and_then(|v| v.as_u64()).unwrap_or(1) as u32);
        match &have {
            Some(h) if canonical(h) == canonical(&want) => return Ok(()),
            // a newer program registered a later version: this one is an older reader, which additive rules keep working
            Some(_) if registered.unwrap_or(1) > m.version => return Ok(()),
            _ => {}
        }
        if !doc.get("collections").is_some_and(|c| c.is_object()) {
            doc["collections"] = serde_json::json!({});
        }
        doc["collections"][coll] = want;
        if !m.replicas.is_empty() {
            doc["replicas"] = serde_json::json!(m.replicas);
        }
        let text = doc.to_string();
        let put = self.inner.store.call(coll, true, |c, cap| c.call::<SchemaPut>(&SchemaPutReq { app: app.clone(), schema: text.clone(), cap: cap.into() }));
        match put {
            Ok(()) => Ok(()),
            Err(DbError::Refused { .. }) => Err(match registered {
                None => DbError::SchemaMissing { app: app.clone(), coll: coll.into() },
                Some(r) => DbError::SchemaBehind { app: app.clone(), coll: coll.into(), registered: r, compiled: m.version },
            }),
            Err(e) => Err(e),
        }
    }

    pub(crate) fn mirror<T: Record>(&self) -> Result<Arc<mirror::Mirror<T>>, DbError> {
        let coll = T::meta().collection;
        let mut g = self.inner.mirrors.lock().map_err(|_| DbError::Store("mirror lock poisoned".into()))?;
        if let Some(m) = g.get(coll) {
            return m.clone().downcast::<mirror::Mirror<T>>().map_err(|_| DbError::Invalid { field: "collection".into(), why: format!("{coll} is opened as another type in this process") });
        }
        let m = mirror::Mirror::<T>::start(self.inner.store.clone());
        g.insert(coll.into(), m.clone());
        Ok(m)
    }
}
