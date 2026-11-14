//! A dev state.store: the typed `state.store` v2 API (Get, Put, Delete, List, Health, Patch, Schema, SchemaPut,
//! ReplHealth and the Watch stream) in memory on loopback, for tests, the db-conformance kit and local demos.
//!
//! It speaks the same wire and schema hash as the real cell, so a client that passes here dials the real store
//! unchanged. Its rules are copied from the engine (repo:state-store-cell/crates/state-store/src/engine.rs at
//! 7ae4f49): the right names (`state.<app>.<coll>.<verb>`, names lowercased to `[a-z0-9]`), "not authorized: <right>",
//! "version conflict: have N", a collection must be in the app's registered schema, values are validated against the
//! schema's JSON Schema subset (`validate`, copied), Patch merges top-level fields (creating from `{}`), List pages in
//! seq order with `next` set when a page is full, Watch sends every change after `since` and then each new one.
//!
//! Not the engine: no encryption, no audit chain, no replication, no Postgres; `per_person` and `hash_keys` are
//! refused rather than imitated. Rights come from dev grants given at start, not from ce-cap chains. Never point it
//! at production data: it holds nothing but what its callers wrote since it started.
use serde_json::{json, Map, Value};
use state_store_api::*;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

type Key = (String, String, String);

#[derive(Default)]
struct State {
    seq: u64,
    rows: BTreeMap<Key, Doc>,
    schemas: HashMap<String, Value>,
}

struct Shared {
    st: Mutex<State>,
    changed: Condvar,
    grants: HashMap<String, Vec<String>>,
}

/// A running dev store. It serves until the process ends.
pub struct DevStore {
    pub addr: std::net::SocketAddr,
    shared: Arc<Shared>,
}

/// `cap` -> the rights it carries. A right is an engine right name (`state.partsdemo.parts.write`), a prefix ending
/// in `.*` (`state.partsdemo.*`), or `*`.
#[derive(Clone, Debug, Default)]
pub struct Grants(pub Vec<(String, Vec<String>)>);

impl Grants {
    /// One cap that may do everything to `app`: read, write and admin of every collection.
    pub fn all_of(cap: &str, app: &str) -> Grants {
        Grants(vec![(cap.into(), vec![format!("state.{}.*", lower(app))])])
    }
    pub fn with(mut self, cap: &str, rights: &[&str]) -> Grants {
        self.0.push((cap.into(), rights.iter().map(|r| r.to_string()).collect()));
        self
    }
    /// `CAP=right,right` (the `--grant` flag).
    pub fn parse_one(s: &str) -> Result<(String, Vec<String>), String> {
        let (c, r) = s.split_once('=').ok_or_else(|| format!("--grant {s}: CAP=right,right"))?;
        if c.is_empty() {
            return Err("an empty cap is never a grant".into());
        }
        Ok((c.into(), r.split(',').map(|x| x.trim().to_string()).filter(|x| !x.is_empty()).collect()))
    }
}

fn lower(s: &str) -> String {
    s.to_lowercase().chars().filter(|c| c.is_ascii_lowercase() || c.is_ascii_digit()).collect()
}

/// The engine's right name (`engine.rs` `action`).
pub fn action(app: &str, coll: Option<&str>, verb: &str) -> String {
    match coll {
        Some(c) => format!("state.{}.{}.{verb}", lower(app), lower(c)),
        None => format!("state.{}.{verb}", lower(app)),
    }
}

fn name_ok(s: &str) -> bool {
    let b = s.as_bytes();
    !b.is_empty() && b.len() <= 64 && b[0].is_ascii_alphanumeric() && b.iter().all(|c| c.is_ascii_alphanumeric() || b"._-".contains(c))
}

/// engine.rs `validate`, verbatim in behaviour: the JSON Schema subset the generators understand.
pub fn validate(value: &Value, schema: &Value, path: &str) -> Result<(), String> {
    let Some(sc) = schema.as_object().filter(|o| !o.is_empty()) else { return Ok(()) };
    if let Some(t) = sc.get("type") {
        let types: Vec<&str> = match t {
            Value::Array(a) => a.iter().filter_map(Value::as_str).collect(),
            Value::String(x) => vec![x.as_str()],
            _ => vec![],
        };
        let ok = types.iter().any(|one| match *one {
            "integer" => value.is_i64() || value.is_u64(),
            "number" => value.is_number(),
            "object" => value.is_object(),
            "array" => value.is_array(),
            "string" => value.is_string(),
            "boolean" => value.is_boolean(),
            "null" => value.is_null(),
            _ => false,
        });
        if !ok {
            return Err(format!("{path}: expected {t}"));
        }
    }
    if let Some(e) = sc.get("enum").and_then(Value::as_array) {
        if !e.contains(value) {
            return Err(format!("{path}: not one of {}", Value::Array(e.clone())));
        }
    }
    if let Some(obj) = value.as_object() {
        for r in sc.get("required").and_then(Value::as_array).into_iter().flatten().filter_map(Value::as_str) {
            if !obj.contains_key(r) {
                return Err(format!("{path}.{r}: required"));
            }
        }
        let props = sc.get("properties").and_then(Value::as_object);
        for (k, v) in obj {
            if let Some(p) = props.and_then(|p| p.get(k)) {
                validate(v, p, &format!("{path}.{k}"))?;
            } else if sc.get("additionalProperties") == Some(&Value::Bool(false)) {
                return Err(format!("{path}.{k}: not in schema"));
            } else if let Some(ap) = sc.get("additionalProperties").filter(|x| x.is_object()) {
                validate(v, ap, &format!("{path}.{k}"))?;
            }
        }
    }
    if let (Some(arr), Some(items)) = (value.as_array(), sc.get("items").filter(|x| x.is_object())) {
        for (i, v) in arr.iter().enumerate() {
            validate(v, items, &format!("{path}[{i}]"))?;
        }
    }
    Ok(())
}

impl Shared {
    fn allowed(&self, cap: &str, right: &str) -> bool {
        self.grants.get(cap).is_some_and(|rs| {
            rs.iter().any(|r| r == "*" || r == right || r.strip_suffix('*').is_some_and(|p| p.ends_with('.') && right.starts_with(p)))
        })
    }
    /// engine.rs `gate`, without `own` (per_person is refused here).
    fn gate(&self, app: &str, coll: Option<&str>, verb: &str, cap: &str) -> Result<(), String> {
        if !name_ok(app) {
            return Err("bad app".into());
        }
        if let Some(c) = coll {
            if !name_ok(c) {
                return Err("bad coll".into());
            }
        }
        if self.allowed(cap, &action(app, coll, verb)) {
            return Ok(());
        }
        Err(format!("not authorized: {}", action(app, coll, verb)))
    }
    fn coll_schema(st: &State, app: &str, coll: &str) -> Result<Value, String> {
        let Some(sc) = st.schemas.get(app) else { return Err(format!("app '{app}' has no schema; register one with state.schema.put")) };
        let cs = sc.get("collections").and_then(|c| c.get(coll)).cloned().ok_or_else(|| format!("collection {app}/{coll} is not in the schema"))?;
        for k in ["hash_keys", "per_person"] {
            if cs.get(k).and_then(Value::as_bool) == Some(true) {
                return Err(format!("dev store: {k} collections are not imitated here (the engine HMACs keys and scopes rows per person)"));
            }
        }
        Ok(cs)
    }
    fn bump(&self) {
        self.changed.notify_all();
    }

    fn get(&self, r: GetReq) -> Result<Option<Doc>, String> {
        check_key(&r.key)?;
        self.gate(&r.at.app, Some(&r.at.coll), "read", &r.cap)?;
        let st = self.st.lock().map_err(|_| "poisoned")?;
        Self::coll_schema(&st, &r.at.app, &r.at.coll)?;
        Ok(st.rows.get(&(r.at.app, r.at.coll, r.key)).filter(|d| !d.deleted).cloned())
    }

    /// engine.rs `write` for put, insert, patch and delete.
    fn write(&self, at: &Coll, key: &str, cap: &str, op: &str, value: Option<Value>, if_version: Option<u64>) -> Result<Option<Doc>, String> {
        check_key(key)?;
        self.gate(&at.app, Some(&at.coll), "write", cap)?;
        let mut st = self.st.lock().map_err(|_| "poisoned")?;
        let cs = Self::coll_schema(&st, &at.app, &at.coll)?;
        let k: Key = (at.app.clone(), at.coll.clone(), key.into());
        let cur = st.rows.get(&k).cloned();
        let live = cur.as_ref().is_some_and(|c| !c.deleted);
        let version = cur.as_ref().map_or(0, |c| c.version);
        if op == "insert" && live {
            return Ok(cur);
        }
        let value: Option<Value> = match op {
            "delete" => {
                if !live {
                    return Ok(None);
                }
                None
            }
            "patch" => {
                let Some(Value::Object(fields)) = value else { return Err("fields must be an object".into()) };
                let base = if live { serde_json::from_str(&cur.as_ref().map(|c| c.value.clone()).unwrap_or_default()).unwrap_or(json!({})) } else { json!({}) };
                let Some(b) = base.as_object() else { return Err("patch needs an object value".into()) };
                let mut m: Map<String, Value> = b.clone();
                for (k, v) in fields {
                    m.insert(k, v);
                }
                Some(Value::Object(m))
            }
            _ => {
                if let Some(want) = if_version {
                    let have = if live { version } else { 0 };
                    if want != have {
                        return Err(format!("version conflict: have {have}"));
                    }
                }
                value.filter(|v| !v.is_null())
            }
        };
        let text = match &value {
            Some(v) => {
                validate(v, cs.get("value").unwrap_or(&Value::Null), "value")?;
                let t = v.to_string();
                check_value(&t)?;
                Some(t)
            }
            None => None,
        };
        st.seq += 1;
        let d = Doc { key: key.into(), value: text.clone().unwrap_or_default(), seq: st.seq, version: version + 1, deleted: text.is_none() };
        st.rows.insert(k, d.clone());
        drop(st);
        self.bump();
        Ok(Some(d))
    }

    fn list(&self, r: &ListReq, include_deleted: bool) -> Result<Page, String> {
        self.gate(&r.at.app, Some(&r.at.coll), "read", &r.cap)?;
        let st = self.st.lock().map_err(|_| "poisoned")?;
        Self::coll_schema(&st, &r.at.app, &r.at.coll)?;
        let limit = r.limit.clamp(1, MAX_PAGE) as usize;
        let mut docs: Vec<Doc> = st
            .rows
            .iter()
            .filter(|((a, c, k), d)| *a == r.at.app && *c == r.at.coll && k.starts_with(&r.prefix) && d.seq > r.since && (include_deleted || !d.deleted))
            .map(|(_, d)| d.clone())
            .collect();
        docs.sort_by_key(|d| d.seq);
        docs.truncate(limit);
        let next = if docs.len() == limit { docs.last().map(|d| d.seq) } else { None };
        Ok(Page { docs, next })
    }

    fn schema(&self, r: SchemaReq) -> Result<Option<String>, String> {
        let st = self.st.lock().map_err(|_| "poisoned")?;
        let sc = st.schemas.get(&r.app).cloned();
        let colls: Vec<String> = sc.as_ref().and_then(|s| s.get("collections")).and_then(Value::as_object).map(|o| o.keys().cloned().collect()).unwrap_or_default();
        if self.allowed(&r.cap, &action(&r.app, None, "admin")) || colls.iter().any(|c| self.allowed(&r.cap, &action(&r.app, Some(c), "read"))) {
            return Ok(sc.map(|s| s.to_string()));
        }
        Err(format!("not authorized: {}", action(&r.app, None, "admin")))
    }

    fn schema_put(&self, r: SchemaPutReq) -> Result<(), String> {
        self.gate(&r.app, None, "admin", &r.cap)?;
        let doc: Value = serde_json::from_str(&r.schema).map_err(|e| format!("schema is not JSON: {e}"))?;
        let Some(colls) = doc.get("collections").and_then(Value::as_object).filter(|_| doc.get("app").and_then(Value::as_str) == Some(r.app.as_str())) else {
            return Err("schema must be {app, collections:{...}} with app matching".into());
        };
        for cn in colls.keys() {
            if !name_ok(cn) {
                return Err(format!("bad collection name '{cn}'"));
            }
        }
        let mut st = self.st.lock().map_err(|_| "poisoned")?;
        st.schemas.insert(r.app, doc);
        st.seq += 1;
        Ok(())
    }
}

/// Start a dev store on `listen` (`127.0.0.1:0` for a free port). Loopback only.
pub fn serve(listen: &str, grants: Grants) -> std::io::Result<DevStore> {
    let ip: std::net::SocketAddr = listen.parse().map_err(|e| std::io::Error::other(format!("{listen}: {e}")))?;
    if !ip.ip().is_loopback() {
        return Err(std::io::Error::other("the dev store binds loopback only"));
    }
    let shared = Arc::new(Shared { st: Mutex::new(State::default()), changed: Condvar::new(), grants: grants.0.into_iter().collect() });
    let gate = |_: &ce_wire::Hello| Ok(ce_wire::Caller { name: "dev".into(), rights: vec!["state.store:read".into(), "state.store:write".into()] });
    let s = shared.clone();
    let (s1, s2, s3, s4, s5, s6, s7, s8, s9) = (s.clone(), s.clone(), s.clone(), s.clone(), s.clone(), s.clone(), s.clone(), s.clone(), s.clone());
    let srv = ce_wire::blocking::Server::<StateStore>::new(gate)
        .on::<Get, _>(move |r: GetReq, _| s1.get(r))
        .on::<Put, _>(move |r: PutReq, _| {
            check_value(&r.value)?;
            let v: Value = serde_json::from_str(&r.value).map_err(|e| format!("value is not JSON: {e}"))?;
            let op = if r.insert_only { "insert" } else { "put" };
            s2.write(&r.at, &r.key, &r.cap, op, Some(v), r.if_version)?.ok_or_else(|| "nothing written".to_string())
        })
        .on::<Delete, _>(move |r: DeleteReq, _| s3.write(&r.at, &r.key, &r.cap, "delete", None, None).map(|_| ()))
        .on::<List, _>(move |r: ListReq, _| s4.list(&r, r.include_deleted))
        .on::<Health, _>(move |_: (), _| {
            let st = s5.st.lock().map_err(|_| "poisoned")?;
            Ok(StoreHealth { backend: "dev-memory".into(), records: st.rows.values().filter(|d| !d.deleted).count() as u64, head_seq: st.seq, audit_rows: 0, schema_hash: <StateStore as ce_wire::Api>::SCHEMA })
        })
        .on::<Patch, _>(move |r: PatchReq, _| {
            check_value(&r.fields)?;
            let f: Value = serde_json::from_str(&r.fields).map_err(|e| format!("fields is not JSON: {e}"))?;
            s6.write(&r.at, &r.key, &r.cap, "patch", Some(f), None)?.ok_or_else(|| "nothing written".to_string())
        })
        .on::<Schema, _>(move |r: SchemaReq, _| s7.schema(r))
        .on::<SchemaPut, _>(move |r: SchemaPutReq, _| s8.schema_put(r))
        .on::<ReplHealth, _>(|_: (), _| Ok(ReplStatus { node: "dev".into(), peers: vec![] }))
        .stream::<Watch, _>(move |r: WatchReq, _, sink| {
            let mut since = r.since;
            let lr = |since| ListReq { at: r.at.clone(), prefix: String::new(), since, limit: MAX_PAGE, include_deleted: true, cap: r.cap.clone() };
            loop {
                let p = s9.list(&lr(since), true)?;
                for d in &p.docs {
                    since = since.max(d.seq);
                    if sink.send(d).is_err() {
                        return Ok(());
                    }
                }
                if p.next.is_none() {
                    // wait for a write (any app) or 5 s, as the cell's watch_pages does
                    let g = s9.st.lock().map_err(|_| "poisoned")?;
                    let head = g.seq;
                    let (_g, _) = s9.changed.wait_timeout_while(g, Duration::from_secs(5), |st| st.seq == head).map_err(|_| "poisoned")?;
                    if !sink.is_open() {
                        return Ok(());
                    }
                }
            }
        });
    let l = std::net::TcpListener::bind(ip)?;
    let addr = l.local_addr()?;
    std::thread::spawn(move || srv.serve(l));
    Ok(DevStore { addr, shared })
}

impl DevStore {
    /// Register `schema` (an app schema document) without a grant: test setup only.
    pub fn put_schema(&self, app: &str, schema: Value) {
        if let Ok(mut st) = self.shared.st.lock() {
            st.schemas.insert(app.into(), schema);
        }
    }
    /// Rows of `app/coll` as stored (live only), for tests that check what really landed.
    pub fn rows(&self, app: &str, coll: &str) -> Vec<Doc> {
        self.shared.st.lock().map(|st| st.rows.iter().filter(|((a, c, _), d)| a == app && c == coll && !d.deleted).map(|(_, d)| d.clone()).collect()).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ce_wire::blocking::Client;

    fn client(s: &DevStore) -> Client<StateStore> {
        Client::<StateStore>::connect(s.addr, vec![]).unwrap()
    }

    #[test]
    fn rules_follow_the_engine() {
        let s = serve("127.0.0.1:0", Grants::all_of("rw", "t-app").with("ro", &["state.tapp.c.read"])).unwrap();
        let c = client(&s);
        let at = Coll { app: "t-app".into(), coll: "c".into() };
        let put = |cap: &str, v: &str, iv| c.call::<Put>(&PutReq { at: at.clone(), key: "k".into(), value: v.into(), if_version: iv, insert_only: false, cap: cap.into() });
        let e = format!("{:?}", put("rw", "{}", None).unwrap_err());
        assert!(e.contains("app 't-app' has no schema"), "{e}");
        c.call::<SchemaPut>(&SchemaPutReq { app: "t-app".into(), schema: json!({"app":"t-app","collections":{"c":{"value":{"type":"object","required":["n"],"properties":{"n":{"type":"integer"}}}}}}).to_string(), cap: "rw".into() }).unwrap();
        let e = format!("{:?}", put("ro", "{\"n\":1}", None).unwrap_err());
        assert!(e.contains("not authorized: state.tapp.c.write"), "{e}");
        assert_eq!(put("rw", "{\"n\":1}", Some(0)).unwrap().version, 1);
        let e = format!("{:?}", put("rw", "{\"n\":1}", Some(0)).unwrap_err());
        assert!(e.contains("version conflict: have 1"), "{e}");
        let e = format!("{:?}", put("rw", "{\"n\":\"x\"}", None).unwrap_err());
        assert!(e.contains("value.n: expected"), "{e}");
        let d = c.call::<Patch>(&PatchReq { at: at.clone(), key: "k".into(), fields: "{\"m\":2}".into(), cap: "rw".into() }).unwrap();
        assert_eq!(d.version, 2);
        let v: Value = serde_json::from_str(&d.value).unwrap();
        assert_eq!(v, json!({"n":1,"m":2}));
        assert!(format!("{:?}", put("rw", &format!("{{\"n\":1,\"s\":\"{}\"}}", "x".repeat(2000)), None).unwrap_err()).contains("the limit is 2000"));
        c.call::<Delete>(&DeleteReq { at: at.clone(), key: "k".into(), cap: "rw".into() }).unwrap();
        assert!(c.call::<Get>(&GetReq { at: at.clone(), key: "k".into(), cap: "ro".into() }).unwrap().is_none());
        let p = c.call::<List>(&ListReq { at: at.clone(), prefix: "".into(), since: 0, limit: 10, include_deleted: true, cap: "ro".into() }).unwrap();
        assert_eq!(p.docs.len(), 1);
        assert!(p.docs[0].deleted);
    }
}
