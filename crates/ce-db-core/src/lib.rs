//! The schema half of ce-db (repo:state-store-cell/docs/DB-SDK-DESIGN.md, sections 1 and 3): the [`Record`] trait
//! that `#[derive(Record)]` implements, the field codecs, the canonical value encoding every language must write
//! byte for byte, and the one typed error enum. No network code, so a `<app>-schema` crate depends on this alone and
//! `ce-wire-gen --records` can compile it quickly to read the schema back ([`Meta::to_json`]).
//!
//! Encoding contract (section 1.3): JSON object, keys sorted, no whitespace; `None` is an absent field; `u64`/`i64`
//! are decimal strings; `u32` and smaller are numbers; bytes are lowercase hex; times are integer unix ms; `Id` is a
//! 26-character ULID; every row carries `_v`, its writer's schema version. Fields a reader does not know are kept.
pub use ce_db_derive::Record;
use serde_json::{Map, Value};
use std::cmp::Ordering;
use std::fmt;
use std::ops::Bound;
use std::sync::OnceLock;

pub use serde_json;

/// A JSON object as the store holds it.
pub type Obj = Map<String, Value>;

/// The largest value the typed door of state.store takes (state_store_api::MAX_VALUE; the China link drops bigger rows).
pub const MAX_VALUE: usize = 2000;
/// Default bound of the in-memory index mirror of slice 1 (section 2.1).
pub const MIRROR_LIMIT: usize = 5000;

pub mod prelude {
    pub use crate::{DbError, Hex, Id, IndexQuery, Millis, Record};
}

// ---------------------------------------------------------------- errors

/// Every failure of every verb, in every language the same (section 1.5). Never matched by string.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DbError {
    NotFound { key: String },
    /// A compare-and-set lost, or `insert` met an existing key: the version the store holds.
    Conflict { held_version: u64 },
    Invalid { field: String, why: String },
    /// The grant lacks a right: the right it needs (`state.<app>.<coll>.write`).
    Refused { need: String },
    TooLarge { bytes: usize, limit: usize },
    Throttled { retry_after_ms: u64 },
    SchemaMissing { app: String, coll: String },
    SchemaBehind { app: String, coll: String, registered: u32, compiled: u32 },
    Unavailable { tried: Vec<String> },
    /// An index query on a collection larger than the mirror holds (slice 1); slice 2 answers it in the engine.
    NeedsServerIndex { rows: usize, limit: usize },
    /// Anything the store said that is none of the above, verbatim.
    Store(String),
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbError::NotFound { key } => write!(f, "not found: {key}"),
            DbError::Conflict { held_version } => write!(f, "conflict: the store holds version {held_version}"),
            DbError::Invalid { field, why } => write!(f, "invalid {field}: {why}"),
            DbError::Refused { need } => write!(f, "refused: the grant lacks {need}"),
            DbError::TooLarge { bytes, limit } => write!(f, "too large: {bytes} bytes, the limit is {limit}"),
            DbError::Throttled { retry_after_ms } => write!(f, "throttled: retry after {retry_after_ms} ms"),
            DbError::SchemaMissing { app, coll } => write!(f, "schema missing: {app}/{coll} is not registered and this grant cannot register it ({})", admin_right(app)),
            DbError::SchemaBehind { app, coll, registered, compiled } => {
                write!(f, "schema behind: {app}/{coll} is registered at version {registered}, this program has {compiled}, and this grant cannot register it ({})", admin_right(app))
            }
            DbError::Unavailable { tried } => write!(f, "unavailable: {}", tried.join("; ")),
            DbError::NeedsServerIndex { rows, limit } => write!(f, "needs a server index: {rows} rows, the mirror holds at most {limit}"),
            DbError::Store(s) => write!(f, "store: {s}"),
        }
    }
}

impl std::error::Error for DbError {}

/// The engine's name of a right (`engine.rs` `action`): app and collection lowercased, everything but `[a-z0-9]` dropped.
pub fn right(app: &str, coll: Option<&str>, verb: &str) -> String {
    let n = |s: &str| s.to_lowercase().chars().filter(|c| c.is_ascii_lowercase() || c.is_ascii_digit()).collect::<String>();
    match coll {
        Some(c) => format!("state.{}.{}.{verb}", n(app), n(c)),
        None => format!("state.{}.{verb}", n(app)),
    }
}

fn admin_right(app: &str) -> String {
    right(app, None, "admin")
}

/// The store answers in text; this is the one place that reads it. `app`/`coll` name the call for schema errors.
pub fn classify(msg: &str, app: &str, coll: &str) -> DbError {
    let m = msg.trim();
    if let Some(i) = m.find("not authorized") {
        let rest = &m[i..];
        let need = rest.rsplit(": ").next().unwrap_or("").trim().trim_end_matches(['"', ')']).to_string();
        return DbError::Refused { need };
    }
    if let Some(r) = m.find("version conflict").map(|i| &m[i..]) {
        let held = r.rsplit(|c: char| !c.is_ascii_digit()).find(|s| !s.is_empty()).and_then(|d| d.parse().ok()).unwrap_or(0);
        // "version conflict: expected E, have H" (the fake) and "version conflict: have H" (the engine): H is last
        return DbError::Conflict { held_version: held };
    }
    if m.contains("has no schema") || m.contains("is not in the schema") {
        return DbError::SchemaMissing { app: app.into(), coll: coll.into() };
    }
    if m.contains("value too large") {
        return DbError::TooLarge { bytes: 0, limit: MAX_VALUE };
    }
    if let Some(r) = m.strip_prefix("value is ") {
        let bytes = r.split(' ').next().and_then(|d| d.parse().ok()).unwrap_or(0);
        return DbError::TooLarge { bytes, limit: MAX_VALUE };
    }
    // engine validate(): "value.<field>: expected ...", "value.<field>: required", "value.<field>: not one of ..."
    if let Some(r) = m.strip_prefix("value.") {
        if let Some((f, why)) = r.split_once(": ") {
            return DbError::Invalid { field: f.into(), why: why.into() };
        }
    }
    if m.starts_with("value: ") || m.starts_with("value is not JSON") || m.starts_with("bad key") {
        return DbError::Invalid { field: "value".into(), why: m.into() };
    }
    if m.starts_with("store unavailable: ") || m.contains("every replica unavailable") {
        return DbError::Unavailable { tried: vec![m.into()] };
    }
    DbError::Store(m.into())
}

// ---------------------------------------------------------------- field types

/// The type of a field as every language sees it; `kind` names are the generator's IR.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FieldTy {
    Str,
    Bool,
    /// A JSON number: u8, u16, u32, i8, i16, i32.
    Small(&'static str),
    /// A decimal string: u64, i64 (JavaScript rounds numbers above 2^53).
    Big(&'static str),
    F64,
    Id,
    Millis,
    Hex,
    List(Box<FieldTy>),
    Opt(Box<FieldTy>),
}

impl FieldTy {
    pub fn to_json(&self) -> Value {
        let k = |s: &str| serde_json::json!({ "kind": s });
        match self {
            FieldTy::Str => k("string"),
            FieldTy::Bool => k("bool"),
            FieldTy::Small(n) | FieldTy::Big(n) => k(n),
            FieldTy::F64 => k("f64"),
            FieldTy::Id => k("id"),
            FieldTy::Millis => k("millis"),
            FieldTy::Hex => k("hex"),
            FieldTy::List(t) => serde_json::json!({ "kind": "list", "of": t.to_json() }),
            FieldTy::Opt(t) => serde_json::json!({ "kind": "option", "of": t.to_json() }),
        }
    }
    /// The JSON Schema subset the engine validates (`engine.rs` `validate`).
    pub fn json_schema(&self) -> Value {
        let t = |s: &str| serde_json::json!({ "type": s });
        match self {
            FieldTy::Str | FieldTy::Big(_) | FieldTy::Id | FieldTy::Hex => t("string"),
            FieldTy::Bool => t("boolean"),
            FieldTy::Small(_) | FieldTy::Millis => t("integer"),
            FieldTy::F64 => t("number"),
            FieldTy::List(i) => serde_json::json!({ "type": "array", "items": i.json_schema() }),
            // a cleared Option written by `update` (Patch) is a JSON null
            FieldTy::Opt(i) => {
                let mut s = i.json_schema();
                if let Some(Value::String(x)) = s.get("type").cloned() {
                    s["type"] = serde_json::json!([x, "null"]);
                }
                s
            }
        }
    }
    pub fn is_option(&self) -> bool {
        matches!(self, FieldTy::Opt(_))
    }
}

/// An index key: comparable across rows of one field. `Null` sorts first (an absent optional value).
#[derive(Clone, Debug, PartialEq)]
pub enum IndexVal {
    Null,
    Bool(bool),
    Int(i128),
    F64(f64),
    Str(String),
}

impl Eq for IndexVal {}
impl PartialOrd for IndexVal {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for IndexVal {
    fn cmp(&self, o: &Self) -> Ordering {
        use IndexVal::*;
        let rank = |v: &IndexVal| match v {
            Null => 0,
            Bool(_) => 1,
            Int(_) | F64(_) => 2,
            Str(_) => 3,
        };
        match (self, o) {
            (Bool(a), Bool(b)) => a.cmp(b),
            (Int(a), Int(b)) => a.cmp(b),
            (F64(a), F64(b)) => a.total_cmp(b),
            (Int(a), F64(b)) => (*a as f64).total_cmp(b),
            (F64(a), Int(b)) => a.total_cmp(&(*b as f64)),
            (Str(a), Str(b)) => a.cmp(b),
            _ => rank(self).cmp(&rank(o)),
        }
    }
}

impl IndexVal {
    /// For opaque cursors: the value as JSON.
    pub fn to_json(&self) -> Value {
        match self {
            IndexVal::Null => Value::Null,
            IndexVal::Bool(b) => Value::Bool(*b),
            IndexVal::Int(i) => Value::String(i.to_string()),
            IndexVal::F64(f) => serde_json::Number::from_f64(*f).map(Value::Number).unwrap_or(Value::Null),
            IndexVal::Str(s) => serde_json::json!({ "s": s }),
        }
    }
    pub fn from_json(v: &Value) -> Option<IndexVal> {
        Some(match v {
            Value::Null => IndexVal::Null,
            Value::Bool(b) => IndexVal::Bool(*b),
            Value::String(s) => IndexVal::Int(s.parse().ok()?),
            Value::Number(n) => IndexVal::F64(n.as_f64()?),
            Value::Object(o) => IndexVal::Str(o.get("s")?.as_str()?.to_string()),
            _ => return None,
        })
    }
}

/// One field's codec. `#[derive(Record)]` calls these; implement it for a newtype to store it as a field.
pub trait Field: Sized {
    fn ty() -> FieldTy;
    /// None = the field is absent from the row.
    fn to_json(&self) -> Option<Value>;
    /// `v` None = absent from the row.
    fn from_json(v: Option<&Value>) -> Result<Self, String>;
    fn index_val(&self) -> IndexVal;
    /// What `#[db(max = N)]` bounds: bytes of a string, items of a list.
    fn bound_len(&self) -> Option<usize> {
        None
    }
}

fn need<'a>(v: Option<&'a Value>) -> Result<&'a Value, String> {
    match v {
        None | Some(Value::Null) => Err("missing".into()),
        Some(v) => Ok(v),
    }
}

impl Field for String {
    fn ty() -> FieldTy {
        FieldTy::Str
    }
    fn to_json(&self) -> Option<Value> {
        Some(Value::String(self.clone()))
    }
    fn from_json(v: Option<&Value>) -> Result<Self, String> {
        need(v)?.as_str().map(str::to_string).ok_or_else(|| "expected a string".into())
    }
    fn index_val(&self) -> IndexVal {
        IndexVal::Str(self.clone())
    }
    fn bound_len(&self) -> Option<usize> {
        Some(self.len())
    }
}

impl Field for bool {
    fn ty() -> FieldTy {
        FieldTy::Bool
    }
    fn to_json(&self) -> Option<Value> {
        Some(Value::Bool(*self))
    }
    fn from_json(v: Option<&Value>) -> Result<Self, String> {
        need(v)?.as_bool().ok_or_else(|| "expected a bool".into())
    }
    fn index_val(&self) -> IndexVal {
        IndexVal::Bool(*self)
    }
}

macro_rules! small {
    ($($t:ty => $n:literal),*) => {$(
        impl Field for $t {
            fn ty() -> FieldTy { FieldTy::Small($n) }
            fn to_json(&self) -> Option<Value> { Some(Value::from(*self)) }
            fn from_json(v: Option<&Value>) -> Result<Self, String> {
                let v = need(v)?;
                v.as_i64().and_then(|i| <$t>::try_from(i).ok()).ok_or_else(|| format!("expected an integer in {}", $n))
            }
            fn index_val(&self) -> IndexVal { IndexVal::Int(*self as i128) }
        }
    )*};
}
small!(u8 => "u8", u16 => "u16", u32 => "u32", i8 => "i8", i16 => "i16", i32 => "i32");

macro_rules! big {
    ($($t:ty => $n:literal),*) => {$(
        impl Field for $t {
            fn ty() -> FieldTy { FieldTy::Big($n) }
            fn to_json(&self) -> Option<Value> { Some(Value::String(self.to_string())) }
            fn from_json(v: Option<&Value>) -> Result<Self, String> {
                match need(v)? {
                    Value::String(s) => s.parse().map_err(|_| format!("expected a decimal string in {}", $n)),
                    _ => Err(format!("expected a decimal string ({} is a string on the wire)", $n)),
                }
            }
            fn index_val(&self) -> IndexVal { IndexVal::Int(*self as i128) }
        }
    )*};
}
big!(u64 => "u64", i64 => "i64");

impl Field for f64 {
    fn ty() -> FieldTy {
        FieldTy::F64
    }
    fn to_json(&self) -> Option<Value> {
        serde_json::Number::from_f64(*self).map(Value::Number)
    }
    fn from_json(v: Option<&Value>) -> Result<Self, String> {
        need(v)?.as_f64().ok_or_else(|| "expected a number".into())
    }
    fn index_val(&self) -> IndexVal {
        IndexVal::F64(*self)
    }
}

impl<T: Field> Field for Vec<T> {
    fn ty() -> FieldTy {
        FieldTy::List(Box::new(T::ty()))
    }
    fn to_json(&self) -> Option<Value> {
        Some(Value::Array(self.iter().map(|x| x.to_json().unwrap_or(Value::Null)).collect()))
    }
    fn from_json(v: Option<&Value>) -> Result<Self, String> {
        match need(v)? {
            Value::Array(a) => a.iter().enumerate().map(|(i, x)| T::from_json(Some(x)).map_err(|e| format!("[{i}]: {e}"))).collect(),
            _ => Err("expected a list".into()),
        }
    }
    fn index_val(&self) -> IndexVal {
        IndexVal::Null
    }
    fn bound_len(&self) -> Option<usize> {
        Some(self.len())
    }
}

impl<T: Field> Field for Option<T> {
    fn ty() -> FieldTy {
        FieldTy::Opt(Box::new(T::ty()))
    }
    fn to_json(&self) -> Option<Value> {
        self.as_ref().and_then(Field::to_json)
    }
    fn from_json(v: Option<&Value>) -> Result<Self, String> {
        match v {
            None | Some(Value::Null) => Ok(None),
            v => T::from_json(v).map(Some),
        }
    }
    fn index_val(&self) -> IndexVal {
        self.as_ref().map_or(IndexVal::Null, Field::index_val)
    }
    fn bound_len(&self) -> Option<usize> {
        self.as_ref().and_then(Field::bound_len)
    }
}

/// A row key the client makes: a ULID (48-bit unix ms + 80 random bits, Crockford base32), sorts by creation time.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Id(String);

const CROCKFORD: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

impl Id {
    pub fn new() -> Id {
        let ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_millis() as u128);
        let mut r = [0u8; 10];
        // no entropy is an invariant failure of the OS, not an input error
        getrandom::getrandom(&mut r).expect("getrandom");
        let mut n: u128 = (ms & ((1 << 48) - 1)) << 80;
        for (i, b) in r.iter().enumerate() {
            n |= (*b as u128) << (72 - 8 * i);
        }
        let mut s = [0u8; 26];
        for i in (0..26).rev() {
            s[i] = CROCKFORD[(n & 31) as usize];
            n >>= 5;
        }
        Id(String::from_utf8_lossy(&s).into_owned())
    }
    /// 26 characters of Crockford base32 (upper case), the first one 0-7.
    pub fn parse(s: &str) -> Result<Id, String> {
        let b = s.as_bytes();
        if b.len() != 26 || !b.iter().all(|c| CROCKFORD.contains(c)) || b[0] > b'7' {
            return Err(format!("not a ULID: {s:?}"));
        }
        Ok(Id(s.into()))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for Id {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::str::FromStr for Id {
    type Err = String;
    fn from_str(s: &str) -> Result<Id, String> {
        Id::parse(s)
    }
}

impl Field for Id {
    fn ty() -> FieldTy {
        FieldTy::Id
    }
    fn to_json(&self) -> Option<Value> {
        Some(Value::String(self.0.clone()))
    }
    fn from_json(v: Option<&Value>) -> Result<Self, String> {
        Id::parse(need(v)?.as_str().ok_or("expected a ULID string")?)
    }
    fn index_val(&self) -> IndexVal {
        IndexVal::Str(self.0.clone())
    }
}

/// Unix time in milliseconds, a JSON integer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Millis(pub i64);

impl Millis {
    pub fn now() -> Millis {
        Millis(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_millis() as i64))
    }
}

impl Field for Millis {
    fn ty() -> FieldTy {
        FieldTy::Millis
    }
    fn to_json(&self) -> Option<Value> {
        Some(Value::from(self.0))
    }
    fn from_json(v: Option<&Value>) -> Result<Self, String> {
        need(v)?.as_i64().map(Millis).ok_or_else(|| "expected integer unix ms".into())
    }
    fn index_val(&self) -> IndexVal {
        IndexVal::Int(self.0 as i128)
    }
}

/// Bytes, stored as lowercase hex (never base64).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
pub struct Hex(pub Vec<u8>);

impl Field for Hex {
    fn ty() -> FieldTy {
        FieldTy::Hex
    }
    fn to_json(&self) -> Option<Value> {
        Some(Value::String(self.0.iter().map(|b| format!("{b:02x}")).collect()))
    }
    fn from_json(v: Option<&Value>) -> Result<Self, String> {
        let s = need(v)?.as_str().ok_or("expected a hex string")?;
        if s.len() % 2 != 0 || !s.bytes().all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c)) {
            return Err("expected lowercase hex".into());
        }
        (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|e| e.to_string())).collect::<Result<_, _>>().map(Hex)
    }
    fn index_val(&self) -> IndexVal {
        IndexVal::Str(self.to_json().and_then(|v| v.as_str().map(str::to_string)).unwrap_or_default())
    }
    fn bound_len(&self) -> Option<usize> {
        Some(self.0.len())
    }
}

/// A key field: how its value becomes the row key.
pub trait KeyField {
    fn key_string(&self) -> String;
}
impl KeyField for Id {
    fn key_string(&self) -> String {
        self.0.clone()
    }
}
impl KeyField for String {
    fn key_string(&self) -> String {
        self.clone()
    }
}

// ---------------------------------------------------------------- the schema

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexKind {
    Eq,
    Ordered,
}

#[derive(Clone, Debug)]
pub struct FieldMeta {
    pub name: &'static str,
    pub ty: FieldTy,
    pub key: bool,
    pub index: Option<IndexKind>,
    pub max: Option<usize>,
    pub since: u32,
    /// The encoded default for a field added later (`#[db(default = ...)]`).
    pub default: Option<Value>,
    pub secret: bool,
    pub doc: &'static str,
}

/// One collection's schema, made by `#[derive(Record)]` from the struct: the one source every language is generated from.
#[derive(Clone, Debug)]
pub struct Meta {
    pub type_name: &'static str,
    pub app: &'static str,
    pub collection: &'static str,
    pub version: u32,
    pub consistency: &'static str,
    pub fields: Vec<FieldMeta>,
    pub hash_keys: bool,
    pub per_person: bool,
    pub replicas: Vec<&'static str>,
    pub mirror_limit: usize,
    pub doc: &'static str,
}

impl Meta {
    pub fn key_field(&self) -> &FieldMeta {
        // the derive refuses a struct without exactly one key field
        self.fields.iter().find(|f| f.key).expect("a Record has one key field")
    }
    pub fn field(&self, name: &str) -> Option<&FieldMeta> {
        self.fields.iter().find(|f| f.name == name)
    }
    pub fn indexes(&self) -> impl Iterator<Item = (&'static str, IndexKind)> + '_ {
        self.fields.iter().filter_map(|f| f.index.map(|k| (f.name, k)))
    }

    /// The generator's input (`ce-wire-gen --records`): everything a language needs to emit the typed collection.
    pub fn to_json(&self) -> Value {
        serde_json::json!({
            "type": self.type_name,
            "app": self.app,
            "collection": self.collection,
            "version": self.version,
            "consistency": self.consistency,
            "key": self.key_field().name,
            "doc": self.doc,
            "hash_keys": self.hash_keys,
            "per_person": self.per_person,
            "replicas": self.replicas,
            "mirror_limit": self.mirror_limit,
            "max_value": MAX_VALUE,
            "fields": self.fields.iter().map(|f| serde_json::json!({
                "name": f.name,
                "ty": f.ty.to_json(),
                "key": f.key,
                "index": f.index.map(|k| if k == IndexKind::Ordered { "ordered" } else { "eq" }),
                "max": f.max,
                "since": f.since,
                "default": f.default,
                "secret": f.secret,
                "doc": f.doc,
            })).collect::<Vec<_>>(),
        })
    }

    /// This collection's entry in the app schema `SchemaPut` takes (`{value, hash_keys, per_person}` plus the
    /// optional keys of the design, which the engine carries and ignores).
    pub fn schema_entry(&self) -> Value {
        let mut props = Obj::new();
        let mut required = vec![Value::String("_v".into())];
        for f in &self.fields {
            props.insert(f.name.into(), f.ty.json_schema());
            if !f.ty.is_option() && f.default.is_none() {
                required.push(Value::String(f.name.into()));
            }
        }
        props.insert("_v".into(), serde_json::json!({ "type": "integer" }));
        serde_json::json!({
            "value": { "type": "object", "properties": props, "required": required },
            "hash_keys": self.hash_keys,
            "per_person": self.per_person,
            "version": self.version,
            "consistency": self.consistency,
            "key": self.key_field().name,
            "indexes": self.indexes().map(|(f, k)| serde_json::json!({ "field": f, "ordered": k == IndexKind::Ordered })).collect::<Vec<_>>(),
            "limits": { "max_value": MAX_VALUE, "mirror_limit": self.mirror_limit },
        })
    }

    /// The whole app schema for a set of collections of one app (`schemas/<app>.json`).
    pub fn app_schema(app: &str, doc: &str, metas: &[&Meta]) -> Value {
        let mut colls = Obj::new();
        let mut replicas: Vec<&str> = vec![];
        for m in metas.iter().filter(|m| m.app == app) {
            colls.insert(m.collection.into(), m.schema_entry());
            replicas.extend(m.replicas.iter().copied().filter(|r| !replicas.contains(r)).collect::<Vec<_>>());
        }
        let mut s = serde_json::json!({ "app": app, "doc": doc, "collections": colls });
        if !replicas.is_empty() {
            s["replicas"] = serde_json::json!(replicas);
        }
        s
    }
}

/// A typed row. Implemented by `#[derive(Record)]`; never by hand.
pub trait Record: Sized + Clone + Send + Sync + 'static {
    fn meta() -> &'static Meta;
    fn key(&self) -> String;
    /// The fields, without `_v`; absent options left out.
    fn encode_fields(&self) -> Obj;
    fn decode_fields(o: &Obj) -> Result<Self, DbError>;
    fn index_val(&self, field: &str) -> IndexVal;
    /// `#[db(max = N)]` bounds.
    fn check(&self) -> Result<(), DbError>;
}

/// The generated `by_<field>` methods are written against this; `ce_db::Collection` implements it.
pub trait IndexQuery<T: Record> {
    type Q;
    fn index_eq(&self, index: &'static str, v: IndexVal) -> Self::Q;
    fn index_range(&self, index: &'static str, lo: Bound<IndexVal>, hi: Bound<IndexVal>) -> Self::Q;
}

/// Helper for the derive: a range bound on a field value as an index bound.
pub fn bound_of<F: Field>(b: Bound<&F>) -> Bound<IndexVal> {
    match b {
        Bound::Included(v) => Bound::Included(v.index_val()),
        Bound::Excluded(v) => Bound::Excluded(v.index_val()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

/// Helper for the derive: one field from a row, or its default when absent.
pub fn field_or<F: Field>(o: &Obj, name: &str, default: Option<fn() -> F>) -> Result<F, DbError> {
    match (o.get(name), default) {
        (None | Some(Value::Null), Some(d)) => Ok(d()),
        (v, _) => F::from_json(v).map_err(|why| DbError::Invalid { field: name.into(), why }),
    }
}

/// Helper for the derive: the `max` check of one field.
pub fn check_max<F: Field>(v: &F, name: &str, max: usize) -> Result<(), DbError> {
    match v.bound_len() {
        Some(n) if n > max => Err(DbError::Invalid { field: name.into(), why: format!("{n} long, the limit is {max}") }),
        _ => Ok(()),
    }
}

/// Helper for the derive: a lazily built `Meta`.
pub fn meta_cell(cell: &'static OnceLock<Meta>, f: fn() -> Meta) -> &'static Meta {
    cell.get_or_init(f)
}

// ---------------------------------------------------------------- canonical encoding

/// The canonical JSON text of `v`: object keys sorted by bytes, no whitespace, strings escaped as serde_json does.
pub fn canonical(v: &Value) -> String {
    let mut s = String::new();
    write_canon(v, &mut s);
    s
}

fn write_canon(v: &Value, out: &mut String) {
    match v {
        Value::Object(o) => {
            let mut keys: Vec<&String> = o.keys().collect();
            keys.sort();
            out.push('{');
            for (i, k) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&Value::String((*k).clone()).to_string());
                out.push(':');
                write_canon(&o[k.as_str()], out);
            }
            out.push('}');
        }
        Value::Array(a) => {
            out.push('[');
            for (i, x) in a.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canon(x, out);
            }
            out.push(']');
        }
        // an integral f64 is written as its integer digits, as JavaScript writes it (serde_json would say `100.0`)
        Value::Number(n) if n.is_f64() => match n.as_f64() {
            Some(f) if f == 0.0 => out.push('0'),
            Some(f) if f.fract() == 0.0 && f.abs() < 1e21 => out.push_str(&format!("{f:.0}")),
            _ => out.push_str(&n.to_string()),
        },
        other => out.push_str(&other.to_string()),
    }
}

/// A row as stored: the typed value, the fields this program does not know (kept and written back), and the
/// schema version of the writer.
#[derive(Clone, Debug, PartialEq)]
pub struct Decoded<T> {
    pub value: T,
    pub extra: Obj,
    pub writer_version: u32,
}

/// The stored text of `t` with `extra` (unknown fields read earlier) merged in: checked against `max` bounds and the
/// 2,000-byte row limit before any network call.
pub fn encode_with<T: Record>(t: &T, extra: &Obj) -> Result<String, DbError> {
    t.check()?;
    let mut o = extra.clone();
    for (k, v) in t.encode_fields() {
        o.insert(k, v);
    }
    o.insert("_v".into(), Value::from(T::meta().version));
    let s = canonical(&Value::Object(o));
    if s.len() > MAX_VALUE {
        return Err(DbError::TooLarge { bytes: s.len(), limit: MAX_VALUE });
    }
    Ok(s)
}

pub fn encode<T: Record>(t: &T) -> Result<String, DbError> {
    encode_with(t, &Obj::new())
}

pub fn decode_obj<T: Record>(o: &Obj) -> Result<Decoded<T>, DbError> {
    let value = T::decode_fields(o)?;
    let known: Vec<&str> = T::meta().fields.iter().map(|f| f.name).collect();
    let extra: Obj = o.iter().filter(|(k, _)| k.as_str() != "_v" && !known.contains(&k.as_str())).map(|(k, v)| (k.clone(), v.clone())).collect();
    let writer_version = o.get("_v").and_then(Value::as_u64).unwrap_or(1) as u32;
    Ok(Decoded { value, extra, writer_version })
}

pub fn decode<T: Record>(text: &str) -> Result<Decoded<T>, DbError> {
    let v: Value = serde_json::from_str(text).map_err(|e| DbError::Invalid { field: "value".into(), why: format!("not JSON: {e}") })?;
    match v {
        Value::Object(o) => decode_obj(&o),
        _ => Err(DbError::Invalid { field: "value".into(), why: "not an object".into() }),
    }
}

/// The top-level fields that differ between two encodings: what `update` sends as a Patch. A field present in `old`
/// and absent in `new` (an Option set to None) is sent as null.
pub fn diff(old: &Obj, new: &Obj) -> Obj {
    let mut d = Obj::new();
    for (k, v) in new {
        if old.get(k) != Some(v) {
            d.insert(k.clone(), v.clone());
        }
    }
    for k in old.keys() {
        if !new.contains_key(k) {
            d.insert(k.clone(), Value::Null);
        }
    }
    d
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_sorts_and_strips_whitespace() {
        let v: Value = serde_json::from_str(r#"{ "b": [1, {"z":1,"a":"\n\u0001é"}], "a": null }"#).unwrap();
        assert_eq!(canonical(&v), r#"{"a":null,"b":[1,{"a":"\n\u0001é","z":1}]}"#);
        // the JavaScript form of numbers: 100.0 is 100, -0.0 is 0, 1e20 in digits
        let n = serde_json::json!([100.0, -0.0, 1.5, 1e20, -3.0]);
        assert_eq!(canonical(&n), "[100,0,1.5,100000000000000000000,-3]");
    }

    #[test]
    fn ulids_parse_and_sort_by_time() {
        let a = Id::new();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let b = Id::new();
        assert!(a < b, "{a} {b}");
        assert_eq!(Id::parse(a.as_str()).unwrap(), a);
        assert!(Id::parse("8ZZZZZZZZZZZZZZZZZZZZZZZZZ").is_err());
        assert!(Id::parse("short").is_err());
    }

    #[test]
    fn the_store_text_becomes_typed_errors() {
        assert_eq!(classify("not authorized: state.partsdemo.parts.write", "parts-demo", "parts"), DbError::Refused { need: "state.partsdemo.parts.write".into() });
        assert_eq!(
            classify("store unavailable: this replica does not root the grant; not authorized here: state.x.y.read", "x", "y"),
            DbError::Refused { need: "state.x.y.read".into() }
        );
        assert_eq!(classify("version conflict: have 3", "a", "b"), DbError::Conflict { held_version: 3 });
        assert_eq!(classify("version conflict: expected 0, have 7", "a", "b"), DbError::Conflict { held_version: 7 });
        assert_eq!(classify("app 'a' has no schema; register one with state.schema.put", "a", "b"), DbError::SchemaMissing { app: "a".into(), coll: "b".into() });
        assert_eq!(classify("value.qty: expected \"integer\"", "a", "b"), DbError::Invalid { field: "qty".into(), why: "expected \"integer\"".into() });
        assert_eq!(classify("value is 2001 bytes, the limit is 2000", "a", "b"), DbError::TooLarge { bytes: 2001, limit: 2000 });
        assert_eq!(right("parts-demo", Some("parts"), "write"), "state.partsdemo.parts.write");
    }

    #[test]
    fn index_values_order() {
        let mut v = vec![IndexVal::Str("b".into()), IndexVal::Int(3), IndexVal::Null, IndexVal::Int(-1), IndexVal::Str("a".into())];
        v.sort();
        assert_eq!(v, vec![IndexVal::Null, IndexVal::Int(-1), IndexVal::Int(3), IndexVal::Str("a".into()), IndexVal::Str("b".into())]);
    }

    #[test]
    fn diff_sends_changed_and_cleared_fields() {
        let o: Obj = serde_json::from_str(r#"{"a":1,"b":2,"c":3}"#).unwrap();
        let n: Obj = serde_json::from_str(r#"{"a":1,"b":5}"#).unwrap();
        assert_eq!(canonical(&Value::Object(diff(&o, &n))), r#"{"b":5,"c":null}"#);
    }
}
