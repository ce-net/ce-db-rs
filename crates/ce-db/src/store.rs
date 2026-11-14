//! The connection to state.store: an ordered list of replicas, each with the cap it honours. A call goes to the one in
//! use; a dial that fails, or a replica that answers "store unavailable: ", hands the call to the next. The first
//! dial and the first call wait at most [`FIRST_DEADLINE`], so a store that is not there fails in seconds with
//! `Unavailable { tried }`, not after a long hang.
use ce_db_core::{classify, DbError};
use ce_wire::blocking::Client;
use ce_wire::{Refusal, WireError};
use state_store_api::StateStore;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub const FIRST_DEADLINE: Duration = Duration::from_secs(2);
pub const CALL_DEADLINE: Duration = Duration::from_secs(8);
/// A connection idle this long is replaced before a write (a write is never resent; see state-store-api kv.rs WRITE_FRESH).
pub const WRITE_FRESH: Duration = Duration::from_secs(20);

/// The Hello token of a dial, made fresh each time (a proof is valid for a short window).
pub type Token = Arc<dyn Fn() -> Result<Vec<u8>, String> + Send + Sync>;

#[derive(Clone)]
pub enum Reach {
    Addr(String),
    /// This machine's door, then net.find's replicas (ce_wire by_name).
    ByName,
}

/// One replica: how to reach it, the cap it honours (hex ce-cap chain), the Hello token.
#[derive(Clone)]
pub struct Endpoint {
    pub reach: Reach,
    pub cap: String,
    pub token: Token,
}

impl Endpoint {
    /// A store at `addr` that takes any Hello (a dev store): an empty token.
    pub fn dev(addr: &str, cap: &str) -> Endpoint {
        Endpoint { reach: Reach::Addr(addr.into()), cap: cap.into(), token: Arc::new(|| Ok(Vec::new())) }
    }
    pub fn label(&self) -> String {
        match &self.reach {
            Reach::Addr(a) => a.clone(),
            Reach::ByName => "state.store by name".into(),
        }
    }
}

struct Conn {
    ep: usize,
    c: Client<StateStore>,
    used: Instant,
}

pub struct Store {
    pub app: String,
    pub eps: Vec<Endpoint>,
    conn: Mutex<Option<Conn>>,
}

fn wire_msg(e: &WireError) -> String {
    match e {
        WireError::Refused(Refusal::Remote(m)) => m.clone(),
        WireError::Refused(Refusal::Gate(m)) => format!("refused at the door: {m}"),
        WireError::Refused(Refusal::Right(r)) => format!("not authorized: {r}"),
        other => format!("{other}"),
    }
}

/// A failure that means "this replica cannot answer", so the next one may.
fn hand_on(e: &WireError) -> bool {
    match e {
        WireError::Refused(Refusal::Remote(m)) => m.starts_with("store unavailable: "),
        WireError::Refused(Refusal::Right(_)) | WireError::Refused(Refusal::Schema { .. }) => false,
        _ => true,
    }
}

impl Store {
    pub fn new(app: &str, eps: Vec<Endpoint>) -> Store {
        Store { app: app.into(), eps, conn: Mutex::new(None) }
    }

    fn dial(ep: &Endpoint, wait: Duration) -> Result<Client<StateStore>, String> {
        let t = (ep.token)()?;
        let c = match &ep.reach {
            Reach::Addr(a) => Client::<StateStore>::connect_opts(a.as_str(), t, false, wait),
            Reach::ByName => Client::<StateStore>::by_name(t),
        }
        .map_err(|e| format!("{}: {}", ep.label(), wire_msg(&e)))?;
        c.set_deadline(wait.max(FIRST_DEADLINE));
        Ok(c)
    }

    /// The endpoint the next call goes to (the watch stream follows it).
    pub fn current(&self) -> usize {
        self.conn.lock().ok().and_then(|g| g.as_ref().map(|c| c.ep)).unwrap_or(0)
    }

    /// Run `f` with a live connection and its replica's cap. A read that fails on a reused connection is tried once
    /// more on a fresh one; a write is never resent once it may have reached the store (a timeout or a connection
    /// closed mid-call), only handed to the next replica when that one refused it outright.
    pub fn call<R>(&self, coll: &str, write: bool, f: impl Fn(&Client<StateStore>, &str) -> Result<R, WireError>) -> Result<R, DbError> {
        let mut g = self.conn.lock().map_err(|_| DbError::Store("connection lock poisoned".into()))?;
        if write && g.as_ref().is_some_and(|c| c.used.elapsed() > WRITE_FRESH) {
            *g = None;
        }
        let n = self.eps.len();
        let start = g.as_ref().map_or(0, |c| c.ep);
        let mut tried: Vec<String> = Vec::new();
        let mut k = 0;
        let mut retried = false;
        while k < n {
            let i = (start + k) % n;
            let ep = &self.eps[i];
            let reused = g.as_ref().is_some_and(|c| c.ep == i);
            if !reused {
                match Self::dial(ep, FIRST_DEADLINE) {
                    Ok(c) => *g = Some(Conn { ep: i, c, used: Instant::now() }),
                    Err(e) => {
                        tried.push(e);
                        *g = None;
                        k += 1;
                        continue;
                    }
                }
            }
            let Some(conn) = g.as_mut() else { break };
            let r = f(&conn.c, &ep.cap);
            conn.used = Instant::now();
            conn.c.set_deadline(CALL_DEADLINE);
            let e = match r {
                Ok(v) => return Ok(v),
                Err(e) => e,
            };
            let m = wire_msg(&e);
            if !hand_on(&e) {
                return Err(classify(&m, &self.app, coll));
            }
            *g = None;
            tried.push(format!("{}: {m}", ep.label()));
            let maybe_landed = matches!(e, WireError::Timeout | WireError::Closed | WireError::Io(_));
            if maybe_landed && reused && !write && !retried {
                retried = true;
                continue;
            }
            if maybe_landed && write {
                tried.push("not resent: the write may have landed".into());
                return Err(DbError::Unavailable { tried });
            }
            if m.starts_with("store unavailable: ") && m.contains("not authorized") && k + 1 >= n {
                return Err(classify(&m, &self.app, coll));
            }
            k += 1;
        }
        Err(DbError::Unavailable { tried })
    }
}

#[allow(dead_code)]
fn _assert_send() {
    fn s<T: Send + Sync>() {}
    s::<Store>();
}
