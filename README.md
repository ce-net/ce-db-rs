# ce-db-rs: typed state.store collections for Rust

Slice 1 of `repo:state-store-cell/docs/DB-SDK-DESIGN.md`: one Rust struct is the schema, and every language gets the
same typed collection from it. This repo is the Rust half and the schema source; TypeScript is `repo:ce-db-ts`, the
cross-language contract is `repo:db-conformance`, a worked app is `repo:parts-demo`.

```rust
use ce_db::prelude::*;                       // a schema crate depends on ce-db-core only (it names ::ce_db_core)

/// A part in a bin.
#[derive(Record, Clone, Debug)]
#[db(app = "parts-demo", collection = "parts", version = 2)]
pub struct Part {
    #[db(key)]            pub id: Id,                 // ULID made by the client
    #[db(max = 120)]      pub name: String,
    #[db(index)]          pub bin: String,            // equality index
    #[db(index, ordered)] pub qty: u32,               // range index
    #[db(since = 2)]      pub supplier: Option<String>,
}

let db = ce_db::open("parts-demo")?;                  // or open_at(app, "127.0.0.1:7811", cap) for a dev store
let parts = db.collection::<Part>()?;                 // reads the registered schema, registers it if missing/older
let id = parts.insert(&part)?;                        // Conflict if the key exists
let one: Option<Part> = parts.get(&id)?;
parts.update(&id, |p| p.qty -= 1)?;                   // Patch of the changed fields only
let in_a4 = parts.by_bin("A4").limit(50).fetch()?;    // use PartIndexes: by_<field> per declared index
let low = parts.by_qty(..20).desc().fetch()?;
for ch in parts.by_bin("A4").watch()? { /* Added(T) | Changed{old,new} | Removed(key) | Synced */ }
```

## Crates

| crate | what |
|---|---|
| `ce-db-core` | `Record` trait, `Field` codecs (`String`, `bool`, `u8..u32`/`i8..i32` as numbers, `u64`/`i64` as decimal strings, `f64`, `Id`, `Millis`, `Hex`, `Vec<T>`, `Option<T>`), canonical encoding, `DbError`, `Meta` (the schema the generator reads). No network. |
| `ce-db-derive` | `#[derive(Record)]`: struct keys `app collection version consistency hash_keys per_person replicas mirror_limit`, field keys `key index ordered max since default secret`. Refused at compile time with the reason: `unique` (needs the engine index, slice 2), `blob` (state.blob, slice 3), `consistency = crdt` (slice 4), `consistency = ledger` (state.consensus and RAFT-BRIEF, a lead decision). |
| `ce-db` | the runtime: `open`, `open_at`, `open_with`, `Db::collection`, `Collection` (insert, get, get_stored, update, replace, replace_stored, remove, prefix, all, by_<index>, watch, watch_key, stats, link), `Query` (limit, after, desc, fetch, fetch_page, fetch_with_rejects, count, watch). |
| `ce-db-dev` | a dev state.store: the typed v2 API in memory on loopback with dev grants (`ce-db-dev-store --grant CAP=state.<app>.*`). For tests, the conformance kit and demos; never production data. Its rules are copied from the engine and listed in its lib doc, with what it does not imitate. |

## How slice 1 works on today's store (no server change)

- get is op 1, insert is Put with `if_version = 0`, update is Patch (op 7) of the changed fields plus `_v`, replace is
  Put, remove is Delete, prefix/all walk op 4 List (cursor = seq).
- Index queries and every watch come from one live mirror per collection: List with tombstones from seq 0, then the
  Watch stream (op 6) from the last seq, one stream shared by all watchers. Own writes go into the mirror from the
  store's answer at once. Over `mirror_limit` rows (default 5,000) index queries answer `NeedsServerIndex`.
- `open` resolves `CE_DB_STORE`/`CE_DB_CAP` (a dev store), else this machine's state.store by name and the replicas
  net.find and `~/.config/ce-state/replicas` name, each with its wallet grant. A dial waits at most 2 s; a store
  that is not there is `Unavailable { tried }`. A missing schema is `SchemaMissing` at once when the grant cannot
  register it.
- Errors are `DbError`: `NotFound`, `Conflict { held_version }`, `Invalid { field, why }`, `Refused { need }`,
  `TooLarge { bytes, limit }`, `Throttled`, `SchemaMissing`, `SchemaBehind`, `Unavailable { tried }`,
  `NeedsServerIndex`, `Store(text)` for anything else. The store's text is read in one place (`classify`).

## Encoding (the contract with every other language)

Sorted keys, no whitespace, `None` absent (a field cleared by `update` is a JSON null, which reads back as `None`),
`u64`/`i64` decimal strings, bytes lowercase hex, `Millis` integer ms, `Id` 26-char ULID, `_v` the writer's schema
version, an integral `f64` written as its digits (`100`, as JavaScript writes it). Unknown fields are kept
(`Stored::extra`) and written back by `replace_stored`. Known gap: non-integral floats below 1e-6 or above 1e21 are
written differently by ryu and by JavaScript; pinned values in db-conformance avoid them.

## Build and test

```
cargo test            # core, derive, the dev store rules, and the ce-db surface against a dev store over ce-wire
```

Not verified live: `open()` against a production replica (it needs a granted `state.<app>` app; state-store-cell
AGENTS.md allows live tests on `repl-probe/ping` only).
