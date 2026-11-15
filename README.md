# ce-db-rs: typed state.store collections for Rust

One Rust struct is the schema; insert, get, update, query by a declared index and watch live, with typed errors.
TypeScript is `repo:ce-db-ts` (the same surface), the cross-language contract is `repo:db-conformance`, a worked
app is `repo:parts-demo`. Design: `repo:state-store-cell/docs/DB-SDK-DESIGN.md`, slice 1.

## Quickstart (a dev store, two terminals)

```sh
cargo run -p ce-db-dev --bin ce-db-dev-store -- --grant dev='state.quickstart.*'        # loopback, 127.0.0.1:7811
CE_DB_STORE=127.0.0.1:7811 CE_DB_CAP=dev cargo run -p ce-db-examples --bin quickstart
```

`examples/quickstart.rs`, the whole program:

```rust
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
```

```
open tasks: 1
Synced
Added(Task { id: Id("01K..."), title: "buy milk", done: false })
```

In your own crate: `ce-db = { git = "https://github.com/ce-net/ce-db-rs" }` (and `ce-db-core` for a schema
crate of its own). On a node with a state.store, drop `CE_DB_STORE`: `open(app)` finds this machine's replica and the
wallet grant for the app (`state.<app>.*`; registering a schema needs `state.<app>.admin`).

A schema change is a compile error in the code that used the old shape, never a surprise at run time:
`crates/ce-db/tests/schema-change/` removes a field and changes a type, and `cargo test` checks that the old caller
fails to build with `no field named bin`, `no method named by_bin`, `-= cannot be applied to String`.

## The surface

```rust
use ce_db::prelude::*;                       // a schema crate depends on ce-db-core only (it names ::ce_db_core)

/// A part in a bin.
#[derive(Record, Clone, Debug)]
#[record(cell = "parts-demo", collection = "parts", version = 2)]
pub struct Part {
    #[record(key)]            pub id: Id,                 // ULID made by the client
    #[record(max = 120)]      pub name: String,
    #[record(index)]          pub bin: String,            // equality index
    #[record(index, ordered)] pub qty: u32,               // range index
    #[record(since = 2)]      pub supplier: Option<String>,
}

let db = ce_db::open("parts-demo")?;                  // or open_at(app, "127.0.0.1:7811", cap) for a dev store
let parts = db.collection::<Part>()?;                 // reads the registered schema, registers it if missing/older
let id = parts.insert(&part)?;                        // Conflict if the key exists
let one: Option<Part> = parts.get(&id)?;
parts.update(&id, |p| p.qty -= 1)?;                   // Patch of the changed fields only
let in_a4 = parts.by_bin("A4").limit(50).fetch()?;    // use PartIndexes: by_<field> per declared index
let low = parts.by_qty(..20).desc().fetch()?;
for ch in parts.by_bin("A4").watch()? { /* Added(T) | Changed{old,new} | Removed(key) | Synced */ }
let links = parts.watch()?.link_changes();            // Live, Offline(why), Resyncing, Live: for a UI badge
```

## Crates

| crate | what |
|---|---|
| `ce-db-core` | `Record` trait, `Field` codecs (`String`, `bool`, `u8..u32`/`i8..i32` as numbers, `u64`/`i64` as decimal strings, `f64`, `Id`, `Millis`, `Hex`, `Vec<T>`, `Option<T>`), canonical encoding, `DbError`, `Meta` (the schema the generator reads). No network. |
| `ce-db-derive` | `#[derive(Record)]`: struct keys `app collection version consistency hash_keys per_person replicas mirror_limit`, field keys `key index ordered max since default secret`. Refused at compile time with the reason: `unique` (needs the engine index, slice 2), `blob` (state.blob, slice 3), `consistency = crdt` (slice 4), `consistency = ledger` (state.consensus and RAFT-BRIEF, a lead decision). |
| `ce-db` | the runtime: `open`, `open_at`, `open_with`, `Db::collection`, `Collection` (insert, get, get_stored, update, replace, replace_stored, remove, prefix, all, by_<index>, watch, watch_key, stats, link), `Query` (limit, after, desc, fetch, fetch_page, fetch_with_rejects, count, watch). |
| `ce-db-dev` | a dev state.store: the typed v2 API in memory on loopback with dev grants (`ce-db-dev-store --grant CAP=state.<app>.* [--data FILE]`; `--data` keeps rows and seqs across a restart, as a replica does). For tests, the conformance kit and demos; never production data. Its rules are copied from the engine and listed in its lib doc, with what it does not imitate. |
| `examples` | `quickstart`: define, insert, query, watch (above); its test runs it against a dev store twice. |

## How slice 1 works on today's store (no server change)

- get is op 1, insert is Put with `if_version = 0`, update is Patch (op 7) of the changed fields plus `_v`, replace is
  Put, remove is Delete, prefix/all walk op 4 List (cursor = seq).
- Index queries and every watch come from one live mirror per collection: List with tombstones from seq 0, then the
  Watch stream (op 6) from the last seq, one stream shared by all watchers. Own writes go into the mirror from the
  store's answer at once. When the stream breaks (store restarted, network, another replica) the link goes
  `Offline`, then `Resyncing` while the collection is listed again, and the watchers get only the difference: the
  changes they missed, no duplicate (db-conformance D11 kills a dev store mid-watch to prove it). Over `mirror_limit` rows (default 5,000) index queries answer `NeedsServerIndex`.
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
cargo test            # core, derive, the dev store rules, the ce-db surface over ce-wire, schema-change compile-fail, quickstart
```

CI: `.github/workflows/ci.yml` runs `cargo test --workspace` on every push and pull request, with rustc pinned
for the compile-fail snapshots.

Dependencies not yet public: `ce-wire` and `ce-gate` (the ce-net wire and gate crates) and `state-store-api` are
fetched from repositories that are not open yet, so a build from a plain clone needs read access to them. CI reads
them with read-only deploy keys; pull requests from forks run without those keys until the crates are published.

Not verified live: `open()` against a production replica; the tests run against the dev store.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Licensed under the Apache License, Version 2.0 ([LICENSE](LICENSE)). See [NOTICE](NOTICE).
