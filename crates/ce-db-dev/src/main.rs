//! `ce-db-dev-store [--listen 127.0.0.1:7811] --grant CAP=right,right ... [--schema FILE]...`: a dev state.store on
//! loopback (see the lib). Prints `ce-db-dev-store listening <addr>` once it accepts, then serves until killed.
fn main() {
    let mut listen = "127.0.0.1:7811".to_string();
    let mut grants = ce_db_dev::Grants::default();
    let mut schemas = Vec::new();
    let mut it = std::env::args().skip(1);
    let fail = |m: String| -> ! {
        eprintln!("ce-db-dev-store: {m}");
        std::process::exit(2)
    };
    while let Some(a) = it.next() {
        match a.as_str() {
            "--listen" => listen = it.next().unwrap_or_else(|| fail("--listen ADDR".into())),
            "--grant" => grants.0.push(ce_db_dev::Grants::parse_one(&it.next().unwrap_or_else(|| fail("--grant CAP=rights".into()))).unwrap_or_else(|e| fail(e))),
            "--schema" => schemas.push(it.next().unwrap_or_else(|| fail("--schema FILE".into()))),
            o => fail(format!("unknown argument {o}; usage: ce-db-dev-store [--listen 127.0.0.1:7811] --grant CAP=state.<app>.* [--schema schemas/<app>.json]")),
        }
    }
    if grants.0.is_empty() {
        fail("no --grant: every call would be refused".into());
    }
    let s = ce_db_dev::serve(&listen, grants).unwrap_or_else(|e| fail(e.to_string()));
    for f in schemas {
        let text = std::fs::read_to_string(&f).unwrap_or_else(|e| fail(format!("{f}: {e}")));
        let v: serde_json::Value = serde_json::from_str(&text).unwrap_or_else(|e| fail(format!("{f}: {e}")));
        let app = v.get("app").and_then(|a| a.as_str()).unwrap_or_else(|| fail(format!("{f}: no app"))).to_string();
        s.put_schema(&app, v);
    }
    println!("ce-db-dev-store listening {}", s.addr);
    loop {
        std::thread::park();
    }
}
