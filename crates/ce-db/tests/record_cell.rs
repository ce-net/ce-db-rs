//! `#[record(cell = Contract)]`: the owning cell is the contract type, so its name comes from `Api::NAME` and a typo
//! does not compile. `#[db(app = "...")]` still works and warns (tests/record-cell/app_alias_warns.rs).
use ce_db::prelude::*;

ce_wire::api! {
    api PartsShop = "parts-shop", version 1;
    types {}
    ops {}
}

#[derive(Record, Clone, Debug, PartialEq)]
#[record(cell = PartsShop, collection = "parts", version = 1)]
pub struct Part {
    #[record(key)]
    pub id: String,
    #[record(index)]
    pub bin: String,
}

/// The old spelling, kept as an alias: same Meta.
#[allow(deprecated)]
mod old {
    use ce_db::prelude::*;
    #[derive(Record, Clone, Debug, PartialEq)]
    #[db(app = "parts-shop", collection = "parts", version = 1)]
    pub struct Part {
        #[db(key)]
        pub id: String,
        #[db(index)]
        pub bin: String,
    }
}

#[test]
fn the_contract_names_the_cell() {
    let m = <Part as Record>::meta();
    assert_eq!(m.app, "parts-shop");
    assert_eq!(m.collection, "parts");
    let o = <old::Part as Record>::meta();
    assert_eq!((o.app, o.collection, o.version), (m.app, m.collection, m.version));
    assert_eq!(o.schema_entry(), m.schema_entry());
}

#[test]
fn the_old_word_warns_and_bad_owners_do_not_compile() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/record-cell/app_alias_warns.rs");
    t.compile_fail("tests/record-cell/unknown_contract.rs");
}
