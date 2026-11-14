//! A schema change breaks the code written against the old schema at compile time, not at run time
//! (tests/schema-change/: the v2 caller compiles on v2, and fails on v3 with the field removed or its type changed).
#[test]
fn a_schema_change_is_a_compile_error() {
    let t = trybuild::TestCases::new();
    t.pass("tests/schema-change/v2_compiles.rs");
    t.compile_fail("tests/schema-change/field_removed.rs");
    t.compile_fail("tests/schema-change/type_changed.rs");
}
