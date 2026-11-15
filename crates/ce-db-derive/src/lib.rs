//! `#[derive(Record)]`: the struct is the schema (repo:state-store-cell/docs/DB-SDK-DESIGN.md section 1.1). It
//! implements `ce_db_core::Record` and writes `<Type>Indexes`, one `by_<field>` per declared index. What slice 1
//! does not do yet is refused here, at compile time, with the reason, never quietly downgraded.
use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TS2};
use quote::{format_ident, quote};
use syn::{parse_macro_input, Data, DeriveInput, Error, Expr, Fields, Lit, Type};

#[proc_macro_derive(Record, attributes(record, db))]
pub fn derive_record(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand(&input) {
        Ok(t) => t.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// Who owns the collection: `cell = Contract` (a type implementing `ce_wire::Api`, so a typo does not compile),
/// `cell = "name"`, or the deprecated `app = "name"`.
enum Owner {
    Name(String),
    Contract(syn::Path),
}

#[derive(Default)]
struct Coll {
    owner: Option<Owner>,
    /// span of the `app` key, when the old word was used: the derive emits a deprecated item there
    app_alias: Option<Span>,
    collection: Option<String>,
    version: u32,
    consistency: String,
    hash_keys: bool,
    per_person: bool,
    replicas: Vec<String>,
    mirror_limit: Option<usize>,
}

struct FieldSpec {
    ident: syn::Ident,
    ty: Type,
    key: bool,
    index: Option<bool>, // Some(ordered)
    max: Option<usize>,
    since: u32,
    default: Option<Expr>,
    secret: bool,
    doc: String,
}

fn doc_of(attrs: &[syn::Attribute]) -> String {
    attrs
        .iter()
        .filter(|a| a.path().is_ident("doc"))
        .filter_map(|a| match &a.meta {
            syn::Meta::NameValue(nv) => match &nv.value {
                Expr::Lit(l) => match &l.lit {
                    Lit::Str(s) => Some(s.value().trim().to_string()),
                    _ => None,
                },
                _ => None,
            },
            _ => None,
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn lit_int<T: std::str::FromStr>(e: &Expr) -> syn::Result<T>
where
    T::Err: std::fmt::Display,
{
    match e {
        Expr::Lit(l) => match &l.lit {
            Lit::Int(i) => i.base10_parse(),
            _ => Err(Error::new_spanned(e, "expected an integer")),
        },
        _ => Err(Error::new_spanned(e, "expected an integer")),
    }
}

/// `#[record(..)]`, or its old spelling `#[db(..)]`.
fn is_ours(a: &syn::Attribute) -> bool {
    a.path().is_ident("record") || a.path().is_ident("db")
}

fn attr_name(a: &syn::Attribute) -> &'static str {
    if a.path().is_ident("db") { "db" } else { "record" }
}

fn lit_str(e: &Expr) -> syn::Result<String> {
    match e {
        Expr::Lit(l) => match &l.lit {
            Lit::Str(s) => Ok(s.value()),
            _ => Err(Error::new_spanned(e, "expected a string")),
        },
        Expr::Path(p) if p.path.get_ident().is_some() => Ok(p.path.get_ident().map(|i| i.to_string()).unwrap_or_default()),
        _ => Err(Error::new_spanned(e, "expected a string")),
    }
}

/// `T` of `Option<T>`, else None.
fn option_inner(t: &Type) -> Option<&Type> {
    let Type::Path(p) = t else { return None };
    let seg = p.path.segments.last()?;
    if seg.ident != "Option" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(a) = &seg.arguments else { return None };
    match a.args.first()? {
        syn::GenericArgument::Type(t) => Some(t),
        _ => None,
    }
}

fn last_ident(t: &Type) -> String {
    match t {
        Type::Path(p) => p.path.segments.last().map(|s| s.ident.to_string()).unwrap_or_default(),
        _ => String::new(),
    }
}

fn name_ok(s: &str) -> bool {
    !s.is_empty() && s.len() <= 64 && s.bytes().all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
}

fn expand(input: &DeriveInput) -> syn::Result<TS2> {
    let name = &input.ident;
    let mut c = Coll { version: 1, consistency: "lww".into(), ..Default::default() };
    for a in input.attrs.iter().filter(|a| is_ours(a)) {
        let attr = attr_name(a);
        a.parse_nested_meta(|m| {
            let key = m.path.get_ident().map(|i| i.to_string()).unwrap_or_default();
            match key.as_str() {
                "cell" | "app" => {
                    if c.owner.is_some() {
                        return Err(m.error("the owning cell is given once: #[record(cell = ...)]"));
                    }
                    let e: Expr = m.value()?.parse()?;
                    c.owner = Some(match (&e, key.as_str()) {
                        (Expr::Path(p), "cell") if p.qself.is_none() => Owner::Contract(p.path.clone()),
                        _ => Owner::Name(lit_str(&e)?),
                    });
                    if key == "app" {
                        c.app_alias = Some(m.path.get_ident().map(|i| i.span()).unwrap_or_else(Span::call_site));
                    }
                }
                "collection" => c.collection = Some(lit_str(&m.value()?.parse()?)?),
                "version" => c.version = lit_int(&m.value()?.parse()?)?,
                "consistency" => c.consistency = lit_str(&m.value()?.parse()?)?,
                "mirror_limit" => c.mirror_limit = Some(lit_int(&m.value()?.parse()?)?),
                "hash_keys" => c.hash_keys = true,
                "per_person" => c.per_person = true,
                "replicas" => {
                    let e: Expr = m.value()?.parse()?;
                    let Expr::Array(a) = &e else { return Err(Error::new_spanned(&e, "replicas = [\"node\", ...]")) };
                    for x in &a.elems {
                        c.replicas.push(lit_str(x)?);
                    }
                }
                other => return Err(m.error(format!("unknown #[{attr}] key `{other}` (cell, collection, version, consistency, hash_keys, per_person, replicas, mirror_limit)"))),
            }
            Ok(())
        })?;
    }
    let span = Span::call_site();
    let owner = c.owner.take().ok_or_else(|| Error::new(span, "#[record(cell = Contract)] is required: the cell that owns this collection (cell = \"name\" also works)"))?;
    let collection = c.collection.clone().ok_or_else(|| Error::new(span, "#[record(collection = \"...\")] is required"))?;
    if let Owner::Name(v) = &owner {
        if !name_ok(v) {
            return Err(Error::new(span, format!("cell {v:?}: 1..=64 of [A-Za-z0-9._-]")));
        }
    }
    if !name_ok(&collection) {
        return Err(Error::new(span, format!("collection {collection:?}: 1..=64 of [A-Za-z0-9._-]")));
    }
    let (app, app_label) = match &owner {
        Owner::Name(v) => (quote!(#v), v.clone()),
        Owner::Contract(p) => (quote!(<#p as ::ce_db_core::CellApi>::NAME), quote!(#p).to_string().replace(' ', "")),
    };
    // A proc macro cannot warn on stable Rust; using a #[deprecated] item it emits at the `app` key makes rustc do it.
    let alias_warning = c.app_alias.map(|sp| {
        let item = format_ident!("app_is_the_old_word_write_record_cell", span = sp);
        quote::quote_spanned! {sp=>
            const _: () = {
                #[deprecated(note = "write #[record(cell = Contract)]; `app` is the old word for the owning cell")]
                #[allow(non_upper_case_globals)]
                const #item: () = ();
                #item
            };
        }
    });
    match c.consistency.as_str() {
        "lww" => {}
        "crdt" => return Err(Error::new(span, "consistency = crdt is slice 4 of DB-SDK-DESIGN.md (Counter, Set, Text as per-writer rows); not built yet")),
        "ledger" => {
            return Err(Error::new(
                span,
                "consistency = ledger needs state.consensus, which has no source yet, and RAFT-BRIEF, which is paused; unpausing it is a decision for the maintainers (DB-SDK-DESIGN.md 4.3). Use lww, or Append with a quorum for an append-only log",
            ))
        }
        o => return Err(Error::new(span, format!("consistency = {o}: lww, crdt or ledger"))),
    }
    if c.version == 0 {
        return Err(Error::new(span, "version starts at 1"));
    }

    let Data::Struct(ds) = &input.data else { return Err(Error::new(span, "Record is for a struct with named fields")) };
    let Fields::Named(named) = &ds.fields else { return Err(Error::new(span, "Record is for a struct with named fields")) };
    let mut specs = Vec::new();
    for f in &named.named {
        let ident = f.ident.clone().ok_or_else(|| Error::new_spanned(f, "named field"))?;
        let mut s = FieldSpec { ident: ident.clone(), ty: f.ty.clone(), key: false, index: None, max: None, since: 1, default: None, secret: false, doc: doc_of(&f.attrs) };
        let mut unique = false;
        let mut ordered = false;
        let mut blob = false;
        for a in f.attrs.iter().filter(|a| is_ours(a)) {
            a.parse_nested_meta(|m| {
                let key = m.path.get_ident().map(|i| i.to_string()).unwrap_or_default();
                match key.as_str() {
                    "key" => s.key = true,
                    "index" => s.index = Some(false),
                    "ordered" => ordered = true,
                    "unique" => unique = true,
                    "secret" => s.secret = true,
                    "blob" => blob = true,
                    "max" => s.max = Some(lit_int(&m.value()?.parse()?)?),
                    "since" => s.since = lit_int(&m.value()?.parse()?)?,
                    "default" => s.default = Some(m.value()?.parse()?),
                    other => return Err(m.error(format!("unknown field key `{other}` (key, index, ordered, unique, max, since, default, secret, blob)"))),
                }
                Ok(())
            })?;
        }
        if ordered {
            if s.index.is_none() {
                return Err(Error::new_spanned(&ident, "`ordered` goes with `index`: #[record(index, ordered)]"));
            }
            s.index = Some(true);
        }
        if unique {
            return Err(Error::new_spanned(&ident, "`unique` needs the engine's index (slice 2, op 13): two clients could both pass a check made from the mirror"));
        }
        if blob {
            return Err(Error::new_spanned(&ident, "`blob` fields need the state.blob cell (slice 3); not built yet"));
        }
        if s.secret && s.index.is_some() {
            return Err(Error::new_spanned(&ident, "a `secret` field is never indexed"));
        }
        if s.since > c.version {
            return Err(Error::new_spanned(&ident, format!("since = {} is newer than the collection's version {}", s.since, c.version)));
        }
        if s.since > 1 && option_inner(&s.ty).is_none() && s.default.is_none() {
            return Err(Error::new_spanned(&ident, "a field added after version 1 is an Option or has a default, so older rows still decode"));
        }
        if s.key && option_inner(&s.ty).is_some() {
            return Err(Error::new_spanned(&ident, "the key is never optional"));
        }
        if s.key && !["Id", "String"].contains(&last_ident(&s.ty).as_str()) {
            return Err(Error::new_spanned(&ident, "the key field is an Id (ULID) or a String"));
        }
        specs.push(s);
    }
    let keys: Vec<&FieldSpec> = specs.iter().filter(|s| s.key).collect();
    if keys.len() != 1 {
        return Err(Error::new(span, "exactly one field carries #[record(key)]"));
    }
    if specs.iter().any(|s| s.ident == "_v") {
        return Err(Error::new(span, "`_v` is the writer's schema version, kept by ce-db"));
    }
    let key_ident = &keys[0].ident;

    let type_name = name.to_string();
    let version = c.version;
    let consistency = &c.consistency;
    let hash_keys = c.hash_keys;
    let per_person = c.per_person;
    let replicas = &c.replicas;
    let mirror_limit = c.mirror_limit.map_or(quote!(::ce_db_core::MIRROR_LIMIT), |n| quote!(#n));
    let doc = doc_of(&input.attrs);

    let field_metas = specs.iter().map(|s| {
        let n = s.ident.to_string();
        let ty = &s.ty;
        let key = s.key;
        let index = match s.index {
            None => quote!(None),
            Some(false) => quote!(Some(::ce_db_core::IndexKind::Eq)),
            Some(true) => quote!(Some(::ce_db_core::IndexKind::Ordered)),
        };
        let max = s.max.map_or(quote!(None), |m| quote!(Some(#m)));
        let since = s.since;
        let default = match &s.default {
            None => quote!(None),
            Some(e) => quote!({ let d: #ty = #e; ::ce_db_core::Field::to_json(&d) }),
        };
        let secret = s.secret;
        let fdoc = &s.doc;
        quote! {
            ::ce_db_core::FieldMeta { name: #n, ty: <#ty as ::ce_db_core::Field>::ty(), key: #key, index: #index, max: #max, since: #since, default: #default, secret: #secret, doc: #fdoc }
        }
    });
    let encodes = specs.iter().map(|s| {
        let i = &s.ident;
        let n = i.to_string();
        quote! { if let Some(v) = ::ce_db_core::Field::to_json(&self.#i) { o.insert(#n.to_string(), v); } }
    });
    let decodes = specs.iter().map(|s| {
        let i = &s.ident;
        let n = i.to_string();
        let ty = &s.ty;
        let d = match &s.default {
            None => quote!(None),
            Some(e) => quote!(Some(|| -> #ty { #e })),
        };
        quote! { #i: ::ce_db_core::field_or::<#ty>(o, #n, #d)? }
    });
    let checks = specs.iter().filter_map(|s| {
        let i = &s.ident;
        let n = i.to_string();
        s.max.map(|m| quote! { ::ce_db_core::check_max(&self.#i, #n, #m)?; })
    });
    let idx_arms = specs.iter().filter(|s| s.index.is_some()).map(|s| {
        let i = &s.ident;
        let n = i.to_string();
        quote! { #n => ::ce_db_core::Field::index_val(&self.#i), }
    });

    let trait_name = format_ident!("{}Indexes", name);
    let mut decls = Vec::new();
    let mut impls = Vec::new();
    for s in specs.iter().filter(|s| s.index.is_some()) {
        let n = s.ident.to_string();
        let m = format_ident!("by_{}", s.ident);
        let arg = option_inner(&s.ty).unwrap_or(&s.ty);
        let fdoc = format!("Rows whose `{n}` {}.", if s.index == Some(true) { "is in the range (ordered index)" } else { "equals `v` (equality index)" });
        if s.index == Some(true) {
            decls.push(quote! { #[doc = #fdoc] fn #m(&self, r: impl ::std::ops::RangeBounds<#arg>) -> Self::Q; });
            impls.push(quote! {
                fn #m(&self, r: impl ::std::ops::RangeBounds<#arg>) -> Self::Q {
                    self.index_range(#n, ::ce_db_core::bound_of(r.start_bound()), ::ce_db_core::bound_of(r.end_bound()))
                }
            });
        } else {
            decls.push(quote! { #[doc = #fdoc] fn #m(&self, v: impl Into<#arg>) -> Self::Q; });
            impls.push(quote! {
                fn #m(&self, v: impl Into<#arg>) -> Self::Q {
                    let v: #arg = v.into();
                    self.index_eq(#n, ::ce_db_core::Field::index_val(&v))
                }
            });
        }
    }
    let trait_doc = format!("The declared indexes of `{type_name}` ({app_label}/{collection}): `use` this to call `by_<field>` on a collection.");

    Ok(quote! {
        #alias_warning

        impl ::ce_db_core::Record for #name {
            fn meta() -> &'static ::ce_db_core::Meta {
                static META: ::std::sync::OnceLock<::ce_db_core::Meta> = ::std::sync::OnceLock::new();
                ::ce_db_core::meta_cell(&META, || ::ce_db_core::Meta {
                    type_name: #type_name,
                    app: #app,
                    collection: #collection,
                    version: #version,
                    consistency: #consistency,
                    fields: vec![#(#field_metas),*],
                    hash_keys: #hash_keys,
                    per_person: #per_person,
                    replicas: vec![#(#replicas),*],
                    mirror_limit: #mirror_limit,
                    doc: #doc,
                })
            }
            fn key(&self) -> String {
                ::ce_db_core::KeyField::key_string(&self.#key_ident)
            }
            fn encode_fields(&self) -> ::ce_db_core::Obj {
                let mut o = ::ce_db_core::Obj::new();
                #(#encodes)*
                o
            }
            fn decode_fields(o: &::ce_db_core::Obj) -> Result<Self, ::ce_db_core::DbError> {
                Ok(#name { #(#decodes),* })
            }
            fn index_val(&self, field: &str) -> ::ce_db_core::IndexVal {
                match field {
                    #(#idx_arms)*
                    _ => ::ce_db_core::IndexVal::Null,
                }
            }
            fn check(&self) -> Result<(), ::ce_db_core::DbError> {
                #(#checks)*
                Ok(())
            }
        }

        impl #name {
            /// The generator's input: this collection's schema as JSON (`ce-wire-gen --records` compiles a probe that
            /// prints it, so the generated languages and this derive can never disagree).
            #[doc(hidden)]
            pub fn __ce_db_records_ir() -> String {
                let m = <#name as ::ce_db_core::Record>::meta();
                ::ce_db_core::serde_json::json!({ "meta": m.to_json(), "entry": m.schema_entry() }).to_string()
            }
        }

        #[doc = #trait_doc]
        pub trait #trait_name {
            type Q;
            #(#decls)*
        }

        impl<C: ::ce_db_core::IndexQuery<#name>> #trait_name for C {
            type Q = <C as ::ce_db_core::IndexQuery<#name>>::Q;
            #(#impls)*
        }
    })
}
