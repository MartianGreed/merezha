//! CT-C1 model acceptance: the design example, all-at-once reporting with
//! TOML paths, `access`, `[[migrations]]`, the canonical form and the
//! fingerprint, and an accepting and a rejecting case for every model rule.
mod common;

use common::*;
use merezha_content::{Access, Cardinality, ContentModel, FieldKind, Kind, ModelOptions, Scope};
use std::path::Path;

fn design_example() -> String {
    doc_block("content.md", "### Example", "toml")
}

#[test]
fn the_section_2_example_parses_as_written() {
    let model = parse(&design_example()).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(model.version(), 3);
    let locales: Vec<_> = model
        .locales()
        .iter()
        .map(|l| (l.code.as_str(), l.default, l.fallback.as_deref()))
        .collect();
    assert_eq!(
        locales,
        [
            ("en", true, None),
            ("fr", false, Some("en")),
            ("fr-BE", false, Some("fr"))
        ]
    );
    assert_eq!(model.default_locale().code, "en");
    let names: Vec<_> = model.types().keys().map(String::as_str).collect();
    assert_eq!(
        names,
        ["article", "author", "partner_brief", "site_settings"]
    );
    let article = model.content_type("article").unwrap();
    assert_eq!(
        (article.kind, article.scope, article.localized),
        (Kind::Collection, Scope::Application, true)
    );
    assert_eq!(article.access, Access::Public);
    let order: Vec<_> = article.fields.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(
        order,
        [
            "title",
            "slug",
            "category",
            "published_on",
            "author",
            "tags",
            "body",
            "sections",
            "seo",
            "reading_minutes"
        ],
        "field order is model order"
    );
    let slug = article.field("slug").unwrap();
    assert!(slug.unique, "slugs are unique by default");
    let author = article.field("author").unwrap();
    assert!(!author.localized && author.filterable);
    let FieldKind::Reference(reference) = &author.kind else {
        panic!("author is a reference")
    };
    assert_eq!(reference.cardinality, Cardinality::One);
    assert!(
        article.field("title").unwrap().filterable,
        "sortable implies filterable"
    );
    assert_eq!(
        model.content_type("site_settings").unwrap().kind,
        Kind::Singleton
    );
    assert_eq!(
        model.content_type("partner_brief").unwrap().access,
        Access::Permission("content.delivery.read".into())
    );
    assert_eq!(model.components().len(), 2);
    assert_eq!(model.fingerprint().len(), 64);
}

#[test]
fn the_example_with_array_tables_is_the_same_model() {
    let inline = parse(&design_example()).unwrap();
    // `[[types.x.fields]]` array tables parse to the same model.
    let tables = r#"
version = 3
locales = [
  { code = "en", default = true },
  { code = "fr", fallback = "en" },
  { code = "fr-BE", fallback = "fr" },
]

[types.site_settings]
kind = "singleton"
access = "public"

[[types.site_settings.fields]]
name = "site_name"
type = "string"
required = true

[[types.site_settings.fields]]
name = "price_floor"
type = "decimal"
precision = 10
scale = 2
"#;
    let model = parse(tables).unwrap();
    let canonical = |m: &ContentModel| -> serde_json::Value {
        serde_json::from_str(m.canonical_json()).unwrap()
    };
    assert_eq!(
        canonical(&model)["types"]["site_settings"],
        canonical(&inline)["types"]["site_settings"]
    );
}

#[test]
fn a_broken_model_reports_every_problem_with_its_toml_path() {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let source = std::fs::read_to_string(fixtures.join("broken.toml")).unwrap();
    let expected = std::fs::read_to_string(fixtures.join("broken.expected")).unwrap();
    let error = ContentModel::parse(&source, &options(false)).unwrap_err();
    assert_eq!(error.failure().code, "content_model_invalid");
    let rendered = format!("{error}\n");
    if std::env::var_os("MEREZHA_BLESS").is_some() {
        std::fs::write(fixtures.join("broken.expected"), &rendered).unwrap();
    }
    assert_eq!(
        rendered, expected,
        "golden output; MEREZHA_BLESS=1 rewrites it"
    );
    assert!(error.issues.len() > 20, "every problem at once");
    assert!(
        error
            .issues
            .iter()
            .all(|i| i.to_string().starts_with("content_model_invalid: "))
    );
}

#[test]
fn a_toml_syntax_error_reports_its_position_without_the_text() {
    let error =
        parse("version = 1\nlocales = [\n  { code = \"en\" default = true },\n]\n").unwrap_err();
    assert_eq!(error.codes(), [("", "toml_syntax")]);
    assert_eq!(
        error.to_string(),
        "content_model_invalid: the model file is not valid TOML (line 3, column 17)"
    );
    let big = "#".repeat(merezha_content::MAX_MODEL_BYTES + 1);
    assert_eq!(parse(&big).unwrap_err().codes(), [("", "model_too_large")]);
}

#[test]
fn access_is_required_and_has_three_forms() {
    for (access, code) in [
        ("", "missing_required"),
        ("access = \"private\"", "invalid_access"),
        (
            "access = { permission = \"billing.read\" }",
            "unknown_permission",
        ),
        ("access = { permission = \"bad name\" }", "invalid_access"),
        (
            "access = { permission = \"partners.read\", extra = 1 }",
            "invalid_access",
        ),
        ("access = true", "invalid_access"),
    ] {
        let source = document(&format!(
            "[types.brief]\n{access}\nfields = [{{ name = \"title\", type = \"string\" }}]\n"
        ));
        assert_eq!(
            issues(&source),
            pairs(&[("types.brief.access", code)]),
            "{access:?}"
        );
    }
    for (access, expected) in [
        ("\"public\"", Access::Public),
        ("\"authenticated\"", Access::Authenticated),
        (
            "{ permission = \"partners.read\" }",
            Access::Permission("partners.read".into()),
        ),
        (
            "{ permission = \"content.delivery.read\" }",
            Access::Permission("content.delivery.read".into()),
        ),
    ] {
        let source = document(&format!(
            "[types.brief]\naccess = {access}\nfields = [{{ name = \"title\", type = \"string\" }}]\n"
        ));
        let model = parse(&source).unwrap_or_else(|e| panic!("{access}: {e}"));
        assert_eq!(model.content_type("brief").unwrap().access, expected);
    }
}

#[test]
fn a_migrations_block_is_content_model_invalid() {
    let source = document("[[migrations]]\nto = 2\nsteps = []\n");
    let error = parse(&source).unwrap_err();
    assert_eq!(error.failure().code, "content_model_invalid");
    assert_eq!(error.codes(), [("migrations", "migrations_unsupported")]);
}

fn fingerprint(source: &str) -> String {
    parse(source)
        .unwrap_or_else(|e| panic!("{e}\n{source}"))
        .fingerprint()
        .to_owned()
}

const BASE: &str = r#"version = 2
locales = [{ code = "en", default = true }, { code = "fr" }]

[types.page]
access = "public"
title_field = "title"
fields = [
  { name = "title", type = "string", required = true },
  { name = "slug", type = "slug", from = "title" },
  { name = "kind", type = "enum", values = ["a", "b"], sortable = true },
]
"#;

#[test]
fn the_fingerprint_ignores_key_order_whitespace_and_spelled_out_defaults() {
    let base = fingerprint(BASE);
    let reordered = r#"
# Same model, keys reordered, whitespace and comments added, defaults spelled out.
locales = [
  { default = true, code = "en" },
  { fallback = "en", code = "fr", default = false },
]
version = 2

[types.page]
fields = [
  { type = "string", name = "title", required = true, localized = true, max_length = 255 },
  { from = "title", type = "slug", name = "slug", unique = true, max_length = 96, required = false },
  { name = "kind", sortable = true, filterable = true, values = ["a", "b"], type = "enum" },
]
title_field   =   "title"
access = "public"
kind = "collection"
scope = "application"
localized = true
"#;
    assert_eq!(fingerprint(reordered), base);
}

#[test]
fn the_fingerprint_changes_on_every_semantic_change() {
    let base = fingerprint(BASE);
    let mut seen = std::collections::BTreeSet::from([base.clone()]);
    for (from, to) in [
        ("version = 2", "version = 3"),
        ("required = true }", "required = false }"),
        (
            "{ name = \"title\", type = \"string\", required = true }",
            "{ name = \"title\", type = \"string\", required = true, max_length = 254 }",
        ),
        ("values = [\"a\", \"b\"]", "values = [\"a\", \"b\", \"c\"]"),
        ("values = [\"a\", \"b\"]", "values = [\"b\", \"a\"]"),
        ("access = \"public\"", "access = \"authenticated\""),
        ("title_field = \"title\"\n", ""),
        ("[types.page]\n", "[types.page]\ndescription = \"Pages\"\n"),
        ("[types.page]\n", "[types.page]\nlocalized = false\n"),
        ("[types.page]\n", "[types.page]\nkind = \"singleton\"\n"),
        ("{ code = \"fr\" }", "{ code = \"fr\" }, { code = \"de\" }"),
        ("sortable = true", "filterable = true"),
        ("from = \"title\"", "from = \"title\", max_length = 50"),
        ("from = \"title\" }", "from = \"title\", unique = false }"),
        (
            "required = true },\n  { name = \"slug\"",
            "required = true, localized = false },\n  { name = \"slug\"",
        ),
    ] {
        assert!(BASE.contains(from), "{from}");
        let changed = BASE.replacen(from, to, 1);
        let print = fingerprint(&changed);
        assert!(seen.insert(print), "{from:?} -> {to:?} kept a fingerprint");
    }
    // Field order is semantic: it is the order of API responses.
    let swapped = BASE.replace(
        "  { name = \"title\", type = \"string\", required = true },\n  { name = \"slug\", type = \"slug\", from = \"title\" },\n",
        "  { name = \"slug\", type = \"slug\", from = \"title\" },\n  { name = \"title\", type = \"string\", required = true },\n",
    );
    assert_ne!(fingerprint(&swapped), base);
}

#[test]
fn the_canonical_form_is_sorted_compact_json_with_defaults() {
    let model = parse(
        r#"version = 1
locales = [{ code = "en", default = true }]
[types.note]
access = { permission = "tasks.read" }
fields = [{ name = "body", type = "text" }]
"#,
    )
    .unwrap();
    assert_eq!(
        model.canonical_json(),
        concat!(
            r#"{"components":{},"locales":[{"code":"en","default":true}],"types":{"note":{"access":{"permission":"tasks.read"},"#,
            r#""fields":[{"filterable":false,"localized":true,"max_length":10000,"name":"body","required":false,"sortable":false,"type":"text","unique":false}],"#,
            r#""kind":"collection","localized":true,"scope":"application"}},"version":1}"#
        )
    );
    assert_eq!(
        model.fingerprint(),
        sha256_hex(model.canonical_json().as_bytes())
    );
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::Digest;
    sha2::Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// One accepting and one rejecting model per rule.
fn accepts(source: &str) {
    if let Err(error) = parse(source) {
        panic!("rejected:\n{source}\n{error}");
    }
}

fn rejects(source: &str, path: &str, code: &str) {
    let found = issues(source);
    assert!(
        found.iter().any(|(p, c)| p == path && c == code),
        "expected {path} {code}, got {found:?}\n{source}"
    );
}

fn field_rule(accepted: &str, rejected: &str, path: &str, code: &str) {
    accepts(&with_fields(accepted));
    rejects(
        &with_fields(rejected),
        &format!("types.t.fields[0]{path}"),
        code,
    );
}

#[test]
fn type_level_keys() {
    let type_with = |keys: &str| {
        document(&format!(
            "[types.t]\naccess = \"public\"\n{keys}\nfields = [{{ name = \"title\", type = \"string\" }}, {{ name = \"n\", type = \"integer\" }}]\n"
        ))
    };
    accepts(&type_with("kind = \"singleton\""));
    rejects(
        &type_with("kind = \"page\""),
        "types.t.kind",
        "invalid_value",
    );
    accepts(&type_with("scope = \"application\""));
    rejects(
        &type_with("scope = \"tenant\""),
        "types.t.scope",
        "invalid_value",
    );
    rejects(
        &type_with("scope = \"organization\""),
        "types.t.scope",
        "organizations_required",
    );
    let organizations = ModelOptions {
        organizations: true,
        ..options(true)
    };
    assert!(ContentModel::parse(&type_with("scope = \"organization\""), &organizations).is_ok());
    accepts(&type_with("localized = false"));
    rejects(
        &type_with("localized = \"no\""),
        "types.t.localized",
        "invalid_type",
    );
    accepts(&type_with("title_field = \"title\""));
    rejects(
        &type_with("title_field = \"n\""),
        "types.t.title_field",
        "invalid_title_field",
    );
    rejects(
        &type_with("title_field = \"nope\""),
        "types.t.title_field",
        "invalid_title_field",
    );
    accepts(&type_with(&format!(
        "description = \"{}\"",
        "é".repeat(500)
    )));
    rejects(
        &type_with(&format!("description = \"{}\"", "é".repeat(501))),
        "types.t.description",
        "out_of_range",
    );
    rejects(
        &type_with("colour = \"red\""),
        "types.t.colour",
        "unknown_key",
    );
    // fields: 1 to 100, required.
    let many = |n: usize| {
        let fields: Vec<String> = (0..n)
            .map(|i| format!("{{ name = \"f{i}\", type = \"boolean\" }}"))
            .collect();
        with_fields(&fields.join(",\n"))
    };
    accepts(&many(100));
    rejects(&many(101), "types.t.fields", "out_of_range");
    rejects(&with_fields(""), "types.t.fields", "out_of_range");
    rejects(
        &document("[types.t]\naccess = \"public\"\n"),
        "types.t.fields",
        "missing_required",
    );
}

#[test]
fn names_and_reserved_names() {
    let named = |name: &str| {
        document(&format!(
            "[types.{name}]\naccess = \"public\"\nfields = [{{ name = \"title\", type = \"string\" }}]\n"
        ))
    };
    accepts(&named(&format!("a{}", "b".repeat(47))));
    rejects(
        &named(&format!("a{}", "b".repeat(48))),
        &format!("types.a{}", "b".repeat(48)),
        "invalid_name",
    );
    rejects(&named("\"Article\""), "types.Article", "invalid_name");
    rejects(&named("\"bad name\""), "types.\"bad name\"", "invalid_name");
    for reserved in ["manage", "assets", "model", "o", "health"] {
        rejects(
            &named(reserved),
            &format!("types.{reserved}"),
            "reserved_name",
        );
    }
    for reserved in ["asset", "block", "span"] {
        rejects(
            &document(&format!(
                "[components.{reserved}]\nfields = [{{ name = \"x\", type = \"string\" }}]\n"
            )),
            &format!("components.{reserved}"),
            "reserved_name",
        );
    }
    field_rule(
        &format!("{{ name = \"a{}\", type = \"string\" }}", "b".repeat(63)),
        &format!("{{ name = \"a{}\", type = \"string\" }}", "b".repeat(64)),
        ".name",
        "invalid_name",
    );
    for bad in ["Title", "_x", "1x", "a-b"] {
        rejects(
            &with_fields(&format!("{{ name = \"{bad}\", type = \"string\" }}")),
            "types.t.fields[0].name",
            "invalid_name",
        );
    }
    for reserved in ["id", "type", "locale", "sys"] {
        rejects(
            &with_fields(&format!("{{ name = \"{reserved}\", type = \"string\" }}")),
            "types.t.fields[0].name",
            "reserved_name",
        );
    }
    rejects(
        &with_fields("{ name = \"a\", type = \"string\" },\n{ name = \"a\", type = \"text\" }"),
        "types.t.fields[1].name",
        "duplicate_name",
    );
    rejects(
        &with_fields("{ type = \"string\" }"),
        "types.t.fields[0].name",
        "missing_required",
    );
    rejects(
        &with_fields("{ name = \"a\" }"),
        "types.t.fields[0].type",
        "missing_required",
    );
    rejects(
        &with_fields("{ name = \"a\", type = \"colour\" }"),
        "types.t.fields[0].type",
        "unknown_field_type",
    );
}

#[test]
fn model_limits() {
    let types = |n: usize| {
        let mut out = String::new();
        // `document` declares `other` already.
        for i in 1..n {
            out.push_str(&format!(
                "[types.t{i}]\naccess = \"public\"\nfields = [{{ name = \"x\", type = \"string\" }}]\n"
            ));
        }
        document(&out)
    };
    accepts(&types(64));
    rejects(&types(65), "types", "out_of_range");
    let components = |n: usize| {
        let mut out = String::new();
        for i in 1..n {
            out.push_str(&format!(
                "[components.c{i}]\nfields = [{{ name = \"x\", type = \"string\" }}]\n"
            ));
        }
        document(&out)
    };
    accepts(&components(32));
    rejects(&components(33), "components", "out_of_range");
    let locales = |n: usize| {
        let mut list = vec!["{ code = \"en\", default = true }".to_owned()];
        for i in 1..n {
            list.push(format!(
                "{{ code = \"x{}{}\" }}",
                (b'a' + (i / 26) as u8) as char,
                (b'a' + (i % 26) as u8) as char
            ));
        }
        format!(
            "version = 1\nlocales = [{}]\n[types.p]\naccess = \"public\"\nfields = [{{ name = \"x\", type = \"string\" }}]\n",
            list.join(", ")
        )
    };
    accepts(&locales(32));
    rejects(&locales(33), "locales", "out_of_range");
    // Nesting: a type's fields are level 1, each component adds one.
    let chain = |levels: usize| {
        let mut out =
            String::from("[components.c1]\nfields = [{ name = \"x\", type = \"string\" }]\n");
        for i in 2..levels {
            out.push_str(&format!(
                "[components.c{i}]\nfields = [{{ name = \"x\", type = \"component\", component = \"c{}\" }}]\n",
                i - 1
            ));
        }
        out.push_str(&format!(
            "[types.deep]\naccess = \"public\"\nfields = [{{ name = \"x\", type = \"list\", of = {{ type = \"component\", component = \"c{}\" }} }}]\n",
            levels - 1
        ));
        document(&out)
    };
    accepts(&chain(4));
    rejects(&chain(5), "types.deep.fields[0]", "nesting_too_deep");
    rejects(
        &document(
            "[components.loop]\nfields = [{ name = \"x\", type = \"blocks\", components = [\"loop\"] }]\n",
        ),
        "components.loop",
        "component_cycle",
    );
    let flagged = |n: usize, flag: &str| {
        let fields: Vec<String> = (0..n)
            .map(|i| format!("{{ name = \"f{i}\", type = \"string\", {flag} = true }}"))
            .collect();
        with_fields(&fields.join(",\n"))
    };
    accepts(&flagged(8, "unique"));
    rejects(&flagged(9, "unique"), "types.t", "too_many_unique");
    accepts(&flagged(16, "filterable"));
    rejects(&flagged(17, "sortable"), "types.t", "too_many_indexed");
    // Slugs count as unique fields.
    let slugs: Vec<String> = (0..9)
        .map(|i| format!("{{ name = \"s{i}\", type = \"slug\" }}"))
        .collect();
    rejects(
        &with_fields(&slugs.join(",\n")),
        "types.t",
        "too_many_unique",
    );
}

#[test]
fn locales_rules() {
    let with = |locales: &str| {
        format!(
            "version = 1\nlocales = [{locales}]\n[types.p]\naccess = \"public\"\nfields = [{{ name = \"x\", type = \"string\" }}]\n"
        )
    };
    accepts(&with(
        "{ code = \"en\", default = true }, { code = \"fr-BE\", fallback = \"en\" }, { code = \"zh-Hant-TW\" }",
    ));
    rejects(&with("{ code = \"en\" }"), "locales", "default_locale");
    rejects(
        &with("{ code = \"en\", default = true }, { code = \"fr\", default = true }"),
        "locales[1].default",
        "default_locale",
    );
    rejects(
        &with("{ code = \"e\", default = true }"),
        "locales[0].code",
        "invalid_locale",
    );
    rejects(
        &with("{ code = \"en_US\", default = true }"),
        "locales[0].code",
        "invalid_locale",
    );
    rejects(
        &with("{ code = \"en\", default = true }, { code = \"EN\" }"),
        "locales[1].code",
        "duplicate_locale",
    );
    rejects(
        &with("{ code = \"en\", default = true }, { code = \"fr\", fallback = \"de\" }"),
        "locales[1].fallback",
        "unknown_locale",
    );
    rejects(
        &with(
            "{ code = \"en\", default = true }, { code = \"fr\", fallback = \"be\" }, { code = \"be\", fallback = \"fr\" }",
        ),
        "locales[1].fallback",
        "fallback_cycle",
    );
    rejects(
        &with("{ code = \"en\", default = true }, { code = \"fr\", fallback = \"fr\" }"),
        "locales[1].fallback",
        "fallback_cycle",
    );
    rejects(
        &with("{ code = \"en\", default = true, fallback = \"en\" }"),
        "locales[0].fallback",
        "invalid_value",
    );
    rejects(
        &with("{ code = \"en\", default = true, region = \"x\" }"),
        "locales[0].region",
        "unknown_key",
    );
    rejects(&with(""), "locales", "out_of_range");
    rejects(
        "version = 1\n[types.p]\naccess = \"public\"\nfields = [{ name = \"x\", type = \"string\" }]\n",
        "locales",
        "missing_required",
    );
    rejects(
        &with("{ code = \"en\", default = true }").replace("version = 1", "version = 0"),
        "version",
        "out_of_range",
    );
    rejects(
        &with("{ code = \"en\", default = true }").replace("version = 1\n", ""),
        "version",
        "missing_required",
    );
    rejects(
        &with("{ code = \"en\", default = true }").replace("version = 1", "version = \"1\""),
        "version",
        "invalid_type",
    );
}

#[test]
fn common_field_keys() {
    field_rule(
        "{ name = \"a\", type = \"string\", required = true }",
        "{ name = \"a\", type = \"string\", required = \"yes\" }",
        ".required",
        "invalid_type",
    );
    // localized: a shared field in a localized type; not in a
    // non-localized type, not inside components.
    accepts(&with_fields(
        "{ name = \"a\", type = \"string\", localized = false }",
    ));
    rejects(
        &document(
            "[types.n]\naccess = \"public\"\nlocalized = false\nfields = [{ name = \"a\", type = \"string\", localized = true }]\n",
        ),
        "types.n.fields[0].localized",
        "not_allowed",
    );
    rejects(
        &document(
            "[components.c]\nfields = [{ name = \"a\", type = \"string\", localized = false }]\n",
        ),
        "components.c.fields[0].localized",
        "not_allowed",
    );
    // default: scalar types, validated against the field's rules.
    field_rule(
        "{ name = \"a\", type = \"integer\", default = 3, max = 5 }",
        "{ name = \"a\", type = \"integer\", default = 6, max = 5 }",
        ".default",
        "invalid_default",
    );
    field_rule(
        "{ name = \"a\", type = \"date\", default = 2026-10-24 }",
        "{ name = \"a\", type = \"date\", default = [1] }",
        ".default",
        "invalid_default",
    );
    field_rule(
        "{ name = \"a\", type = \"decimal\", default = \"1.5\" }",
        "{ name = \"a\", type = \"json\", default = \"x\" }",
        ".default",
        "not_allowed",
    );
    // unique: string, slug, integer, enum; top level only.
    for kind in [
        "type = \"string\"",
        "type = \"slug\"",
        "type = \"integer\"",
        "type = \"enum\", values = [\"a\"]",
    ] {
        accepts(&with_fields(&format!(
            "{{ name = \"a\", {kind}, unique = true }}"
        )));
    }
    field_rule(
        "{ name = \"a\", type = \"string\", unique = true }",
        "{ name = \"a\", type = \"text\", unique = true }",
        ".unique",
        "not_allowed",
    );
    // filterable: scalars, references, lists of scalars.
    accepts(&with_fields(
        "{ name = \"a\", type = \"reference\", to = [\"other\"], filterable = true }",
    ));
    accepts(&with_fields(
        "{ name = \"a\", type = \"list\", of = { type = \"string\" }, filterable = true }",
    ));
    field_rule(
        "{ name = \"a\", type = \"boolean\", filterable = true }",
        "{ name = \"a\", type = \"json\", filterable = true }",
        ".filterable",
        "not_allowed",
    );
    rejects(
        &with_fields(
            "{ name = \"a\", type = \"list\", of = { type = \"component\", component = \"quote\" }, filterable = true }",
        ),
        "types.t.fields[0].filterable",
        "not_allowed",
    );
    // sortable: string, slug, integer, float, decimal, date, datetime, enum.
    for kind in [
        "string", "slug", "integer", "float", "decimal", "date", "datetime",
    ] {
        accepts(&with_fields(&format!(
            "{{ name = \"a\", type = \"{kind}\", sortable = true }}"
        )));
    }
    field_rule(
        "{ name = \"a\", type = \"date\", sortable = true }",
        "{ name = \"a\", type = \"boolean\", sortable = true }",
        ".sortable",
        "not_allowed",
    );
    for key in ["unique", "filterable", "sortable"] {
        rejects(
            &document(&format!(
                "[components.c]\nfields = [{{ name = \"a\", type = \"string\", {key} = true }}]\n"
            )),
            &format!("components.c.fields[0].{key}"),
            "not_allowed",
        );
    }
    field_rule(
        "{ name = \"a\", type = \"string\", description = \"Shown in forms\" }",
        "{ name = \"a\", type = \"string\", description = 3 }",
        ".description",
        "invalid_type",
    );
    field_rule(
        "{ name = \"a\", type = \"string\", max_length = 3 }",
        "{ name = \"a\", type = \"string\", max = 3 }",
        ".max",
        "unknown_key",
    );
}

#[test]
fn field_type_keys() {
    let rule = field_rule;
    // string
    rule(
        "{ name = \"a\", type = \"string\", max_length = 4096 }",
        "{ name = \"a\", type = \"string\", max_length = 4097 }",
        ".max_length",
        "out_of_range",
    );
    rule(
        "{ name = \"a\", type = \"string\", min_length = 2, max_length = 2 }",
        "{ name = \"a\", type = \"string\", min_length = 3, max_length = 2 }",
        ".min_length",
        "out_of_range",
    );
    rule(
        "{ name = \"a\", type = \"string\", pattern = \"[a-z]+\" }",
        "{ name = \"a\", type = \"string\", pattern = \"(\" }",
        ".pattern",
        "invalid_pattern",
    );
    // A pattern cannot escape its anchors.
    rejects(
        &with_fields("{ name = \"a\", type = \"string\", pattern = \"a)|(b\" }"),
        "types.t.fields[0].pattern",
        "invalid_pattern",
    );
    // The 1 MiB compiled size limit admits ordinary patterns.
    for ordinary in [".{1,255}", "\\w{5}", "\\w{50}", "[a-z]{1,1000}"] {
        accepts(&with_fields(&format!(
            "{{ name = \"a\", type = \"string\", pattern = '{ordinary}' }}"
        )));
    }
    rejects(
        &with_fields("{ name = \"a\", type = \"string\", pattern = '\\w{100}' }"),
        "types.t.fields[0].pattern",
        "pattern_too_large",
    );
    // At most 1024 bytes of source.
    accepts(&with_fields(&format!(
        "{{ name = \"a\", type = \"string\", pattern = \"{}\" }}",
        "a".repeat(1024)
    )));
    rejects(
        &with_fields(&format!(
            "{{ name = \"a\", type = \"string\", pattern = \"{}\" }}",
            "a".repeat(1025)
        )),
        "types.t.fields[0].pattern",
        "out_of_range",
    );
    // text
    rule(
        "{ name = \"a\", type = \"text\", max_length = 100000 }",
        "{ name = \"a\", type = \"text\", max_length = 100001 }",
        ".max_length",
        "out_of_range",
    );
    rule(
        "{ name = \"a\", type = \"text\", min_length = 1 }",
        "{ name = \"a\", type = \"text\", min_length = -1 }",
        ".min_length",
        "out_of_range",
    );
    // rich_text
    rule(
        "{ name = \"a\", type = \"rich_text\", styles = [\"normal\", \"h6\", \"blockquote\"] }",
        "{ name = \"a\", type = \"rich_text\", styles = [\"normal\", \"h7\"] }",
        ".styles[1]",
        "invalid_value",
    );
    rule(
        "{ name = \"a\", type = \"rich_text\", styles = [\"normal\"] }",
        "{ name = \"a\", type = \"rich_text\", styles = [] }",
        ".styles",
        "out_of_range",
    );
    rule(
        "{ name = \"a\", type = \"rich_text\", marks = [\"strike\", \"underline\"] }",
        "{ name = \"a\", type = \"rich_text\", marks = [\"blink\"] }",
        ".marks[0]",
        "invalid_value",
    );
    rule(
        "{ name = \"a\", type = \"rich_text\", marks = [\"em\"] }",
        "{ name = \"a\", type = \"rich_text\", marks = [\"em\", \"em\"] }",
        ".marks[1]",
        "duplicate_value",
    );
    rule(
        "{ name = \"a\", type = \"rich_text\", annotations = [\"entry_link\"] }",
        "{ name = \"a\", type = \"rich_text\", annotations = [\"footnote\"] }",
        ".annotations[0]",
        "invalid_value",
    );
    rule(
        "{ name = \"a\", type = \"rich_text\", lists = [\"number\"] }",
        "{ name = \"a\", type = \"rich_text\", lists = [\"check\"] }",
        ".lists[0]",
        "invalid_value",
    );
    rule(
        "{ name = \"a\", type = \"rich_text\", embeds = [\"quote\", \"asset\"] }",
        "{ name = \"a\", type = \"rich_text\", embeds = [\"table\"] }",
        ".embeds[0]",
        "unknown_component",
    );
    rule(
        "{ name = \"a\", type = \"rich_text\", max_blocks = 5000 }",
        "{ name = \"a\", type = \"rich_text\", max_blocks = 5001 }",
        ".max_blocks",
        "out_of_range",
    );
    rule(
        "{ name = \"a\", type = \"rich_text\", max_text_length = 1 }",
        "{ name = \"a\", type = \"rich_text\", max_text_length = 0 }",
        ".max_text_length",
        "out_of_range",
    );
    rule(
        "{ name = \"a\", type = \"rich_text\", annotations = [\"entry_link\"], entry_link_to = [\"other\"] }",
        "{ name = \"a\", type = \"rich_text\", entry_link_to = [\"other\"] }",
        ".entry_link_to",
        "not_allowed",
    );
    rejects(
        &with_fields(
            "{ name = \"a\", type = \"rich_text\", annotations = [\"entry_link\"], entry_link_to = [\"ghost\"] }",
        ),
        "types.t.fields[0].entry_link_to[0]",
        "unknown_type",
    );
    // integer
    rule(
        "{ name = \"a\", type = \"integer\", min = -9007199254740991, max = 9007199254740991 }",
        "{ name = \"a\", type = \"integer\", max = 9007199254740992 }",
        ".max",
        "out_of_range",
    );
    rule(
        "{ name = \"a\", type = \"integer\", min = 1, max = 1 }",
        "{ name = \"a\", type = \"integer\", min = 2, max = 1 }",
        ".min",
        "out_of_range",
    );
    rule(
        "{ name = \"a\", type = \"integer\", min = 0 }",
        "{ name = \"a\", type = \"integer\", min = 0.5 }",
        ".min",
        "invalid_type",
    );
    // float
    rule(
        "{ name = \"a\", type = \"float\", min = 0, max = 1.5 }",
        "{ name = \"a\", type = \"float\", max = nan }",
        ".max",
        "invalid_type",
    );
    rule(
        "{ name = \"a\", type = \"float\", min = -1.5 }",
        "{ name = \"a\", type = \"float\", min = 2.5, max = 1.0 }",
        ".min",
        "out_of_range",
    );
    // decimal
    rule(
        "{ name = \"a\", type = \"decimal\", precision = 28, scale = 28 }",
        "{ name = \"a\", type = \"decimal\", precision = 29 }",
        ".precision",
        "out_of_range",
    );
    rule(
        "{ name = \"a\", type = \"decimal\", precision = 4, scale = 4 }",
        "{ name = \"a\", type = \"decimal\", precision = 4, scale = 5 }",
        ".scale",
        "out_of_range",
    );
    rule(
        "{ name = \"a\", type = \"decimal\", precision = 4, scale = 2, min = \"-10\", max = \"99.99\" }",
        "{ name = \"a\", type = \"decimal\", precision = 4, scale = 2, max = \"100\" }",
        ".max",
        "invalid_value",
    );
    rule(
        "{ name = \"a\", type = \"decimal\", min = \"1\" }",
        "{ name = \"a\", type = \"decimal\", min = 1 }",
        ".min",
        "invalid_type",
    );
    rule(
        "{ name = \"a\", type = \"decimal\", min = \"1\", max = \"1.00\" }",
        "{ name = \"a\", type = \"decimal\", min = \"2\", max = \"1\" }",
        ".min",
        "out_of_range",
    );
    // datetime and date
    rule(
        "{ name = \"a\", type = \"datetime\", min = \"2020-01-01T00:00:00Z\", max = 2030-01-01T00:00:00+01:00 }",
        "{ name = \"a\", type = \"datetime\", min = \"2020-01-01\" }",
        ".min",
        "invalid_value",
    );
    rule(
        "{ name = \"a\", type = \"datetime\", max = \"2030-01-01T00:00:00.123Z\" }",
        "{ name = \"a\", type = \"datetime\", max = \"2030-01-01T00:00:00.1234Z\" }",
        ".max",
        "invalid_value",
    );
    rule(
        "{ name = \"a\", type = \"date\", min = \"2020-01-01\", max = 2030-12-31 }",
        "{ name = \"a\", type = \"date\", min = \"2020-02-30\" }",
        ".min",
        "invalid_value",
    );
    rule(
        "{ name = \"a\", type = \"date\", min = \"2020-01-01\" }",
        "{ name = \"a\", type = \"date\", min = \"2021-01-01\", max = \"2020-01-01\" }",
        ".min",
        "out_of_range",
    );
    // enum
    rule(
        "{ name = \"a\", type = \"enum\", values = [\"news\", \"how-to\", \"v2_x\", \"9\"] }",
        "{ name = \"a\", type = \"enum\", values = [\"News\"] }",
        ".values[0]",
        "invalid_value",
    );
    rule(
        "{ name = \"a\", type = \"enum\", values = [\"a\"] }",
        "{ name = \"a\", type = \"enum\" }",
        ".values",
        "missing_required",
    );
    rule(
        "{ name = \"a\", type = \"enum\", values = [\"a\", \"b\"] }",
        "{ name = \"a\", type = \"enum\", values = [\"a\", \"a\"] }",
        ".values[1]",
        "duplicate_value",
    );
    let values = |n: usize| {
        (0..n)
            .map(|i| format!("\"v{i}\""))
            .collect::<Vec<_>>()
            .join(", ")
    };
    rule(
        &format!(
            "{{ name = \"a\", type = \"enum\", values = [{}] }}",
            values(200)
        ),
        &format!(
            "{{ name = \"a\", type = \"enum\", values = [{}] }}",
            values(201)
        ),
        ".values",
        "out_of_range",
    );
    rule(
        "{ name = \"a\", type = \"enum\", values = [\"a\"] }",
        "{ name = \"a\", type = \"enum\", values = [] }",
        ".values",
        "out_of_range",
    );
    // slug
    rule(
        "{ name = \"a\", type = \"slug\", max_length = 200 }",
        "{ name = \"a\", type = \"slug\", max_length = 201 }",
        ".max_length",
        "out_of_range",
    );
    accepts(&with_fields(
        "{ name = \"title\", type = \"string\" },\n{ name = \"a\", type = \"slug\", from = \"title\" }",
    ));
    rejects(
        &with_fields(
            "{ name = \"n\", type = \"integer\" },\n{ name = \"a\", type = \"slug\", from = \"n\" }",
        ),
        "types.t.fields[1].from",
        "invalid_slug_source",
    );
    rejects(
        &with_fields("{ name = \"a\", type = \"slug\", from = \"ghost\" }"),
        "types.t.fields[0].from",
        "invalid_slug_source",
    );
    // reference
    rule(
        "{ name = \"a\", type = \"reference\", to = [\"other\", \"t\"] }",
        "{ name = \"a\", type = \"reference\", to = [\"ghost\"] }",
        ".to[0]",
        "unknown_type",
    );
    rule(
        "{ name = \"a\", type = \"reference\", to = [\"other\"] }",
        "{ name = \"a\", type = \"reference\" }",
        ".to",
        "missing_required",
    );
    let to = |n: usize| (0..n).map(|_| "\"other\"").collect::<Vec<_>>().join(", ");
    rule(
        "{ name = \"a\", type = \"reference\", to = [\"other\"], cardinality = \"many\", min_items = 1, max_items = 1000 }",
        "{ name = \"a\", type = \"reference\", to = [\"other\"], cardinality = \"many\", max_items = 1001 }",
        ".max_items",
        "out_of_range",
    );
    rule(
        "{ name = \"a\", type = \"reference\", to = [\"other\"], cardinality = \"one\" }",
        "{ name = \"a\", type = \"reference\", to = [\"other\"], cardinality = \"two\" }",
        ".cardinality",
        "invalid_value",
    );
    rule(
        "{ name = \"a\", type = \"reference\", to = [\"other\"], require_published = true }",
        "{ name = \"a\", type = \"reference\", to = [\"other\"], min_items = 1 }",
        ".min_items",
        "not_allowed",
    );
    rejects(
        &with_fields(&format!(
            "{{ name = \"a\", type = \"reference\", to = [{}] }}",
            to(17)
        )),
        "types.t.fields[0].to",
        "out_of_range",
    );
    rule(
        "{ name = \"a\", type = \"reference\", to = [\"other\"], cardinality = \"many\", min_items = 2, max_items = 2 }",
        "{ name = \"a\", type = \"reference\", to = [\"other\"], cardinality = \"many\", min_items = 3, max_items = 2 }",
        ".min_items",
        "out_of_range",
    );
    // asset
    rule(
        "{ name = \"a\", type = \"asset\", accept = [\"image/*\", \"application/pdf\"] }",
        "{ name = \"a\", type = \"asset\", accept = [\"Image/PNG\"] }",
        ".accept[0]",
        "invalid_value",
    );
    rule(
        "{ name = \"a\", type = \"asset\", cardinality = \"many\", max_items = 10 }",
        "{ name = \"a\", type = \"asset\", max_items = 10 }",
        ".max_items",
        "not_allowed",
    );
    let without_media = ContentModel::parse(
        &with_fields("{ name = \"a\", type = \"asset\" }"),
        &options(false),
    )
    .unwrap_err();
    assert_eq!(
        without_media.codes(),
        [("types.t.fields[0].type", "media_disabled")]
    );
    let embed_without_media = ContentModel::parse(
        &with_fields("{ name = \"a\", type = \"rich_text\", embeds = [\"asset\"] }"),
        &options(false),
    )
    .unwrap_err();
    assert_eq!(
        embed_without_media.codes(),
        [("types.t.fields[0].embeds[0]", "media_disabled")]
    );
    // component
    rule(
        "{ name = \"a\", type = \"component\", component = \"quote\" }",
        "{ name = \"a\", type = \"component\", component = \"ghost\" }",
        ".component",
        "unknown_component",
    );
    rule(
        "{ name = \"a\", type = \"component\", component = \"quote\" }",
        "{ name = \"a\", type = \"component\" }",
        ".component",
        "missing_required",
    );
    // list
    rule(
        "{ name = \"a\", type = \"list\", of = { type = \"string\", max_length = 40 }, max_items = 1000 }",
        "{ name = \"a\", type = \"list\", of = { type = \"string\" }, max_items = 1001 }",
        ".max_items",
        "out_of_range",
    );
    rule(
        "{ name = \"a\", type = \"list\", of = { type = \"component\", component = \"quote\" } }",
        "{ name = \"a\", type = \"list\", of = { type = \"json\" } }",
        ".of.type",
        "not_allowed",
    );
    rule(
        "{ name = \"a\", type = \"list\", of = { type = \"enum\", values = [\"x\"] } }",
        "{ name = \"a\", type = \"list\", of = { type = \"list\", of = { type = \"string\" } } }",
        ".of.type",
        "not_allowed",
    );
    rule(
        "{ name = \"a\", type = \"list\", of = { type = \"integer\" } }",
        "{ name = \"a\", type = \"list\" }",
        ".of",
        "missing_required",
    );
    rule(
        "{ name = \"a\", type = \"list\", of = { type = \"integer\", max = 3 } }",
        "{ name = \"a\", type = \"list\", of = { type = \"integer\", name = \"x\" } }",
        ".of.name",
        "unknown_key",
    );
    rule(
        "{ name = \"a\", type = \"list\", of = { type = \"integer\" } }",
        "{ name = \"a\", type = \"list\", of = { type = \"integer\", required = true } }",
        ".of.required",
        "unknown_key",
    );
    // blocks
    rule(
        "{ name = \"a\", type = \"blocks\", components = [\"quote\"], min_items = 1, max_items = 30 }",
        "{ name = \"a\", type = \"blocks\", components = [\"ghost\"] }",
        ".components[0]",
        "unknown_component",
    );
    rule(
        "{ name = \"a\", type = \"blocks\", components = [\"quote\"] }",
        "{ name = \"a\", type = \"blocks\" }",
        ".components",
        "missing_required",
    );
    let many = (0..33).map(|_| "\"quote\"").collect::<Vec<_>>().join(", ");
    rejects(
        &with_fields(&format!(
            "{{ name = \"a\", type = \"blocks\", components = [{many}] }}"
        )),
        "types.t.fields[0].components",
        "out_of_range",
    );
    // json
    rule(
        "{ name = \"a\", type = \"json\", max_bytes = 262144 }",
        "{ name = \"a\", type = \"json\", max_bytes = 262145 }",
        ".max_bytes",
        "out_of_range",
    );
    // boolean takes no type-specific keys.
    rule(
        "{ name = \"a\", type = \"boolean\" }",
        "{ name = \"a\", type = \"boolean\", max = 1 }",
        ".max",
        "unknown_key",
    );
}

#[test]
fn defaults_are_filled_into_the_canonical_form() {
    let model = parse(&with_fields(
        "{ name = \"s\", type = \"string\" },\n{ name = \"r\", type = \"rich_text\" },\n{ name = \"d\", type = \"decimal\" },\n{ name = \"g\", type = \"slug\" },\n{ name = \"f\", type = \"reference\", to = [\"other\"] },\n{ name = \"l\", type = \"list\", of = { type = \"string\" } },\n{ name = \"j\", type = \"json\" }",
    ))
    .unwrap();
    let canonical: serde_json::Value = serde_json::from_str(model.canonical_json()).unwrap();
    let fields = &canonical["types"]["t"]["fields"];
    assert_eq!(fields[0]["max_length"], 255);
    assert_eq!(fields[1]["max_blocks"], 500);
    assert_eq!(fields[1]["max_text_length"], 100_000);
    assert_eq!(fields[1]["annotations"], serde_json::json!(["link"]));
    assert_eq!(fields[2]["precision"], 18);
    assert_eq!(fields[2]["scale"], 2);
    assert_eq!(fields[3]["max_length"], 96);
    assert_eq!(fields[3]["unique"], true);
    assert_eq!(fields[4]["cardinality"], "one");
    assert_eq!(fields[4]["max_items"], 100);
    assert_eq!(fields[5]["max_items"], 100);
    assert_eq!(
        fields[5]["of"],
        serde_json::json!({"max_length": 255, "type": "string"})
    );
    assert_eq!(fields[6]["max_bytes"], 16_384);
    assert_eq!(canonical["locales"][1]["fallback"], "en");
}

#[test]
fn component_fields_take_no_default() {
    rejects(
        &document("[components.c]\nfields = [{ name = \"a\", type = \"integer\", default = 1 }]\n"),
        "components.c.fields[0].default",
        "not_allowed",
    );
}

#[test]
fn a_slug_without_from_is_generated_from_the_title_field() {
    let source = document(
        "[types.t]\naccess = \"public\"\ntitle_field = \"title\"\nfields = [{ name = \"title\", type = \"string\" }, { name = \"slug\", type = \"slug\" }]\n",
    );
    let model = parse(&source).unwrap();
    let canonical: serde_json::Value = serde_json::from_str(model.canonical_json()).unwrap();
    assert_eq!(canonical["types"]["t"]["fields"][1]["from"], "title");
    let explicit = source.replace(
        "{ name = \"slug\", type = \"slug\" }",
        "{ name = \"slug\", type = \"slug\", from = \"title\" }",
    );
    assert_eq!(fingerprint(&explicit), model.fingerprint());
    let fields = validate(
        &model,
        &serde_json::json!({ "title": "Hello World" }),
        &DRAFT,
    )
    .unwrap();
    assert_eq!(fields["slug"], "hello-world");
    // Without a title field nothing is generated.
    let untitled = parse(&with_fields("{ name = \"slug\", type = \"slug\" }")).unwrap();
    let canonical: serde_json::Value = serde_json::from_str(untitled.canonical_json()).unwrap();
    assert!(canonical["types"]["t"]["fields"][0].get("from").is_none());
}

#[test]
fn bounds_are_compared_with_effective_defaults() {
    for (field, key) in [
        (
            "{ name = \"a\", type = \"string\", min_length = 256 }",
            ".min_length",
        ),
        (
            "{ name = \"a\", type = \"text\", min_length = 10001 }",
            ".min_length",
        ),
        (
            "{ name = \"a\", type = \"list\", of = { type = \"string\" }, min_items = 101 }",
            ".min_items",
        ),
        (
            "{ name = \"a\", type = \"blocks\", components = [\"quote\"], min_items = 101 }",
            ".min_items",
        ),
        (
            "{ name = \"a\", type = \"reference\", to = [\"other\"], cardinality = \"many\", min_items = 101 }",
            ".min_items",
        ),
        (
            "{ name = \"a\", type = \"asset\", cardinality = \"many\", min_items = 101 }",
            ".min_items",
        ),
    ] {
        rejects(
            &with_fields(field),
            &format!("types.t.fields[0]{key}"),
            "out_of_range",
        );
    }
    accepts(&with_fields(
        "{ name = \"a\", type = \"string\", min_length = 255 }",
    ));
    accepts(&with_fields(
        "{ name = \"a\", type = \"list\", of = { type = \"string\" }, min_items = 100 }",
    ));
}

#[test]
fn list_items_take_no_slug_source() {
    accepts(&with_fields(
        "{ name = \"a\", type = \"list\", of = { type = \"slug\" } }",
    ));
    rejects(
        &with_fields(
            "{ name = \"title\", type = \"string\" },\n{ name = \"a\", type = \"list\", of = { type = \"slug\", from = \"title\" } }",
        ),
        "types.t.fields[1].of.from",
        "not_allowed",
    );
}

#[test]
fn negative_zero_float_bounds_fingerprint_like_zero() {
    let with = |bound: &str| {
        fingerprint(&with_fields(&format!(
            "{{ name = \"a\", type = \"float\", min = {bound} }}"
        )))
    };
    assert_eq!(with("-0.0"), with("0.0"));
    assert_eq!(with("0"), with("0.0"));
    assert_ne!(with("0.5"), with("0.0"));
}
