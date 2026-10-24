//! CT-C1 field value validators: an accepting and a rejecting case for
//! every rule of the field type table, write-time versus publish-time
//! rules, normalization, `_key`s, slug generation and detail paths that
//! never carry user text.
mod common;

use common::*;
use merezha_content::{
    ContentModel, ReferenceTarget, Stage, ValidateOptions, ValueError, valid_key,
};
use serde_json::{Value, json};

fn model(fields: &str) -> ContentModel {
    parse(&with_fields(fields)).unwrap_or_else(|e| panic!("{e}"))
}

/// `value` for field `a` is accepted at draft time and stored as `stored`.
fn ok(model: &ContentModel, value: Value, stored: Value) {
    let fields = validate(model, &json!({ "a": value }), &DRAFT)
        .unwrap_or_else(|e| panic!("{value} rejected: {e:?}"));
    assert_eq!(fields.get("a"), Some(&stored), "{value}");
}
fn same(model: &ContentModel, value: Value) {
    ok(model, value.clone(), value);
}
/// `value` for field `a` fails with exactly one detail.
fn bad(model: &ContentModel, value: Value, path: &str, code: &str) {
    let errors = validate(model, &json!({ "a": value }), &DRAFT)
        .err()
        .unwrap_or_else(|| panic!("{value} accepted"));
    assert_eq!(errors, pairs(&[(path, code)]), "{value}");
}

#[test]
fn string_rules() {
    let m = model(
        "{ name = \"a\", type = \"string\", min_length = 2, max_length = 5, pattern = \"[a-zé]+\" }",
    );
    same(&m, json!("abc"));
    same(&m, json!("ééééé"));
    bad(&m, json!("éééééé"), "/fields/a", "max_length");
    bad(&m, json!("a"), "/fields/a", "min_length");
    bad(&m, json!("ABC"), "/fields/a", "pattern");
    bad(&m, json!("ab\ncd"), "/fields/a", "invalid_character");
    bad(&m, json!("ab\rcd"), "/fields/a", "invalid_character");
    bad(&m, json!("ab\u{0}c"), "/fields/a", "invalid_character");
    bad(&m, json!(12), "/fields/a", "type_mismatch");
    // The pattern is anchored: a partial match is not enough.
    bad(&m, json!("abc1"), "/fields/a", "pattern");
    let plain = model("{ name = \"a\", type = \"string\" }");
    same(&plain, json!("x".repeat(255)));
    bad(&plain, json!("x".repeat(256)), "/fields/a", "max_length");
}

#[test]
fn text_rules() {
    let m = model("{ name = \"a\", type = \"text\", min_length = 1, max_length = 6 }");
    same(&m, json!("a\nb\r\nc"));
    bad(&m, json!("abcdefg"), "/fields/a", "max_length");
    bad(&m, json!(""), "/fields/a", "min_length");
    bad(&m, json!("a\u{0}"), "/fields/a", "invalid_character");
    bad(&m, json!(["a"]), "/fields/a", "type_mismatch");
    let plain = model("{ name = \"a\", type = \"text\" }");
    same(&plain, json!("x".repeat(10_000)));
    bad(&plain, json!("x".repeat(10_001)), "/fields/a", "max_length");
}

#[test]
fn integer_rules_and_the_safe_range() {
    let m = model("{ name = \"a\", type = \"integer\", min = -5, max = 10 }");
    same(&m, json!(10));
    same(&m, json!(-5));
    bad(&m, json!(11), "/fields/a", "max");
    bad(&m, json!(-6), "/fields/a", "min");
    bad(&m, json!(1.0), "/fields/a", "type_mismatch");
    bad(&m, json!(1.5), "/fields/a", "type_mismatch");
    bad(&m, json!("3"), "/fields/a", "type_mismatch");
    let plain = model("{ name = \"a\", type = \"integer\" }");
    same(&plain, json!(9_007_199_254_740_991_i64));
    same(&plain, json!(-9_007_199_254_740_991_i64));
    bad(&plain, json!(9_007_199_254_740_992_i64), "/fields/a", "max");
    bad(
        &plain,
        json!(-9_007_199_254_740_992_i64),
        "/fields/a",
        "min",
    );
    bad(&plain, json!(u64::MAX), "/fields/a", "max");
    // Beyond u64 JSON parses as binary64, which is not an integer here.
    // Beyond u64 JSON parses as binary64: still an integral number, so out of range.
    let huge: Value = serde_json::from_str("123456789012345678901234567890").unwrap();
    bad(&plain, huge, "/fields/a", "max");
}

#[test]
fn float_rules() {
    let m = model("{ name = \"a\", type = \"float\", min = 0, max = 1.5 }");
    same(&m, json!(0.25));
    same(&m, json!(1));
    bad(&m, json!(1.6), "/fields/a", "max");
    bad(&m, json!(-0.1), "/fields/a", "min");
    bad(&m, json!("0.5"), "/fields/a", "type_mismatch");
    let plain = model("{ name = \"a\", type = \"float\" }");
    same(&plain, json!(9_007_199_254_740_991_i64));
    bad(&plain, json!(9_007_199_254_740_992_i64), "/fields/a", "max");
}

#[test]
fn decimal_rules_and_normalization() {
    let m = model(
        "{ name = \"a\", type = \"decimal\", precision = 6, scale = 2, min = \"-10\", max = \"1000\" }",
    );
    ok(&m, json!("10"), json!("10.00"));
    ok(&m, json!("1.5"), json!("1.50"));
    ok(&m, json!("-0"), json!("0.00"));
    ok(&m, json!("-0.00"), json!("0.00"));
    ok(&m, json!("-9.99"), json!("-9.99"));
    ok(&m, json!("1000.00"), json!("1000.00"));
    bad(&m, json!("1.234"), "/fields/a", "scale");
    bad(&m, json!("12345.6"), "/fields/a", "precision");
    bad(&m, json!("1000.01"), "/fields/a", "max");
    bad(&m, json!("-10.01"), "/fields/a", "min");
    for malformed in ["01", "1.", ".5", "+1", "1e3", "", "1,5", " 1", "--1"] {
        bad(&m, json!(malformed), "/fields/a", "type_mismatch");
    }
    bad(&m, json!(1.5), "/fields/a", "type_mismatch");
    let scale0 = model("{ name = \"a\", type = \"decimal\", precision = 28, scale = 0 }");
    ok(
        &scale0,
        json!("9999999999999999999999999999"),
        json!("9999999999999999999999999999"),
    );
    bad(
        &scale0,
        json!("99999999999999999999999999999"),
        "/fields/a",
        "precision",
    );
    bad(&scale0, json!("1.0"), "/fields/a", "scale");
}

#[test]
fn boolean_rules() {
    let m = model("{ name = \"a\", type = \"boolean\" }");
    same(&m, json!(false));
    bad(&m, json!("true"), "/fields/a", "type_mismatch");
    bad(&m, json!(1), "/fields/a", "type_mismatch");
}

#[test]
fn datetime_rules_precision_and_utc_normalization() {
    let m = model(
        "{ name = \"a\", type = \"datetime\", min = \"2020-01-01T00:00:00Z\", max = \"2030-01-01T00:00:00+01:00\" }",
    );
    ok(
        &m,
        json!("2026-10-22T16:41:07.512+02:00"),
        json!("2026-10-22T14:41:07.512Z"),
    );
    ok(
        &m,
        json!("2026-10-22T16:41:07Z"),
        json!("2026-10-22T16:41:07.000Z"),
    );
    ok(
        &m,
        json!("2026-10-22t16:41:07.5z"),
        json!("2026-10-22T16:41:07.500Z"),
    );
    ok(
        &m,
        json!("2026-01-01T00:30:00-01:00"),
        json!("2026-01-01T01:30:00.000Z"),
    );
    ok(
        &m,
        json!("2029-12-31T23:00:00.000Z"),
        json!("2029-12-31T23:00:00.000Z"),
    );
    bad(
        &m,
        json!("2026-10-22T16:41:07.5123Z"),
        "/fields/a",
        "datetime_precision",
    );
    bad(&m, json!("2029-12-31T23:00:00.001Z"), "/fields/a", "max");
    bad(&m, json!("2019-12-31T23:59:59.999Z"), "/fields/a", "min");
    for malformed in [
        "2026-10-22T16:41:07",
        "2026-10-22 16:41:07Z",
        "2026-02-30T00:00:00Z",
        "2026-10-22T24:00:00Z",
        "2026-10-22T16:41:60Z",
        "2026-10-22T16:41:07+0200",
        "2026-10-22T16:41:07+24:00",
        "2026-10-22T16:41:07.Z",
        "2026-10-22T16:41:07Z[Europe/Paris]",
        "2026-10-22",
    ] {
        bad(&m, json!(malformed), "/fields/a", "type_mismatch");
    }
    bad(&m, json!(1_700_000_000), "/fields/a", "type_mismatch");
}

#[test]
fn date_rules() {
    let m = model("{ name = \"a\", type = \"date\", min = \"2020-01-01\", max = \"2030-12-31\" }");
    same(&m, json!("2024-02-29"));
    bad(&m, json!("2023-02-29"), "/fields/a", "type_mismatch");
    bad(
        &m,
        json!("2026-10-22T00:00:00Z"),
        "/fields/a",
        "type_mismatch",
    );
    bad(&m, json!("2026-1-01"), "/fields/a", "type_mismatch");
    bad(&m, json!("2019-12-31"), "/fields/a", "min");
    bad(&m, json!("2031-01-01"), "/fields/a", "max");
}

#[test]
fn enum_rules() {
    let m = model("{ name = \"a\", type = \"enum\", values = [\"news\", \"guide\"] }");
    same(&m, json!("guide"));
    bad(&m, json!("release"), "/fields/a", "not_in_enum");
    bad(&m, json!("News"), "/fields/a", "not_in_enum");
    bad(&m, json!(1), "/fields/a", "type_mismatch");
}

#[test]
fn slug_rules_and_generation() {
    let m = model(
        "{ name = \"title\", type = \"string\" },\n{ name = \"a\", type = \"slug\", from = \"title\", max_length = 12 }",
    );
    same(&m, json!("a-1-b"));
    for invalid in ["A-b", "a--b", "-a", "a-", "a_b", "é"] {
        bad(&m, json!(invalid), "/fields/a", "pattern");
    }
    bad(&m, json!("abcdefghijklm"), "/fields/a", "max_length");
    bad(&m, json!(7), "/fields/a", "type_mismatch");
    // Absent: generated from `from`, at a word boundary.
    let generated = validate(&m, &json!({ "title": "Notes de version 1.4" }), &DRAFT).unwrap();
    assert_eq!(generated["a"], "notes-de");
    // Present: never changed.
    let kept = validate(&m, &json!({ "title": "Other", "a": "keep-me" }), &DRAFT).unwrap();
    assert_eq!(kept["a"], "keep-me");
    // Nothing usable: stays absent.
    let none = validate(&m, &json!({ "title": "!!!" }), &DRAFT).unwrap();
    assert!(!none.contains_key("a"));
}

const UUID: &str = "0192f7a0-5d1c-7c3e-9b1a-6f0e2c4d8a11";

#[test]
fn reference_rules() {
    let one = model("{ name = \"a\", type = \"reference\", to = [\"other\"] }");
    same(&one, json!({ "id": UUID }));
    same(&one, json!({ "id": UUID, "type": "other" }));
    ok(
        &one,
        json!({ "id": UUID.to_uppercase() }),
        json!({ "id": UUID }),
    );
    bad(
        &one,
        json!({ "id": UUID, "type": "t" }),
        "/fields/a",
        "reference_type",
    );
    bad(
        &one,
        json!({ "id": UUID, "type": "ghost" }),
        "/fields/a",
        "reference_type",
    );
    bad(
        &one,
        json!({ "id": "not-a-uuid" }),
        "/fields/a/id",
        "type_mismatch",
    );
    bad(
        &one,
        json!({ "id": UUID.replace('-', "") }),
        "/fields/a/id",
        "type_mismatch",
    );
    bad(&one, json!({}), "/fields/a/id", "type_mismatch");
    bad(&one, json!([{ "id": UUID }]), "/fields/a", "type_mismatch");
    let errors = validate(&one, &json!({ "a": { "id": UUID, "entry": {} } }), &DRAFT).unwrap_err();
    assert_eq!(errors, pairs(&[("/fields/a", "unknown_field")]));

    let many = model(
        "{ name = \"a\", type = \"reference\", to = [\"other\", \"t\"], cardinality = \"many\", min_items = 1, max_items = 2, require_published = true }",
    );
    same(&many, json!([{ "id": UUID }, { "id": UUID, "type": "t" }]));
    same(&many, json!([]));
    bad(
        &many,
        json!([{ "id": UUID }, { "id": UUID }, { "id": UUID }]),
        "/fields/a",
        "max_items",
    );
    bad(
        &many,
        json!([{ "id": UUID }, { "id": "x" }]),
        "/fields/a/1/id",
        "type_mismatch",
    );
    bad(&many, json!({ "id": UUID }), "/fields/a", "type_mismatch");
    let errors = validate(&many, &json!({ "a": [] }), &PUBLISH).unwrap_err();
    assert_eq!(errors, pairs(&[("/fields/a", "min_items")]));
    // References are reported for the caller to resolve.
    let valid = many
        .validate_fields(
            "t",
            &json!({ "a": [{ "id": UUID, "type": "t" }] }),
            &DRAFT,
            &mut Counter(0),
        )
        .unwrap();
    assert_eq!(valid.references.len(), 1);
    assert_eq!(valid.references[0].path, "/fields/a/0");
    assert_eq!(
        valid.references[0].target,
        ReferenceTarget::Entry {
            id: UUID.parse().unwrap(),
            content_type: Some("t".into()),
            allowed: vec!["other".into(), "t".into()],
            require_published: true,
        }
    );
}

#[test]
fn asset_rules() {
    let one = model("{ name = \"a\", type = \"asset\", accept = [\"image/*\"] }");
    same(&one, json!({ "id": UUID }));
    same(&one, json!({ "id": UUID, "alt": "é".repeat(500) }));
    bad(
        &one,
        json!({ "id": UUID, "alt": "é".repeat(501) }),
        "/fields/a/alt",
        "max_length",
    );
    bad(
        &one,
        json!({ "id": UUID, "alt": "a\u{0}" }),
        "/fields/a/alt",
        "invalid_character",
    );
    bad(
        &one,
        json!({ "id": UUID, "alt": 3 }),
        "/fields/a/alt",
        "type_mismatch",
    );
    bad(&one, json!({ "id": 3 }), "/fields/a/id", "type_mismatch");
    bad(&one, json!("x"), "/fields/a", "type_mismatch");
    let many = model("{ name = \"a\", type = \"asset\", cardinality = \"many\", max_items = 1 }");
    same(&many, json!([{ "id": UUID, "alt": "Team" }]));
    bad(
        &many,
        json!([{ "id": UUID }, { "id": UUID }]),
        "/fields/a",
        "max_items",
    );
    let valid = one
        .validate_fields(
            "t",
            &json!({ "a": { "id": UUID } }),
            &DRAFT,
            &mut Counter(0),
        )
        .unwrap();
    assert_eq!(
        valid.references[0].target,
        ReferenceTarget::Asset {
            id: UUID.parse().unwrap(),
            accept: vec!["image/*".into()],
        }
    );
}

#[test]
fn component_rules() {
    let m = model("{ name = \"a\", type = \"component\", component = \"quote\" }");
    same(&m, json!({ "text": "Ship small.", "by": "The team" }));
    same(&m, json!({}));
    bad(&m, json!({ "text": 1 }), "/fields/a/text", "type_mismatch");
    bad(
        &m,
        json!({ "text": "x", "colour": "red" }),
        "/fields/a",
        "unknown_field",
    );
    bad(&m, json!("text"), "/fields/a", "type_mismatch");
    let errors = validate(&m, &json!({ "a": { "by": "x" } }), &PUBLISH).unwrap_err();
    assert_eq!(errors, pairs(&[("/fields/a/text", "required")]));
}

#[test]
fn list_rules() {
    let m = model(
        "{ name = \"a\", type = \"list\", of = { type = \"string\", max_length = 3 }, min_items = 1, max_items = 2 }",
    );
    same(&m, json!(["ab", "cd"]));
    same(&m, json!([]));
    bad(&m, json!(["ab", "abcd"]), "/fields/a/1", "max_length");
    bad(&m, json!(["a", "b", "c"]), "/fields/a", "max_items");
    bad(&m, json!("ab"), "/fields/a", "type_mismatch");
    let errors = validate(&m, &json!({ "a": [] }), &PUBLISH).unwrap_err();
    assert_eq!(errors, pairs(&[("/fields/a", "min_items")]));
    let default_max = model("{ name = \"a\", type = \"list\", of = { type = \"integer\" } }");
    same(&default_max, json!(vec![1; 100]));
    bad(&default_max, json!(vec![1; 101]), "/fields/a", "max_items");
}

#[test]
fn component_lists_and_blocks_carry_keys() {
    let m = model(
        "{ name = \"a\", type = \"list\", of = { type = \"component\", component = \"quote\" } },\n{ name = \"b\", type = \"blocks\", components = [\"quote\"], max_items = 3 }",
    );
    let fields = validate(
        &m,
        &json!({
            "a": [{ "_key": "k7Qa", "text": "one" }, { "text": "two" }],
            "b": [{ "_type": "quote", "text": "three" }],
        }),
        &DRAFT,
    )
    .unwrap();
    assert_eq!(
        Value::Object(fields),
        json!({
            "a": [{ "_key": "k7Qa", "text": "one" }, { "_key": "g1", "text": "two" }],
            "b": [{ "_key": "g2", "_type": "quote", "text": "three" }],
        })
    );
    let errors = validate(
        &m,
        &json!({
            "a": [{ "_key": "x", "text": "1" }, { "_key": "x", "text": "2" }, { "_key": "bad key", "text": "3" }, "four"],
            "b": [{ "_key": "q", "_type": "seo" }, { "_key": "r", "_type": "quote", "text": 5 }],
        }),
        &DRAFT,
    )
    .unwrap_err();
    assert_eq!(
        errors,
        pairs(&[
            ("/fields/a/1", "duplicate_key"),
            ("/fields/a/2/_key", "pattern"),
            ("/fields/a/3", "type_mismatch"),
            ("/fields/b/q/_type", "type_mismatch"),
            ("/fields/b/r/text", "type_mismatch"),
        ])
    );
    assert!(
        valid_key("k7Qa")
            && valid_key(&"a".repeat(32))
            && !valid_key(&"a".repeat(33))
            && !valid_key("a/b")
    );
}

#[test]
fn json_rules() {
    let m = model("{ name = \"a\", type = \"json\", max_bytes = 23 }");
    same(&m, json!({ "a": [1, 2.5, null, true] }));
    bad(
        &m,
        json!({ "a": [1, 2.5, null, false] }),
        "/fields/a",
        "json_too_large",
    );
    bad(&m, json!({ "k\u{0}": 1 }), "/fields/a", "invalid_character");
    bad(&m, json!(["\u{0}"]), "/fields/a", "invalid_character");
    bad(&m, json!([1_u64 << 60]), "/fields/a", "type_mismatch");
}

#[test]
fn required_and_min_items_are_publish_time_rules() {
    let m = model(
        "{ name = \"title\", type = \"string\", required = true },\n{ name = \"tags\", type = \"list\", of = { type = \"string\" }, required = true },\n{ name = \"body\", type = \"rich_text\", required = true },\n{ name = \"refs\", type = \"reference\", to = [\"other\"], cardinality = \"many\", min_items = 2 }",
    );
    let half = json!({ "title": "", "tags": [], "body": [], "refs": [] });
    assert!(
        validate(&m, &half, &DRAFT).is_ok(),
        "drafts may be incomplete"
    );
    assert!(validate(&m, &json!({}), &DRAFT).is_ok());
    assert_eq!(
        validate(&m, &half, &PUBLISH).unwrap_err(),
        pairs(&[
            ("/fields/title", "required"),
            ("/fields/tags", "required"),
            ("/fields/body", "required"),
            ("/fields/refs", "min_items"),
        ]),
        "empty string, list and rich text count as absent"
    );
    assert_eq!(
        validate(&m, &json!({ "title": null }), &PUBLISH).unwrap_err(),
        pairs(&[
            ("/fields/title", "required"),
            ("/fields/tags", "required"),
            ("/fields/body", "required"),
        ]),
        "null is absent"
    );
}

#[test]
fn shared_fields_belong_to_the_default_variant() {
    let m = model(
        "{ name = \"title\", type = \"string\", required = true },\n{ name = \"author\", type = \"string\", localized = false, required = true }",
    );
    let other = ValidateOptions {
        default_variant: false,
        ..PUBLISH
    };
    assert_eq!(
        validate(&m, &json!({ "title": "Bonjour", "author": "Ada" }), &other).unwrap_err(),
        pairs(&[("/fields/author", "field_not_localized")])
    );
    // Its `required` check runs when the default variant publishes.
    assert!(validate(&m, &json!({ "title": "Bonjour" }), &other).is_ok());
    assert_eq!(
        validate(&m, &json!({ "title": "Hello" }), &PUBLISH).unwrap_err(),
        pairs(&[("/fields/author", "required")])
    );
    // A non-localized type has one variant, so nothing is shared.
    let single = parse(&document(
        "[types.t]\naccess = \"public\"\nlocalized = false\nfields = [{ name = \"a\", type = \"string\" }]\n",
    ))
    .unwrap();
    assert!(validate(&single, &json!({ "a": "x" }), &other).is_ok());
}

#[test]
fn defaults_fill_new_variants_only() {
    let m = model(
        "{ name = \"a\", type = \"integer\", default = 3 },\n{ name = \"b\", type = \"decimal\", default = \"2\" }",
    );
    let created = ValidateOptions {
        new_variant: true,
        ..DRAFT
    };
    assert_eq!(
        Value::Object(validate(&m, &json!({}), &created).unwrap()),
        json!({ "a": 3, "b": "2.00" })
    );
    assert_eq!(
        Value::Object(validate(&m, &json!({ "a": 5 }), &created).unwrap()),
        json!({ "a": 5, "b": "2.00" })
    );
    assert!(validate(&m, &json!({}), &DRAFT).unwrap().is_empty());
}

#[test]
fn every_violation_is_reported_at_once() {
    let model = parse(&doc_block("content.md", "### Example", "toml")).unwrap();
    let fields = json!({
        "slug": "Not A Slug",
        "category": "opinion",
        "author": { "id": "nope" },
        "tags": ["ok", "x".repeat(41)],
        "sections": [{ "_type": "quote", "_key": "k7Qa", "text": "x".repeat(10_001) }],
        "reading_minutes": 0,
        "unknown": true,
    });
    let error = model
        .validate_fields("article", &fields, &PUBLISH, &mut Counter(0))
        .unwrap_err();
    assert_eq!(error.failure().code, "validation_failed");
    let details: Vec<(&str, &str)> = error
        .details()
        .iter()
        .map(|d| (d.path.as_str(), d.code.as_str()))
        .collect();
    assert_eq!(
        details,
        [
            ("/fields", "unknown_field"),
            ("/fields/title", "required"),
            ("/fields/slug", "pattern"),
            ("/fields/category", "not_in_enum"),
            ("/fields/author/id", "type_mismatch"),
            ("/fields/tags/1", "max_length"),
            ("/fields/sections/k7Qa/text", "max_length"),
            ("/fields/reading_minutes", "min"),
        ]
    );
}

#[test]
fn details_carry_pointer_paths_and_no_user_text() {
    const MARK: &str = "ZQXMARK";
    let model = parse(&doc_block("content.md", "### Example", "toml")).unwrap();
    let marked = |s: &str| format!("{s} {MARK} /{MARK}~1");
    let fields = json!({
        MARK: marked("unknown field"),
        "title": format!("{MARK}\n"),
        "slug": marked("slug"),
        "category": marked("enum"),
        "author": { "id": marked("id"), MARK: 1 },
        "tags": [marked("x".repeat(40).as_str())],
        "body": [
            { "_type": MARK, "_key": marked("key") },
            { "_type": "block", "_key": "b1", "style": MARK, "markDefs": [{ "_type": "link", "_key": "l1", "href": marked("javascript:") }],
              "children": [{ "_type": "span", "_key": marked("k"), "text": MARK, "marks": [MARK] }] },
        ],
        "sections": [{ "_type": "quote", "_key": "s1", MARK: marked("x") }, { "_type": MARK, "_key": marked("k") }],
        "seo": { "title": marked("x".repeat(70).as_str()), "image": { "id": UUID, "alt": marked(&"x".repeat(500)) } },
        "reading_minutes": marked("n"),
    });
    let error = model
        .validate_fields("article", &fields, &PUBLISH, &mut Counter(0))
        .unwrap_err();
    let ValueError::Invalid(details) = error else {
        panic!("{error:?}")
    };
    assert!(details.len() >= 12, "{details:?}");
    for detail in &details {
        assert!(detail.path.starts_with("/fields"), "{detail:?}");
        assert!(!detail.path.contains(MARK), "user text in {detail:?}");
        assert!(
            merezha_content::DETAIL_CODES.contains(&detail.code.as_str()),
            "{detail:?}"
        );
    }
}

#[test]
fn detail_paths_fit_the_platform_bound() {
    // Four levels of long names and keys exceed 256 bytes; the path is cut
    // at a segment boundary instead of dropping the detail.
    let long = |p: &str| format!("{p}{}", "x".repeat(47));
    let (c1, c2, c3) = (long("a"), long("b"), long("c"));
    let field = format!("f{}", "x".repeat(63));
    let source = document(&format!(
        "[components.{c1}]\nfields = [{{ name = \"{field}\", type = \"string\", max_length = 1 }}]\n\
         [components.{c2}]\nfields = [{{ name = \"{field}\", type = \"blocks\", components = [\"{c1}\"] }}]\n\
         [components.{c3}]\nfields = [{{ name = \"{field}\", type = \"blocks\", components = [\"{c2}\"] }}]\n\
         [types.t]\naccess = \"public\"\nfields = [{{ name = \"{field}\", type = \"blocks\", components = [\"{c3}\"] }}]\n"
    ));
    let model = parse(&source).unwrap();
    let key = "k".repeat(32);
    let item = |t: &str, inner: Value| json!([{ "_type": t, "_key": key, field.clone(): inner }]);
    let value = item(&c3, item(&c2, item(&c1, json!("too long"))));
    let errors = validate(&model, &json!({ field.clone(): value }), &DRAFT).unwrap_err();
    assert_eq!(errors.len(), 1);
    let (path, code) = &errors[0];
    assert_eq!(code, "max_length");
    assert!(
        path.len() <= 256 && path.starts_with(&format!("/fields/{field}/{key}")),
        "{path}"
    );
}

#[test]
fn key_generation_failures_are_reported_as_failures() {
    struct Broken;
    impl merezha_content::KeySource for Broken {
        fn next_key(&mut self) -> Result<String, merezha_domain::Failure> {
            Err(merezha_domain::Failure::transient("random_unavailable"))
        }
    }
    let m = model("{ name = \"a\", type = \"blocks\", components = [\"quote\"] }");
    let error = m
        .validate_fields(
            "t",
            &json!({ "a": [{ "_type": "quote", "text": "x" }] }),
            &DRAFT,
            &mut Broken,
        )
        .unwrap_err();
    assert_eq!(error.failure().code, "random_unavailable");
    let error = m
        .validate_fields("ghost", &json!({}), &DRAFT, &mut Broken)
        .unwrap_err();
    assert_eq!(error.failure().code, "content_type_not_found");
    let error = m
        .validate_fields("t", &json!([]), &DRAFT, &mut Broken)
        .unwrap_err();
    assert_eq!(error.failure().code, "invalid_json");
    // Random keys are 12 characters of [A-Za-z0-9].
    let mut keys = merezha_content::RandomKeys(&merezha_platform::OsRandom);
    let key = merezha_content::KeySource::next_key(&mut keys).unwrap();
    assert_eq!(key.len(), 12);
    assert!(key.bytes().all(|b| b.is_ascii_alphanumeric()));
    let _ = Stage::Draft;
}

#[test]
fn items_with_generated_keys_are_addressed_by_index() {
    let m = model(
        "{ name = \"a\", type = \"list\", of = { type = \"component\", component = \"quote\" } },\n{ name = \"r\", type = \"rich_text\", embeds = [\"asset\"] }",
    );
    let errors = validate(
        &m,
        &json!({ "a": [{ "_key": "mine", "text": 1 }, { "text": 2 }] }),
        &DRAFT,
    )
    .unwrap_err();
    assert_eq!(
        errors,
        pairs(&[
            ("/fields/a/mine/text", "type_mismatch"),
            ("/fields/a/1/text", "type_mismatch")
        ]),
        "a key the client sent is kept; a generated one never appears"
    );
    let valid = m
        .validate_fields(
            "t",
            &json!({ "r": [
                { "_type": "asset", "_key": "sent", "id": UUID },
                { "_type": "asset", "id": UUID },
                { "_type": "block", "style": "normal", "markDefs": [],
                  "children": [{ "_type": "span", "text": "x\u{0}" }] },
            ] }),
            &DRAFT,
            &mut Counter(0),
        )
        .unwrap_err();
    assert_eq!(
        valid
            .details()
            .iter()
            .map(|d| d.path.as_str())
            .collect::<Vec<_>>(),
        ["/fields/r/2/children/0/text"]
    );
    let valid = m
        .validate_fields(
            "t",
            &json!({ "r": [{ "_type": "asset", "_key": "sent", "id": UUID }, { "_type": "asset", "id": UUID }] }),
            &DRAFT,
            &mut Counter(0),
        )
        .unwrap();
    let paths: Vec<_> = valid.references.iter().map(|r| r.path.as_str()).collect();
    assert_eq!(paths, ["/fields/r/sent", "/fields/r/1"]);
}

#[test]
fn generated_keys_never_collide_with_keys_later_in_the_array() {
    let m = model(
        "{ name = \"a\", type = \"list\", of = { type = \"component\", component = \"quote\" } }",
    );
    // Counter yields g1 first, which a later item already carries.
    let fields = validate(
        &m,
        &json!({ "a": [{ "text": "one" }, { "_key": "g1", "text": "two" }, { "text": "three" }] }),
        &DRAFT,
    )
    .unwrap();
    let keys: Vec<_> = fields["a"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["_key"].as_str().unwrap())
        .collect();
    assert_eq!(keys, ["g2", "g1", "g3"]);
}

#[test]
fn rich_text_without_text_or_embeds_is_absent_for_required() {
    let m = model("{ name = \"a\", type = \"rich_text\", required = true, embeds = [\"asset\"] }");
    let empty_block = json!({ "_type": "block", "_key": "b", "style": "normal", "markDefs": [],
        "children": [{ "_type": "span", "_key": "s", "text": "", "marks": [] }] });
    assert_eq!(
        validate(&m, &json!({ "a": [empty_block.clone(), { "_type": "block", "_key": "c", "style": "normal", "markDefs": [], "children": [] }] }), &PUBLISH).unwrap_err(),
        pairs(&[("/fields/a", "required")])
    );
    let mut text = empty_block.clone();
    text["children"][0]["text"] = json!("Hello");
    assert!(validate(&m, &json!({ "a": [text] }), &PUBLISH).is_ok());
    assert!(
        validate(
            &m,
            &json!({ "a": [empty_block, { "_type": "asset", "_key": "x", "id": UUID }] }),
            &PUBLISH
        )
        .is_ok(),
        "an embed is content"
    );
}

#[test]
fn numbers_of_the_right_kind_beyond_range_are_min_or_max() {
    let int = model("{ name = \"a\", type = \"integer\" }");
    for (text, code) in [
        ("1e20", "max"),
        ("-1e20", "min"),
        ("123456789012345678901234567890", "max"),
        ("-123456789012345678901234567890", "min"),
        ("1.0", "type_mismatch"),
        ("2.5", "type_mismatch"),
    ] {
        bad(&int, serde_json::from_str(text).unwrap(), "/fields/a", code);
    }
    ok(&int, serde_json::from_str("-0").unwrap(), json!(0));
    let float = model("{ name = \"a\", type = \"float\" }");
    bad(&float, json!(9_007_199_254_740_992_i64), "/fields/a", "max");
    bad(
        &float,
        json!(-9_007_199_254_740_992_i64),
        "/fields/a",
        "min",
    );
    bad(
        &float,
        serde_json::from_str("1e300").unwrap(),
        "/fields/a",
        "max",
    );
    same(&float, json!(1.5e300_f64.recip()));
}
