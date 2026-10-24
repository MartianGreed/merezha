//! CT-C1 Portable Text: the design's accepted structure validates as
//! written, and each rejection is reported with its JSON pointer.
mod common;

use common::*;
use merezha_content::{ContentModel, ReferenceTarget};
use serde_json::{Value, json};

fn article() -> ContentModel {
    parse(&doc_block("content.md", "### Example", "toml")).unwrap()
}

fn body(model: &ContentModel, value: Value) -> Result<Value, Vec<(String, String)>> {
    model
        .validate_fields(
            "article",
            &json!({ "body": value }),
            &DRAFT,
            &mut Counter(0),
        )
        .map(|v| v.fields["body"].clone())
        .map_err(|e| {
            e.details()
                .iter()
                .map(|d| (d.path.clone(), d.code.clone()))
                .collect()
        })
}

fn block(extra: Value) -> Value {
    let mut base = json!({
        "_type": "block", "_key": "b1", "style": "normal", "markDefs": [],
        "children": [{ "_type": "span", "_key": "s1", "text": "Hello", "marks": [] }]
    });
    if let (Value::Object(base), Value::Object(extra)) = (&mut base, extra) {
        base.extend(extra);
    }
    base
}

#[test]
fn the_design_example_is_accepted_as_written() {
    let model = article();
    let example: Value =
        serde_json::from_str(&doc_block("content.md", "### Rich text format", "json")).unwrap();
    let stored = body(&model, example.clone()).unwrap_or_else(|e| panic!("{e:?}"));
    assert_eq!(stored, example, "valid Portable Text is stored unchanged");
    let valid = model
        .validate_fields(
            "article",
            &json!({ "body": example }),
            &DRAFT,
            &mut Counter(0),
        )
        .unwrap();
    let references: Vec<_> = valid
        .references
        .iter()
        .map(|r| {
            (
                r.path.as_str(),
                matches!(r.target, ReferenceTarget::Asset { .. }),
            )
        })
        .collect();
    assert_eq!(
        references,
        [
            ("/fields/body/b2/markDefs/l2", false),
            ("/fields/body/c3", true)
        ]
    );
}

#[test]
fn missing_keys_are_generated_for_blocks_and_spans() {
    let model = article();
    let stored = body(
        &model,
        json!([{ "_type": "block", "style": "h2", "markDefs": [],
                 "children": [{ "_type": "span", "text": "Title" }] }]),
    )
    .unwrap();
    assert_eq!(
        stored,
        json!([{ "_type": "block", "_key": "g1", "style": "h2", "markDefs": [],
                 "children": [{ "_type": "span", "_key": "g2", "text": "Title", "marks": [] }] }])
    );
}

#[test]
fn each_rich_text_rejection_has_its_pointer() {
    let model = article();
    let invalid = "rich_text_invalid";
    let long_href = format!("https://example.com/{}", "a".repeat(2048));
    let cases: Vec<(Value, &str, &str)> = vec![
        (json!({ "_type": "block" }), "/fields/body", invalid),
        (
            json!([{ "_type": "table", "_key": "t1" }]),
            "/fields/body/t1",
            invalid,
        ),
        (json!([{ "_key": "t1" }]), "/fields/body/t1", invalid),
        (json!(["text"]), "/fields/body/0", "type_mismatch"),
        (
            json!([block(json!({ "style": "h1" }))]),
            "/fields/body/b1/style",
            invalid,
        ),
        (
            json!([block(json!({ "style": null }))]),
            "/fields/body/b1/style",
            invalid,
        ),
        (
            json!([block(json!({ "color": "red" }))]),
            "/fields/body/b1",
            invalid,
        ),
        (
            json!([block(json!({ "listItem": "check" }))]),
            "/fields/body/b1/listItem",
            invalid,
        ),
        (
            json!([block(json!({ "listItem": "bullet", "level": 7 }))]),
            "/fields/body/b1/level",
            invalid,
        ),
        (
            json!([block(json!({ "level": 1 }))]),
            "/fields/body/b1/level",
            invalid,
        ),
        (
            json!([block(json!({ "markDefs": null }))]),
            "/fields/body/b1/markDefs",
            invalid,
        ),
        (
            json!([block(
                json!({ "markDefs": [{ "_type": "link", "href": "https://a.example" }] })
            )]),
            "/fields/body/b1/markDefs/0",
            invalid,
        ),
        (
            json!([block(
                json!({ "markDefs": [{ "_type": "link", "_key": "strong", "href": "https://a.example" }] })
            )]),
            "/fields/body/b1/markDefs/0",
            invalid,
        ),
        (
            json!([block(json!({ "markDefs": [
            { "_type": "link", "_key": "l1", "href": "https://a.example" },
            { "_type": "link", "_key": "l1", "href": "https://b.example" }] }))]),
            "/fields/body/b1/markDefs/1",
            invalid,
        ),
        (
            json!([block(
                json!({ "markDefs": [{ "_type": "link", "_key": "l1", "href": "javascript:alert(1)" }] })
            )]),
            "/fields/body/b1/markDefs/l1",
            invalid,
        ),
        (
            json!([block(
                json!({ "markDefs": [{ "_type": "link", "_key": "l1", "href": "https://" }] })
            )]),
            "/fields/body/b1/markDefs/l1",
            invalid,
        ),
        (
            json!([block(
                json!({ "markDefs": [{ "_type": "link", "_key": "l1", "href": "https://a.example/a b" }] })
            )]),
            "/fields/body/b1/markDefs/l1",
            invalid,
        ),
        (
            json!([block(
                json!({ "markDefs": [{ "_type": "link", "_key": "l1", "href": long_href }] })
            )]),
            "/fields/body/b1/markDefs/l1",
            invalid,
        ),
        (
            json!([block(
                json!({ "markDefs": [{ "_type": "link", "_key": "l1", "href": "https://a.example", "target": "_blank" }] })
            )]),
            "/fields/body/b1/markDefs/l1",
            invalid,
        ),
        (
            json!([block(
                json!({ "markDefs": [{ "_type": "footnote", "_key": "f1" }] })
            )]),
            "/fields/body/b1/markDefs/f1",
            invalid,
        ),
        (
            json!([block(
                json!({ "markDefs": [{ "_type": "entry_link", "_key": "e1", "id": "x" }] })
            )]),
            "/fields/body/b1/markDefs/e1/id",
            "type_mismatch",
        ),
        (
            json!([block(
                json!({ "markDefs": [{ "_type": "entry_link", "_key": "e1", "id": "0192f7a0-5d1c-7c3e-9b1a-6f0e2c4d8a11", "type": "ghost" }] })
            )]),
            "/fields/body/b1/markDefs/e1",
            "reference_type",
        ),
        (
            json!([block(json!({ "children": "Hello" }))]),
            "/fields/body/b1/children",
            invalid,
        ),
        (
            json!([block(
                json!({ "children": [{ "_type": "image", "_key": "i1" }] })
            )]),
            "/fields/body/b1/children/i1",
            invalid,
        ),
        (
            json!([block(
                json!({ "children": [{ "_type": "span", "_key": "s1" }] })
            )]),
            "/fields/body/b1/children/s1/text",
            invalid,
        ),
        (
            json!([block(
                json!({ "children": [{ "_type": "span", "_key": "s1", "text": "a\u{0}" }] })
            )]),
            "/fields/body/b1/children/s1/text",
            "invalid_character",
        ),
        (
            json!([block(
                json!({ "children": [{ "_type": "span", "_key": "s1", "text": "a", "marks": ["underline"] }] })
            )]),
            "/fields/body/b1/children/s1/marks",
            invalid,
        ),
        (
            json!([block(
                json!({ "children": [{ "_type": "span", "_key": "s1", "text": "a", "marks": ["l9"] }] })
            )]),
            "/fields/body/b1/children/s1/marks",
            invalid,
        ),
        (
            json!([block(
                json!({ "children": [{ "_type": "span", "_key": "s1", "text": "a", "marks": ["em", "em"] }] })
            )]),
            "/fields/body/b1/children/s1/marks",
            invalid,
        ),
        (
            json!([block(
                json!({ "children": [{ "_type": "span", "_key": "s1", "text": "a", "bold": true }] })
            )]),
            "/fields/body/b1/children/s1",
            invalid,
        ),
        (
            json!([block(json!({ "children": [
            { "_type": "span", "_key": "s1", "text": "a" }, { "_type": "span", "_key": "s1", "text": "b" }] }))]),
            "/fields/body/b1/children/1",
            "duplicate_key",
        ),
        (
            json!([block(json!({})), block(json!({}))]),
            "/fields/body/1",
            "duplicate_key",
        ),
        (
            json!([{ "_type": "seo", "_key": "e1" }]),
            "/fields/body/e1",
            invalid,
        ),
        (
            json!([{ "_type": "quote", "_key": "q1", "text": 3 }]),
            "/fields/body/q1/text",
            "type_mismatch",
        ),
        (
            json!([{ "_type": "quote", "_key": "q1", "text": "x", "colour": "red" }]),
            "/fields/body/q1",
            "unknown_field",
        ),
        (
            json!([{ "_type": "asset", "_key": "a1", "id": "nope" }]),
            "/fields/body/a1/id",
            "type_mismatch",
        ),
    ];
    for (value, path, code) in cases {
        assert_eq!(
            body(&model, value.clone()),
            Err(pairs(&[(path, code)])),
            "{value}"
        );
    }
}

#[test]
fn rich_text_bounds() {
    let model = parse(&with_fields(
        "{ name = \"a\", type = \"rich_text\", max_blocks = 2, max_text_length = 5, embeds = [\"asset\"], annotations = [] }",
    ))
    .unwrap();
    let run = |value: Value| validate(&model, &json!({ "a": value }), &DRAFT);
    let span = |key: &str, text: &str| {
        json!({ "_type": "block", "_key": key, "style": "normal", "markDefs": [],
                "children": [{ "_type": "span", "_key": "s", "text": text, "marks": [] }] })
    };
    assert!(run(json!([span("a", "ab"), span("b", "cdé")])).is_ok());
    assert_eq!(
        run(json!([span("a", "ab"), span("b", "cdef")])).unwrap_err(),
        pairs(&[("/fields/a", "max_length")]),
        "max_text_length bounds the total span text"
    );
    assert_eq!(
        run(json!([span("a", ""), span("b", ""), span("c", "")])).unwrap_err(),
        pairs(&[("/fields/a", "max_items")]),
        "max_blocks bounds the block count"
    );
    // Annotations not declared on the field are refused.
    let link = json!([{ "_type": "block", "_key": "a", "style": "normal",
        "markDefs": [{ "_type": "link", "_key": "l", "href": "mailto:team@example.com" }], "children": [] }]);
    assert_eq!(
        run(link).unwrap_err(),
        pairs(&[("/fields/a/a/markDefs/l", "rich_text_invalid")])
    );
    // Asset embeds carry alt text with the asset rules.
    assert!(run(json!([{ "_type": "asset", "_key": "x", "id": "0192f7a1-0b44-7aa2-8c55-1d3e9f20a7b4", "alt": "Team" }])).is_ok());
    assert_eq!(
        run(json!([{ "_type": "asset", "_key": "x", "id": "0192f7a1-0b44-7aa2-8c55-1d3e9f20a7b4", "alt": "é".repeat(501) }])).unwrap_err(),
        pairs(&[("/fields/a/x/alt", "max_length")])
    );
    // Components not listed in `embeds` are refused.
    assert_eq!(
        run(json!([{ "_type": "quote", "_key": "q", "text": "x" }])).unwrap_err(),
        pairs(&[("/fields/a/q", "rich_text_invalid")])
    );
}

#[test]
fn entry_links_are_limited_by_entry_link_to() {
    let model = parse(&with_fields(
        "{ name = \"a\", type = \"rich_text\", annotations = [\"entry_link\"], entry_link_to = [\"other\"] }",
    ))
    .unwrap();
    let with_type = |t: &str| {
        json!({ "a": [{ "_type": "block", "_key": "b", "style": "normal",
            "markDefs": [{ "_type": "entry_link", "_key": "e", "id": "0192f7a0-5d1c-7c3e-9b1a-6f0e2c4d8a11", "type": t }],
            "children": [{ "_type": "span", "_key": "s", "text": "see", "marks": ["e"] }] }] })
    };
    assert!(validate(&model, &with_type("other"), &DRAFT).is_ok());
    assert_eq!(
        validate(&model, &with_type("t"), &DRAFT).unwrap_err(),
        pairs(&[("/fields/a/b/markDefs/e", "reference_type")])
    );
}
