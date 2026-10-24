//! Diff classification without a database: every row of the schema
//! changes table, plus the rules the table implies for type-specific keys.
use merezha_content::{
    ActiveModel, ChangeClass, ContentModel, ModelOptions, Plan, Status, change_classes, diff_models,
};
use serde_json::Value;

fn parse(source: &str) -> ContentModel {
    let options = ModelOptions {
        permissions: &["partners.read"],
        media: true,
        organizations: true,
    };
    ContentModel::parse(source, &options).unwrap_or_else(|e| panic!("{e}\n{source}"))
}

/// A base model whose `t` type has `fields`, plus `rest`.
fn base(version: u32, locales: &str, fields: &str, rest: &str) -> String {
    format!(
        r#"version = {version}
locales = {locales}

[components.quote]
fields = [{{ name = "text", type = "text" }}, {{ name = "by", type = "string" }}]

[components.card]
fields = [{{ name = "title", type = "string" }}]

[types.v]
access = "public"
fields = [{{ name = "a", type = "string" }}]

[types.t]
access = "public"
title_field = "title"
fields = [
{fields}
]
{rest}"#
    )
}

const LOCALES: &str = r#"[{ code = "en", default = true }, { code = "fr" }]"#;

/// The changes from `old` to `new` field lists as `(class, path, code)`.
fn changes(old: &str, new: &str) -> Vec<(ChangeClass, String, &'static str)> {
    changes_full(LOCALES, old, "", LOCALES, new, "")
}

fn changes_full(
    old_locales: &str,
    old: &str,
    old_rest: &str,
    new_locales: &str,
    new: &str,
    new_rest: &str,
) -> Vec<(ChangeClass, String, &'static str)> {
    let a = parse(&base(1, old_locales, old, old_rest));
    let b = parse(&base(2, new_locales, new, new_rest));
    diff_models(&a, &b)
        .into_iter()
        .map(|c| (c.class, c.path, c.code))
        .collect()
}

fn one(old: &str, new: &str) -> (ChangeClass, String, &'static str) {
    let found = changes(old, new);
    assert_eq!(found.len(), 1, "{old} -> {new}: {found:?}");
    found.into_iter().next().unwrap()
}

const TITLE: &str = r#"{ name = "title", type = "string" },"#;

fn with(field: &str) -> String {
    format!("{TITLE}\n{field}")
}

#[test]
fn identical_models_and_spelled_out_defaults_have_no_changes() {
    assert!(changes(TITLE, TITLE).is_empty());
    assert!(
        changes(
            TITLE,
            r#"{ name = "title", type = "string", max_length = 255, required = false },"#
        )
        .is_empty()
    );
}

#[test]
fn field_rule_changes_are_classified() {
    use ChangeClass::*;
    let f = "types.t.fields.f";
    let cases: Vec<(&str, &str, ChangeClass, String, &str)> = vec![
        // Bounds: raising a minimum or lowering a maximum tightens.
        (
            r#"{ name = "f", type = "string", max_length = 50 }"#,
            r#"{ name = "f", type = "string", max_length = 40 }"#,
            Tightening,
            format!("{f}.max_length"),
            "rule_tightened",
        ),
        (
            r#"{ name = "f", type = "string", max_length = 50 }"#,
            r#"{ name = "f", type = "string", max_length = 60 }"#,
            Additive,
            format!("{f}.max_length"),
            "rule_relaxed",
        ),
        (
            r#"{ name = "f", type = "string" }"#,
            r#"{ name = "f", type = "string", min_length = 2 }"#,
            Tightening,
            format!("{f}.min_length"),
            "rule_tightened",
        ),
        (
            r#"{ name = "f", type = "string", min_length = 2 }"#,
            r#"{ name = "f", type = "string" }"#,
            Additive,
            format!("{f}.min_length"),
            "rule_relaxed",
        ),
        (
            r#"{ name = "f", type = "integer", min = 1 }"#,
            r#"{ name = "f", type = "integer", min = 0 }"#,
            Additive,
            format!("{f}.min"),
            "rule_relaxed",
        ),
        (
            r#"{ name = "f", type = "integer", max = 10 }"#,
            r#"{ name = "f", type = "integer", max = 9 }"#,
            Tightening,
            format!("{f}.max"),
            "rule_tightened",
        ),
        (
            r#"{ name = "f", type = "float", min = 0.5 }"#,
            r#"{ name = "f", type = "float", min = 0.25 }"#,
            Additive,
            format!("{f}.min"),
            "rule_relaxed",
        ),
        (
            r#"{ name = "f", type = "decimal", max = "9.50" }"#,
            r#"{ name = "f", type = "decimal", max = "10" }"#,
            Additive,
            format!("{f}.max"),
            "rule_relaxed",
        ),
        (
            r#"{ name = "f", type = "decimal", max = "10" }"#,
            r#"{ name = "f", type = "decimal", max = "9.5" }"#,
            Tightening,
            format!("{f}.max"),
            "rule_tightened",
        ),
        (
            r#"{ name = "f", type = "decimal", precision = 18 }"#,
            r#"{ name = "f", type = "decimal", precision = 12 }"#,
            Tightening,
            format!("{f}.precision"),
            "rule_tightened",
        ),
        (
            r#"{ name = "f", type = "date", min = "2020-01-01" }"#,
            r#"{ name = "f", type = "date", min = "2021-01-01" }"#,
            Tightening,
            format!("{f}.min"),
            "rule_tightened",
        ),
        (
            r#"{ name = "f", type = "datetime", max = "2030-01-01T00:00:00Z" }"#,
            r#"{ name = "f", type = "datetime", max = "2031-01-01T00:00:00+01:00" }"#,
            Additive,
            format!("{f}.max"),
            "rule_relaxed",
        ),
        (
            r#"{ name = "f", type = "json" }"#,
            r#"{ name = "f", type = "json", max_bytes = 1024 }"#,
            Tightening,
            format!("{f}.max_bytes"),
            "rule_tightened",
        ),
        // Patterns: added or changed tightens, removed relaxes.
        (
            r#"{ name = "f", type = "string" }"#,
            r#"{ name = "f", type = "string", pattern = "[a-z]+" }"#,
            Tightening,
            format!("{f}.pattern"),
            "rule_tightened",
        ),
        (
            r#"{ name = "f", type = "string", pattern = "[a-z]+" }"#,
            r#"{ name = "f", type = "string", pattern = "[a-z0-9]+" }"#,
            Tightening,
            format!("{f}.pattern"),
            "rule_tightened",
        ),
        (
            r#"{ name = "f", type = "string", pattern = "[a-z]+" }"#,
            r#"{ name = "f", type = "string" }"#,
            Additive,
            format!("{f}.pattern"),
            "rule_relaxed",
        ),
        // `required`.
        (
            r#"{ name = "f", type = "string" }"#,
            r#"{ name = "f", type = "string", required = true }"#,
            Tightening,
            format!("{f}.required"),
            "required_added",
        ),
        (
            r#"{ name = "f", type = "string", required = true }"#,
            r#"{ name = "f", type = "string" }"#,
            Additive,
            format!("{f}.required"),
            "required_removed",
        ),
        // Enum values and sets.
        (
            r#"{ name = "f", type = "enum", values = ["a"] }"#,
            r#"{ name = "f", type = "enum", values = ["a", "b"] }"#,
            Additive,
            format!("{f}.values"),
            "rule_relaxed",
        ),
        (
            r#"{ name = "f", type = "enum", values = ["a", "b"] }"#,
            r#"{ name = "f", type = "enum", values = ["b"] }"#,
            Refused,
            format!("{f}.values"),
            "value_removed",
        ),
        (
            r#"{ name = "f", type = "reference", to = ["t"] }"#,
            r#"{ name = "f", type = "reference", to = ["t", "v"] }"#,
            Additive,
            format!("{f}.to"),
            "rule_relaxed",
        ),
        (
            r#"{ name = "f", type = "reference", to = ["t", "v"] }"#,
            r#"{ name = "f", type = "reference", to = ["t"] }"#,
            Tightening,
            format!("{f}.to"),
            "rule_tightened",
        ),
        (
            r#"{ name = "f", type = "reference", to = ["t"] }"#,
            r#"{ name = "f", type = "reference", to = ["t"], require_published = true }"#,
            Tightening,
            format!("{f}.require_published"),
            "required_added",
        ),
        (
            r#"{ name = "f", type = "reference", to = ["t"] }"#,
            r#"{ name = "f", type = "reference", to = ["t"], cardinality = "many" }"#,
            Refused,
            format!("{f}.cardinality"),
            "cardinality_changed",
        ),
        (
            r#"{ name = "f", type = "asset", accept = ["image/png"] }"#,
            r#"{ name = "f", type = "asset", accept = ["image/png", "image/webp"] }"#,
            Additive,
            format!("{f}.accept"),
            "rule_relaxed",
        ),
        (
            r#"{ name = "f", type = "decimal", scale = 2 }"#,
            r#"{ name = "f", type = "decimal", scale = 3 }"#,
            Refused,
            format!("{f}.scale"),
            "scale_changed",
        ),
        (
            r#"{ name = "f", type = "component", component = "quote" }"#,
            r#"{ name = "f", type = "component", component = "card" }"#,
            Refused,
            format!("{f}.component"),
            "component_changed",
        ),
        (
            r#"{ name = "f", type = "blocks", components = ["quote", "card"] }"#,
            r#"{ name = "f", type = "blocks", components = ["quote"] }"#,
            Refused,
            format!("{f}.components"),
            "value_removed",
        ),
        (
            r#"{ name = "f", type = "blocks", components = ["quote"] }"#,
            r#"{ name = "f", type = "blocks", components = ["quote", "card"] }"#,
            Additive,
            format!("{f}.components"),
            "rule_relaxed",
        ),
        (
            r#"{ name = "f", type = "rich_text", styles = ["normal", "h2"] }"#,
            r#"{ name = "f", type = "rich_text", styles = ["normal"] }"#,
            Tightening,
            format!("{f}.styles"),
            "rule_tightened",
        ),
        (
            r#"{ name = "f", type = "rich_text" }"#,
            r#"{ name = "f", type = "rich_text", entry_link_to = ["t"], annotations = ["link", "entry_link"] }"#,
            Additive,
            format!("{f}.annotations"),
            "rule_relaxed",
        ),
        // List items compare recursively.
        (
            r#"{ name = "f", type = "list", of = { type = "string", max_length = 40 } }"#,
            r#"{ name = "f", type = "list", of = { type = "string", max_length = 30 } }"#,
            Tightening,
            format!("{f}.of.max_length"),
            "rule_tightened",
        ),
        (
            r#"{ name = "f", type = "list", of = { type = "string" } }"#,
            r#"{ name = "f", type = "list", of = { type = "integer" } }"#,
            Refused,
            format!("{f}.of"),
            "field_type_changed",
        ),
        // Documentation, defaults and slug sources.
        (
            r#"{ name = "f", type = "string" }"#,
            r#"{ name = "f", type = "string", description = "Shown in forms" }"#,
            Additive,
            format!("{f}.description"),
            "description_changed",
        ),
        (
            r#"{ name = "f", type = "string", default = "a" }"#,
            r#"{ name = "f", type = "string", default = "b" }"#,
            Additive,
            format!("{f}.default"),
            "default_changed",
        ),
        (
            r#"{ name = "f", type = "slug", from = "title" }"#,
            r#"{ name = "f", type = "slug", from = "other" }"#,
            Additive,
            format!("{f}.from"),
            "slug_source_changed",
        ),
        // Index flags.
        (
            r#"{ name = "f", type = "string" }"#,
            r#"{ name = "f", type = "string", filterable = true }"#,
            Index,
            format!("{f}.filterable"),
            "filterable_changed",
        ),
        (
            r#"{ name = "f", type = "string", filterable = true }"#,
            r#"{ name = "f", type = "string" }"#,
            Index,
            format!("{f}.filterable"),
            "filterable_changed",
        ),
        (
            r#"{ name = "f", type = "string", unique = true }"#,
            r#"{ name = "f", type = "string" }"#,
            Index,
            format!("{f}.unique"),
            "unique_removed",
        ),
        (
            r#"{ name = "f", type = "string" }"#,
            r#"{ name = "f", type = "string", unique = true }"#,
            Refused,
            format!("{f}.unique"),
            "unique_added",
        ),
        (
            r#"{ name = "f", type = "string" }"#,
            r#"{ name = "f", type = "string", localized = false }"#,
            Refused,
            format!("{f}.localized"),
            "localized_changed",
        ),
        (
            r#"{ name = "f", type = "string" }"#,
            r#"{ name = "f", type = "text" }"#,
            Refused,
            f.to_owned(),
            "field_type_changed",
        ),
    ];
    for (old, new, class, path, code) in cases {
        let old = with(&format!(
            "{old},\n{{ name = \"other\", type = \"string\" }},"
        ));
        let new = with(&format!(
            "{new},\n{{ name = \"other\", type = \"string\" }},"
        ));
        let found = changes(&old, &new);
        let wanted = (class, path.clone(), code);
        assert!(found.contains(&wanted), "{wanted:?} not in {found:?}");
        if class != Refused {
            assert!(
                found.iter().all(|(c, ..)| *c != Refused),
                "{path}: {found:?}"
            );
        }
    }
}

#[test]
fn fields_types_components_and_locales_are_matched_by_name() {
    use ChangeClass::*;
    let added = one(
        TITLE,
        &with(r#"{ name = "f", type = "string", required = true },"#),
    );
    assert_eq!(
        added,
        (Additive, "types.t.fields.f".to_owned(), "field_added")
    );
    // A new `unique` field activates (its index is built first).
    let added = one(
        TITLE,
        &with(r#"{ name = "code", type = "string", unique = true },"#),
    );
    assert_eq!(added.0, Additive);
    let removed = one(&with(r#"{ name = "f", type = "string" },"#), TITLE);
    assert_eq!(
        removed,
        (Refused, "types.t.fields.f".to_owned(), "field_removed")
    );
    let reordered = changes(
        &with(r#"{ name = "f", type = "string" },"#),
        r#"{ name = "f", type = "string" },
{ name = "title", type = "string" },"#,
    );
    assert_eq!(
        reordered,
        [(Additive, "types.t.fields".to_owned(), "order_changed")]
    );

    let new_type = changes_full(
        LOCALES,
        TITLE,
        "",
        LOCALES,
        TITLE,
        "[types.u]\naccess = \"public\"\nfields = [{ name = \"a\", type = \"string\" }]\n",
    );
    assert_eq!(new_type, [(Additive, "types.u".to_owned(), "type_added")]);
    let gone = changes_full(
        LOCALES,
        TITLE,
        "[types.u]\naccess = \"public\"\nfields = [{ name = \"a\", type = \"string\" }]\n",
        LOCALES,
        TITLE,
        "",
    );
    assert_eq!(gone, [(Refused, "types.u".to_owned(), "type_removed")]);
    let access = changes_full(
        LOCALES,
        TITLE,
        "[types.u]\naccess = \"public\"\nfields = [{ name = \"a\", type = \"string\" }]\n",
        LOCALES,
        TITLE,
        "[types.u]\naccess = { permission = \"partners.read\" }\nfields = [{ name = \"a\", type = \"string\" }]\n",
    );
    assert_eq!(
        access,
        [(Access, "types.u.access".to_owned(), "access_changed")]
    );
    let kind = changes_full(
        LOCALES,
        TITLE,
        "[types.u]\naccess = \"public\"\nfields = [{ name = \"a\", type = \"string\" }]\n",
        LOCALES,
        TITLE,
        "[types.u]\nkind = \"singleton\"\naccess = \"public\"\nfields = [{ name = \"a\", type = \"string\" }]\n",
    );
    assert_eq!(kind, [(Refused, "types.u.kind".to_owned(), "kind_changed")]);
    let component = changes_full(
        LOCALES,
        TITLE,
        "[components.extra]\nfields = [{ name = \"a\", type = \"string\" }]\n",
        LOCALES,
        TITLE,
        "",
    );
    assert_eq!(
        component,
        [(Refused, "components.extra".to_owned(), "component_removed")]
    );
    let component = changes_full(
        LOCALES,
        TITLE,
        "",
        LOCALES,
        TITLE,
        "[components.extra]\nfields = [{ name = \"a\", type = \"string\" }]\n",
    );
    assert_eq!(
        component,
        [(Additive, "components.extra".to_owned(), "component_added")]
    );

    let three = r#"[{ code = "en", default = true }, { code = "fr" }, { code = "fr-BE", fallback = "fr" }]"#;
    let added = changes_full(LOCALES, TITLE, "", three, TITLE, "");
    assert_eq!(
        added,
        [(Additive, "locales.fr-BE".to_owned(), "locale_added")]
    );
    let fallback = changes_full(
        three,
        TITLE,
        "",
        r#"[{ code = "en", default = true }, { code = "fr" }, { code = "fr-BE" }]"#,
        TITLE,
        "",
    );
    assert_eq!(
        fallback,
        [(
            Additive,
            "locales.fr-BE.fallback".to_owned(),
            "fallback_changed"
        )]
    );
    let removed = changes_full(three, TITLE, "", LOCALES, TITLE, "");
    assert_eq!(
        removed,
        [(Refused, "locales.fr-BE".to_owned(), "locale_removed")]
    );
}

#[test]
fn plans_follow_the_version_rules() {
    let v1 = parse(&base(1, LOCALES, TITLE, ""));
    let active = ActiveModel {
        version: 1,
        fingerprint: v1.fingerprint().to_owned(),
        definition: serde_json::from_str::<Value>(v1.canonical_json()).unwrap(),
    };
    assert_eq!(Plan::new(None, &v1).status, Status::Fresh);
    assert_eq!(Plan::new(None, &v1).change_classes(), ["additive"]);
    assert_eq!(Plan::new(Some(&active), &v1).status, Status::Current);
    let same_version = parse(&base(
        1,
        LOCALES,
        &with(r#"{ name = "f", type = "string" },"#),
        "",
    ));
    let plan = Plan::new(Some(&active), &same_version);
    assert_eq!(plan.status, Status::VersionUnchanged);
    assert_eq!(
        plan.failure().unwrap().code,
        "content_model_version_unchanged"
    );
    let pending = parse(&base(
        2,
        LOCALES,
        &with(r#"{ name = "f", type = "string" },"#),
        "",
    ));
    let plan = Plan::new(Some(&active), &pending);
    assert_eq!(plan.status, Status::Pending);
    assert!(plan.failure().is_none());
    assert_eq!(change_classes(&plan.changes), ["additive"]);
    let breaking = parse(&base(2, r#"[{ code = "en", default = true }]"#, TITLE, ""));
    let plan = Plan::new(Some(&active), &breaking);
    assert_eq!(plan.status, Status::Breaking);
    assert_eq!(plan.failure().unwrap().code, "content_model_breaking");
    assert_eq!(plan.refused().count(), 1);
    let newer = ActiveModel {
        version: 5,
        ..active
    };
    assert_eq!(Plan::new(Some(&newer), &pending).status, Status::Outdated);
}
