//! The canonical JSON form of a model and its fingerprint.
//!
//! Every default is written out, so a model that spells a default and one
//! that omits it are the same model. Object keys are sorted and nothing is
//! indented, so TOML key order and whitespace never reach the fingerprint.
//! Array order is kept: it is the order of `fields`, `locales` and every
//! list in the file, which the model endpoint returns as written.
use crate::model::*;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;

pub(crate) fn render(model: &ContentModel) -> String {
    let locales: Vec<Value> = model
        .locales
        .iter()
        .map(|l| {
            let mut out = Map::new();
            out.insert("code".into(), json!(l.code));
            out.insert("default".into(), json!(l.default));
            if let Some(fallback) = &l.fallback {
                out.insert("fallback".into(), json!(fallback));
            }
            Value::Object(out)
        })
        .collect();
    let components: Map<String, Value> = model
        .components
        .iter()
        .map(|(name, c)| {
            let mut out = Map::new();
            describe(&mut out, &c.description);
            out.insert("fields".into(), fields(&c.fields, false));
            (name.clone(), Value::Object(out))
        })
        .collect();
    let types: Map<String, Value> = model
        .types
        .iter()
        .map(|(name, t)| {
            let mut out = Map::new();
            out.insert("kind".into(), json!(t.kind.as_str()));
            out.insert("scope".into(), json!(t.scope.as_str()));
            out.insert("localized".into(), json!(t.localized));
            out.insert(
                "access".into(),
                match &t.access {
                    Access::Public => json!("public"),
                    Access::Authenticated => json!("authenticated"),
                    Access::Permission(p) => json!({ "permission": p }),
                },
            );
            if let Some(title) = &t.title_field {
                out.insert("title_field".into(), json!(title));
            }
            describe(&mut out, &t.description);
            out.insert("fields".into(), fields(&t.fields, true));
            (name.clone(), Value::Object(out))
        })
        .collect();
    let value = json!({
        "version": model.version,
        "locales": locales,
        "components": components,
        "types": types,
    });
    let mut out = String::new();
    write(&value, &mut out);
    out
}

/// Lowercase hex SHA-256 of the canonical text.
pub(crate) fn fingerprint(canonical: &str) -> String {
    let digest = Sha256::digest(canonical.as_bytes());
    let mut hex = String::with_capacity(64);
    for byte in digest {
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

fn describe(out: &mut Map<String, Value>, description: &Option<String>) {
    if let Some(description) = description {
        out.insert("description".into(), json!(description));
    }
}

fn fields(fields: &[Field], top: bool) -> Value {
    Value::Array(fields.iter().map(|f| field(f, top, false)).collect())
}

fn field(f: &Field, top: bool, item: bool) -> Value {
    let mut out = Map::new();
    if !item {
        out.insert("name".into(), json!(f.name));
        out.insert("required".into(), json!(f.required));
    }
    if top {
        out.insert("localized".into(), json!(f.localized));
        out.insert("unique".into(), json!(f.unique));
        out.insert("filterable".into(), json!(f.filterable));
        out.insert("sortable".into(), json!(f.sortable));
    }
    describe(&mut out, &f.description);
    if let Some(default) = &f.default {
        out.insert("default".into(), default.clone());
    }
    out.insert("type".into(), json!(f.kind.type_name()));
    let mut put = |key: &str, value: Value| {
        out.insert(key.to_owned(), value);
    };
    let optional = |value: Option<Value>| value.unwrap_or(Value::Null);
    match &f.kind {
        FieldKind::String(r) => {
            put("min_length", optional(r.min_length.map(Value::from)));
            put("max_length", json!(r.max_length));
            put(
                "pattern",
                optional(r.pattern.as_ref().map(|p| json!(p.source))),
            );
        }
        FieldKind::Text(r) => {
            put("min_length", optional(r.min_length.map(Value::from)));
            put("max_length", json!(r.max_length));
        }
        FieldKind::RichText(r) => {
            put("styles", json!(r.styles));
            put("marks", json!(r.marks));
            put("annotations", json!(r.annotations));
            put("lists", json!(r.lists));
            put("embeds", json!(r.embeds));
            put("max_blocks", json!(r.max_blocks));
            put("max_text_length", json!(r.max_text_length));
            put(
                "entry_link_to",
                optional(r.entry_link_to.as_ref().map(|t| json!(t))),
            );
        }
        FieldKind::Integer(r) => {
            put("min", optional(r.min.map(Value::from)));
            put("max", optional(r.max.map(Value::from)));
        }
        FieldKind::Float(r) => {
            put("min", optional(r.min.map(Value::from)));
            put("max", optional(r.max.map(Value::from)));
        }
        FieldKind::Decimal(r) => {
            put("precision", json!(r.precision));
            put("scale", json!(r.scale));
            put("min", optional(r.min.clone().map(Value::from)));
            put("max", optional(r.max.clone().map(Value::from)));
        }
        FieldKind::Boolean => {}
        FieldKind::Datetime(r) | FieldKind::Date(r) => {
            put("min", optional(r.min.clone().map(Value::from)));
            put("max", optional(r.max.clone().map(Value::from)));
        }
        FieldKind::Enum(r) => put("values", json!(r.values)),
        FieldKind::Slug(r) => {
            put("from", optional(r.from.clone().map(Value::from)));
            put("max_length", json!(r.max_length));
        }
        FieldKind::Reference(r) => {
            put("to", json!(r.to));
            put("cardinality", json!(r.cardinality.as_str()));
            put("min_items", optional(r.min_items.map(Value::from)));
            put("max_items", json!(r.max_items));
            put("require_published", json!(r.require_published));
        }
        FieldKind::Asset(r) => {
            put("accept", json!(r.accept));
            put("cardinality", json!(r.cardinality.as_str()));
            put("min_items", optional(r.min_items.map(Value::from)));
            put("max_items", json!(r.max_items));
        }
        FieldKind::Component(name) => put("component", json!(name)),
        FieldKind::List(r) => {
            put("of", field(&r.of, false, true));
            put("min_items", optional(r.min_items.map(Value::from)));
            put("max_items", json!(r.max_items));
        }
        FieldKind::Blocks(r) => {
            put("components", json!(r.components));
            put("min_items", optional(r.min_items.map(Value::from)));
            put("max_items", json!(r.max_items));
        }
        FieldKind::Json(r) => put("max_bytes", json!(r.max_bytes)),
    }
    // Absent optional rules are omitted rather than written as null.
    out.retain(|_, v| !v.is_null());
    Value::Object(out)
}

/// Compact JSON with object keys in byte order, whatever map type
/// `serde_json` was built with.
pub(crate) fn write(value: &Value, out: &mut String) {
    match value {
        Value::Object(map) => {
            let mut entries: Vec<(&String, &Value)> = map.iter().collect();
            entries.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
            out.push('{');
            for (index, (key, value)) in entries.into_iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&Value::String(key.clone()).to_string());
                out.push(':');
                write(value, out);
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write(item, out);
            }
            out.push(']');
        }
        scalar => out.push_str(&scalar.to_string()),
    }
}
