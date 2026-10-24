//! Classification of the changes between the active model and a new one
//! (section 2, schema changes, and the cross-block "Deferred from v1"
//! rule): v1 activates only changes that rewrite no stored data and refuses
//! the rest.
//!
//! Both sides are canonical JSON forms ([`crate::ContentModel::canonical_json`]),
//! the stored `models.definition` and the file's, so the comparison never
//! needs to rebuild a model from the database. Every default is filled in
//! on both sides, so spelling a default out is no change.
use rust_decimal::Decimal;
use serde_json::{Map, Value};
use std::{cmp::Ordering, collections::BTreeSet, str::FromStr};

/// How a change activates.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ChangeClass {
    /// Rewrites nothing and loosens or adds: new types, components, fields
    /// and locales, relaxed rules, fallback and documentation changes.
    Additive,
    /// A stricter rule on existing data. Activated; it applies at the next
    /// write, and the host counts the stored rows that would fail.
    Tightening,
    /// A type's delivery `access`. Activated; ETags of every scope change.
    Access,
    /// `filterable` or `sortable` added or removed, or `unique` removed:
    /// derived indexes are built before activation and dropped after it.
    Index,
    /// Would rewrite stored data or cannot be made safe without stopping
    /// writes. Startup fails with `content_model_breaking`.
    Refused,
}
impl ChangeClass {
    pub fn as_str(self) -> &'static str {
        match self {
            ChangeClass::Additive => "additive",
            ChangeClass::Tightening => "tightening",
            ChangeClass::Access => "access",
            ChangeClass::Index => "index",
            ChangeClass::Refused => "refused",
        }
    }
}

/// One classified change. `path` is a dotted model path such as
/// `types.article.fields.title.max_length`, built from model names only;
/// `code` names the change, for example `field_removed`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Change {
    pub path: String,
    pub class: ChangeClass,
    pub code: &'static str,
}
impl std::fmt::Display for Change {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {}", self.class.as_str(), self.path, self.code)
    }
}

/// The classes present in `changes`, refused excluded, in class order: the
/// `change_classes` of `content.model_activated`.
pub fn change_classes(changes: &[Change]) -> Vec<&'static str> {
    let classes: BTreeSet<ChangeClass> = changes
        .iter()
        .map(|c| c.class)
        .filter(|c| *c != ChangeClass::Refused)
        .collect();
    classes.into_iter().map(ChangeClass::as_str).collect()
}

/// Every change from `old` to `new`, both canonical model forms, in model
/// order: locales, components, types. The `version` is not compared.
///
/// ```
/// use merezha_content::{ChangeClass, ContentModel, ModelOptions, diff_models};
/// let options = ModelOptions { permissions: &["tasks.read"], media: false, organizations: false };
/// let parse = |source: &str| ContentModel::parse(source, &options).unwrap();
/// let old = parse(r#"
/// version = 1
/// locales = [{ code = "en", default = true }]
/// [types.page]
/// access = "public"
/// fields = [{ name = "title", type = "string" }, { name = "body", type = "text" }]
/// "#);
/// let new = parse(r#"
/// version = 2
/// locales = [{ code = "en", default = true }]
/// [types.page]
/// access = "public"
/// fields = [{ name = "title", type = "string", max_length = 80 }]
/// "#);
/// let changes = diff_models(&old, &new);
/// let found: Vec<_> = changes.iter().map(|c| (c.class, c.path.as_str(), c.code)).collect();
/// assert_eq!(found, [
///     (ChangeClass::Tightening, "types.page.fields.title.max_length", "rule_tightened"),
///     (ChangeClass::Refused, "types.page.fields.body", "field_removed"),
/// ]);
/// ```
pub fn diff(old: &Value, new: &Value) -> Vec<Change> {
    let mut out = Changes::default();
    let empty = Map::new();
    let object = |value: &Value, key: &str| -> Map<String, Value> {
        value
            .get(key)
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default()
    };
    locales(
        old.get("locales").unwrap_or(&Value::Null),
        new.get("locales").unwrap_or(&Value::Null),
        &mut out,
    );
    let (old_components, new_components) = (object(old, "components"), object(new, "components"));
    named(
        "components",
        &old_components,
        &new_components,
        ("component_removed", "component_added"),
        &mut out,
        component,
    );
    let (old_types, new_types) = (object(old, "types"), object(new, "types"));
    named(
        "types",
        &old_types,
        &new_types,
        ("type_removed", "type_added"),
        &mut out,
        content_type,
    );
    for key in keys(
        old.as_object().unwrap_or(&empty),
        new.as_object().unwrap_or(&empty),
    ) {
        if !["version", "locales", "components", "types"].contains(&key.as_str())
            && old.get(&key) != new.get(&key)
        {
            out.push(&key, ChangeClass::Refused, "unsupported_change");
        }
    }
    out.0
}

/// [`diff`] of two parsed models.
pub fn diff_models(old: &crate::ContentModel, new: &crate::ContentModel) -> Vec<Change> {
    let parse = |model: &crate::ContentModel| {
        serde_json::from_str::<Value>(model.canonical_json()).unwrap_or(Value::Null)
    };
    diff(&parse(old), &parse(new))
}

#[derive(Default)]
struct Changes(Vec<Change>);
impl Changes {
    fn push(&mut self, path: &str, class: ChangeClass, code: &'static str) {
        self.0.push(Change {
            path: path.to_owned(),
            class,
            code,
        });
    }
}

fn join(base: &str, key: &str) -> String {
    format!("{base}.{key}")
}

/// Union of the keys of two objects, sorted.
fn keys(old: &Map<String, Value>, new: &Map<String, Value>) -> BTreeSet<String> {
    old.keys().chain(new.keys()).cloned().collect()
}

fn locales(old: &Value, new: &Value, out: &mut Changes) {
    let list = |value: &Value| -> Vec<(String, Value)> {
        value
            .as_array()
            .map(|items| {
                items
                    .iter()
                    .map(|l| {
                        let code = l.get("code").and_then(Value::as_str).unwrap_or_default();
                        (code.to_owned(), l.clone())
                    })
                    .collect()
            })
            .unwrap_or_default()
    };
    let (old, new) = (list(old), list(new));
    let find = |list: &[(String, Value)], code: &str| {
        list.iter().find(|(c, _)| c == code).map(|(_, v)| v.clone())
    };
    for (code, old_locale) in &old {
        let path = join("locales", code);
        let Some(new_locale) = find(&new, code) else {
            out.push(&path, ChangeClass::Refused, "locale_removed");
            continue;
        };
        if old_locale.get("default") != new_locale.get("default") {
            out.push(&path, ChangeClass::Refused, "default_locale_changed");
        }
        if old_locale.get("fallback") != new_locale.get("fallback") {
            out.push(
                &join(&path, "fallback"),
                ChangeClass::Additive,
                "fallback_changed",
            );
        }
    }
    for (code, _) in &new {
        if find(&old, code).is_none() {
            out.push(
                &join("locales", code),
                ChangeClass::Additive,
                "locale_added",
            );
        }
    }
    order(
        "locales",
        &old.iter().map(|(c, _)| c.clone()).collect::<Vec<_>>(),
        &new.iter().map(|(c, _)| c.clone()).collect::<Vec<_>>(),
        out,
    );
}

/// An additive `order_changed` when the names both sides share appear in
/// another order.
fn order(path: &str, old: &[String], new: &[String], out: &mut Changes) {
    let common_old: Vec<&String> = old.iter().filter(|n| new.contains(n)).collect();
    let common_new: Vec<&String> = new.iter().filter(|n| old.contains(n)).collect();
    if common_old != common_new {
        out.push(path, ChangeClass::Additive, "order_changed");
    }
}

fn named(
    section: &str,
    old: &Map<String, Value>,
    new: &Map<String, Value>,
    (removed, added): (&'static str, &'static str),
    out: &mut Changes,
    both: impl Fn(&str, &Value, &Value, &mut Changes),
) {
    for name in keys(old, new) {
        let path = join(section, &name);
        match (old.get(&name), new.get(&name)) {
            (Some(_), None) => out.push(&path, ChangeClass::Refused, removed),
            (None, Some(_)) => out.push(&path, ChangeClass::Additive, added),
            (Some(old), Some(new)) => both(&path, old, new, out),
            (None, None) => {}
        }
    }
}

fn component(path: &str, old: &Value, new: &Value, out: &mut Changes) {
    for key in keys(object(old), object(new)) {
        let (a, b) = (old.get(&key), new.get(&key));
        if a == b {
            continue;
        }
        match key.as_str() {
            "description" => out.push(
                &join(path, &key),
                ChangeClass::Additive,
                "description_changed",
            ),
            "fields" => fields(path, a, b, out),
            _ => out.push(
                &join(path, &key),
                ChangeClass::Refused,
                "unsupported_change",
            ),
        }
    }
}

fn content_type(path: &str, old: &Value, new: &Value, out: &mut Changes) {
    for key in keys(object(old), object(new)) {
        let (a, b) = (old.get(&key), new.get(&key));
        if a == b {
            continue;
        }
        let at = join(path, &key);
        match key.as_str() {
            "kind" => out.push(&at, ChangeClass::Refused, "kind_changed"),
            "scope" => out.push(&at, ChangeClass::Refused, "scope_changed"),
            "localized" => out.push(&at, ChangeClass::Refused, "localized_changed"),
            "access" => out.push(&at, ChangeClass::Access, "access_changed"),
            "title_field" => out.push(&at, ChangeClass::Additive, "title_field_changed"),
            "description" => out.push(&at, ChangeClass::Additive, "description_changed"),
            "fields" => fields(path, a, b, out),
            _ => out.push(&at, ChangeClass::Refused, "unsupported_change"),
        }
    }
}

fn object(value: &Value) -> &Map<String, Value> {
    static EMPTY: std::sync::OnceLock<Map<String, Value>> = std::sync::OnceLock::new();
    value
        .as_object()
        .unwrap_or_else(|| EMPTY.get_or_init(Map::new))
}

/// Fields matched by name. A renamed field is a removal plus an addition,
/// so it is refused.
fn fields(path: &str, old: Option<&Value>, new: Option<&Value>, out: &mut Changes) {
    let list = |value: Option<&Value>| -> Vec<(String, Value)> {
        value
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .map(|f| {
                        let name = f.get("name").and_then(Value::as_str).unwrap_or_default();
                        (name.to_owned(), f.clone())
                    })
                    .collect()
            })
            .unwrap_or_default()
    };
    let (old, new) = (list(old), list(new));
    let base = join(path, "fields");
    for (name, old_field) in &old {
        let at = join(&base, name);
        match new.iter().find(|(n, _)| n == name) {
            None => out.push(&at, ChangeClass::Refused, "field_removed"),
            Some((_, new_field)) => field(&at, old_field, new_field, out),
        }
    }
    for (name, _) in &new {
        if !old.iter().any(|(n, _)| n == name) {
            out.push(&join(&base, name), ChangeClass::Additive, "field_added");
        }
    }
    order(
        &base,
        &old.iter().map(|(n, _)| n.clone()).collect::<Vec<_>>(),
        &new.iter().map(|(n, _)| n.clone()).collect::<Vec<_>>(),
        out,
    );
}

/// One field, or a list's `of` item definition, present on both sides.
fn field(path: &str, old: &Value, new: &Value, out: &mut Changes) {
    if old.get("type") != new.get("type") {
        out.push(path, ChangeClass::Refused, "field_type_changed");
        return;
    }
    let field_type = old.get("type").and_then(Value::as_str).unwrap_or_default();
    for key in keys(object(old), object(new)) {
        let (a, b) = (old.get(&key), new.get(&key));
        if a == b || key == "name" || key == "type" {
            continue;
        }
        let at = join(path, &key);
        let flag = |value: Option<&Value>| value.and_then(Value::as_bool).unwrap_or(false);
        let (class, code) = match key.as_str() {
            "description" => (ChangeClass::Additive, "description_changed"),
            "default" => (ChangeClass::Additive, "default_changed"),
            "from" => (ChangeClass::Additive, "slug_source_changed"),
            "required" | "require_published" if flag(b) => {
                (ChangeClass::Tightening, "required_added")
            }
            "required" | "require_published" => (ChangeClass::Additive, "required_removed"),
            "localized" => (ChangeClass::Refused, "localized_changed"),
            // `unique` cannot be made safe on an existing field without
            // stopping writes (section 2): only a new field may have it.
            "unique" if flag(b) => (ChangeClass::Refused, "unique_added"),
            "unique" => (ChangeClass::Index, "unique_removed"),
            "filterable" => (ChangeClass::Index, "filterable_changed"),
            "sortable" => (ChangeClass::Index, "sortable_changed"),
            "min_length" | "min" | "min_items" => bound(a, b, field_type, Ordering::Greater),
            "max_length" | "max" | "max_items" | "max_blocks" | "max_text_length" | "max_bytes"
            | "precision" => bound(a, b, field_type, Ordering::Less),
            // Whether one regex accepts more than another is undecidable
            // here; a changed pattern counts as tightening, which activates.
            "pattern" if b.is_some() => (ChangeClass::Tightening, "rule_tightened"),
            "pattern" => (ChangeClass::Additive, "rule_relaxed"),
            "to" | "accept" | "styles" | "marks" | "annotations" | "lists" | "embeds" => {
                set(a, b, ChangeClass::Tightening)
            }
            // Absent means every type.
            "entry_link_to" => match (a, b) {
                (None, _) => (ChangeClass::Tightening, "rule_tightened"),
                (_, None) => (ChangeClass::Additive, "rule_relaxed"),
                _ => set(a, b, ChangeClass::Tightening),
            },
            // Stored values of a removed enum value or blocks component no
            // longer fit the model; C10's `map_enum` migrates them.
            "values" | "components" => set(a, b, ChangeClass::Refused),
            // A different shape (object or array), another component or a
            // new normalization of stored decimals.
            "cardinality" => (ChangeClass::Refused, "cardinality_changed"),
            "component" => (ChangeClass::Refused, "component_changed"),
            "scale" => (ChangeClass::Refused, "scale_changed"),
            "of" => {
                field(
                    &at,
                    a.unwrap_or(&Value::Null),
                    b.unwrap_or(&Value::Null),
                    out,
                );
                continue;
            }
            _ => (ChangeClass::Refused, "unsupported_change"),
        };
        out.push(&at, class, code);
    }
}

/// A bound moving in `tighter` direction (`Greater` for minimums, `Less`
/// for maximums) or appearing tightens; the opposite relaxes.
fn bound(
    old: Option<&Value>,
    new: Option<&Value>,
    field_type: &str,
    tighter: Ordering,
) -> (ChangeClass, &'static str) {
    let tightened = match (old, new) {
        (None, Some(_)) => true,
        (Some(_), None) => false,
        (Some(a), Some(b)) => compare(a, b, field_type).is_none_or(|o| o == tighter),
        (None, None) => false,
    };
    if tightened {
        (ChangeClass::Tightening, "rule_tightened")
    } else {
        (ChangeClass::Additive, "rule_relaxed")
    }
}

/// `new` compared with `old`; `None` when they cannot be compared, which
/// counts as tightening.
fn compare(old: &Value, new: &Value, field_type: &str) -> Option<Ordering> {
    match (old, new) {
        (Value::Number(a), Value::Number(b)) => match (a.as_i64(), b.as_i64()) {
            (Some(a), Some(b)) => Some(b.cmp(&a)),
            _ => b.as_f64()?.partial_cmp(&a.as_f64()?),
        },
        (Value::String(a), Value::String(b)) if field_type == "decimal" => {
            Some(Decimal::from_str(b).ok()?.cmp(&Decimal::from_str(a).ok()?))
        }
        // `date` and `datetime` bounds are stored in fixed-width forms that
        // sort in time order.
        (Value::String(a), Value::String(b)) => Some(b.as_str().cmp(a.as_str())),
        _ => None,
    }
}

/// A set-valued rule: items removed is `on_removal`, items only added (or
/// reordered) is additive.
fn set(
    old: Option<&Value>,
    new: Option<&Value>,
    on_removal: ChangeClass,
) -> (ChangeClass, &'static str) {
    let items = |value: Option<&Value>| -> Vec<Value> {
        value.and_then(Value::as_array).cloned().unwrap_or_default()
    };
    let (old, new) = (items(old), items(new));
    if old.iter().any(|item| !new.contains(item)) {
        let code = if on_removal == ChangeClass::Refused {
            "value_removed"
        } else {
            "rule_tightened"
        };
        (on_removal, code)
    } else {
        (ChangeClass::Additive, "rule_relaxed")
    }
}
