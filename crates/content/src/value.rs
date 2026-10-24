//! Entry field values against the model: write-time and publish-time
//! validation, normalization, `_key` assignment, slug generation and the
//! references a save must resolve.
//!
//! Every violation is reported at once as a [`FieldError`] whose path is a
//! JSON pointer built from field names, `_key`s and list indexes, never
//! from other input. A field the model does not know is reported at its
//! containing object, so its name is not echoed either.
use crate::{model::*, rich_text, scalar, slug::slugify};
use merezha_domain::Failure;
use merezha_platform::{FieldError, Random, errors::MAX_DETAILS};
use serde_json::{Map, Value};
use std::collections::HashSet;
use uuid::Uuid;

/// Longest detail path; deeper paths are cut at a segment boundary.
pub const MAX_DETAIL_PATH: usize = merezha_platform::errors::MAX_DETAIL_PATH;
/// Length of a generated `_key`.
pub const GENERATED_KEY_LENGTH: usize = 12;
/// Longest asset `alt` text, in Unicode scalar values.
pub const MAX_ALT_LENGTH: usize = 500;

/// Which rules a validation applies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    /// A draft save: every rule except `required` and `min_items`.
    Draft,
    /// Publishing: every rule. `require_published` needs the database and
    /// is the caller's, through [`Validated::references`].
    Publish,
}

/// How the fields being validated relate to their variant.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ValidateOptions {
    pub stage: Stage,
    /// The fields belong to the default-locale variant. In a localized
    /// type, other variants may not carry shared (`localized = false`)
    /// fields, and their `required` checks are skipped: the default
    /// variant owns them.
    pub default_variant: bool,
    /// The variant is being created: absent top-level fields with a
    /// `default` take it.
    pub new_variant: bool,
}

/// Where generated `_key`s come from.
pub trait KeySource {
    /// A fresh key matching `^[A-Za-z0-9_-]{1,32}$`.
    fn next_key(&mut self) -> Result<String, Failure>;
}
/// 12 random characters of `[A-Za-z0-9]` from the platform CSPRNG.
pub struct RandomKeys<'a>(pub &'a dyn Random);
impl KeySource for RandomKeys<'_> {
    fn next_key(&mut self) -> Result<String, Failure> {
        const ALPHABET: &[u8; 62] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        let mut key = String::with_capacity(GENERATED_KEY_LENGTH);
        let mut bytes = [0u8; 32];
        while key.len() < GENERATED_KEY_LENGTH {
            self.0.fill(&mut bytes)?;
            // Rejection sampling keeps every character equally likely.
            for byte in bytes {
                if byte < 248 && key.len() < GENERATED_KEY_LENGTH {
                    key.push(char::from(ALPHABET[usize::from(byte % 62)]));
                }
            }
        }
        Ok(key)
    }
}

/// `^[A-Za-z0-9_-]{1,32}$`, the `_key` syntax.
pub fn valid_key(key: &str) -> bool {
    (1..=32).contains(&key.len())
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// Fields that passed, in their stored form.
#[derive(Clone, Debug, PartialEq)]
pub struct Validated {
    /// Normalized values: decimals at their scale, datetimes in UTC,
    /// references with lowercase ids, missing `_key`s assigned and absent
    /// slugs generated. Absent and `null` fields are left out.
    pub fields: Map<String, Value>,
    /// Every entry and asset reference, for the caller to resolve against
    /// the database (existence, trash, scope, asset types and
    /// `require_published`).
    pub references: Vec<ValueReference>,
}

/// One reference found in the fields.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValueReference {
    /// JSON pointer of the reference value, such as `/fields/author` or
    /// `/fields/body/b2/markDefs/l2`.
    pub path: String,
    pub target: ReferenceTarget,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReferenceTarget {
    Entry {
        id: Uuid,
        /// The `type` the write named, already checked against `allowed`.
        content_type: Option<String>,
        /// Types the target may have; empty allows every type.
        allowed: Vec<String>,
        require_published: bool,
    },
    Asset {
        id: Uuid,
        /// MIME types or `type/*` patterns; empty accepts every type.
        accept: Vec<String>,
    },
}

/// Why fields were not accepted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ValueError {
    /// Field rules failed: `validation_failed` with these details.
    Invalid(Vec<FieldError>),
    /// Something else failed, such as `content_type_not_found`,
    /// `invalid_json` for a non-object or `random_unavailable`.
    Failed(Failure),
}
impl ValueError {
    /// Permanent `validation_failed` for field rules, else the failure.
    pub fn failure(&self) -> Failure {
        match self {
            ValueError::Invalid(_) => Failure::permanent("validation_failed"),
            ValueError::Failed(failure) => failure.clone(),
        }
    }
    /// The field errors, empty unless [`ValueError::Invalid`].
    pub fn details(&self) -> &[FieldError] {
        match self {
            ValueError::Invalid(details) => details,
            ValueError::Failed(_) => &[],
        }
    }
}

impl ContentModel {
    /// Validate one variant's `fields` object for `content_type`.
    /// Every violation is collected (up to the platform's 100 details).
    /// The caller bounds the input first, with `limits.max_entry_bytes`.
    ///
    /// ```
    /// use merezha_content::{ContentModel, ModelOptions, Stage, ValidateOptions, KeySource};
    /// struct Fixed;
    /// impl KeySource for Fixed {
    ///     fn next_key(&mut self) -> Result<String, merezha_domain::Failure> { Ok("k1".into()) }
    /// }
    /// let options = ModelOptions { permissions: &["tasks.read"], media: false, organizations: false };
    /// let model = ContentModel::parse(r#"
    /// version = 1
    /// locales = [{ code = "en", default = true }]
    /// [types.product]
    /// access = "public"
    /// fields = [
    ///   { name = "name", type = "string", required = true },
    ///   { name = "slug", type = "slug", from = "name" },
    ///   { name = "price", type = "decimal", precision = 10, scale = 2 },
    /// ]
    /// "#, &options).unwrap();
    /// let draft = ValidateOptions { stage: Stage::Draft, default_variant: true, new_variant: true };
    /// let fields = serde_json::json!({ "name": "Café crème", "price": "4.5" });
    /// let valid = model.validate_fields("product", &fields, &draft, &mut Fixed).unwrap();
    /// assert_eq!(valid.fields["slug"], "cafe-creme");
    /// assert_eq!(valid.fields["price"], "4.50");
    ///
    /// let publish = ValidateOptions { stage: Stage::Publish, ..draft };
    /// let error = model.validate_fields("product", &serde_json::json!({ "price": "1.234" }), &publish, &mut Fixed).unwrap_err();
    /// let details: Vec<_> = error.details().iter().map(|d| (d.path.as_str(), d.code.as_str())).collect();
    /// assert_eq!(details, [("/fields/name", "required"), ("/fields/price", "scale")]);
    /// ```
    pub fn validate_fields(
        &self,
        content_type: &str,
        fields: &Value,
        options: &ValidateOptions,
        keys: &mut dyn KeySource,
    ) -> Result<Validated, ValueError> {
        let Some(declared) = self.types.get(content_type) else {
            return Err(ValueError::Failed(Failure::permanent(
                "content_type_not_found",
            )));
        };
        let Value::Object(input) = fields else {
            return Err(ValueError::Failed(Failure::permanent("invalid_json")));
        };
        let mut ctx = Ctx {
            model: self,
            stage: options.stage,
            keys,
            details: Vec::new(),
            seen: HashSet::new(),
            references: Vec::new(),
            failed: None,
        };
        let top = Top {
            shared_check: declared.localized && !options.default_variant,
            new_variant: options.new_variant,
        };
        let fields = ctx.object(&declared.fields, input, "/fields", Some(top), &[]);
        if let Some(failure) = ctx.failed {
            return Err(ValueError::Failed(failure));
        }
        if !ctx.details.is_empty() {
            return Err(ValueError::Invalid(ctx.details));
        }
        Ok(Validated {
            fields,
            references: ctx.references,
        })
    }
}

/// Settings that apply to a type's own fields only.
#[derive(Clone, Copy)]
pub(crate) struct Top {
    shared_check: bool,
    new_variant: bool,
}

pub(crate) struct Ctx<'m, 'k> {
    pub(crate) model: &'m ContentModel,
    pub(crate) stage: Stage,
    keys: &'k mut dyn KeySource,
    details: Vec<FieldError>,
    seen: HashSet<(String, &'static str)>,
    references: Vec<ValueReference>,
    failed: Option<Failure>,
}

/// `path` cut at segment boundaries to fit a detail.
fn bounded(path: &str) -> &str {
    let mut path = path;
    while path.len() > MAX_DETAIL_PATH {
        path = match path.rfind('/') {
            Some(at) if at > 0 => &path[..at],
            _ => "",
        };
    }
    path
}

/// `parent/segment` as a JSON pointer; segments are names, keys or
/// indexes, which never need escaping, but escape them anyway.
pub(crate) fn child(parent: &str, segment: &str) -> String {
    let escaped = segment.replace('~', "~0").replace('/', "~1");
    format!("{parent}/{escaped}")
}

impl Ctx<'_, '_> {
    pub(crate) fn violation(&mut self, path: &str, code: &'static str) {
        if self.details.len() >= MAX_DETAILS {
            return;
        }
        let path = bounded(path);
        if !self.seen.insert((path.to_owned(), code)) {
            return;
        }
        if let Ok(detail) = FieldError::new(path, code) {
            self.details.push(detail);
        }
    }
    pub(crate) fn publishing(&self) -> bool {
        self.stage == Stage::Publish
    }
    pub(crate) fn reference(&mut self, path: &str, target: ReferenceTarget) {
        self.references.push(ValueReference {
            path: bounded(path).to_owned(),
            target,
        });
    }

    /// Validate an object of `fields`; `meta` keys (`_key`, `_type`) are
    /// the caller's and skipped here.
    pub(crate) fn object(
        &mut self,
        fields: &[Field],
        input: &Map<String, Value>,
        path: &str,
        top: Option<Top>,
        meta: &[&str],
    ) -> Map<String, Value> {
        if input
            .keys()
            .any(|k| !meta.contains(&k.as_str()) && !fields.iter().any(|f| &f.name == k))
        {
            self.violation(path, "unknown_field");
        }
        let mut out = Map::new();
        for field in fields {
            let at = child(path, &field.name);
            let mut value = input.get(&field.name).filter(|v| !v.is_null()).cloned();
            if top.is_some_and(|t| t.shared_check) && !field.localized {
                if value.is_some() {
                    self.violation(&at, "field_not_localized");
                }
                continue;
            }
            if value.is_none() && top.is_some_and(|t| t.new_variant) {
                value = field.default.clone();
            }
            if value.is_none()
                && let FieldKind::Slug(SlugRules {
                    from: Some(from),
                    max_length,
                }) = &field.kind
                && let Some(Value::String(source)) = input.get(from)
            {
                value = slugify(source, *max_length as usize).map(Value::String);
            }
            let Some(value) = value else {
                if self.publishing() && field.required {
                    self.violation(&at, "required");
                }
                continue;
            };
            if let Some(stored) = self.value(field, &value, &at) {
                if self.publishing() && field.required && empty(&stored) {
                    self.violation(&at, "required");
                }
                out.insert(field.name.clone(), stored);
            }
        }
        out
    }

    fn value(&mut self, field: &Field, value: &Value, path: &str) -> Option<Value> {
        match &field.kind {
            FieldKind::RichText(rules) => rich_text::validate(self, rules, value, path),
            FieldKind::Reference(rules) => self.references_value(rules, value, path),
            FieldKind::Asset(rules) => self.assets(rules, value, path),
            FieldKind::Component(name) => {
                let Value::Object(input) = value else {
                    self.violation(path, "type_mismatch");
                    return None;
                };
                let component = self.model.component(name)?;
                Some(Value::Object(self.object(
                    &component.fields,
                    input,
                    path,
                    None,
                    &[],
                )))
            }
            FieldKind::List(rules) => self.list(rules, value, path),
            FieldKind::Blocks(rules) => self.blocks(rules, value, path),
            FieldKind::Json(rules) => self.json(rules, value, path),
            kind => match scalar::check(kind, value) {
                Ok(stored) => Some(stored),
                Err(code) => {
                    self.violation(path, code);
                    None
                }
            },
        }
    }

    /// The array of a list-like value with its count checked, or `None`
    /// when it is not an array or holds more than `max_items`.
    pub(crate) fn array<'v>(
        &mut self,
        value: &'v Value,
        path: &str,
        min_items: Option<u32>,
        max_items: u32,
    ) -> Option<&'v Vec<Value>> {
        let Value::Array(items) = value else {
            self.violation(path, "type_mismatch");
            return None;
        };
        if items.len() > max_items as usize {
            // Items past the bound are not inspected.
            self.violation(path, "max_items");
            return None;
        }
        if self.publishing() && min_items.is_some_and(|min| items.len() < min as usize) {
            self.violation(path, "min_items");
        }
        Some(items)
    }

    /// Items identified by `_key`: missing keys are generated, malformed or
    /// repeated ones are violations and the item is addressed by index.
    /// `each` validates one item object (with `_key` still in it) at its
    /// path and returns its stored form without `_key`.
    pub(crate) fn keyed(
        &mut self,
        items: &[Value],
        path: &str,
        mut each: impl FnMut(&mut Self, &Map<String, Value>, &str) -> Option<Map<String, Value>>,
    ) -> Vec<Value> {
        let taken: HashSet<&str> = items
            .iter()
            .filter_map(|i| i.get("_key").and_then(Value::as_str))
            .filter(|k| valid_key(k))
            .collect();
        let mut used: HashSet<String> = HashSet::new();
        let mut out = Vec::with_capacity(items.len());
        for (index, item) in items.iter().enumerate() {
            let index_path = child(path, &index.to_string());
            let Value::Object(object) = item else {
                self.violation(&index_path, "type_mismatch");
                continue;
            };
            let (key, item_path) = match object.get("_key") {
                None => match self.fresh_key(&taken, &used) {
                    Some(key) => {
                        let at = child(path, &key);
                        (Some(key), at)
                    }
                    None => return out,
                },
                Some(Value::String(key)) if valid_key(key) => {
                    if used.contains(key) {
                        self.violation(&index_path, "duplicate_key");
                        (None, index_path)
                    } else {
                        (Some(key.clone()), child(path, key))
                    }
                }
                Some(_) => {
                    self.violation(&child(&index_path, "_key"), "pattern");
                    (None, index_path)
                }
            };
            if let Some(key) = &key {
                used.insert(key.clone());
            }
            if let Some(mut stored) = each(self, object, &item_path)
                && let Some(key) = key
            {
                stored.insert("_key".into(), Value::String(key));
                out.push(Value::Object(stored));
            }
        }
        out
    }

    fn fresh_key(&mut self, taken: &HashSet<&str>, used: &HashSet<String>) -> Option<String> {
        for _ in 0..8 {
            match self.keys.next_key() {
                Ok(key)
                    if valid_key(&key) && !taken.contains(key.as_str()) && !used.contains(&key) =>
                {
                    return Some(key);
                }
                Ok(_) => {}
                Err(failure) => {
                    self.failed = Some(failure);
                    return None;
                }
            }
        }
        self.failed = Some(Failure::transient("random_unavailable"));
        None
    }

    fn list(&mut self, rules: &ListRules, value: &Value, path: &str) -> Option<Value> {
        let items = self.array(value, path, rules.min_items, rules.max_items)?;
        if let FieldKind::Component(name) = &rules.of.kind {
            let component = self.model.component(name)?;
            let fields = &component.fields;
            let out = self.keyed(items, path, |ctx, object, at| {
                Some(ctx.object(fields, object, at, None, &["_key"]))
            });
            return Some(Value::Array(out));
        }
        let mut out = Vec::with_capacity(items.len());
        for (index, item) in items.iter().enumerate() {
            if let Some(stored) = self.value(&rules.of, item, &child(path, &index.to_string())) {
                out.push(stored);
            }
        }
        Some(Value::Array(out))
    }

    fn blocks(&mut self, rules: &BlocksRules, value: &Value, path: &str) -> Option<Value> {
        let items = self.array(value, path, rules.min_items, rules.max_items)?;
        let out = self.keyed(items, path, |ctx, object, at| {
            let name = object.get("_type").and_then(Value::as_str);
            let Some(name) = name.filter(|n| rules.components.iter().any(|c| c == n)) else {
                ctx.violation(&child(at, "_type"), "type_mismatch");
                return None;
            };
            let component = ctx.model.component(name)?;
            let mut stored = ctx.object(&component.fields, object, at, None, &["_key", "_type"]);
            stored.insert("_type".into(), Value::String(name.to_owned()));
            Some(stored)
        });
        Some(Value::Array(out))
    }

    /// The items of a reference or asset value: one object, or an array
    /// of them with its count checked.
    fn one_or_many<'v>(
        &mut self,
        cardinality: Cardinality,
        value: &'v Value,
        path: &str,
        min_items: Option<u32>,
        max_items: u32,
    ) -> Option<Vec<(String, &'v Value)>> {
        match cardinality {
            Cardinality::One => Some(vec![(path.to_owned(), value)]),
            Cardinality::Many => {
                let items = self.array(value, path, min_items, max_items)?;
                Some(
                    items
                        .iter()
                        .enumerate()
                        .map(|(i, item)| (child(path, &i.to_string()), item))
                        .collect(),
                )
            }
        }
    }

    fn references_value(
        &mut self,
        rules: &ReferenceRules,
        value: &Value,
        path: &str,
    ) -> Option<Value> {
        let items = self.one_or_many(
            rules.cardinality,
            value,
            path,
            rules.min_items,
            rules.max_items,
        )?;
        let mut out = Vec::with_capacity(items.len());
        for (at, item) in items {
            if let Some(stored) = self.entry_reference(
                item,
                &at,
                &rules.to,
                rules.require_published,
                &["id", "type"],
            ) {
                out.push(stored);
            }
        }
        Some(match rules.cardinality {
            Cardinality::One => out.pop()?,
            Cardinality::Many => Value::Array(out),
        })
    }

    /// `{"id", "type"?}` pointing at an entry whose type is in `allowed`
    /// (every model type when empty). `keys` lists the accepted members.
    pub(crate) fn entry_reference(
        &mut self,
        item: &Value,
        path: &str,
        allowed: &[String],
        require_published: bool,
        keys: &[&str],
    ) -> Option<Value> {
        let Value::Object(object) = item else {
            self.violation(path, "type_mismatch");
            return None;
        };
        if object.keys().any(|k| !keys.contains(&k.as_str())) {
            self.violation(path, "unknown_field");
        }
        let id = self.uuid(object.get("id"), path)?;
        let content_type = match object.get("type") {
            None => None,
            Some(Value::String(t))
                if (allowed.is_empty() && self.model.types.contains_key(t))
                    || allowed.contains(t) =>
            {
                Some(t.clone())
            }
            Some(_) => {
                self.violation(path, "reference_type");
                return None;
            }
        };
        self.reference(
            path,
            ReferenceTarget::Entry {
                id,
                content_type: content_type.clone(),
                allowed: allowed.to_vec(),
                require_published,
            },
        );
        let mut stored = Map::new();
        stored.insert("id".into(), Value::String(id.hyphenated().to_string()));
        if let Some(content_type) = content_type {
            stored.insert("type".into(), Value::String(content_type));
        }
        Some(Value::Object(stored))
    }

    /// A UUID in its hyphenated 36-character form, any case.
    pub(crate) fn uuid(&mut self, value: Option<&Value>, path: &str) -> Option<Uuid> {
        let parsed = value
            .and_then(Value::as_str)
            .filter(|s| s.len() == 36)
            .and_then(|s| Uuid::try_parse(s).ok());
        if parsed.is_none() {
            self.violation(&child(path, "id"), "type_mismatch");
        }
        parsed
    }

    fn assets(&mut self, rules: &AssetRules, value: &Value, path: &str) -> Option<Value> {
        let items = self.one_or_many(
            rules.cardinality,
            value,
            path,
            rules.min_items,
            rules.max_items,
        )?;
        let mut out = Vec::with_capacity(items.len());
        for (at, item) in items {
            if let Some(stored) = self.asset_reference(item, &at, &rules.accept, &["id", "alt"]) {
                out.push(stored);
            }
        }
        Some(match rules.cardinality {
            Cardinality::One => out.pop()?,
            Cardinality::Many => Value::Array(out),
        })
    }

    /// `{"id", "alt"?}` pointing at an asset.
    pub(crate) fn asset_reference(
        &mut self,
        item: &Value,
        path: &str,
        accept: &[String],
        keys: &[&str],
    ) -> Option<Value> {
        let Value::Object(object) = item else {
            self.violation(path, "type_mismatch");
            return None;
        };
        if object.keys().any(|k| !keys.contains(&k.as_str())) {
            self.violation(path, "unknown_field");
        }
        let id = self.uuid(object.get("id"), path)?;
        let mut stored = Map::new();
        stored.insert("id".into(), Value::String(id.hyphenated().to_string()));
        match object.get("alt") {
            None | Some(Value::Null) => {}
            Some(Value::String(alt)) if alt.contains('\0') => {
                self.violation(&child(path, "alt"), "invalid_character");
                return None;
            }
            Some(Value::String(alt)) if scalar::length(alt) > MAX_ALT_LENGTH => {
                self.violation(&child(path, "alt"), "max_length");
                return None;
            }
            Some(Value::String(alt)) => {
                stored.insert("alt".into(), Value::String(alt.clone()));
            }
            Some(_) => {
                self.violation(&child(path, "alt"), "type_mismatch");
                return None;
            }
        }
        self.reference(
            path,
            ReferenceTarget::Asset {
                id,
                accept: accept.to_vec(),
            },
        );
        Some(Value::Object(stored))
    }

    fn json(&mut self, rules: &JsonRules, value: &Value, path: &str) -> Option<Value> {
        if contains_nul(value) {
            self.violation(path, "invalid_character");
            return None;
        }
        if merezha_domain::compatible_json(value).is_err() {
            self.violation(path, "type_mismatch");
            return None;
        }
        let size = serde_json::to_vec(value).map_or(usize::MAX, |b| b.len());
        if size > rules.max_bytes as usize {
            self.violation(path, "json_too_large");
            return None;
        }
        Some(value.clone())
    }
}

/// Whether any string or object key in `value` holds U+0000, which
/// `jsonb` cannot store. Iterative, so depth costs no stack.
fn contains_nul(value: &Value) -> bool {
    let mut stack = vec![value];
    while let Some(value) = stack.pop() {
        match value {
            Value::String(s) if s.contains('\0') => return true,
            Value::Array(items) => stack.extend(items),
            Value::Object(map) => {
                if map.keys().any(|k| k.contains('\0')) {
                    return true;
                }
                stack.extend(map.values());
            }
            _ => {}
        }
    }
    false
}

/// Empty strings, lists and rich text count as absent for `required`.
fn empty(value: &Value) -> bool {
    match value {
        Value::String(s) => s.is_empty(),
        Value::Array(items) => items.is_empty(),
        _ => false,
    }
}
