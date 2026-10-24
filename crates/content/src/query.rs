//! Management list queries (section 5, filters, sorting and pagination):
//! the query string, typed filter values, sort keys with nulls last, and
//! keyset cursors bound to a fingerprint of the query.
use crate::{
    model::{Cardinality, ContentModel, ContentType, Field, FieldKind},
    scalar,
    settings::Limits,
    store::{Rejection, VARIANT_FROM, variant_columns},
};
use base64::Engine as _;
use merezha_domain::Failure;
use merezha_platform::{FieldError, errors::MAX_DETAILS};
use merezha_postgres::tokio_postgres::types::ToSql;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;

/// Most values one `in` or `has` clause takes.
pub const MAX_FILTER_VALUES: usize = 50;
/// Longest accepted query string.
pub const MAX_QUERY_BYTES: usize = 16_384;
/// Longest accepted cursor.
pub const MAX_CURSOR_BYTES: usize = 4096;

/// One `key=value` pair of a query string, percent-decoded (`+` is a
/// space). Fails `invalid_query` on malformed escapes or non-UTF-8.
pub(crate) fn pairs(query: Option<&str>) -> Result<Vec<(String, String)>, Rejection> {
    let query = query.unwrap_or("");
    if query.len() > MAX_QUERY_BYTES {
        return Err(problem("invalid_query", "", "too_long"));
    }
    let mut out = Vec::new();
    for pair in query.split('&').filter(|p| !p.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        match (decode(key), decode(value)) {
            (Some(key), Some(value)) => out.push((key, value)),
            _ => return Err(problem("invalid_query", "", "invalid_encoding")),
        }
    }
    Ok(out)
}

fn decode(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'%' => {
                let hex = bytes.get(index + 1..index + 3)?;
                let hex = std::str::from_utf8(hex).ok()?;
                out.push(u8::from_str_radix(hex, 16).ok()?);
                index += 3;
            }
            b'+' => {
                out.push(b' ');
                index += 1;
            }
            byte => {
                out.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8(out).ok()
}

pub(crate) fn detail(path: &str, code: &str) -> FieldError {
    FieldError::new(path, code).unwrap_or_else(|_| FieldError {
        path: String::new(),
        code: code.to_owned(),
    })
}
fn problem(code: &str, path: &str, reason: &str) -> Rejection {
    Rejection {
        failure: Failure::permanent(code),
        details: vec![detail(path, reason)],
    }
}
fn rejection(code: &str, mut details: Vec<FieldError>) -> Rejection {
    details.truncate(MAX_DETAILS);
    Rejection {
        failure: Failure::permanent(code),
        details,
    }
}

/// `status=` of the management list.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatusFilter {
    Draft,
    Published,
    Changed,
    Any,
}
impl StatusFilter {
    fn parse(text: &str) -> Option<Self> {
        Some(match text {
            "draft" => Self::Draft,
            "published" => Self::Published,
            "changed" => Self::Changed,
            "any" => Self::Any,
            _ => return None,
        })
    }
    fn as_str(self) -> &'static str {
        match self {
            Self::Draft => "draft",
            Self::Published => "published",
            Self::Changed => "changed",
            Self::Any => "any",
        }
    }
    fn sql(self) -> &'static str {
        match self {
            Self::Draft => " AND v.published_revision IS NULL",
            Self::Published => " AND v.published_revision = v.draft_revision",
            Self::Changed => {
                " AND v.published_revision IS NOT NULL AND v.published_revision <> v.draft_revision"
            }
            Self::Any => "",
        }
    }
}

/// Filter operators (section 5).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Op {
    Eq,
    In,
    Has,
    Ne,
    Exists,
    Lt,
    Lte,
    Gt,
    Gte,
}
impl Op {
    fn parse(text: &str) -> Option<Self> {
        Some(match text {
            "eq" => Self::Eq,
            "in" => Self::In,
            "has" => Self::Has,
            "ne" => Self::Ne,
            "exists" => Self::Exists,
            "lt" => Self::Lt,
            "lte" => Self::Lte,
            "gt" => Self::Gt,
            "gte" => Self::Gte,
            _ => return None,
        })
    }
    fn as_str(self) -> &'static str {
        match self {
            Self::Eq => "eq",
            Self::In => "in",
            Self::Has => "has",
            Self::Ne => "ne",
            Self::Exists => "exists",
            Self::Lt => "lt",
            Self::Lte => "lte",
            Self::Gt => "gt",
            Self::Gte => "gte",
        }
    }
    fn repeatable(self) -> bool {
        matches!(self, Self::In | Self::Has)
    }
    fn comparison(self) -> Option<&'static str> {
        match self {
            Self::Lt => Some("<"),
            Self::Lte => Some("<="),
            Self::Gt => Some(">"),
            Self::Gte => Some(">="),
            _ => None,
        }
    }
}

/// What a listing reads: the management list over `variants`, delivery
/// over `published`, and preview over the variants a locale chain
/// resolves (exposed as `v` with the variant columns).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Source {
    Manage,
    Published,
    Preview,
}
impl Source {
    /// The JSON column of the entry's fields.
    pub(crate) fn data(self) -> &'static str {
        match self {
            Self::Published => "p.data",
            Self::Manage | Self::Preview => "v.draft_data",
        }
    }
    /// The entry id column, the final sort key.
    pub(crate) fn id(self) -> &'static str {
        match self {
            Self::Published => "p.entry_id",
            Self::Manage | Self::Preview => "v.entry_id",
        }
    }
    /// The column of the system timestamp `sys.<name>` and whether it may
    /// be null: `updated_at` and `created_at` in management,
    /// `published_at` in delivery. Preview reads the variant's
    /// `published_at`, null for a draft never published.
    fn timestamp(self, name: &str) -> Option<(&'static str, bool)> {
        match (self, name) {
            (Self::Manage, "updated_at") => Some(("v.draft_updated_at", false)),
            (Self::Manage, "created_at") => Some(("e.created_at", false)),
            (Self::Published, "published_at") => Some(("p.published_at", false)),
            (Self::Preview, "published_at") => Some(("v.published_at", true)),
            _ => None,
        }
    }
    /// The sort of a listing that names none.
    fn default_sort(self) -> &'static str {
        match self {
            Self::Manage => "-sys.updated_at",
            Self::Published | Self::Preview => "-sys.published_at",
        }
    }
}

/// The cast a sortable field's text takes in SQL, as in its derived index.
fn cast(kind: &FieldKind) -> &'static str {
    match kind {
        FieldKind::Integer(_) => "bigint",
        FieldKind::Float(_) => "float8",
        FieldKind::Decimal(_) => "numeric",
        _ => "text",
    }
}
/// The typed SQL expression of a top-level field, matching the derived
/// sort index.
fn field_expression(source: Source, name: &str, kind: &FieldKind) -> String {
    let data = source.data();
    match cast(kind) {
        "text" => format!("({data}->>'{name}')"),
        cast => format!("(({data}->>'{name}'))::{cast}"),
    }
}
fn null_expression(source: Source, name: &str) -> String {
    format!("(({}->'{name}') IS NULL)", source.data())
}

/// A filter value parsed with the field's type: the stored JSON form, as
/// containment matches it, plus the text a typed comparison casts.
fn scalar_value(kind: &FieldKind, text: &str) -> Result<(Value, String), &'static str> {
    if text.contains('\0') {
        return Err("invalid_value");
    }
    let value = match kind {
        FieldKind::Integer(_) => {
            let n: i64 = text.parse().map_err(|_| "invalid_value")?;
            if n.abs() > crate::model::MAX_SAFE_INTEGER {
                return Err("invalid_value");
            }
            Value::from(n)
        }
        FieldKind::Float(_) => {
            let n: f64 = text.parse().map_err(|_| "invalid_value")?;
            if !n.is_finite() {
                return Err("invalid_value");
            }
            serde_json::Number::from_f64(n)
                .map(Value::Number)
                .ok_or("invalid_value")?
        }
        FieldKind::Decimal(rules) => Value::String(
            scalar::decimal(text, rules.precision, rules.scale).map_err(|_| "invalid_value")?,
        ),
        FieldKind::Boolean => match text {
            "true" => Value::Bool(true),
            "false" => Value::Bool(false),
            _ => return Err("invalid_value"),
        },
        FieldKind::Datetime(_) => {
            Value::String(scalar::datetime(text).map_err(|_| "invalid_value")?)
        }
        FieldKind::Date(_) => Value::String(scalar::date(text).map_err(|_| "invalid_value")?),
        FieldKind::Enum(rules) if !rules.values.iter().any(|v| v == text) => {
            return Err("invalid_value");
        }
        FieldKind::String(_) | FieldKind::Text(_) | FieldKind::Slug(_) | FieldKind::Enum(_) => {
            Value::String(text.to_owned())
        }
        _ => return Err("not_filterable"),
    };
    let text = match &value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    Ok((value, text))
}

fn uuid_value(text: &str) -> Result<String, &'static str> {
    if text.len() != 36 {
        return Err("invalid_value");
    }
    uuid::Uuid::try_parse(text)
        .map(|id| id.hyphenated().to_string())
        .map_err(|_| "invalid_value")
}

/// One sort key with its SQL.
#[derive(Clone, Debug)]
struct SortKey {
    /// `-name`, `name`, `sys.updated_at`, ...
    spelled: String,
    desc: bool,
    /// `IS NULL` expression for fields; system timestamps are never null.
    null: Option<String>,
    value: String,
    /// The value as text, what the cursor carries.
    text: String,
    /// The cast a cursor value takes back.
    cast: &'static str,
    /// As the last key, `entry_id` runs against its direction: the static
    /// `published_list` index orders `published_at DESC, entry_id`, so
    /// `sys.published_at` in either direction is one scan of it.
    reverse_tie: bool,
}
impl SortKey {
    /// The direction of the `entry_id` tie-breaker after this key.
    fn tie_desc(&self) -> bool {
        self.desc != self.reverse_tie
    }
}

fn sort_key(
    source: Source,
    content_type: &ContentType,
    spelled: &str,
) -> Result<SortKey, &'static str> {
    let (desc, name) = match spelled.strip_prefix('-') {
        Some(name) => (true, name),
        None => (false, spelled),
    };
    if let Some(system) = name.strip_prefix("sys.") {
        let (column, nullable) = source.timestamp(system).ok_or("unknown_field")?;
        return Ok(SortKey {
            spelled: spelled.to_owned(),
            desc,
            null: nullable.then(|| format!("({column} IS NULL)")),
            value: column.to_owned(),
            text: format!(
                "to_char({column} AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')"
            ),
            cast: "timestamptz",
            reverse_tie: source == Source::Published,
        });
    }
    let field = content_type.field(name).ok_or("unknown_field")?;
    if !field.sortable {
        return Err("not_sortable");
    }
    let value = field_expression(source, &field.name, &field.kind);
    Ok(SortKey {
        spelled: spelled.to_owned(),
        desc,
        null: Some(null_expression(source, &field.name)),
        text: format!("({value})::text"),
        value,
        cast: cast(&field.kind),
        reverse_tie: false,
    })
}

/// A parsed management list request, ready to run.
#[derive(Debug)]
pub(crate) struct ListQuery {
    pub limit: u32,
    pub fields: Option<Vec<String>>,
    pub statement: String,
    pub params: Vec<Box<dyn ToSql + Sync + Send>>,
    pub keys: usize,
    /// The 16 hex characters cursors of this query carry.
    pub fingerprint: String,
}

/// Collects SQL parameters, numbering them.
#[derive(Debug, Default)]
pub(crate) struct Params(pub(crate) Vec<Box<dyn ToSql + Sync + Send>>);
impl Params {
    pub(crate) fn push(&mut self, value: impl ToSql + Sync + Send + 'static) -> String {
        self.0.push(Box::new(value));
        format!("${}", self.0.len())
    }
}

/// A decoded cursor: the last row's sort values (text, `None` for null)
/// and its entry id.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cursor {
    pub keys: Vec<Option<String>>,
    pub id: String,
}

/// `base64url({"v":1,"q":...,"k":[...],"id":...})`, without padding.
pub(crate) fn encode_cursor(fingerprint: &str, keys: &[Option<String>], id: &str) -> String {
    let body = json!({"v": 1, "q": fingerprint, "k": keys, "id": id});
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(body.to_string())
}

/// Decode a cursor minted for the query with `fingerprint`, holding
/// `keys` sort values; anything else is `invalid_cursor`.
pub(crate) fn decode_cursor(
    text: &str,
    fingerprint: &str,
    keys: usize,
) -> Result<Cursor, Rejection> {
    let invalid = || problem("invalid_cursor", "/cursor", "invalid_value");
    if text.len() > MAX_CURSOR_BYTES {
        return Err(invalid());
    }
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(text)
        .map_err(|_| invalid())?;
    let Ok(Value::Object(body)) = serde_json::from_slice::<Value>(&bytes) else {
        return Err(invalid());
    };
    if body.len() != 4 || body.get("v") != Some(&json!(1)) {
        return Err(invalid());
    }
    if body.get("q").and_then(Value::as_str) != Some(fingerprint) {
        return Err(invalid());
    }
    let id = body
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(invalid)?
        .to_owned();
    let values = body
        .get("k")
        .and_then(Value::as_array)
        .ok_or_else(invalid)?;
    if values.len() != keys {
        return Err(invalid());
    }
    let mut out = Vec::with_capacity(keys);
    for value in values {
        match value {
            Value::Null => out.push(None),
            Value::String(s) if s.len() <= 1024 => out.push(Some(s.clone())),
            _ => return Err(invalid()),
        }
    }
    Ok(Cursor { keys: out, id })
}

/// Whether `text` parses under `cast` exactly as PostgreSQL will cast it,
/// so a forged cursor never reaches the database as a cast error.
pub(crate) fn castable(cast: &str, text: &str) -> bool {
    let digits = |t: &str| !t.is_empty() && t.bytes().all(|b| b.is_ascii_digit());
    match cast {
        "bigint" => {
            let unsigned = text.strip_prefix('-').unwrap_or(text);
            digits(unsigned) && unsigned.len() <= 19 && text.parse::<i64>().is_ok()
        }
        // PostgreSQL's float8 output: `-?d+(.d+)?(e[+-]d+)?`. A mantissa
        // that is not zero but parses to 0 underflows, which PostgreSQL
        // refuses where Rust rounds.
        "float8" => {
            let unsigned = text.strip_prefix('-').unwrap_or(text);
            let (mantissa, exponent) = match unsigned.split_once('e') {
                Some((m, e)) => (m, Some(e)),
                None => (unsigned, None),
            };
            let (whole, fraction) = match mantissa.split_once('.') {
                Some((w, f)) => (w, Some(f)),
                None => (mantissa, None),
            };
            let shaped = digits(whole)
                && fraction.is_none_or(digits)
                && exponent.is_none_or(|e| {
                    e.strip_prefix(['+', '-'])
                        .is_some_and(|d| digits(d) && d.len() <= 3)
                });
            let zero_mantissa = mantissa.bytes().all(|b| b == b'0' || b == b'.');
            shaped
                && text.len() <= 32
                && text
                    .parse::<f64>()
                    .is_ok_and(|v| v.is_finite() && (v != 0.0 || zero_mantissa))
        }
        "numeric" => {
            let digits = text.strip_prefix('-').unwrap_or(text);
            let (whole, fraction) = digits.split_once('.').unwrap_or((digits, ""));
            !whole.is_empty()
                && whole.len() <= 40
                && fraction.len() <= 40
                && whole.bytes().all(|b| b.is_ascii_digit())
                && fraction.bytes().all(|b| b.is_ascii_digit())
                && (!digits.contains('.') || !fraction.is_empty())
        }
        // Exactly the `to_char` form the list selects:
        // `YYYY-MM-DDTHH:MM:SS.ffffffZ`.
        "timestamptz" => {
            let b = text.as_bytes();
            let shape = b.len() == 27
                && b.iter().enumerate().all(|(i, c)| match i {
                    4 | 7 => *c == b'-',
                    10 => *c == b'T',
                    13 | 16 => *c == b':',
                    19 => *c == b'.',
                    26 => *c == b'Z',
                    _ => c.is_ascii_digit(),
                });
            shape && text.parse::<jiff::Timestamp>().is_ok()
        }
        _ => !text.contains('\0'),
    }
}

pub(crate) fn hex16(text: &str) -> String {
    let digest = Sha256::digest(text.as_bytes());
    let mut out = String::with_capacity(16);
    for byte in &digest[..8] {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// A listing's query string split into its parts: the parameters every
/// listing takes (`filter[..]`, `sort`, `limit`, `cursor`, `fields`), the
/// extra ones its route allows, and the problems found so far, which the
/// caller completes before refusing with `invalid_query`.
#[derive(Debug, Default)]
pub(crate) struct Split {
    pub extra: std::collections::HashMap<String, String>,
    clauses: Vec<((String, Op), Vec<String>)>,
    filter_problems: Vec<FieldError>,
    sort: Option<String>,
    cursor: Option<String>,
    pub limit: u32,
    pub fields: Option<Vec<String>>,
    pub problems: Vec<FieldError>,
}

/// Split `query` for a listing of `content_type` whose route also takes
/// the parameters `extra`.
pub(crate) fn split(
    content_type: &ContentType,
    query: Option<&str>,
    limits: &Limits,
    extra: &[&str],
) -> Result<Split, Rejection> {
    let mut out = Split::default();
    let mut limit = None;
    let mut fields = None;
    for (key, value) in pairs(query)? {
        if let Some(rest) = key.strip_prefix("filter[") {
            let parsed = rest
                .strip_suffix(']')
                .and_then(|r| r.split_once("]["))
                .filter(|(f, o)| {
                    !f.is_empty() && !o.contains(['[', ']']) && !f.contains(['[', ']'])
                });
            let Some((field, op)) = parsed else {
                out.filter_problems.push(detail("/filter", "invalid_key"));
                continue;
            };
            let Some(op) = Op::parse(op) else {
                out.filter_problems
                    .push(detail("/filter", "unknown_operator"));
                continue;
            };
            let key = (field.to_owned(), op);
            match out.clauses.iter_mut().find(|(k, _)| *k == key) {
                Some((_, values)) if op.repeatable() => values.push(value),
                Some(_) => out.filter_problems.push(detail("/filter", "duplicate_key")),
                None => out.clauses.push((key, vec![value])),
            }
            continue;
        }
        let slot = match key.as_str() {
            "sort" => &mut out.sort,
            "limit" => &mut limit,
            "cursor" => &mut out.cursor,
            "fields" => &mut fields,
            other if extra.contains(&other) => {
                if out.extra.contains_key(other) {
                    out.problems
                        .push(detail(&format!("/{key}"), "duplicate_parameter"));
                } else {
                    out.extra.insert(key, value);
                }
                continue;
            }
            _ => {
                out.problems.push(detail("", "unknown_parameter"));
                continue;
            }
        };
        if slot.is_some() {
            out.problems
                .push(detail(&format!("/{key}"), "duplicate_parameter"));
        } else {
            *slot = Some(value);
        }
    }
    out.limit = match limit.as_deref() {
        None => limits.default_page_size,
        Some(text) => match text.parse::<u32>() {
            Ok(n) if (1..=limits.max_page_size).contains(&n) => n,
            _ => {
                out.problems.push(detail("/limit", "invalid_value"));
                limits.default_page_size
            }
        },
    };
    out.fields = fields.as_deref().map(|text| {
        let names: Vec<String> = text.split(',').map(str::to_owned).collect();
        if names.iter().any(|n| content_type.field(n).is_none()) {
            out.problems.push(detail("/fields", "unknown_field"));
        }
        names
    });
    Ok(out)
}
impl Split {
    /// The problems found so far as `invalid_query`, if any.
    pub(crate) fn check(&mut self) -> Result<(), Rejection> {
        if self.problems.is_empty() {
            Ok(())
        } else {
            Err(rejection(
                "invalid_query",
                std::mem::take(&mut self.problems),
            ))
        }
    }
}

/// The clauses of a listing as SQL over its [`Source`], ready to join a
/// `FROM` and the scope predicates of the caller.
#[derive(Debug)]
pub(crate) struct Built {
    /// ` AND ...` per filter clause.
    pub conditions: String,
    /// ` AND (...)`, the keyset condition after the cursor, or empty.
    pub after: String,
    /// The `ORDER BY` list, `entry_id` last.
    pub order: String,
    /// `, <text of each sort key>`, selected after the row's columns.
    pub texts: String,
    pub keys: usize,
    /// The 16 hex characters cursors of this query carry.
    pub fingerprint: String,
}

/// Build the filters, sort and cursor of `split` over `source`, numbering
/// parameters after those already in `params`. `context` names the
/// listing (scope, stage, locale and the like) in the cursor fingerprint
/// beside the type, the filters and the sort.
pub(crate) fn build(
    split: &mut Split,
    content_type: &ContentType,
    limits: &Limits,
    source: Source,
    context: &str,
    params: &mut Params,
) -> Result<Built, Rejection> {
    let mut conditions = String::new();
    let mut canonical: Vec<String> = Vec::new();
    let mut filter_problems = std::mem::take(&mut split.filter_problems);
    if split.clauses.len() > limits.max_filters as usize {
        filter_problems.push(detail("/filter", "too_many_filters"));
    }
    for ((name, op), values) in &split.clauses {
        match filter(source, content_type, name, *op, values, params) {
            Ok((sql, normalized)) => {
                conditions.push_str(" AND ");
                conditions.push_str(&sql);
                canonical.push(format!(
                    "{name}\u{1f}{}\u{1f}{}",
                    op.as_str(),
                    normalized.join("\u{1e}")
                ));
            }
            Err((path, code)) => filter_problems.push(detail(&path, code)),
        }
    }
    if !filter_problems.is_empty() {
        return Err(rejection("invalid_filter", filter_problems));
    }
    canonical.sort();

    let spelled: Vec<String> = match split.sort.as_deref() {
        None => vec![source.default_sort().to_owned()],
        Some(text) => text.split(',').map(str::to_owned).collect(),
    };
    let mut sort_problems = Vec::new();
    if spelled.len() > limits.max_sort_keys as usize {
        sort_problems.push(detail("/sort", "too_many_keys"));
    }
    let mut keys = Vec::new();
    for spelled in &spelled {
        let bare = spelled.trim_start_matches('-');
        if keys
            .iter()
            .any(|k: &SortKey| k.spelled.trim_start_matches('-') == bare)
        {
            sort_problems.push(detail("/sort", "duplicate_key"));
            continue;
        }
        match sort_key(source, content_type, spelled) {
            Ok(key) => keys.push(key),
            Err(code) => sort_problems.push(detail("/sort", code)),
        }
    }
    if !sort_problems.is_empty() {
        return Err(rejection("invalid_sort", sort_problems));
    }
    let fingerprint = hex16(&format!(
        "{}\n{context}\n{}\n{}",
        content_type.name,
        canonical.join("\n"),
        spelled.join(",")
    ));

    let mut after = String::new();
    if let Some(text) = split.cursor.take() {
        let cursor = decode_cursor(&text, &fingerprint, keys.len())?;
        let valid = uuid_value(&cursor.id).is_ok()
            && keys
                .iter()
                .zip(&cursor.keys)
                .all(|(key, value)| match value {
                    None => key.null.is_some(),
                    Some(text) => castable(key.cast, text),
                });
        if !valid {
            return Err(problem("invalid_cursor", "/cursor", "invalid_value"));
        }
        after = after_condition(source, &keys, &cursor, params);
    }

    let mut order = String::new();
    let mut texts = String::new();
    for key in &keys {
        if let Some(null) = &key.null {
            let _ = write!(order, "{null} ASC, ");
        }
        let _ = write!(
            order,
            "{} {}, ",
            key.value,
            if key.desc { "DESC" } else { "ASC" }
        );
        let _ = write!(texts, ", {}", key.text);
    }
    // `entry_id` follows the last key's direction, as in the derived
    // indexes and `variants_by_update`, so either direction is one scan
    // (against it after `sys.published_at`, as `published_list` orders).
    let tie_desc = keys.last().is_some_and(SortKey::tie_desc);
    let _ = write!(
        order,
        "{} {}",
        source.id(),
        if tie_desc { "DESC" } else { "ASC" }
    );
    Ok(Built {
        conditions,
        after,
        order,
        texts,
        keys: keys.len(),
        fingerprint,
    })
}

/// Parse a management list query string for `content_type` in `scope`.
pub(crate) fn parse_list(
    model: &ContentModel,
    content_type: &ContentType,
    scope: &str,
    query: Option<&str>,
    limits: &Limits,
) -> Result<ListQuery, Rejection> {
    let mut split = split(content_type, query, limits, &["locale", "status"])?;
    let status = match split.extra.get("status").map(String::as_str) {
        None => StatusFilter::Any,
        Some(text) => match StatusFilter::parse(text) {
            Some(status) => status,
            None => {
                split.problems.push(detail("/status", "invalid_value"));
                StatusFilter::Any
            }
        },
    };
    split.check()?;
    let locale = match split.extra.get("locale") {
        None => model.default_locale().code.clone(),
        Some(code) => code.clone(),
    };
    if !locale_allowed(model, content_type, &locale) {
        return Err(Failure::permanent("locale_unknown").into());
    }

    let mut params = Params(Vec::new());
    let scope_param = params.push(scope.to_owned());
    let type_literal = type_literal(content_type);
    let locale_param = params.push(locale.clone());
    let built = build(
        &mut split,
        content_type,
        limits,
        Source::Manage,
        &format!("{scope}\ndraft\n{locale}\n{}", status.as_str()),
        &mut params,
    )?;
    let limit = split.limit;
    let fetch = params.push(i64::from(limit) + 1);
    let statement = format!(
        "SELECT {}{} FROM {VARIANT_FROM} \
          WHERE v.scope = {scope_param} AND v.content_type = {type_literal} \
            AND v.locale = {locale_param} AND NOT v.deleted AND e.deleted_at IS NULL\
            {}{}{} \
          ORDER BY {} LIMIT {fetch}",
        variant_columns(),
        built.texts,
        status.sql(),
        built.conditions,
        built.after,
        built.order,
    );
    Ok(ListQuery {
        limit,
        fields: split.fields.take(),
        statement,
        params: params.0,
        keys: built.keys,
        fingerprint: built.fingerprint,
    })
}

/// The type as an SQL literal, not a parameter: the derived indexes are
/// partial on `content_type = '<type>'`, which a generic plan could not
/// prove from a parameter. Type names are `[a-z][a-z0-9_]*`.
pub(crate) fn type_literal(content_type: &ContentType) -> String {
    format!("'{}'", content_type.name.replace('\'', "''"))
}

/// Whether `locale` addresses a variant of `content_type`: a configured
/// locale, and the default one for a non-localized type.
pub(crate) fn locale_allowed(
    model: &ContentModel,
    content_type: &ContentType,
    locale: &str,
) -> bool {
    if content_type.localized {
        model.locales().iter().any(|l| l.code == locale)
    } else {
        model.default_locale().code == locale
    }
}

/// The keyset condition "after `cursor`" for `keys` then `entry_id`:
/// lexicographic over `(is null, value)` per key, nulls last.
fn after_condition(
    source: Source,
    keys: &[SortKey],
    cursor: &Cursor,
    params: &mut Params,
) -> String {
    let mut terms: Vec<String> = Vec::new();
    let mut prefix: Vec<String> = Vec::new();
    let joined = |prefix: &[String], last: String| {
        if prefix.is_empty() {
            last
        } else {
            format!("{} AND {last}", prefix.join(" AND "))
        }
    };
    for (key, value) in keys.iter().zip(&cursor.keys) {
        if let Some(null) = &key.null {
            let last_null = value.is_none();
            if !last_null {
                // Every null row comes after every non-null one.
                terms.push(joined(&prefix, null.clone()));
                prefix.push(format!("NOT {null}"));
            } else {
                prefix.push(null.clone());
            }
        }
        if let Some(text) = value {
            let param = params.push(text.clone());
            let typed = format!("{param}::text::{}", key.cast);
            let op = if key.desc { "<" } else { ">" };
            terms.push(joined(&prefix, format!("{} {op} {typed}", key.value)));
            prefix.push(format!("{} = {typed}", key.value));
        }
    }
    let id = params.push(cursor.id.clone());
    let op = if keys.last().is_some_and(SortKey::tie_desc) {
        "<"
    } else {
        ">"
    };
    terms.push(joined(
        &prefix,
        format!("{} {op} {id}::text::uuid", source.id()),
    ));
    format!(
        " AND ({})",
        terms
            .iter()
            .map(|t| format!("({t})"))
            .collect::<Vec<_>>()
            .join(" OR ")
    )
}

type FilterError = (String, &'static str);

/// One filter clause as SQL plus its normalized values.
fn filter(
    source: Source,
    content_type: &ContentType,
    name: &str,
    op: Op,
    values: &[String],
    params: &mut Params,
) -> Result<(String, Vec<String>), FilterError> {
    if values.len() > MAX_FILTER_VALUES {
        return Err(("/filter".into(), "too_many_values"));
    }
    if let Some(system) = name.strip_prefix("sys.") {
        return system_filter(source, system, op, values, params);
    }
    let Some(field) = content_type.field(name) else {
        return Err(("/filter".into(), "unknown_field"));
    };
    let path = format!("/filter/{name}/{}", op.as_str());
    if !field.filterable {
        return Err((path, "not_filterable"));
    }
    let single = || values.first().cloned().unwrap_or_default();
    match op {
        Op::Exists => {
            let present = match single().as_str() {
                "true" => true,
                "false" => false,
                _ => return Err((path, "invalid_value")),
            };
            let param = params.push(field.name.clone());
            let sql = format!("{} ? {param}", source.data());
            Ok((
                if present { sql } else { format!("NOT ({sql})") },
                vec![present.to_string()],
            ))
        }
        Op::Eq | Op::In | Op::Ne => {
            if op != Op::In && values.len() != 1 {
                return Err((path, "invalid_value"));
            }
            let mut terms = Vec::new();
            let mut normalized = Vec::new();
            for text in values {
                let stored = match &field.kind {
                    FieldKind::Reference(rules) if rules.cardinality == Cardinality::One => {
                        json!({ "id": uuid_value(text).map_err(|c| (path.clone(), c))? })
                    }
                    kind if kind.is_scalar() => {
                        scalar_value(kind, text).map_err(|c| (path.clone(), c))?.0
                    }
                    _ => return Err((path, "not_supported")),
                };
                normalized.push(stored.to_string());
                terms.push(containment(source, field, stored, params));
            }
            normalized.sort();
            normalized.dedup();
            let sql = format!("({})", terms.join(" OR "));
            Ok((
                if op == Op::Ne {
                    format!("NOT {sql}")
                } else {
                    sql
                },
                normalized,
            ))
        }
        Op::Has => {
            let mut items = Vec::new();
            for text in values {
                let item = match &field.kind {
                    FieldKind::Reference(rules) if rules.cardinality == Cardinality::Many => {
                        json!({ "id": uuid_value(text).map_err(|c| (path.clone(), c))? })
                    }
                    FieldKind::List(list) if list.of.kind.is_scalar() => {
                        scalar_value(&list.of.kind, text)
                            .map_err(|c| (path.clone(), c))?
                            .0
                    }
                    _ => return Err((path, "not_supported")),
                };
                items.push(item);
            }
            let mut normalized: Vec<String> = items.iter().map(Value::to_string).collect();
            normalized.sort();
            normalized.dedup();
            Ok((
                containment(source, field, Value::Array(items), params),
                normalized,
            ))
        }
        Op::Lt | Op::Lte | Op::Gt | Op::Gte => {
            if !field.sortable {
                return Err((path, "not_sortable"));
            }
            if values.len() != 1 {
                return Err((path, "invalid_value"));
            }
            let (_, text) = scalar_value(&field.kind, &single()).map_err(|c| (path.clone(), c))?;
            let param = params.push(text.clone());
            let comparison = op.comparison().unwrap_or("=");
            Ok((
                format!(
                    "{} {comparison} {param}::text::{}",
                    field_expression(source, &field.name, &field.kind),
                    cast(&field.kind)
                ),
                vec![text],
            ))
        }
    }
}

fn containment(source: Source, field: &Field, value: Value, params: &mut Params) -> String {
    let mut object = Map::new();
    object.insert(field.name.clone(), value);
    let param = params.push(Value::Object(object).to_string());
    format!("{} @> {param}::text::jsonb", source.data())
}

fn system_filter(
    source: Source,
    name: &str,
    op: Op,
    values: &[String],
    params: &mut Params,
) -> Result<(String, Vec<String>), FilterError> {
    let path = || format!("/filter/sys.{name}/{}", op.as_str());
    if name == "id" {
        let ids = values
            .iter()
            .map(|v| uuid_value(v))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|c| (path(), c))?;
        let column = source.id();
        return match op {
            Op::Eq | Op::Ne if ids.len() == 1 => {
                let param = params.push(ids[0].clone());
                let cmp = if op == Op::Eq { "=" } else { "<>" };
                Ok((format!("{column} {cmp} {param}::text::uuid"), ids))
            }
            Op::In => {
                let mut sorted = ids.clone();
                sorted.sort();
                sorted.dedup();
                let param = params.push(ids);
                Ok((format!("{column} = ANY({param}::text[]::uuid[])"), sorted))
            }
            Op::Eq | Op::Ne => Err((path(), "invalid_value")),
            _ => Err((path(), "unknown_operator")),
        };
    }
    let Some((column, _)) = source.timestamp(name) else {
        return Err(("/filter".into(), "unknown_field"));
    };
    let comparison = match op {
        Op::Eq => "=",
        other => other.comparison().ok_or((path(), "unknown_operator"))?,
    };
    let [value] = values else {
        return Err((path(), "invalid_value"));
    };
    let value = scalar::datetime(value).map_err(|_| (path(), "invalid_value"))?;
    let param = params.push(value.clone());
    Ok((
        format!("{column} {comparison} {param}::text::timestamptz"),
        vec![value],
    ))
}

impl crate::module::ContentState {
    /// `EXPLAIN (COSTS OFF)` of the application-scope management list of
    /// `content_type` for `query`, with its parameters bound: the plan the
    /// route runs, for index tests and operators. Not a route.
    #[doc(hidden)]
    pub async fn explain_list(&self, content_type: &str, query: &str) -> Result<String, Rejection> {
        let model = self.model().clone();
        let declared = model
            .content_type(content_type)
            .ok_or_else(|| Rejection::from(Failure::permanent("content_type_not_found")))?;
        let parsed = parse_list(
            &model,
            declared,
            "app",
            Some(query),
            &self.settings().limits,
        )?;
        let database = self
            .database()
            .ok_or_else(|| Rejection::from(Failure::transient("content_model_pending")))?;
        let statement = format!("EXPLAIN (COSTS OFF) {}", parsed.statement);
        let params = parsed.params;
        crate::store::transaction(
            database,
            std::time::Instant::now() + std::time::Duration::from_secs(10),
            move |tx| {
                Box::pin(async move {
                    let refs: Vec<&(dyn ToSql + Sync)> = params
                        .iter()
                        .map(|p| p.as_ref() as &(dyn ToSql + Sync))
                        .collect();
                    let rows = tx
                        .query(&statement, &refs)
                        .await
                        .map_err(|e| Rejection::from(merezha_postgres::classify(&e)))?;
                    Ok(rows
                        .iter()
                        .map(|r| r.get::<_, String>(0))
                        .collect::<Vec<_>>()
                        .join("\n"))
                })
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_strings_decode_percent_escapes_and_plus() {
        let pairs = pairs(Some("filter%5Btitle%5D%5Beq%5D=a+b%2Bc&limit=5")).unwrap();
        assert_eq!(
            pairs,
            vec![
                ("filter[title][eq]".into(), "a b+c".into()),
                ("limit".into(), "5".into())
            ]
        );
        assert!(super::pairs(Some("a=%zz")).is_err());
        assert!(super::pairs(Some("a=%ff")).is_err());
    }
    #[test]
    fn cursors_round_trip_and_refuse_other_queries() {
        let text = encode_cursor("0123456789abcdef", &[Some("4".into()), None], "id");
        let cursor = decode_cursor(&text, "0123456789abcdef", 2).unwrap();
        assert_eq!(cursor.keys, vec![Some("4".into()), None]);
        for (fingerprint, keys) in [("fedcba9876543210", 2), ("0123456789abcdef", 1)] {
            assert_eq!(
                decode_cursor(&text, fingerprint, keys)
                    .unwrap_err()
                    .failure
                    .code,
                "invalid_cursor"
            );
        }
        assert!(decode_cursor("!!", "0123456789abcdef", 2).is_err());
    }
    #[test]
    fn forged_cursor_values_never_reach_a_cast() {
        assert!(castable("bigint", "-12"));
        assert!(!castable("bigint", "1.5"));
        assert!(castable("numeric", "-10.25"));
        assert!(!castable("numeric", "1e5"));
        assert!(!castable("numeric", "1."));
        assert!(castable("timestamptz", "2026-10-24T09:00:00.123456Z"));
        assert!(!castable("timestamptz", "yesterday"));
        assert!(!castable("float8", "NaN"));
        for good in ["1.5", "-0", "0", "1e+100", "2.5e-05", "-12"] {
            assert!(castable("float8", good), "{good}");
        }
        for bad in [
            "1e-400", "1e400", "+1", "1E5", "1.", ".5", "Infinity", "1e5",
        ] {
            assert!(!castable("float8", bad), "{bad}");
        }
        for bad in [
            "+002026-10-24T09:00:00Z",
            "-000001-01-01T00:00:00Z",
            "2026-10-24T09Z",
            "2026-10-24T09:00Z",
            "2026-10-24T24:00:00.000000Z",
            "2026-10-24T09:00:00.123456789Z",
            "2026-10-24T09:00:00,5Z",
            "20261024T090000Z",
            "9999-12-31T23:59:59Z",
            "2026-10-24T09:00:00.123456+00:00",
        ] {
            assert!(!castable("timestamptz", bad), "{bad}");
        }
        assert!(!castable("bigint", "+5"));
        assert!(!castable("bigint", "99999999999999999999"));
    }
}
