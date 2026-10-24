//! `content.toml` to [`ContentModel`]: structural parsing, field rules and
//! cross references, with every problem collected at once.
use crate::{
    canonical,
    issues::{Issues, ModelError, join},
    model::*,
    scalar,
};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use toml::{Table, Value as Toml};

/// Largest accepted model file.
pub const MAX_MODEL_BYTES: usize = 1_048_576;
/// Longest accepted `pattern` source.
pub const MAX_PATTERN_BYTES: usize = 1024;
/// Compiled size limit of a `pattern`, for the program and the lazy DFA.
pub const PATTERN_SIZE_LIMIT: usize = 1_048_576;
/// Longest `description`, in Unicode scalar values.
pub const MAX_DESCRIPTION: usize = 500;

/// The permissions the content block contributes to the RBAC catalog
/// (section 5). A type's `access` may always name one of them.
pub const CONTENT_PERMISSIONS: [&str; 11] = [
    "content.model.read",
    "content.entries.read",
    "content.entries.write",
    "content.entries.publish",
    "content.entries.delete",
    "content.entries.purge",
    "content.assets.read",
    "content.assets.write",
    "content.assets.delete",
    "content.delivery.read",
    "content.preview",
];

/// The RBAC permission catalog `access = { permission = ".." }` is checked
/// against.
pub trait PermissionCatalog {
    fn contains(&self, permission: &str) -> bool;
}
impl PermissionCatalog for BTreeSet<String> {
    fn contains(&self, permission: &str) -> bool {
        BTreeSet::contains(self, permission)
    }
}
impl PermissionCatalog for HashSet<String> {
    fn contains(&self, permission: &str) -> bool {
        HashSet::contains(self, permission)
    }
}
impl<const N: usize> PermissionCatalog for [&str; N] {
    fn contains(&self, permission: &str) -> bool {
        <[&str]>::contains(self, &permission)
    }
}
impl PermissionCatalog for Vec<String> {
    fn contains(&self, permission: &str) -> bool {
        self.iter().any(|p| p == permission)
    }
}

/// What the rest of the host provides to the model.
#[derive(Clone, Copy)]
pub struct ModelOptions<'a> {
    /// The application's and the blocks' permissions. The content
    /// permissions are accepted whether or not the catalog lists them.
    pub permissions: &'a dyn PermissionCatalog,
    /// `[modules.content.media]` is configured; without it `asset` fields
    /// and embeds are refused.
    pub media: bool,
    /// Identity runs with organizations; without it `scope =
    /// "organization"` is refused.
    pub organizations: bool,
}

impl ContentModel {
    /// Parse and validate a model file. Reports every problem at once, each
    /// with its TOML path, as `content_model_invalid`.
    ///
    /// ```
    /// use merezha_content::{ContentModel, ModelOptions};
    /// let options = ModelOptions { permissions: &["tasks.read"], media: false, organizations: false };
    /// let model = ContentModel::parse(r#"
    /// version = 1
    /// locales = [{ code = "en", default = true }]
    /// [types.page]
    /// access = "public"
    /// fields = [{ name = "title", type = "string", required = true }]
    /// "#, &options).unwrap();
    /// assert_eq!(model.fingerprint().len(), 64);
    ///
    /// let error = ContentModel::parse(r#"
    /// version = 1
    /// locales = [{ code = "en", default = true }]
    /// [types.page]
    /// fields = [{ name = "title", type = "string", max_length = 0 }]
    /// "#, &options).unwrap_err();
    /// assert_eq!(error.to_string(), "\
    /// content_model_invalid: types.page.access is required: \"public\", \"authenticated\" or { permission = \"<name>\" }
    /// content_model_invalid: types.page.fields[0].max_length must be between 1 and 4096");
    /// ```
    pub fn parse(source: &str, options: &ModelOptions<'_>) -> Result<Self, ModelError> {
        let mut issues = Issues::default();
        if source.len() > MAX_MODEL_BYTES {
            issues.push(
                "",
                "model_too_large",
                format!("the model file is larger than {MAX_MODEL_BYTES} bytes"),
            );
            return Err(issues.into_error());
        }
        let table = match source.parse::<Table>() {
            Ok(table) => table,
            Err(error) => {
                let (line, column) = error
                    .span()
                    .map(|span| position(source, span.start))
                    .unwrap_or((1, 1));
                issues.push(
                    "",
                    "toml_syntax",
                    format!("the model file is not valid TOML (line {line}, column {column})"),
                );
                return Err(issues.into_error());
            }
        };
        let parsed = parse_root(&table, options, &mut issues);
        match parsed {
            Some(model) if issues.is_empty() => Ok(model),
            _ => Err(issues.into_error()),
        }
    }
}

/// 1-based line and column (in characters) of byte `offset`.
fn position(source: &str, offset: usize) -> (usize, usize) {
    let before = &source[..source.floor_char_boundary(offset.min(source.len()))];
    let line = before.matches('\n').count() + 1;
    let column = before.rsplit('\n').next().map_or(0, |l| l.chars().count()) + 1;
    (line, column)
}

/// Reads the keys of one table, remembering which were consumed so the
/// rest can be reported as unknown.
struct Reader<'t> {
    table: &'t Table,
    path: String,
    used: BTreeSet<&'t str>,
}
impl<'t> Reader<'t> {
    fn new(table: &'t Table, path: &str) -> Self {
        Self {
            table,
            path: path.to_owned(),
            used: BTreeSet::new(),
        }
    }
    fn at(&self, key: &str) -> String {
        join(&self.path, key)
    }
    fn take(&mut self, key: &str) -> Option<&'t Toml> {
        let (key, value) = self.table.get_key_value(key)?;
        self.used.insert(key.as_str());
        Some(value)
    }
    fn present(&self, key: &str) -> bool {
        self.table.contains_key(key)
    }
    fn missing(&self, key: &str, issues: &mut Issues) {
        issues.push(self.at(key), "missing_required", "is required");
    }
    fn string(&mut self, key: &str, issues: &mut Issues) -> Option<&'t str> {
        match self.take(key)? {
            Toml::String(s) => Some(s),
            _ => {
                issues.push(self.at(key), "invalid_type", "must be a string");
                None
            }
        }
    }
    fn boolean(&mut self, key: &str, issues: &mut Issues) -> Option<bool> {
        match self.take(key)? {
            Toml::Boolean(b) => Some(*b),
            _ => {
                issues.push(self.at(key), "invalid_type", "must be a boolean");
                None
            }
        }
    }
    fn integer(&mut self, key: &str, min: i64, max: i64, issues: &mut Issues) -> Option<i64> {
        match self.take(key)? {
            Toml::Integer(n) if (min..=max).contains(n) => Some(*n),
            Toml::Integer(_) => {
                issues.push(
                    self.at(key),
                    "out_of_range",
                    format!("must be between {min} and {max}"),
                );
                None
            }
            _ => {
                issues.push(self.at(key), "invalid_type", "must be an integer");
                None
            }
        }
    }
    /// An integer bound in `min..=max` as `u32`.
    fn count(&mut self, key: &str, min: u32, max: u32, issues: &mut Issues) -> Option<u32> {
        self.integer(key, i64::from(min), i64::from(max), issues)
            .and_then(|n| u32::try_from(n).ok())
    }
    fn choice<T: Copy>(
        &mut self,
        key: &str,
        choices: &[(&str, T)],
        default: T,
        issues: &mut Issues,
    ) -> T {
        let Some(value) = self.string(key, issues) else {
            return default;
        };
        if let Some((_, choice)) = choices.iter().find(|(name, _)| *name == value) {
            return *choice;
        }
        let names: Vec<String> = choices.iter().map(|(n, _)| format!("\"{n}\"")).collect();
        issues.push(
            self.at(key),
            "invalid_value",
            format!("must be one of {}", names.join(", ")),
        );
        default
    }
    /// A list of distinct strings, each accepted by `valid`.
    fn strings(
        &mut self,
        key: &str,
        (min, max): (usize, usize),
        valid: &dyn Fn(&str) -> bool,
        rule: &str,
        issues: &mut Issues,
    ) -> Option<Vec<String>> {
        let path = self.at(key);
        let value = self.take(key)?;
        let Toml::Array(items) = value else {
            issues.push(path, "invalid_type", "must be an array of strings");
            return None;
        };
        let mut ok = true;
        if items.len() < min || items.len() > max {
            issues.push(
                path.clone(),
                "out_of_range",
                format!("must list between {min} and {max} values"),
            );
            ok = false;
        }
        let mut out: Vec<String> = Vec::new();
        for (index, item) in items.iter().enumerate().take(max.max(1)) {
            let at = format!("{path}[{index}]");
            match item {
                Toml::String(s) if !valid(s) => {
                    issues.push(at, "invalid_value", rule.to_owned());
                    ok = false;
                }
                Toml::String(s) if out.contains(s) => {
                    issues.push(at, "duplicate_value", "is listed twice");
                    ok = false;
                }
                Toml::String(s) => out.push(s.clone()),
                _ => {
                    issues.push(at, "invalid_type", "must be a string");
                    ok = false;
                }
            }
        }
        ok.then_some(out)
    }
    fn description(&mut self, issues: &mut Issues) -> Option<String> {
        let text = self.string("description", issues)?;
        if scalar::length(text) > MAX_DESCRIPTION || text.contains('\0') {
            issues.push(
                self.at("description"),
                "out_of_range",
                format!("must be at most {MAX_DESCRIPTION} characters without U+0000"),
            );
            return None;
        }
        Some(text.to_owned())
    }
    fn finish(self, what: &str, issues: &mut Issues) {
        for key in self.table.keys() {
            if !self.used.contains(key.as_str()) {
                issues.push(
                    join(&self.path, key),
                    "unknown_key",
                    format!("is not a known key {what}"),
                );
            }
        }
    }
}

fn identifier(name: &str, max: usize) -> bool {
    let bytes = name.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= max
        && bytes[0].is_ascii_lowercase()
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'_')
}
/// `^[a-z][a-z0-9_]{0,47}$`, for types and components.
pub(crate) fn valid_type_name(name: &str) -> bool {
    identifier(name, 48)
}
/// `^[a-z][a-z0-9_]{0,63}$`.
pub(crate) fn valid_field_name(name: &str) -> bool {
    identifier(name, 64)
}
/// `^[a-z0-9][a-z0-9_-]{0,63}$`.
fn valid_enum_value(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 64
        && (bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit())
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'_' || *b == b'-')
}
/// A BCP 47 shaped code: a 2 to 8 letter language subtag, then subtags of
/// 1 to 8 letters or digits, 2 to 35 bytes in all.
fn valid_locale_code(code: &str) -> bool {
    let mut parts = code.split('-');
    let language_ok = parts
        .next()
        .is_some_and(|p| (2..=8).contains(&p.len()) && p.bytes().all(|b| b.is_ascii_alphabetic()));
    (2..=35).contains(&code.len())
        && language_ok
        && parts.all(|p| (1..=8).contains(&p.len()) && p.bytes().all(|b| b.is_ascii_alphanumeric()))
}
/// `type/subtype` or `type/*` of MIME token characters, lowercase.
fn valid_media_pattern(value: &str) -> bool {
    let token = |s: &str| {
        !s.is_empty()
            && s.len() <= 127
            && s.bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"!#$&^_.+-".contains(&b))
    };
    value
        .split_once('/')
        .is_some_and(|(kind, sub)| token(kind) && (sub == "*" || token(sub)))
}

/// Where a field definition sits, which decides its allowed common keys.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Site {
    /// A top-level field of a content type.
    Top { localized_type: bool },
    /// A field of a component.
    Component,
    /// A list's `of` item definition.
    Item,
}

/// Fields of one type or component with their index in the file.
struct Container {
    path: String,
    fields: Vec<(usize, Field)>,
}

fn parse_root(
    table: &Table,
    options: &ModelOptions<'_>,
    issues: &mut Issues,
) -> Option<ContentModel> {
    let mut root = Reader::new(table, "");
    if root.take("migrations").is_some() {
        issues.push(
            "migrations",
            "migrations_unsupported",
            "is not accepted in v1: model migrations are a later slice (C10)",
        );
    }
    let version = root.integer("version", 1, i64::from(i32::MAX), issues);
    if !root.present("version") {
        root.missing("version", issues);
    }
    let locales = parse_locales(root.take("locales"), issues);
    let mut containers: Vec<Container> = Vec::new();
    let mut components = BTreeMap::new();
    for (name, path, table) in sections(
        root.take("components"),
        "components",
        MAX_COMPONENTS,
        issues,
    ) {
        if !valid_type_name(name) {
            issues.push(path, "invalid_name", "must match ^[a-z][a-z0-9_]{0,47}$");
            continue;
        }
        if RESERVED_COMPONENT_NAMES.contains(name) {
            issues.push(
                path,
                "reserved_name",
                "is reserved as a rich text block type",
            );
            continue;
        }
        let mut reader = Reader::new(table, &path);
        let description = reader.description(issues);
        let fields = parse_fields(reader.take("fields"), &path, Site::Component, issues);
        reader.finish("of a component", issues);
        components.insert(
            name.to_owned(),
            Component {
                name: name.to_owned(),
                description,
                fields: fields.iter().map(|(_, f)| f.clone()).collect(),
            },
        );
        containers.push(Container { path, fields });
    }
    let mut types = BTreeMap::new();
    let mut type_containers = Vec::new();
    for (name, path, table) in sections(root.take("types"), "types", MAX_TYPES, issues) {
        if !valid_type_name(name) {
            issues.push(path, "invalid_name", "must match ^[a-z][a-z0-9_]{0,47}$");
            continue;
        }
        if RESERVED_TYPE_NAMES.contains(name) {
            issues.push(path, "reserved_name", "is reserved by the delivery routes");
            continue;
        }
        if let Some((content_type, container)) = parse_type(name, table, &path, options, issues) {
            types.insert(name.to_owned(), content_type);
            type_containers.push(container);
        }
    }
    root.finish("of the content model", issues);

    let component_names: BTreeSet<&str> = components.keys().map(String::as_str).collect();
    let type_names: BTreeSet<&str> = types.keys().map(String::as_str).collect();
    for container in containers.iter().chain(&type_containers) {
        for (index, field) in &container.fields {
            let path = format!("{}.fields[{index}]", container.path);
            check_references(field, &path, &component_names, &type_names, options, issues);
        }
    }
    let heights = component_heights(&components, issues);
    for container in &type_containers {
        for (index, field) in &container.fields {
            let depth = 1 + field
                .kind
                .nested_components()
                .iter()
                .filter_map(|c| heights.get(*c))
                .max()
                .copied()
                .unwrap_or(0);
            if depth > MAX_NESTING_DEPTH {
                issues.push(
                    format!("{}.fields[{index}]", container.path),
                    "nesting_too_deep",
                    format!("nests components deeper than {MAX_NESTING_DEPTH} levels"),
                );
            }
        }
    }
    let version = u32::try_from(version?).ok()?;
    if locales.is_empty() || !issues.is_empty() {
        return None;
    }
    let mut model = ContentModel {
        version,
        locales,
        components,
        types,
        canonical: String::new(),
        fingerprint: String::new(),
    };
    model.canonical = canonical::render(&model);
    model.fingerprint = canonical::fingerprint(&model.canonical);
    Some(model)
}

/// The `[components.*]` or `[types.*]` tables, each with its path.
fn sections<'t>(
    value: Option<&'t Toml>,
    key: &str,
    max: usize,
    issues: &mut Issues,
) -> Vec<(&'t str, String, &'t Table)> {
    let Some(value) = value else {
        return Vec::new();
    };
    let Toml::Table(table) = value else {
        issues.push(key, "invalid_type", "must be a table");
        return Vec::new();
    };
    if table.len() > max {
        issues.push(
            key,
            "out_of_range",
            format!("must declare at most {max} entries"),
        );
    }
    let mut out = Vec::new();
    for (name, value) in table.iter().take(max) {
        let path = join(key, name);
        match value {
            Toml::Table(inner) => out.push((name.as_str(), path, inner)),
            _ => issues.push(path, "invalid_type", "must be a table"),
        }
    }
    out
}

fn parse_locales(value: Option<&Toml>, issues: &mut Issues) -> Vec<Locale> {
    let Some(value) = value else {
        issues.push("locales", "missing_required", "is required");
        return Vec::new();
    };
    let Toml::Array(items) = value else {
        issues.push(
            "locales",
            "invalid_type",
            "must be an array of locale tables",
        );
        return Vec::new();
    };
    if items.is_empty() || items.len() > MAX_LOCALES {
        issues.push(
            "locales",
            "out_of_range",
            format!("must declare between 1 and {MAX_LOCALES} locales"),
        );
    }
    let before = issues.count();
    let mut locales: Vec<(usize, Locale)> = Vec::new();
    for (index, item) in items.iter().enumerate().take(MAX_LOCALES) {
        let path = format!("locales[{index}]");
        let Toml::Table(table) = item else {
            issues.push(path, "invalid_type", "must be a table");
            continue;
        };
        let mut reader = Reader::new(table, &path);
        let code = reader.string("code", issues);
        if !reader.present("code") {
            reader.missing("code", issues);
        }
        let default = reader.boolean("default", issues).unwrap_or(false);
        let fallback = reader.string("fallback", issues).map(str::to_owned);
        let code_path = reader.at("code");
        let fallback_path = reader.at("fallback");
        reader.finish("of a locale", issues);
        let Some(code) = code else { continue };
        if !valid_locale_code(code) {
            issues.push(
                code_path,
                "invalid_locale",
                "must be a BCP 47 code of 2 to 35 bytes such as fr-BE",
            );
            continue;
        }
        if locales
            .iter()
            .any(|(_, l)| l.code.eq_ignore_ascii_case(code))
        {
            issues.push(code_path, "duplicate_locale", "is declared twice");
            continue;
        }
        if default && fallback.is_some() {
            issues.push(
                fallback_path,
                "invalid_value",
                "is not allowed on the default locale",
            );
        }
        locales.push((
            index,
            Locale {
                code: code.to_owned(),
                default,
                fallback,
            },
        ));
    }
    let defaults: Vec<usize> = locales
        .iter()
        .filter(|(_, l)| l.default)
        .map(|(i, _)| *i)
        .collect();
    if defaults.is_empty() && !locales.is_empty() {
        issues.push(
            "locales",
            "default_locale",
            "must mark exactly one locale with default = true",
        );
    }
    for index in defaults.iter().skip(1) {
        issues.push(
            format!("locales[{index}].default"),
            "default_locale",
            "marks a second default locale",
        );
    }
    let codes: Vec<&str> = locales.iter().map(|(_, l)| l.code.as_str()).collect();
    for (index, locale) in &locales {
        let Some(fallback) = &locale.fallback else {
            continue;
        };
        let path = format!("locales[{index}].fallback");
        if !codes.contains(&fallback.as_str()) {
            issues.push(path, "unknown_locale", "names no configured locale");
            continue;
        }
        // Follow the chain; a chain longer than the locale count loops.
        let mut current = fallback.as_str();
        let mut steps = 0;
        let mut cycle = current == locale.code;
        while !cycle && steps <= codes.len() {
            let next = locales
                .iter()
                .find(|(_, l)| l.code == current)
                .and_then(|(_, l)| l.fallback.as_deref());
            match next {
                Some(next) if next == locale.code => cycle = true,
                Some(next) => current = next,
                None => break,
            }
            steps += 1;
        }
        if cycle || steps > codes.len() {
            issues.push(path, "fallback_cycle", "forms a fallback cycle");
        }
    }
    if issues.count() != before || defaults.len() != 1 {
        return Vec::new();
    }
    let default_code = locales
        .iter()
        .find(|(_, l)| l.default)
        .map(|(_, l)| l.code.clone());
    locales
        .into_iter()
        .map(|(_, mut l)| {
            if !l.default && l.fallback.is_none() {
                l.fallback = default_code.clone();
            }
            l
        })
        .collect()
}

fn parse_type(
    name: &str,
    table: &Table,
    path: &str,
    options: &ModelOptions<'_>,
    issues: &mut Issues,
) -> Option<(ContentType, Container)> {
    let mut reader = Reader::new(table, path);
    let kind = reader.choice(
        "kind",
        &[
            ("collection", Kind::Collection),
            ("singleton", Kind::Singleton),
        ],
        Kind::Collection,
        issues,
    );
    let scope = reader.choice(
        "scope",
        &[
            ("application", Scope::Application),
            ("organization", Scope::Organization),
        ],
        Scope::Application,
        issues,
    );
    if scope == Scope::Organization && !options.organizations {
        issues.push(
            reader.at("scope"),
            "organizations_required",
            "needs identity with organizations enabled",
        );
    }
    let localized = reader.boolean("localized", issues).unwrap_or(true);
    let access_path = reader.at("access");
    let access = parse_access(reader.take("access"), &access_path, options, issues);
    let title_field = reader.string("title_field", issues).map(str::to_owned);
    let title_path = reader.at("title_field");
    let description = reader.description(issues);
    let mut fields = parse_fields(
        reader.take("fields"),
        path,
        Site::Top {
            localized_type: localized,
        },
        issues,
    );
    reader.finish("of a content type", issues);
    if let Some(title) = &title_field {
        let is_string = fields
            .iter()
            .any(|(_, f)| &f.name == title && matches!(f.kind, FieldKind::String(_)));
        if is_string {
            // A top-level slug without `from` is generated from the title.
            for (_, field) in &mut fields {
                if let FieldKind::Slug(rules) = &mut field.kind
                    && rules.from.is_none()
                {
                    rules.from = Some(title.clone());
                }
            }
        } else {
            issues.push(
                title_path,
                "invalid_title_field",
                "must name a string field of the type",
            );
        }
    }
    let unique = fields.iter().filter(|(_, f)| f.unique).count();
    if unique > MAX_UNIQUE_FIELDS {
        issues.push(
            path,
            "too_many_unique",
            format!("declares more than {MAX_UNIQUE_FIELDS} unique fields"),
        );
    }
    let indexed = fields
        .iter()
        .filter(|(_, f)| f.filterable || f.sortable)
        .count();
    if indexed > MAX_INDEXED_FIELDS {
        issues.push(
            path,
            "too_many_indexed",
            format!("declares more than {MAX_INDEXED_FIELDS} filterable or sortable fields"),
        );
    }
    let content_type = ContentType {
        name: name.to_owned(),
        kind,
        scope,
        localized,
        // A missing or invalid `access` is already an issue, so this model
        // is never built; keeping the type avoids follow-on reference issues.
        access: access.unwrap_or(Access::Public),
        title_field,
        description,
        fields: fields.iter().map(|(_, f)| f.clone()).collect(),
    };
    Some((
        content_type,
        Container {
            path: path.to_owned(),
            fields,
        },
    ))
}

const ACCESS_FORMS: &str = "\"public\", \"authenticated\" or { permission = \"<name>\" }";

fn parse_access(
    value: Option<&Toml>,
    path: &str,
    options: &ModelOptions<'_>,
    issues: &mut Issues,
) -> Option<Access> {
    let Some(value) = value else {
        issues.push(
            path,
            "missing_required",
            format!("is required: {ACCESS_FORMS}"),
        );
        return None;
    };
    match value {
        Toml::String(s) if s == "public" => Some(Access::Public),
        Toml::String(s) if s == "authenticated" => Some(Access::Authenticated),
        Toml::Table(table) if table.len() == 1 => match table.get("permission") {
            Some(Toml::String(permission)) if merezha_domain::valid_name(permission) => {
                let known = CONTENT_PERMISSIONS.contains(permission.as_str())
                    || options.permissions.contains(permission);
                if known {
                    Some(Access::Permission(permission.clone()))
                } else {
                    issues.push(
                        path,
                        "unknown_permission",
                        "names a permission missing from the RBAC catalog",
                    );
                    None
                }
            }
            _ => {
                issues.push(path, "invalid_access", format!("must be {ACCESS_FORMS}"));
                None
            }
        },
        _ => {
            issues.push(path, "invalid_access", format!("must be {ACCESS_FORMS}"));
            None
        }
    }
}

fn parse_fields(
    value: Option<&Toml>,
    container: &str,
    site: Site,
    issues: &mut Issues,
) -> Vec<(usize, Field)> {
    let path = join(container, "fields");
    let Some(value) = value else {
        issues.push(path, "missing_required", "is required");
        return Vec::new();
    };
    let Toml::Array(items) = value else {
        issues.push(path, "invalid_type", "must be an array of field tables");
        return Vec::new();
    };
    if items.is_empty() || items.len() > MAX_FIELDS {
        issues.push(
            path.clone(),
            "out_of_range",
            format!("must declare between 1 and {MAX_FIELDS} fields"),
        );
    }
    let mut fields: Vec<(usize, Field)> = Vec::new();
    for (index, item) in items.iter().enumerate().take(MAX_FIELDS) {
        let at = format!("{path}[{index}]");
        let Toml::Table(table) = item else {
            issues.push(at, "invalid_type", "must be a table");
            continue;
        };
        let Some(field) = parse_field(table, &at, site, issues) else {
            continue;
        };
        if fields.iter().any(|(_, f)| f.name == field.name) {
            issues.push(join(&at, "name"), "duplicate_name", "is declared twice");
            continue;
        }
        fields.push((index, field));
    }
    for (index, field) in &fields {
        if let FieldKind::Slug(SlugRules {
            from: Some(from), ..
        }) = &field.kind
        {
            let ok = fields
                .iter()
                .any(|(_, f)| &f.name == from && matches!(f.kind, FieldKind::String(_)));
            if !ok {
                issues.push(
                    format!("{path}[{index}].from"),
                    "invalid_slug_source",
                    "must name a string field beside the slug",
                );
            }
        }
    }
    fields
}

fn parse_field(table: &Table, path: &str, site: Site, issues: &mut Issues) -> Option<Field> {
    let mut reader = Reader::new(table, path);
    let name = if site == Site::Item {
        Some(String::new())
    } else {
        let name = reader.string("name", issues);
        if !reader.present("name") {
            reader.missing("name", issues);
        }
        match name {
            Some(n) if !valid_field_name(n) => {
                issues.push(
                    reader.at("name"),
                    "invalid_name",
                    "must match ^[a-z][a-z0-9_]{0,63}$",
                );
                None
            }
            Some(n) if RESERVED_FIELD_NAMES.contains(n) => {
                issues.push(
                    reader.at("name"),
                    "reserved_name",
                    "is reserved for system attributes",
                );
                None
            }
            other => other.map(str::to_owned),
        }
    };
    let type_name = reader.string("type", issues);
    if !reader.present("type") {
        reader.missing("type", issues);
    }
    let description = reader.description(issues);
    let mut flags = Flags::default();
    match site {
        Site::Top { .. } => {
            flags.required = reader.boolean("required", issues);
            flags.localized = reader.boolean("localized", issues);
            flags.unique = reader.boolean("unique", issues);
            flags.filterable = reader.boolean("filterable", issues);
            flags.sortable = reader.boolean("sortable", issues);
            flags.default = reader.take("default");
        }
        Site::Component => {
            flags.required = reader.boolean("required", issues);
            if reader.take("default").is_some() {
                issues.push(
                    reader.at("default"),
                    "not_allowed",
                    "is only allowed on top-level fields of a content type",
                );
            }
            if reader.take("localized").is_some() {
                issues.push(
                    reader.at("localized"),
                    "not_allowed",
                    "is not allowed inside components: a component follows its containing field",
                );
            }
            for key in ["unique", "filterable", "sortable"] {
                if reader.take(key).is_some() {
                    issues.push(
                        reader.at(key),
                        "not_allowed",
                        "is only allowed on top-level fields of a content type",
                    );
                }
            }
        }
        Site::Item => {}
    }
    let kind = parse_kind(type_name?, &mut reader, path, issues)?;
    if site == Site::Item
        && let FieldKind::Slug(SlugRules { from: Some(_), .. }) = &kind
    {
        issues.push(
            join(path, "from"),
            "not_allowed",
            "is not allowed on list items: an item has no sibling field",
        );
    }
    let what = if site == Site::Item {
        "for list items".to_owned()
    } else {
        format!("for {} fields", kind.type_name())
    };
    reader.finish(&what, issues);
    let flag_path = |key: &str| join(path, key);
    if flags.unique == Some(true) && !kind.can_be_unique() {
        issues.push(
            flag_path("unique"),
            "not_allowed",
            "is only allowed on string, slug, integer and enum fields",
        );
    }
    if flags.sortable == Some(true) && !kind.can_be_sortable() {
        issues.push(
            flag_path("sortable"),
            "not_allowed",
            "is only allowed on string, slug, integer, float, decimal, date, datetime and enum fields",
        );
    }
    if flags.filterable == Some(true) && !kind.can_be_filterable() {
        issues.push(
            flag_path("filterable"),
            "not_allowed",
            "is only allowed on scalar, reference and scalar list fields",
        );
    }
    if let Site::Top {
        localized_type: false,
    } = site
        && flags.localized == Some(true)
    {
        issues.push(
            flag_path("localized"),
            "not_allowed",
            "requires a localized content type",
        );
    }
    let default = match flags.default {
        None => None,
        Some(_) if !kind.is_scalar() => {
            issues.push(
                flag_path("default"),
                "not_allowed",
                "is only allowed on scalar fields",
            );
            None
        }
        Some(raw) => match toml_scalar(raw).map(|v| scalar::check(&kind, &v)) {
            Some(Ok(value)) => Some(value),
            Some(Err(code)) => {
                issues.push(
                    flag_path("default"),
                    "invalid_default",
                    format!("does not satisfy the field's rules ({code})"),
                );
                None
            }
            None => {
                issues.push(
                    flag_path("default"),
                    "invalid_default",
                    "must be a scalar value",
                );
                None
            }
        },
    };
    // A field with problems is still returned for the cross-reference
    // checks; the issues already keep the model from being built.
    let top = matches!(site, Site::Top { .. });
    let localized = match site {
        Site::Top { localized_type } => localized_type && flags.localized.unwrap_or(true),
        _ => true,
    };
    let sortable = flags.sortable.unwrap_or(false);
    Some(Field {
        name: name?,
        description,
        required: flags.required.unwrap_or(false),
        localized,
        unique: top && flags.unique.unwrap_or(matches!(kind, FieldKind::Slug(_))),
        filterable: flags.filterable.unwrap_or(false) || sortable,
        sortable,
        default,
        kind,
    })
}

#[derive(Default)]
struct Flags<'t> {
    required: Option<bool>,
    localized: Option<bool>,
    unique: Option<bool>,
    filterable: Option<bool>,
    sortable: Option<bool>,
    default: Option<&'t Toml>,
}

/// A TOML scalar as JSON; datetimes become their RFC 3339 text.
fn toml_scalar(value: &Toml) -> Option<Value> {
    match value {
        Toml::String(s) => Some(Value::String(s.clone())),
        Toml::Integer(n) => Some(Value::from(*n)),
        Toml::Float(f) => serde_json::Number::from_f64(*f).map(Value::Number),
        Toml::Boolean(b) => Some(Value::Bool(*b)),
        Toml::Datetime(d) => Some(Value::String(d.to_string())),
        Toml::Array(_) | Toml::Table(_) => None,
    }
}

const FIELD_TYPES: &str = "string, text, rich_text, integer, float, decimal, boolean, datetime, date, enum, slug, reference, asset, component, list, blocks or json";

fn order<T: PartialOrd>(
    reader: &Reader<'_>,
    (low_key, low): (&str, Option<T>),
    (high_key, high): (&str, Option<T>),
    issues: &mut Issues,
) {
    if let (Some(low), Some(high)) = (low, high)
        && low > high
    {
        issues.push(
            reader.at(low_key),
            "out_of_range",
            format!("must not exceed {high_key}"),
        );
    }
}

fn items(reader: &mut Reader<'_>, issues: &mut Issues, allowed: bool) -> (Option<u32>, u32) {
    if !allowed {
        for key in ["min_items", "max_items"] {
            if reader.take(key).is_some() {
                issues.push(
                    reader.at(key),
                    "not_allowed",
                    "requires cardinality = \"many\"",
                );
            }
        }
        return (None, 100);
    }
    let min = reader.count("min_items", 0, 1000, issues);
    let max = reader.count("max_items", 1, 1000, issues);
    order(
        reader,
        ("min_items", min),
        ("max_items", max.or(Some(100))),
        issues,
    );
    (min, max.unwrap_or(100))
}

fn cardinality(reader: &mut Reader<'_>, issues: &mut Issues) -> Cardinality {
    reader.choice(
        "cardinality",
        &[("one", Cardinality::One), ("many", Cardinality::Many)],
        Cardinality::One,
        issues,
    )
}

fn names(
    reader: &mut Reader<'_>,
    key: &str,
    bounds: (usize, usize),
    issues: &mut Issues,
) -> Option<Vec<String>> {
    reader.strings(
        key,
        bounds,
        &valid_type_name,
        "must match ^[a-z][a-z0-9_]{0,47}$",
        issues,
    )
}

fn required_names(
    reader: &mut Reader<'_>,
    key: &str,
    bounds: (usize, usize),
    issues: &mut Issues,
) -> Vec<String> {
    if !reader.present(key) {
        reader.missing(key, issues);
    }
    names(reader, key, bounds, issues).unwrap_or_default()
}

fn vocabulary(
    reader: &mut Reader<'_>,
    key: &str,
    allowed: &[&str],
    min: usize,
    default: &[&str],
    issues: &mut Issues,
) -> Vec<String> {
    let rule = format!("must be one of {}", allowed.join(", "));
    reader
        .strings(
            key,
            (min, allowed.len()),
            &|s| allowed.contains(&s),
            &rule,
            issues,
        )
        .unwrap_or_else(|| default.iter().map(|s| (*s).to_owned()).collect())
}

/// A `min`/`max` bound of a date or datetime field.
fn time_bound(
    reader: &mut Reader<'_>,
    key: &str,
    check: fn(&str) -> Result<String, &'static str>,
    rule: &str,
    issues: &mut Issues,
) -> Option<String> {
    let text = match reader.take(key)? {
        Toml::String(s) => s.clone(),
        Toml::Datetime(d) => d.to_string(),
        _ => {
            issues.push(reader.at(key), "invalid_type", "must be a string");
            return None;
        }
    };
    match check(&text) {
        Ok(normalized) => Some(normalized),
        Err(_) => {
            issues.push(reader.at(key), "invalid_value", rule.to_owned());
            None
        }
    }
}

fn float_bound(reader: &mut Reader<'_>, key: &str, issues: &mut Issues) -> Option<f64> {
    match reader.take(key)? {
        // `-0.0` and `0.0` are one bound, so they fingerprint the same.
        Toml::Float(f) if *f == 0.0 => Some(0.0),
        Toml::Float(f) if f.is_finite() => Some(*f),
        // Integers in the safe range are exact as binary64.
        Toml::Integer(n) if n.unsigned_abs() <= MAX_SAFE_INTEGER as u64 => Some(*n as f64),
        _ => {
            issues.push(
                reader.at(key),
                "invalid_type",
                "must be a finite number within the safe integer range",
            );
            None
        }
    }
}

fn pattern(reader: &mut Reader<'_>, issues: &mut Issues) -> Option<Pattern> {
    let source = reader.string("pattern", issues)?;
    let at = reader.at("pattern");
    if source.len() > MAX_PATTERN_BYTES {
        issues.push(
            at,
            "out_of_range",
            format!("must be at most {MAX_PATTERN_BYTES} bytes"),
        );
        return None;
    }
    let build = |text: &str| {
        regex::RegexBuilder::new(text)
            .size_limit(PATTERN_SIZE_LIMIT)
            .dfa_size_limit(PATTERN_SIZE_LIMIT)
            .build()
    };
    // The bare pattern must compile on its own, so `a)|(b` cannot escape
    // the anchors added around it.
    match build(source).and_then(|_| build(&format!("^(?:{source})$"))) {
        Ok(regex) => Some(Pattern {
            source: source.to_owned(),
            regex,
        }),
        Err(regex::Error::CompiledTooBig(_)) => {
            issues.push(
                at,
                "pattern_too_large",
                format!("compiles beyond the {PATTERN_SIZE_LIMIT} byte size limit"),
            );
            None
        }
        Err(_) => {
            issues.push(at, "invalid_pattern", "is not a valid regular expression");
            None
        }
    }
}

fn parse_kind(
    type_name: &str,
    reader: &mut Reader<'_>,
    path: &str,
    issues: &mut Issues,
) -> Option<FieldKind> {
    Some(match type_name {
        "string" => {
            let min = reader.count("min_length", 0, 4096, issues);
            let max = reader.count("max_length", 1, 4096, issues);
            order(
                reader,
                ("min_length", min),
                ("max_length", max.or(Some(255))),
                issues,
            );
            FieldKind::String(StringRules {
                min_length: min,
                max_length: max.unwrap_or(255),
                pattern: pattern(reader, issues),
            })
        }
        "text" => {
            let min = reader.count("min_length", 0, 100_000, issues);
            let max = reader.count("max_length", 1, 100_000, issues);
            order(
                reader,
                ("min_length", min),
                ("max_length", max.or(Some(10_000))),
                issues,
            );
            FieldKind::Text(TextRules {
                min_length: min,
                max_length: max.unwrap_or(10_000),
            })
        }
        "rich_text" => {
            let styles = vocabulary(
                reader,
                "styles",
                &RICH_TEXT_STYLES,
                1,
                &RICH_TEXT_STYLES,
                issues,
            );
            let marks = vocabulary(
                reader,
                "marks",
                &RICH_TEXT_MARKS,
                0,
                &RICH_TEXT_MARKS,
                issues,
            );
            let annotations = vocabulary(
                reader,
                "annotations",
                &RICH_TEXT_ANNOTATIONS,
                0,
                &["link"],
                issues,
            );
            let lists = vocabulary(
                reader,
                "lists",
                &RICH_TEXT_LISTS,
                0,
                &RICH_TEXT_LISTS,
                issues,
            );
            let embeds = reader
                .strings(
                    "embeds",
                    (0, 32),
                    &|s| s == "asset" || valid_type_name(s),
                    "must be \"asset\" or a component name",
                    issues,
                )
                .unwrap_or_default();
            let max_blocks = reader.count("max_blocks", 1, 5000, issues).unwrap_or(500);
            let max_text_length = reader
                .count("max_text_length", 1, 1_000_000, issues)
                .unwrap_or(100_000);
            let entry_link_to = names(reader, "entry_link_to", (1, MAX_TYPES), issues);
            if entry_link_to.is_some() && !annotations.iter().any(|a| a == "entry_link") {
                issues.push(
                    reader.at("entry_link_to"),
                    "not_allowed",
                    "requires the entry_link annotation",
                );
            }
            FieldKind::RichText(RichTextRules {
                styles,
                marks,
                annotations,
                lists,
                embeds,
                max_blocks,
                max_text_length,
                entry_link_to,
            })
        }
        "integer" => {
            let min = reader.integer("min", -MAX_SAFE_INTEGER, MAX_SAFE_INTEGER, issues);
            let max = reader.integer("max", -MAX_SAFE_INTEGER, MAX_SAFE_INTEGER, issues);
            order(reader, ("min", min), ("max", max), issues);
            FieldKind::Integer(IntegerRules { min, max })
        }
        "float" => {
            let min = float_bound(reader, "min", issues);
            let max = float_bound(reader, "max", issues);
            order(reader, ("min", min), ("max", max), issues);
            FieldKind::Float(FloatRules { min, max })
        }
        "decimal" => {
            let precision = reader.count("precision", 1, 28, issues).unwrap_or(18);
            let scale = reader.count("scale", 0, 28, issues).unwrap_or(2);
            let mut bounds = [None, None];
            if scale > precision {
                issues.push(
                    reader.at("scale"),
                    "out_of_range",
                    "must not exceed precision",
                );
            } else {
                for (slot, key) in bounds.iter_mut().zip(["min", "max"]) {
                    let Some(text) = reader.string(key, issues) else {
                        continue;
                    };
                    match scalar::decimal(text, precision, scale) {
                        Ok(normalized) => *slot = Some(normalized),
                        Err(_) => issues.push(
                            reader.at(key),
                            "invalid_value",
                            "must be a decimal string within the field's precision and scale",
                        ),
                    }
                }
            }
            let [min, max] = bounds;
            let number = |b: &Option<String>| b.as_deref().and_then(scalar::decimal_number);
            order(reader, ("min", number(&min)), ("max", number(&max)), issues);
            FieldKind::Decimal(DecimalRules {
                precision,
                scale,
                min,
                max,
            })
        }
        "boolean" => FieldKind::Boolean,
        "datetime" => {
            let rule =
                "must be an RFC 3339 datetime with an offset and at most 3 fractional digits";
            let min = time_bound(reader, "min", scalar::datetime, rule, issues);
            let max = time_bound(reader, "max", scalar::datetime, rule, issues);
            order(reader, ("min", min.clone()), ("max", max.clone()), issues);
            FieldKind::Datetime(TextRange { min, max })
        }
        "date" => {
            let rule = "must be a YYYY-MM-DD date";
            let min = time_bound(reader, "min", scalar::date, rule, issues);
            let max = time_bound(reader, "max", scalar::date, rule, issues);
            order(reader, ("min", min.clone()), ("max", max.clone()), issues);
            FieldKind::Date(TextRange { min, max })
        }
        "enum" => {
            if !reader.present("values") {
                reader.missing("values", issues);
            }
            let values = reader
                .strings(
                    "values",
                    (1, 200),
                    &valid_enum_value,
                    "must match ^[a-z0-9][a-z0-9_-]{0,63}$",
                    issues,
                )
                .unwrap_or_default();
            FieldKind::Enum(EnumRules { values })
        }
        "slug" => {
            let from = reader.string("from", issues).map(str::to_owned);
            let max_length = reader.count("max_length", 1, 200, issues).unwrap_or(96);
            FieldKind::Slug(SlugRules { from, max_length })
        }
        "reference" => {
            let to = required_names(reader, "to", (1, 16), issues);
            let cardinality = cardinality(reader, issues);
            let (min_items, max_items) = items(reader, issues, cardinality == Cardinality::Many);
            let require_published = reader.boolean("require_published", issues).unwrap_or(false);
            FieldKind::Reference(ReferenceRules {
                to,
                cardinality,
                min_items,
                max_items,
                require_published,
            })
        }
        "asset" => {
            let accept = reader
                .strings(
                    "accept",
                    (1, 32),
                    &valid_media_pattern,
                    "must be a lowercase MIME type such as image/png or a pattern such as image/*",
                    issues,
                )
                .unwrap_or_default();
            let cardinality = cardinality(reader, issues);
            let (min_items, max_items) = items(reader, issues, cardinality == Cardinality::Many);
            FieldKind::Asset(AssetRules {
                accept,
                cardinality,
                min_items,
                max_items,
            })
        }
        "component" => {
            let component = reader.string("component", issues);
            if !reader.present("component") {
                reader.missing("component", issues);
            }
            match component {
                Some(c) if valid_type_name(c) => FieldKind::Component(c.to_owned()),
                Some(_) => {
                    issues.push(
                        reader.at("component"),
                        "invalid_name",
                        "must match ^[a-z][a-z0-9_]{0,47}$",
                    );
                    return None;
                }
                None => return None,
            }
        }
        "list" => {
            let of_path = join(path, "of");
            let of = match reader.take("of") {
                Some(Toml::Table(table)) => parse_field(table, &of_path, Site::Item, issues),
                Some(_) => {
                    issues.push(of_path.clone(), "invalid_type", "must be a field table");
                    None
                }
                None => {
                    reader.missing("of", issues);
                    None
                }
            };
            let of = of.filter(|of| {
                let ok = of.kind.is_scalar() || matches!(of.kind, FieldKind::Component(_));
                if !ok {
                    issues.push(
                        join(&of_path, "type"),
                        "not_allowed",
                        "must be a scalar type or component",
                    );
                }
                ok
            });
            let (min_items, max_items) = items(reader, issues, true);
            FieldKind::List(ListRules {
                of: Box::new(of?),
                min_items,
                max_items,
            })
        }
        "blocks" => {
            let components = required_names(reader, "components", (1, 32), issues);
            let (min_items, max_items) = items(reader, issues, true);
            FieldKind::Blocks(BlocksRules {
                components,
                min_items,
                max_items,
            })
        }
        "json" => FieldKind::Json(JsonRules {
            max_bytes: reader
                .count("max_bytes", 1, 262_144, issues)
                .unwrap_or(16_384),
        }),
        _ => {
            issues.push(
                reader.at("type"),
                "unknown_field_type",
                format!("must be one of {FIELD_TYPES}"),
            );
            return None;
        }
    })
}

fn check_references(
    field: &Field,
    path: &str,
    components: &BTreeSet<&str>,
    types: &BTreeSet<&str>,
    options: &ModelOptions<'_>,
    issues: &mut Issues,
) {
    let component = |name: &str, at: String, issues: &mut Issues| {
        if !components.contains(name) {
            issues.push(at, "unknown_component", "names no declared component");
        }
    };
    let content_type = |name: &str, at: String, issues: &mut Issues| {
        if !types.contains(name) {
            issues.push(at, "unknown_type", "names no declared content type");
        }
    };
    match &field.kind {
        FieldKind::Component(name) => component(name, join(path, "component"), issues),
        FieldKind::List(list) => {
            check_references(
                &list.of,
                &join(path, "of"),
                components,
                types,
                options,
                issues,
            );
        }
        FieldKind::Blocks(blocks) => {
            for (index, name) in blocks.components.iter().enumerate() {
                component(name, format!("{path}.components[{index}]"), issues);
            }
        }
        FieldKind::Reference(reference) => {
            for (index, name) in reference.to.iter().enumerate() {
                content_type(name, format!("{path}.to[{index}]"), issues);
            }
        }
        FieldKind::Asset(_) if !options.media => issues.push(
            join(path, "type"),
            "media_disabled",
            "needs [modules.content.media] to be configured",
        ),
        FieldKind::RichText(rich) => {
            for (index, name) in rich.embeds.iter().enumerate() {
                let at = format!("{path}.embeds[{index}]");
                if name == "asset" {
                    if !options.media {
                        issues.push(
                            at,
                            "media_disabled",
                            "needs [modules.content.media] to be configured",
                        );
                    }
                } else {
                    component(name, at, issues);
                }
            }
            for (index, name) in rich.entry_link_to.iter().flatten().enumerate() {
                content_type(name, format!("{path}.entry_link_to[{index}]"), issues);
            }
        }
        _ => {}
    }
}

/// Levels of fields each component contributes, its own fields counting
/// as one. A component that contains itself is reported and counted as 1.
fn component_heights(
    components: &BTreeMap<String, Component>,
    issues: &mut Issues,
) -> HashMap<String, usize> {
    #[derive(Clone, Copy, PartialEq)]
    enum State {
        Visiting,
        Done(usize),
    }
    fn visit<'c>(
        name: &'c str,
        components: &'c BTreeMap<String, Component>,
        states: &mut HashMap<&'c str, State>,
        issues: &mut Issues,
    ) -> usize {
        match states.get(name) {
            Some(State::Done(height)) => return *height,
            Some(State::Visiting) => {
                issues.push(
                    join("components", name),
                    "component_cycle",
                    "contains itself through its fields",
                );
                return 1;
            }
            None => {}
        }
        let Some(component) = components.get(name) else {
            return 0;
        };
        states.insert(name, State::Visiting);
        let mut height = 1;
        for field in &component.fields {
            for nested in field.kind.nested_components() {
                height = height.max(1 + visit(nested, components, states, issues));
            }
        }
        states.insert(name, State::Done(height));
        height
    }
    let mut states = HashMap::new();
    for name in components.keys() {
        visit(name, components, &mut states, issues);
    }
    states
        .into_iter()
        .filter_map(|(name, state)| match state {
            State::Done(height) => Some((name.to_owned(), height)),
            State::Visiting => None,
        })
        .collect()
}
