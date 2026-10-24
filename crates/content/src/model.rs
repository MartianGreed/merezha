//! The validated content model: locales, components, content types and
//! their fields. A [`ContentModel`] only exists after
//! [`ContentModel::parse`] accepted it, so every value here satisfies the
//! rules of `docs/design/content.md` section 2.
use regex::Regex;
use serde_json::Value;
use std::collections::BTreeMap;

/// Most content types in one model.
pub const MAX_TYPES: usize = 64;
/// Most components in one model.
pub const MAX_COMPONENTS: usize = 32;
/// Most locales in one model.
pub const MAX_LOCALES: usize = 32;
/// Most fields in one content type or component.
pub const MAX_FIELDS: usize = 100;
/// Deepest nesting of component fields, a type's own fields counting as 1.
pub const MAX_NESTING_DEPTH: usize = 4;
/// Most `unique` fields per content type, slugs included.
pub const MAX_UNIQUE_FIELDS: usize = 8;
/// Most `filterable` or `sortable` fields per content type.
pub const MAX_INDEXED_FIELDS: usize = 16;
/// The safe integer range of application values, ±(2^53 - 1).
pub const MAX_SAFE_INTEGER: i64 = 9_007_199_254_740_991;
/// Type names that share the delivery path namespace.
pub const RESERVED_TYPE_NAMES: [&str; 5] = ["manage", "assets", "model", "o", "health"];
/// Component names that would be ambiguous as a rich text `_type`.
pub const RESERVED_COMPONENT_NAMES: [&str; 3] = ["asset", "block", "span"];
/// Field names reserved for system attributes. Names starting with `_`
/// are reserved too; the field name syntax already excludes them.
pub const RESERVED_FIELD_NAMES: [&str; 4] = ["id", "type", "locale", "sys"];
/// Rich text block styles.
pub const RICH_TEXT_STYLES: [&str; 8] =
    ["normal", "h1", "h2", "h3", "h4", "h5", "h6", "blockquote"];
/// Rich text decorators.
pub const RICH_TEXT_MARKS: [&str; 5] = ["strong", "em", "code", "underline", "strike"];
/// Rich text annotations, the `markDefs` types.
pub const RICH_TEXT_ANNOTATIONS: [&str; 2] = ["link", "entry_link"];
/// Rich text list item kinds.
pub const RICH_TEXT_LISTS: [&str; 2] = ["bullet", "number"];

/// A validated content model with its canonical form and fingerprint.
#[derive(Clone, Debug)]
pub struct ContentModel {
    pub(crate) version: u32,
    pub(crate) locales: Vec<Locale>,
    pub(crate) components: BTreeMap<String, Component>,
    pub(crate) types: BTreeMap<String, ContentType>,
    pub(crate) canonical: String,
    pub(crate) fingerprint: String,
}
impl ContentModel {
    /// The model's `version`, 1 to `i32::MAX`.
    pub fn version(&self) -> u32 {
        self.version
    }
    /// Locales in model order. Exactly one is the default, and every other
    /// one names its fallback (the default locale when the file named none).
    pub fn locales(&self) -> &[Locale] {
        &self.locales
    }
    /// The default locale.
    pub fn default_locale(&self) -> &Locale {
        // Parsing guarantees exactly one default locale.
        self.locales
            .iter()
            .find(|l| l.default)
            .unwrap_or(&self.locales[0])
    }
    /// Components by name, in name order.
    pub fn components(&self) -> &BTreeMap<String, Component> {
        &self.components
    }
    /// Content types by name, in name order.
    pub fn types(&self) -> &BTreeMap<String, ContentType> {
        &self.types
    }
    pub fn component(&self, name: &str) -> Option<&Component> {
        self.components.get(name)
    }
    pub fn content_type(&self, name: &str) -> Option<&ContentType> {
        self.types.get(name)
    }
    /// The canonical JSON form: sorted keys, no whitespace, every default
    /// filled in. Stored as `models.definition`.
    pub fn canonical_json(&self) -> &str {
        &self.canonical
    }
    /// Lowercase hex SHA-256 of [`ContentModel::canonical_json`].
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }
}

/// A configured locale.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Locale {
    /// A BCP 47 code such as `fr-BE`, 2 to 35 bytes.
    pub code: String,
    pub default: bool,
    /// `None` only for the default locale.
    pub fallback: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Collection,
    Singleton,
}
impl Kind {
    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Collection => "collection",
            Kind::Singleton => "singleton",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scope {
    /// Stored as `app`.
    Application,
    /// Stored as `org:<organization id>`.
    Organization,
}
impl Scope {
    pub fn as_str(self) -> &'static str {
        match self {
            Scope::Application => "application",
            Scope::Organization => "organization",
        }
    }
}

/// Who reads published entries through delivery (owner answer CT1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Access {
    /// Anyone, anonymous included.
    Public,
    /// Any principal; for organization types one whose tenant is the
    /// path's organization.
    Authenticated,
    /// A permission from the RBAC catalog, checked in the type's resource
    /// scope.
    Permission(String),
}

/// A content type.
#[derive(Clone, Debug)]
pub struct ContentType {
    pub name: String,
    pub kind: Kind,
    pub scope: Scope,
    pub localized: bool,
    pub access: Access,
    /// A top-level `string` field.
    pub title_field: Option<String>,
    pub description: Option<String>,
    /// In model order.
    pub fields: Vec<Field>,
}
impl ContentType {
    pub fn field(&self, name: &str) -> Option<&Field> {
        self.fields.iter().find(|f| f.name == name)
    }
}

/// A reusable group of fields embedded in entries.
#[derive(Clone, Debug)]
pub struct Component {
    pub name: String,
    pub description: Option<String>,
    pub fields: Vec<Field>,
}

/// A field of a content type or component, or the item definition of a
/// `list` (`of`, whose `name` is empty).
#[derive(Clone, Debug)]
pub struct Field {
    pub name: String,
    pub description: Option<String>,
    /// Checked at publish time only.
    pub required: bool,
    /// For top-level fields of a content type: `false` makes it a shared
    /// field owned by the default-locale variant. Always `false` in a
    /// non-localized type. Component fields and list items follow their
    /// containing field and keep `true`.
    pub localized: bool,
    /// Top-level only; `true` by default for slugs.
    pub unique: bool,
    /// Top-level only; `true` whenever `sortable` is.
    pub filterable: bool,
    /// Top-level only.
    pub sortable: bool,
    /// The normalized default for new variants (scalar types only).
    pub default: Option<Value>,
    pub kind: FieldKind,
}

/// A field type and its type-specific rules, defaults filled in.
#[derive(Clone, Debug)]
pub enum FieldKind {
    String(StringRules),
    Text(TextRules),
    RichText(RichTextRules),
    Integer(IntegerRules),
    Float(FloatRules),
    Decimal(DecimalRules),
    Boolean,
    Datetime(TextRange),
    Date(TextRange),
    Enum(EnumRules),
    Slug(SlugRules),
    Reference(ReferenceRules),
    Asset(AssetRules),
    /// The component's name.
    Component(String),
    List(ListRules),
    Blocks(BlocksRules),
    Json(JsonRules),
}
impl FieldKind {
    /// The `type` name in the model file.
    pub fn type_name(&self) -> &'static str {
        match self {
            FieldKind::String(_) => "string",
            FieldKind::Text(_) => "text",
            FieldKind::RichText(_) => "rich_text",
            FieldKind::Integer(_) => "integer",
            FieldKind::Float(_) => "float",
            FieldKind::Decimal(_) => "decimal",
            FieldKind::Boolean => "boolean",
            FieldKind::Datetime(_) => "datetime",
            FieldKind::Date(_) => "date",
            FieldKind::Enum(_) => "enum",
            FieldKind::Slug(_) => "slug",
            FieldKind::Reference(_) => "reference",
            FieldKind::Asset(_) => "asset",
            FieldKind::Component(_) => "component",
            FieldKind::List(_) => "list",
            FieldKind::Blocks(_) => "blocks",
            FieldKind::Json(_) => "json",
        }
    }
    /// Scalar types: those that take `default` and may be list items.
    pub fn is_scalar(&self) -> bool {
        matches!(
            self,
            FieldKind::String(_)
                | FieldKind::Text(_)
                | FieldKind::Integer(_)
                | FieldKind::Float(_)
                | FieldKind::Decimal(_)
                | FieldKind::Boolean
                | FieldKind::Datetime(_)
                | FieldKind::Date(_)
                | FieldKind::Enum(_)
                | FieldKind::Slug(_)
        )
    }
    /// Types that may be `unique`.
    pub fn can_be_unique(&self) -> bool {
        matches!(
            self,
            FieldKind::String(_) | FieldKind::Slug(_) | FieldKind::Integer(_) | FieldKind::Enum(_)
        )
    }
    /// Types that may be `sortable` and used with range filters.
    pub fn can_be_sortable(&self) -> bool {
        matches!(
            self,
            FieldKind::String(_)
                | FieldKind::Slug(_)
                | FieldKind::Integer(_)
                | FieldKind::Float(_)
                | FieldKind::Decimal(_)
                | FieldKind::Date(_)
                | FieldKind::Datetime(_)
                | FieldKind::Enum(_)
        )
    }
    /// Types that may be `filterable`: scalars, references and lists of
    /// scalars.
    pub fn can_be_filterable(&self) -> bool {
        match self {
            FieldKind::Reference(_) => true,
            FieldKind::List(list) => list.of.kind.is_scalar(),
            other => other.is_scalar(),
        }
    }
    /// Components whose fields this field embeds.
    pub fn nested_components(&self) -> Vec<&str> {
        match self {
            FieldKind::Component(name) => vec![name.as_str()],
            FieldKind::List(list) => list.of.kind.nested_components(),
            FieldKind::Blocks(blocks) => blocks.components.iter().map(String::as_str).collect(),
            FieldKind::RichText(rich) => rich
                .embeds
                .iter()
                .filter(|e| e.as_str() != "asset")
                .map(String::as_str)
                .collect(),
            _ => Vec::new(),
        }
    }
}

/// A compiled `pattern`, anchored as `^(?:...)$`.
#[derive(Clone)]
pub struct Pattern {
    pub source: String,
    pub(crate) regex: Regex,
}
impl Pattern {
    pub fn is_match(&self, value: &str) -> bool {
        self.regex.is_match(value)
    }
}
impl std::fmt::Debug for Pattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Pattern").field(&self.source).finish()
    }
}

#[derive(Clone, Debug)]
pub struct StringRules {
    pub min_length: Option<u32>,
    pub max_length: u32,
    pub pattern: Option<Pattern>,
}
#[derive(Clone, Debug)]
pub struct TextRules {
    pub min_length: Option<u32>,
    pub max_length: u32,
}
#[derive(Clone, Debug)]
pub struct RichTextRules {
    pub styles: Vec<String>,
    pub marks: Vec<String>,
    pub annotations: Vec<String>,
    pub lists: Vec<String>,
    /// `asset` or component names.
    pub embeds: Vec<String>,
    pub max_blocks: u32,
    /// Bound on the total span text, in Unicode scalar values.
    pub max_text_length: u32,
    /// Types `entry_link` annotations may target; `None` is every type.
    pub entry_link_to: Option<Vec<String>>,
}
#[derive(Clone, Debug)]
pub struct IntegerRules {
    pub min: Option<i64>,
    pub max: Option<i64>,
}
#[derive(Clone, Debug)]
pub struct FloatRules {
    pub min: Option<f64>,
    pub max: Option<f64>,
}
#[derive(Clone, Debug)]
pub struct DecimalRules {
    pub precision: u32,
    pub scale: u32,
    /// Normalized to `scale` fractional digits.
    pub min: Option<String>,
    pub max: Option<String>,
}
/// Bounds of a `date` or `datetime` field in their stored form, which sorts
/// in time order.
#[derive(Clone, Debug)]
pub struct TextRange {
    pub min: Option<String>,
    pub max: Option<String>,
}
#[derive(Clone, Debug)]
pub struct EnumRules {
    pub values: Vec<String>,
}
#[derive(Clone, Debug)]
pub struct SlugRules {
    /// A sibling `string` field the slug is generated from.
    pub from: Option<String>,
    pub max_length: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cardinality {
    One,
    Many,
}
impl Cardinality {
    pub fn as_str(self) -> &'static str {
        match self {
            Cardinality::One => "one",
            Cardinality::Many => "many",
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReferenceRules {
    pub to: Vec<String>,
    pub cardinality: Cardinality,
    /// `Many` only.
    pub min_items: Option<u32>,
    /// `Many` only; 100 by default.
    pub max_items: u32,
    pub require_published: bool,
}
#[derive(Clone, Debug)]
pub struct AssetRules {
    /// MIME types or `type/*` patterns; empty accepts every type.
    pub accept: Vec<String>,
    pub cardinality: Cardinality,
    pub min_items: Option<u32>,
    pub max_items: u32,
}
#[derive(Clone, Debug)]
pub struct ListRules {
    /// A scalar or component item definition with an empty name.
    pub of: Box<Field>,
    pub min_items: Option<u32>,
    pub max_items: u32,
}
#[derive(Clone, Debug)]
pub struct BlocksRules {
    pub components: Vec<String>,
    pub min_items: Option<u32>,
    pub max_items: u32,
}
#[derive(Clone, Debug)]
pub struct JsonRules {
    pub max_bytes: u32,
}
