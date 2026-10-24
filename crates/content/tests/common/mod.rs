//! Shared helpers for the content model tests.
#![allow(dead_code)]
use merezha_content::{
    ContentModel, KeySource, ModelError, ModelOptions, Stage, ValidateOptions, ValueError,
};
use serde_json::Value;
use std::path::Path;

/// Permissions an application might declare besides the content ones.
pub const CATALOG: [&str; 2] = ["partners.read", "tasks.read"];

pub fn options(media: bool) -> ModelOptions<'static> {
    ModelOptions {
        permissions: &CATALOG,
        media,
        organizations: false,
    }
}

pub fn parse(source: &str) -> Result<ContentModel, ModelError> {
    ContentModel::parse(source, &options(true))
}

/// `(path, code)` of every issue, or a panic when the model is accepted.
pub fn issues(source: &str) -> Vec<(String, String)> {
    match parse(source) {
        Ok(_) => panic!("model accepted:\n{source}"),
        Err(error) => error
            .issues
            .iter()
            .map(|i| (i.path.clone(), i.code.to_owned()))
            .collect(),
    }
}

/// A model with two locales, a `quote` component and an `other` type, plus
/// `rest` (more TOML tables).
pub fn document(rest: &str) -> String {
    format!(
        r#"version = 1
locales = [{{ code = "en", default = true }}, {{ code = "fr" }}]

[components.quote]
fields = [
  {{ name = "text", type = "text", required = true }},
  {{ name = "by", type = "string" }},
]

[types.other]
access = "public"
fields = [{{ name = "title", type = "string" }}]

{rest}"#
    )
}

/// A model whose type `t` has exactly the given field definitions.
pub fn with_fields(fields: &str) -> String {
    document(&format!(
        "[types.t]\naccess = \"public\"\nfields = [\n{fields}\n]\n"
    ))
}

/// Keys `g1`, `g2`, ... in order.
pub struct Counter(pub usize);
impl KeySource for Counter {
    fn next_key(&mut self) -> Result<String, merezha_domain::Failure> {
        self.0 += 1;
        Ok(format!("g{}", self.0))
    }
}

pub const DRAFT: ValidateOptions = ValidateOptions {
    stage: Stage::Draft,
    default_variant: true,
    new_variant: false,
};
pub const PUBLISH: ValidateOptions = ValidateOptions {
    stage: Stage::Publish,
    default_variant: true,
    new_variant: false,
};

/// Validate `fields` for type `t`; errors as `(path, code)` pairs.
pub fn validate(
    model: &ContentModel,
    fields: &Value,
    options: &ValidateOptions,
) -> Result<serde_json::Map<String, Value>, Vec<(String, String)>> {
    model
        .validate_fields("t", fields, options, &mut Counter(0))
        .map(|v| v.fields)
        .map_err(|e| match e {
            ValueError::Invalid(details) => details.into_iter().map(|d| (d.path, d.code)).collect(),
            ValueError::Failed(f) => vec![(String::new(), f.code)],
        })
}

pub fn pairs(list: &[(&str, &str)]) -> Vec<(String, String)> {
    list.iter()
        .map(|(p, c)| ((*p).to_owned(), (*c).to_owned()))
        .collect()
}

/// The fenced block of `language` that follows `heading` in a design doc.
pub fn doc_block(doc: &str, heading: &str, language: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/design")
        .join(doc);
    let text = std::fs::read_to_string(&path).unwrap();
    let after = &text[text
        .find(heading)
        .unwrap_or_else(|| panic!("{heading} in {doc}"))..];
    let fence = format!("```{language}\n");
    let start = after.find(&fence).unwrap() + fence.len();
    let end = after[start..].find("```").unwrap();
    after[start..start + end].to_owned()
}
