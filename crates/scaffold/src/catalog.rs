//! The embedded templates and their `template.toml` indexes (scaffold §2.8).
use crate::{Block, Error};
use serde::Deserialize;
use std::collections::BTreeMap;

mod embedded {
    include!(concat!(env!("OUT_DIR"), "/templates.rs"));
}

/// Every embedded file as `(path under templates/, bytes)`, sorted by path.
pub fn embedded_files() -> &'static [(&'static str, &'static [u8])] {
    embedded::FILES
}

/// Largest `template.toml`.
const MAX_INDEX_BYTES: usize = 64 * 1024;

/// A template's `template.toml`.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Index {
    pub name: String,
    pub description: String,
    /// The blocks the template knows. With `fixed`, the project always gets
    /// all of them and `--blocks` is refused (`blocks_fixed_by_template`).
    pub blocks: Vec<String>,
    #[serde(default)]
    pub fixed: bool,
    /// Parts of the template switched on or off as a whole, by name: a
    /// `when` or `unless` may name one as it names a block. The starter's
    /// `push` and `webhooks` (SC-Q7) are such switches.
    #[serde(default)]
    pub features: BTreeMap<String, bool>,
    /// Commands the report suggests after `cd` into the project.
    #[serde(default)]
    pub next: Vec<String>,
    #[serde(default, rename = "file")]
    pub files: Vec<FileEntry>,
    #[serde(default, rename = "tree")]
    pub trees: Vec<TreeEntry>,
    #[serde(default, rename = "secret")]
    pub secrets: Vec<SecretEntry>,
}

/// One generated file: a `source` rendered under `path`, or the
/// concatenation of the `fragments` whose block is selected.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileEntry {
    pub path: String,
    pub source: Option<String>,
    pub fragments: Option<Vec<Fragment>>,
    #[serde(default)]
    pub when: Vec<String>,
    #[serde(default)]
    pub unless: Vec<String>,
}

/// One part of a `fragments` list: a path whose file name `NN-<block>.*`
/// says when it applies (`NN-kernel.*` always does), or a table whose
/// `when` and `unless` do.
#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
pub enum Fragment {
    Named(String),
    Conditional {
        source: String,
        #[serde(default)]
        when: Vec<String>,
        #[serde(default)]
        unless: Vec<String>,
    },
}

impl Fragment {
    pub fn source(&self) -> &str {
        match self {
            Fragment::Named(source) | Fragment::Conditional { source, .. } => source,
        }
    }
}

/// A directory copied only with (`when`) or without (`unless`) blocks.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TreeEntry {
    pub source: String,
    #[serde(default)]
    pub when: Vec<String>,
    #[serde(default)]
    pub unless: Vec<String>,
}

/// A development secret written to `.env` (SC-D6).
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecretEntry {
    /// The environment variable both configuration files reference.
    pub name: String,
    pub kind: SecretKind,
    #[serde(default)]
    pub when: Vec<String>,
}

/// How a secret is generated. Every kind is hex or base64, so sourcing
/// `.env` in a shell is safe.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecretKind {
    /// 256 random bits as lowercase hex (`merezha_platform::context::random_token`).
    Token,
    /// Base64 of 32 random bytes.
    Key,
    /// A VAPID private key: base64url of the P-256 scalar, as `merezha
    /// notifications vapid generate` prints it.
    VapidPrivateKey,
}

/// One template: its index and its files, by path relative to the
/// template directory.
#[derive(Clone, Debug)]
pub struct Template {
    pub index: Index,
    pub files: BTreeMap<String, &'static [u8]>,
}

impl Template {
    /// The blocks the index names, refusing an unknown one.
    pub fn blocks(&self) -> Result<Vec<Block>, Error> {
        self.index
            .blocks
            .iter()
            .map(|name| Block::parse(name).ok_or_else(|| index_error(&self.index.name, name)))
            .collect()
    }
}

impl Template {
    /// Whether every name in `when` holds and none in `unless` does, for
    /// `selection`. A name is a block the template knows or one of its
    /// features; anything else is a template defect.
    pub fn applies(
        &self,
        selection: &crate::Selection,
        when: &[String],
        unless: &[String],
    ) -> Result<bool, Error> {
        let known = self.blocks()?;
        let mut keep = true;
        for (names, wanted) in [(when, true), (unless, false)] {
            for name in names {
                let on = match (Block::parse(name), self.index.features.get(name)) {
                    (Some(block), None) if known.contains(&block) => selection.contains(block),
                    (None, Some(on)) => *on,
                    _ => return Err(index_error(&self.index.name, name)),
                };
                keep &= on == wanted;
            }
        }
        Ok(keep)
    }
}

/// The set of templates, by name.
#[derive(Clone, Debug)]
pub struct Catalog {
    templates: BTreeMap<String, Template>,
}

impl Catalog {
    /// The templates built into this binary.
    pub fn embedded() -> Result<Self, Error> {
        Self::from_files(embedded::FILES.iter().copied())
    }

    /// A catalog from `(path, bytes)` pairs, as the build script embeds
    /// them: `<template>/template.toml` and the template's files under
    /// `<template>/`.
    pub fn from_files(
        files: impl IntoIterator<Item = (&'static str, &'static [u8])>,
    ) -> Result<Self, Error> {
        let mut grouped: BTreeMap<&str, BTreeMap<String, &'static [u8]>> = BTreeMap::new();
        for (path, bytes) in files {
            let (template, relative) = path
                .split_once('/')
                .ok_or_else(|| index_error(path, "file outside a template"))?;
            grouped
                .entry(template)
                .or_default()
                .insert(relative.to_owned(), bytes);
        }
        let mut templates = BTreeMap::new();
        for (name, mut files) in grouped {
            let index = files
                .remove("template.toml")
                .ok_or_else(|| index_error(name, "template.toml missing"))?;
            if index.len() > MAX_INDEX_BYTES {
                return Err(index_error(name, "template.toml too large"));
            }
            let index: Index = std::str::from_utf8(index)
                .ok()
                .and_then(|text| toml::from_str(text).ok())
                .ok_or_else(|| index_error(name, "template.toml does not parse"))?;
            if index.name != name {
                return Err(index_error(name, "name differs from the directory"));
            }
            let template = Template { index, files };
            template.blocks()?;
            templates.insert(name.to_owned(), template);
        }
        Ok(Self { templates })
    }

    pub fn get(&self, name: &str) -> Option<&Template> {
        self.templates.get(name)
    }

    pub fn templates(&self) -> impl Iterator<Item = &Template> {
        self.templates.values()
    }
}

/// A defect in a template, which the render tests catch before release.
pub(crate) fn index_error(template: &str, what: &str) -> Error {
    Error::failed("template_invalid").with_reason(format!("{template}: {what}"))
}
