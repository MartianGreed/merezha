//! Rendering a template for a block selection (scaffold §2.8): plain files
//! are copied, `.tmpl` files get the closed set of `%%name%%` substitutions
//! and lose the suffix, and `[[file]]` entries with `fragments` concatenate
//! the fragments whose block is selected. There is no template engine.
use crate::{
    Block, Error, Selection,
    catalog::{Template, index_error},
};
use std::collections::{BTreeMap, BTreeSet};

/// Where the guest SDK comes from with a revision pin (SC-D8, SC-Q2).
pub const GIT_URL: &str = "https://github.com/MartianGreed/merezha";

/// The licence header the renderer writes at the top of every generated
/// file whose format has comments (SC-Q9's recommendation: everything
/// `merezha new` copies is offered as `MIT OR MIT-0`, so a generated
/// project owes no notice). Set to `None` to write files without it.
pub const LICENSE: Option<&str> = Some("SPDX-License-Identifier: MIT OR MIT-0");

/// Outputs the command writes itself, which no template may produce.
pub const RESERVED_OUTPUTS: [&str; 2] = [".env", ".merezha-version"];

/// The merezha source a project builds against (§2.9).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Pin {
    /// A full commit of [`GIT_URL`].
    Rev(String),
    /// A merezha checkout, as a `/`-separated path relative to the project.
    Path(String),
}

impl Pin {
    /// The `merezha-domain` line of the project's `Cargo.toml`, the value of
    /// `%%merezha_dependency%%`.
    pub fn dependency(&self) -> String {
        match self {
            Pin::Rev(rev) => format!(
                "merezha-domain = {{ git = {}, rev = {} }}",
                toml_string(GIT_URL),
                toml_string(rev)
            ),
            Pin::Path(path) => format!(
                "merezha-domain = {{ path = {} }}",
                toml_string(&join(path, "crates/domain"))
            ),
        }
    }

    /// The contents of `.merezha-version`.
    pub fn version_file(&self) -> String {
        match self {
            Pin::Rev(rev) => format!(
                "# The merezha revision this project builds against. scripts/dev.sh and CI install and check it.\n\
                 rev = {}\n",
                toml_string(rev)
            ),
            Pin::Path(path) => format!(
                "# The merezha checkout this project builds against, relative to this file.\n\
                 path = {}\n",
                toml_string(path)
            ),
        }
    }

    /// The revision, empty for a path pin.
    pub fn rev(&self) -> &str {
        match self {
            Pin::Rev(rev) => rev,
            Pin::Path(_) => "",
        }
    }
}

fn join(base: &str, rest: &str) -> String {
    if base == "." {
        rest.to_owned()
    } else {
        format!("{base}/{rest}")
    }
}

/// A TOML basic string.
pub(crate) fn toml_string(text: &str) -> String {
    toml::Value::String(text.to_owned()).to_string()
}

/// The values of the substitutions.
#[derive(Clone, Debug)]
pub struct Values {
    /// `%%app_id%%` and `%%app_name%%`.
    pub app_id: String,
    /// `%%merezha_dependency%%` and `%%merezha_rev%%` (empty with a path pin).
    pub pin: Pin,
    /// `%%merezha_version%%`: the version of the running `merezha`.
    pub merezha_version: String,
}

impl Values {
    fn lookup(&self, name: &str) -> Option<String> {
        Some(match name {
            "app_id" | "app_name" => self.app_id.clone(),
            "merezha_dependency" => self.pin.dependency(),
            "merezha_rev" => self.pin.rev().to_owned(),
            "merezha_version" => self.merezha_version.clone(),
            _ => return None,
        })
    }
}

/// Replaces every `%%name%%` in `text`. An unknown name is a template
/// defect (the build script refuses it before this can run).
pub fn substitute(text: &str, values: &Values) -> Result<String, Error> {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find("%%") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let end = after
            .find("%%")
            .ok_or_else(|| Error::failed("template_invalid"))?;
        let value = values
            .lookup(&after[..end])
            .ok_or_else(|| Error::failed("template_invalid"))?;
        out.push_str(&value);
        rest = &after[end + 2..];
    }
    out.push_str(rest);
    Ok(out)
}

/// A file ready to be written.
#[derive(Clone, PartialEq, Eq)]
pub struct Rendered {
    /// `/`-separated, relative to the project.
    pub path: String,
    pub contents: Vec<u8>,
    /// Requested mode; the umask applies.
    pub mode: u32,
}

impl std::fmt::Debug for Rendered {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Rendered")
            .field("path", &self.path)
            .field("bytes", &self.contents.len())
            .field("mode", &format_args!("{:o}", self.mode))
            .finish()
    }
}

/// Renders `template` for `selection`, sorted by path. Every path is
/// relative and unique, and none is one of [`RESERVED_OUTPUTS`].
pub fn render(
    template: &Template,
    selection: &Selection,
    values: &Values,
) -> Result<Vec<Rendered>, Error> {
    let name = template.index.name.as_str();
    let known: BTreeSet<Block> = template.blocks()?.into_iter().collect();
    let applies = |when: &[String], unless: &[String]| -> Result<bool, Error> {
        let mut keep = true;
        for (names, wanted) in [(when, true), (unless, false)] {
            for block in names {
                let block = Block::parse(block)
                    .filter(|block| known.contains(block))
                    .ok_or_else(|| index_error(name, block))?;
                keep &= selection.contains(block) == wanted;
            }
        }
        Ok(keep)
    };
    let source = |path: &str| -> Result<String, Error> {
        let bytes = template
            .files
            .get(path)
            .ok_or_else(|| index_error(name, &format!("{path} missing")))?;
        if path.ends_with(".tmpl") {
            let text = std::str::from_utf8(bytes).map_err(|_| index_error(name, path))?;
            substitute(text, values)
        } else {
            String::from_utf8(bytes.to_vec()).map_err(|_| index_error(name, path))
        }
    };

    let mut consumed = BTreeSet::new();
    let mut out: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    let mut insert = |path: String, contents: Vec<u8>| -> Result<(), Error> {
        if !valid_relative(&path) || RESERVED_OUTPUTS.contains(&path.as_str()) {
            return Err(index_error(name, &format!("output {path}")));
        }
        if out.insert(path.clone(), contents).is_some() {
            return Err(index_error(name, &format!("{path} written twice")));
        }
        Ok(())
    };

    for entry in &template.index.files {
        let parts: Vec<&String> = match (&entry.source, &entry.fragments) {
            (Some(source), None) => vec![source],
            (None, Some(fragments)) => fragments.iter().collect(),
            _ => return Err(index_error(name, &entry.path)),
        };
        consumed.extend(parts.iter().map(|part| part.as_str()));
        if !applies(&entry.when, &entry.unless)? {
            continue;
        }
        let mut text = String::new();
        for part in parts {
            if entry.fragments.is_some() {
                let owner = fragment_block(part).ok_or_else(|| index_error(name, part))?;
                if let Some(owner) = owner {
                    if !known.contains(&owner) {
                        return Err(index_error(name, part));
                    }
                    if !selection.contains(owner) {
                        continue;
                    }
                }
            }
            text.push_str(&source(part)?);
        }
        let contents = stamp(&entry.path, text);
        insert(entry.path.clone(), contents.into_bytes())?;
    }

    for path in template.files.keys() {
        if consumed.contains(path.as_str()) {
            continue;
        }
        let mut keep = true;
        for tree in &template.index.trees {
            let prefix = format!("{}/", tree.source.trim_end_matches('/'));
            if path.starts_with(&prefix) {
                keep &= applies(&tree.when, &tree.unless)?;
            }
        }
        if !keep {
            continue;
        }
        let output = path.strip_suffix(".tmpl").unwrap_or(path);
        let contents = if path.ends_with(".tmpl") || header_style(output).is_some() {
            stamp(output, source(path)?).into_bytes()
        } else {
            template.files[path].to_vec()
        };
        insert(output.to_owned(), contents)?;
    }

    Ok(out
        .into_iter()
        .map(|(path, contents)| Rendered {
            mode: mode_for(&path),
            path,
            contents,
        })
        .collect())
}

/// The block a fragment belongs to, from its file name `NN-<block>.*`:
/// `Some(None)` for `NN-kernel.*`, which always applies, `None` for a name
/// of another shape.
fn fragment_block(path: &str) -> Option<Option<Block>> {
    let file = path.rsplit('/').next()?;
    let (number, rest) = file.split_once('-')?;
    if number.is_empty() || !number.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let block = rest.split('.').next()?;
    if block == "kernel" {
        return Some(None);
    }
    Block::parse(block).map(Some)
}

/// The comment leader of a generated file's format, when it has one.
fn header_style(path: &str) -> Option<&'static str> {
    let file = path.rsplit('/').next().unwrap_or(path);
    let hash = [".toml", ".sh", ".yaml", ".yml"];
    if hash.iter().any(|suffix| file.ends_with(suffix))
        || matches!(file, ".gitignore" | ".env.example" | "Dockerfile")
    {
        Some("#")
    } else if file.ends_with(".rs") || file.ends_with(".wit") {
        Some("//")
    } else {
        None
    }
}

/// `text` with the [`LICENSE`] header, after a `#!` line if any.
fn stamp(path: &str, text: String) -> String {
    let (Some(licence), Some(leader)) = (LICENSE, header_style(path)) else {
        return text;
    };
    let header = format!("{leader} {licence}\n");
    match text.strip_prefix("#!") {
        Some(_) => match text.split_once('\n') {
            Some((shebang, rest)) => format!("{shebang}\n{header}{rest}"),
            None => format!("{text}\n{header}"),
        },
        None => format!("{header}{text}"),
    }
}

/// `0755` for scripts, `0644` for everything else (§2.4).
fn mode_for(path: &str) -> u32 {
    if path.starts_with("scripts/") {
        0o755
    } else {
        0o644
    }
}

/// A plain relative path: `/`-separated normal segments.
pub(crate) fn valid_relative(path: &str) -> bool {
    !path.is_empty()
        && path.split('/').all(|part| {
            !part.is_empty() && part != "." && part != ".." && !part.contains(['\\', '\0'])
        })
}
