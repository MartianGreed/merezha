//! `merezha new` (scaffold §2): a new project from a template embedded in
//! the binary, written whole or not at all.
//!
//! [`Command::parse`] reads the arguments, [`prepare`] runs every check of
//! §2.3 and renders the project in memory (secrets included), and
//! [`Plan::write`] writes it through a 0700 staging directory renamed onto
//! the target. Nothing touches the network or runs a subprocess; the caller
//! supplies the revision the binary was built from and a [`SecretSource`].
//!
//! Templates live in `templates/<name>/` with a `template.toml` index
//! ([`catalog::Index`]); the build script embeds them (§2.7). Template files
//! are offered under `MIT OR MIT-0` (SC-Q9), and the renderer writes that
//! SPDX header into every generated file whose format has comments
//! ([`render::LICENSE`]).

pub mod blocks;
pub mod catalog;
pub mod pin;
pub mod render;
pub mod target;
mod write;

pub use blocks::{Block, Selection};
pub use catalog::{Catalog, SecretKind, Template};
pub use render::{Pin, Rendered, Values};
pub use target::Target;
pub use write::{SECRETS_MODE, STAGING_MODE};

use std::{
    ffi::OsString,
    fmt,
    io::Read,
    path::{Component, Path, PathBuf},
};

/// The template used without `--template`.
pub const DEFAULT_TEMPLATE: &str = "starter";
/// Longest application id: the stricter of `[app] id` (63) and a
/// Contremaitre name (40).
pub const MAX_NAME_BYTES: usize = 40;
/// Names `merezha new` refuses: `merezha` would share the Contremaitre
/// environments of the merezha repository.
pub const RESERVED_NAMES: [&str; 1] = ["merezha"];
/// The failed code when a [`SecretSource`] cannot make a key for another
/// reason than randomness (`random_unavailable`), such as encoding a VAPID
/// key. A report code of the command, never an HTTP error.
pub const SECRET_GENERATION_FAILED: &str = "secret_generation_failed";
/// Bound on the arguments after `new`.
const MAX_ARGUMENTS: usize = 64;
/// Largest value a [`SecretSource`] may return.
const MAX_SECRET_BYTES: usize = 1024;

pub const USAGE: &str = "merezha new NAME [--dir PATH] [--template NAME] [--blocks LIST] \
     [--merezha-rev REV | --merezha-path PATH] [--no-secrets]";

/// Why `merezha new` stopped. `Invalid` input exits 2 and nothing was
/// written; a `Failed` write exits 1 and left nothing behind.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error {
    pub kind: ErrorKind,
    pub code: &'static str,
    /// Detail for template defects; never a value from the environment.
    pub reason: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    Invalid,
    Failed,
}

impl Error {
    pub fn invalid(code: &'static str) -> Self {
        Self {
            kind: ErrorKind::Invalid,
            code,
            reason: None,
        }
    }

    pub fn failed(code: &'static str) -> Self {
        Self {
            kind: ErrorKind::Failed,
            code,
            reason: None,
        }
    }

    pub fn with_reason(mut self, reason: String) -> Self {
        self.reason = Some(reason);
        self
    }

    /// The report's `status`.
    pub fn status(&self) -> &'static str {
        match self.kind {
            ErrorKind::Invalid => "invalid",
            ErrorKind::Failed => "failed",
        }
    }

    pub fn exit_code(&self) -> u8 {
        match self.kind {
            ErrorKind::Invalid => 2,
            ErrorKind::Failed => 1,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.code)?;
        if let Some(reason) = &self.reason {
            write!(f, ": {reason}")?;
        }
        Ok(())
    }
}

impl std::error::Error for Error {}

/// What the arguments ask for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    /// `--help`: print [`help`] and exit 0.
    Help,
    New(Request),
}

/// The arguments of `merezha new`, before any check but their shape.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Request {
    pub name: String,
    pub dir: Option<PathBuf>,
    pub template: Option<String>,
    pub blocks: Option<String>,
    pub merezha_rev: Option<String>,
    pub merezha_path: Option<PathBuf>,
    pub no_secrets: bool,
}

impl Command {
    /// Parses the arguments after `new`. An unknown flag, a repeated flag,
    /// a missing value, a second `NAME` or a missing one is
    /// `invalid_arguments`; `--help` anywhere wins.
    pub fn parse(args: impl IntoIterator<Item = OsString>) -> Result<Self, Error> {
        let invalid = || Error::invalid("invalid_arguments");
        let args: Vec<OsString> = args.into_iter().take(MAX_ARGUMENTS + 1).collect();
        if args.len() > MAX_ARGUMENTS {
            return Err(invalid());
        }
        if args.iter().any(|a| a == "--help" || a == "-h") {
            return Ok(Command::Help);
        }
        let text = |value: OsString| value.into_string().map_err(|_| invalid());
        let mut request = Request::default();
        let mut name = None;
        let mut args = args.into_iter();
        while let Some(argument) = args.next() {
            let flag = argument.to_str().unwrap_or_default();
            if flag == "--no-secrets" {
                if request.no_secrets {
                    return Err(invalid());
                }
                request.no_secrets = true;
                continue;
            }
            if !flag.starts_with('-') {
                if name.replace(text(argument)?).is_some() {
                    return Err(invalid());
                }
                continue;
            }
            let value = args.next().ok_or_else(invalid)?;
            let fresh = match flag {
                "--dir" => request.dir.replace(PathBuf::from(value)).is_none(),
                "--merezha-path" => request.merezha_path.replace(PathBuf::from(value)).is_none(),
                "--template" => request.template.replace(text(value)?).is_none(),
                "--blocks" => request.blocks.replace(text(value)?).is_none(),
                "--merezha-rev" => request.merezha_rev.replace(text(value)?).is_none(),
                _ => false,
            };
            if !fresh {
                return Err(invalid());
            }
        }
        request.name = name.ok_or_else(invalid)?;
        Ok(Command::New(request))
    }
}

/// `merezha new --help`: the synopsis, the templates and the blocks.
pub fn help(catalog: &Catalog) -> String {
    let mut out = format!("usage: {USAGE}\n\ntemplates:\n");
    for template in catalog.templates() {
        let index = &template.index;
        let default = if index.name == DEFAULT_TEMPLATE {
            " (default)"
        } else {
            ""
        };
        let blocks = if index.fixed {
            format!("blocks fixed: {}", index.blocks.join(","))
        } else {
            format!("--blocks from {} or none", index.blocks.join(","))
        };
        out.push_str(&format!(
            "  {}{default}  {}; {blocks}\n",
            index.name, index.description
        ));
    }
    out.push_str(
        "\nblocks: identity needs notifications, content needs identity; telemetry stands alone.\n\
         The target must not exist or must be an empty directory. Without --merezha-rev or\n\
         --merezha-path the project pins the revision this binary was built from.\n",
    );
    out
}

/// Whether `name` is a valid application id: `^[a-z]([a-z0-9-]{0,38}[a-z0-9])?$`
/// and not reserved.
pub fn valid_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    let (Some(first), Some(last)) = (bytes.first(), bytes.last()) else {
        return false;
    };
    bytes.len() <= MAX_NAME_BYTES
        && first.is_ascii_lowercase()
        && (last.is_ascii_lowercase() || last.is_ascii_digit())
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
        && !RESERVED_NAMES.contains(&name)
}

/// Makes the development secrets of `.env`.
pub trait SecretSource {
    /// A new value of `kind`; fails without returning a predictable one.
    fn generate(&mut self, kind: SecretKind) -> Result<String, Error>;
}

/// What [`prepare`] needs from the running binary.
#[derive(Clone, Copy, Debug)]
pub struct Context<'a> {
    pub catalog: &'a Catalog,
    /// Relative `--dir` and `--merezha-path` resolve against it.
    pub cwd: &'a Path,
    /// The revision the binary was built from, when known (§2.9).
    pub built_from: Option<&'a str>,
    /// `%%merezha_version%%`.
    pub merezha_version: &'a str,
}

/// A checked and rendered project, ready to be written. `Debug` shows no
/// file contents, so no secret.
pub struct Plan {
    target: Target,
    staging: String,
    entries: Vec<write::Entry>,
    template: String,
    selection: Selection,
    pin: Pin,
    secrets: Vec<String>,
    next: Vec<String>,
}

impl fmt::Debug for Plan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Plan")
            .field("target", &self.target)
            .field("template", &self.template)
            .field("blocks", &self.selection.names())
            .field("pin", &self.pin)
            .field("secrets", &self.secrets)
            .field("files", &self.entries.len())
            .finish()
    }
}

/// The project `merezha new` wrote.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Created {
    pub dir: PathBuf,
    pub template: String,
    pub blocks: Vec<&'static str>,
    pub pin: Pin,
    /// The variables written to `.env`, never their values.
    pub secrets: Vec<String>,
    pub files: usize,
    pub next: Vec<String>,
}

/// Runs every check of §2.3, in the order of its table, and renders the
/// project. Nothing is written.
pub fn prepare(
    request: &Request,
    context: &Context<'_>,
    secrets: &mut dyn SecretSource,
) -> Result<Plan, Error> {
    if !valid_name(&request.name) {
        return Err(Error::invalid("invalid_name"));
    }
    let template_name = request.template.as_deref().unwrap_or(DEFAULT_TEMPLATE);
    let template = context
        .catalog
        .get(template_name)
        .ok_or_else(|| Error::invalid("unknown_template"))?;
    let known = template.blocks()?;
    let selection = match (&request.blocks, template.index.fixed) {
        (Some(_), true) => return Err(Error::invalid("blocks_fixed_by_template")),
        (None, _) => known.iter().copied().collect::<Selection>().checked()?,
        (Some(list), false) => {
            let selection = Selection::parse(list)?;
            if selection.iter().any(|block| !known.contains(&block)) {
                return Err(Error::invalid("unknown_block"));
            }
            selection
        }
    };
    if request.merezha_rev.is_some() && request.merezha_path.is_some() {
        return Err(Error::invalid("conflicting_source"));
    }
    let checkout = match &request.merezha_path {
        Some(path) => Some(merezha_checkout(&context.cwd.join(path))?),
        None => None,
    };
    let rev = match checkout {
        Some(_) => None,
        None => Some(
            pin::select_rev(request.merezha_rev.as_deref(), context.built_from)
                .map_err(Error::invalid)?,
        ),
    };
    let dir = request
        .dir
        .clone()
        .unwrap_or_else(|| PathBuf::from(&request.name));
    let target = target::resolve(&dir, context.cwd)?;
    let pin = match (rev, checkout) {
        (Some(rev), _) => Pin::Rev(rev),
        (None, Some(checkout)) => Pin::Path(
            relative(&target.dir, &checkout)
                .ok_or_else(|| Error::invalid("invalid_merezha_path"))?,
        ),
        (None, None) => return Err(Error::invalid("source_rev_unknown")),
    };

    let values = Values {
        app_id: request.name.clone(),
        pin: pin.clone(),
        merezha_version: context.merezha_version.to_owned(),
    };
    let mut entries: Vec<write::Entry> = render::render(template, &selection, &values)?
        .into_iter()
        .map(|file| write::Entry {
            file,
            secret: false,
        })
        .collect();
    entries.push(write::Entry {
        file: Rendered {
            path: ".merezha-version".to_owned(),
            contents: pin.version_file().into_bytes(),
            mode: 0o644,
        },
        secret: false,
    });
    let mut names = Vec::new();
    if !request.no_secrets {
        let mut env = String::from(
            "# Development secrets generated by `merezha new`. Mode 0600 and listed in\n\
             # .gitignore: never commit this file. .env.example lists every variable.\n",
        );
        for entry in &template.index.secrets {
            let wanted = entry.when.iter().try_fold(true, |keep, block| {
                Block::parse(block)
                    .map(|block| keep && selection.contains(block))
                    .ok_or_else(|| catalog::index_error(template_name, block))
            })?;
            if !wanted {
                continue;
            }
            let value = secrets.generate(entry.kind)?;
            if !shell_safe(&value) {
                return Err(Error::failed("secret_invalid"));
            }
            env.push_str(&format!("{}={value}\n", entry.name));
            names.push(entry.name.clone());
        }
        // Last: after the `.gitignore` that lists it. A selection without
        // secrets gets no `.env`.
        if !names.is_empty() {
            entries.push(write::Entry {
                file: Rendered {
                    path: ".env".to_owned(),
                    contents: env.into_bytes(),
                    mode: SECRETS_MODE,
                },
                secret: true,
            });
        }
    }
    Ok(Plan {
        staging: write::staging_name(&request.name)?,
        target,
        entries,
        template: template_name.to_owned(),
        selection,
        pin,
        secrets: names,
        next: template.index.next.clone(),
    })
}

impl Plan {
    /// The checked target directory.
    pub fn target(&self) -> &Path {
        &self.target.dir
    }

    /// The paths that will be written, in order.
    pub fn paths(&self) -> impl Iterator<Item = &str> {
        self.entries.iter().map(|entry| entry.file.path.as_str())
    }

    /// Writes the project (§2.4): `write_failed` or `rename_failed` leave
    /// nothing behind.
    pub fn write(self) -> Result<Created, Error> {
        write::write(
            &self.target.parent,
            &self.staging,
            &self.target.dir,
            &self.entries,
        )?;
        let dir = self.target.dir;
        let mut next = vec![format!("cd {}", dir.display())];
        next.extend(self.next);
        Ok(Created {
            blocks: self.selection.names(),
            template: self.template,
            pin: self.pin,
            secrets: self.secrets,
            files: self.entries.len(),
            next,
            dir,
        })
    }
}

/// Hex or base64 (standard or URL-safe) only, so `. ./.env` is safe.
fn shell_safe(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_SECRET_BYTES
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'/' | b'=' | b'-' | b'_'))
}

/// The canonical merezha checkout at `path`: it must hold
/// `crates/domain/Cargo.toml` naming the package `merezha-domain`
/// (`invalid_merezha_path`).
fn merezha_checkout(path: &Path) -> Result<PathBuf, Error> {
    let invalid = || Error::invalid("invalid_merezha_path");
    let checkout = std::fs::canonicalize(path).map_err(|_| invalid())?;
    let manifest =
        std::fs::File::open(checkout.join("crates/domain/Cargo.toml")).map_err(|_| invalid())?;
    if !manifest.metadata().map_err(|_| invalid())?.is_file() {
        return Err(invalid());
    }
    let mut text = String::new();
    manifest
        .take(64 * 1024)
        .read_to_string(&mut text)
        .map_err(|_| invalid())?;
    let manifest: toml::Table = toml::from_str(&text).map_err(|_| invalid())?;
    let name = manifest
        .get("package")
        .and_then(|package| package.get("name"))
        .and_then(|name| name.as_str());
    if name != Some("merezha-domain") {
        return Err(invalid());
    }
    Ok(checkout)
}

/// `to` relative to `from`, both absolute and free of `.` and `..`, as a
/// `/`-separated UTF-8 path (`.` when equal); `None` when a component is not
/// UTF-8.
fn relative(from: &Path, to: &Path) -> Option<String> {
    let parts = |path: &Path| -> Option<Vec<String>> {
        path.components()
            .filter_map(|component| match component {
                Component::Normal(part) => Some(part.to_str().map(str::to_owned)),
                _ => None,
            })
            .collect()
    };
    let from = parts(from)?;
    let to = parts(to)?;
    let common = from.iter().zip(&to).take_while(|(a, b)| a == b).count();
    let mut out: Vec<&str> = vec![".."; from.len() - common];
    out.extend(to[common..].iter().map(String::as_str));
    Some(if out.is_empty() {
        ".".to_owned()
    } else {
        out.join("/")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relative_paths() {
        let cases = [
            ("/a/b/project", "/a/b", ".."),
            ("/a/b/project", "/a/b/project", "."),
            ("/a/b/examples/teamboard", "/a/b", "../.."),
            ("/x/project", "/a/merezha", "../../a/merezha"),
            ("/a/project", "/a/project/inner", "inner"),
        ];
        for (from, to, expected) in cases {
            assert_eq!(
                relative(Path::new(from), Path::new(to)).as_deref(),
                Some(expected),
                "{from} -> {to}"
            );
        }
    }

    #[test]
    fn names() {
        for good in ["a", "acme", "a1", "my-app", &"a".repeat(40)] {
            assert!(valid_name(good), "{good}");
        }
        for bad in [
            "",
            "1a",
            "-a",
            "a-",
            "Acme",
            "a_b",
            "a.b",
            "merezha",
            "é",
            &"a".repeat(41),
        ] {
            assert!(!valid_name(bad), "{bad}");
        }
    }

    #[test]
    fn shell_safe_values() {
        assert!(shell_safe("0123abcdef"));
        assert!(shell_safe("q83v+/Q==") && shell_safe("abc-_"));
        for bad in ["", "a b", "a;b", "$(x)", "a\nb", "'", "\""] {
            assert!(!shell_safe(bad), "{bad:?}");
        }
    }
}
