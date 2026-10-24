//! Model validation problems, reported all at once with their TOML path.
use merezha_domain::Failure;

/// Most problems one [`ModelError`] lists; the rest are counted.
pub const MAX_ISSUES: usize = 200;
/// The startup code of every model problem.
pub const MODEL_INVALID: &str = "content_model_invalid";

/// One problem in the model file: a TOML path such as
/// `types.article.fields[2].max_length`, a stable lower snake case code and
/// a message built from the rule, never from field values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelIssue {
    pub path: String,
    pub code: &'static str,
    pub message: String,
}
impl std::fmt::Display for ModelIssue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.path.is_empty() {
            write!(f, "{MODEL_INVALID}: {}", self.message)
        } else {
            write!(f, "{MODEL_INVALID}: {} {}", self.path, self.message)
        }
    }
}

/// Every problem the model has, in file order of the sections checked.
/// Displays one `content_model_invalid: <path> <message>` line per problem.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelError {
    pub issues: Vec<ModelIssue>,
    /// Problems found beyond [`MAX_ISSUES`].
    pub omitted: usize,
}
impl ModelError {
    /// Permanent `content_model_invalid`.
    pub fn failure(&self) -> Failure {
        Failure::permanent(MODEL_INVALID)
    }
    /// `(path, code)` pairs, for tests and tooling.
    pub fn codes(&self) -> Vec<(&str, &str)> {
        self.issues
            .iter()
            .map(|i| (i.path.as_str(), i.code))
            .collect()
    }
}
impl std::fmt::Display for ModelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, issue) in self.issues.iter().enumerate() {
            if index > 0 {
                f.write_str("\n")?;
            }
            write!(f, "{issue}")?;
        }
        if self.omitted > 0 {
            write!(
                f,
                "\n{MODEL_INVALID}: {} more problems not shown",
                self.omitted
            )?;
        }
        Ok(())
    }
}
impl std::error::Error for ModelError {}

#[derive(Default)]
pub(crate) struct Issues {
    list: Vec<ModelIssue>,
    omitted: usize,
}
impl Issues {
    pub(crate) fn push(
        &mut self,
        path: impl Into<String>,
        code: &'static str,
        message: impl Into<String>,
    ) {
        if self.list.len() < MAX_ISSUES {
            self.list.push(ModelIssue {
                path: path.into(),
                code,
                message: message.into(),
            });
        } else {
            self.omitted += 1;
        }
    }
    pub(crate) fn count(&self) -> usize {
        self.list.len() + self.omitted
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.list.is_empty() && self.omitted == 0
    }
    pub(crate) fn into_error(self) -> ModelError {
        ModelError {
            issues: self.list,
            omitted: self.omitted,
        }
    }
}

/// `base.key` with `key` quoted when it is not a plain identifier, so a
/// hostile key cannot forge another path; long keys are cut.
pub(crate) fn join(base: &str, key: &str) -> String {
    let plain = !key.is_empty()
        && key.len() <= 64
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-');
    let segment = if plain {
        key.to_owned()
    } else {
        let mut quoted = String::from("\"");
        for (count, c) in key.chars().enumerate() {
            if count == 64 {
                quoted.push_str("...");
                break;
            }
            match c {
                '"' => quoted.push_str("\\\""),
                '\\' => quoted.push_str("\\\\"),
                c if c.is_control() => quoted.push_str(&format!("\\u{{{:x}}}", c as u32)),
                c => quoted.push(c),
            }
        }
        quoted.push('"');
        quoted
    };
    if base.is_empty() {
        segment
    } else {
        format!("{base}.{segment}")
    }
}
