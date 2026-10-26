//! The template registry (NT §4): project files under `templates.dir` over
//! the block's embedded `en` and `fr` defaults, compiled once at startup into
//! one immutable minijinja environment with strict undefined, auto-escaping
//! by extension, fuel and output limits.
//!
//! Layout, relative to `templates.dir`:
//!
//! ```text
//! _layouts/email.html            # {% block content %}; _partials/ for includes
//! tasks.assigned/
//!   template.toml                # description, required = [...], [preview]
//!   en/subject.txt  en/body.html  en/body.txt  en/sms.txt
//!   en/push.toml                 # title, body, url (template strings)
//!   en/inbox.toml                # title, body, link, [payload] (template strings)
//! ```
mod builtin;
pub mod filters;
pub mod sms;

use crate::{
    config::{Settings, segment, valid_locale},
    topics::{Category, valid_dotted_name},
};
use merezha_platform::{
    config::{ConfigIssue, Origin},
    ports::Channel,
};
use minijinja::{AutoEscape, Environment, ErrorKind, UndefinedBehavior, Value as Jinja};
use serde_json::{Map, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use uuid::Uuid;

pub use sms::{Encoding, segments};

/// Rendered HTML email body limit.
pub const MAX_HTML_BYTES: usize = 512 * 1024;
/// Rendered plain text limit (email text, SMS before segmenting).
pub const MAX_TEXT_BYTES: usize = 128 * 1024;
/// Rendered subject limit, in characters.
pub const MAX_SUBJECT_CHARS: usize = 255;
/// Web Push payload limit (NT §5.3); the body is truncated to fit at send.
pub const MAX_PUSH_PAYLOAD_BYTES: usize = 3000;
pub const MAX_INBOX_TITLE_CHARS: usize = 200;
pub const MAX_INBOX_BODY_CHARS: usize = 2000;
pub const MAX_LINK_BYTES: usize = 2048;
pub const MAX_INBOX_PAYLOAD_BYTES: usize = 4096;
/// Data keys the block sets itself (`reserved_data_key`).
pub const RESERVED_KEYS: [&str; 5] = ["recipient", "links", "app", "notification", "secret"];

/// Bounds on the project's template directory.
const MAX_FILE_BYTES: u64 = 512 * 1024;
const MAX_FILES: usize = 4096;
const MAX_TOTAL_BYTES: u64 = 32 * 1024 * 1024;
const MAX_PAYLOAD_KEYS: usize = 32;
/// Fixed id of preview renders.
pub const PREVIEW_NOTIFICATION_ID: Uuid =
    Uuid::from_u128(0x0192_0000_0000_7000_8000_0000_0000_0001);

/// The files of one locale directory.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum File {
    Subject,
    BodyHtml,
    BodyText,
    Sms,
    Push,
    Inbox,
}
impl File {
    fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "subject.txt" => File::Subject,
            "body.html" => File::BodyHtml,
            "body.txt" => File::BodyText,
            "sms.txt" => File::Sms,
            "push.toml" => File::Push,
            "inbox.toml" => File::Inbox,
            _ => return None,
        })
    }
    fn name(self) -> &'static str {
        match self {
            File::Subject => "subject.txt",
            File::BodyHtml => "body.html",
            File::BodyText => "body.txt",
            File::Sms => "sms.txt",
            File::Push => "push.toml",
            File::Inbox => "inbox.toml",
        }
    }
}

/// One template: its description, required data keys, canonical preview
/// data and the files present per locale.
#[derive(Clone, Debug)]
pub struct TemplateInfo {
    pub name: String,
    pub description: String,
    /// Data keys a request must supply.
    pub required: Vec<String>,
    /// `[preview]` without its `secret` table.
    pub preview: Map<String, Value>,
    /// `[preview.secret]`, rendered as `secret` in previews.
    pub preview_secret: Map<String, Value>,
    files: BTreeMap<String, BTreeSet<File>>,
    /// Inbox payload keys per locale.
    payload_keys: BTreeMap<String, Vec<String>>,
}
impl TemplateInfo {
    fn has(&self, locale: &str, channel: Channel) -> bool {
        let Some(files) = self.files.get(locale) else {
            return false;
        };
        match channel {
            Channel::Email => files.contains(&File::Subject) && files.contains(&File::BodyText),
            Channel::Sms => files.contains(&File::Sms),
            Channel::Push => files.contains(&File::Push),
            Channel::Inbox => files.contains(&File::Inbox),
        }
    }
    /// Locales with at least one file.
    pub fn locales(&self) -> Vec<String> {
        self.files.keys().cloned().collect()
    }
    /// Channels this template renders in `locale`, after fallback.
    pub fn channels(&self, registry: &TemplateRegistry, locale: &str) -> Vec<Channel> {
        [Channel::Email, Channel::Sms, Channel::Push, Channel::Inbox]
            .into_iter()
            .filter(|c| registry.file_locale(self, locale, *c).is_some())
            .collect()
    }
}

/// Settings the registry renders with.
#[derive(Clone, Debug)]
struct Rendering {
    default_locale: String,
    locales: Vec<String>,
    app_name: String,
    app_base_url: Option<String>,
    preferences_page: Option<String>,
    public_base_url: Option<String>,
    allow_http_links: bool,
    max_segments: u32,
    organisation_prefix: Option<String>,
}

/// Rendered email: subject, plain text and optional HTML.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenderedEmail {
    pub subject: String,
    pub text: String,
    pub html: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenderedSms {
    pub text: String,
    pub encoding: Encoding,
    pub segments: u32,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenderedPush {
    pub title: String,
    pub body: String,
    pub url: Option<String>,
}
#[derive(Clone, Debug, PartialEq)]
pub struct RenderedInbox {
    pub title: String,
    pub body: String,
    pub link: Option<String>,
    pub payload: Map<String, Value>,
}
/// One channel's rendered message. `Debug` of a rendered message holds
/// message content: never log it.
#[derive(Clone, Debug, PartialEq)]
pub enum Rendered {
    Email(RenderedEmail),
    Sms(RenderedSms),
    Push(RenderedPush),
    Inbox(RenderedInbox),
}
impl Rendered {
    /// The preview and capture shape (NT §7.1).
    pub fn to_json(&self) -> Value {
        match self {
            Rendered::Email(e) => {
                serde_json::json!({"subject": e.subject, "html": e.html, "text": e.text})
            }
            Rendered::Sms(s) => serde_json::json!({
                "text": s.text, "encoding": s.encoding.as_str(), "segments": s.segments
            }),
            Rendered::Push(p) => {
                serde_json::json!({"title": p.title, "body": p.body, "url": p.url})
            }
            Rendered::Inbox(i) => serde_json::json!({
                "title": i.title, "body": i.body, "link": i.link, "payload": i.payload
            }),
        }
    }
}

/// Why a render failed: the channel and a stable code. It never carries
/// template text, data or the engine's message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RenderError {
    pub channel: Channel,
    /// `template_missing`, `undefined_variable`, `invalid_url`,
    /// `out_of_fuel`, `output_too_large`, `control_character`,
    /// `empty_output`, `too_many_segments`, `invalid_datetime` or
    /// `render_error`.
    pub code: &'static str,
}

/// What one render needs besides the template.
#[derive(Clone, Copy, Debug)]
pub struct RenderContext<'a> {
    pub topic: &'a str,
    pub category: Category,
    pub notification_id: Uuid,
    /// Request data; must not use [`RESERVED_KEYS`].
    pub data: &'a Map<String, Value>,
    /// `secret_data`: rendered into email, SMS and push, never the inbox.
    pub secret: &'a Map<String, Value>,
    pub recipient_name: Option<&'a str>,
    /// IANA name for the `datetime` filter; UTC when unset or unknown.
    pub timezone: Option<&'a str>,
    /// The unsubscribe link, for non-security email only.
    pub unsubscribe: Option<&'a str>,
}

/// The compiled templates. Immutable after [`TemplateRegistry::load`].
pub struct TemplateRegistry {
    env: Environment<'static>,
    templates: BTreeMap<String, TemplateInfo>,
    rendering: Rendering,
}
impl std::fmt::Debug for TemplateRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TemplateRegistry")
            .field("templates", &self.templates.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

/// Collects template issues under `modules.notifications.templates`.
struct Issues(Vec<ConfigIssue>);
impl Issues {
    fn push(&mut self, suffix: &str, code: &'static str) {
        self.0.push(ConfigIssue::new(
            format!("modules.notifications.templates.{suffix}"),
            code,
            Origin::File,
        ));
    }
}

/// Read the project directory into `(relative path, text)` pairs, bounded.
fn read_dir(root: &Path, explicit: bool, issues: &mut Issues) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    if !root.is_dir() {
        if explicit {
            issues.push("dir", "invalid_path");
        }
        return out;
    }
    let mut total = 0u64;
    let mut stack = vec![(root.to_path_buf(), String::new(), 0usize)];
    while let Some((dir, prefix, depth)) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            issues.push(&segment_or_dir(&prefix), "template_unreadable");
            continue;
        };
        let mut entries: Vec<_> = entries.filter_map(Result::ok).collect();
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                issues.push(&segment_or_dir(&prefix), "template_unreadable");
                continue;
            };
            if name.starts_with('.') {
                continue;
            }
            let relative = if prefix.is_empty() {
                name.clone()
            } else {
                format!("{prefix}/{name}")
            };
            let Ok(meta) = std::fs::metadata(entry.path()) else {
                issues.push(&segment(&relative), "template_unreadable");
                continue;
            };
            if meta.is_dir() {
                if depth >= 3 {
                    issues.push(&segment(&relative), "template_too_deep");
                } else {
                    stack.push((entry.path(), relative, depth + 1));
                }
                continue;
            }
            if out.len() >= MAX_FILES {
                issues.push("dir", "too_many_items");
                return out;
            }
            total += meta.len();
            if meta.len() > MAX_FILE_BYTES || total > MAX_TOTAL_BYTES {
                issues.push(&segment(&relative), "template_too_large");
                continue;
            }
            match std::fs::read_to_string(entry.path()) {
                Ok(text) => {
                    out.insert(relative, text);
                }
                Err(_) => issues.push(&segment(&relative), "template_unreadable"),
            }
        }
    }
    out
}
fn segment_or_dir(prefix: &str) -> String {
    if prefix.is_empty() {
        "dir".into()
    } else {
        segment(prefix)
    }
}

/// `toml::Value` as JSON; datetimes become RFC 3339 strings.
pub(crate) fn toml_to_json(value: toml::Value) -> Value {
    match value {
        toml::Value::String(s) => Value::String(s),
        toml::Value::Integer(i) => Value::from(i),
        toml::Value::Float(f) => serde_json::Number::from_f64(f).map_or(Value::Null, Value::Number),
        toml::Value::Boolean(b) => Value::Bool(b),
        toml::Value::Datetime(d) => Value::String(d.to_string()),
        toml::Value::Array(items) => Value::Array(items.into_iter().map(toml_to_json).collect()),
        toml::Value::Table(table) => Value::Object(
            table
                .into_iter()
                .map(|(k, v)| (k, toml_to_json(v)))
                .collect(),
        ),
    }
}

/// JSON as a template value.
fn to_jinja(value: &Value) -> Jinja {
    match value {
        Value::Null => Jinja::from(()),
        Value::Bool(b) => Jinja::from(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Jinja::from(i)
            } else if let Some(u) = n.as_u64() {
                Jinja::from(u)
            } else {
                Jinja::from(n.as_f64().unwrap_or(0.0))
            }
        }
        Value::String(s) => Jinja::from(s.as_str()),
        Value::Array(items) => Jinja::from(items.iter().map(to_jinja).collect::<Vec<_>>()),
        Value::Object(map) => Jinja::from(object(map)),
    }
}
fn object(map: &Map<String, Value>) -> BTreeMap<String, Jinja> {
    map.iter().map(|(k, v)| (k.clone(), to_jinja(v))).collect()
}
/// A template map from fixed keys.
fn map<const N: usize>(pairs: [(&str, Jinja); N]) -> Jinja {
    Jinja::from(
        pairs
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v))
            .collect::<BTreeMap<String, Jinja>>(),
    )
}
fn optional(value: Option<&str>) -> Jinja {
    value.map_or_else(|| Jinja::from(()), Jinja::from)
}

/// Engine names: `<template>/<locale>/<file>` and, for the TOML files,
/// `<template>/<locale>/<file>#<field>`.
fn engine_name(template: &str, locale: &str, file: File, field: Option<&str>) -> String {
    match field {
        Some(field) => format!("{template}/{locale}/{}#{field}", file.name()),
        None => format!("{template}/{locale}/{}", file.name()),
    }
}

impl TemplateRegistry {
    /// Load the embedded defaults and the project's files, compile them and
    /// run every startup check of NT §4 and §7.2 against `settings`.
    /// Every problem is returned at once.
    pub fn load(settings: &Settings) -> Result<Self, Vec<ConfigIssue>> {
        let mut issues = Issues(Vec::new());
        let listed: BTreeSet<&str> = settings
            .templates
            .locales
            .iter()
            .map(String::as_str)
            .collect();
        // Embedded defaults first; a project file with the same path wins.
        let mut sources: BTreeMap<String, (String, bool)> = BTreeMap::new();
        for (path, text) in builtin::FILES {
            let locale = path.split('/').nth(1).unwrap_or_default();
            if path.ends_with("template.toml") || path.starts_with('_') || listed.contains(locale) {
                sources.insert((*path).to_owned(), ((*text).to_owned(), true));
            }
        }
        for (path, text) in read_dir(
            &settings.templates.dir,
            settings.templates.dir_explicit,
            &mut issues,
        ) {
            sources.insert(path, (text, false));
        }

        let mut env = Environment::new();
        env.set_undefined_behavior(UndefinedBehavior::Strict);
        env.set_fuel(Some(settings.templates.fuel));
        env.set_auto_escape_callback(|name| {
            if name.ends_with(".html") {
                AutoEscape::Html
            } else {
                AutoEscape::None
            }
        });
        filters::register(&mut env, settings.templates.allow_http_links);

        let mut templates: BTreeMap<String, TemplateInfo> = BTreeMap::new();
        let mut inbox_fields: Vec<(String, String)> = Vec::new();
        for (path, (text, builtin)) in sources {
            let parts: Vec<&str> = path.split('/').collect();
            let key = segment(&path);
            if parts[0] == "_layouts" || parts[0] == "_partials" {
                if parts.len() < 2 {
                    continue;
                }
                if let Err(error) = env.add_template_owned(path.clone(), text) {
                    issues.push(&key, syntax_code(&error));
                }
                continue;
            }
            let name = parts[0];
            if !valid_dotted_name(name) {
                issues.push(&segment(name), "invalid_template_name");
                continue;
            }
            let info = templates
                .entry(name.to_owned())
                .or_insert_with(|| TemplateInfo {
                    name: name.to_owned(),
                    description: String::new(),
                    required: Vec::new(),
                    preview: Map::new(),
                    preview_secret: Map::new(),
                    files: BTreeMap::new(),
                    payload_keys: BTreeMap::new(),
                });
            match parts.as_slice() {
                [_, "template.toml"] => {
                    if let Err(code) = parse_template_toml(info, &text) {
                        issues.push(&key, code);
                    }
                }
                [_, locale, file] => {
                    if !valid_locale(locale) || !listed.contains(locale) {
                        if !builtin {
                            issues.push(&key, "unknown_locale");
                        }
                        continue;
                    }
                    let Some(kind) = File::parse(file) else {
                        issues.push(&key, "unknown_template_file");
                        continue;
                    };
                    let added = match kind {
                        File::Push | File::Inbox => add_toml_fields(
                            &mut env,
                            info,
                            name,
                            locale,
                            kind,
                            &text,
                            &mut inbox_fields,
                        ),
                        _ => env
                            .add_template_owned(engine_name(name, locale, kind, None), text)
                            .map_err(|e| syntax_code(&e)),
                    };
                    match added {
                        Ok(()) => {
                            info.files
                                .entry((*locale).to_owned())
                                .or_default()
                                .insert(kind);
                        }
                        Err(code) => issues.push(&key, code),
                    }
                }
                _ => issues.push(&key, "unknown_template_file"),
            }
        }

        // An inbox template never sees `secret` (R2).
        for (engine, key) in &inbox_fields {
            if let Ok(template) = env.get_template(engine)
                && template.undeclared_variables(false).contains("secret")
            {
                issues.push(key, "inbox_references_secret");
            }
        }
        // Email needs its text part and a subject.
        for info in templates.values() {
            for (locale, files) in &info.files {
                let base = format!("{}/{locale}", info.name);
                if files.contains(&File::BodyHtml) && !files.contains(&File::BodyText) {
                    issues.push(
                        &segment(&format!("{base}/body.html")),
                        "template_text_missing",
                    );
                }
                if files.contains(&File::Subject) != files.contains(&File::BodyText) {
                    let missing = if files.contains(&File::Subject) {
                        "body.txt"
                    } else {
                        "subject.txt"
                    };
                    issues.push(
                        &segment(&format!("{base}/{missing}")),
                        "template_file_missing",
                    );
                }
            }
        }

        let registry = Self {
            env,
            templates,
            rendering: Rendering {
                default_locale: settings.templates.default_locale.clone(),
                locales: settings.templates.locales.clone(),
                app_name: settings.templates.app_name.clone(),
                app_base_url: settings.templates.app_base_url.clone(),
                preferences_page: settings.links.preferences_page.clone(),
                public_base_url: settings.public_base_url.clone(),
                allow_http_links: settings.templates.allow_http_links,
                max_segments: settings.sms.max_segments,
                organisation_prefix: settings.sms.organisation_prefix.clone(),
            },
        };

        // Every topic channel has its template in the default locale.
        let default_locale = &registry.rendering.default_locale;
        for (name, topic) in &settings.topics {
            let key = format!("modules.notifications.topics.{}.template", segment(name));
            let Some(info) = registry.templates.get(&topic.template) else {
                issues
                    .0
                    .push(ConfigIssue::new(key, "template_missing", Origin::File));
                continue;
            };
            if topic.channels.iter().any(|c| !info.has(default_locale, *c)) {
                issues
                    .0
                    .push(ConfigIssue::new(key, "template_missing", Origin::File));
            }
        }
        // Every preview renders in every locale under strict mode.
        for info in registry.templates.values() {
            let base = segment(&info.name);
            if info.files.is_empty() {
                issues.push(&base, "template_file_missing");
                continue;
            }
            if info.required.iter().any(|k| !info.preview.contains_key(k)) {
                issues.push(&format!("{base}.preview"), "preview_missing_required");
            }
            let topic = settings.topics.values().find(|t| t.template == info.name);
            for locale in &registry.rendering.locales {
                for channel in info.channels(&registry, locale) {
                    let unsubscribe = registry.preview_unsubscribe(
                        topic.map_or(Category::Transactional, |t| t.category),
                        channel,
                    );
                    let context = RenderContext {
                        topic: topic.map_or(info.name.as_str(), |t| t.name.as_str()),
                        category: topic.map_or(Category::Transactional, |t| t.category),
                        notification_id: PREVIEW_NOTIFICATION_ID,
                        data: &info.preview,
                        secret: &info.preview_secret,
                        recipient_name: Some("Ada Lovelace"),
                        timezone: Some("UTC"),
                        unsubscribe: unsubscribe.as_deref(),
                    };
                    if let Err(error) = registry.render(&info.name, channel, locale, &context) {
                        issues.push(
                            &format!("{base}.preview.{locale}.{}", channel.as_str()),
                            error.code,
                        );
                    }
                }
            }
        }
        if issues.0.is_empty() {
            Ok(registry)
        } else {
            Err(issues.0)
        }
    }

    /// Every template, by name.
    pub fn templates(&self) -> impl Iterator<Item = &TemplateInfo> {
        self.templates.values()
    }
    pub fn get(&self, name: &str) -> Option<&TemplateInfo> {
        self.templates.get(name)
    }
    pub fn default_locale(&self) -> &str {
        &self.rendering.default_locale
    }
    pub fn locales(&self) -> &[String] {
        &self.rendering.locales
    }

    /// The first candidate in the closed locale list, each candidate tried
    /// as is and then by its language (`fr-CA`, then `fr`), else the
    /// default locale (NT §4).
    pub fn resolve_locale<'a>(&self, candidates: impl IntoIterator<Item = &'a str>) -> String {
        let listed = |locale: &str| self.rendering.locales.iter().any(|l| l == locale);
        for candidate in candidates {
            if listed(candidate) {
                return candidate.to_owned();
            }
            if let Some((language, _)) = candidate.split_once('-')
                && listed(language)
            {
                return language.to_owned();
            }
        }
        self.rendering.default_locale.clone()
    }

    /// The locale whose files render `channel` for a resolved `locale`:
    /// the locale, its language, then the default locale.
    fn file_locale(&self, info: &TemplateInfo, locale: &str, channel: Channel) -> Option<String> {
        let mut chain = vec![locale.to_owned()];
        if let Some((language, _)) = locale.split_once('-') {
            chain.push(language.to_owned());
        }
        chain.push(self.rendering.default_locale.clone());
        chain.into_iter().find(|l| info.has(l, channel))
    }

    /// The unsubscribe link previews show for non-security email.
    pub fn preview_unsubscribe(&self, category: Category, channel: Channel) -> Option<String> {
        (category != Category::Security && channel == Channel::Email).then(|| {
            let base = self
                .rendering
                .public_base_url
                .as_deref()
                .unwrap_or("https://example.invalid");
            format!("{base}/v1/notifications/unsubscribe?token=preview")
        })
    }

    fn context(
        &self,
        locale: &str,
        ctx: &RenderContext<'_>,
        channel: Channel,
        guard: Option<&Arc<SecretGuard>>,
    ) -> Jinja {
        let mut values = object(ctx.data);
        values.insert(
            "recipient".into(),
            map([
                ("name", optional(ctx.recipient_name)),
                ("locale", Jinja::from(locale)),
                ("timezone", Jinja::from(ctx.timezone.unwrap_or("UTC"))),
            ]),
        );
        let unsubscribe = if channel == Channel::Email && ctx.category != Category::Security {
            ctx.unsubscribe
        } else {
            None
        };
        values.insert(
            "links".into(),
            map([
                ("app", optional(self.rendering.app_base_url.as_deref())),
                ("unsubscribe", optional(unsubscribe)),
                (
                    "preferences",
                    optional(self.rendering.preferences_page.as_deref()),
                ),
            ]),
        );
        values.insert(
            "app".into(),
            map([("name", Jinja::from(self.rendering.app_name.as_str()))]),
        );
        values.insert(
            "notification".into(),
            map([
                ("id", Jinja::from(ctx.notification_id.to_string())),
                ("topic", Jinja::from(ctx.topic)),
            ]),
        );
        match guard {
            // The inbox never receives secret data (R2): `secret` is a
            // guard holding nothing, which records any use so the render
            // is refused, through partials too.
            Some(guard) => {
                values.insert("secret".into(), Jinja::from_dyn_object(guard.clone()));
            }
            None => {
                values.insert("secret".into(), Jinja::from(object(ctx.secret)));
            }
        }
        Jinja::from(values)
    }

    /// Render one engine template into a buffer capped at `cap` bytes, so
    /// output stops growing at the channel limit instead of after fuel.
    fn run(&self, engine: &str, scope: &Scope<'_>, cap: usize) -> Result<String, RenderError> {
        let fail = |code| RenderError {
            channel: scope.channel,
            code,
        };
        let template = self
            .env
            .get_template(engine)
            .map_err(|_| fail("template_missing"))?;
        let mut out = Capped {
            buffer: Vec::new(),
            cap,
            exceeded: false,
        };
        let result = template.render_captured_to(scope.context, &mut out);
        if scope.guard.is_some_and(|g| g.used()) {
            return Err(fail("inbox_references_secret"));
        }
        match result {
            Ok(_) => String::from_utf8(out.buffer).map_err(|_| fail("render_error")),
            Err(_) if out.exceeded => Err(fail("output_too_large")),
            Err(error) => Err(fail(render_code(&error))),
        }
    }

    /// Render `template` for `channel` in the resolved `locale`, with the
    /// limits of NT §4. Missing channel files fall back to the language and
    /// then to the default locale.
    pub fn render(
        &self,
        template: &str,
        channel: Channel,
        locale: &str,
        ctx: &RenderContext<'_>,
    ) -> Result<Rendered, RenderError> {
        let fail = |code| RenderError { channel, code };
        let info = self
            .templates
            .get(template)
            .ok_or(fail("template_missing"))?;
        let files = self
            .file_locale(info, locale, channel)
            .ok_or(fail("template_missing"))?;
        let guard = (channel == Channel::Inbox).then(|| Arc::new(SecretGuard::default()));
        let context = self.context(locale, ctx, channel, guard.as_ref());
        let scope = Scope {
            context: &context,
            channel,
            guard: guard.as_deref(),
        };
        let name = |file, field| engine_name(template, &files, file, field);
        match channel {
            Channel::Email => {
                let subject =
                    self.run(&name(File::Subject, None), &scope, MAX_SUBJECT_CHARS * 4)?;
                single_line(&subject, MAX_SUBJECT_CHARS).map_err(fail)?;
                let text = self.run(&name(File::BodyText, None), &scope, MAX_TEXT_BYTES)?;
                bounded(&text, MAX_TEXT_BYTES).map_err(fail)?;
                let html = if info.files[&files].contains(&File::BodyHtml) {
                    let html = self.run(&name(File::BodyHtml, None), &scope, MAX_HTML_BYTES)?;
                    bounded(&html, MAX_HTML_BYTES).map_err(fail)?;
                    Some(html)
                } else {
                    None
                };
                Ok(Rendered::Email(RenderedEmail {
                    subject,
                    text,
                    html,
                }))
            }
            Channel::Sms => {
                let text = self.run(&name(File::Sms, None), &scope, MAX_TEXT_BYTES)?;
                bounded(&text, MAX_TEXT_BYTES).map_err(fail)?;
                if text
                    .chars()
                    .any(|c| (c.is_control() && c != '\n') || invisible(c))
                {
                    return Err(fail("control_character"));
                }
                let counted = match &self.rendering.organisation_prefix {
                    Some(prefix) => format!("{prefix} {text}"),
                    None => text.clone(),
                };
                let (encoding, segments) = segments(&counted);
                if segments > self.rendering.max_segments {
                    return Err(fail("too_many_segments"));
                }
                Ok(Rendered::Sms(RenderedSms {
                    text,
                    encoding,
                    segments,
                }))
            }
            Channel::Push => {
                let title = self.run(
                    &name(File::Push, Some("title")),
                    &scope,
                    MAX_INBOX_TITLE_CHARS * 4,
                )?;
                single_line(&title, MAX_INBOX_TITLE_CHARS).map_err(fail)?;
                let body = self.run(&name(File::Push, Some("body")), &scope, MAX_TEXT_BYTES)?;
                bounded(&body, MAX_TEXT_BYTES).map_err(fail)?;
                let url = self.optional_field(&name(File::Push, Some("url")), &scope)?;
                let skeleton = serde_json::json!({
                    "id": Uuid::nil().to_string(), "notification_id": Uuid::nil().to_string(),
                    "topic": ctx.topic, "title": title, "body": "", "url": url,
                    // The worst case of a collapse key, which only has a
                    // byte limit: control characters, six bytes each once
                    // escaped (`\u0001`).
                    "tag": "\u{1}".repeat(merezha_platform::ports::MAX_COLLAPSE_KEY),
                });
                if skeleton.to_string().len() > MAX_PUSH_PAYLOAD_BYTES {
                    return Err(fail("output_too_large"));
                }
                Ok(Rendered::Push(RenderedPush { title, body, url }))
            }
            Channel::Inbox => {
                let title = self.run(
                    &name(File::Inbox, Some("title")),
                    &scope,
                    MAX_INBOX_TITLE_CHARS * 4,
                )?;
                single_line(&title, MAX_INBOX_TITLE_CHARS).map_err(fail)?;
                let body = self.run(
                    &name(File::Inbox, Some("body")),
                    &scope,
                    MAX_INBOX_BODY_CHARS * 4,
                )?;
                if body.chars().count() > MAX_INBOX_BODY_CHARS {
                    return Err(fail("output_too_large"));
                }
                let link = self.optional_field(&name(File::Inbox, Some("link")), &scope)?;
                let mut payload = Map::new();
                for key in info.payload_keys.get(&files).into_iter().flatten() {
                    let field = format!("payload.{key}");
                    let engine = engine_name(template, &files, File::Inbox, Some(&field));
                    let value = self.run(&engine, &scope, MAX_INBOX_PAYLOAD_BYTES)?;
                    payload.insert(key.clone(), Value::String(value));
                }
                if Value::Object(payload.clone()).to_string().len() > MAX_INBOX_PAYLOAD_BYTES {
                    return Err(fail("output_too_large"));
                }
                Ok(Rendered::Inbox(RenderedInbox {
                    title,
                    body,
                    link,
                    payload,
                }))
            }
        }
    }

    /// A link field: absent or empty renders `None`; otherwise it must be
    /// a URL the `url` filter accepts.
    fn optional_field(
        &self,
        engine: &str,
        scope: &Scope<'_>,
    ) -> Result<Option<String>, RenderError> {
        if self.env.get_template(engine).is_err() {
            return Ok(None);
        }
        let value = self.run(engine, scope, MAX_LINK_BYTES * 2)?;
        let value = value.trim();
        if value.is_empty() {
            return Ok(None);
        }
        if value.len() > MAX_LINK_BYTES
            || !filters::link_allowed(value, self.rendering.allow_http_links)
        {
            return Err(RenderError {
                channel: scope.channel,
                code: "invalid_url",
            });
        }
        Ok(Some(value.to_owned()))
    }
}

/// One render's context, channel and inbox secret guard.
struct Scope<'a> {
    context: &'a Jinja,
    channel: Channel,
    guard: Option<&'a SecretGuard>,
}

/// The inbox's `secret`: holds no data and records every use.
#[derive(Debug, Default)]
struct SecretGuard(AtomicBool);
impl SecretGuard {
    fn used(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }
    fn touch(&self) {
        self.0.store(true, Ordering::Relaxed);
    }
}
impl minijinja::value::Object for SecretGuard {
    fn get_value(self: &Arc<Self>, _: &Jinja) -> Option<Jinja> {
        self.touch();
        None
    }
    fn enumerate(self: &Arc<Self>) -> minijinja::value::Enumerator {
        self.touch();
        minijinja::value::Enumerator::NonEnumerable
    }
    fn is_true(self: &Arc<Self>) -> bool {
        self.touch();
        false
    }
    /// Prints nothing and records the use: returning an error here would
    /// make string conversions (`|string`, `~`) panic inside the engine.
    fn render(self: &Arc<Self>, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    where
        Self: Sized + 'static,
    {
        self.touch();
        Ok(())
    }
}

/// A writer that refuses to grow past `cap` bytes.
struct Capped {
    buffer: Vec<u8>,
    cap: usize,
    exceeded: bool,
}
impl std::io::Write for Capped {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.buffer.len() + bytes.len() > self.cap {
            self.exceeded = true;
            return Err(std::io::Error::other("output limit"));
        }
        self.buffer.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Line and paragraph separators and bidirectional overrides and
/// isolates: not controls to Unicode, but they break lines or reorder text
/// on the recipient's screen, so subjects, titles and SMS refuse them too.
fn invisible(c: char) -> bool {
    matches!(c, '\u{2028}' | '\u{2029}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
}

/// One line: no control characters at all, at most `max` characters, not
/// empty.
fn single_line(text: &str, max: usize) -> Result<(), &'static str> {
    if text.chars().any(|c| c.is_control() || invisible(c)) {
        Err("control_character")
    } else if text.trim().is_empty() {
        Err("empty_output")
    } else if text.chars().count() > max {
        Err("output_too_large")
    } else {
        Ok(())
    }
}
fn bounded(text: &str, max: usize) -> Result<(), &'static str> {
    if text.trim().is_empty() {
        Err("empty_output")
    } else if text.len() > max {
        Err("output_too_large")
    } else {
        Ok(())
    }
}

fn syntax_code(error: &minijinja::Error) -> &'static str {
    match error.kind() {
        ErrorKind::SyntaxError => "template_syntax",
        _ => "template_invalid",
    }
}
/// The stable code of a render error; the engine's message is dropped.
fn render_code(error: &minijinja::Error) -> &'static str {
    match error.kind() {
        ErrorKind::UndefinedError => "undefined_variable",
        ErrorKind::OutOfFuel => "out_of_fuel",
        ErrorKind::InvalidOperation => match error.detail() {
            Some(filters::URL_REJECTED) => "invalid_url",
            Some(filters::DATETIME_REJECTED) => "invalid_datetime",
            _ => "render_error",
        },
        _ => "render_error",
    }
}

/// `template.toml`: `description`, `required` and `[preview]` (its
/// `secret` table is the preview's secret data).
fn parse_template_toml(info: &mut TemplateInfo, text: &str) -> Result<(), &'static str> {
    let mut table: toml::Table = toml::from_str(text).map_err(|_| "invalid_template_config")?;
    if let Some(description) = table.remove("description") {
        let description = description.as_str().ok_or("invalid_template_config")?;
        if description.len() > 200 || description.chars().any(char::is_control) {
            return Err("invalid_template_config");
        }
        info.description = description.to_owned();
    }
    if let Some(required) = table.remove("required") {
        let required = required.as_array().ok_or("invalid_template_config")?;
        info.required = required
            .iter()
            .map(|v| v.as_str().map(str::to_owned))
            .collect::<Option<Vec<_>>>()
            .ok_or("invalid_template_config")?;
        if info.required.len() > 64
            || info
                .required
                .iter()
                .any(|k| k.is_empty() || RESERVED_KEYS.contains(&k.as_str()))
        {
            return Err("invalid_template_config");
        }
    }
    if let Some(preview) = table.remove("preview") {
        let toml::Value::Table(mut preview) = preview else {
            return Err("invalid_template_config");
        };
        if let Some(secret) = preview.remove("secret") {
            match toml_to_json(secret) {
                Value::Object(secret) => info.preview_secret = secret,
                _ => return Err("invalid_template_config"),
            }
        }
        if RESERVED_KEYS.iter().any(|k| preview.contains_key(*k)) {
            return Err("reserved_data_key");
        }
        if let Value::Object(map) = toml_to_json(toml::Value::Table(preview)) {
            info.preview = map;
        }
    }
    if table.is_empty() {
        Ok(())
    } else {
        Err("unknown_key")
    }
}

/// Register the template strings of `push.toml` or `inbox.toml`.
fn add_toml_fields(
    env: &mut Environment<'static>,
    info: &mut TemplateInfo,
    name: &str,
    locale: &str,
    kind: File,
    text: &str,
    inbox_fields: &mut Vec<(String, String)>,
) -> Result<(), &'static str> {
    let mut table: toml::Table = toml::from_str(text).map_err(|_| "invalid_template_config")?;
    let fields: &[&str] = match kind {
        File::Push => &["title", "body", "url"],
        _ => &["title", "body", "link"],
    };
    let key = segment(&format!("{name}/{locale}/{}", kind.name()));
    let mut add = |env: &mut Environment<'static>, field: &str, source: String| {
        let engine = engine_name(name, locale, kind, Some(field));
        env.add_template_owned(engine.clone(), source)
            .map_err(|e| syntax_code(&e))?;
        if kind == File::Inbox {
            inbox_fields.push((engine, key.clone()));
        }
        Ok::<(), &'static str>(())
    };
    for field in fields {
        match table.remove(*field) {
            Some(toml::Value::String(source)) => add(env, field, source)?,
            Some(_) => return Err("invalid_template_config"),
            None if *field == "url" || *field == "link" => {}
            None => return Err("template_field_missing"),
        }
    }
    if kind == File::Inbox
        && let Some(payload) = table.remove("payload")
    {
        let toml::Value::Table(payload) = payload else {
            return Err("invalid_template_config");
        };
        if payload.len() > MAX_PAYLOAD_KEYS {
            return Err("invalid_template_config");
        }
        let mut keys = Vec::new();
        for (field, source) in payload {
            let toml::Value::String(source) = source else {
                return Err("invalid_template_config");
            };
            add(env, &format!("payload.{field}"), source)?;
            keys.push(field);
        }
        info.payload_keys.insert(locale.to_owned(), keys);
    }
    if table.is_empty() {
        Ok(())
    } else {
        Err("unknown_key")
    }
}
