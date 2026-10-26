//! Topics (NT §1, §7.2): named kinds of notification with their category,
//! channels, template and priority. The identity topics (R2) and the
//! block's own alert are predeclared; a project may override their channels,
//! template and priority, never their category.
use crate::config::{Reader, segment};
use merezha_platform::{config::ConfigIssue, ports::Channel};
use std::collections::{BTreeMap, BTreeSet};

/// `security` cannot be turned off, `transactional` is on by default,
/// `marketing` is off until the user opts in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Category {
    Security,
    Transactional,
    Marketing,
}
impl Category {
    pub fn as_str(self) -> &'static str {
        match self {
            Category::Security => "security",
            Category::Transactional => "transactional",
            Category::Marketing => "marketing",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Priority {
    Low,
    Normal,
    High,
}
impl Priority {
    pub fn as_str(self) -> &'static str {
        match self {
            Priority::Low => "low",
            Priority::Normal => "normal",
            Priority::High => "high",
        }
    }
}

/// One topic, built-in or declared.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Topic {
    pub name: String,
    pub category: Category,
    /// Allowed channels, in declaration order, without duplicates.
    pub channels: Vec<Channel>,
    /// A subset of `channels`; the channels used without an explicit
    /// selection.
    pub default_channels: Vec<Channel>,
    /// Template name; default the topic name.
    pub template: String,
    pub priority: Priority,
    /// Shown in the preferences API; always `false` for security topics.
    pub user_configurable: bool,
    pub description: String,
    pub max_per_target_per_hour: Option<u32>,
    /// Predeclared by the block.
    pub builtin: bool,
    /// Present in `[modules.notifications.topics]`.
    pub declared: bool,
}

/// Most bytes in a topic, template or event type name.
pub const MAX_NAME_BYTES: usize = 128;

/// `^[a-z][a-z0-9_]*(\.[a-z0-9_]+){1,4}$`, at most 128 bytes: topic,
/// template and event type names such as `tasks.assigned`.
pub fn valid_dotted_name(name: &str) -> bool {
    let segments: Vec<&str> = name.split('.').collect();
    name.len() <= MAX_NAME_BYTES
        && (2..=5).contains(&segments.len())
        && segments[0]
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_lowercase)
        && segments.iter().all(|s| {
            !s.is_empty()
                && s.bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        })
}

/// Parse a channel name.
pub fn channel(name: &str) -> Option<Channel> {
    match name {
        "email" => Some(Channel::Email),
        "sms" => Some(Channel::Sms),
        "push" => Some(Channel::Push),
        "inbox" => Some(Channel::Inbox),
        _ => None,
    }
}

/// The predeclared topics: every identity topic of R2 and
/// `notifications.webhook_disabled`.
pub const BUILTIN: [(&str, Category, &[Channel], Priority, &str); 11] = [
    (
        "identity.verify_email",
        Category::Security,
        &[Channel::Email],
        Priority::High,
        "Confirm your email address",
    ),
    (
        "identity.password_reset",
        Category::Security,
        &[Channel::Email],
        Priority::High,
        "Reset your password",
    ),
    (
        "identity.magic_link",
        Category::Security,
        &[Channel::Email],
        Priority::High,
        "Sign in with a link",
    ),
    (
        "identity.account_exists",
        Category::Security,
        &[Channel::Email],
        Priority::Normal,
        "Someone tried to register with your address",
    ),
    (
        "identity.password_changed",
        Category::Security,
        &[Channel::Email, Channel::Inbox],
        Priority::Normal,
        "Your password was changed",
    ),
    (
        "identity.email_change_confirm",
        Category::Security,
        &[Channel::Email],
        Priority::High,
        "Confirm your new email address",
    ),
    (
        "identity.email_change_notice",
        Category::Security,
        &[Channel::Email],
        Priority::Normal,
        "Your email address is changing",
    ),
    (
        "identity.invitation",
        Category::Transactional,
        &[Channel::Email],
        Priority::Normal,
        "You were invited to an organization",
    ),
    (
        "identity.new_sign_in",
        Category::Security,
        &[Channel::Email, Channel::Inbox],
        Priority::Normal,
        "A new sign-in to your account",
    ),
    (
        "identity.sign_in_method_added",
        Category::Security,
        &[Channel::Email, Channel::Inbox],
        Priority::Normal,
        "A new sign-in method was added to your account",
    ),
    (
        "notifications.webhook_disabled",
        Category::Transactional,
        &[Channel::Email, Channel::Inbox],
        Priority::Normal,
        "A webhook endpoint was disabled",
    ),
];

fn builtins() -> BTreeMap<String, Topic> {
    BUILTIN
        .iter()
        .map(|(name, category, channels, priority, description)| {
            (
                (*name).to_owned(),
                Topic {
                    name: (*name).to_owned(),
                    category: *category,
                    channels: channels.to_vec(),
                    default_channels: channels.to_vec(),
                    template: (*name).to_owned(),
                    priority: *priority,
                    // Invitations address people who may have no account.
                    user_configurable: *category != Category::Security
                        && *name != "identity.invitation",
                    description: (*description).to_owned(),
                    max_per_target_per_hour: None,
                    builtin: true,
                    declared: false,
                },
            )
        })
        .collect()
}

const CATEGORIES: &[(&str, Category)] = &[
    ("security", Category::Security),
    ("transactional", Category::Transactional),
    ("marketing", Category::Marketing),
];
const PRIORITIES: &[(&str, Priority)] = &[
    ("low", Priority::Low),
    ("normal", Priority::Normal),
    ("high", Priority::High),
];

/// A channel list: known names, no duplicates. `None` after an issue.
fn channels(r: &mut Reader<'_>, key: &str) -> Option<Vec<Channel>> {
    let names = r.list(key, 4)?;
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for name in names {
        match channel(&name) {
            Some(channel) if seen.insert(channel.as_str()) => out.push(channel),
            Some(_) => {
                r.issue(key, "duplicate_channel");
                return None;
            }
            None => {
                r.issue(key, "invalid_channel");
                return None;
            }
        }
    }
    Some(out)
}

/// Built-ins plus `[modules.notifications.topics]`, with every issue
/// recorded under `prefix` (`modules.notifications.topics`).
pub(crate) fn parse(
    prefix: &str,
    declared: toml::Table,
    issues: &mut Vec<ConfigIssue>,
) -> BTreeMap<String, Topic> {
    let mut topics = builtins();
    if topics.len() + declared.len() > crate::config::MAX_TOPICS {
        issues.push(ConfigIssue::new(
            prefix.to_owned(),
            "too_many_items",
            merezha_platform::config::Origin::File,
        ));
        return topics;
    }
    for (name, value) in declared {
        let key = format!("{prefix}.{}", segment(&name));
        if !valid_dotted_name(&name) {
            issues.push(ConfigIssue::new(
                key,
                "invalid_topic_name",
                merezha_platform::config::Origin::File,
            ));
            continue;
        }
        let toml::Value::Table(table) = value else {
            issues.push(ConfigIssue::new(
                key,
                "expected_table",
                merezha_platform::config::Origin::File,
            ));
            continue;
        };
        let builtin = topics.get(&name).cloned();
        let mut r = Reader::new(key, table, BTreeMap::new(), issues);
        let category = match (&builtin, r.has("category")) {
            (Some(builtin), true) => {
                let category = r.choice("category", builtin.category, CATEGORIES);
                if category != builtin.category {
                    r.issue("category", "category_fixed");
                }
                builtin.category
            }
            (Some(builtin), false) => builtin.category,
            (None, true) => r.choice("category", Category::Transactional, CATEGORIES),
            (None, false) => {
                r.issue_default("category", "missing_required");
                Category::Transactional
            }
        };
        let had_channels = r.has("channels");
        let declared_channels = channels(&mut r, "channels");
        let channel_list = match (&declared_channels, &builtin) {
            (Some(list), _) if list.is_empty() => {
                r.issue("channels", "missing_required");
                Vec::new()
            }
            (Some(list), _) => list.clone(),
            (None, Some(builtin)) if !had_channels => builtin.channels.clone(),
            (None, None) if !had_channels => {
                r.issue_default("channels", "missing_required");
                Vec::new()
            }
            (None, _) => Vec::new(),
        };
        let default_channels = match channels(&mut r, "default_channels") {
            Some(defaults) => {
                if defaults.iter().any(|c| !channel_list.contains(c)) {
                    r.issue("default_channels", "default_channel_not_allowed");
                }
                defaults
            }
            None => match &builtin {
                Some(builtin) if !had_channels => builtin.default_channels.clone(),
                _ => channel_list.clone(),
            },
        };
        let template = match r.string("template") {
            Some(template) if valid_dotted_name(&template) => template,
            Some(_) => {
                r.issue("template", "invalid_template_name");
                name.clone()
            }
            None => builtin
                .as_ref()
                .map_or_else(|| name.clone(), |b| b.template.clone()),
        };
        let priority = r.choice(
            "priority",
            builtin.as_ref().map_or(Priority::Normal, |b| b.priority),
            PRIORITIES,
        );
        let configurable_default = builtin
            .as_ref()
            .map_or(category != Category::Security, |b| b.user_configurable);
        let user_configurable = r.boolean("user_configurable", configurable_default);
        if user_configurable && category == Category::Security {
            r.issue("user_configurable", "security_not_configurable");
        }
        let description = r
            .string("description")
            .or_else(|| builtin.as_ref().map(|b| b.description.clone()))
            .unwrap_or_default();
        if description.len() > 200 || description.chars().any(char::is_control) {
            r.issue("description", "out_of_range");
        }
        let max_per_target_per_hour = r
            .optional_integer("max_per_target_per_hour", 1, 10_000)
            .map(|n| n as u32);
        r.finish();
        topics.insert(
            name.clone(),
            Topic {
                name,
                category,
                channels: channel_list,
                default_channels,
                template,
                priority,
                user_configurable: user_configurable && category != Category::Security,
                description,
                max_per_target_per_hour,
                builtin: builtin.is_some(),
                declared: true,
            },
        );
    }
    topics
}
