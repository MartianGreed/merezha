//! Preferences and suppressions (NT §6, §2.2 invariants 6 and 7): the
//! effective rule for a user target and the reason coverage of
//! suppressions, evaluated at expansion and again before each send.
use crate::topics::{Category, Topic};
use merezha_platform::ports::Channel;

/// The `(topic, channel)` and `('*', channel)` preference rows of one user
/// that matter for one topic.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct UserPreferences {
    pub topic: Option<bool>,
    pub any: Option<bool>,
}

/// Why a target is not sent, as `status_reason` stores it.
pub mod reason {
    /// The user's preference is off.
    pub const PREFERENCE: &str = "preference";
    /// A marketing topic without the user's explicit opt-in.
    pub const NOT_OPTED_IN: &str = "not_opted_in";
    pub const SUPPRESSED: &str = "suppressed";
    /// The channel's driver is `disabled` or its section is absent.
    pub const CHANNEL_DISABLED: &str = "channel_disabled";
    pub const NO_SUBSCRIPTION: &str = "no_subscription";
    pub const UNKNOWN_USER: &str = "unknown_user";
    pub const NO_DIRECTORY: &str = "no_directory";
    pub const TOO_MANY_TARGETS: &str = "too_many_targets";
    pub const RENDER_FAILED: &str = "render_failed";
    pub const DATA_PURGED: &str = "data_purged";
    pub const SECRET_PURGED: &str = "secret_purged";
    pub const RETRIES_EXHAUSTED: &str = "retries_exhausted";
    pub const EXPIRED: &str = "expired";
    pub const INVALID_ADDRESS: &str = "invalid_address";
    /// A resend would fall past the margin of Brevo's idempotency window
    /// after an attempt it may have processed (NT-N3): never resent.
    pub const IDEMPOTENCY_WINDOW: &str = "idempotency_window";
    /// Brevo refused the account until the deadline (NT-N3).
    pub const PROVIDER_ACCOUNT_UNRESOLVED: &str = "provider_account_unresolved";
    /// The target already got the topic's `max_per_target_per_hour`
    /// deliveries in the last hour (NT §6, NT-N8).
    pub const TARGET_RATE_LIMITED: &str = "target_rate_limited";
    /// The notification's organization was deleted (issue #63): an open
    /// delivery is `cancelled` with it at its next Tx A.
    pub const TENANT_DELETED: &str = "tenant_deleted";
    /// `reclaim_stuck`: the delivery's job was lost (issue #63).
    pub const JOB_LOST: &str = "job_lost";
}

/// Whether `channel` is on for a user target of `topic` (NT §6): security
/// always; then the `(topic, channel)` row; then `('*', channel)`; then the
/// topic default, on for a transactional topic's `default_channels`. A
/// marketing delivery needs an explicit `enabled = true` row for the topic
/// and channel (invariant 7). `Err` carries the skip reason.
pub fn user_allows(
    topic: &Topic,
    channel: Channel,
    prefs: UserPreferences,
) -> Result<(), &'static str> {
    match topic.category {
        Category::Security => Ok(()),
        Category::Marketing => match prefs.topic {
            Some(true) => Ok(()),
            Some(false) => Err(reason::PREFERENCE),
            None => Err(reason::NOT_OPTED_IN),
        },
        Category::Transactional => {
            let on = prefs
                .topic
                .or(prefs.any)
                .unwrap_or_else(|| topic.default_channels.contains(&channel));
            if on { Ok(()) } else { Err(reason::PREFERENCE) }
        }
    }
}

/// Whether a suppression with `reason` on `topic` (`None`: every topic its
/// reason covers) blocks a delivery of `wanted` in `category`. Security
/// topics ignore every suppression except `invalid` (invariant 6);
/// `hard_bounce`, `spam_complaint`, `unsubscribed`, `blocked` and `manual`
/// without a topic cover every non-security topic; `invalid` covers every
/// topic.
pub fn suppression_blocks(
    reason: &str,
    topic: Option<&str>,
    wanted: &str,
    category: Category,
) -> bool {
    if topic.is_some_and(|t| t != wanted) {
        return false;
    }
    match (reason, category) {
        ("invalid", _) => topic.is_none() || topic == Some(wanted),
        (_, Category::Security) => false,
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topics::Priority;

    fn topic(category: Category) -> Topic {
        Topic {
            name: "tasks.assigned".into(),
            category,
            channels: vec![Channel::Inbox, Channel::Email],
            default_channels: vec![Channel::Inbox],
            template: "tasks.assigned".into(),
            priority: Priority::Normal,
            user_configurable: category != Category::Security,
            description: String::new(),
            max_per_target_per_hour: None,
            builtin: false,
            declared: true,
        }
    }

    #[test]
    fn the_first_matching_rule_decides() {
        let none = UserPreferences::default();
        let t = topic(Category::Transactional);
        assert_eq!(user_allows(&t, Channel::Inbox, none), Ok(()));
        assert_eq!(user_allows(&t, Channel::Email, none), Err("preference"));
        let any_off = UserPreferences {
            topic: None,
            any: Some(false),
        };
        assert_eq!(user_allows(&t, Channel::Inbox, any_off), Err("preference"));
        let topic_on = UserPreferences {
            topic: Some(true),
            any: Some(false),
        };
        assert_eq!(user_allows(&t, Channel::Email, topic_on), Ok(()));
        let off = UserPreferences {
            topic: Some(false),
            any: None,
        };
        assert_eq!(
            user_allows(&topic(Category::Security), Channel::Email, off),
            Ok(())
        );
        let m = topic(Category::Marketing);
        assert_eq!(user_allows(&m, Channel::Inbox, none), Err("not_opted_in"));
        let any_on = UserPreferences {
            topic: None,
            any: Some(true),
        };
        assert_eq!(user_allows(&m, Channel::Inbox, any_on), Err("not_opted_in"));
        assert_eq!(user_allows(&m, Channel::Inbox, topic_on), Ok(()));
    }

    #[test]
    fn suppressions_cover_by_reason() {
        use Category::*;
        assert!(suppression_blocks(
            "hard_bounce",
            None,
            "a.b",
            Transactional
        ));
        assert!(!suppression_blocks("hard_bounce", None, "a.b", Security));
        assert!(suppression_blocks("invalid", None, "a.b", Security));
        assert!(suppression_blocks(
            "unsubscribed",
            Some("a.b"),
            "a.b",
            Marketing
        ));
        assert!(!suppression_blocks(
            "unsubscribed",
            Some("a.c"),
            "a.b",
            Marketing
        ));
        assert!(!suppression_blocks("manual", Some("a.b"), "a.b", Security));
    }
}
