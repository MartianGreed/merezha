//! Deterministic identifiers (NT §2.2, §2.5) and the target keys and masks
//! of deliveries.
//!
//! A notification's id is `UUIDv5(NS_NOTIFY, source "\n" dedupe_key)`, so a
//! caller knows it before expansion runs and a second request with the same
//! key names the same notification. A delivery's id is `UUIDv5(NS_DELIVERY,
//! notification_id "\n" channel "\n" target_key)`, so rerunning an expansion
//! cannot create a second delivery.
use merezha_platform::ports::Channel;
use uuid::Uuid;

/// Namespace of notification ids.
pub const NS_NOTIFY: Uuid = Uuid::from_u128(0x6d7a_6e74_0000_5000_8000_6e6f_7469_6679);
/// Namespace of delivery ids.
pub const NS_DELIVERY: Uuid = Uuid::from_u128(0x6d7a_6e74_0001_5000_8000_6465_6c69_7672);
/// Namespace of webhook event ids.
pub const NS_WEBHOOK_EVENT: Uuid = Uuid::from_u128(0x6d7a_6e74_0002_5000_8000_7768_6576_6e74);

/// The id of the notification `(source, dedupe_key)`.
pub fn notification_id(source: &str, dedupe_key: &str) -> Uuid {
    Uuid::new_v5(&NS_NOTIFY, format!("{source}\n{dedupe_key}").as_bytes())
}
/// The id of the delivery of `notification` on `channel` to `target_key`.
pub fn delivery_id(notification: Uuid, channel: Channel, target_key: &str) -> Uuid {
    Uuid::new_v5(
        &NS_DELIVERY,
        format!("{notification}\n{}\n{target_key}", channel.as_str()).as_bytes(),
    )
}
/// The id of the webhook event `(source, source_event_id)`.
pub fn webhook_event_id(source: &str, source_event_id: &str) -> Uuid {
    Uuid::new_v5(
        &NS_WEBHOOK_EVENT,
        format!("{source}\n{source_event_id}").as_bytes(),
    )
}
/// The provider idempotency key of one delivery generation:
/// `UUIDv5(delivery_id, "g{generation}")`, stable across its attempts.
pub fn idempotency_key(delivery: Uuid, generation: i32) -> Uuid {
    Uuid::new_v5(&delivery, format!("g{generation}").as_bytes())
}

/// Most bytes of a stored target key.
pub const MAX_TARGET_KEY: usize = 400;

/// `email:<lowercased address>`: the same mailbox from a user and from an
/// explicit address is one target.
pub fn email_key(address: &str) -> String {
    format!("email:{}", address.to_lowercase())
}
pub fn sms_key(e164: &str) -> String {
    format!("sms:{e164}")
}
pub fn inbox_key(user: &str) -> String {
    format!("inbox:{user}")
}
pub fn push_key(user: &str) -> String {
    format!("push:{user}")
}
/// One push subscription: a user gets one push delivery per subscription.
pub fn push_subscription_key(subscription: Uuid) -> String {
    format!("pushsub:{subscription}")
}
/// A user the Directory did not resolve: one skipped delivery per channel.
pub fn user_key(user: &str) -> String {
    format!("user:{user}")
}
/// A tenant or application recipient nobody could expand (no Directory).
pub fn group_key(kind: &str, name: &str) -> String {
    let mut key = format!("{kind}:{name}");
    key.truncate(MAX_TARGET_KEY);
    key
}

/// The masked target the status route shows: `a***@example.com`,
/// `+33•••••78`, `push` or `inbox`. Never the full address.
pub fn mask(channel: Channel, address: Option<&str>) -> String {
    match (channel, address) {
        (Channel::Email, Some(address)) => match address.split_once('@') {
            Some((local, domain)) => {
                let first: String = local.chars().take(1).collect();
                format!("{first}***@{domain}")
            }
            None => "***".to_owned(),
        },
        (Channel::Sms, Some(number)) => {
            let chars: Vec<char> = number.chars().collect();
            if chars.len() <= 5 {
                return "•".repeat(chars.len());
            }
            let head: String = chars[..3].iter().collect();
            let tail: String = chars[chars.len() - 2..].iter().collect();
            format!("{head}{}{tail}", "•".repeat(chars.len() - 5))
        }
        (Channel::Push, _) => "push".to_owned(),
        (Channel::Inbox, _) => "inbox".to_owned(),
        (_, None) => "none".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_deterministic_and_separated_by_source() {
        let a = notification_id("identity", "identity.magic_link:t1");
        assert_eq!(a, notification_id("identity", "identity.magic_link:t1"));
        assert_ne!(a, notification_id("api", "identity.magic_link:t1"));
        assert_eq!(a.get_version_num(), 5);
        let d = delivery_id(a, Channel::Email, &email_key("Ada@Example.com"));
        assert_eq!(
            d,
            delivery_id(a, Channel::Email, &email_key("ada@example.com"))
        );
        assert_ne!(
            d,
            delivery_id(a, Channel::Inbox, &email_key("ada@example.com"))
        );
        assert_eq!(idempotency_key(d, 0), idempotency_key(d, 0));
        assert_ne!(idempotency_key(d, 0), idempotency_key(d, 1));
    }

    #[test]
    fn targets_are_masked() {
        assert_eq!(
            mask(Channel::Email, Some("ada@example.com")),
            "a***@example.com"
        );
        assert_eq!(mask(Channel::Sms, Some("+33612345678")), "+33•••••••78");
        assert_eq!(mask(Channel::Push, None), "push");
        assert_eq!(mask(Channel::Inbox, Some("usr_1")), "inbox");
    }
}
