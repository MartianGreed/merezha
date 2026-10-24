//! Cross-module ports. Blocks implement them; other blocks and the host use
//! them through [`crate::AppContext`]. A disabled block leaves a defined
//! fallback that never silently drops a security-relevant effect.
//!
//! Ports that write "inside the caller's transaction" ([`Outbox`],
//! [`Notifier`]) take `&mut Tx<'_>` from `merezha-postgres` and never commit
//! it. No port method waits on a second pooled connection while a caller's
//! transaction is open: callers read the [`Directory`] first.
mod access;
mod blob;
mod directory;
mod jobs;
mod notifier;
mod outbox;

pub use access::*;
pub use blob::*;
pub use directory::*;
pub use jobs::*;
pub use notifier::*;
pub use outbox::*;

use merezha_domain::Failure;

/// The caller a source-bound port handle writes for: a module name, or
/// `api` (HTTP routes) and `application` (reactions). It names the outbox
/// dedupe namespace and the notification membership rule. `[a-z][a-z0-9_]*`,
/// at most 64 bytes.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Source(String);
impl Source {
    /// Fails permanent `invalid_source`.
    pub fn new(value: &str) -> Result<Self, Failure> {
        if crate::config::valid_module_name(value) {
            Ok(Self(value.to_owned()))
        } else {
            Err(Failure::permanent("invalid_source"))
        }
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use merezha_domain::Identity;
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    fn id(value: &str) -> Identity {
        Identity::new(value).unwrap()
    }
    fn request(key: &str) -> NotificationRequest {
        NotificationRequest {
            topic: id("identity.magic_link"),
            recipients: vec![Recipient::Address(Address::Email {
                address: EmailAddress::new("ada@example.com").unwrap(),
                name: Some("Ada Lovelace".into()),
                user: None,
                locale: None,
            })],
            channels: ChannelSelection::Only(vec![Channel::Email]),
            template: None,
            data: json!({"display_name": "Ada"}).as_object().unwrap().clone(),
            secret_data: json!({"link": "https://app.example/m/t0k3n-s3cr3t"})
                .as_object()
                .unwrap()
                .clone(),
            locale: None,
            dedupe_key: DedupeKey::new(key).unwrap(),
            tenant: None,
            members: Vec::new(),
            not_before: None,
            expires_at: None,
            collapse_key: None,
            correlation: Correlation {
                correlation_id: id("corr1"),
                causation_id: None,
            },
        }
    }

    /// Everything the log subscriber writes, as text.
    #[derive(Clone, Default)]
    struct Captured(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for Captured {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn capture_notifier_never_logs_or_keeps_secret_data() {
        let logs = Captured::default();
        let writer = logs.clone();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_writer(move || writer.clone())
            .finish();
        let factory = CaptureNotifications::default();
        assert!(!factory.delivers());
        let notifier = factory.capture_notifier(&Source::new("identity").unwrap());
        tracing::subscriber::with_default(subscriber, || {
            for n in 0..70 {
                let request = request(&format!("identity.magic_link:k{n}"));
                let printed = format!("{request:?}");
                for secret in ["t0k3n-s3cr3t", "ada@example.com", "Ada Lovelace", "\"Ada\""] {
                    assert!(
                        !printed.contains(secret),
                        "Debug leaked {secret}: {printed}"
                    );
                }
                let handle = notifier.capture(request).unwrap();
                assert!(!handle.duplicate);
            }
        });
        let text = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
        assert!(text.contains("notification.captured"));
        for secret in [
            "t0k3n-s3cr3t",
            "https://app.example",
            "ada@example.com",
            "Ada",
            "display_name",
        ] {
            assert!(!text.contains(secret), "the log leaked {secret}");
        }
        let captured = factory.captured();
        assert_eq!(captured.len(), CaptureNotifications::CAPACITY);
        assert_eq!(captured[0].dedupe_key, "identity.magic_link:k6");
        assert_eq!(captured[0].recipient_kinds, vec!["email_address"]);
        assert!(captured[0].had_secret_data);
        assert!(!format!("{captured:?}").contains("t0k3n-s3cr3t"));
    }

    #[test]
    fn membership_is_checked_from_the_request_for_application_and_api() {
        let mut request = request("reaction:tasks:e1:0");
        request.recipients = vec![
            Recipient::User { id: id("usr_1") },
            Recipient::User { id: id("usr_2") },
        ];
        request.tenant = Some(id("org_1"));
        request.members = vec![id("usr_1")];
        for source in ["application", "api"] {
            assert_eq!(
                check_members(&Source::new(source).unwrap(), &request),
                Err(NotifyError::RecipientNotInTenant)
            );
        }
        // Identity is exempt: invitations target non-members.
        assert_eq!(
            check_members(&Source::new("identity").unwrap(), &request),
            Ok(())
        );
        request.members.push(id("usr_2"));
        assert_eq!(
            check_members(&Source::new("api").unwrap(), &request),
            Ok(())
        );
        request.tenant = None;
        request.members.clear();
        assert_eq!(
            check_members(&Source::new("api").unwrap(), &request),
            Ok(())
        );
        assert_eq!(
            Failure::from(NotifyError::RecipientNotInTenant).code,
            "recipient_not_in_tenant"
        );
        assert_eq!(
            Failure::from(NotifyError::Unavailable).classification,
            merezha_domain::Classification::Transient
        );
    }

    #[test]
    fn capture_validation_refuses_structural_errors() {
        let notifier = CaptureNotifications::default().bind(&Source::new("api").unwrap());
        let mut empty = request("k");
        empty.recipients.clear();
        assert_eq!(
            notifier.validate(&empty),
            Err(NotifyError::InvalidRecipient)
        );
        let mut many = request("k");
        many.recipients = vec![Recipient::User { id: id("usr_1") }; MAX_RECIPIENTS + 1];
        assert_eq!(
            notifier.validate(&many),
            Err(NotifyError::TooManyRecipients)
        );
        let mut reserved = request("k");
        reserved.data.insert("secret".into(), json!("x"));
        assert_eq!(
            notifier.validate(&reserved),
            Err(NotifyError::ReservedDataKey)
        );
        let mut foreign = request("k");
        foreign.tenant = Some(id("org_1"));
        foreign.recipients = vec![Recipient::Tenant {
            id: id("org_2"),
            roles: None,
        }];
        assert_eq!(
            notifier.validate(&foreign),
            Err(NotifyError::InvalidRecipient)
        );
    }

    #[test]
    fn addresses_and_contacts_are_validated_and_redacted() {
        assert!(EmailAddress::new("a@b").is_ok());
        for bad in ["", "ab", "a@", "@b", "a@b@c", "a b@c"] {
            assert!(EmailAddress::new(bad).is_err(), "{bad}");
        }
        assert!(PhoneNumber::new("+33612345678").is_ok());
        for bad in ["33612345678", "+0612", "+1", "+33 6", "+1234567890123456"] {
            assert!(PhoneNumber::new(bad).is_err(), "{bad}");
        }
        let phone = Recipient::Address(Address::Phone {
            e164: PhoneNumber::new("+33612345678").unwrap(),
            user: None,
            locale: Some(Locale::new("fr").unwrap()),
        });
        assert!(!format!("{phone:?}").contains("612345678"));
        let contact = Contact {
            user: id("usr_1"),
            email: "ada@example.com".into(),
            display_name: Some("Ada".into()),
            locale: None,
        };
        assert!(!format!("{contact:?}").contains("ada@"));
        assert!(Locale::new("pt-BR").is_ok());
        assert!(Locale::new("1x").is_err());
    }

    #[tokio::test]
    async fn the_empty_directory_resolves_nothing() {
        let directory = EmptyDirectory;
        assert!(directory.users(&[id("usr_1")]).await.unwrap().is_empty());
        let too_many = vec![id("usr_1"); MAX_USERS_PER_CALL + 1];
        assert_eq!(
            directory.users(&too_many).await.unwrap_err().code,
            "too_many_ids"
        );
        let page = PageRequest::first(MAX_PAGE).unwrap();
        assert_eq!(
            directory
                .tenant_members(&id("org_1"), None, page.clone())
                .await
                .unwrap(),
            Page::empty()
        );
        assert!(
            directory
                .application_members(None, page)
                .await
                .unwrap()
                .items
                .is_empty()
        );
        assert!(
            !directory
                .is_member(&id("usr_1"), &id("org_1"))
                .await
                .unwrap()
        );
        assert_eq!(PageRequest::first(0).unwrap_err().code, "invalid_page");
        assert_eq!(
            PageRequest::first(MAX_PAGE + 1).unwrap_err().code,
            "invalid_page"
        );
    }

    #[test]
    fn sources_are_module_like_names() {
        assert!(Source::new("identity").is_ok());
        assert!(Source::new("api").is_ok());
        assert_eq!(Source::new("Api").unwrap_err().code, "invalid_source");
    }

    #[test]
    fn webhook_event_ids_fit_a_source_and_a_full_dedupe_key() {
        let longest = format!("{}:{}", "s".repeat(64), "k".repeat(MAX_DEDUPE_KEY_BYTES));
        assert_eq!(longest.len(), MAX_EVENT_ID_BYTES);
        assert_eq!(EventId::new(longest.clone()).unwrap().as_str(), longest);
        assert!(
            DedupeKey::new(longest.clone()).is_err(),
            "too long for a dedupe key"
        );
        assert_eq!(
            EventId::new(format!("{longest}x")).unwrap_err().code,
            "invalid_event_id"
        );
        for bad in ["", "has space", "é"] {
            assert!(EventId::new(bad).is_err(), "{bad}");
        }
    }
}
