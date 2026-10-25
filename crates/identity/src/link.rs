//! OAuth account linking decisions (section 6): a pure function of what
//! the provider asserted, who is signed in, who holds the asserted address
//! and the configuration. The callback reads the rows, asks
//! [`decide_link`], and carries the decision out under the user locks.
//!
//! Owner answer ID1: no automatic linking by verified email. With
//! `link_by_verified_email = false` (the default) a trusted address that
//! names an existing verified account answers `account_exists`; the user
//! signs in with an existing method and links from settings.
use uuid::Uuid;

/// What the provider asserted about the address, after the email trust
/// rule (section 6).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AssertedEmail {
    /// No usable address.
    Absent,
    /// An address the provider is not trusted for, or did not assert as
    /// verified.
    Unverified,
    /// A provider with `trust_email = true` asserted this exact address as
    /// verified.
    Trusted,
}

/// What the provider asserted, and who is involved.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LinkInput {
    /// `(provider, subject)` is already linked to this user.
    pub linked_user: Option<Uuid>,
    /// The flow's intent is `link`, with this signed-in user.
    pub signed_in_user: Option<Uuid>,
    pub email: AssertedEmail,
    /// A user holding the asserted address, and whether its address is
    /// verified.
    pub existing_user: Option<(Uuid, bool)>,
    pub link_by_verified_email: bool,
    pub registration_open: bool,
    pub allow_sign_up: bool,
}

/// The outcome of a linking decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LinkDecision {
    /// Rule 1: sign this user in and refresh the stored assertion.
    SignIn(Uuid),
    /// Rule 2: attach the identity to the signed-in user (a no-op refresh
    /// when it is already theirs).
    Attach(Uuid),
    /// Rule 3, `link_by_verified_email = true`: link to this verified user
    /// and sign in.
    LinkAndSignIn(Uuid),
    /// Rule 3: apply the pre-hijacking rule to this unverified user (strip
    /// every credential and session), then link and sign in.
    ClaimAndLink(Uuid),
    /// Rule 4: create a user with the asserted address and link.
    CreateUser,
    /// Refuse with this callback error code.
    Refuse(&'static str),
}

/// Section 6's rules, in order.
pub fn decide_link(input: &LinkInput) -> LinkDecision {
    // 1. The identity is linked: sign that user in. A link intent by
    //    another user cannot take it.
    if let Some(linked) = input.linked_user {
        return match input.signed_in_user {
            Some(user) if user == linked => LinkDecision::Attach(user),
            Some(_) => LinkDecision::Refuse("identity_already_linked"),
            None => LinkDecision::SignIn(linked),
        };
    }
    // 2. A link intent attaches it to the signed-in user.
    if let Some(user) = input.signed_in_user {
        return LinkDecision::Attach(user);
    }
    // 3. A trusted verified address that names an existing user.
    if let (AssertedEmail::Trusted, Some((user, verified))) = (input.email, input.existing_user) {
        return if !verified {
            // Trusted proof of the address outranks an unproven claim.
            LinkDecision::ClaimAndLink(user)
        } else if input.link_by_verified_email {
            LinkDecision::LinkAndSignIn(user)
        } else {
            LinkDecision::Refuse("account_exists")
        };
    }
    // 4. No match: sign up, never onto someone else's address.
    match (input.email, input.existing_user) {
        (_, Some(_)) => LinkDecision::Refuse("account_exists"),
        (AssertedEmail::Absent, None) => LinkDecision::Refuse("email_unverified"),
        _ if !(input.registration_open && input.allow_sign_up) => {
            LinkDecision::Refuse("registration_closed")
        }
        _ => LinkDecision::CreateUser,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: Uuid = Uuid::from_u128(1);
    const B: Uuid = Uuid::from_u128(2);

    fn input() -> LinkInput {
        LinkInput {
            linked_user: None,
            signed_in_user: None,
            email: AssertedEmail::Trusted,
            existing_user: None,
            link_by_verified_email: false,
            registration_open: true,
            allow_sign_up: true,
        }
    }

    /// The truth table of section 6, one row per rule and branch.
    #[test]
    fn decide_link_truth_table() {
        use AssertedEmail::*;
        use LinkDecision::*;
        let rows: Vec<(&str, LinkInput, LinkDecision)> = vec![
            // Rule 1: linked identities sign in, whatever the address says.
            (
                "linked sign-in",
                LinkInput {
                    linked_user: Some(A),
                    ..input()
                },
                SignIn(A),
            ),
            (
                "linked sign-in, untrusted address of another user",
                LinkInput {
                    linked_user: Some(A),
                    email: Unverified,
                    existing_user: Some((B, true)),
                    ..input()
                },
                SignIn(A),
            ),
            (
                "linked sign-in, registration closed",
                LinkInput {
                    linked_user: Some(A),
                    registration_open: false,
                    allow_sign_up: false,
                    ..input()
                },
                SignIn(A),
            ),
            (
                "link intent, already the caller's",
                LinkInput {
                    linked_user: Some(A),
                    signed_in_user: Some(A),
                    ..input()
                },
                Attach(A),
            ),
            (
                "link intent, linked to another user",
                LinkInput {
                    linked_user: Some(B),
                    signed_in_user: Some(A),
                    ..input()
                },
                Refuse("identity_already_linked"),
            ),
            // Rule 2: link intent attaches, whatever the address says.
            (
                "link intent",
                LinkInput {
                    signed_in_user: Some(A),
                    ..input()
                },
                Attach(A),
            ),
            (
                "link intent, address of another verified user",
                LinkInput {
                    signed_in_user: Some(A),
                    existing_user: Some((B, true)),
                    ..input()
                },
                Attach(A),
            ),
            (
                "link intent, no address",
                LinkInput {
                    signed_in_user: Some(A),
                    email: Absent,
                    ..input()
                },
                Attach(A),
            ),
            // Rule 3: a trusted address of an existing user.
            (
                "trusted address of a verified user (ID1 default)",
                LinkInput {
                    existing_user: Some((A, true)),
                    ..input()
                },
                Refuse("account_exists"),
            ),
            (
                "trusted address of a verified user, link_by_verified_email",
                LinkInput {
                    existing_user: Some((A, true)),
                    link_by_verified_email: true,
                    ..input()
                },
                LinkAndSignIn(A),
            ),
            (
                "trusted address of an unverified user: claim",
                LinkInput {
                    existing_user: Some((A, false)),
                    ..input()
                },
                ClaimAndLink(A),
            ),
            (
                "trusted address of an unverified user, registration closed: still the owner",
                LinkInput {
                    existing_user: Some((A, false)),
                    registration_open: false,
                    ..input()
                },
                ClaimAndLink(A),
            ),
            // Rule 4: sign-up.
            ("new trusted address", input(), CreateUser),
            (
                "new untrusted address",
                LinkInput {
                    email: Unverified,
                    ..input()
                },
                CreateUser,
            ),
            (
                "untrusted address of a verified user",
                LinkInput {
                    email: Unverified,
                    existing_user: Some((A, true)),
                    ..input()
                },
                Refuse("account_exists"),
            ),
            (
                "untrusted address of an unverified user",
                LinkInput {
                    email: Unverified,
                    existing_user: Some((A, false)),
                    ..input()
                },
                Refuse("account_exists"),
            ),
            (
                "untrusted address of a verified user, link_by_verified_email",
                LinkInput {
                    email: Unverified,
                    existing_user: Some((A, true)),
                    link_by_verified_email: true,
                    ..input()
                },
                Refuse("account_exists"),
            ),
            (
                "no address",
                LinkInput {
                    email: Absent,
                    ..input()
                },
                Refuse("email_unverified"),
            ),
            (
                "registration closed",
                LinkInput {
                    registration_open: false,
                    ..input()
                },
                Refuse("registration_closed"),
            ),
            (
                "provider sign-up off",
                LinkInput {
                    allow_sign_up: false,
                    ..input()
                },
                Refuse("registration_closed"),
            ),
            (
                "no address, registration closed",
                LinkInput {
                    email: Absent,
                    registration_open: false,
                    ..input()
                },
                Refuse("email_unverified"),
            ),
        ];
        for (name, input, expected) in rows {
            assert_eq!(decide_link(&input), expected, "{name}");
        }
    }
}
