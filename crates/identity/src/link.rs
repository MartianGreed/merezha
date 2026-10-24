//! OAuth account linking decisions (section 6), as a stub: ID PR 3 adds the
//! providers, flows and `decide_link`'s callers. The types fix the inputs and
//! outcomes so PR 3 fills in [`decide_link`] and its truth table without
//! changing the signature. Until then every decision refuses.

/// What the provider asserted, and who is involved.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LinkInput {
    /// `(provider, subject)` is already linked to this user.
    pub linked_user: Option<uuid::Uuid>,
    /// The flow's intent is `link`, with this signed-in user.
    pub signed_in_user: Option<uuid::Uuid>,
    /// The provider is trusted (`trust_email`) and asserted the address as
    /// verified.
    pub trusted_verified_email: bool,
    /// A user holding the asserted address, and whether its address is
    /// verified.
    pub existing_user: Option<(uuid::Uuid, bool)>,
    pub link_by_verified_email: bool,
    pub registration_open: bool,
    pub allow_sign_up: bool,
}

/// The outcome of a linking decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LinkDecision {
    SignIn(uuid::Uuid),
    Attach(uuid::Uuid),
    /// Apply the pre-hijacking rule to this unverified user, then link.
    ClaimAndLink(uuid::Uuid),
    CreateUser,
    Refuse(&'static str),
}

/// Section 6's rules, in order. PR 1 has no provider, so this refuses with
/// `oauth_failed` until ID PR 3 implements the table.
pub fn decide_link(input: &LinkInput) -> LinkDecision {
    let _ = input;
    LinkDecision::Refuse("oauth_failed")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_stub_refuses_everything() {
        let input = LinkInput {
            linked_user: Some(uuid::Uuid::nil()),
            signed_in_user: None,
            trusted_verified_email: true,
            existing_user: None,
            link_by_verified_email: false,
            registration_open: true,
            allow_sign_up: true,
        };
        assert_eq!(decide_link(&input), LinkDecision::Refuse("oauth_failed"));
    }
}
