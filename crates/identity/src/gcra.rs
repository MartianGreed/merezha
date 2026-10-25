//! GCRA, the generic cell rate algorithm (section 7), and the policy
//! table. One theoretical arrival time (`tat`) per bucket: with emission
//! interval `T = period / limit` and burst `B = limit`, a request at `now`
//! is allowed when `max(tat, now) + T <= now + B·T`, and then sets
//! `tat = max(tat, now) + T`. The PostgreSQL statement `sql::RATE_CHARGE`
//! evaluates exactly [`admit`].
use std::time::Duration;

/// `limit` requests per `period`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Rate {
    pub limit: u32,
    pub period: Duration,
}
impl Rate {
    pub const fn new(limit: u32, period: Duration) -> Self {
        Self { limit, period }
    }
    /// `T` in microseconds, at least 1.
    pub fn interval_us(&self) -> i64 {
        (self.period.as_micros() as i64 / i64::from(self.limit.max(1))).max(1)
    }
    /// `B·T` in microseconds.
    pub fn burst_us(&self) -> i64 {
        self.interval_us() * i64::from(self.limit.max(1))
    }
}

/// Decide a request at `now` (microseconds) against a bucket whose stored
/// `tat` is `tat`: `Ok(new_tat)` when allowed, `Err(retry_after_us)` when
/// denied.
pub fn admit(tat: Option<i64>, now: i64, rate: Rate) -> Result<i64, i64> {
    let base = tat.map_or(now, |tat| tat.max(now));
    let next = base + rate.interval_us();
    if next <= now + rate.burst_us() {
        Ok(next)
    } else {
        Err(next - (now + rate.burst_us()))
    }
}

/// When a policy is charged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Charge {
    /// Every request, before the work.
    Arrival,
    /// Checked without charging before the work, charged when it fails, so
    /// naming a victim's address does not spend the victim's budget.
    Failure,
}

/// One rate-limit policy of section 7.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Policy {
    /// Name such as `sign_in.ip`, part of the bucket key and the
    /// `identity.policy` metric attribute.
    pub name: &'static str,
    pub charge: Charge,
    pub default: Rate,
}
const fn minutes(m: u64) -> Duration {
    Duration::from_secs(m * 60)
}

pub const SIGN_IN_IP: Policy = Policy {
    name: "sign_in.ip",
    charge: Charge::Arrival,
    default: Rate::new(30, minutes(5)),
};
pub const SIGN_IN_ACCOUNT_IP: Policy = Policy {
    name: "sign_in.account_ip",
    charge: Charge::Failure,
    default: Rate::new(5, minutes(15)),
};
pub const SIGN_IN_ACCOUNT: Policy = Policy {
    name: "sign_in.account",
    charge: Charge::Failure,
    default: Rate::new(50, minutes(24 * 60)),
};
pub const REGISTRATION_IP: Policy = Policy {
    name: "registration.ip",
    charge: Charge::Arrival,
    default: Rate::new(10, minutes(60)),
};
pub const EMAIL_ADDRESS_IP: Policy = Policy {
    name: "email.address_ip",
    charge: Charge::Arrival,
    default: Rate::new(5, minutes(60)),
};
pub const EMAIL_ADDRESS: Policy = Policy {
    name: "email.address",
    charge: Charge::Arrival,
    default: Rate::new(30, minutes(60)),
};
pub const EMAIL_IP: Policy = Policy {
    name: "email.ip",
    charge: Charge::Arrival,
    default: Rate::new(20, minutes(60)),
};
pub const TOKEN_IP: Policy = Policy {
    name: "token.ip",
    charge: Charge::Failure,
    default: Rate::new(30, minutes(10)),
};
pub const REAUTH_SESSION: Policy = Policy {
    name: "reauth.session",
    charge: Charge::Failure,
    default: Rate::new(5, minutes(15)),
};

pub const INVITATION_ORGANIZATION: Policy = Policy {
    name: "invitation.organization",
    charge: Charge::Arrival,
    default: Rate::new(100, minutes(24 * 60)),
};
pub const API_KEY_INVALID_IP: Policy = Policy {
    name: "api_key.invalid.ip",
    charge: Charge::Failure,
    default: Rate::new(60, minutes(1)),
};

pub const PASSKEY_IP: Policy = Policy {
    name: "passkey.ip",
    charge: Charge::Arrival,
    default: Rate::new(60, minutes(10)),
};
pub const OAUTH_IP: Policy = Policy {
    name: "oauth.ip",
    charge: Charge::Arrival,
    default: Rate::new(30, minutes(10)),
};
/// Failed OAuth callbacks per IP (unknown or unbound `state`, refused
/// codes): checked before the callback does any work, charged when it
/// fails, so a flood of forged callbacks costs nothing past its budget.
pub const OAUTH_CALLBACK_IP: Policy = Policy {
    name: "oauth_callback.ip",
    charge: Charge::Failure,
    default: Rate::new(30, minutes(10)),
};
/// Every policy of section 7.
pub const POLICIES: [Policy; 14] = [
    SIGN_IN_IP,
    SIGN_IN_ACCOUNT_IP,
    SIGN_IN_ACCOUNT,
    REGISTRATION_IP,
    EMAIL_ADDRESS_IP,
    EMAIL_ADDRESS,
    EMAIL_IP,
    TOKEN_IP,
    REAUTH_SESSION,
    INVITATION_ORGANIZATION,
    API_KEY_INVALID_IP,
    PASSKEY_IP,
    OAUTH_IP,
    OAUTH_CALLBACK_IP,
];

/// The configuration table name of a policy: dots become underscores.
pub fn table_name(policy: &Policy) -> String {
    policy.name.replace('.', "_")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gcra_admits_the_burst_then_one_per_interval() {
        let rate = Rate::new(5, Duration::from_secs(50));
        assert_eq!(rate.interval_us(), 10_000_000);
        assert_eq!(rate.burst_us(), 50_000_000);
        let mut tat = None;
        let now = 1_000_000_000;
        for _ in 0..5 {
            tat = Some(admit(tat, now, rate).unwrap());
        }
        // The sixth request at the same instant waits one interval.
        assert_eq!(admit(tat, now, rate), Err(10_000_000));
        // One interval later exactly one more fits.
        let later = now + 10_000_000;
        tat = Some(admit(tat, later, rate).unwrap());
        assert!(admit(tat, later, rate).is_err());
        // A drained bucket behaves like a new one.
        let much_later = later + 3_600_000_000;
        assert_eq!(admit(tat, much_later, rate), admit(None, much_later, rate));
        // Retry-After shrinks as time passes.
        let full = Some(now + 50_000_000);
        assert_eq!(admit(full, now, rate), Err(10_000_000));
        assert_eq!(admit(full, now + 4_000_000, rate), Err(6_000_000));
    }

    #[test]
    fn policies_have_distinct_names_and_table_names() {
        let names: std::collections::BTreeSet<_> = POLICIES.iter().map(|p| p.name).collect();
        assert_eq!(names.len(), POLICIES.len());
        assert_eq!(table_name(&SIGN_IN_IP), "sign_in_ip");
        assert_eq!(table_name(&EMAIL_ADDRESS_IP), "email_address_ip");
        assert_eq!(table_name(&API_KEY_INVALID_IP), "api_key_invalid_ip");
        assert_eq!(table_name(&PASSKEY_IP), "passkey_ip");
        assert_eq!(table_name(&OAUTH_IP), "oauth_ip");
        assert_eq!(SIGN_IN_ACCOUNT.charge, Charge::Failure);
        assert_eq!(
            EMAIL_ADDRESS.default,
            Rate::new(30, Duration::from_secs(3600))
        );
    }
}
