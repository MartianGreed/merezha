//! Every statement identity runs, registered with the kernel's statement
//! registry (TE §1.3 "PostgreSQL"): spans and metrics carry the name and the
//! summary, never the SQL, parameters or values. [`STATEMENTS`] lists them.
use merezha_postgres::Statement;

macro_rules! user_columns {
    () => {
        "u.id, u.email, u.email_normalized, u.email_verified_at, u.display_name, u.locale, \
         u.status, u.created_at, p.phc"
    };
}
macro_rules! session_columns {
    () => {
        "s.id, s.user_id, s.transport, s.method, s.created_at, s.authenticated_at, \
         s.last_seen_at, s.idle_expires_at, s.absolute_expires_at, s.ip, s.user_agent"
    };
}
macro_rules! user_from {
    () => {
        " FROM merezha_identity.users u \
         LEFT JOIN merezha_identity.password_credentials p ON p.user_id = u.id "
    };
}

// ----- rate limits -----

pub const RATE_READ: Statement = Statement::new(
    "identity.rate_limits.read",
    "SELECT merezha_identity.rate_limits",
    "SELECT tat FROM merezha_identity.rate_limits WHERE bucket = $1",
);
pub const RATE_READ_MANY: Statement = Statement::new(
    "identity.rate_limits.read_many",
    "SELECT merezha_identity.rate_limits",
    "SELECT bucket, tat FROM merezha_identity.rate_limits WHERE bucket = ANY($1)",
);
pub const RATE_CHARGE: Statement = Statement::new(
    "identity.rate_limits.charge",
    "INSERT merezha_identity.rate_limits",
    "INSERT INTO merezha_identity.rate_limits AS r (bucket, tat)
     VALUES ($1, $2::timestamptz + $3::int8 * interval '1 microsecond')
     ON CONFLICT (bucket) DO UPDATE
        SET tat = greatest(r.tat, $2::timestamptz) + $3::int8 * interval '1 microsecond'
      WHERE greatest(r.tat, $2::timestamptz) + $3::int8 * interval '1 microsecond'
            <= $2::timestamptz + $4::int8 * interval '1 microsecond'
     RETURNING tat",
);

// ----- audit -----

pub const AUDIT_INSERT: Statement = Statement::new(
    "identity.audit_events.insert",
    "INSERT merezha_identity.audit_events",
    "INSERT INTO merezha_identity.audit_events
        (id, occurred_at, action, outcome, actor_kind, actor_id, target_kind, target_id,
         subject_hmac, ip, user_agent, request_id, details)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
);
pub const AUDIT_LIST: Statement = Statement::new(
    "identity.audit_events.list",
    "SELECT merezha_identity.audit_events",
    "SELECT id, occurred_at, action, outcome, ip, user_agent, details
       FROM merezha_identity.audit_events
      WHERE (actor_id = $1 OR target_id = $1) AND ($2::uuid IS NULL OR id < $2)
      ORDER BY id DESC LIMIT $3",
);

// ----- users -----

pub const USER_BY_EMAIL: Statement = Statement::new(
    "identity.users.by_email",
    "SELECT merezha_identity.users",
    concat!(
        "SELECT ",
        user_columns!(),
        user_from!(),
        "WHERE u.email_normalized = $1"
    ),
);
pub const USER_BY_EMAIL_LOCKED: Statement = Statement::new(
    "identity.users.by_email_locked",
    "SELECT merezha_identity.users",
    concat!(
        "SELECT ",
        user_columns!(),
        user_from!(),
        "WHERE u.email_normalized = $1 FOR UPDATE OF u"
    ),
);
pub const USER_BY_ID: Statement = Statement::new(
    "identity.users.by_id",
    "SELECT merezha_identity.users",
    concat!("SELECT ", user_columns!(), user_from!(), "WHERE u.id = $1"),
);
pub const USER_BY_ID_LOCKED: Statement = Statement::new(
    "identity.users.by_id_locked",
    "SELECT merezha_identity.users",
    concat!(
        "SELECT ",
        user_columns!(),
        user_from!(),
        "WHERE u.id = $1 FOR UPDATE OF u"
    ),
);
pub const USER_INSERT: Statement = Statement::new(
    "identity.users.insert",
    "INSERT merezha_identity.users",
    "INSERT INTO merezha_identity.users
        (id, email, email_normalized, email_verified_at, display_name, created_at, updated_at)
     VALUES ($1, $2, $3, $4, $5, $6, $6)
     ON CONFLICT (email_normalized) DO NOTHING RETURNING id",
);
pub const USER_BUMP: Statement = Statement::new(
    "identity.users.bump",
    "UPDATE merezha_identity.users",
    "UPDATE merezha_identity.users SET updated_at = $2, authz_version = authz_version + 1
      WHERE id = $1",
);
pub const USER_VERIFY: Statement = Statement::new(
    "identity.users.verify",
    "UPDATE merezha_identity.users",
    "UPDATE merezha_identity.users SET email_verified_at = $2, updated_at = $2 WHERE id = $1",
);
pub const USER_SIGNED_IN: Statement = Statement::new(
    "identity.users.signed_in",
    "UPDATE merezha_identity.users",
    "UPDATE merezha_identity.users SET last_signed_in_at = $2 WHERE id = $1",
);
pub const USER_PROFILE: Statement = Statement::new(
    "identity.users.profile",
    "UPDATE merezha_identity.users",
    "UPDATE merezha_identity.users SET
        display_name = CASE WHEN $2 THEN $3 ELSE display_name END,
        locale = CASE WHEN $4 THEN $5 ELSE locale END,
        updated_at = $6
      WHERE id = $1",
);
pub const USER_EMAIL_TAKEN: Statement = Statement::new(
    "identity.users.email_taken",
    "SELECT merezha_identity.users",
    "SELECT 1 FROM merezha_identity.users WHERE email_normalized = $1 AND id <> $2",
);
pub const USER_SET_EMAIL: Statement = Statement::new(
    "identity.users.set_email",
    "UPDATE merezha_identity.users",
    "UPDATE merezha_identity.users
        SET email = $2, email_normalized = $3, email_verified_at = $4, updated_at = $4
      WHERE id = $1",
);
pub const USER_DELETE: Statement = Statement::new(
    "identity.users.delete",
    "DELETE merezha_identity.users",
    "DELETE FROM merezha_identity.users WHERE id = $1",
);
pub const USER_UNVERIFIED_LOCK: Statement = Statement::new(
    "identity.users.unverified_lock",
    "SELECT merezha_identity.users",
    "SELECT id FROM merezha_identity.users
      WHERE email_verified_at IS NULL AND last_signed_in_at IS NULL AND created_at < $1
      ORDER BY created_at LIMIT $2
      FOR UPDATE SKIP LOCKED",
);
pub const USER_UNVERIFIED_DELETE: Statement = Statement::new(
    "identity.users.unverified_delete",
    "DELETE merezha_identity.users",
    "DELETE FROM merezha_identity.users
      WHERE id = ANY($1) AND email_verified_at IS NULL
        AND last_signed_in_at IS NULL AND created_at < $2
     RETURNING id",
);

// ----- passwords -----

pub const PASSWORD_SET: Statement = Statement::new(
    "identity.password_credentials.set",
    "INSERT merezha_identity.password_credentials",
    "INSERT INTO merezha_identity.password_credentials (user_id, phc, set_at)
     VALUES ($1, $2, $3)
     ON CONFLICT (user_id) DO UPDATE SET phc = EXCLUDED.phc, set_at = EXCLUDED.set_at",
);
pub const PASSWORD_DELETE: Statement = Statement::new(
    "identity.password_credentials.delete",
    "DELETE merezha_identity.password_credentials",
    "DELETE FROM merezha_identity.password_credentials WHERE user_id = $1",
);

// ----- one-time tokens -----

pub const TOKEN_UPSERT: Statement = Statement::new(
    "identity.tokens.issue",
    "INSERT merezha_identity.one_time_tokens",
    "INSERT INTO merezha_identity.one_time_tokens
        (id, purpose, token_hash, subject_key, user_id, email, email_normalized, return_to,
         binding_hash, session_id, created_at, expires_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
     ON CONFLICT (purpose, subject_key) DO UPDATE
        SET id = EXCLUDED.id, token_hash = EXCLUDED.token_hash, user_id = EXCLUDED.user_id,
            email = EXCLUDED.email, email_normalized = EXCLUDED.email_normalized,
            return_to = EXCLUDED.return_to, binding_hash = EXCLUDED.binding_hash,
            session_id = EXCLUDED.session_id, created_at = EXCLUDED.created_at,
            expires_at = EXCLUDED.expires_at",
);
pub const TOKEN_LOCK_OWNER: Statement = Statement::new(
    "identity.tokens.lock_owner",
    "SELECT merezha_identity.users",
    "SELECT 1 FROM merezha_identity.users u
       JOIN merezha_identity.one_time_tokens t
         ON u.id = t.user_id OR (t.user_id IS NULL AND u.email_normalized = t.email_normalized)
      WHERE t.token_hash = $1 AND t.purpose = $2
        FOR UPDATE OF u",
);
pub const TOKEN_PEEK: Statement = Statement::new(
    "identity.tokens.peek",
    "SELECT merezha_identity.one_time_tokens",
    "SELECT email, expires_at, session_id FROM merezha_identity.one_time_tokens
      WHERE token_hash = $1 AND purpose = $2",
);
pub const TOKEN_CONSUME: Statement = Statement::new(
    "identity.tokens.consume",
    "DELETE merezha_identity.one_time_tokens",
    "DELETE FROM merezha_identity.one_time_tokens
      WHERE token_hash = $1 AND purpose = $2
     RETURNING id, subject_key, user_id, email, email_normalized, return_to, expires_at,
               binding_hash, session_id",
);
pub const TOKENS_OF_USER_DELETE: Statement = Statement::new(
    "identity.tokens.delete_of_user",
    "DELETE merezha_identity.one_time_tokens",
    "DELETE FROM merezha_identity.one_time_tokens WHERE user_id = $1",
);

// ----- sessions -----

pub const SESSION_INSERT: Statement = Statement::new(
    "identity.sessions.insert",
    "INSERT merezha_identity.sessions",
    "INSERT INTO merezha_identity.sessions
        (id, token_hash, user_id, transport, method, created_at, authenticated_at,
         last_seen_at, idle_expires_at, absolute_expires_at, ip, user_agent)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $7, $8, $9, $10, $11)",
);
pub const SESSION_EVICT: Statement = Statement::new(
    "identity.sessions.evict",
    "DELETE merezha_identity.sessions",
    "DELETE FROM merezha_identity.sessions WHERE id IN (
        SELECT id FROM merezha_identity.sessions WHERE user_id = $1
         ORDER BY last_seen_at DESC, id DESC OFFSET $2)",
);
pub const SESSION_TAKE: Statement = Statement::new(
    "identity.sessions.take",
    "DELETE merezha_identity.sessions",
    concat!(
        "DELETE FROM merezha_identity.sessions s WHERE s.id = $1 RETURNING ",
        session_columns!()
    ),
);
pub const SESSION_BY_ID: Statement = Statement::new(
    "identity.sessions.by_id",
    "SELECT merezha_identity.sessions",
    concat!(
        "SELECT ",
        session_columns!(),
        " FROM merezha_identity.sessions s WHERE s.id = $1"
    ),
);
pub const SESSION_LIST: Statement = Statement::new(
    "identity.sessions.list",
    "SELECT merezha_identity.sessions",
    concat!(
        "SELECT ",
        session_columns!(),
        " FROM merezha_identity.sessions s
          WHERE s.user_id = $1 AND s.idle_expires_at > $2 AND s.absolute_expires_at > $2
          ORDER BY s.last_seen_at DESC, s.id DESC LIMIT $3"
    ),
);
pub const SESSION_AUTHENTICATE: Statement = Statement::new(
    "identity.sessions.authenticate",
    "SELECT merezha_identity.sessions",
    concat!(
        "SELECT ",
        session_columns!(),
        ", u.status, u.email_verified_at
           FROM merezha_identity.sessions s
           JOIN merezha_identity.users u ON u.id = s.user_id
          WHERE s.token_hash = $1"
    ),
);
pub const SESSION_TOUCH: Statement = Statement::new(
    "identity.sessions.touch",
    "UPDATE merezha_identity.sessions",
    "UPDATE merezha_identity.sessions SET last_seen_at = $2, idle_expires_at = $3
      WHERE id = $1 AND last_seen_at = $4",
);
pub const SESSION_DELETE_OWN: Statement = Statement::new(
    "identity.sessions.delete_own",
    "DELETE merezha_identity.sessions",
    "DELETE FROM merezha_identity.sessions WHERE id = $1 AND user_id = $2",
);
pub const SESSIONS_OF_USER_DELETE: Statement = Statement::new(
    "identity.sessions.delete_of_user",
    "DELETE merezha_identity.sessions",
    "DELETE FROM merezha_identity.sessions
      WHERE user_id = $1 AND ($2::uuid IS NULL OR id <> $2)",
);
pub const SESSIONS_ACTIVE: Statement = Statement::new(
    "identity.sessions.count_active",
    "SELECT merezha_identity.sessions",
    "SELECT count(*) FROM merezha_identity.sessions
      WHERE idle_expires_at > $1 AND absolute_expires_at > $1",
);

// ----- cleanup -----

pub const CLEANUP_SESSIONS: Statement = Statement::new(
    "identity.cleanup.sessions",
    "DELETE merezha_identity.sessions",
    "DELETE FROM merezha_identity.sessions WHERE id IN (
        SELECT id FROM merezha_identity.sessions
         WHERE idle_expires_at <= $1 OR absolute_expires_at <= $1 LIMIT $2)",
);
pub const CLEANUP_TOKENS: Statement = Statement::new(
    "identity.cleanup.tokens",
    "DELETE merezha_identity.one_time_tokens",
    "DELETE FROM merezha_identity.one_time_tokens WHERE id IN (
        SELECT id FROM merezha_identity.one_time_tokens WHERE expires_at <= $1 LIMIT $2)",
);
pub const CLEANUP_BUCKETS: Statement = Statement::new(
    "identity.cleanup.rate_limits",
    "DELETE merezha_identity.rate_limits",
    "DELETE FROM merezha_identity.rate_limits WHERE bucket IN (
        SELECT bucket FROM merezha_identity.rate_limits WHERE tat < $1 LIMIT $2)",
);
pub const CLEANUP_AUDIT: Statement = Statement::new(
    "identity.cleanup.audit_events",
    "DELETE merezha_identity.audit_events",
    "DELETE FROM merezha_identity.audit_events WHERE id IN (
        SELECT id FROM merezha_identity.audit_events WHERE occurred_at < $1 LIMIT $2)",
);

/// Every identity statement, for the registry check.
pub const STATEMENTS: &[Statement] = &[
    RATE_READ,
    RATE_READ_MANY,
    RATE_CHARGE,
    AUDIT_INSERT,
    AUDIT_LIST,
    USER_BY_EMAIL,
    USER_BY_EMAIL_LOCKED,
    USER_BY_ID,
    USER_BY_ID_LOCKED,
    USER_INSERT,
    USER_BUMP,
    USER_VERIFY,
    USER_SIGNED_IN,
    USER_PROFILE,
    USER_EMAIL_TAKEN,
    USER_SET_EMAIL,
    USER_DELETE,
    USER_UNVERIFIED_LOCK,
    USER_UNVERIFIED_DELETE,
    PASSWORD_SET,
    PASSWORD_DELETE,
    TOKEN_UPSERT,
    TOKEN_LOCK_OWNER,
    TOKEN_PEEK,
    TOKEN_CONSUME,
    TOKENS_OF_USER_DELETE,
    SESSION_INSERT,
    SESSION_EVICT,
    SESSION_TAKE,
    SESSION_BY_ID,
    SESSION_LIST,
    SESSION_AUTHENTICATE,
    SESSION_TOUCH,
    SESSION_DELETE_OWN,
    SESSIONS_OF_USER_DELETE,
    SESSIONS_ACTIVE,
    CLEANUP_SESSIONS,
    CLEANUP_TOKENS,
    CLEANUP_BUCKETS,
    CLEANUP_AUDIT,
];

#[cfg(test)]
mod tests {
    #[test]
    fn the_statements_are_registered_beside_the_kernel() {
        assert_eq!(
            merezha_postgres::check_registry(&[
                merezha_postgres::KERNEL_STATEMENTS,
                super::STATEMENTS
            ]),
            Ok(())
        );
    }
    /// No source of the block hands SQL to the driver.
    #[test]
    fn the_block_hands_no_sql_to_the_driver() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let found = merezha_postgres::statement::scan_sources(&dir, &[]).unwrap();
        assert!(found.is_empty(), "unregistered SQL: {found:?}");
    }
}
