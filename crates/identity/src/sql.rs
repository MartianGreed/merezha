//! Every statement identity runs, registered with the kernel's statement
//! registry (TE §1.3 "PostgreSQL"): spans and metrics carry the name and the
//! summary, never the SQL, parameters or values. [`STATEMENTS`] lists them.
use merezha_postgres::Statement;

macro_rules! user_columns {
    () => {
        "u.id, u.email, u.email_normalized, u.email_verified_at, u.display_name, u.locale, \
         u.status, u.created_at, p.phc, \
         (SELECT count(*) FROM merezha_identity.passkeys k WHERE k.user_id = u.id), \
         ARRAY(SELECT o.provider FROM merezha_identity.oauth_identities o \
                WHERE o.user_id = u.id ORDER BY o.provider)"
    };
}
macro_rules! session_columns {
    () => {
        "s.id, s.user_id, s.transport, s.method, s.created_at, s.authenticated_at, \
         s.last_seen_at, s.idle_expires_at, s.absolute_expires_at, s.ip, s.user_agent, \
         s.active_organization_id"
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
         subject_hmac, ip, user_agent, request_id, details, organization_id)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
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
        (id, email, email_normalized, email_verified_at, display_name, created_at, updated_at,
         binding_hash)
     VALUES ($1, $2, $3, $4, $5, $6, $6, $7)
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
    "UPDATE merezha_identity.users SET email_verified_at = $2, updated_at = $2, binding_hash = NULL
      WHERE id = $1",
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
pub const USER_BINDING: Statement = Statement::new(
    "identity.users.binding",
    "SELECT merezha_identity.users",
    "SELECT binding_hash FROM merezha_identity.users WHERE id = $1",
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
        AND NOT EXISTS (SELECT 1 FROM merezha_identity.memberships m WHERE m.user_id = users.id)
        AND NOT EXISTS (SELECT 1 FROM merezha_identity.application_role_assignments a
                         WHERE a.user_id = users.id)
      ORDER BY created_at LIMIT $2
      FOR UPDATE SKIP LOCKED",
);
pub const USER_UNVERIFIED_DELETE: Statement = Statement::new(
    "identity.users.unverified_delete",
    "DELETE merezha_identity.users",
    "DELETE FROM merezha_identity.users
      WHERE id = ANY($1) AND email_verified_at IS NULL
        AND last_signed_in_at IS NULL AND created_at < $2
        AND NOT EXISTS (SELECT 1 FROM merezha_identity.memberships m WHERE m.user_id = users.id)
        AND NOT EXISTS (SELECT 1 FROM merezha_identity.application_role_assignments a
                         WHERE a.user_id = users.id)
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
         session_id, created_at, expires_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
     ON CONFLICT (purpose, subject_key) DO UPDATE
        SET id = EXCLUDED.id, token_hash = EXCLUDED.token_hash, user_id = EXCLUDED.user_id,
            email = EXCLUDED.email, email_normalized = EXCLUDED.email_normalized,
            return_to = EXCLUDED.return_to,
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
               session_id",
);
pub const TOKEN_REBIND: Statement = Statement::new(
    "identity.tokens.rebind",
    "UPDATE merezha_identity.one_time_tokens",
    "UPDATE merezha_identity.one_time_tokens SET session_id = $2
      WHERE session_id = $1 AND purpose = 'email_change'",
);
pub const TOKENS_OF_USER_DELETE_KEEPING: Statement = Statement::new(
    "identity.tokens.delete_of_user_keeping",
    "DELETE merezha_identity.one_time_tokens",
    "DELETE FROM merezha_identity.one_time_tokens
      WHERE user_id = $1 AND NOT (purpose = 'email_change' AND session_id = $2)",
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
/// The session, share-locked: a concurrent revocation (sign-out, revoke
/// all, the ownership claim) waits for the caller's transaction, and the
/// caller sees a revocation that committed first.
pub const SESSION_HOLD: Statement = Statement::new(
    "identity.sessions.hold",
    "SELECT merezha_identity.sessions",
    concat!(
        "SELECT ",
        session_columns!(),
        " FROM merezha_identity.sessions s WHERE s.id = $1 FOR SHARE"
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
/// The session, its user and the organization `$2` names, or else the
/// session's active one, with the user's membership there (section 3).
pub const SESSION_AUTHENTICATE: Statement = Statement::new(
    "identity.sessions.authenticate",
    "SELECT merezha_identity.sessions",
    concat!(
        "SELECT ",
        session_columns!(),
        ", u.status, u.email_verified_at, u.authz_version, o.id, o.authz_version, m.id
           FROM merezha_identity.sessions s
           JOIN merezha_identity.users u ON u.id = s.user_id
           LEFT JOIN merezha_identity.organizations o
                  ON o.id = coalesce($2::uuid, s.active_organization_id)
           LEFT JOIN merezha_identity.memberships m
                  ON m.organization_id = o.id AND m.user_id = u.id
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

pub const SESSION_SET_ORGANIZATION: Statement = Statement::new(
    "identity.sessions.set_organization",
    "UPDATE merezha_identity.sessions",
    "UPDATE merezha_identity.sessions SET active_organization_id = $2 WHERE id = $1",
);
pub const SESSIONS_LEAVE_ORGANIZATION: Statement = Statement::new(
    "identity.sessions.leave_organization",
    "UPDATE merezha_identity.sessions",
    "UPDATE merezha_identity.sessions SET active_organization_id = NULL
      WHERE user_id = $1 AND active_organization_id = $2",
);

// ----- grants (section 5) -----

/// A user's version and status, and its membership in organization `$2`.
pub const USER_IN_ORGANIZATION: Statement = Statement::new(
    "identity.users.in_organization",
    "SELECT merezha_identity.users",
    "SELECT u.authz_version, u.status, o.id, o.authz_version, m.id
       FROM merezha_identity.users u
       LEFT JOIN merezha_identity.organizations o ON o.id = $2
       LEFT JOIN merezha_identity.memberships m ON m.organization_id = o.id AND m.user_id = u.id
      WHERE u.id = $1",
);
pub const GRANTS_APPLICATION_USER: Statement = Statement::new(
    "identity.grants.application_user",
    "SELECT merezha_identity.application_role_assignments",
    "SELECT r.key, a.resource_kind, a.resource_id
       FROM merezha_identity.application_role_assignments a
       JOIN merezha_identity.roles r ON r.id = a.role_id
      WHERE a.user_id = $1
      ORDER BY a.resource_kind NULLS FIRST, a.id LIMIT $2",
);
pub const GRANTS_APPLICATION_SERVICE: Statement = Statement::new(
    "identity.grants.application_service_account",
    "SELECT merezha_identity.application_role_assignments",
    "SELECT r.key, a.resource_kind, a.resource_id
       FROM merezha_identity.application_role_assignments a
       JOIN merezha_identity.roles r ON r.id = a.role_id
      WHERE a.service_account_id = $1
      ORDER BY a.resource_kind NULLS FIRST, a.id LIMIT $2",
);
/// Tenant grants of a membership (`$1`) or a tenant service account (`$2`)
/// in organization `$3`. The custom role join includes the organization,
/// so a role of another organization never contributes.
pub const GRANTS_TENANT: Statement = Statement::new(
    "identity.grants.tenant",
    "SELECT merezha_identity.role_assignments",
    "SELECT coalesce(a.builtin_role, r.key), a.custom_role_id IS NOT NULL, r.permissions,
            a.resource_kind, a.resource_id
       FROM merezha_identity.role_assignments a
       LEFT JOIN merezha_identity.roles r
              ON r.id = a.custom_role_id AND r.organization_id = a.organization_id
      WHERE a.organization_id = $3
        AND (a.membership_id = $1 OR a.service_account_id = $2)
      ORDER BY a.resource_kind NULLS FIRST, a.id LIMIT $4",
);

// ----- API keys -----

macro_rules! key_authenticate {
    ($filter:literal) => {
        concat!(
            "SELECT k.id, k.secret_hash, k.organization_id, k.user_id, k.service_account_id,
                    k.permissions, k.expires_at, k.revoked_at, k.last_used_at,
                    u.status, u.email_verified_at, u.authz_version,
                    sa.status, sa.authz_version, sa.organization_id,
                    o.id, o.authz_version, m.id,
                    k.created_by, mu.status, mu.email_verified_at, mu.authz_version, mm.id
               FROM merezha_identity.api_keys k
               LEFT JOIN merezha_identity.users u ON u.id = k.user_id
               LEFT JOIN merezha_identity.users mu ON mu.id = k.created_by
               LEFT JOIN merezha_identity.memberships mm
                      ON mm.organization_id = k.organization_id AND mm.user_id = k.created_by
               LEFT JOIN merezha_identity.service_accounts sa ON sa.id = k.service_account_id
               LEFT JOIN merezha_identity.organizations o
                      ON o.id = coalesce(k.organization_id, $2::uuid)
               LEFT JOIN merezha_identity.memberships m
                      ON m.organization_id = o.id AND m.user_id = k.user_id
              WHERE ",
            $filter
        )
    };
}
/// A key by public id with its owner, organization and membership (the
/// key's own organization, or `$2` for an application key), and its
/// minter's status, version and membership in the key's organization.
pub const KEY_AUTHENTICATE: Statement = Statement::new(
    "identity.api_keys.authenticate",
    "SELECT merezha_identity.api_keys",
    key_authenticate!("k.public_id = $1"),
);
/// [`KEY_AUTHENTICATE`] by key id, for re-checking an inviting key.
pub const KEY_RESOLVE: Statement = Statement::new(
    "identity.api_keys.resolve",
    "SELECT merezha_identity.api_keys",
    key_authenticate!("k.id = $1::uuid"),
);
/// Revoke every key user `$1` minted, anywhere (account deletion and
/// disabling).
pub const KEYS_REVOKE_MINTED_ALL: Statement = Statement::new(
    "identity.api_keys.revoke_minted_all",
    "UPDATE merezha_identity.api_keys",
    "UPDATE merezha_identity.api_keys SET revoked_at = $2
      WHERE created_by = $1 AND revoked_at IS NULL
     RETURNING id, organization_id",
);
/// Revoke the service account keys user `$1` minted in organization `$2`.
pub const KEYS_REVOKE_MINTED: Statement = Statement::new(
    "identity.api_keys.revoke_minted",
    "UPDATE merezha_identity.api_keys",
    "UPDATE merezha_identity.api_keys SET revoked_at = $3
      WHERE created_by = $1 AND organization_id = $2 AND service_account_id IS NOT NULL
        AND revoked_at IS NULL
     RETURNING id",
);
pub const KEY_TOUCH: Statement = Statement::new(
    "identity.api_keys.touch",
    "UPDATE merezha_identity.api_keys",
    "UPDATE merezha_identity.api_keys SET last_used_at = $2
      WHERE id = $1 AND (last_used_at IS NULL OR last_used_at <= $3)",
);
pub const KEY_INSERT: Statement = Statement::new(
    "identity.api_keys.insert",
    "INSERT merezha_identity.api_keys",
    "INSERT INTO merezha_identity.api_keys
        (id, public_id, secret_hash, organization_id, user_id, service_account_id, name,
         permissions, created_by, created_at, expires_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
);
macro_rules! key_columns {
    () => {
        "k.id, k.organization_id, k.user_id, k.service_account_id, k.name, k.permissions, \
         k.created_by, k.created_at, k.expires_at, k.last_used_at, k.revoked_at"
    };
}
/// Keys of one scope (`$1` organization, NULL for the application), all or
/// those of user `$2` or service account `$3`, newest first.
pub const KEYS_LIST: Statement = Statement::new(
    "identity.api_keys.list",
    "SELECT merezha_identity.api_keys",
    concat!(
        "SELECT ",
        key_columns!(),
        " FROM merezha_identity.api_keys k
          WHERE k.organization_id IS NOT DISTINCT FROM $1
            AND ($2::uuid IS NULL OR k.user_id = $2)
            AND ($3::uuid IS NULL OR k.service_account_id = $3)
          ORDER BY k.id DESC LIMIT $4"
    ),
);
pub const KEY_GET: Statement = Statement::new(
    "identity.api_keys.get",
    "SELECT merezha_identity.api_keys",
    concat!(
        "SELECT ",
        key_columns!(),
        " FROM merezha_identity.api_keys k
          WHERE k.id = $1 AND k.organization_id IS NOT DISTINCT FROM $2"
    ),
);
pub const KEY_REVOKE: Statement = Statement::new(
    "identity.api_keys.revoke",
    "UPDATE merezha_identity.api_keys",
    "UPDATE merezha_identity.api_keys SET revoked_at = $2 WHERE id = $1 AND revoked_at IS NULL",
);
/// Unrevoked, unexpired keys of one scope.
pub const KEYS_COUNT: Statement = Statement::new(
    "identity.api_keys.count",
    "SELECT merezha_identity.api_keys",
    "SELECT count(*) FROM merezha_identity.api_keys
      WHERE organization_id IS NOT DISTINCT FROM $1 AND revoked_at IS NULL AND expires_at > $2",
);
/// Revoke user `$1`'s keys for organization `$2` (membership removal).
pub const KEYS_REVOKE_MEMBER: Statement = Statement::new(
    "identity.api_keys.revoke_member",
    "UPDATE merezha_identity.api_keys",
    "UPDATE merezha_identity.api_keys SET revoked_at = $3
      WHERE user_id = $1 AND organization_id = $2 AND revoked_at IS NULL
     RETURNING id",
);
pub const KEYS_DELETE_OF_USER: Statement = Statement::new(
    "identity.api_keys.delete_of_user",
    "DELETE merezha_identity.api_keys",
    "DELETE FROM merezha_identity.api_keys WHERE user_id = $1",
);

// ----- organizations -----

macro_rules! org_columns {
    () => {
        "o.id, o.slug, o.name, o.created_by, o.created_at, o.updated_at"
    };
}
pub const ORG_INSERT: Statement = Statement::new(
    "identity.organizations.insert",
    "INSERT merezha_identity.organizations",
    "INSERT INTO merezha_identity.organizations (id, slug, name, created_by, created_at, updated_at)
     VALUES ($1, $2, $3, $4, $5, $5)
     ON CONFLICT (slug) DO NOTHING RETURNING id",
);
pub const ORG_GET: Statement = Statement::new(
    "identity.organizations.get",
    "SELECT merezha_identity.organizations",
    concat!(
        "SELECT ",
        org_columns!(),
        " FROM merezha_identity.organizations o WHERE o.id = $1"
    ),
);
/// The organization row, locked: every write to an organization's
/// memberships, roles, assignments, invitations and keys takes it first,
/// which serializes owner changes (section 2).
pub const ORG_LOCK: Statement = Statement::new(
    "identity.organizations.lock",
    "SELECT merezha_identity.organizations",
    concat!(
        "SELECT ",
        org_columns!(),
        " FROM merezha_identity.organizations o WHERE o.id = $1 FOR UPDATE"
    ),
);
pub const ORG_UPDATE: Statement = Statement::new(
    "identity.organizations.update",
    "UPDATE merezha_identity.organizations",
    "UPDATE merezha_identity.organizations
        SET name = coalesce($2, name), slug = coalesce($3, slug), updated_at = $4
      WHERE id = $1",
);
pub const ORG_SLUG_TAKEN: Statement = Statement::new(
    "identity.organizations.slug_taken",
    "SELECT merezha_identity.organizations",
    "SELECT 1 FROM merezha_identity.organizations WHERE slug = $1 AND id <> $2",
);
pub const ORG_DELETE: Statement = Statement::new(
    "identity.organizations.delete",
    "DELETE merezha_identity.organizations",
    "DELETE FROM merezha_identity.organizations WHERE id = $1",
);
/// A custom role definition changed: snapshots of this organization
/// rebuild on the next request (section 5).
pub const ORG_BUMP: Statement = Statement::new(
    "identity.organizations.bump",
    "UPDATE merezha_identity.organizations",
    "UPDATE merezha_identity.organizations SET authz_version = authz_version + 1, updated_at = $2
      WHERE id = $1",
);
pub const ORG_COUNT_CREATED_BY: Statement = Statement::new(
    "identity.organizations.count_created_by",
    "SELECT merezha_identity.organizations",
    "SELECT count(*) FROM merezha_identity.organizations WHERE created_by = $1",
);
/// The organizations of user `$1` with the membership, by organization id.
pub const ORGS_OF_USER: Statement = Statement::new(
    "identity.organizations.of_user",
    "SELECT merezha_identity.organizations",
    concat!(
        "SELECT ",
        org_columns!(),
        ", m.id, m.created_at
           FROM merezha_identity.memberships m
           JOIN merezha_identity.organizations o ON o.id = m.organization_id
          WHERE m.user_id = $1 AND ($2::uuid IS NULL OR o.id > $2)
          ORDER BY o.id LIMIT $3"
    ),
);
/// Every member's version, before a change that affects all of them.
pub const USERS_BUMP_OF_ORG: Statement = Statement::new(
    "identity.users.bump_of_organization",
    "UPDATE merezha_identity.users",
    "UPDATE merezha_identity.users SET authz_version = authz_version + 1
      WHERE id IN (SELECT user_id FROM merezha_identity.memberships WHERE organization_id = $1)",
);

// ----- memberships -----

pub const MEMBERSHIP_INSERT: Statement = Statement::new(
    "identity.memberships.insert",
    "INSERT merezha_identity.memberships",
    "INSERT INTO merezha_identity.memberships (id, organization_id, user_id, created_at)
     VALUES ($1, $2, $3, $4)
     ON CONFLICT (organization_id, user_id) DO NOTHING RETURNING id",
);
macro_rules! member_columns {
    () => {
        "m.id, m.organization_id, m.user_id, m.created_at, u.email, u.display_name"
    };
}
pub const MEMBERSHIP_GET: Statement = Statement::new(
    "identity.memberships.get",
    "SELECT merezha_identity.memberships",
    concat!(
        "SELECT ",
        member_columns!(),
        " FROM merezha_identity.memberships m
          JOIN merezha_identity.users u ON u.id = m.user_id
         WHERE m.id = $1 AND m.organization_id = $2"
    ),
);
pub const MEMBERSHIP_OF_USER: Statement = Statement::new(
    "identity.memberships.of_user",
    "SELECT merezha_identity.memberships",
    "SELECT id FROM merezha_identity.memberships WHERE organization_id = $1 AND user_id = $2",
);
pub const MEMBERSHIP_DELETE: Statement = Statement::new(
    "identity.memberships.delete",
    "DELETE merezha_identity.memberships",
    "DELETE FROM merezha_identity.memberships WHERE id = $1 AND organization_id = $2",
);
pub const MEMBERS_LIST: Statement = Statement::new(
    "identity.memberships.list",
    "SELECT merezha_identity.memberships",
    concat!(
        "SELECT ",
        member_columns!(),
        " FROM merezha_identity.memberships m
          JOIN merezha_identity.users u ON u.id = m.user_id
         WHERE m.organization_id = $1 AND ($2::uuid IS NULL OR m.id > $2)
         ORDER BY m.id LIMIT $3"
    ),
);
pub const MEMBERS_COUNT: Statement = Statement::new(
    "identity.memberships.count",
    "SELECT merezha_identity.memberships",
    "SELECT count(*) FROM merezha_identity.memberships WHERE organization_id = $1",
);
/// The memberships of user `$1`, for `identity.membership_removed` on
/// account deletion.
pub const MEMBERSHIPS_OF_USER: Statement = Statement::new(
    "identity.memberships.list_of_user",
    "SELECT merezha_identity.memberships",
    "SELECT id, organization_id FROM merezha_identity.memberships WHERE user_id = $1 ORDER BY id",
);
/// The organizations user `$1` owns, locked in id order, so concurrent
/// owner changes serialize.
pub const ORGS_OWNED_LOCK: Statement = Statement::new(
    "identity.organizations.owned_lock",
    "SELECT merezha_identity.organizations",
    "SELECT o.id FROM merezha_identity.organizations o
      WHERE o.id IN (SELECT a.organization_id FROM merezha_identity.role_assignments a
                       JOIN merezha_identity.memberships m ON m.id = a.membership_id
                      WHERE m.user_id = $1 AND a.builtin_role = 'owner'
                        AND a.resource_kind IS NULL)
      ORDER BY o.id FOR UPDATE OF o",
);
/// Members other than user `$2` holding `owner` at organization scope in
/// organization `$1`.
/// Whether user `$2` holds `owner` at organization scope in `$1` now.
pub const USER_IS_OWNER: Statement = Statement::new(
    "identity.role_assignments.user_is_owner",
    "SELECT merezha_identity.role_assignments",
    "SELECT EXISTS (SELECT 1 FROM merezha_identity.role_assignments a
                      JOIN merezha_identity.memberships m ON m.id = a.membership_id
                     WHERE a.organization_id = $1 AND m.user_id = $2
                       AND a.builtin_role = 'owner' AND a.resource_kind IS NULL)",
);
/// Whether user `$1`, active, holds `platform_admin` now.
pub const USER_IS_PLATFORM_ADMIN: Statement = Statement::new(
    "identity.application_role_assignments.user_is_platform_admin",
    "SELECT merezha_identity.application_role_assignments",
    "SELECT EXISTS (SELECT 1 FROM merezha_identity.application_role_assignments a
                      JOIN merezha_identity.users u ON u.id = a.user_id
                     WHERE a.user_id = $1 AND u.status = 'active'
                       AND a.role_id = '00000000-0000-7000-8000-000000000004')",
);
pub const OTHER_OWNERS: Statement = Statement::new(
    "identity.role_assignments.other_owners",
    "SELECT merezha_identity.role_assignments",
    "SELECT count(DISTINCT m.user_id) FROM merezha_identity.role_assignments a
       JOIN merezha_identity.memberships m ON m.id = a.membership_id
      WHERE a.organization_id = $1 AND a.builtin_role = 'owner' AND a.resource_kind IS NULL
        AND m.user_id <> $2",
);

// ----- roles -----

macro_rules! role_columns {
    () => {
        "r.id, r.scope, r.organization_id, r.key, r.name, r.description, r.permissions, \
         r.created_at, r.updated_at"
    };
}
/// A role an organization may use: a built-in tenant role or one of its
/// own custom roles.
pub const ROLE_IN_ORG: Statement = Statement::new(
    "identity.roles.in_organization",
    "SELECT merezha_identity.roles",
    concat!(
        "SELECT ",
        role_columns!(),
        " FROM merezha_identity.roles r
          WHERE r.id = $1 AND r.scope = 'tenant'
            AND (r.organization_id = $2 OR r.organization_id IS NULL)"
    ),
);
/// An application role: built-in or declared.
pub const ROLE_APPLICATION: Statement = Statement::new(
    "identity.roles.application",
    "SELECT merezha_identity.roles",
    concat!(
        "SELECT ",
        role_columns!(),
        " FROM merezha_identity.roles r WHERE r.id = $1 AND r.scope = 'application'"
    ),
);
pub const ROLES_OF_ORG: Statement = Statement::new(
    "identity.roles.list_tenant",
    "SELECT merezha_identity.roles",
    concat!(
        "SELECT ",
        role_columns!(),
        " FROM merezha_identity.roles r
          WHERE r.scope = 'tenant' AND (r.organization_id = $1 OR r.organization_id IS NULL)
          ORDER BY r.organization_id NULLS FIRST, r.key LIMIT 1000"
    ),
);
pub const ROLES_OF_APPLICATION: Statement = Statement::new(
    "identity.roles.list_application",
    "SELECT merezha_identity.roles",
    concat!(
        "SELECT ",
        role_columns!(),
        " FROM merezha_identity.roles r WHERE r.scope = 'application' ORDER BY r.key LIMIT 1000"
    ),
);
pub const ROLE_INSERT: Statement = Statement::new(
    "identity.roles.insert",
    "INSERT merezha_identity.roles",
    "INSERT INTO merezha_identity.roles
        (id, scope, organization_id, key, name, description, permissions, created_at, updated_at)
     VALUES ($1, 'tenant', $2, $3, $4, $5, $6, $7, $7)
     ON CONFLICT (organization_id, key) WHERE organization_id IS NOT NULL DO NOTHING
     RETURNING id",
);
pub const ROLE_UPDATE: Statement = Statement::new(
    "identity.roles.update",
    "UPDATE merezha_identity.roles",
    "UPDATE merezha_identity.roles
        SET name = coalesce($3, name),
            description = CASE WHEN $4 THEN $5 ELSE description END,
            permissions = coalesce($6, permissions), updated_at = $7
      WHERE id = $1 AND organization_id = $2",
);
pub const ROLE_DELETE: Statement = Statement::new(
    "identity.roles.delete",
    "DELETE merezha_identity.roles",
    "DELETE FROM merezha_identity.roles WHERE id = $1 AND organization_id = $2",
);
pub const ROLES_COUNT: Statement = Statement::new(
    "identity.roles.count_custom",
    "SELECT merezha_identity.roles",
    "SELECT count(*) FROM merezha_identity.roles WHERE organization_id = $1",
);
/// Assignments and pending invitations that use custom role `$1`.
pub const ROLE_USES: Statement = Statement::new(
    "identity.roles.uses",
    "SELECT merezha_identity.role_assignments",
    "SELECT (SELECT count(*) FROM merezha_identity.role_assignments WHERE custom_role_id = $1)
          + (SELECT count(*) FROM merezha_identity.invitation_roles WHERE custom_role_id = $1)",
);
/// Serializes the declared role sync of instances starting together.
pub const ROLES_SYNC_LOCK: Statement = Statement::new(
    "identity.roles.sync_lock",
    "SELECT pg_advisory_xact_lock",
    "SELECT pg_advisory_xact_lock(hashtextextended('merezha_identity.roles.sync', 0))",
);
pub const ROLE_UPSERT_DECLARED: Statement = Statement::new(
    "identity.roles.upsert_declared",
    "INSERT merezha_identity.roles",
    "INSERT INTO merezha_identity.roles
        (id, scope, organization_id, key, name, description, created_at, updated_at)
     VALUES ($1, 'application', NULL, $2, $3, $4, $5, $5)
     ON CONFLICT (key) WHERE organization_id IS NULL DO UPDATE
        SET name = EXCLUDED.name, description = EXCLUDED.description,
            updated_at = EXCLUDED.updated_at
      WHERE merezha_identity.roles.scope = 'application'
     RETURNING id",
);
/// Declared roles no longer configured, with their assignment counts.
pub const ROLES_UNDECLARED: Statement = Statement::new(
    "identity.roles.undeclared",
    "SELECT merezha_identity.roles",
    "SELECT r.id, r.key,
            (SELECT count(*) FROM merezha_identity.application_role_assignments a
              WHERE a.role_id = r.id)
       FROM merezha_identity.roles r
      WHERE r.scope = 'application' AND r.organization_id IS NULL
        AND r.id <> '00000000-0000-7000-8000-000000000004' AND NOT (r.key = ANY($1))
      ORDER BY r.key",
);
pub const ROLE_DELETE_DECLARED: Statement = Statement::new(
    "identity.roles.delete_declared",
    "DELETE merezha_identity.roles",
    "DELETE FROM merezha_identity.roles
      WHERE id = $1 AND scope = 'application' AND organization_id IS NULL",
);
/// Stored custom role grants that name nothing in catalog `$1`.
pub const ROLES_UNKNOWN_GRANTS: Statement = Statement::new(
    "identity.roles.unknown_grants",
    "SELECT merezha_identity.roles",
    "SELECT count(*) FROM merezha_identity.roles r, unnest(r.permissions) g
      WHERE r.organization_id IS NOT NULL
        AND NOT (g = ANY($1::text[])
                 OR (right(g, 2) = '.*' AND EXISTS (
                        SELECT 1 FROM unnest($1::text[]) c WHERE starts_with(c, left(g, -1)))))",
);

// ----- tenant role assignments -----

macro_rules! assignment_columns {
    () => {
        "a.id, a.membership_id, a.service_account_id, a.builtin_role, a.custom_role_id, \
         coalesce(a.builtin_role, r.key), a.resource_kind, a.resource_id, a.created_by, \
         a.created_at, m.user_id"
    };
}
macro_rules! assignment_from {
    () => {
        " FROM merezha_identity.role_assignments a
          LEFT JOIN merezha_identity.roles r
                 ON r.id = a.custom_role_id AND r.organization_id = a.organization_id
          LEFT JOIN merezha_identity.memberships m ON m.id = a.membership_id "
    };
}
pub const ASSIGNMENT_INSERT: Statement = Statement::new(
    "identity.role_assignments.insert",
    "INSERT merezha_identity.role_assignments",
    "INSERT INTO merezha_identity.role_assignments
        (id, organization_id, membership_id, service_account_id, builtin_role, custom_role_id,
         resource_kind, resource_id, created_by, created_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
     ON CONFLICT DO NOTHING RETURNING id",
);
pub const ASSIGNMENT_GET: Statement = Statement::new(
    "identity.role_assignments.get",
    "SELECT merezha_identity.role_assignments",
    concat!(
        "SELECT ",
        assignment_columns!(),
        assignment_from!(),
        "WHERE a.id = $1 AND a.organization_id = $2"
    ),
);
pub const ASSIGNMENT_DELETE: Statement = Statement::new(
    "identity.role_assignments.delete",
    "DELETE merezha_identity.role_assignments",
    "DELETE FROM merezha_identity.role_assignments WHERE id = $1 AND organization_id = $2",
);
pub const ASSIGNMENTS_LIST: Statement = Statement::new(
    "identity.role_assignments.list",
    "SELECT merezha_identity.role_assignments",
    concat!(
        "SELECT ",
        assignment_columns!(),
        assignment_from!(),
        "WHERE a.organization_id = $1
           AND ($2::uuid IS NULL OR a.membership_id = $2)
           AND ($3::uuid IS NULL OR a.service_account_id = $3)
           AND ($4::text IS NULL OR a.resource_kind = $4)
           AND ($5::text IS NULL OR a.resource_id = $5)
           AND ($6::uuid IS NULL OR a.id > $6)
         ORDER BY a.id LIMIT $7"
    ),
);
/// Organization-scope assignments of one holder, for member listings and
/// role replacement.
pub const ASSIGNMENTS_OF_HOLDER: Statement = Statement::new(
    "identity.role_assignments.of_holder",
    "SELECT merezha_identity.role_assignments",
    concat!(
        "SELECT ",
        assignment_columns!(),
        assignment_from!(),
        "WHERE a.organization_id = $1 AND (a.membership_id = $2 OR a.service_account_id = $3)
           AND a.resource_kind IS NULL
         ORDER BY a.id LIMIT 1000"
    ),
);
/// Organization-scope roles of the memberships in `$2`, for listings.
pub const ROLES_OF_MEMBERSHIPS: Statement = Statement::new(
    "identity.role_assignments.of_memberships",
    "SELECT merezha_identity.role_assignments",
    "SELECT a.membership_id, coalesce(a.custom_role_id, CASE a.builtin_role
                WHEN 'owner' THEN '00000000-0000-7000-8000-000000000001'::uuid
                WHEN 'admin' THEN '00000000-0000-7000-8000-000000000002'::uuid
                ELSE '00000000-0000-7000-8000-000000000003'::uuid END),
            coalesce(a.builtin_role, r.key)
       FROM merezha_identity.role_assignments a
       LEFT JOIN merezha_identity.roles r
              ON r.id = a.custom_role_id AND r.organization_id = a.organization_id
      WHERE a.organization_id = $1 AND a.membership_id = ANY($2) AND a.resource_kind IS NULL
      ORDER BY a.membership_id, a.id",
);
/// Resource assignments of one holder in an organization.
pub const ASSIGNMENTS_COUNT_HOLDER: Statement = Statement::new(
    "identity.role_assignments.count_holder",
    "SELECT merezha_identity.role_assignments",
    "SELECT count(*) FROM merezha_identity.role_assignments
      WHERE organization_id = $1 AND (membership_id = $2 OR service_account_id = $3)",
);

// ----- application role assignments -----

macro_rules! app_assignment_columns {
    () => {
        "a.id, a.user_id, a.service_account_id, a.role_id, r.key, a.resource_kind, \
         a.resource_id, a.created_by, a.created_at"
    };
}
pub const APP_ASSIGNMENT_INSERT: Statement = Statement::new(
    "identity.application_role_assignments.insert",
    "INSERT merezha_identity.application_role_assignments",
    "INSERT INTO merezha_identity.application_role_assignments
        (id, user_id, service_account_id, role_id, resource_kind, resource_id, created_by,
         created_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
     ON CONFLICT DO NOTHING RETURNING id",
);
pub const APP_ASSIGNMENT_GET: Statement = Statement::new(
    "identity.application_role_assignments.get",
    "SELECT merezha_identity.application_role_assignments",
    concat!(
        "SELECT ",
        app_assignment_columns!(),
        " FROM merezha_identity.application_role_assignments a
          JOIN merezha_identity.roles r ON r.id = a.role_id
         WHERE a.id = $1"
    ),
);
pub const APP_ASSIGNMENT_DELETE: Statement = Statement::new(
    "identity.application_role_assignments.delete",
    "DELETE merezha_identity.application_role_assignments",
    "DELETE FROM merezha_identity.application_role_assignments WHERE id = $1",
);
pub const APP_ASSIGNMENTS_LIST: Statement = Statement::new(
    "identity.application_role_assignments.list",
    "SELECT merezha_identity.application_role_assignments",
    concat!(
        "SELECT ",
        app_assignment_columns!(),
        " FROM merezha_identity.application_role_assignments a
          JOIN merezha_identity.roles r ON r.id = a.role_id
         WHERE ($1::uuid IS NULL OR a.user_id = $1)
           AND ($2::uuid IS NULL OR a.service_account_id = $2)
           AND ($3::uuid IS NULL OR a.role_id = $3)
           AND ($4::uuid IS NULL OR a.id > $4)
         ORDER BY a.id LIMIT $5"
    ),
);
pub const APP_ASSIGNMENTS_COUNT_HOLDER: Statement = Statement::new(
    "identity.application_role_assignments.count_holder",
    "SELECT merezha_identity.application_role_assignments",
    "SELECT count(*) FROM merezha_identity.application_role_assignments
      WHERE user_id = $1 OR service_account_id = $2",
);
/// The application role keys user `$1` holds at application scope.
pub const APP_ROLE_KEYS_OF_USER: Statement = Statement::new(
    "identity.application_role_assignments.keys_of_user",
    "SELECT merezha_identity.application_role_assignments",
    "SELECT DISTINCT r.key FROM merezha_identity.application_role_assignments a
       JOIN merezha_identity.roles r ON r.id = a.role_id
      WHERE a.user_id = $1 AND a.resource_kind IS NULL ORDER BY r.key",
);
pub const PLATFORM_ADMIN_GRANT: Statement = Statement::new(
    "identity.application_role_assignments.grant_platform_admin",
    "INSERT merezha_identity.application_role_assignments",
    "INSERT INTO merezha_identity.application_role_assignments
        (id, user_id, role_id, created_at)
     VALUES ($1, $2, '00000000-0000-7000-8000-000000000004', $3)
     ON CONFLICT DO NOTHING RETURNING id",
);
pub const PLATFORM_ADMIN_REVOKE: Statement = Statement::new(
    "identity.application_role_assignments.revoke_platform_admin",
    "DELETE merezha_identity.application_role_assignments",
    "DELETE FROM merezha_identity.application_role_assignments
      WHERE user_id = $1 AND role_id = '00000000-0000-7000-8000-000000000004'
     RETURNING id",
);

// ----- service accounts -----

macro_rules! service_columns {
    () => {
        "s.id, s.scope, s.organization_id, s.name, s.description, s.status, s.created_by, \
         s.created_at, s.updated_at"
    };
}
pub const SERVICE_INSERT: Statement = Statement::new(
    "identity.service_accounts.insert",
    "INSERT merezha_identity.service_accounts",
    "INSERT INTO merezha_identity.service_accounts
        (id, scope, organization_id, name, description, created_by, created_at, updated_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $7)",
);
/// A service account of one scope (`$2` organization, NULL for the
/// application), locked.
pub const SERVICE_LOCK: Statement = Statement::new(
    "identity.service_accounts.lock",
    "SELECT merezha_identity.service_accounts",
    concat!(
        "SELECT ",
        service_columns!(),
        " FROM merezha_identity.service_accounts s
          WHERE s.id = $1 AND s.organization_id IS NOT DISTINCT FROM $2 FOR UPDATE"
    ),
);
pub const SERVICE_UPDATE: Statement = Statement::new(
    "identity.service_accounts.update",
    "UPDATE merezha_identity.service_accounts",
    "UPDATE merezha_identity.service_accounts
        SET name = coalesce($2, name),
            description = CASE WHEN $3 THEN $4 ELSE description END,
            status = coalesce($5, status),
            authz_version = authz_version + CASE WHEN $5 IS NULL OR $5 = status THEN 0 ELSE 1 END,
            updated_at = $6
      WHERE id = $1",
);
pub const SERVICE_BUMP: Statement = Statement::new(
    "identity.service_accounts.bump",
    "UPDATE merezha_identity.service_accounts",
    "UPDATE merezha_identity.service_accounts SET authz_version = authz_version + 1 WHERE id = $1",
);
pub const SERVICE_DELETE: Statement = Statement::new(
    "identity.service_accounts.delete",
    "DELETE merezha_identity.service_accounts",
    "DELETE FROM merezha_identity.service_accounts WHERE id = $1",
);
pub const SERVICES_LIST: Statement = Statement::new(
    "identity.service_accounts.list",
    "SELECT merezha_identity.service_accounts",
    concat!(
        "SELECT ",
        service_columns!(),
        " FROM merezha_identity.service_accounts s
          WHERE s.organization_id IS NOT DISTINCT FROM $1 AND ($2::uuid IS NULL OR s.id > $2)
          ORDER BY s.id LIMIT $3"
    ),
);
pub const SERVICES_COUNT: Statement = Statement::new(
    "identity.service_accounts.count",
    "SELECT merezha_identity.service_accounts",
    "SELECT count(*) FROM merezha_identity.service_accounts
      WHERE organization_id IS NOT DISTINCT FROM $1",
);

// ----- invitations -----

pub const INVITATION_DELETE_FOR_EMAIL: Statement = Statement::new(
    "identity.invitations.delete_for_email",
    "DELETE merezha_identity.invitations",
    "DELETE FROM merezha_identity.invitations WHERE organization_id = $1 AND email_normalized = $2",
);
pub const INVITATION_INSERT: Statement = Statement::new(
    "identity.invitations.insert",
    "INSERT merezha_identity.invitations",
    "INSERT INTO merezha_identity.invitations
        (id, organization_id, email, email_normalized, token_hash, invited_by, created_at,
         expires_at, invited_by_key)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
);
pub const INVITATION_ROLE_INSERT: Statement = Statement::new(
    "identity.invitation_roles.insert",
    "INSERT merezha_identity.invitation_roles",
    "INSERT INTO merezha_identity.invitation_roles
        (invitation_id, organization_id, builtin_role, custom_role_id)
     VALUES ($1, $2, $3, $4)
     ON CONFLICT DO NOTHING",
);
macro_rules! invitation_columns {
    () => {
        "i.id, i.organization_id, i.email, i.email_normalized, i.invited_by, i.created_at, \
         i.expires_at, \
         ARRAY(SELECT coalesce(x.custom_role_id, CASE x.builtin_role \
                  WHEN 'owner' THEN '00000000-0000-7000-8000-000000000001'::uuid \
                  WHEN 'admin' THEN '00000000-0000-7000-8000-000000000002'::uuid \
                  ELSE '00000000-0000-7000-8000-000000000003'::uuid END) \
                 FROM merezha_identity.invitation_roles x WHERE x.invitation_id = i.id), \
         i.invited_by_key"
    };
}
pub const INVITATIONS_LIST: Statement = Statement::new(
    "identity.invitations.list",
    "SELECT merezha_identity.invitations",
    concat!(
        "SELECT ",
        invitation_columns!(),
        " FROM merezha_identity.invitations i
          WHERE i.organization_id = $1 AND i.expires_at > $2
          ORDER BY i.id LIMIT 1000"
    ),
);
/// Pending invitations user `$2` sent in organization `$1`, deleted.
pub const INVITATIONS_DELETE_BY_INVITER: Statement = Statement::new(
    "identity.invitations.delete_by_inviter",
    "DELETE merezha_identity.invitations",
    "DELETE FROM merezha_identity.invitations WHERE organization_id = $1 AND invited_by = $2
     RETURNING id",
);
pub const INVITATION_DELETE: Statement = Statement::new(
    "identity.invitations.delete",
    "DELETE merezha_identity.invitations",
    "DELETE FROM merezha_identity.invitations WHERE id = $1 AND organization_id = $2",
);
/// An invitation by token digest, with its organization and inviter.
pub const INVITATION_BY_TOKEN: Statement = Statement::new(
    "identity.invitations.by_token",
    "SELECT merezha_identity.invitations",
    concat!(
        "SELECT ",
        invitation_columns!(),
        ", o.name, o.slug, u.display_name
           FROM merezha_identity.invitations i
           JOIN merezha_identity.organizations o ON o.id = i.organization_id
           LEFT JOIN merezha_identity.users u ON u.id = i.invited_by
          WHERE i.token_hash = $1"
    ),
);
/// Consume an invitation (single use).
pub const INVITATION_CONSUME: Statement = Statement::new(
    "identity.invitations.consume",
    "DELETE merezha_identity.invitations",
    "DELETE FROM merezha_identity.invitations WHERE id = $1 AND token_hash = $2 RETURNING id",
);
/// The roles of an invitation, read before it is consumed.
pub const INVITATION_ROLES: Statement = Statement::new(
    "identity.invitation_roles.of_invitation",
    "SELECT merezha_identity.invitation_roles",
    "SELECT builtin_role, custom_role_id FROM merezha_identity.invitation_roles
      WHERE invitation_id = $1",
);

// ----- OAuth and OIDC (section 6) -----

pub const OAUTH_FLOW_INSERT: Statement = Statement::new(
    "identity.oauth_flows.insert",
    "INSERT merezha_identity.oauth_flows",
    "INSERT INTO merezha_identity.oauth_flows
        (state_hash, binding_hash, provider, intent, user_id, session_id, pkce_verifier, nonce,
         return_to, created_at, expires_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
);
/// Single use: of two callbacks with one `state`, one gets the row.
pub const OAUTH_FLOW_CONSUME: Statement = Statement::new(
    "identity.oauth_flows.consume",
    "DELETE merezha_identity.oauth_flows",
    "DELETE FROM merezha_identity.oauth_flows WHERE state_hash = $1
     RETURNING binding_hash, provider, intent, user_id, session_id, pkce_verifier, nonce,
               return_to, expires_at",
);
/// Flows still pending for a browser binding: the binding cookie is
/// cleared only once none is left, so two tabs both complete.
pub const OAUTH_FLOWS_OF_BINDING: Statement = Statement::new(
    "identity.oauth_flows.of_binding",
    "SELECT merezha_identity.oauth_flows",
    "SELECT count(*) FROM merezha_identity.oauth_flows WHERE binding_hash = $1 AND expires_at > $2",
);
pub const OAUTH_FLOWS_DELETE_OF_USER: Statement = Statement::new(
    "identity.oauth_flows.delete_of_user",
    "DELETE merezha_identity.oauth_flows",
    "DELETE FROM merezha_identity.oauth_flows WHERE user_id = $1",
);
pub const OAUTH_IDENTITY_BY_SUBJECT: Statement = Statement::new(
    "identity.oauth_identities.by_subject",
    "SELECT merezha_identity.oauth_identities",
    "SELECT id, user_id, issuer FROM merezha_identity.oauth_identities
      WHERE provider = $1 AND subject = $2",
);
/// `None` when `(provider, subject)` or `(user, provider)` is taken.
pub const OAUTH_IDENTITY_INSERT: Statement = Statement::new(
    "identity.oauth_identities.insert",
    "INSERT merezha_identity.oauth_identities",
    "INSERT INTO merezha_identity.oauth_identities
        (id, user_id, provider, subject, email_normalized, email_verified, created_at, last_used_at,
         issuer)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $7, $8)
     ON CONFLICT DO NOTHING RETURNING id",
);
/// Refresh the stored assertion of an identity of user `$6`.
pub const OAUTH_IDENTITY_TOUCH: Statement = Statement::new(
    "identity.oauth_identities.touch",
    "UPDATE merezha_identity.oauth_identities",
    "UPDATE merezha_identity.oauth_identities
        SET email_normalized = $3, email_verified = $4, last_used_at = $5
      WHERE provider = $1 AND subject = $2 AND user_id = $6 AND issuer = $7
     RETURNING id",
);
pub const OAUTH_IDENTITIES_OF_USER: Statement = Statement::new(
    "identity.oauth_identities.of_user",
    "SELECT merezha_identity.oauth_identities",
    "SELECT id, provider, email_normalized, created_at, last_used_at
       FROM merezha_identity.oauth_identities WHERE user_id = $1 ORDER BY provider",
);
pub const OAUTH_IDENTITY_DELETE: Statement = Statement::new(
    "identity.oauth_identities.delete",
    "DELETE merezha_identity.oauth_identities",
    "DELETE FROM merezha_identity.oauth_identities WHERE id = $1 AND user_id = $2
     RETURNING provider",
);
pub const OAUTH_IDENTITIES_DELETE_OF_USER: Statement = Statement::new(
    "identity.oauth_identities.delete_of_user",
    "DELETE merezha_identity.oauth_identities",
    "DELETE FROM merezha_identity.oauth_identities WHERE user_id = $1",
);
/// What can still sign user `$1` in once passkey `$2` or identity `$3`
/// is gone: its password, the other passkeys, the other identities'
/// providers and whether the address is verified (magic links).
pub const CREDENTIALS_OF_USER: Statement = Statement::new(
    "identity.users.credentials",
    "SELECT merezha_identity.users",
    "SELECT EXISTS (SELECT 1 FROM merezha_identity.password_credentials p WHERE p.user_id = u.id),
            (SELECT count(*) FROM merezha_identity.passkeys k
              WHERE k.user_id = u.id AND k.id IS DISTINCT FROM $2),
            ARRAY(SELECT o.provider FROM merezha_identity.oauth_identities o
                   WHERE o.user_id = u.id AND o.id IS DISTINCT FROM $3),
            u.email_verified_at IS NOT NULL
       FROM merezha_identity.users u WHERE u.id = $1",
);

// ----- passkeys (section 6) -----

macro_rules! passkey_columns {
    () => {
        "k.id, k.credential_id, k.passkey, k.label, k.aaguid, k.backup_eligible, k.backup_state, \
         k.discoverable, k.transports, k.revision, k.created_at, k.last_used_at, k.user_id"
    };
}
/// The WebAuthn user handle of user `$1`, assigned `$2` when absent.
pub const USER_WEBAUTHN_HANDLE: Statement = Statement::new(
    "identity.users.webauthn_handle",
    "UPDATE merezha_identity.users",
    "UPDATE merezha_identity.users SET webauthn_handle = coalesce(webauthn_handle, $2)
      WHERE id = $1 RETURNING webauthn_handle",
);
pub const PASSKEYS_OF_USER: Statement = Statement::new(
    "identity.passkeys.of_user",
    "SELECT merezha_identity.passkeys",
    concat!(
        "SELECT ",
        passkey_columns!(),
        " FROM merezha_identity.passkeys k WHERE k.user_id = $1 ORDER BY k.created_at, k.id LIMIT 100"
    ),
);
/// The passkey of a credential id, with its user's WebAuthn handle.
pub const PASSKEY_BY_CREDENTIAL: Statement = Statement::new(
    "identity.passkeys.by_credential",
    "SELECT merezha_identity.passkeys",
    concat!(
        "SELECT ",
        passkey_columns!(),
        ", u.webauthn_handle FROM merezha_identity.passkeys k
           JOIN merezha_identity.users u ON u.id = k.user_id WHERE k.credential_id = $1"
    ),
);
pub const PASSKEY_GET: Statement = Statement::new(
    "identity.passkeys.get",
    "SELECT merezha_identity.passkeys",
    concat!(
        "SELECT ",
        passkey_columns!(),
        " FROM merezha_identity.passkeys k WHERE k.id = $1 AND k.user_id = $2"
    ),
);
pub const PASSKEYS_COUNT: Statement = Statement::new(
    "identity.passkeys.count",
    "SELECT merezha_identity.passkeys",
    "SELECT count(*) FROM merezha_identity.passkeys WHERE user_id = $1",
);
/// `None` when the credential id is registered already (`passkey_exists`).
pub const PASSKEY_INSERT: Statement = Statement::new(
    "identity.passkeys.insert",
    "INSERT merezha_identity.passkeys",
    "INSERT INTO merezha_identity.passkeys
        (id, user_id, credential_id, passkey, label, aaguid, backup_eligible, backup_state,
         discoverable, transports, created_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
     ON CONFLICT (credential_id) DO NOTHING RETURNING id",
);
/// Compare-and-swap on `revision` after an authentication.
pub const PASSKEY_UPDATE: Statement = Statement::new(
    "identity.passkeys.update",
    "UPDATE merezha_identity.passkeys",
    "UPDATE merezha_identity.passkeys
        SET passkey = $3, backup_eligible = $4, backup_state = $5, last_used_at = $6,
            revision = revision + 1
      WHERE id = $1 AND revision = $2",
);
pub const PASSKEY_RENAME: Statement = Statement::new(
    "identity.passkeys.rename",
    "UPDATE merezha_identity.passkeys",
    "UPDATE merezha_identity.passkeys SET label = $3 WHERE id = $1 AND user_id = $2",
);
pub const PASSKEY_DELETE: Statement = Statement::new(
    "identity.passkeys.delete",
    "DELETE merezha_identity.passkeys",
    "DELETE FROM merezha_identity.passkeys WHERE id = $1 AND user_id = $2",
);
pub const PASSKEYS_DELETE_OF_USER: Statement = Statement::new(
    "identity.passkeys.delete_of_user",
    "DELETE merezha_identity.passkeys",
    "DELETE FROM merezha_identity.passkeys WHERE user_id = $1",
);
pub const CEREMONY_INSERT: Statement = Statement::new(
    "identity.webauthn_ceremonies.insert",
    "INSERT merezha_identity.webauthn_ceremonies",
    "INSERT INTO merezha_identity.webauthn_ceremonies
        (id, purpose, user_id, session_id, state, created_at, expires_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7)",
);
/// Single use: consumed by `DELETE ... RETURNING` whatever comes next.
pub const CEREMONY_CONSUME: Statement = Statement::new(
    "identity.webauthn_ceremonies.consume",
    "DELETE merezha_identity.webauthn_ceremonies",
    "DELETE FROM merezha_identity.webauthn_ceremonies WHERE id = $1 AND purpose = ANY($2)
     RETURNING purpose, user_id, session_id, state, expires_at",
);
pub const CEREMONIES_DELETE_OF_USER: Statement = Statement::new(
    "identity.webauthn_ceremonies.delete_of_user",
    "DELETE merezha_identity.webauthn_ceremonies",
    "DELETE FROM merezha_identity.webauthn_ceremonies WHERE user_id = $1",
);

// ----- users (application scope) -----

pub const USERS_LIST: Statement = Statement::new(
    "identity.users.list",
    "SELECT merezha_identity.users",
    concat!(
        "SELECT ",
        user_columns!(),
        user_from!(),
        "WHERE ($1::text IS NULL OR u.email_normalized = $1) AND ($2::uuid IS NULL OR u.id > $2)
         ORDER BY u.id LIMIT $3"
    ),
);
pub const USER_SET_STATUS: Statement = Statement::new(
    "identity.users.set_status",
    "UPDATE merezha_identity.users",
    "UPDATE merezha_identity.users SET status = $2, updated_at = $3,
            authz_version = authz_version + 1
      WHERE id = $1 AND status <> $2",
);

// ----- directory (R7) -----

pub const DIRECTORY_USERS: Statement = Statement::new(
    "identity.directory.users",
    "SELECT merezha_identity.users",
    "SELECT u.id, u.email, u.display_name, u.locale FROM merezha_identity.users u
      WHERE u.id = ANY($1) AND u.status = 'active' AND u.email_verified_at IS NOT NULL
      ORDER BY u.id",
);
/// Active verified members of `$1`, all (`$2` NULL) or holding one of the
/// organization-scope role keys `$2`, after `$3`.
pub const DIRECTORY_TENANT_MEMBERS: Statement = Statement::new(
    "identity.directory.tenant_members",
    "SELECT merezha_identity.memberships",
    "SELECT u.id, u.email, u.display_name, u.locale
       FROM merezha_identity.memberships m
       JOIN merezha_identity.users u ON u.id = m.user_id
      WHERE m.organization_id = $1 AND u.status = 'active' AND u.email_verified_at IS NOT NULL
        AND ($3::uuid IS NULL OR u.id > $3)
        AND ($2::text[] IS NULL OR EXISTS (
              SELECT 1 FROM merezha_identity.role_assignments a
                LEFT JOIN merezha_identity.roles r
                       ON r.id = a.custom_role_id AND r.organization_id = a.organization_id
               WHERE a.membership_id = m.id AND a.resource_kind IS NULL
                 AND coalesce(a.builtin_role, r.key) = ANY($2)))
      ORDER BY u.id LIMIT $4",
);
/// Every active verified user (`$1` NULL), or those holding one of the
/// application role keys `$1`, after `$2`.
pub const DIRECTORY_APPLICATION_MEMBERS: Statement = Statement::new(
    "identity.directory.application_members",
    "SELECT merezha_identity.users",
    "SELECT u.id, u.email, u.display_name, u.locale FROM merezha_identity.users u
      WHERE u.status = 'active' AND u.email_verified_at IS NOT NULL
        AND ($2::uuid IS NULL OR u.id > $2)
        AND ($1::text[] IS NULL OR EXISTS (
              SELECT 1 FROM merezha_identity.application_role_assignments a
                JOIN merezha_identity.roles r ON r.id = a.role_id
               WHERE a.user_id = u.id AND a.resource_kind IS NULL AND r.key = ANY($1)))
      ORDER BY u.id LIMIT $3",
);
/// A membership of an active user with a verified address, like the
/// other Directory calls.
pub const DIRECTORY_IS_MEMBER: Statement = Statement::new(
    "identity.directory.is_member",
    "SELECT merezha_identity.memberships",
    "SELECT EXISTS (SELECT 1 FROM merezha_identity.memberships m
                      JOIN merezha_identity.users u ON u.id = m.user_id
                     WHERE m.user_id = $1 AND m.organization_id = $2
                       AND u.status = 'active' AND u.email_verified_at IS NOT NULL)",
);
/// The users among `$1` that are members of `$2`, counted as
/// [`DIRECTORY_IS_MEMBER`] counts them.
pub const DIRECTORY_MEMBERS_AMONG: Statement = Statement::new(
    "identity.directory.members_among",
    "SELECT merezha_identity.memberships",
    "SELECT m.user_id FROM merezha_identity.memberships m
       JOIN merezha_identity.users u ON u.id = m.user_id
      WHERE m.user_id = ANY($1) AND m.organization_id = $2
        AND u.status = 'active' AND u.email_verified_at IS NOT NULL",
);

// ----- audit listings -----

pub const AUDIT_LIST_ORG: Statement = Statement::new(
    "identity.audit_events.list_organization",
    "SELECT merezha_identity.audit_events",
    "SELECT id, occurred_at, action, outcome, actor_kind, actor_id, organization_id,
            target_kind, target_id, details
       FROM merezha_identity.audit_events
      WHERE ($1::uuid IS NULL OR organization_id = $1)
        AND ($2::text IS NULL OR action = $2)
        AND ($3::uuid IS NULL OR actor_id = $3)
        AND ($4::uuid IS NULL OR id < $4)
      ORDER BY id DESC LIMIT $5",
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
pub const CLEANUP_INVITATIONS: Statement = Statement::new(
    "identity.cleanup.invitations",
    "DELETE merezha_identity.invitations",
    "DELETE FROM merezha_identity.invitations WHERE id IN (
        SELECT id FROM merezha_identity.invitations WHERE expires_at <= $1 LIMIT $2)",
);
/// Keys revoked, or expired, before `$1` (30 days ago): a use after
/// revocation is audited as such until then.
pub const CLEANUP_KEYS: Statement = Statement::new(
    "identity.cleanup.api_keys",
    "DELETE merezha_identity.api_keys",
    "DELETE FROM merezha_identity.api_keys WHERE id IN (
        SELECT id FROM merezha_identity.api_keys
         WHERE revoked_at < $1 OR expires_at < $1 LIMIT $2)",
);
pub const CLEANUP_OAUTH_FLOWS: Statement = Statement::new(
    "identity.cleanup.oauth_flows",
    "DELETE merezha_identity.oauth_flows",
    "DELETE FROM merezha_identity.oauth_flows WHERE state_hash IN (
        SELECT state_hash FROM merezha_identity.oauth_flows WHERE expires_at <= $1 LIMIT $2)",
);
pub const CLEANUP_CEREMONIES: Statement = Statement::new(
    "identity.cleanup.webauthn_ceremonies",
    "DELETE merezha_identity.webauthn_ceremonies",
    "DELETE FROM merezha_identity.webauthn_ceremonies WHERE id IN (
        SELECT id FROM merezha_identity.webauthn_ceremonies WHERE expires_at <= $1 LIMIT $2)",
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
    USER_BINDING,
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
    TOKEN_REBIND,
    TOKENS_OF_USER_DELETE_KEEPING,
    TOKENS_OF_USER_DELETE,
    SESSION_INSERT,
    SESSION_EVICT,
    SESSION_TAKE,
    SESSION_BY_ID,
    SESSION_HOLD,
    SESSION_LIST,
    SESSION_AUTHENTICATE,
    SESSION_TOUCH,
    SESSION_DELETE_OWN,
    SESSIONS_OF_USER_DELETE,
    SESSIONS_ACTIVE,
    SESSION_SET_ORGANIZATION,
    SESSIONS_LEAVE_ORGANIZATION,
    USER_IN_ORGANIZATION,
    GRANTS_APPLICATION_USER,
    GRANTS_APPLICATION_SERVICE,
    GRANTS_TENANT,
    KEY_AUTHENTICATE,
    KEY_TOUCH,
    KEY_INSERT,
    KEYS_LIST,
    KEY_GET,
    KEY_REVOKE,
    KEYS_COUNT,
    KEYS_REVOKE_MEMBER,
    KEYS_DELETE_OF_USER,
    ORG_INSERT,
    ORG_GET,
    ORG_LOCK,
    ORG_UPDATE,
    ORG_SLUG_TAKEN,
    ORG_DELETE,
    ORG_BUMP,
    ORG_COUNT_CREATED_BY,
    ORGS_OF_USER,
    USERS_BUMP_OF_ORG,
    MEMBERSHIP_INSERT,
    MEMBERSHIP_GET,
    MEMBERSHIP_OF_USER,
    MEMBERSHIP_DELETE,
    MEMBERS_LIST,
    MEMBERS_COUNT,
    MEMBERSHIPS_OF_USER,
    ORGS_OWNED_LOCK,
    OTHER_OWNERS,
    ROLE_IN_ORG,
    ROLE_APPLICATION,
    ROLES_OF_ORG,
    ROLES_OF_APPLICATION,
    ROLE_INSERT,
    ROLE_UPDATE,
    ROLE_DELETE,
    ROLES_COUNT,
    ROLE_USES,
    ROLES_SYNC_LOCK,
    ROLE_UPSERT_DECLARED,
    ROLES_UNDECLARED,
    ROLE_DELETE_DECLARED,
    ROLES_UNKNOWN_GRANTS,
    ASSIGNMENT_INSERT,
    ASSIGNMENT_GET,
    ASSIGNMENT_DELETE,
    ASSIGNMENTS_LIST,
    ASSIGNMENTS_OF_HOLDER,
    ROLES_OF_MEMBERSHIPS,
    ASSIGNMENTS_COUNT_HOLDER,
    APP_ASSIGNMENT_INSERT,
    APP_ASSIGNMENT_GET,
    APP_ASSIGNMENT_DELETE,
    APP_ASSIGNMENTS_LIST,
    APP_ASSIGNMENTS_COUNT_HOLDER,
    APP_ROLE_KEYS_OF_USER,
    PLATFORM_ADMIN_GRANT,
    PLATFORM_ADMIN_REVOKE,
    SERVICE_INSERT,
    SERVICE_LOCK,
    SERVICE_UPDATE,
    SERVICE_BUMP,
    SERVICE_DELETE,
    SERVICES_LIST,
    SERVICES_COUNT,
    INVITATION_DELETE_FOR_EMAIL,
    INVITATION_INSERT,
    INVITATION_ROLE_INSERT,
    INVITATIONS_LIST,
    INVITATION_DELETE,
    INVITATION_BY_TOKEN,
    INVITATION_CONSUME,
    INVITATION_ROLES,
    USERS_LIST,
    USER_SET_STATUS,
    DIRECTORY_USERS,
    DIRECTORY_TENANT_MEMBERS,
    DIRECTORY_APPLICATION_MEMBERS,
    DIRECTORY_IS_MEMBER,
    DIRECTORY_MEMBERS_AMONG,
    AUDIT_LIST_ORG,
    CLEANUP_SESSIONS,
    CLEANUP_TOKENS,
    CLEANUP_BUCKETS,
    CLEANUP_INVITATIONS,
    CLEANUP_KEYS,
    CLEANUP_AUDIT,
    OAUTH_FLOW_INSERT,
    OAUTH_FLOW_CONSUME,
    OAUTH_FLOWS_OF_BINDING,
    OAUTH_FLOWS_DELETE_OF_USER,
    OAUTH_IDENTITY_BY_SUBJECT,
    OAUTH_IDENTITY_INSERT,
    OAUTH_IDENTITY_TOUCH,
    OAUTH_IDENTITIES_OF_USER,
    OAUTH_IDENTITY_DELETE,
    OAUTH_IDENTITIES_DELETE_OF_USER,
    CREDENTIALS_OF_USER,
    USER_WEBAUTHN_HANDLE,
    PASSKEYS_OF_USER,
    PASSKEY_BY_CREDENTIAL,
    PASSKEY_GET,
    PASSKEYS_COUNT,
    PASSKEY_INSERT,
    PASSKEY_UPDATE,
    PASSKEY_RENAME,
    PASSKEY_DELETE,
    PASSKEYS_DELETE_OF_USER,
    CEREMONY_INSERT,
    CEREMONY_CONSUME,
    CEREMONIES_DELETE_OF_USER,
    CLEANUP_OAUTH_FLOWS,
    CLEANUP_CEREMONIES,
    KEY_RESOLVE,
    KEYS_REVOKE_MINTED,
    KEYS_REVOKE_MINTED_ALL,
    USER_IS_OWNER,
    USER_IS_PLATFORM_ADMIN,
    INVITATIONS_DELETE_BY_INVITER,
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
}
