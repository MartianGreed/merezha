# Identity block design

Status: proposed 2026-10-23, aligned 2026-10-24 with
[cross-block-decisions.md](cross-block-decisions.md) (R1 to R15, owner
answers ID1 to ID10), which wins where the two differ. Conforms to
[the framework architecture](../architecture/framework.md) with two accepted
amendments (R15): the loopback development cookie and the password policy.

The identity block is the crate `merezha-identity`, PostgreSQL schema
`merezha_identity`, routes under `/v1/identity/`, and the implementations of
the `Authenticator`, `Authorizer` and `Directory` ports. It ships in three
pull requests (section 10).

Standards referenced by short name below:

- ASVS: OWASP Application Security Verification Standard 5.0 (V6
  authentication, V7 session management, V8 authorization, V10 OAuth and OIDC,
  V11 cryptography, V16 security logging).
- 800-63B: NIST SP 800-63B-4 (2025), password verifiers, rate limiting,
  reauthentication.
- RFC 9700 (OAuth 2.0 security best current practice), RFC 7636 (PKCE),
  RFC 9106 (Argon2), OpenID Connect Core 1.0, WebAuthn Level 3, RFC 6265bis
  (cookie prefixes).

## 1. Scope and ubiquitous language

In scope: accounts with email and password, email verification, password
reset, magic links, server-side sessions (cookie and bearer), organizations,
memberships, invitations, RBAC with built-in and custom roles at application
and tenant scope, service accounts, API keys, OAuth 2.0 and OIDC social
sign-in (Google, GitHub, generic OIDC), WebAuthn passkeys, rate limits, audit
events, the `Directory` port.

Out of scope for this program: TOTP and other second factors (ID6), an OAuth
authorization server for agents and MCP clients (ID9, a later decision),
SAML, SCIM, native-app OAuth without a browser redirect, an admin UI,
passkey-only sign-up (ID7), phone numbers on users (ID10). Section 11 records
the owner's decisions.

| Term | Meaning |
| --- | --- |
| User | A person with an account in one application. Users are global to the application's database, not owned by an organization. Identified by `usr_` ids. |
| Email address | The single primary address of a user. Unique per application after normalization. Verified when the user proves control of the inbox, or a trusted provider asserts it (section 6). |
| Credential | Something that proves a user's identity at sign-in: a password, a passkey, an OAuth identity, or control of the verified email (through magic links). A user always keeps at least one. |
| Password credential | An Argon2id PHC string bound to one user. At most one per user. |
| OAuth identity | A link between a user and `(provider, subject)` at an external provider. `subject` is the provider's stable id, never the email or login name. |
| Passkey | A WebAuthn public-key credential registered by a user, with user verification required. |
| One-time token | A random 256-bit secret delivered by email for one purpose (email verification, magic link, password reset, email change). Stored as a SHA-256 digest. Single use, short-lived. |
| Magic link | A one-time token of purpose `magic_link` that signs the holder in (and can create the account). |
| Session | Server-side record of an authenticated browser or client. The client holds an opaque 256-bit token, in a cookie or as a bearer token. Has an idle and an absolute expiry. |
| Application | The whole deployed application: one database, one user base. The widest permission scope. Applications without organizations grant everything at this scope. |
| Organization | A tenant. Owns memberships, invitations, custom roles, tenant service accounts, tenant API keys and, in other blocks and the application, tenant data. Identified by `org_` ids and a unique slug. |
| Membership | The fact that a user belongs to an organization. Carries tenant role assignments. |
| Invitation | A pending offer of membership sent to an email address, with the roles to grant. Accepted with a one-time token. |
| Permission | A named action, such as `identity.members.invite` or `tasks.create`. The catalog is the union of block and application permissions. |
| Role | A named set of permissions with a scope. Application roles are built in (`platform_admin`, the implicit `user`) or declared in configuration. Tenant roles are built in (`owner`, `admin`, `member`) or custom to one organization. |
| Role assignment | `(holder, role, scope)`. At application scope the holder is a user or an application service account, and the scope is the application or one application-level resource (R5). At tenant scope it is a membership or a tenant service account, and the scope is the organization or one resource inside it. |
| Scope | Where a permission check applies: K's `ResourceScope`, one of `Application`, `Tenant(org)` or `Resource { tenant, kind, id }` (R5). |
| Service account | A non-human holder of role assignments, scoped to the application or to one organization. It has no password or session and acts only through its API keys. Survives staff changes. Identified by `svc_` ids. |
| API key | A machine credential owned by a user or a service account, pinned to the application or to one organization, with an explicit permission list. Its effective power is the intersection of that list and its owner's current grants at the key's scope, so a key never exceeds its owner. |
| Principal | The host-established answer to "who is calling": subject, kind (`user`, `api_key`, `system`), owner, tenant, a grant snapshot with roles and permissions per scope, session facts. Built by the `Authenticator`, read by the `Authorizer`, adopted by the kernel (R13) and passed to guests (protocol version 2). |
| Reauthentication | Proving a credential again inside an existing session. Sensitive operations require one within the reauthentication window. |
| Audit event | An append-only security record kept apart from diagnostic logs. |

## 2. Domain model and invariants

### Storage style: state-stored, not event-sourced

Identity state lives in ordinary PostgreSQL tables in `merezha_identity`. The
application core stays event-sourced in `merezha_app`; identity does not use
it. Reasons:

1. Erasure. Credentials, emails and IP addresses are personal data that must
   be deleted on account deletion. An immutable event log either keeps them
   forever or needs crypto-shredding. Rows can be deleted.
2. Uniqueness and atomic consumption are database constraints here. Email
   uniqueness, `(provider, subject)` uniqueness and single-use tokens map to
   unique indexes and `DELETE ... RETURNING`. Event-sourced equivalents need
   reservation streams.
3. Write shape. Session touches, rate-limit buckets and API key last-use
   stamps are high-frequency overwrites with no history value.
4. History is still kept where it matters. Security history goes to the
   audit table, and other blocks receive integration events through the
   outbox. That covers the reasons one would event-source.

The TypeScript `@structure-ai/auth-pg` made the same call. Keep it.

### Aggregates and plain records

| Unit | Kind | Consistency boundary and invariants |
| --- | --- | --- |
| User | Aggregate root over `users`, `password_credentials`, `passkeys`, `oauth_identities` | Normalized email unique. At least one usable credential after any removal. A disabled user has no live sessions and its API keys stop authenticating. Writes lock the `users` row (`SELECT ... FOR UPDATE`). |
| Organization | Aggregate root over `organizations`, `memberships`, `role_assignments`, custom `roles`, `invitations`, `invitation_roles`, tenant `service_accounts` | At least one member holds `owner` at organization scope. Slug unique. A tenant role assignment or invitation role names a built-in tenant role or a custom role of the same organization, and a holder of the same organization (composite foreign keys), so a custom role grants nothing outside its organization. An assignment cannot grant a permission its creator lacks. Writes lock the `organizations` row, which serializes owner changes. |
| Application grants | Records `application_role_assignments` and application `service_accounts` | An application assignment references an application role. A tenant service account never holds one, and `platform_admin` goes only to users (constraints in section 3). An assignment cannot grant a permission its creator lacks at application scope. |
| Session | Lifecycle record | Belongs to one user. Dead when idle or absolute expiry passes, when deleted, or when the user is disabled. Token digest unique. |
| One-time token | Lifecycle record | One live token per `(purpose, subject_key)`. A new request replaces the old one. Consumption deletes the row. |
| API key | Lifecycle record | One owner (user or service account) and one scope (application or one organization). A tenant service account's keys are pinned to its organization. A user's tenant key works only while its owner is a member there: membership removal revokes it, and authentication refuses it without one. Permissions are a subset of the catalog. Usable only while the owner is active. Its power shrinks with the owner's grants on the next request. |
| OAuth flow, WebAuthn ceremony | Ephemeral records | Single use, consumed by `DELETE ... RETURNING`, expire in minutes. |
| Rate-limit bucket | Counter | One GCRA timestamp per hashed key. |
| Audit event | Append-only record | Never updated. Deleted only by the retention job. |

Every use case runs in one transaction that also appends its audit event (for
successes), its integration events (`Outbox::append`, R1) and its
notification requests (`Notifier::request`, R2), all on the caller's
transaction. A notification request and the token it carries therefore commit
or roll back together.

### Identifiers

The database stores `uuid` values generated in Rust as UUIDv7. The API renders
them with a type prefix and the 32-character lowercase hex form, for example
`usr_0192f5c4e1b07a3c9d2e4f6a8b0c1d2e`. Prefixes: `usr`, `org`, `mem`, `inv`,
`rol`, `asg`, `svc`, `ses`, `key`, `pky`, `oid`. Every rendered id is a valid
`merezha_domain::Identity`, so `Principal.subject` can be the guest metadata
`actor`. Parsing rejects a wrong prefix with `not_found`, which keeps an id of
one type from being accepted where another is expected.

### Normalization rules

- Email: trim, Unicode NFC, split at the last `@`, convert the domain with
  IDNA to ASCII lowercase, lowercase the local part. Reject if the local part
  is over 64 bytes, the total over 254 bytes, either side is empty, or the
  domain has no dot. No provider-specific folding (no dot or `+tag` removal).
  `email` keeps the trimmed NFC input for display. `email_normalized` is the
  uniqueness key.
- Password: NFKC normalization (800-63B), length counted in Unicode scalar
  values, at most 1024 bytes after normalization, so Argon2 input is bounded.
- Display name: NFC, 1 to 128 scalar values, no control characters.
- Organization slug: `^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`, plus a
  reserved list (`api`, `admin`, `v1`, `identity`, `www`, `static`, `assets`,
  `health`, `me`, `new`).

### Account pre-hijacking rule

An attacker can register a victim's address with a password and never verify
it. When the real owner later proves control of that address through a magic
link, password reset, invitation, trusted OAuth provider, or a verification
link opened outside the browser that registered (below), the service claims
the unverified account. In one transaction it removes every
credential on it (password, passkeys, OAuth identities), deletes its sessions
and API keys, marks the email verified, then continues the flow that proved
ownership. It audits `user.unverified_account_claimed`. Unverified accounts
cannot sign in while `require_verified_email = true` (the default), so in the
default configuration only the password is ever present. The TypeScript
service marked such accounts verified and kept the attacker's password. That
is the main correctness fix carried over.

The verification link itself is mailed at registration, so the attacker's
registration sends the victim a "verify your email" link. Registration
therefore sets a binding cookie in the browser that asked: `__Host-merezha_verify` (`merezha_verify` with the loopback
development cookie), 32 random bytes, `HttpOnly`, `SameSite=Lax`, `Secure`,
`Max-Age` of `email_verification_ttl`; a browser that presents a well-formed
one keeps it. Its SHA-256 digest is stored on the new user row
(`users.binding_hash`, cleared once the address is verified), never on a
token, so a verification resend (`POST /email-verifications`), which neither
sets nor replaces the cookie, cannot make any other browser "the
registering one": a victim who resends from their own browser and clicks
there still claims the account. A confirmation that carries the matching
cookie verifies the account as it is: the password stays and the user is
signed in. Any other confirmation of an unverified account is an ownership
claim under this rule: the password and every other credential go, every
session is revoked, the address is verified and the clicker is signed in
with a session whose answer carries `"password_unset": true` (also visible as
`user.credentials.password = false`), so the client asks for a password
(`PUT /me/password` accepts the fresh session as recent). Both paths set the
cookie, so registration still answers identically for new and existing
addresses. Non-browser clients send the cookie back on confirmation.

For ID PR 3: an OAuth or OIDC sign-in that lands on an unverified account
(linking rule 3) always runs this ownership claim first; a provider
assertion never counts as the registering browser.

Only while `require_verified_email = true`, the cleanup job deletes
unverified accounts older than `unverified_account_ttl` (default 7 days)
that never signed in (`last_signed_in_at IS NULL`) and hold no membership or
application role, so never an organization owner. It locks candidates
`FOR UPDATE SKIP LOCKED`, then deletes in a second statement whose snapshot
postdates the locks and re-checks every condition; sign-in and every
attachment lock the user row first. Otherwise unverified accounts stay.

## 3. SQL schema (first migrations)

Three migrations, registered through the `merezha-postgres` runner as module
`identity`, versions 1 to 3, one per pull request. All DDL is additive. Times
are `timestamptz`. Digests are `bytea` of exactly 32 bytes. The floor is
PostgreSQL 14, with CI on 16 (R14). No extension is needed (UUIDs come from
Rust).

### Migration 1: `identity_core`

```sql
CREATE SCHEMA IF NOT EXISTS merezha_identity;

CREATE TABLE merezha_identity.users (
    id                 uuid PRIMARY KEY,
    email              text NOT NULL,
    email_normalized   text NOT NULL,
    email_verified_at  timestamptz,
    display_name       text,
    locale             text,
    status             text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled')),
    authz_version      bigint NOT NULL DEFAULT 0,
    last_signed_in_at  timestamptz,             -- set by every session creation
    created_at         timestamptz NOT NULL,
    updated_at         timestamptz NOT NULL,
    CONSTRAINT users_email_normalized_key UNIQUE (email_normalized),
    CHECK (octet_length(email_normalized) <= 254)
);
CREATE INDEX users_unverified_created_idx
    ON merezha_identity.users (created_at)
    WHERE email_verified_at IS NULL AND last_signed_in_at IS NULL;

CREATE TABLE merezha_identity.password_credentials (
    user_id     uuid PRIMARY KEY REFERENCES merezha_identity.users (id) ON DELETE CASCADE,
    phc         text NOT NULL,              -- $argon2id$v=19$m=...,t=...,p=...$salt$hash
    set_at      timestamptz NOT NULL
);

CREATE TABLE merezha_identity.sessions (
    id                   uuid PRIMARY KEY,
    token_hash           bytea NOT NULL CHECK (octet_length(token_hash) = 32),
    user_id              uuid NOT NULL REFERENCES merezha_identity.users (id) ON DELETE CASCADE,
    transport            text NOT NULL CHECK (transport IN ('cookie', 'bearer')),
    method               text NOT NULL CHECK (method IN ('password', 'magic_link', 'email_token', 'oauth', 'passkey', 'invitation')),
    created_at           timestamptz NOT NULL,
    authenticated_at     timestamptz NOT NULL,   -- last sign-in or reauthentication
    last_seen_at         timestamptz NOT NULL,
    idle_expires_at      timestamptz NOT NULL,
    absolute_expires_at  timestamptz NOT NULL,
    ip                   inet,
    user_agent           text CHECK (octet_length(user_agent) <= 512),
    CONSTRAINT sessions_token_hash_key UNIQUE (token_hash),
    CHECK (idle_expires_at <= absolute_expires_at)
);
CREATE INDEX sessions_user_idx ON merezha_identity.sessions (user_id, created_at);
CREATE INDEX sessions_idle_expiry_idx ON merezha_identity.sessions (idle_expires_at);

CREATE TABLE merezha_identity.one_time_tokens (
    id           uuid PRIMARY KEY,
    purpose      text NOT NULL CHECK (purpose IN ('email_verification', 'magic_link', 'password_reset', 'email_change')),
    token_hash   bytea NOT NULL CHECK (octet_length(token_hash) = 32),
    subject_key  text NOT NULL,      -- email_normalized, or user id for email_change
    user_id      uuid REFERENCES merezha_identity.users (id) ON DELETE CASCADE,
    email_normalized text NOT NULL,  -- the address the token was sent to
    return_to    text,               -- validated relative path (magic links)
    created_at   timestamptz NOT NULL,
    expires_at   timestamptz NOT NULL,
    CONSTRAINT one_time_tokens_hash_key UNIQUE (token_hash),
    CONSTRAINT one_time_tokens_subject_key UNIQUE (purpose, subject_key)
);
CREATE INDEX one_time_tokens_expiry_idx ON merezha_identity.one_time_tokens (expires_at);

CREATE TABLE merezha_identity.rate_limits (
    bucket  bytea PRIMARY KEY CHECK (octet_length(bucket) = 32),
    tat     timestamptz NOT NULL      -- GCRA theoretical arrival time
);
CREATE INDEX rate_limits_tat_idx ON merezha_identity.rate_limits (tat);

CREATE TABLE merezha_identity.audit_events (
    id               uuid PRIMARY KEY,           -- UUIDv7, time ordered
    occurred_at      timestamptz NOT NULL,
    action           text NOT NULL,              -- e.g. 'session.created'
    outcome          text NOT NULL CHECK (outcome IN ('succeeded', 'failed', 'denied')),
    actor_kind       text CHECK (actor_kind IN ('user', 'api_key', 'system', 'anonymous')),
    actor_id         uuid,
    organization_id  uuid,                       -- no FK: survives deletion
    target_kind      text,
    target_id        uuid,
    subject_hmac     bytea,                      -- HMAC of an email for unknown accounts
    ip               inet,
    user_agent       text CHECK (octet_length(user_agent) <= 512),
    request_id       text,
    details          jsonb NOT NULL DEFAULT '{}'::jsonb   -- allowlisted keys only
);
CREATE INDEX audit_events_org_idx ON merezha_identity.audit_events (organization_id, occurred_at DESC)
    WHERE organization_id IS NOT NULL;
CREATE INDEX audit_events_actor_idx ON merezha_identity.audit_events (actor_id, occurred_at DESC)
    WHERE actor_id IS NOT NULL;
CREATE INDEX audit_events_target_idx ON merezha_identity.audit_events (target_id, occurred_at DESC)
    WHERE target_id IS NOT NULL;
CREATE INDEX audit_events_time_idx ON merezha_identity.audit_events (occurred_at);

CREATE FUNCTION merezha_identity.audit_events_immutable() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'audit events are append-only';
END $$;
CREATE TRIGGER audit_events_no_update
    BEFORE UPDATE ON merezha_identity.audit_events
    FOR EACH ROW EXECUTE FUNCTION merezha_identity.audit_events_immutable();
```

PR 1 adds two things to this migration as written: `one_time_tokens.email`
(the trimmed NFC address the token was sent to, so a magic-link sign-up or
an email change keeps the address's display form) with a 512-byte bound on
`return_to`, and the indexes `sessions_absolute_expiry_idx` and
`one_time_tokens_user_idx` that the cleanup job and the per-user token
deletes read.

The retention job deletes audit rows by `occurred_at`, so the trigger blocks
`UPDATE` only. The trigger guards against application bugs, not against a
database superuser.

### Migration 2: `organizations_rbac_api_keys`

```sql
CREATE TABLE merezha_identity.organizations (
    id             uuid PRIMARY KEY,
    slug           text NOT NULL,
    name           text NOT NULL CHECK (char_length(name) BETWEEN 1 AND 128),
    authz_version  bigint NOT NULL DEFAULT 0,
    created_by     uuid,                     -- no FK: creator may be deleted
    created_at     timestamptz NOT NULL,
    updated_at     timestamptz NOT NULL,
    CONSTRAINT organizations_slug_key UNIQUE (slug)
);

CREATE TABLE merezha_identity.memberships (
    id               uuid PRIMARY KEY,
    organization_id  uuid NOT NULL REFERENCES merezha_identity.organizations (id) ON DELETE CASCADE,
    user_id          uuid NOT NULL REFERENCES merezha_identity.users (id) ON DELETE CASCADE,
    created_at       timestamptz NOT NULL,
    CONSTRAINT memberships_org_user_key UNIQUE (organization_id, user_id),
    CONSTRAINT memberships_id_org_key UNIQUE (id, organization_id)
);
CREATE INDEX memberships_user_idx ON merezha_identity.memberships (user_id);

CREATE TABLE merezha_identity.service_accounts (
    id               uuid PRIMARY KEY,
    scope            text NOT NULL CHECK (scope IN ('application', 'tenant')),
    organization_id  uuid REFERENCES merezha_identity.organizations (id) ON DELETE CASCADE,
    name             text NOT NULL CHECK (char_length(name) BETWEEN 1 AND 128),
    description      text CHECK (char_length(description) <= 1024),
    status           text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled')),
    authz_version    bigint NOT NULL DEFAULT 0,
    created_by       uuid,
    created_at       timestamptz NOT NULL,
    updated_at       timestamptz NOT NULL,
    CHECK ((scope = 'application') = (organization_id IS NULL)),
    CONSTRAINT service_accounts_id_scope_key UNIQUE (id, scope),
    CONSTRAINT service_accounts_id_org_key UNIQUE (id, organization_id)
);
CREATE INDEX service_accounts_org_idx ON merezha_identity.service_accounts (organization_id);

-- Built-in and declared roles have organization_id NULL and take their
-- permissions from code and configuration. Custom roles are tenant roles.
CREATE TABLE merezha_identity.roles (
    id               uuid PRIMARY KEY,
    scope            text NOT NULL CHECK (scope IN ('application', 'tenant')),
    organization_id  uuid REFERENCES merezha_identity.organizations (id) ON DELETE CASCADE,
    key              text NOT NULL CHECK (key ~ '^[a-z][a-z0-9_]{0,62}$'),
    name             text NOT NULL CHECK (char_length(name) BETWEEN 1 AND 128),
    description      text CHECK (char_length(description) <= 1024),
    permissions      text[] NOT NULL DEFAULT '{}',
    created_at       timestamptz NOT NULL,
    updated_at       timestamptz NOT NULL,
    CHECK (organization_id IS NOT NULL OR permissions = '{}'),
    CHECK (scope = 'tenant' OR organization_id IS NULL),
    CONSTRAINT roles_id_scope_key UNIQUE (id, scope),
    CONSTRAINT roles_id_org_key UNIQUE (id, organization_id)   -- target of custom role FKs
);
CREATE UNIQUE INDEX roles_shared_key ON merezha_identity.roles (key)
    WHERE organization_id IS NULL;
CREATE UNIQUE INDEX roles_org_key ON merezha_identity.roles (organization_id, key)
    WHERE organization_id IS NOT NULL;
INSERT INTO merezha_identity.roles (id, scope, organization_id, key, name, created_at, updated_at) VALUES
    ('00000000-0000-7000-8000-000000000001', 'tenant',      NULL, 'owner',          'Owner',          now(), now()),
    ('00000000-0000-7000-8000-000000000002', 'tenant',      NULL, 'admin',          'Admin',          now(), now()),
    ('00000000-0000-7000-8000-000000000003', 'tenant',      NULL, 'member',         'Member',         now(), now()),
    ('00000000-0000-7000-8000-000000000004', 'application', NULL, 'platform_admin', 'Platform admin', now(), now());

-- Tenant scope: the organization or one resource in it. The custom role FK
-- includes the NOT NULL organization_id, so it is always checked.
CREATE TABLE merezha_identity.role_assignments (
    id                  uuid PRIMARY KEY,
    organization_id     uuid NOT NULL REFERENCES merezha_identity.organizations (id) ON DELETE CASCADE,
    membership_id       uuid,
    service_account_id  uuid,
    builtin_role        text CHECK (builtin_role IN ('owner', 'admin', 'member')),
    custom_role_id      uuid,
    resource_kind       text CHECK (resource_kind ~ '^[a-z][a-z0-9_]{0,62}$'),
    resource_id         text CHECK (octet_length(resource_id) BETWEEN 1 AND 128),
    created_by          uuid,
    created_at          timestamptz NOT NULL,
    FOREIGN KEY (membership_id, organization_id)
        REFERENCES merezha_identity.memberships (id, organization_id) ON DELETE CASCADE,
    FOREIGN KEY (service_account_id, organization_id)
        REFERENCES merezha_identity.service_accounts (id, organization_id) ON DELETE CASCADE,
    FOREIGN KEY (custom_role_id, organization_id)
        REFERENCES merezha_identity.roles (id, organization_id),   -- NO ACTION: see below
    CHECK (num_nonnulls(membership_id, service_account_id) = 1),
    CHECK (num_nonnulls(builtin_role, custom_role_id) = 1),
    CHECK ((resource_kind IS NULL) = (resource_id IS NULL))
);
CREATE UNIQUE INDEX role_assignments_unique ON merezha_identity.role_assignments
    (coalesce(membership_id, service_account_id), coalesce(builtin_role, custom_role_id::text),
     coalesce(resource_kind, ''), coalesce(resource_id, ''));
CREATE INDEX role_assignments_org_role_idx ON merezha_identity.role_assignments (organization_id, builtin_role);
CREATE INDEX role_assignments_custom_role_idx ON merezha_identity.role_assignments (custom_role_id);
CREATE INDEX role_assignments_resource_idx ON merezha_identity.role_assignments
    (organization_id, resource_kind, resource_id) WHERE resource_kind IS NOT NULL;

-- Application scope: the whole application, every tenant included, or one
-- application-level resource (a resource without tenant, R5).
CREATE TABLE merezha_identity.application_role_assignments (
    id                     uuid PRIMARY KEY,
    user_id                uuid REFERENCES merezha_identity.users (id) ON DELETE CASCADE,
    service_account_id     uuid,
    service_account_scope  text NOT NULL DEFAULT 'application' CHECK (service_account_scope = 'application'),
    role_id                uuid NOT NULL,
    role_scope             text NOT NULL DEFAULT 'application' CHECK (role_scope = 'application'),
    resource_kind          text CHECK (resource_kind ~ '^[a-z][a-z0-9_]{0,62}$'),
    resource_id            text CHECK (octet_length(resource_id) BETWEEN 1 AND 128),
    created_by             uuid,
    created_at             timestamptz NOT NULL,
    FOREIGN KEY (service_account_id, service_account_scope)
        REFERENCES merezha_identity.service_accounts (id, scope) ON DELETE CASCADE,
    FOREIGN KEY (role_id, role_scope)
        REFERENCES merezha_identity.roles (id, scope) ON DELETE RESTRICT,
    CHECK (num_nonnulls(user_id, service_account_id) = 1),
    CHECK (service_account_id IS NULL OR role_id <> '00000000-0000-7000-8000-000000000004'),
    CHECK ((resource_kind IS NULL) = (resource_id IS NULL)),
    CHECK (resource_kind IS NULL OR role_id <> '00000000-0000-7000-8000-000000000004')
);
CREATE UNIQUE INDEX application_role_assignments_unique
    ON merezha_identity.application_role_assignments
    (coalesce(user_id, service_account_id), role_id, coalesce(resource_kind, ''), coalesce(resource_id, ''));
CREATE INDEX application_role_assignments_role_idx
    ON merezha_identity.application_role_assignments (role_id);

CREATE TABLE merezha_identity.invitations (
    id                uuid PRIMARY KEY,
    organization_id   uuid NOT NULL REFERENCES merezha_identity.organizations (id) ON DELETE CASCADE,
    email             text NOT NULL,
    email_normalized  text NOT NULL,
    token_hash        bytea NOT NULL CHECK (octet_length(token_hash) = 32),
    invited_by        uuid,
    created_at        timestamptz NOT NULL,
    expires_at        timestamptz NOT NULL,
    CONSTRAINT invitations_token_hash_key UNIQUE (token_hash),
    CONSTRAINT invitations_org_email_key UNIQUE (organization_id, email_normalized),
    CONSTRAINT invitations_id_org_key UNIQUE (id, organization_id)
);
CREATE INDEX invitations_expiry_idx ON merezha_identity.invitations (expires_at);

-- The 1 to 16 roles an invitation grants, same rule as role_assignments.
CREATE TABLE merezha_identity.invitation_roles (
    invitation_id    uuid NOT NULL,
    organization_id  uuid NOT NULL,
    builtin_role     text CHECK (builtin_role IN ('owner', 'admin', 'member')),
    custom_role_id   uuid,
    FOREIGN KEY (invitation_id, organization_id)
        REFERENCES merezha_identity.invitations (id, organization_id) ON DELETE CASCADE,
    FOREIGN KEY (custom_role_id, organization_id)
        REFERENCES merezha_identity.roles (id, organization_id),
    CHECK (num_nonnulls(builtin_role, custom_role_id) = 1)
);
CREATE UNIQUE INDEX invitation_roles_unique ON merezha_identity.invitation_roles
    (invitation_id, coalesce(builtin_role, custom_role_id::text));
CREATE INDEX invitation_roles_custom_role_idx ON merezha_identity.invitation_roles (custom_role_id);

CREATE TABLE merezha_identity.api_keys (
    id               uuid PRIMARY KEY,
    public_id        text NOT NULL CHECK (public_id ~ '^[a-z2-7]{16}$'),
    secret_hash      bytea NOT NULL CHECK (octet_length(secret_hash) = 32),
    organization_id  uuid REFERENCES merezha_identity.organizations (id) ON DELETE CASCADE,  -- NULL: application key
    user_id          uuid REFERENCES merezha_identity.users (id) ON DELETE CASCADE,
    service_account_id uuid REFERENCES merezha_identity.service_accounts (id) ON DELETE CASCADE,
    name             text NOT NULL CHECK (char_length(name) BETWEEN 1 AND 128),
    permissions      text[] NOT NULL CHECK (cardinality(permissions) BETWEEN 1 AND 256),
    created_by       uuid,                   -- the minting user; differs from the owner for service accounts
    created_at       timestamptz NOT NULL,
    expires_at       timestamptz NOT NULL,
    last_used_at     timestamptz,
    revoked_at       timestamptz,
    CHECK (num_nonnulls(user_id, service_account_id) = 1),
    CONSTRAINT api_keys_public_id_key UNIQUE (public_id)
);
CREATE INDEX api_keys_org_idx ON merezha_identity.api_keys (organization_id, created_at);
CREATE INDEX api_keys_user_idx ON merezha_identity.api_keys (user_id);
CREATE INDEX api_keys_service_account_idx ON merezha_identity.api_keys (service_account_id);

ALTER TABLE merezha_identity.sessions
    ADD COLUMN active_organization_id uuid
    REFERENCES merezha_identity.organizations (id) ON DELETE SET NULL;
```

Invitations are deleted on acceptance or revocation; the audit table keeps
the history. Revoked API keys keep their row for 30 days so that a use after
revocation is audited as such, then the cleanup job deletes them.

The composite foreign keys carry the scope rules: no tenant role or tenant
service account in an application assignment, no holder of one organization
in another's assignments, no custom role of one organization in another's
assignments or invitations, and the fixed-id check keeps `platform_admin`
from service accounts. The API names built-in tenant roles by their seeded
ids, which storage maps to `builtin_role`. The custom role keys are
`NO ACTION`, checked at statement end, so deleting an organization cascades
in one statement while deleting a role still used by an assignment or a
pending invitation fails `409 role_in_use`. Code, with the owner's row
locked, keeps a service account's keys in its own scope.

### Migration 3: `oauth_passkeys`

```sql
CREATE TABLE merezha_identity.oauth_identities (
    id                uuid PRIMARY KEY,
    user_id           uuid NOT NULL REFERENCES merezha_identity.users (id) ON DELETE CASCADE,
    provider          text NOT NULL CHECK (provider ~ '^[a-z][a-z0-9_]{0,31}$'),
    subject           text NOT NULL CHECK (octet_length(subject) BETWEEN 1 AND 255),
    email_normalized  text,                 -- as asserted at last sign-in
    email_verified    boolean NOT NULL,
    created_at        timestamptz NOT NULL,
    last_used_at      timestamptz,
    CONSTRAINT oauth_identities_provider_subject_key UNIQUE (provider, subject),
    CONSTRAINT oauth_identities_user_provider_key UNIQUE (user_id, provider)
);

CREATE TABLE merezha_identity.oauth_flows (
    state_hash     bytea PRIMARY KEY CHECK (octet_length(state_hash) = 32),
    binding_hash   bytea NOT NULL CHECK (octet_length(binding_hash) = 32),
    provider       text NOT NULL,
    intent         text NOT NULL CHECK (intent IN ('sign_in', 'link')),
    user_id        uuid REFERENCES merezha_identity.users (id) ON DELETE CASCADE,
    session_id     uuid REFERENCES merezha_identity.sessions (id) ON DELETE CASCADE,
    pkce_verifier  text NOT NULL,
    nonce          text,                    -- OIDC providers only
    return_to      text,
    created_at     timestamptz NOT NULL,
    expires_at     timestamptz NOT NULL,
    CHECK ((intent = 'link') = (user_id IS NOT NULL))
);
CREATE INDEX oauth_flows_expiry_idx ON merezha_identity.oauth_flows (expires_at);

ALTER TABLE merezha_identity.users
    ADD COLUMN webauthn_handle uuid,
    ADD CONSTRAINT users_webauthn_handle_key UNIQUE (webauthn_handle);

CREATE TABLE merezha_identity.passkeys (
    id              uuid PRIMARY KEY,
    user_id         uuid NOT NULL REFERENCES merezha_identity.users (id) ON DELETE CASCADE,
    credential_id   bytea NOT NULL CHECK (octet_length(credential_id) BETWEEN 16 AND 1023),
    passkey         jsonb NOT NULL,          -- webauthn_rs::prelude::Passkey
    label           text CHECK (char_length(label) BETWEEN 1 AND 64),
    aaguid          uuid,
    backup_eligible boolean NOT NULL,
    backup_state    boolean NOT NULL,
    discoverable    boolean,                 -- credProps.rk when reported
    transports      text[] NOT NULL DEFAULT '{}',
    revision        bigint NOT NULL DEFAULT 0,   -- compare-and-swap on counter updates
    created_at      timestamptz NOT NULL,
    last_used_at    timestamptz,
    CONSTRAINT passkeys_credential_id_key UNIQUE (credential_id)
);
CREATE INDEX passkeys_user_idx ON merezha_identity.passkeys (user_id);

CREATE TABLE merezha_identity.webauthn_ceremonies (
    id          uuid PRIMARY KEY,
    purpose     text NOT NULL CHECK (purpose IN ('registration', 'authentication', 'discoverable', 'reauthentication')),
    user_id     uuid REFERENCES merezha_identity.users (id) ON DELETE CASCADE,
    session_id  uuid REFERENCES merezha_identity.sessions (id) ON DELETE CASCADE,
    state       jsonb NOT NULL,               -- PasskeyRegistration / PasskeyAuthentication / DiscoverableAuthentication
    created_at  timestamptz NOT NULL,
    expires_at  timestamptz NOT NULL
);
CREATE INDEX webauthn_ceremonies_expiry_idx ON merezha_identity.webauthn_ceremonies (expires_at);
```

`webauthn_handle` is a random UUIDv4 assigned the first time a user starts
passkey registration. It is the WebAuthn user handle, unrelated to the user
id, so the authenticator never stores the account id.

### Key queries

Session authentication, one round trip per request, with the optional active
organization (`$2` may be NULL):

```sql
SELECT s.id, s.user_id, s.transport, s.method, s.authenticated_at,
       s.last_seen_at, s.idle_expires_at, s.absolute_expires_at,
       s.active_organization_id,
       u.status, u.email_verified_at, u.authz_version,
       o.authz_version AS org_authz_version, m.id AS membership_id
FROM merezha_identity.sessions s
JOIN merezha_identity.users u ON u.id = s.user_id
LEFT JOIN merezha_identity.organizations o ON o.id = coalesce($2, s.active_organization_id)
LEFT JOIN merezha_identity.memberships m ON m.organization_id = o.id AND m.user_id = u.id
WHERE s.token_hash = $1;
```

API key authentication looks the key up by public id with the same joins,
plus `memberships m ON m.organization_id = k.organization_id AND
m.user_id = k.user_id`. It fails `401 unauthenticated` when the key is
revoked or expired, its owner is not `active`, or it is a user's tenant key
(`user_id` and `organization_id` set) with no `m` row, on every request,
cache hit or not.

Single-use consumption of tokens, flows and ceremonies is
`DELETE ... WHERE token_hash = $1 AND purpose = $2 RETURNING ...`. An expired
row is deleted and then rejected, so the same token never gets a second try.

## 4. HTTP API

### Conventions

- Every route is under `/v1/identity/`, mounted by the `Module` trait.
  Request and response bodies are JSON (`Content-Type: application/json`
  required on bodies, otherwise `415 unsupported_media_type`). Request bodies
  are capped at 16 KiB, except passkey verification routes at 64 KiB
  (`413 body_limit`).
- Every response carries `Cache-Control: no-store`.
- Errors use the framework shape `{"error": {"code", "classification",
  "details"?}}` (R8). `validation_failed` fills `details` with one entry per
  field, such as `{"path": "/email", "code": "invalid"}`, and never echoes
  input. `rate_limited` and `overloaded` add `Retry-After` in seconds.
- Ids in paths use the prefixed form. An id from another organization returns
  `404 not_found`, never `403`, so ids cannot be probed across tenants.
- Timestamps are RFC 3339 UTC strings.
- "Auth" in the tables below: `none` (anonymous allowed), `session` (user
  session, cookie or bearer), `recent` (session with `authenticated_at`
  inside the reauthentication window), `user` (session or API key whose
  owner is the user), or a permission name. A permission is checked at
  `Tenant(org_id)` under `/organizations/{org_id}` and at `Application` under
  `/application/`.

### Credential transport

Two credential types arrive at the edge:

| Credential | Format | Where |
| --- | --- | --- |
| Session token | `mzs_` + 43 base64url characters (32 random bytes) | Cookie `__Host-merezha_session`, or `Authorization: Bearer` |
| API key | `mzk_` + 16-character base32 public id + `_` + 43 base64url characters | `Authorization: Bearer` only |

The prefixes let secret scanners (GitHub push protection and similar)
recognize leaked values and let the edge route the lookup without a database
query. A request that carries both a cookie session and an `Authorization`
header fails with `400 ambiguous_credentials`. A malformed, unknown, revoked
or expired credential fails with `401 unauthenticated`. The edge never falls
back to anonymous when a credential was presented. A database error during
authentication returns `503 unavailable`, never anonymous.

Cookie attributes: `__Host-merezha_session=<token>; Path=/; Secure; HttpOnly;
SameSite=Lax; Max-Age=<seconds to absolute expiry>`. When the host listens on
loopback and `app_origin` is `http://localhost` or `http://127.0.0.1`, the
cookie is `merezha_session` without `Secure` and without the prefix, for
local development. Startup refuses that combination when `http.public = true`.
framework.md records this loopback cookie as an accepted deviation (R15).

Sign-in routes accept `"transport": "cookie" | "bearer"` (default `cookie`).
With `bearer`, the token appears once in the response body and no cookie is
set. Bearer sessions have the same expiry rules.

### CSRF and origin policy

Cookie-authenticated requests with an unsafe method (`POST`, `PUT`, `PATCH`,
`DELETE`) must pass two checks, as framework.md requires:

1. `Origin` must equal one of `allowed_origins` (default `[app_origin]`). A
   missing `Origin` is accepted only with `Sec-Fetch-Site: same-origin`.
   Failure returns `403 origin_rejected`.
2. `X-CSRF-Token` must equal
   `base64url(HMAC-SHA256(csrf_key, "merezha.csrf.v1" || session_id))`,
   compared in constant time. `csrf_key` is derived from `secret_key` with
   HKDF-SHA256 (label `merezha/identity/csrf`). During key rotation the value
   derived from `secret_key_previous` is also accepted. Failure returns
   `403 csrf_failed`.

The token needs no storage, changes whenever the session token rotates
(because rotation creates a new session id), and is returned by every
response that creates a session and by `GET /v1/identity/session`.

Anonymous unsafe requests (sign-in, registration, token consumption) get the
origin check only. That stops login CSRF, because a cross-site form cannot
send `application/json` without a CORS preflight and the origin check rejects
it. Bearer-authenticated requests skip both checks: a bearer token is never
sent by the browser on its own.

### Session lifecycle

| Rule | Behavior | Reference |
| --- | --- | --- |
| Creation | A new 256-bit token per sign-in, which also sets `users.last_signed_in_at` under the user row lock. There are no pre-authentication sessions, so fixation has nothing to fix. | ASVS V7 |
| Idle expiry | `idle_timeout` (default 7 days). Each authenticated request extends `idle_expires_at` to `min(now + idle_timeout, absolute_expires_at)`, writing only when `last_seen_at` is older than `touch_interval` (60 s). | 800-63B reauthentication |
| Absolute expiry | `absolute_timeout` (default 30 days) from creation. Never extended. | 800-63B |
| Reauthentication | Sensitive routes require `authenticated_at` within `reauthentication_window` (10 min), else `401 reauthentication_required`. `POST /v1/identity/session/reauthenticate` refreshes it. | ASVS V7 |
| Rotation | Reauthentication, password change and email change replace the current session with a new id and token (insert new, delete old, one transaction) and return the new cookie or token. No periodic rotation: the cookie is HttpOnly, and rotating under concurrent requests causes spurious sign-outs for little gain. | ASVS V7 |
| Revocation | Deleting the row. Effective on the next request on every host instance, because every request reads the row. | |
| Log out everywhere | `POST /v1/identity/sessions/revoke-all` deletes all of the user's sessions, optionally keeping the current one. | |
| Forced revocation | Password reset: all sessions. Password change: all others. Email change: all others. User disabled: all. | |
| Cap | At most `max_per_user` sessions (default 50). Creating one more deletes the least recently seen. | |

### Route table

Request and response bodies are summarized; field types follow the examples.

#### Registration, verification, sign-in (PR 1)

| Method and path | Auth | Request | Success | Errors |
| --- | --- | --- | --- | --- |
| `POST /registrations` | none | `{email, password, display_name?}` | `202 {"status":"pending_verification"}` | `validation_failed`, `password_too_short`, `password_too_long`, `password_blocklisted`, `password_contextual`, `registration_closed`, `rate_limited` |
| `POST /email-verifications` | none | `{email}` | `202 {"status":"sent_if_eligible"}` | `validation_failed`, `rate_limited` |
| `POST /email-verifications/confirm` | none | `{token, transport?}` | `201 {session, user, csrf_token}` | `invalid_token`, `rate_limited` |
| `POST /sessions` | none | `{email, password, transport?}` | `201 {session, user, csrf_token}` | `invalid_credentials`, `email_unverified`, `account_disabled`, `rate_limited`, `overloaded` |
| `GET /session` | session | | `200 {session, user, csrf_token}` | `unauthenticated` |
| `DELETE /session` | session | | `204`, clears cookie | `unauthenticated` |
| `POST /session/reauthenticate` | session | `{password}` | `200 {session, csrf_token}` (rotated) | `invalid_credentials`, `rate_limited` |
| `GET /sessions` | session | | `200 {items:[session]}` | |
| `DELETE /sessions/{session_id}` | session | | `204` | `not_found` |
| `POST /sessions/revoke-all` | session | `{keep_current: true}` | `200 {revoked: 3}` | |
| `POST /magic-links` | none | `{email, return_to?}` | `202 {"status":"sent_if_eligible"}` | `validation_failed`, `rate_limited` |
| `POST /magic-links/consume` | none | `{token, transport?}` | `201 {session, user, csrf_token, return_to}` | `invalid_token`, `registration_closed`, `account_disabled`, `rate_limited` |
| `POST /password-resets` | none | `{email}` | `202 {"status":"sent_if_eligible"}` | `validation_failed`, `rate_limited` |
| `POST /password-resets/complete` | none | `{token, new_password, transport?}` | `201 {session, user, csrf_token}` | `invalid_token`, password policy codes, `rate_limited` |
| `GET /me` | user | | `200 {user}` | |
| `PATCH /me` | session | `{display_name?, locale?}` | `200 {user}` | `validation_failed` |
| `PUT /me/password` | recent | `{new_password}` | `200 {session, csrf_token}` (rotated, others revoked) | password policy codes, `reauthentication_required` |
| `POST /me/email-changes` | recent | `{new_email}` | `202 {"status":"sent_if_eligible"}` | `validation_failed`, `rate_limited` |
| `POST /me/email-changes/confirm` | none | `{token}` | `200 {user}` | `invalid_token`, `email_taken` |
| `DELETE /me` | recent | `{confirm_email}` | `204`, clears cookie | `sole_owner`, `validation_failed` |
| `GET /me/security-events` | session | `?cursor&limit` | `200 {items:[audit_event], next_cursor}` | |

Shapes:

```json
{"session": {"id": "ses_0192f5c4...", "method": "password", "transport": "cookie",
  "created_at": "2026-10-23T09:00:00Z", "authenticated_at": "2026-10-23T09:00:00Z",
  "idle_expires_at": "2026-10-30T09:00:00Z", "absolute_expires_at": "2026-11-22T09:00:00Z",
  "active_organization_id": null, "current": true, "token": "mzs_... (bearer, creating response only)"},
 "user": {"id": "usr_0192f5c4...", "email": "ada@example.com", "email_verified": true,
  "display_name": "Ada", "locale": "en", "created_at": "2026-10-01T12:00:00Z",
  "credentials": {"password": true, "passkeys": 1, "oauth": ["github"]}},
 "csrf_token": "q3Jv..."}
```

Session listings add `ip` truncated to /24 (IPv4) or /48 (IPv6) and the
user agent string, so users can recognize devices without the API returning
full addresses.

Behavior notes:

- `POST /registrations` always answers `202` for a syntactically valid
  request. When the email is new, it creates the user and password and sends
  `identity.verify_email`. When the email exists, it creates nothing and sends
  `identity.account_exists` (a "you already have an account, sign in or
  reset" message) to that address. Both paths compute one Argon2id hash, so
  response timing does not reveal which one ran. The TypeScript framework
  returned a conflict here, which leaked account existence.
- `POST /sessions` with an unknown email verifies against a dummy PHC string
  computed at startup with the configured parameters. Unknown email and wrong
  password both return `invalid_credentials`. `email_unverified` and
  `account_disabled` are returned only after the password matched, so they
  reveal nothing to someone without the password. On success, if the stored
  PHC parameters differ from configuration, the password is rehashed in the
  same transaction.
- `email-verifications/confirm` signs the user in. The token proves inbox
  control, the same proof a magic link gives. Without the registering
  browser's binding cookie it claims the unverified account first (section
  2) and answers `session.password_unset: true`; it clears the binding
  cookie either way.
- Magic links and verification links point at application pages configured in
  `links` with the token in the URL fragment, for example
  `https://app.example.com/auth/magic#token=...`. A fragment never reaches
  server logs or `Referer` headers. The page POSTs the token, so mail
  scanners that fetch links do not consume them.
- `return_to` is accepted only as a relative path that starts with `/` and
  not with `//` or `/\`, at most 512 bytes. It is resolved against
  `app_origin` and stored with the token or flow. Anything else is
  `validation_failed`. This closes the open-redirect class.
- `POST /magic-links` for an unknown email sends a sign-up link when
  `registration = "open"` and `magic_links.sign_up = true`, otherwise sends
  nothing. The response is identical either way.
- `POST /me/email-changes` sends `identity.email_change_confirm` to the new
  address and `identity.email_change_notice` to the current one, as an
  explicit `Address` captured at request time. The token is bound to the
  requesting session (`one_time_tokens.session_id`): only that session,
  cookie or bearer, confirms. Any other caller gets `401 sign_in_required`,
  nothing changes and the token stays usable, so someone who receives a link
  for an address change they did not ask for cannot attach their inbox to
  another account. Confirmation checks uniqueness at that moment
  (`409 email_taken`), swaps the address, rotates the requesting session and
  revokes the user's other sessions.
- `POST /password-resets`, `POST /email-verifications` and `POST /magic-links`
  validate the input, charge the rate limits and append one
  `identity.link_request` job record to the outbox (queue `identity`), then
  answer `202`. The payload holds personal data (the address, `return_to`,
  the request id, client address and user agent for the audit row) and a
  random nonce, so the record is flagged `contains_secret`: its payload is
  nulled at relay and at the job's terminal state, the admin API never shows
  it, a dead one is not replayed (the user asks again), and the nonce keeps
  the retained payload digest from being matched by guessing an address.
  The job looks the account up, issues the token and requests the
  notification. The request path runs the same statements for known and
  unknown addresses (section 7).
- `DELETE /me` fails with `409 sole_owner` while the user is the only owner
  of any organization. Otherwise it hard-deletes the user (cascade removes
  credentials, sessions, memberships, application role assignments, the
  user's API keys), keeps audit rows (user id only), and publishes
  `identity.user_deleted`.

#### Organizations, members, roles, service accounts, API keys (PR 2)

Paths below are relative to `/v1/identity/organizations/{org_id}` unless they
start with `/v1/identity/`. Checks are at `Tenant(org_id)`.

| Method and path | Auth | Request | Success | Errors |
| --- | --- | --- | --- | --- |
| `POST /v1/identity/organizations` | session | `{name, slug}` | `201 {organization, membership}` (caller is owner) | `slug_taken`, `slug_reserved`, `validation_failed`, `organization_limit` |
| `GET /v1/identity/organizations` | user | `?cursor&limit` | `200 {items:[{organization, membership}]}` | |
| `PUT /v1/identity/session/organization` | session | `{organization_id \| null}` | `200 {session}` | `not_found` |
| `GET /` | `identity.organization.read` | | `200 {organization}` | `not_found` |
| `PATCH /` | `identity.organization.update` | `{name?, slug?}` | `200 {organization}` | `slug_taken` |
| `DELETE /` | `identity.organization.delete` + recent | `{confirm_slug}` | `204` | `validation_failed` |
| `POST /ownership-transfers` | `identity.organization.transfer` + recent | `{membership_id, keep_owner: bool}` | `200 {membership}` | `not_found` |
| `GET /me/permissions` | user | | `200 {organization_id, roles, permissions:[...]}` | `not_found` |
| `GET /members` | `identity.members.read` | `?cursor&limit` | `200 {items:[member]}` | |
| `PATCH /members/{membership_id}` | `identity.members.update_roles` | `{role_ids:[...]}` | `200 {member}` | `permission_escalation`, `sole_owner` |
| `DELETE /members/{membership_id}` | `identity.members.remove`, or self | | `204` (revokes the member's keys for the organization) | `sole_owner` |
| `POST /invitations` | `identity.members.invite` | `{email, role_ids}` | `201 {invitation}` | `already_member`, `permission_escalation`, `not_found` (role of another organization), `rate_limited` |
| `GET /invitations` | `identity.members.invite` | | `200 {items:[invitation]}` | |
| `DELETE /invitations/{invitation_id}` | `identity.members.invite` | | `204` | `not_found` |
| `POST /v1/identity/invitations/inspect` | none | `{token}` | `200 {organization:{name, slug}, inviter_display_name, email_masked, expires_at, account_exists}` | `invalid_token` |
| `POST /v1/identity/invitations/accept` | none or session | `{token, transport?}` | `201 {membership, session?, csrf_token?}` | `invalid_token`, `sign_in_required`, `email_mismatch`, `already_member` |
| `GET /roles` | `identity.roles.read` | | `200 {items:[role]}` (built-in and custom) | |
| `POST /roles` | `identity.roles.manage` | `{key, name, description?, permissions}` | `201 {role}` | `role_key_taken`, `unknown_permission`, `permission_escalation`, `role_limit` |
| `PATCH /roles/{role_id}` | `identity.roles.manage` | `{name?, description?, permissions?}` | `200 {role}` | `builtin_role`, `unknown_permission`, `permission_escalation` |
| `DELETE /roles/{role_id}` | `identity.roles.manage` | | `204` | `builtin_role`, `role_in_use` (assignments or pending invitations) |
| `GET /role-assignments` | `identity.roles.read` | `?membership_id&service_account_id&resource_kind&resource_id` | `200 {items:[assignment]}` | |
| `POST /role-assignments` | `identity.roles.assign` | `{membership_id \| service_account_id, role_id, resource?: {kind, id}}` | `201 {assignment}` | `permission_escalation`, `assignment_limit`, `unknown_resource_kind`, `not_found` (role of another organization), `conflict` |
| `DELETE /role-assignments/{assignment_id}` | `identity.roles.assign` | | `204` | `sole_owner` |
| `GET /service-accounts` | `identity.service_accounts.read` | `?cursor&limit` | `200 {items:[service_account]}` | |
| `POST /service-accounts` | `identity.service_accounts.manage` + recent | `{name, description?}` | `201 {service_account}` | `service_account_limit`, `validation_failed` |
| `PATCH /service-accounts/{service_account_id}` | `identity.service_accounts.manage` + recent | `{name?, description?, status?}` | `200 {service_account}` | `not_found` |
| `DELETE /service-accounts/{service_account_id}` | `identity.service_accounts.manage` + recent | | `204` (keys and assignments go with it) | `not_found` |
| `GET /api-keys` | `identity.api_keys.read`, or own keys | `?service_account_id` | `200 {items:[api_key]}` | |
| `POST /api-keys` | own key: `identity.api_keys.create` + recent + membership; service account key: `identity.service_accounts.manage` + recent | `{name, permissions, expires_at, service_account_id?}` | `201 {api_key, key}` | `unknown_permission`, `permission_escalation`, `api_key_limit`, `validation_failed`, `unauthorized` (own key without membership) |
| `DELETE /api-keys/{key_id}` | own key, or `identity.api_keys.revoke` | | `204` | `not_found` |
| `GET /audit-events` | `identity.audit.read` | `?cursor&limit&action&actor_id` | `200 {items:[audit_event], next_cursor}` | |
| `GET /v1/identity/permissions` | user | | `200 {items:[{name, description, source}]}` | |

#### Application scope (PR 2)

Paths below are relative to `/v1/identity/application`, except the first,
and checks are at `Application`. An application without organizations
administers users and grants here. Service account and API key routes
repeat the tenant ones above under `/service-accounts` and `/api-keys`, with
the same bodies and errors.

| Method and path | Auth | Request | Success | Errors |
| --- | --- | --- | --- | --- |
| `GET /v1/identity/me/permissions` | user | | `200 {application: {roles, permissions:[...]}}` | |
| `GET /users` | `identity.users.read` | `?cursor&limit&email` | `200 {items:[user], next_cursor}` | |
| `PATCH /users/{user_id}` | `identity.users.manage` + recent | `{status: "active" \| "disabled"}` | `200 {user}` | `permission_escalation`, `not_found` |
| `GET /roles` | `identity.roles.read` | | `200 {items:[role]}` (built-in and declared) | |
| `GET /role-assignments` | `identity.roles.read` | `?user_id&service_account_id&role_id&cursor&limit` | `200 {items:[assignment], next_cursor}` | |
| `POST /role-assignments` | `identity.roles.assign` + recent | `{user_id \| service_account_id, role_id, resource?: {kind, id}}` | `201 {assignment}` | `permission_escalation`, `builtin_role`, `unknown_resource_kind`, `conflict` |
| `DELETE /role-assignments/{assignment_id}` | `identity.roles.assign` + recent | | `204` | `builtin_role`, `not_found` |
| `GET /audit-events` | `identity.audit.read` | `?cursor&limit&action&actor_id&organization_id` | `200 {items:[audit_event], next_cursor}` (every organization) | |

Behavior notes:

- Invitations are delivered through `identity.invitation`. Acceptance rules:
  a signed-in user whose email equals the invited address joins (an
  unverified address becomes verified).
  A signed-in user with another address gets `403 email_mismatch`. An
  anonymous caller whose address has no account, or only an unverified one,
  gets a verified account and a session (the token proves inbox control);
  an unverified account is claimed under the pre-hijacking rule first. Only
  a verified account gets `401 sign_in_required`, and `inspect` reports
  `account_exists` for verified accounts only. The front end then signs in
  and retries with the same token, which is consumed only on success.
- Every `role_id` in a tenant request resolves, under the organization
  lock, to a built-in tenant role or a custom role of that organization
  (`404 not_found` otherwise). Invitations store them in `invitation_roles`,
  and acceptance copies them into `role_assignments` under the same lock.
  Acceptance first re-checks the inviter as it stands now, under that lock:
  the inviting member's live grants (or the inviting key's, when a key
  invited) must still allow granting every invited role, `owner` by an
  owner; otherwise the invitation is deleted and the acceptance answers
  `409 invitation_stale`, committing nothing else. Invitation listings show
  `invited_by` and `invited_by_key`.
- `permission_escalation`: a caller can create or edit a role, assign a role,
  invite with roles, mint an API key, or change a user's status only if the
  caller holds every permission involved in the same scope (for a status
  change, every application permission of the target). Only owners assign
  `owner`, and owner-only paths (granting or removing `owner`, ownership
  transfer) re-read the caller's ownership under the organization lock, so
  a demotion racing the request is honoured. `platform_admin` and `user` are
  never assigned over HTTP.
- The active organization (`PUT /session/organization` or the
  `Merezha-Organization` header, which wins) must be one the caller belongs
  to, or any organization when the caller holds `identity.organization.enter`
  at `Application` (audited as `organization.entered`). A tenant key is
  active in its own organization only (a header naming another is `404`),
  and a user's one only while its owner is a member there (else 401).
- Removing a membership locks the organization row and, in one transaction,
  deletes it, revokes the user's unrevoked keys for that organization and
  every service account key it minted there (`created_by`; audited
  `api_key.revoked`, reason `membership_removed`, not revived by
  rejoining), deletes the invitations it sent that are still pending
  (`invitation.revoked`, reason `inviter_removed`) and bumps
  `users.authz_version`; the service accounts themselves stay. A role
  change that leaves the member without `identity.service_accounts.manage`,
  `identity.api_keys.create` or `identity.members.invite` in the
  organization revokes, in the same transaction, the service account keys
  it minted, its own keys, or deletes its pending invitations
  respectively (reason `minter_demoted`, `inviter_demoted`). Key listings
  show `created_by`. Minting a user's tenant key takes the same lock and
  requires membership, so it cannot race a removal.
- A service account's keys share its scope. A user can hold both kinds.
  Disabling a service account stops its keys on the next request.
- The sole-owner invariant is checked with the organization row locked, so
  two owners demoting each other at once leave one owner standing.
- API key creation returns `key` (`mzk_...`) once. Only the SHA-256 digest of
  the secret part is stored. `expires_at` is required and at most `max_ttl`
  (default 365 days) ahead.

#### OAuth and passkeys (PR 3)

| Method and path | Auth | Request | Success | Errors |
| --- | --- | --- | --- | --- |
| `GET /oauth/providers` | none | | `200 {items:[{id, kind, display_name}]}` | |
| `POST /oauth/{provider}/start` | none (sign-in) or recent (link) | `{intent: "sign_in" \| "link", return_to?}` | `200 {authorization_url}` and sets `__Host-merezha_oauth` | `not_found`, `rate_limited`, `provider_unavailable` |
| `GET /oauth/{provider}/callback` | binding cookie | provider query `code`, `state`, `iss?` or `error` | `303` to `return_to` or `links.sign_in_redirect`, sets session cookie | `303` to `links.oauth_error#error=<code>`; `oauth_callback.ip` throttles failures |
| `GET /me/oauth-identities` | session | | `200 {items:[{id, provider, email, created_at, last_used_at}]}` | |
| `DELETE /me/oauth-identities/{id}` | recent | | `204` | `last_credential`, `not_found` |
| `POST /passkeys/registrations/options` | recent | | `200 {ceremony_id, public_key}` | `passkey_limit`, `reauthentication_required` |
| `POST /passkeys/registrations/verify` | recent | `{ceremony_id, credential, label?}` | `201 {passkey}` | `invalid_ceremony`, `passkey_rejected`, `passkey_exists` |
| `POST /passkeys/authentications/options` | none | `{email?}` | `200 {ceremony_id, public_key, mediation}` | `rate_limited` |
| `POST /passkeys/authentications/verify` | none | `{ceremony_id, credential, transport?}` | `201 {session, user, csrf_token}` | `invalid_ceremony`, `invalid_credentials`, `account_disabled`, `rate_limited` |
| `POST /session/reauthenticate/passkey/options` | session | | `200 {ceremony_id, public_key}` | |
| `POST /session/reauthenticate/passkey/verify` | session | `{ceremony_id, credential}` | `200 {session, csrf_token}` (rotated) | `invalid_ceremony`, `invalid_credentials` |
| `GET /me/passkeys` | session | | `200 {items:[passkey]}` | |
| `PATCH /me/passkeys/{id}` | session | `{label}` | `200 {passkey}` | `not_found` |
| `DELETE /me/passkeys/{id}` | recent | | `204` | `last_credential`, `not_found` |

The OAuth callback is a browser navigation, so it answers with redirects, not
JSON. Error codes in the fragment: `oauth_denied`, `oauth_failed`,
`invalid_state`, `account_exists`, `identity_already_linked`,
`registration_closed`, `email_unverified`, `account_disabled`, and
`provider_unavailable` (PR 3 as built, section 10).

`POST /passkeys/authentications/options` without `email` starts a
discoverable (usernameless, conditional UI) ceremony. With `email`, it
returns the user's credential ids in `allowCredentials`. For an unknown email,
or a user without passkeys, it returns one or two fake credential ids derived
as `HMAC(secret_key, email)`, stable across requests, so the response has the
same shape as a real one. Verification then fails as `invalid_credentials`.

### Error code catalog

Identity registers its codes in K's error registry at startup (R8). The
canonical codes (`unauthenticated`, `unauthorized`, `actor_mismatch`,
`tenant_required`, `body_limit`, `rate_limited`, `overloaded`) are K's.

| HTTP | Code | Classification |
| --- | --- | --- |
| 400 | `ambiguous_credentials` | permanent |
| 401 | `unauthenticated`, `invalid_credentials`, `invalid_token`, `invalid_ceremony`, `reauthentication_required`, `sign_in_required` | permanent |
| 403 | `unauthorized`, `actor_mismatch`, `tenant_required`, `permission_escalation`, `origin_rejected`, `csrf_failed`, `email_unverified`, `email_mismatch`, `account_disabled`, `registration_closed`, `builtin_role` | permanent |
| 404 | `not_found` | permanent |
| 409 | `email_taken`, `slug_taken`, `slug_reserved`, `sole_owner`, `last_credential`, `identity_already_linked`, `role_key_taken`, `role_in_use`, `already_member`, `invitation_stale`, `passkey_exists`, `conflict` | conflict |
| 413 | `body_limit` | permanent |
| 415 | `unsupported_media_type` | permanent |
| 422 | `validation_failed` (with `details`; one status everywhere, R8), `password_too_short`, `password_too_long`, `password_blocklisted`, `password_contextual`, `unknown_permission`, `unknown_resource_kind`, `passkey_rejected`, `organization_limit`, `role_limit`, `assignment_limit`, `service_account_limit`, `api_key_limit`, `passkey_limit` | permanent |
| 429 | `rate_limited` (with `Retry-After`) | transient |
| 503 | `overloaded` (with `Retry-After`), `unavailable`, `provider_unavailable`, `audit_unavailable` (K's: a break-glass use whose audit row could not be written) | transient |

## 5. RBAC

### Permission names

- Grammar: two to four segments matching `[a-z][a-z0-9_]{0,31}`, joined by
  `.`. Examples: `tasks.create`, `identity.members.invite`,
  `content.entries.publish`.
- Block permissions start with the block name, using the block prefixes
  listed in AP §C.4. Application permissions must not use a block prefix.
  Startup fails if they do.
- Application permissions are declared in `application.toml`:

  ```toml
  [[permissions]]
  name = "tasks.create"
  description = "Create tasks"
  ```

  Every route `permission` must name a catalog entry. Startup reports every
  violation at once.
- Grants may use a trailing wildcard segment, `tasks.*` or `content.*`.
  The bare `*` exists only in the built-in `owner` and `platform_admin`
  roles. Custom roles, declared application roles and API keys list concrete
  permissions or trailing wildcards inside the application's own namespaces.
  There are no deny rules: absence is denial.
- Wildcards are expanded against the catalog when a principal snapshot is
  built, so a check is a hash-set lookup.

Identity's own permissions, checked at `Tenant` under `/organizations/` and
at `Application` under `/application/`: `identity.organization.read`,
`identity.organization.update`, `identity.organization.delete`,
`identity.organization.transfer`, `identity.members.read`,
`identity.members.invite`, `identity.members.update_roles`,
`identity.members.remove`, `identity.roles.read`, `identity.roles.manage`,
`identity.roles.assign`, `identity.service_accounts.read`,
`identity.service_accounts.manage`, `identity.api_keys.read`,
`identity.api_keys.create`, `identity.api_keys.revoke`,
`identity.audit.read`. Application scope only: `identity.users.read`,
`identity.users.manage`, `identity.organization.enter` (act in any
organization without a membership).

### Built-in and declared roles

| Role | Scope | Grants |
| --- | --- | --- |
| `platform_admin` | application | `*` at every scope: the break-glass grant (R5). Users only, never reachable through an API key. Assigned only through `merezha identity platform-admin grant\|revoke <email>` (CLI, audited). |
| `user` | application, implicit | Held by every active user without an assignment row: `[modules.identity.rbac.builtin_grants].user`, empty by default. Baseline grants for applications without organizations. Being application grants, they apply in every organization (evaluation step 4), so tenant-data permissions belong in `member`; startup warns when `user` grants one a route checks at `scope = "tenant"`. |
| `owner` | tenant | `*` |
| `admin` | tenant | every permission except `identity.organization.delete` and `identity.organization.transfer`; cannot assign `owner` |
| `member` | tenant | `identity.organization.read`, `identity.members.read`, `identity.roles.read`, `identity.api_keys.create`, plus `[modules.identity.rbac.builtin_grants].member` |

A tenant grant, even `owner`'s `*`, never satisfies an `Application` check,
so an owner cannot list the application's users or act in another
organization.

`[modules.identity.rbac.builtin_grants]` adds application permissions to
`user`, `admin` and `member`. It cannot remove identity's own grants, which
keeps the organization manageable. `admin` and `member` may name a trailing
wildcard inside a block namespace (`content.*`); `user`, which every
active user holds in every organization, follows the custom role rule and
may not (`identity.*` is `unknown_permission`).

Other application roles are declared in configuration
(`[modules.identity.rbac.application_roles.<key>]`, section 8), not created
over HTTP: they are application-wide policy and belong in reviewed
configuration. Startup syncs them into `roles`. Removing a declared role that
still has assignments fails startup with the role key and the count.

Custom roles are tenant roles of one organization, usable only there
(section 3 foreign keys), so the escalation check at that `Tenant` covers
every grant they make. Keys are unique per organization. Unknown
permissions are rejected at write time. If
a later release removes a permission from the catalog, stored roles keep the
string, evaluation ignores it, and startup logs a warning with a count.

### Scopes

```rust
pub enum ResourceScope {                 // K's type (R5, R6)
    Application,
    Tenant(TenantId),
    Resource { tenant: Option<TenantId>, kind: ResourceKind, id: ResourceId },
}
```

- `Application` covers the whole application, every tenant and resource
  included. Application role assignments and the implicit `user` role grant
  it. Every check other designs call "global" is an `Application` check (R5).
- `Tenant(t)` covers `t` and every resource in `t`.
- `Resource { tenant: None, kind, id }` is an application-level resource,
  such as an application-scoped content type (R5). Application grants and
  application assignments on that resource satisfy it.
- `Resource { tenant: Some(t), kind, id }` covers that resource only. There is no resource
  hierarchy in this version: a grant on project `p1` says nothing about tasks
  inside it. Applications that need inheritance check the parent resource in
  the route declaration or encode the rule in the guest using the principal.
- A tenant check, or a check on a resource in a tenant, requires the
  principal's tenant to be that organization unless an application grant
  matches, else `unauthorized`.

### Platform port

framework.md places `Principal` and `Authorizer` in `merezha-platform`. K
adopts these shapes (R13):

```rust
pub struct Principal {
    pub subject: Identity,                          // "usr_...", "key_..." or "sys_<name>"
    pub kind: PrincipalKind,                        // User | ApiKey | System
    pub user: Option<UserId>,                       // self for User, owner of a user key
    pub service_account: Option<ServiceAccountId>,  // owner of a service account key
    pub tenant: Option<TenantId>,                   // active organization
    pub grants: Arc<GrantSnapshot>,                 // roles and permissions per scope, immutable
    pub session: Option<SessionFacts>,              // id, method, authenticated_at, transport
}

pub trait Authorizer: Send + Sync {
    fn check(&self, principal: &Principal, permission: &Permission, scope: &ResourceScope)
        -> Result<Allowed, AccessDenied>;
}
pub enum Allowed { Grant, BreakGlass }

pub trait Authenticator: Send + Sync {
    /// Dyn-compatible: returns a boxed future.
    fn authenticate<'a>(&'a self, request: &'a RequestHead)
        -> BoxFuture<'a, Result<Option<Arc<Principal>>, Failure>>;
}
```

`SessionFacts` is never serialised or recorded in telemetry. Guests get AP's
projection: subject, kind, tenant, role keys of both scopes. System
principals (jobs, the CLI) use `sys_<name>` subjects (`sys_reactor_<name>`
for reactors) and an explicit grant set checked the same way.

`check` is synchronous and does no I/O. The `Authenticator` loads everything
a check needs into `GrantSnapshot` once per request:

```rust
pub struct GrantSnapshot {
    break_glass: bool,                                   // platform_admin, user sessions only
    application: Grants,                                 // application roles, implicit `user` included
    application_resources: HashMap<(ResourceKind, ResourceId), PermissionSet>,  // resources without tenant
    tenant: Option<Grants>,                              // tenant assignments in principal.tenant
    resources: HashMap<(ResourceKind, ResourceId), PermissionSet>,  // in principal.tenant
    key: Option<KeyLimit>,                               // API keys only
}
pub struct Grants { roles: Vec<RoleKey>, permissions: PermissionSet }
pub struct KeyLimit { permissions: PermissionSet, tenant: Option<TenantId> }  // tenant None: application key
```

For an API key, `application`, `tenant` and `resources` are its owner's
live grants: the user's, or the service account's (a tenant service account
has an empty `application`). A user's tenant key without its owner's live
membership never becomes a principal (section 3), so step 4 cannot apply
the owner's application grants, `user` included, to an ex-member's key.
Evaluation, in order:

1. `break_glass` set: allow with `Allowed::BreakGlass`. It is never set for
   an API key, so break-glass needs a user session.
2. API key pinned to tenant `k`: deny (`key_scope`) when the scope is
   `Application`, a resource without tenant, or names a tenant other than `k`.
3. API key whose list lacks the permission: deny (`key_limit`).
4. `application` contains the permission: allow, whatever the scope.
5. Scope is `Application`: deny (`no_grant`). Scope is a resource without
   tenant: allow if `application_resources[(kind, id)]` contains it, else
   deny (`no_grant`).
6. Scope names a tenant other than `principal.tenant`: deny
   (`tenant_mismatch`).
7. Allow if `tenant` contains it, or the scope is `Resource` and
   `resources[(kind, id)]` contains it. Otherwise deny (`no_grant`).

Steps 2 to 4 make a key's power its list intersected with its owner's live
grants at the key's scope. Minting checks the same (`permission_escalation`),
so a key never exceeds its owner.

A key also never exceeds its minter (`api_keys.created_by`). This is an
invariant of evaluation, not of the paths that change grants. When the
minter is not the owner (a service account's key), the `Authenticator`
caps the key's list at what the minter holds now at the key's scope
(`Tenant(org)` for a tenant key, `Application` otherwise). The cap comes
from the minter's own snapshot, cached and stamped with the minter's
`authz_version`, its membership and the organization's version, and the
key snapshot is stamped with the same. So a demotion, a custom role edit,
an application role revocation or any later path shrinks the key on the
next request on every instance. A minter that is deleted, disabled,
unverified (while verification is required) or, for a tenant key, no
longer a member leaves the key no power: it answers `401 unauthenticated`
as a revoked key does. Account deletion and disabling also revoke every
key the user minted or owns (`api_key.revoked`, reasons `minter_deleted`
and `minter_disabled`), and removal and role changes keep their
revocations (§4), so listings show the state. Listing keys with
break-glass is audited like every break-glass use.

`AccessDenied` carries the internal reason (`no_grant`, `tenant_mismatch`,
`key_scope`, `key_limit`, `unauthenticated`) for audit and debug logs, as the
TypeScript `Decision.reason` did. HTTP shows only `unauthorized` (403), or
`unauthenticated` (401) without a principal. A break-glass allow fails
closed: its `authorization.break_glass` row is written before the use,
once per session and permission per instance and marked only once
committed; a write that fails or takes more than 2 s refuses the use with
K's transient `audit_unavailable` (503, `DenyReason::AuditUnavailable`).
Identity's own routes await the write; the host `Authorizer`, synchronous,
waits for it in place on a multi-threaded runtime (`block_in_place`) and,
on a current-thread runtime, refuses the first use while the row is
written in the background. Break-glass writes share no slot with the
best-effort audits (`authorization.denied`, `organization.entered`),
which are dropped past 64 in flight and logged `identity.audit_dropped`.

The `Authenticator` caps a snapshot at `max_resource_assignments` (default
500) per holder per organization. Above the cap the membership still works,
and resource grants past the cap are ignored with a warning log. The cap
keeps snapshot size bounded. Applications that need thousands of resource
grants per user should model access in their own data.

### Route permissions

AP §C.4 owns the route syntax (`permission` with `scope = "application" |
"tenant"` and an optional `resource`, or `access`) and its enforcement in the
host before any guest call (AP-C2, R6). Identity supplies the catalog, the
`Authorizer` that AP calls with K's `ResourceScope`, and the resource kinds:
an assignment may target only a kind some route names in `resource` or a
block declares, such as content's `content_type` (`unknown_resource_kind`). The host writes `principal.subject` into
`Metadata.actor`, and a principal acting for another actor gets
`403 actor_mismatch`. Identity's own routes call `Authorizer::check` in their
handlers.

### Caching and invalidation

Building a snapshot costs two or three indexed queries. The cache avoids
repeating them while staying exact across host instances:

- `users.authz_version` increments, in the same transaction, on any change
  to that user's memberships, tenant or application role assignments, or
  status. `service_accounts.authz_version` does the same for a service
  account. `organizations.authz_version` increments on any change to that
  organization's custom role definitions, enough since a custom role is
  held only in its organization.
- The per-request authentication query (section 3) already returns the
  versions; the API key lookup joins the owner's row the same way. The
  in-process LRU cache, keyed by `(credential id, organization id)`, holds
  `(owner version, org version, catalog generation, snapshot)`. A version
  mismatch rebuilds the snapshot. A configuration reload of built-in grants
  or declared application roles bumps the catalog generation.
- Result: a role change is visible on the next request on every instance,
  with no cross-instance messaging and no TTL. Default capacity is 10,000
  entries, about 20 MB at typical sizes.

## 6. OAuth, OIDC, passkeys, email tokens

### Providers

| Provider | Protocol | Subject | Email trust (default) |
| --- | --- | --- | --- |
| `google` | OIDC, discovery at `https://accounts.google.com` | `sub` | `trust_email = true`; uses `email` only when `email_verified = true`. Optional `allowed_hosted_domains` checks `hd`. |
| `github` | OAuth 2.0 (not OIDC). Scopes `read:user user:email`. `GET https://api.github.com/user`, then `GET /user/emails`. | numeric `id` as a string (never `login`) | `trust_email = true`; uses the entry with `primary = true` and `verified = true` only |
| any name, `kind = "oidc"` | OIDC with discovery from `issuer` | `sub` (scoped by issuer through the provider name) | `trust_email = false` |

Generic OIDC defaults to untrusted email because an identity provider
controlled by someone else can assert any address. The "nOAuth" incidents
with Azure AD's mutable `email` claim are the known case. An operator who
runs the IdP can set `trust_email = true`. Turning `trust_email` on later
verifies nothing retroactively: accounts the provider created while
untrusted stay unverified until they verify through the normal flows (the
verification link, a magic link, a reset, an invitation, or a later
trusted assertion of their exact address, rule 1).

Subjects are scoped by their issuer: every identity stores the issuer it
was linked under (the OIDC issuer, GitHub's API base), and an identity
whose stored issuer differs from the provider's current one never signs in
(`oauth_failed`), so changing a provider's `issuer` never re-maps subjects
onto accounts.

### Authorization code flow

1. `POST /oauth/{provider}/start` creates an `oauth_flows` row with
   `state` (32 random bytes, stored as SHA-256), a PKCE S256 verifier
   (RFC 7636; `plain` is never offered), and an OIDC `nonce`. The response
   sets `__Host-merezha_oauth=<binding>` (32 random bytes, digest stored in
   the flow row; `Max-Age` = flow TTL, 10 min; `SameSite=Lax`, so the
   provider's top-level redirect back carries it).
2. The redirect URI is exact and per provider:
   `{api_base_url}/v1/identity/oauth/{provider}/callback`. A distinct callback
   per provider is the RFC 9700 mix-up defense. When the provider returns
   `iss` (RFC 9207), it must equal the configured issuer; when its
   discovery document advertises
   `authorization_response_iss_parameter_supported`, `iss` is required.
3. The callback consumes the flow by `state` digest (`DELETE ... RETURNING`),
   requires the binding cookie to match, and clears it once no other flow of
   that binding is pending (two tabs share one binding, and both complete).
   A missing or mismatched binding fails `invalid_state` and leaves the
   browser's cookie alone. This blocks login CSRF and callback injection,
   where an attacker finishes a flow in the victim's browser with the
   attacker's code. Failed callbacks naming a live flow are charged to
   `oauth_callback.ip`, checked before any work; a callback naming no live
   flow is counted, not audited, and charged to its own looser
   `oauth_callback_unknown.ip`, so junk never locks out the same IP's
   legitimate callbacks.
4. Code exchange and ID token validation use `openidconnect`: signature
   against the provider JWKS (refetched once on an unknown `kid`), allowed
   algorithms `RS256`, `ES256` and `EdDSA` only, `iss`, `aud`, `azp`,
   `exp`, `iat` with 5 minutes of skew, and `nonce`. GitHub uses `oauth2`
   plus the two API calls.
5. Provider access and refresh tokens are discarded after the profile is
   read. Identity never stores or forwards them.
6. The outbound HTTP client is the platform client wrapped as
   `openidconnect::AsyncHttpClient`: redirects disabled (an SSRF and
   token-leak precaution recommended by the `oauth2` crate docs), 10 s
   deadline, 1 MiB response cap, no retries. Discovery documents and JWKS are
   cached for 1 hour; concurrent callers share one fetch, and a failed
   discovery is remembered for 30 s. A provider that cannot be reached
   returns `provider_unavailable` and does not affect readiness.

### Account linking rules

Applied in order by a pure function `decide_link(identity, asserted_email,
trusted, signed_in_user, existing_user_by_email, config)`:

1. `(provider, subject)` already linked: sign in that user (link intent with
   another signed-in user: `identity_already_linked`). Update the stored
   email assertion.
2. Link intent, signed-in user from the flow row: attach the identity. The
   flow carries `session_id`, and that session must still be alive at the
   callback (share-locked, so a revocation is not lost). Starting a link
   needs a recent session, and the callback re-checks that it still is
   (`reauthentication_required` otherwise; the provider round trip does not
   stretch the window). One identity per provider per user. A link onto
   an existing account sends `identity.sign_in_method_added`.
3. Sign-in intent, no linked identity, and a trusted verified email that
   matches an existing user:
   - The existing user's email is verified and `link_by_verified_email =
     true`: link and sign in.
   - Otherwise: fail with `account_exists`. The user signs in with an existing
     method and links from settings. Default `link_by_verified_email = false`.
   - The existing user is unverified: apply the pre-hijacking rule (section
     2), then link and sign in. Trusted proof of the address outranks an
     unproven claim to it.
4. Sign-in intent, no match: create a user if `registration = "open"` and the
   provider's `allow_sign_up = true`, else `registration_closed`. The email
   is verified only if trusted and asserted verified. An untrusted or absent
   email that collides with an existing user fails `account_exists`. A
   provider without an email (rare) creates no account: `email_unverified`.

Email verification trust rule, stated once: an address becomes verified only
through an identity token sent to it (verification, magic link, reset,
invitation, email change) or through a provider with `trust_email = true`
that asserts that exact normalized address as verified. Nothing else marks it
verified.

Unlinking requires a recent session and that another credential remains
(`last_credential` otherwise).

### Passkeys with webauthn-rs

Configuration builds one `Webauthn` at startup with `rp_id`, `rp_name`, the
first origin, and `append_allowed_origin` for the others. RP id and origins
are configuration only, never request input. The passkey API is used, which
fixes user verification to `required`, attestation conveyance to `none`, and
algorithms to ES256 and RS256 (webauthn-rs defaults).

Registration ceremony (signed-in user, recent session):

1. Assign `users.webauthn_handle` if absent.
2. `start_passkey_registration(handle, email, display_name,
   exclude_credentials = user's credential ids)`.
3. Store the returned `PasskeyRegistration` as JSON in `webauthn_ceremonies`
   (purpose `registration`, user, session, TTL 5 min). Serializing it needs
   the `danger-allow-state-serialisation` feature, which is safe here because
   the state never leaves the server. That is the case the crate documents as
   safe.
4. Return `{ceremony_id, public_key}` (the `CreationChallengeResponse`).
5. Verify: consume the ceremony (must belong to the same user and session),
   `finish_passkey_registration`, store the `Passkey` JSON, credential id,
   backup flags, transports, `credProps.rk` if reported, AAGUID if nonzero,
   and the label. Credential id uniqueness is a database constraint
   (`passkey_exists`).

Authentication ceremony:

- With email: `start_passkey_authentication(user's passkeys)`, state stored
  with purpose `authentication`.
- Without email: `start_discoverable_authentication()` (feature
  `conditional-ui`), purpose `discoverable`. On verify,
  `identify_discoverable_authentication` yields the user handle and
  credential id. The passkey row is loaded by credential id and its user's
  `webauthn_handle` must match, then `finish_discoverable_authentication`.
- After success: update the stored `Passkey` with `update_credential`, using a
  compare-and-swap on `revision`. A lost race retries once from a fresh read.
  A counter regression on a credential with a nonzero counter rejects the
  sign-in and audits `passkey.counter_regression`. Synced passkeys report
  counter zero, which is accepted.
- The session method is `passkey`. Because UV is required, the session
  records that it was established with a multi-factor-equivalent credential.

Resident keys: webauthn-rs 0.5 sends `residentKey: discouraged` in passkey
registration. Platform and synced passkeys (iCloud Keychain, Google Password
Manager, Windows Hello) are discoverable regardless. Some hardware security
keys are not. Users with those keys sign in with the email-first flow. We do
not enable `resident-key-support`, which in 0.5 is attested-only and marked
incomplete in its source.

Attestation policy: none requested, none verified. AAGUID is kept only to
show a provider name. Enterprise or certified-authenticator requirements are
out of scope for this version.

Limits: `max_per_user = 20` passkeys. Removing the last credential fails
`last_credential`.

### Email tokens

| Purpose | TTL default | Subject key | On consumption |
| --- | --- | --- | --- |
| `email_verification` | 24 h | normalized email; the registering browser is bound on the user row (`users.binding_hash`), not on the token | Marks verified, signs in. Without the registration's binding cookie, claims an unverified account first (section 2), resent links included. |
| `magic_link` | 15 min | normalized email | Signs in, creating the account if allowed. Applies the pre-hijacking rule. |
| `password_reset` | 30 min | normalized email | Sets the password, deletes all sessions, signs in, marks the email verified, applies the pre-hijacking rule. |
| `email_change` | 1 h | user id; bound to the requesting session, rebound to its new id when that session rotates (reauthentication, password change) | Only that session confirms (`sign_in_required` otherwise). Swaps the email, revokes other sessions. A password change keeps it; it deletes every other token of the user. |
| invitation | 7 days | (organization, email) in `invitations` | Membership; account creation if none; claims an unverified account (pre-hijacking rule). |

Common lifecycle: 32 random bytes from the OS CSPRNG, base64url in the link,
SHA-256 digest at rest, one live token per subject key (a new request
replaces the row, so old links stop working), consumed with
`DELETE ... RETURNING` so that two concurrent consumers get one success and
one `invalid_token`. Expired, consumed and unknown tokens all return
`invalid_token`. Tokens for a user are deleted when the password changes or
the user is disabled. Rate limits apply to issuance per address and per IP.

## 7. Security

### Passwords

- Argon2id (RFC 9106), version 0x13. Defaults: `m = 65536` KiB (64 MiB),
  `t = 2`, `p = 1`, 16-byte salt, 32-byte output. That is above the OWASP
  Password Storage Cheat Sheet minimum (19 MiB, t = 2) and measures about 60
  to 120 ms per hash on current server cores. PR 1 adds a benchmark command
  so operators can tune it. Startup refuses `m < 19456` or `t < 2`.
- Hashing runs on `spawn_blocking` behind a semaphore of `hash_concurrency`
  permits (default: available parallelism). Waiting longer than
  `hash_queue_timeout` (2 s) returns `503 overloaded` with `Retry-After`.
  This bounds memory to `permits × m` and stops sign-in floods from starving
  the host.
- Policy per 800-63B and ASVS V6: minimum length `min_length` (default 12,
  floor 8, owner answer ID2), maximum 256 characters, no composition rules,
  no periodic expiry. Reject passwords on the blocklist (built-in list of the
  100,000 most common breached passwords from SecLists, MIT licensed,
  embedded compressed, plus an optional operator file) and context words
  (the email local part, the email domain label, the application name),
  compared after NFKC and lowercasing. framework.md records this policy,
  length plus a breached-password check, as an accepted deviation (R15).
- No pepper. A database-only leak is covered by Argon2id cost. A pepper
  would add a key that must be rotated with every hash, for a modest gain.
- Verification is constant time inside `argon2`. Unknown accounts verify
  against a dummy hash.

### Secrets and tokens

- Session tokens, API key secrets, one-time tokens, OAuth `state` and the
  binding value: 32 bytes from `getrandom` (the OS CSPRNG). Stored as
  SHA-256, as framework.md requires. SHA-256 without salt or key is enough
  for 256-bit random values, which cannot be brute-forced. Digest lookups go
  through a unique index, and API keys compare the stored digest in constant
  time (`subtle`) after lookup by public id.
- `secret_key` (at least 32 bytes, `{ env }` or `{ file }`) is the single
  identity secret. HKDF-SHA256 derives purpose keys from it: `csrf`,
  `ratelimit` (HMAC keys for rate-limit buckets) and `audit-subject` (HMAC of
  emails for unknown accounts). Rotation: set the new value, move the old one
  to `secret_key_previous` for at least one session idle timeout. CSRF
  accepts both. Rate-limit buckets reset, which is acceptable. Changing the
  secret invalidates no session, password or API key.
- OAuth client secrets come only from `{ env }` or `{ file }` and live in
  `secrecy::SecretString`. PKCE verifiers and nonces sit in `oauth_flows`
  for at most 10 minutes and are useless without the provider's code.
- Raw tokens appear in exactly two places: the HTTP response or `Set-Cookie`
  that delivers them, and the `secret_data` of the `Notifier` request that
  emails them. The outbox record and the jobs created from it carry
  `contains_secret`, and their payload is nulled when relayed or terminal
  (R3, section 8).

### Rate limits

Storage: `merezha_identity.rate_limits`, one row per bucket, so limits hold
across restarts and host instances. Bucket key:
`HMAC-SHA256(ratelimit_key, policy || 0x00 || subject)`. Emails, IPs and
tokens never appear in the table.

Algorithm: GCRA (generic cell rate algorithm), one timestamp per bucket.
With emission interval `T = period / limit` and burst `B = limit`, a request
at `now` is allowed when `max(tat, now) + T <= now + B·T`. One statement
does it:

```sql
INSERT INTO merezha_identity.rate_limits AS r (bucket, tat)
VALUES ($1, $2 + $3)                                   -- $2 = now, $3 = T
ON CONFLICT (bucket) DO UPDATE
    SET tat = greatest(r.tat, $2) + $3
    WHERE greatest(r.tat, $2) + $3 <= $2 + $4          -- $4 = B·T
RETURNING tat;
```

No row returned means denied. `Retry-After` is
`ceil(greatest(tat, now) + T − (now + B·T))`, from a follow-up read. A peek
(check without charging) is the same predicate in a `SELECT`. Policies marked
"on failure" peek before the work and charge only when it fails, the
approach the TypeScript limiter introduced, so naming a victim's email does
not spend the victim's budget. Peek-then-charge lets attempts already in
flight pass a nearly empty bucket, at most `hash_concurrency` of them, and
the arrival-charged `sign_in.ip` policy bounds the rest. PR 1 enforces that
bound with an admission gate of `hash_concurrency` permits, held from the
peek (one statement for every policy of the route) through the Argon2id
verification to the charge; the account lookup runs before it. The gate is
global, which is the residual trade-off: a flood of failing sign-ins from
many addresses can push other sign-ins into `503 overloaded` (with
`Retry-After`) for as long as it lasts, never into another account's outcome.
`sign_in.ip`, charged on arrival before the gate, bounds what each address
contributes; an operator facing a distributed flood raises
`hash_concurrency` or rate-limits in front of the host. The cleanup job
deletes rows with `tat < now`. `now` comes from the platform `Clock`, which
keeps tests deterministic.

The client IP is the edge's resolved address (trusted proxy CIDRs only).
IPv6 addresses are keyed by their /64.

| Policy | Subject | Default | Charged |
| --- | --- | --- | --- |
| `sign_in.ip` | IP | 30 per 5 min | on arrival |
| `sign_in.account_ip` | email + IP | 5 per 15 min | on failure |
| `sign_in.account` | email | 50 per 24 h | on failure (800-63B caps consecutive failures at 100) |
| `registration.ip` | IP | 10 per hour | on arrival |
| `email.address_ip` | email + IP | 5 per hour | on arrival, all email-sending routes |
| `email.address` | email | 30 per hour | on arrival, after `email.address_ip` admitted the request |
| `email.ip` | IP | 20 per hour | on arrival, all email-sending routes |
| `token.ip` | IP | 30 per 10 min | on failure, token consumption routes |
| `reauth.session` | session | 5 per 15 min | on failure |
| `passkey.ip` | IP | 60 per 10 min | on arrival, options routes |
| `oauth.ip` | IP | 30 per 10 min | on arrival, start route |
| `oauth_callback.ip` | IP | 30 per 10 min | on failure of a callback naming a live flow (checked first) |
| `oauth_callback_unknown.ip` | IP | 300 per 10 min | on failure of a callback naming no live flow; past it such callbacks answer `rate_limited`, never the IP's legitimate ones |
| `invitation.organization` | organization | 100 per day | on arrival |
| `api_key.invalid.ip` | IP | 60 per min | on failure |

Naming a victim's address spends the address-and-IP budgets of the
attacker's IPs, not the victim's. Two caps are shared per address: one IP
can drain `sign_in.account` and stop password sign-in for a day, and
`email.address` (also the ceiling on mail to one inbox) needs six IPs every
hour to keep email sign-in blocked. Passkeys and OAuth are keyed by IP only.
A distributed attacker can delay password and email sign-in for one account;
nothing blocks passkey or OAuth sign-in.

### Enumeration resistance

| Route | Measure |
| --- | --- |
| Registration | Always 202, with a binding cookie on both paths. Existing address gets an "account exists" email instead. Same hash cost on both paths. Residual: the new-address path writes the user, the credential and the token where the existing-address path only requests a mail, a few statements' difference behind one Argon2id hash; PR 1 accepts it and relies on `registration.ip` and the `email.*` limits. |
| Sign-in | Same code and same Argon2 cost for unknown email and wrong password. Unverified and disabled are revealed only after the password matched. |
| Reset, magic link, verification resend | Always 202. The request path validates, charges the rate limits and queues one flagged `identity.link_request` job (payload nulled at relay and terminal state, a random nonce against digest matching), the same statements for every address; the lookup runs in the job. A resend never sets the binding cookie. |
| Email change | Always 202 for a recent session. |
| Passkey options with email | Indistinguishable ceremony for unknown emails. |
| Invitation inspect | Requires the 256-bit token. `account_exists` is shown only to the token holder. |
| Cross-organization ids | 404. |

### Audit events

Written to `merezha_identity.audit_events`. Successful state changes are
written in the same transaction. Failures (failed sign-in, denied checks on
identity routes, rate-limit denials) are written in their own short
transaction. Rate-limit denials are recorded once per bucket per period, so
a flood cannot fill the table. A failed audit write fails the state change
for successes and increments the audit write failure counter (below) for
failures.

Actions: `user.registered`, `user.email_verified`,
`user.email_change_requested`, `user.email_changed`, `user.password_changed`,
`user.password_reset_requested`, `user.password_reset`, `user.disabled`,
`user.enabled`, `user.deleted`, `user.unverified_account_claimed`,
`session.created`, `session.sign_in_failed`, `session.reauthenticated`,
`session.revoked`, `session.revoked_all`, `magic_link.requested`,
`oauth.linked`, `oauth.unlinked`, `oauth.sign_in_failed`,
`passkey.registered`, `passkey.renamed`, `passkey.removed`,
`passkey.counter_regression`, `organization.created`, `organization.updated`,
`organization.deleted`, `organization.ownership_transferred`,
`organization.entered`, `membership.created`, `membership.roles_changed`,
`membership.removed`, `invitation.created`, `invitation.revoked`,
`invitation.accepted`, `role.created`, `role.updated`, `role.deleted`,
`role_assignment.created`, `role_assignment.deleted` (both with
`details.scope`), `service_account.created`, `service_account.updated`,
`service_account.deleted`, `api_key.created`, `api_key.revoked`,
`api_key.used_after_revocation`, `platform_admin.granted`,
`platform_admin.revoked`, `rate_limit.exceeded`, `authorization.denied`,
`authorization.break_glass`. These are audit actions, not R11 event types.

`details` accepts only allowlisted keys per action (method, provider,
reason code, scope, role keys, permission names, counts). Free text, tokens
and emails never go in. Retention: `audit.retention` (default 400 days),
deleted daily by the cleanup job.

### Telemetry redaction

TE's `names.rs` names every metric, span, attribute and log event (R9);
identity defines none. Spans are TE's `identity {operation}`.

- Attributes use the `identity.*` prefix that TE's allowlist keeps (method,
  provider, credential kind, outcome, error code, policy, permission). The
  organization id is TE's tenant attribute, on by default; the user id is
  off by default (TE1). Session ids never appear in telemetry.
- Never emitted: passwords, any token or digest, cookies, `Authorization`
  headers, emails, IP addresses, user agents, display names, OAuth codes,
  provider responses. IPs and user agents live only in the audit table and
  session rows.
- Secret-holding types (`SessionToken`, `ApiKeySecret`, `OneTimeToken`,
  `Password`) wrap `secrecy::SecretBox`, implement `Debug` as `"[redacted]"`,
  do not implement `Serialize`, and zeroize on drop.
- Metrics are TE §1.3's identity rows, landed by TE-T6 inside ID PR 1 and
  PR 2, including password-hash queue wait, grant-cache lookups by result,
  audit write failures and active sessions (set by the cleanup job). Log events are
  TE's (`identity.login_failed`), plus `identity.dev_link` (section 8).
- PR 1 includes a test that plants known secrets and session ids in
  requests and asserts they appear in no captured log line, span or metric.

### Threats and mitigations

| Threat | Mitigation |
| --- | --- |
| Credential stuffing, password spraying | Per-IP and per-account-and-IP GCRA limits, Argon2id cost, blocklist, passkeys available. |
| Account pre-hijacking | Verified email required to sign in by default. Ownership proof, invitation acceptance included, strips unverified accounts. Never-used unverified accounts expire while verification is required. |
| Session theft through XSS | HttpOnly cookie. Token never in JS for cookie transport. Short idle timeout configurable. Log out everywhere. |
| Session fixation | No pre-auth sessions. New token on every sign-in and reauthentication. |
| CSRF, login CSRF | `SameSite=Lax`, Origin allowlist, HMAC CSRF token, JSON-only bodies. |
| OAuth code injection, login CSRF through callback | Single-use `state`, PKCE S256, browser binding cookie, nonce. |
| OAuth mix-up | Per-provider redirect URIs, `iss` check (RFC 9207). |
| Open redirect | `return_to` limited to relative paths resolved against `app_origin`. |
| Provider email spoofing (nOAuth) | Email trust per provider, generic OIDC untrusted by default, link by subject only, auto-link by email off by default. |
| ID token forgery, algorithm confusion | Signature checked against JWKS, algorithm allowlist without `none` or HMAC, audience and nonce checks. |
| Token leakage in logs, Referer, mail scanners | Tokens in URL fragments, POST consumption, redaction rules, digests at rest. |
| Database read by an attacker | Only digests and Argon2id hashes. Raw tokens exist only in flagged outbox and job payloads until relay or terminal state (R3). |
| Double consumption under races | `DELETE ... RETURNING` on tokens, flows, ceremonies. Unique constraints for users and identities. |
| WebAuthn ceremony replay | State stored server-side, consumed once, bound to user and session, 5-minute TTL. |
| Cloned authenticator | Counter regression check for nonzero counters. The check runs against the counter read before verification, so two assertions verified concurrently can both pass; the stored counter keeps the maximum. |
| A credential added by a session that is revoked meanwhile | Passkey registration and OAuth linking re-check their session, share-locked, under the user lock before storing anything; a credential removed mid sign-in signs nobody in. A new method sends `identity.sign_in_method_added`. |
| WebAuthn origin spoofing | RP id and origins from configuration. webauthn-rs checks origin and RP id hash. |
| Privilege escalation through roles, keys or service accounts | Escalation check on every grant path at the target scope, owner-only owner assignment, `platform_admin` CLI-only and unreachable from keys, key power intersected live with the owner's, scope rules in foreign keys, custom roles bound to their organization. |
| Cross-tenant access (IDOR) | Organization in every scoped check, tenant match required unless an application grant matches, tenant keys pinned, 404 for foreign ids. |
| Last owner lockout | Sole-owner invariant under row lock. |
| Leaked API key | Prefixed format for secret scanning, mandatory expiry, revocation, last-use stamps, audit of use after revocation, dies when the owner leaves or is disabled. |
| A minter's authority outlives it through a key | A key is capped at its minter's live grants at the key's scope on every request (§5) and has no power once the minter is deleted, disabled or no longer a member; deletion, disabling, removal and losing the minting permission also revoke it. |
| Ex-member keeps access | Membership removal bumps `authz_version`, revokes the user's keys for the organization and the service account keys it minted there, and deletes its pending invitations, in one transaction; a demotion does the same for the authority it loses; acceptance re-checks the inviter's live grants; sessions lose the organization; authentication refuses a user's tenant key without a live membership. |
| Argon2 denial of service | Semaphore with queue timeout, per-IP limits before hashing, 1 KiB password byte cap. |
| Rate-limit table growth | Keys are fixed-size digests. Cleanup removes drained buckets. |
| SSRF through OIDC | Issuers come only from configuration. Redirects disabled. Response size cap. |
| Email change takeover | Recent session required, confirmation from the new inbox, notice to the old one, other sessions revoked. |
| Notifier outage hides verification | Identity refuses to start with email flows on and no delivery path (section 8). Outbox retries. |
| Break-glass misuse | `platform_admin` granted only by CLI, user sessions only, every use audited as `authorization.break_glass` before it is allowed (fail closed, `audit_unavailable`). |
| Clock skew between instances | Expiry compares against the platform `Clock`. Instances must run NTP. Five-minute skew allowed only for ID token `iat`. |

## 8. Integration

### Ports

| Port | Identity's role |
| --- | --- |
| `Authenticator` | Implements it. Resolves cookie, bearer session and API key credentials into `Principal` (section 5). Runs at the edge for every request that carries a credential. |
| `Authorizer` | Implements the RBAC evaluator (section 5). The host's route enforcement calls it (AP §C.4, AP-C2); identity's handlers call it for identity routes. |
| `Notifier` | Calls `request(tx, NotificationRequest)` inside the use case's transaction for the topics below (R2). Never calls `emit_webhook`: NT turns identity events into webhooks through AP subscriptions (R11). |
| `Outbox` | `append(tx, "identity", records, OnDuplicate::Ignore)` with `OutboxTarget::Event { topic }` for integration events, in the use case's transaction (R1). |
| `Directory` | Implements K's port (R7). See below. |
| Jobs | Registers the recurring `identity.cleanup` job (default every 10 minutes): expired sessions, tokens, flows, ceremonies, invitations, drained rate buckets, revoked keys past 30 days, unverified accounts past their TTL under section 2's conditions, audit rows past retention. Each pass deletes in batches of 1,000 to keep transactions short. |

### Notification topics

Identity owns this topic list, and NT predeclares every topic in it (R2):

| Topic | Sent when | Recipient | `secret_data` |
| --- | --- | --- | --- |
| `identity.verify_email` | registration of a new address, verification resend | `Address` | `link` |
| `identity.password_reset` | reset requested | `Address` | `link` |
| `identity.magic_link` | magic link requested, sign-up links included | `Address` | `link` |
| `identity.account_exists` | registration with an address that has an account | `Address` | none |
| `identity.password_changed` | password changed or reset | `User` | none |
| `identity.email_change_confirm` | email change requested | `Address` (the new address) | `link` |
| `identity.email_change_notice` | email change requested | `Address` (the current address, captured at request time) | none |
| `identity.invitation` | invitation created | `Address` | `link` |
| `identity.new_sign_in` | session created, when `notify_new_sign_in = true` (default false) | `User` | none |
| `identity.sign_in_method_added` | an OAuth identity linked to, or a passkey registered on, an existing account (PR 3) | `User` | none |

Token mail goes to `Address`, the exact address the token was issued for
(with `user` set when an account exists). Notices go to `User` through the
`Directory`, except the email change notice, which goes to the address the
account had when the change was requested. `data` holds only non-secret fields (`display_name`,
`organization_name`, `expires_at`); the link goes in `secret_data.link`.
The request's `expires_at` is the token's expiry, and its dedupe key is
`{topic}:{id}` with the token, invitation or audit event id. The flagged
outbox record and its jobs lose their payload at relay and at the terminal
state (R3). Identity refuses to re-request a dead-lettered email, since its
token may be superseded; the user asks for a new link.

Without the notifications block, the framework's capture sink would silently
drop verification and reset mail, which framework.md forbids for
security-relevant effects. Identity therefore refuses to start when an email
flow is configured and the `Notifier` is the capture sink, unless
`dev_print_links = true` on loopback. Then each link goes to stderr as log
event `identity.dev_link`, the only place a raw token is ever logged.

### Directory

K's port (R7), which NT uses to resolve `User`, `Tenant` and `Application`
recipients, so user emails never travel in events. K2 fixes the signature;
identity implements this reading of it:

```rust
pub trait Directory: Send + Sync {
    fn users<'a>(&'a self, ids: &'a [UserId]) -> BoxFuture<'a, Result<Vec<Contact>, Failure>>;
    fn tenant_members<'a>(&'a self, tenant: &'a TenantId, roles: Option<&'a [RoleKey]>, page: PageRequest)
        -> BoxFuture<'a, Result<Page<Contact>, Failure>>;
    fn application_members<'a>(&'a self, roles: Option<&'a [RoleKey]>, page: PageRequest)
        -> BoxFuture<'a, Result<Page<Contact>, Failure>>;
    fn is_member<'a>(&'a self, user: &'a UserId, tenant: &'a TenantId)
        -> BoxFuture<'a, Result<bool, Failure>>;
}
pub struct Contact { pub user: UserId, pub email: String, pub display_name: Option<String>, pub locale: Option<String> }
```

Only active users with a verified email are returned; other ids are absent
and NT skips them. No phone numbers (ID10). `users` takes at most 100 ids.
Role filters match organization-scope tenant roles or application roles;
`None` (or `user`) means everyone. Pages are keyset-ordered by user id, at
most 500. Service accounts never appear. `is_member` counts memberships
only, not application grants, and only of active users with a verified
address like the other calls, so NT's `recipient_not_in_tenant` check (R4)
stays a membership check.

### Integration events

Envelope: `event_id`, `type`, `version`, `occurred_at`, `correlation_id`,
`tenant` (the organization id, or null), `payload`. AP adopts this envelope
as the starting point of its own (R11) and owns it from there. Payloads
carry ids, not personal data. Type names follow R11:
`<module>.<aggregate>_<past verb>`. All at version 1.

| Type | Payload |
| --- | --- |
| `identity.user_registered` | `{user_id, method: "password" \| "magic_link" \| "oauth" \| "invitation", registered_at}` |
| `identity.user_email_verified` | `{user_id, verified_at}` |
| `identity.user_email_changed` | `{user_id, changed_at}` |
| `identity.user_disabled` | `{user_id, disabled_at}` |
| `identity.user_enabled` | `{user_id, enabled_at}` |
| `identity.user_deleted` | `{user_id, deleted_at}`. Consumers must erase or anonymize personal data they hold for the user. |
| `identity.organization_created` | `{organization_id, slug, created_by, created_at}` |
| `identity.organization_updated` | `{organization_id, slug, updated_at}` |
| `identity.organization_deleted` | `{organization_id, deleted_at}`. Consumers delete tenant data per their retention rules. |
| `identity.membership_created` | `{membership_id, organization_id, user_id, role_keys, via: "creation" \| "invitation" \| "transfer"}` |
| `identity.membership_roles_changed` | `{membership_id, organization_id, user_id, role_keys}` |
| `identity.membership_removed` | `{membership_id, organization_id, user_id, reason: "removed" \| "left" \| "user_deleted"}` |
| `identity.user_application_roles_changed` | `{user_id, role_keys}` |

Identity consumes no events in this program. Sessions, service accounts and
API keys publish no events: they are internal, and audit covers them. A
breaking payload change publishes version 2 alongside version 1 for one
release.

### Configuration: `[modules.identity]`

Durations use K's duration type (R12). Secrets accept only
`{ env = "..." }` or `{ file = "..." }`. Unknown keys fail startup. There are
no `enabled` keys: the module is on when `[modules.identity]` is present, and
a sign-in method when its table is (`password`, `magic_links`, `passkeys`,
`oauth.providers.<name>`). `organizations.user_can_create = false` gives an
application without organizations.

```toml
[modules.identity]
app_origin = "https://app.example.com"     # front end origin: links, return_to, WebAuthn origin, CSRF origin
api_base_url = "https://app.example.com"   # externally visible base of this host; OAuth callbacks. Default: app_origin
allowed_origins = ["https://app.example.com"]  # default: [app_origin]
secret_key = { env = "MEREZHA_IDENTITY_SECRET_KEY" }   # required, >= 32 bytes
secret_key_previous = { env = "MEREZHA_IDENTITY_SECRET_KEY_PREVIOUS" }  # optional
registration = "open"                      # open | invite_only | closed
require_verified_email = true
unverified_account_ttl = "7d"                # cleanup runs only when require_verified_email = true
dev_print_links = false                    # loopback only
notify_new_sign_in = false                 # identity.new_sign_in notice

[modules.identity.links]                   # paths on app_origin, or absolute URLs on it
verify_email = "/auth/verify-email"
magic_link = "/auth/magic-link"
password_reset = "/auth/reset-password"
email_change = "/auth/confirm-email"
invitation = "/auth/invitation"
sign_in_redirect = "/"
oauth_error = "/auth/error"

[modules.identity.password]
min_length = 12                            # floor 8
max_length = 256                           # ceiling 256
blocklist_file = { file = "/etc/app/passwords.txt" }   # optional, adds to the built-in list
argon2_memory_kib = 65536                  # floor 19456
argon2_iterations = 2                      # floor 2
argon2_parallelism = 1
hash_concurrency = 0                       # 0 = available parallelism
hash_queue_timeout = "2s"

[modules.identity.sessions]
idle_timeout = "7d"
absolute_timeout = "30d"
reauthentication_window = "10m"
touch_interval = "60s"
max_per_user = 50
bearer = true                              # allow transport = "bearer"

[modules.identity.tokens]
email_verification_ttl = "24h"
magic_link_ttl = "15m"
password_reset_ttl = "30m"
email_change_ttl = "1h"
invitation_ttl = "7d"

[modules.identity.magic_links]
sign_up = true                             # unknown addresses get a sign-up link when registration = "open"

[modules.identity.organizations]
user_can_create = true                     # no personal organization at sign-up (ID4)
max_per_user = 50                          # organizations a user may create
max_members = 10000

[modules.identity.rbac]
cache_entries = 10000
max_custom_roles = 100                     # per organization
max_resource_assignments = 500             # per holder per organization

[modules.identity.rbac.builtin_grants]
user = []                                  # implicit application role of every active user, valid in every organization
admin = ["tasks.*"]
member = ["tasks.read", "tasks.create"]

[modules.identity.rbac.application_roles.support]   # one table per declared application role
name = "Support"
permissions = ["identity.users.read", "identity.organization.enter", "tasks.read"]

[modules.identity.service_accounts]
max_per_scope = 100                        # per organization, and for the application

[modules.identity.api_keys]
max_per_scope = 100                        # per organization, and for the application
max_ttl = "365d"
touch_interval = "5m"

[modules.identity.oauth]
flow_ttl = "10m"
link_by_verified_email = false             # owner answer ID1
http_timeout = "10s"

[modules.identity.oauth.providers.google]
client_id = "123.apps.googleusercontent.com"
client_secret = { env = "GOOGLE_CLIENT_SECRET" }
display_name = "Google"
trust_email = true
allow_sign_up = true
allowed_hosted_domains = []                # empty = any

[modules.identity.oauth.providers.github]     # same keys as google, without allowed_hosted_domains
client_id = "Iv1.abc"
client_secret = { env = "GITHUB_CLIENT_SECRET" }

[modules.identity.oauth.providers.acme_sso]
kind = "oidc"                              # google and github imply their kind
issuer = "https://sso.acme.example"
client_id = "merezha"
client_secret = { file = "/run/secrets/acme_sso" }
display_name = "Acme SSO"
scopes = ["openid", "email", "profile"]   # default for oidc
trust_email = false
allow_sign_up = false

[modules.identity.passkeys]
rp_id = "example.com"                      # default: host of app_origin
rp_name = "Example"
origins = ["https://app.example.com"]      # default: [app_origin]
ceremony_ttl = "5m"
discoverable = true
max_per_user = 20

[modules.identity.rate_limits.sign_in_ip]   # one table per policy in section 7, keys: limit, period
limit = 30
period = "5m"

[modules.identity.audit]
retention = "400d"

[modules.identity.cleanup]
interval = "10m"
batch_size = 1000
```

Rate-limit table names replace the policy's dots with underscores
(`sign_in.ip` becomes `sign_in_ip`). Every policy from section 7 has the
same two keys.

Startup validation, all errors reported at once: `app_origin` and
`api_base_url` are HTTPS unless loopback development; `rp_id` is
`app_origin`'s host or a registrable suffix of it; every origin is in
`allowed_origins`; at least one sign-in method is configured; email flows
have a delivery path (above); provider names match `[a-z][a-z0-9_]{0,31}`;
`builtin_grants` and declared application roles name catalog permissions,
and declared role keys do not collide with built-in ones; Argon2 floors hold;
`idle_timeout <= absolute_timeout`.

## 9. Crates

Verified 2026-10-23 with `cargo info` on cargo 1.98.0, and compiled together
with `cargo +1.98.0 check` in a scratch crate (edition 2024, `rust-version =
"1.98.0"`), which finished cleanly. Every MSRV is at or below 1.98. `jiff`
replaced `time` on 2026-10-24 (R14), after that probe: its metadata and the
`tokio-postgres` `with-jiff-0_2` feature were checked with `cargo info`, and
PR 1's build is its compile check.

| Crate | Version | License | MSRV | Features | Use |
| --- | --- | --- | --- | --- | --- |
| `argon2` | 0.6.0 | MIT OR Apache-2.0 | 1.85 | default (`alloc`, `getrandom`, `password-hash`), `zeroize` | Password hashing, PHC strings |
| `webauthn-rs` | 0.5.5 | MPL-2.0 | 1.88 | default (`attestation`), `danger-allow-state-serialisation`, `conditional-ui` | Passkey ceremonies |
| `openssl` | 0.10.81 | Apache-2.0 | 1.80 | `vendored` | Pulled in by `webauthn-rs-core`; vendored so the prebuilt host needs no system OpenSSL |
| `openidconnect` | 4.0.1 | MIT | 1.65 | `default-features = false` | OIDC discovery, code exchange, ID token validation |
| `oauth2` | 5.0.0 | MIT OR Apache-2.0 | 1.65 | `default-features = false` | GitHub flow, PKCE types |
| `reqwest` | 0.13.5 | MIT OR Apache-2.0 | 1.85 | `default-features = false`, `rustls`, `json`, `http2` (the kernel client's set) | Platform outbound client (already planned in framework.md); wrapped as `AsyncHttpClient` |
| `sha2` | 0.11.1 | MIT OR Apache-2.0 | 1.88 | default | Token digests |
| `hmac` | 0.13.0 | MIT OR Apache-2.0 | 1.85 | default | CSRF tokens, rate-limit keys, HKDF building block |
| `subtle` | 2.6.1 | BSD-3-Clause | not declared | default | Constant-time comparisons |
| `rand` | 0.10.3 | MIT OR Apache-2.0 | 1.85 | default | Token generation through the platform CSPRNG |
| `getrandom` | 0.4.3 | MIT OR Apache-2.0 | 1.85 | default | OS entropy |
| `base64` | 0.23.1 | MIT OR Apache-2.0 | 1.71 | default | base64url for tokens |
| `uuid` | 1.29.0 | Apache-2.0 OR MIT | 1.89 | workspace pin: `v4`, `v5`, `v7`, `serde` | Ids, WebAuthn handles |
| `jiff` | 0.2.38 | Unlicense OR MIT | 1.70 | workspace pin: `default-features = false`, `std`, `serde`, `tz-system`, `tzdb-bundle-always` | Timestamps and durations, the one time crate (R14) |
| `tokio-postgres` | 0.7.18 | MIT OR Apache-2.0 | 1.85 | `with-uuid-1`, `with-jiff-0_2`, `with-serde_json-1` | Through `merezha-postgres` |
| `deadpool-postgres` | 0.14.2 | MIT OR Apache-2.0 | 1.85 | default | Through `merezha-postgres` |
| `secrecy` | 0.10.3 | Apache-2.0 OR MIT | 1.60 | default | Secret wrappers |
| `zeroize` | 1.9.1 | Apache-2.0 OR MIT | 1.85 | `derive` | Wiping secrets |
| `unicode-normalization` | 0.1.26 | (MIT OR Apache-2.0) AND Unicode-3.0 | 1.36 | default | NFC and NFKC |
| `idna` | 1.1.0 | MIT OR Apache-2.0 | 1.57 | default | Email domain normalization |
| `url` | 2.5.8 | MIT OR Apache-2.0 | 1.63 | default | Origin and redirect parsing |
| `ipnet` | 2.12.2 | MIT OR Apache-2.0 | not declared | default | IPv6 /64 keys, display truncation |
| `cookie` | 0.18.2 | MIT OR Apache-2.0 | 1.56 | default | Cookie parsing and `Set-Cookie` |
| `webauthn-authenticator-rs` (dev only) | 0.5.5 | MPL-2.0 | 1.88 | `softpasskey` | Software authenticator for ceremony tests (metadata checked, not in the compile probe) |

Decisions and notes:

- `webauthn-rs` stable is 0.5.5. The 0.6.x line on crates.io is published as
  `-dev` prereleases; do not use it until a stable 0.6.0 ships.
- `webauthn-rs` is MPL-2.0. That is file-level copyleft and compatible with
  shipping an MIT project that depends on it unmodified. `deny.toml` must
  list `MPL-2.0` explicitly, and the release notes must point to the crate
  source. Its README reports a security audit by SUSE product security, and
  Kanidm runs it in production. The pure-Rust alternative
  `webauthn_rp` 0.3.0 (MIT OR Apache-2.0) was rejected: one maintainer, a
  self-hosted repository and little production use.
- OpenSSL comes in through `webauthn-rs-core` and cannot be switched off.
  The `openssl/vendored` build needs a C compiler and Perl in CI and in the
  release builder. The `passkeys` cargo feature of `merezha-identity` gates
  the dependency, so a build without passkeys has no OpenSSL. The prebuilt
  host enables it.
- `openidconnect` 4.0.1 pulls `rsa` 0.9.10, which has RUSTSEC-2023-0071
  (Marvin timing attack on private-key operations, no patch). Identity only
  verifies signatures with public keys, which the advisory does not affect.
  Record a scoped ignore in `deny.toml` with that reason and re-check it when
  `openidconnect` moves to `rsa` 0.10.
- `openidconnect` and `oauth2` with default features would add `reqwest`
  0.12 next to the platform's 0.13. With `default-features = false` and our
  own `AsyncHttpClient`, there is one `reqwest`, and redirects stay disabled
  under our control.
- Duplicate versions accepted: `sha2` 0.10 and `hmac` 0.12 (inside
  `openidconnect`), `rand` 0.8, 0.9 and 0.10, `base64` 0.21, 0.22 and 0.23.
  They come from upstream pins and cost compile time only.
- `openidconnect` 4.0.1 and `oauth2` 5.0.0 both require `chrono` 0.4 and
  use it in claim types (crates.io, 2026-10-24). R14 allows it inside the
  OAuth/OIDC provider adapter only: identity converts to `jiff` there, and no
  `chrono` type leaves it.
- Rejected: `jsonwebtoken` (would duplicate ID token validation that
  `openidconnect` does with stricter defaults), `governor` (in-memory rate
  limiting, cannot survive restarts or span instances), `bcrypt` and
  `scrypt` (framework.md fixes Argon2id), `tower-sessions` (its store model
  does not fit idle/absolute expiry plus digest-only storage, and the session
  logic is small).

PR 1 (2026-10-24) uses `argon2`, `hmac`, `sha2`, `subtle`, `base64`,
`uuid`, `jiff`, `secrecy`, `zeroize`, `unicode-normalization`, `idna` and
`tokio-postgres` (its `with-uuid-1`, `with-jiff-0_2` and `with-serde_json-1`
features enabled from `merezha-identity`) as listed, with
`argon2` at `default-features = false` and `alloc`, `password-hash`,
`zeroize` (salts come from the platform CSPRNG, so no `getrandom` feature).
It adds `miniz_oxide` 0.9.1 (MIT OR Zlib OR Apache-2.0, already locked
through `flate2`) to decompress the embedded blocklist. It needs no `url`,
`ipnet`, `cookie`, `rand` or direct `getrandom`: origin parsing, the IPv6
/64 key, the cookie header and token bytes are a few lines each on `std`
and the platform `Random`. HKDF is written on `hmac` (RFC 5869, tested
against its vector) instead of adding `hkdf`.

Workspace pins follow the repository's existing style (exact pins for
`wasmtime` and `rusqlite`): exact `=` pins for the crates on the
authentication path (`webauthn-rs`, `openidconnect`, `oauth2`, `argon2`), so
an upgrade is always a reviewed change, and caret requirements elsewhere.

## 10. Delivery slices

Each PR follows framework.md: tests against real PostgreSQL through
`TestDatabase` (`MEREZHA_TEST_DATABASE_URL`), documentation exercised as
written, an independent review, green CI. Tests use the platform fake
`Clock` and seeded CSPRNG where determinism matters, and a capture `Notifier`
double that records requests so tests can read emailed tokens.

### PR 1: identity core (stage 1)

Depends on K2 (contract alignment) and AP-B3 (the jobs runtime through cron:
the outbox relay of AP-B1 and the workers of AP-B2 carry its mail, and the
`identity.cleanup` schedule needs AP-B3).

Scope: crate `merezha-identity` with pure domain modules (normalization,
password policy, link decisions as stubs), migration 1, the `Module`
implementation, registration, verification, password sign-in, sessions
(cookie and bearer), CSRF and origin checks, reauthentication, magic links,
password reset, password change, email change, account deletion, the
`Authenticator` for sessions, rate limits, audit, cleanup job, the
`Principal` with an empty grant snapshot, the identity topics of section 8,
and the `identity.password-benchmark` CLI command. The `Authorizer` is not
yet provided, so the application keeps `AllowAll` on loopback.

Acceptance criteria:

1. Migration 1 applies on an empty database, is recorded with its checksum,
   and a second start applies nothing.
2. Every route in the PR 1 table answers as specified, including every
   listed error code.
3. Registration of a new and of an existing email return identical status
   and body. The capture notifier receives `identity.verify_email` and
   `identity.account_exists` respectively.
4. Sign-in with an unknown email and with a wrong password return identical
   responses and both run one Argon2id verification (asserted by counting
   hasher calls).
5. Sessions expire at idle and absolute limits under the fake clock.
   `touch_interval` limits writes (asserted by row update counts).
6. Revocation and log out everywhere take effect on the next request, also
   through a second `Application` instance sharing the database.
7. Unsafe cookie requests without a valid Origin or CSRF token fail. Bearer
   requests do not need them.
8. Tokens are single use, superseded by newer ones, and expire.
9. The pre-hijacking rule removes the attacker's password when the victim
   uses a magic link or reset.
10. No raw token, password, email, IP or session id appears in logs, spans
    or metrics (planted secrets test).
11. Tokens travel only in `secret_data`. The outbox record is flagged
    `contains_secret` and its payload is null after relay.
12. Identity refuses to start with email flows and no delivery path, except
    `dev_print_links` on loopback.
13. Cleanup deletes a never-used unverified account after its TTL only with
    `require_verified_email = true`; with `false`, or once it has signed in,
    it survives every pass.
14. Requests naming one address from five IPs exhaust only their own
    `email.address_ip` buckets: the victim's IP still gets its magic link.

Test plan:

- Unit tests: email and password normalization tables, blocklist and
  context rules, GCRA arithmetic, CSRF derivation, `return_to` validation
  (including `//evil`, `/\evil`, `https://evil`, encoded variants).
- PostgreSQL integration: every route through the axum router with real
  migrations.
- Concurrency: 50 parallel consumptions of one magic link produce exactly one
  session; 20 parallel registrations of one email produce one user; 100
  parallel requests against an arrival-charged bucket of 5 admit exactly 5,
  and against a failure-charged bucket of 5 admit at most 5 plus
  `hash_concurrency`; log out everywhere racing with requests leaves no live
  session afterwards; password change racing with a reset ends with one
  password and only the winner's session.
- Failure cases: outbox append failure rolls back the token and the user;
  pool exhaustion and statement timeout during authentication return 503,
  not 401; hash semaphore saturation returns 503 `overloaded` within the
  queue timeout; host restart keeps rate-limit state; a changed migration
  checksum blocks readiness.

### PR 2: organizations, RBAC, application scope, Directory (stage 2, first)

Depends on PR 1 and K2. It lands first in stage 2 because NT resolves
recipients through the `Directory`.

Scope: migration 2; the tenant and application routes of section 4; declared
application roles and the implicit `user` role; the RBAC evaluator
implementing `Authorizer`; version-cached grant snapshots; user and service
account API keys in the `Authenticator`; active organization selection; the
`Directory` implementation; the `platform-admin grant|revoke` CLI command;
integration events. Route enforcement is AP-C2's, which calls this
`Authorizer` (R6).

Acceptance criteria:

1. Every route in the PR 2 tables answers as specified.
2. Permission matrix: for each built-in role, a declared application role
   and a custom role, a table test asserts allow or deny for every identity
   permission and a sample of application permissions at application,
   tenant, tenant resource and application-level resource scope, and renders the matrix into the docs, as the
   TypeScript `policy.toMarkdown()` did. No tenant grant satisfies an
   `Application` check.
3. No path grants a permission the actor lacks (escalation tests for every
   grant path in section 4). An organization always keeps an owner.
4. Grant changes take effect on the next request on a second instance
   sharing the database, with no restart.
5. A key never exceeds its owner: a tenant key is denied outside its
   organization and at `Application`, an application key gets only its
   owner's application grants, revoking the owner's role shrinks the key on
   the next request, and a `platform_admin`'s key gets no break-glass. Keys
   die with their owner's membership, status or deletion.
6. The section 3 scope constraints reject every forbidden row shape,
   including organization A's custom role in a row of organization B.
7. An application with `user_can_create = false` works on the implicit
   `user` role and declared roles alone.
8. `Directory` returns only active verified users in bounded pages, and
   `is_member` is false for a holder of only application grants.
9. Ids from another organization return 404; integration events commit with
   the change, under R11 names.
10. X owns A and is admin in B: assigning or inviting with A's custom role in
    B returns `404 not_found`, and editing it changes no grant in B.
11. With `builtin_grants.user = ["tasks.read"]`, Bob's tenant key in K gets
    401 on both instances once he is removed, also after rejoining, and also
    when his membership row is deleted directly in SQL.
12. Anonymous acceptance of an invitation for an unverified account's
    address returns a session and leaves no attacker credential or session.

Test plan:

- Unit tests: wildcard expansion, every evaluation step of section 5, key
  intersection and key scope, slug rules, declared role sync.
- PostgreSQL integration: full route suite with two organizations and
  members in each, plus an application-only setup.
- Concurrency: two owners demote each other in parallel (one remains owner);
  two invitations accepted in parallel for one user (one membership,
  `already_member` for the other); custom role deleted while being assigned
  (`role_in_use` or success, never a dangling assignment); an application
  role revoked while its holder's key is in use (the next request is denied);
  key minting racing the minter's removal (no live key without membership).
- Failure cases: a failed role change does not bump versions; catalog shrink
  with stored roles holding removed permissions; a declared role removed
  while assigned (startup fails); API key presented after revocation (401
  plus audit `api_key.used_after_revocation`); a disabled service account's
  key (401).

PR 2 as built (2026-10-25):

- The permission matrix of acceptance 2 is
  [identity-permissions.md](identity-permissions.md), generated by
  `crates/identity/tests/rbac.rs`.
- Migration 2 is the SQL above plus `CHECK (cardinality(permissions) <= 256)`
  on `roles` and the indexes `organizations_created_by_idx`,
  `role_assignments_membership_idx`, `role_assignments_service_account_idx`,
  `application_role_assignments_user_idx`,
  `application_role_assignments_service_account_idx`, `api_keys_revoked_idx`
  and `api_keys_expiry_idx` that the per-user count, the snapshot queries and
  the cleanup job read.
- The catalog is identity's and the host's permissions plus the blocks' lists
  the binary hands the module (`IdentityModule::add_block_permissions`).
  `application.toml` `[[permissions]]` and route `resource` kinds are AP-C2's
  syntax; since AP-C2 they join through `add_application_permissions` and
  `add_tenant_resource_kinds` (`merezha_cli::identity_module`), beside the
  blocks' kinds (`content_type`, `add_resource_kinds`). Routes check their
  kinds inside the caller's organization only, so an application-level
  assignment on one fails `resource_kind_tenant_only` (422). Wildcards inside
  a block namespace are accepted only in `builtin_grants` (operator
  configuration), so a release adding a block permission never widens a
  custom role, a declared role or a key. `admin` holds every catalog
  permission except `identity.organization.delete`,
  `identity.organization.transfer` and the application-only identity ones.
- The RBAC `Authorizer` decides application routes since AP-C2, which
  checks each route's permission with the principal at its scope through
  `Authorizer::check` before any guest call; its payload hook `authorize`
  still refuses and is no longer called for application messages. Every
  other host check (application and jobs admin, `/v1/admin/status`,
  content) uses it too.
- Identity's own tenant routes evaluate the caller in the path's
  organization: a member's session gets that organization's snapshot
  without selecting it first; a caller with no standing there (no
  membership, no application grant) gets `404`, a member without the
  permission `403`.
- An application key never takes tenant grants: a `Merezha-Organization`
  header on one is `404` unless the key may enter any organization.
- Changing a member's roles, editing or deleting a custom role, and deleting
  an assignment require holding the permissions of every role involved,
  added and removed, old and new; removing a member who is an owner
  requires an owner. Custom role keys cannot be `owner`, `admin`, `member`,
  `platform_admin` or `user` (`role_key_taken`), `owner` is never assigned
  on a resource, and resource ids use identity characters (`[A-Za-z0-9._:/-]`).
- Routes that need a recent session refuse API keys with `403 unauthorized`.
  `GET /me` accepts a user's key.
- Removing a membership also clears that organization from the user's
  sessions. Accepting an invitation counts `organizations.max_members`
  (`organization_limit`) and, for a new account, refuses with
  `registration_closed` only when registration is `closed`. Anonymous
  acceptances sign in with session method `invitation`.
- `organization.entered` is audited once per session and organization per
  instance when a header enters one, and on every `PUT session/organization`
  that does; `authorization.denied` once per principal and permission per
  instance on identity's routes (both best effort: past 64 writes in flight
  a row is dropped and logged `identity.audit_dropped`);
  `authorization.break_glass` before the use, fail closed (§5). Keys are
  capped by their minter's live grants (§5). The reviews of PR #47 added the authority cleanup on removal and demotion, the
  inviter re-check at acceptance, the fail-closed break-glass audit, the
  live owner re-check, `is_member` for active verified users only and the
  `builtin_grants.user` wildcard rule.
- The cleanup job deletes expired invitations and keys revoked or expired
  more than 30 days ago. `platform-admin grant` needs a verified, active
  account. TE's catalog gains `identity.declared_role_in_use` and
  `identity.unknown_permissions`.
- The startup warning when `builtin_grants.user` grants a permission a
  route checks at `scope = "tenant"` landed with AP-C2's route declarations
  (`identity.user_grant_tenant_route`).

### PR 3: OAuth, OIDC and passkeys (stage 2)

Scope: migration 3, providers (Google, GitHub, generic OIDC), flows,
callback, linking rules, unlink, passkey registration, email-first and
discoverable authentication, reauthentication with a passkey, passkey
management, the `passkeys` cargo feature, `deny.toml` entries (MPL-2.0, the
scoped RUSTSEC-2023-0071 ignore).

Acceptance criteria:

1. Sign-in, sign-up and link work against an in-process mock OIDC provider
   (axum, test-only signing keys) and a mock GitHub API, with the real
   `openidconnect` and `oauth2` code paths.
2. Every linking rule in section 6 has a test, including `account_exists`,
   the pre-hijacking claim and `link_by_verified_email = true`.
3. Callbacks without the binding cookie, with a replayed state, with a wrong
   `iss`, with a wrong nonce, with an ID token signed by an unknown key, with
   `alg = none` or `HS256`, or with an expired token fail with the documented
   code and create no session.
4. Passkey registration and both authentication flows pass with
   `webauthn-authenticator-rs` soft passkeys. Ceremony state is single use
   and bound to user and session. Counter regression rejects sign-in.
5. A user cannot remove the last credential.
6. Provider outage returns `provider_unavailable` and readiness stays green.

Test plan:

- Unit tests: `decide_link` truth table, provider profile mapping (GitHub
  email selection with unverified primaries and missing emails), redirect
  URI construction.
- PostgreSQL integration: full OAuth and passkey route suites.
- Concurrency: the same callback delivered twice in parallel (one session);
  two parallel passkey authentications with one credential (counter CAS:
  both may succeed with increasing counters, the stored counter is the
  maximum, no lost update); parallel link of the same provider identity to
  two users (one `identity_already_linked`).
- Failure cases: JWKS fetch timeout; discovery document over 1 MiB; provider
  redirect response (not followed); ceremony expired between options and
  verify; user deleted mid-flow (flow cascade-deleted, callback
  `invalid_state`).

PR 3 as built (2026-10-25):

- Migration 3 is the SQL above plus bounds (`oauth_identities.email_normalized`
  at most 254 bytes, `pkce_verifier` 43 to 128 bytes, `nonce` and
  `return_to` bounded, at most 8 `transports`), checks that a link flow and
  a registration or reauthentication ceremony carry their session, and the
  indexes `oauth_flows_user_idx`, `oauth_flows_session_idx`,
  `webauthn_ceremonies_user_idx` and `webauthn_ceremonies_session_idx` the
  cascades and the claim read.
- Providers live in `crate::oauth`; `oauth::adapter` is the only module
  that touches `openidconnect`, `oauth2` and `chrono` (R14). Google's
  issuer and GitHub's endpoints can be replaced in code only
  (`IdentityModule::override_provider_endpoints`, for GitHub Enterprise
  Server and the tests' mock providers), never by request input or
  configuration. An OIDC `issuer` may be `http://` on a loopback host only
  on a private loopback listener (a development identity provider).
  GitHub sends no `iss`; one it sends is ignored, since GitHub has no
  issuer to compare.
- The callback's fragment codes add `provider_unavailable` for a provider
  unreachable at the code exchange. The start route answers
  `503 provider_unavailable` for any discovery failure (unreachable,
  redirect, error status, document over 1 MiB, mismatched `issuer`).
- Discovery fetches the key set with the document; an unknown `kid`
  refetches it at most once a minute per provider and instance, so a
  stream of tokens with foreign keys cannot make identity hammer the
  provider. `azp`, which `openidconnect` leaves to the client, is checked
  here: when present it must name this client. Tokens with audiences
  besides this client never reach that check, since `openidconnect`
  refuses them.
- Linking: rule 1 and rule 2 mark an unverified account verified, without
  the claim, when a trusted provider asserts its exact address (the
  identity was linked by someone holding a session on the account, which
  the pre-hijacking rule already required). Rule 1 on an unverified
  account answers `email_unverified` while verification is required.
  Rule 4 with an untrusted address and verification required creates the
  account unverified with the identity, sets the registration binding
  cookie and mails `identity.verify_email` in the same transaction, and
  lands on `email_unverified`; the link clicked in that browser verifies
  without a claim, so the identity stays. A sign-in through a trusted
  provider onto an unverified account (rule 3) runs the claim, which now
  also deletes passkeys, OAuth identities, pending OAuth link flows and
  WebAuthn ceremonies, in every flow that claims (verification without the
  binding, magic link, reset, invitation, OAuth).
- "Another credential remains" counts what can sign in under the running
  configuration: a password with password sign-in on, a passkey with
  passkeys on, an identity of a configured provider, or a verified address
  with magic links on (section 1).
- Security review of PR #55: starting a link needs a recent session and a
  link or a passkey registration sends `identity.sign_in_method_added`;
  passkey registration re-checks its session (share-locked) under the user
  lock, and a passkey removed mid sign-in refuses it
  (`invalid_credentials`); failed callbacks are throttled
  (`oauth_callback.ip`, and `oauth_callback_unknown.ip` for states naming
  no flow) and unknown states not audited; the link callback re-checks
  the reauthentication window; the binding cookie
  is cleared only by its own browser's callback once no flow of it is
  pending; rule 4 reuses a presented registration binding; failed
  discovery is remembered 30 s; identities store their issuer
  (`oauth_identities.issuer`, `oauth_flows_binding_idx`); `iss` is
  required when advertised; decoy credential ids are 16, 32 or 64 bytes.
- Ceremony ids are random UUIDv4 rendered `cer_<hex>`, not time ordered.
  `passkey.ip` is charged on the three options routes; a failed passkey
  reauthentication charges `reauth.session` like a wrong password.
  Reauthentication options for a user without passkeys answer
  `404 not_found`. Email-first answers drop `transports` and `extensions`
  so real and decoy answers have one shape; decoy ids come from a
  `passkey-decoy` key derived from `secret_key`. A lost compare-and-swap
  on a passkey's `revision` merges into a fresh read (counters only grow)
  and retries once; every passkey writer holds the user lock.
- Passkeys alone do not satisfy `no_sign_in_method` (ID7: they are
  registered from a signed-in session). New error codes:
  `invalid_ceremony`, `last_credential`, `identity_already_linked`,
  `passkey_exists`, `passkey_limit`, `passkey_rejected`, `passkeys_config`,
  `provider_unavailable`.
- Crates as listed in section 9, plus `chrono` 0.4.45 and `reqwest` as
  direct dependencies of the adapter, and `openssl` as a dev-dependency for
  the mock provider's test-only signing keys, generated at test start.
  `deny.toml` adds a scoped ignore of RUSTSEC-2026-0387 (`crypto-bigint`
  0.5 through `elliptic-curve` 0.13 in `openidconnect`), unreachable for
  the same reason as RUSTSEC-2023-0071: identity only verifies signatures
  with public keys.

## 11. Decisions

The owner answered this design's open questions on 2026-10-24.
[cross-block-decisions.md](cross-block-decisions.md) is the record; this
design applies them:

- ID1 automatic OAuth linking by verified email: off. ID2 minimum password
  length 12, configurable down to 8. ID3 sessions: 7 days idle, 30 days
  absolute, 10-minute reauthentication. ID4 no personal organization.
- ID5 application-level roles, application-scoped API keys and service
  accounts (R5, section 5).
- ID6 no TOTP. ID7 no passkey-only sign-up.
- ID8 registration `open` by default, or `invite_only` or `closed`. ID9 an
  OAuth authorization server for agents and MCP clients: a later decision.
  ID10 no phone numbers on users in v1.
- P1 framework amendments accepted (R15): the loopback development cookie
  and the password policy.

## Appendix: lessons from `@structure-ai/auth`, `auth-pg`, `authorization`

| Kept | Changed |
| --- | --- |
| Opaque 256-bit sessions, digests only at rest | Users are global per application with organizations as tenants, instead of every user row scoped by a tenant id. framework.md requires memberships, and per-tenant users cannot express one person in several organizations. |
| Atomic `DELETE ... RETURNING` consumption of tokens, states, challenges | Registration no longer leaks existence through a conflict error. |
| Enumeration-safe reset and magic-link requests, dummy hash on unknown accounts | Pre-hijacked unverified accounts are stripped on ownership proof instead of being marked verified with the attacker's password intact. |
| Tokens consumed by POST from application pages so link scanners do nothing | Tokens travel in URL fragments, not query strings. |
| Rate-limit keys hashed, caller-scoped anonymous walls, charge-on-failure via peek | Hashes are keyed HMACs, because a plain SHA-256 of an email is reversible with a list of emails. Storage and algorithm are fixed (PostgreSQL, GCRA) instead of an application port. |
| Account linking denied by default, link by provider subject | The email trust rule is per provider, with generic OIDC untrusted by default. |
| Strict passkey checks, UV required, counter rules, RP id and origins from configuration | Hand-written CBOR and COSE parsing replaced by webauthn-rs. |
| API keys with prefixed format, expiry, last use, immediate revocation | Power is the live intersection with the owner's permissions instead of a static scope list plus an application hook. Storage is SHA-256, as framework.md specifies, instead of HMAC with versioned peppers, which a 256-bit secret does not need. |
| Fail-closed policy, unknown permissions grant nothing, unmapped routes denied | Route declarations are checked at startup, so an unmapped route fails the boot instead of failing each request. |
| Scoped roles, explained decisions | Scopes are typed (application, tenant, resource) instead of opaque strings. Conditions are left to guests with the principal; the RBAC evaluator stays a pure set lookup. |
| Session expiry, single and all-session revocation | TS had absolute expiry only. Idle expiry, the reauthentication window and rotation on privilege change are added, per ASVS V7. |
| Origin check on mutations | CSRF token added, as framework.md requires. |
| Frozen base migration plus additive upgrades | Same rule through the `merezha-postgres` checksummed runner. |
