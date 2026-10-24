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
    last_signed_in_at  timestamptz,
    binding_hash       bytea CHECK (octet_length(binding_hash) = 32),  -- the registering browser, until verified
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
    phc         text NOT NULL,
    set_at      timestamptz NOT NULL
);

CREATE TABLE merezha_identity.sessions (
    id                   uuid PRIMARY KEY,
    token_hash           bytea NOT NULL CHECK (octet_length(token_hash) = 32),
    user_id              uuid NOT NULL REFERENCES merezha_identity.users (id) ON DELETE CASCADE,
    transport            text NOT NULL CHECK (transport IN ('cookie', 'bearer')),
    method               text NOT NULL CHECK (method IN ('password', 'magic_link', 'email_token', 'oauth', 'passkey', 'invitation')),
    created_at           timestamptz NOT NULL,
    authenticated_at     timestamptz NOT NULL,
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
CREATE INDEX sessions_absolute_expiry_idx ON merezha_identity.sessions (absolute_expires_at);

CREATE TABLE merezha_identity.one_time_tokens (
    id           uuid PRIMARY KEY,
    purpose      text NOT NULL CHECK (purpose IN ('email_verification', 'magic_link', 'password_reset', 'email_change')),
    token_hash   bytea NOT NULL CHECK (octet_length(token_hash) = 32),
    subject_key  text NOT NULL,
    user_id      uuid REFERENCES merezha_identity.users (id) ON DELETE CASCADE,
    email        text NOT NULL,
    email_normalized text NOT NULL,
    return_to    text CHECK (octet_length(return_to) <= 512),
    session_id   uuid,                                            -- email change: the requesting session
    created_at   timestamptz NOT NULL,
    expires_at   timestamptz NOT NULL,
    CONSTRAINT one_time_tokens_hash_key UNIQUE (token_hash),
    CONSTRAINT one_time_tokens_subject_key UNIQUE (purpose, subject_key)
);
CREATE INDEX one_time_tokens_expiry_idx ON merezha_identity.one_time_tokens (expires_at);
CREATE INDEX one_time_tokens_user_idx ON merezha_identity.one_time_tokens (user_id);

CREATE TABLE merezha_identity.rate_limits (
    bucket  bytea PRIMARY KEY CHECK (octet_length(bucket) = 32),
    tat     timestamptz NOT NULL
);
CREATE INDEX rate_limits_tat_idx ON merezha_identity.rate_limits (tat);

CREATE TABLE merezha_identity.audit_events (
    id               uuid PRIMARY KEY,
    occurred_at      timestamptz NOT NULL,
    action           text NOT NULL,
    outcome          text NOT NULL CHECK (outcome IN ('succeeded', 'failed', 'denied')),
    actor_kind       text CHECK (actor_kind IN ('user', 'api_key', 'system', 'anonymous')),
    actor_id         uuid,
    organization_id  uuid,
    target_kind      text,
    target_id        uuid,
    subject_hmac     bytea,
    ip               inet,
    user_agent       text CHECK (octet_length(user_agent) <= 512),
    request_id       text,
    details          jsonb NOT NULL DEFAULT '{}'::jsonb
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
