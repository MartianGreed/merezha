CREATE TABLE merezha_identity.oauth_identities (
    id                uuid PRIMARY KEY,
    user_id           uuid NOT NULL REFERENCES merezha_identity.users (id) ON DELETE CASCADE,
    provider          text NOT NULL CHECK (provider ~ '^[a-z][a-z0-9_]{0,31}$'),
    subject           text NOT NULL CHECK (octet_length(subject) BETWEEN 1 AND 255),
    issuer            text NOT NULL CHECK (octet_length(issuer) BETWEEN 1 AND 512),  -- subjects are scoped by it
    email_normalized  text CHECK (octet_length(email_normalized) <= 254),  -- as asserted at last sign-in
    email_verified    boolean NOT NULL,
    created_at        timestamptz NOT NULL,
    last_used_at      timestamptz,
    CONSTRAINT oauth_identities_provider_subject_key UNIQUE (provider, subject),
    CONSTRAINT oauth_identities_user_provider_key UNIQUE (user_id, provider)
);

CREATE TABLE merezha_identity.oauth_flows (
    state_hash     bytea PRIMARY KEY CHECK (octet_length(state_hash) = 32),
    binding_hash   bytea NOT NULL CHECK (octet_length(binding_hash) = 32),
    provider       text NOT NULL CHECK (provider ~ '^[a-z][a-z0-9_]{0,31}$'),
    intent         text NOT NULL CHECK (intent IN ('sign_in', 'link')),
    user_id        uuid REFERENCES merezha_identity.users (id) ON DELETE CASCADE,
    session_id     uuid REFERENCES merezha_identity.sessions (id) ON DELETE CASCADE,
    pkce_verifier  text NOT NULL CHECK (octet_length(pkce_verifier) BETWEEN 43 AND 128),
    nonce          text CHECK (octet_length(nonce) <= 128),   -- OIDC providers only
    return_to      text CHECK (octet_length(return_to) <= 512),
    created_at     timestamptz NOT NULL,
    expires_at     timestamptz NOT NULL,
    CHECK ((intent = 'link') = (user_id IS NOT NULL)),
    CHECK ((intent = 'link') = (session_id IS NOT NULL))
);
CREATE INDEX oauth_flows_expiry_idx ON merezha_identity.oauth_flows (expires_at);
CREATE INDEX oauth_flows_binding_idx ON merezha_identity.oauth_flows (binding_hash);
CREATE INDEX oauth_flows_user_idx ON merezha_identity.oauth_flows (user_id) WHERE user_id IS NOT NULL;
CREATE INDEX oauth_flows_session_idx ON merezha_identity.oauth_flows (session_id) WHERE session_id IS NOT NULL;

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
    transports      text[] NOT NULL DEFAULT '{}' CHECK (cardinality(transports) <= 8),
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
    expires_at  timestamptz NOT NULL,
    CHECK ((purpose IN ('registration', 'reauthentication')) = (session_id IS NOT NULL)),
    CHECK (purpose = 'discoverable' OR purpose = 'authentication' OR user_id IS NOT NULL)
);
CREATE INDEX webauthn_ceremonies_expiry_idx ON merezha_identity.webauthn_ceremonies (expires_at);
CREATE INDEX webauthn_ceremonies_user_idx ON merezha_identity.webauthn_ceremonies (user_id) WHERE user_id IS NOT NULL;
CREATE INDEX webauthn_ceremonies_session_idx ON merezha_identity.webauthn_ceremonies (session_id) WHERE session_id IS NOT NULL;
