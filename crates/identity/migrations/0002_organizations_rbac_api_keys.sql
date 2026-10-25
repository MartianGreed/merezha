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
CREATE INDEX organizations_created_by_idx ON merezha_identity.organizations (created_by);

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
    CHECK (cardinality(permissions) <= 256),
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
CREATE INDEX role_assignments_membership_idx ON merezha_identity.role_assignments (membership_id);
CREATE INDEX role_assignments_service_account_idx ON merezha_identity.role_assignments (service_account_id);
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
CREATE INDEX application_role_assignments_user_idx
    ON merezha_identity.application_role_assignments (user_id);
CREATE INDEX application_role_assignments_service_account_idx
    ON merezha_identity.application_role_assignments (service_account_id);

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
CREATE INDEX api_keys_revoked_idx ON merezha_identity.api_keys (revoked_at) WHERE revoked_at IS NOT NULL;
CREATE INDEX api_keys_expiry_idx ON merezha_identity.api_keys (expires_at);

ALTER TABLE merezha_identity.sessions
    ADD COLUMN active_organization_id uuid
    REFERENCES merezha_identity.organizations (id) ON DELETE SET NULL;
