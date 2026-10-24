CREATE SCHEMA merezha_content;
SET LOCAL search_path = merezha_content;      -- ends with the migration's transaction

-- Every activated model version; exactly one is active.
CREATE TABLE models (
  version integer PRIMARY KEY CHECK (version > 0),
  fingerprint text NOT NULL UNIQUE,              -- SHA-256 hex of canonical JSON
  definition jsonb NOT NULL,                     -- canonical normalized model
  state text NOT NULL CHECK (state IN ('active', 'superseded')),
  activated_at timestamptz NOT NULL);
CREATE UNIQUE INDEX models_one_active ON models ((true)) WHERE state = 'active';

-- Synchronized from the model at activation; rows are never deleted.
CREATE TABLE locales (
  code text PRIMARY KEY CHECK (length(code) BETWEEN 2 AND 35),
  fallback text REFERENCES locales (code),
  is_default boolean NOT NULL DEFAULT false,
  retired_at timestamptz);                       -- set by retire_locale (C10)
CREATE UNIQUE INDEX locales_one_default ON locales ((true)) WHERE is_default;

CREATE TABLE entries (
  id uuid PRIMARY KEY,                           -- UUIDv7, host generated or client supplied
  scope text NOT NULL CHECK (scope = 'app' OR scope ~ '^org:[A-Za-z0-9._:/-]{1,128}$'),
  content_type text NOT NULL,
  singleton boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL, created_by text NOT NULL,
  deleted_at timestamptz, deleted_by text,
  UNIQUE (id, scope, content_type));             -- target of the composite keys below
CREATE INDEX entries_by_type ON entries (scope, content_type, id) WHERE deleted_at IS NULL;
CREATE UNIQUE INDEX entries_one_singleton ON entries (scope, content_type)
  WHERE singleton AND deleted_at IS NULL;
CREATE INDEX entries_trash ON entries (deleted_at) WHERE deleted_at IS NOT NULL;

CREATE TABLE variants (
  entry_id uuid NOT NULL,
  locale text NOT NULL REFERENCES locales (code),
  scope text NOT NULL, content_type text NOT NULL,   -- copied from entries, kept equal by the key below
  deleted boolean NOT NULL DEFAULT false,
  draft_revision bigint NOT NULL CHECK (draft_revision > 0),
  draft_data jsonb NOT NULL,                     -- includes shared fields copied from the default variant
  draft_slug text,
  draft_updated_at timestamptz NOT NULL, draft_updated_by text NOT NULL,
  published_revision bigint CHECK (published_revision <= draft_revision),
  published_at timestamptz, first_published_at timestamptz,
  PRIMARY KEY (entry_id, locale),
  FOREIGN KEY (entry_id, scope, content_type)
    REFERENCES entries (id, scope, content_type) ON DELETE CASCADE);
CREATE INDEX variants_list ON variants (scope, content_type, locale, draft_updated_at DESC, entry_id)
  WHERE NOT deleted;
CREATE INDEX variants_data ON variants USING gin (draft_data jsonb_path_ops) WHERE NOT deleted;

CREATE TABLE revisions (
  entry_id uuid NOT NULL, locale text NOT NULL,
  revision bigint NOT NULL CHECK (revision > 0),
  model_version integer NOT NULL REFERENCES models (version),
  kind text NOT NULL CHECK (kind IN ('create', 'save', 'restore')),
  restored_from bigint,
  data jsonb NOT NULL,                           -- the variant's own fields, without shared fields
  created_at timestamptz NOT NULL, created_by text NOT NULL,
  PRIMARY KEY (entry_id, locale, revision),
  FOREIGN KEY (entry_id, locale) REFERENCES variants ON DELETE CASCADE);

-- Delivery rows, materialized at publish time: one row per entry and per
-- configured locale whose fallback chain reaches a published variant.
CREATE TABLE published (
  entry_id uuid NOT NULL,
  locale text NOT NULL REFERENCES locales (code),        -- requested locale
  source_locale text NOT NULL REFERENCES locales (code), -- variant actually served
  scope text NOT NULL, content_type text NOT NULL,
  revision bigint NOT NULL,
  slug text,
  data jsonb NOT NULL,                           -- source variant data plus shared fields
  published_at timestamptz NOT NULL, first_published_at timestamptz NOT NULL,
  PRIMARY KEY (entry_id, locale),
  FOREIGN KEY (entry_id, scope, content_type)
    REFERENCES entries (id, scope, content_type) ON DELETE CASCADE);
CREATE INDEX published_list ON published (scope, content_type, locale, published_at DESC, entry_id);
CREATE INDEX published_slug ON published (scope, content_type, locale, slug) WHERE slug IS NOT NULL;
CREATE UNIQUE INDEX published_own_slug ON published (scope, content_type, locale, slug)
  WHERE slug IS NOT NULL AND source_locale = locale;
CREATE INDEX published_data ON published USING gin (data jsonb_path_ops);

-- Outgoing references from drafts and from published variants.
CREATE TABLE refs (
  scope text NOT NULL,
  from_entry uuid NOT NULL REFERENCES entries (id) ON DELETE CASCADE,
  from_locale text NOT NULL,
  stage text NOT NULL CHECK (stage IN ('draft', 'published')),
  path text NOT NULL,                            -- JSON pointer, e.g. /fields/body/c3
  target_kind text NOT NULL CHECK (target_kind IN ('entry', 'asset')),
  target_id uuid NOT NULL,
  PRIMARY KEY (from_entry, from_locale, stage, path, target_id));
CREATE INDEX refs_by_target ON refs (target_kind, target_id, stage);

CREATE TABLE blobs (
  scope text NOT NULL,
  sha256 text NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
  storage_key text NOT NULL UNIQUE,
  size bigint NOT NULL CHECK (size >= 0),
  content_type text NOT NULL,
  state text NOT NULL CHECK (state IN ('pending', 'stored', 'orphaned', 'deleting')),
  pending_until timestamptz,                     -- upload lease
  orphaned_at timestamptz, created_at timestamptz NOT NULL,
  PRIMARY KEY (scope, sha256));
CREATE INDEX blobs_cleanup ON blobs (state, orphaned_at, pending_until) WHERE state <> 'stored';

CREATE TABLE assets (
  id uuid PRIMARY KEY,
  scope text NOT NULL, sha256 text NOT NULL,
  filename text NOT NULL CHECK (filename ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$'),
  content_type text NOT NULL, size bigint NOT NULL,
  width integer, height integer,
  title text, focal_point jsonb,                 -- {"x": 0..1, "y": 0..1}
  created_at timestamptz NOT NULL, created_by text NOT NULL,
  updated_at timestamptz NOT NULL, deleted_at timestamptz,
  FOREIGN KEY (scope, sha256) REFERENCES blobs (scope, sha256));
CREATE INDEX assets_list ON assets (scope, created_at DESC, id) WHERE deleted_at IS NULL;
CREATE INDEX assets_by_blob ON assets (scope, sha256);

-- Bumped on every change visible to delivery in a scope; feeds ETags.
CREATE TABLE generations (scope text PRIMARY KEY, generation bigint NOT NULL);
