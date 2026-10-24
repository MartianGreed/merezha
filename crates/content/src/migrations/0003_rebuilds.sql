-- Fallback rebuilds of `published` (C4): one row per model version whose
-- activation added a locale or changed a fallback, with the resume cursor
-- of its batches.
CREATE TABLE merezha_content.rebuilds (
  version integer PRIMARY KEY REFERENCES merezha_content.models (version),
  after_entry uuid,
  state text NOT NULL CHECK (state IN ('pending', 'done')),
  updated_at timestamptz NOT NULL);
