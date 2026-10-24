-- Fallback rebuilds of `published` (C4): one row per model version whose
-- activation added a locale, changed a fallback or carried an older pending
-- rebuild forward (that one turns `superseded`), with the resume cursor of
-- the background job's batches.
CREATE TABLE merezha_content.rebuilds (
  version integer PRIMARY KEY REFERENCES merezha_content.models (version),
  after_entry uuid,
  state text NOT NULL CHECK (state IN ('pending', 'done', 'superseded')),
  updated_at timestamptz NOT NULL);
