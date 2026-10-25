-- Deleted organization scopes (issue #56): one row per organization whose
-- content stopped being served and is being purged, kept once the purge is
-- done so the scope never answers again. Written by the
-- `identity.organization_deleted` subscription or by `merezha content
-- purge-organization`.
CREATE TABLE merezha_content.scopes_deleted (
  scope text PRIMARY KEY CHECK (scope ~ '^org:[A-Za-z0-9._:/-]{1,128}$'),
  source text NOT NULL CHECK (source IN ('event', 'operator')),
  state text NOT NULL CHECK (state IN ('purging', 'purged')),
  deleted_at timestamptz NOT NULL,
  purged_at timestamptz,
  CHECK ((state = 'purged') = (purged_at IS NOT NULL)));
