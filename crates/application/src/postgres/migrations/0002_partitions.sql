-- Tenant-partitioned projections (AP §A.4, §A.7, slice C2). Forward-only.
-- A partitioned projection keeps one state per tenant and generation; the
-- row in `projections` carries the checkpoint, the CAS row revision and
-- the served generation. No row for a tenant means the configured initial
-- state. A rebuild writes the next generation and swaps it in atomically;
-- older generations are deleted in chunks after the swap.
CREATE TABLE merezha_app.projection_partitions (
    name text NOT NULL,
    revision integer NOT NULL,
    generation integer NOT NULL CHECK (generation > 0),
    tenant text NOT NULL CHECK (tenant ~ '^[A-Za-z0-9_-]{1,40}$'),
    state json NOT NULL,
    PRIMARY KEY (name, revision, generation, tenant),
    FOREIGN KEY (name, revision) REFERENCES merezha_app.projections ON DELETE CASCADE);

-- Each rebuild request is a new candidate. A run compares its candidate
-- with the row on every batch, so a rebuild abandoned and requested again
-- while the run still holds its lock is never continued as the old one.
ALTER TABLE merezha_app.projection_rebuilds
    ADD COLUMN candidate bigint GENERATED ALWAYS AS IDENTITY;
