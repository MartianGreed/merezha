-- Reactor isolation counts only the guest's own transient failures
-- (issue #69). `failures` counts every failed batch and drives the backoff;
-- `guest_failures` counts the consecutive transient failures of the guest
-- call itself, so Directory, storage and notifier outages and the host's
-- own overload never send a healthy batch into one-at-a-time isolation.
-- A column rather than memory: every retry is a new run that any instance
-- may take, and the count must survive restarts. Forward-only.
ALTER TABLE merezha_app.reactors
    ADD COLUMN guest_failures integer NOT NULL DEFAULT 0 CHECK (guest_failures >= 0);
