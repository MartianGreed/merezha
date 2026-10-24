# Cross-block decisions

This record settles where the block designs disagreed with each other or with
[framework.md](../architecture/framework.md), and records the owner's answers to
their open questions (2026-10-24). When a design and this record differ, this
record wins. Each design keeps the detail; this file names the owner of every
shared contract.

Abbreviations: FW framework.md, K the kernel (`merezha-postgres`,
`merezha-platform`, PR #14), AP [application-platform](application-platform.md),
ID [identity](identity.md), NT [notifications](notifications.md),
CT [content](content.md), TE [telemetry](telemetry.md).

## Shared contracts

The kernel ports in PR #14 predate these decisions. A contract-alignment PR (K2)
reshapes them before stage 1 starts.

### R1. Outbox and jobs

AP §B.3 owns the API. Every module writes outbox records inside the caller's
transaction:

```rust
outbox.append(tx, source, &[OutboxRecord { dedupe_key, target }], OnDuplicate)
// target: OutboxTarget::Event { topic } | OutboxTarget::Job { queue, kind }
```

`JobsHandle::enqueue` keeps its own-transaction form for callers without a
transaction. NT's expansion step writes its delivery jobs as
`OutboxTarget::Job` records inside the expansion transaction, and maps its
Done/Retry/Permanent outcomes onto AP's `JobFailure` classification. Queue
settings (concurrency, backoff, attempts) live only under `[jobs.queues.<name>]`.
The webhook queue sets `backoff_max_ms = 86400000` so NT's 24-hour retry ladder
fits under AP's cap.

### R2. Notifier

NT §3.1 owns the port, made object-safe with `BoxFuture`:

- `request(tx, NotificationRequest)` and `emit_webhook(tx, WebhookEvent)`, both
  inside the caller's transaction.
- `NotificationRequest` gains `secret_data`: values such as sign-in links and
  codes. It is rendered into the message, never stored in the inbox, never
  logged, never exported to telemetry, and nulled at the terminal state (R3).

ID owns the identity topic list; NT predeclares every one of them under
`identity.`: `identity.verify_email`, `identity.password_reset`,
`identity.magic_link`, `identity.account_exists`, `identity.password_changed`,
`identity.email_change_confirm`, `identity.email_change_notice`,
`identity.invitation`, `identity.new_sign_in`.

### R3. Secrets in durable rows

- Outbox records carry a `contains_secret` flag. The relay nulls the payload of
  a flagged record when it marks it relayed.
- Jobs created from a flagged record inherit the flag. The jobs runtime nulls
  their payload at the terminal state (succeeded, dead, cancelled). A
  dead-lettered flagged job cannot be replayed with its secret; replay
  re-requests the notification from the owning module or is refused.
- NT's separate one-hour purge of link data is removed.

### R4. Reactions and effects

NT's vocabulary wins; AP §C.2 and §C.5 adopt it:

- Effect fields: `effect`, `recipients`, `topic`, `event_type`, `data`.
- Channel name `inbox`. Recipients: `User { id }`,
  `Tenant { id, roles }`, `Application { roles }` (R5), and `Address { .. }`
  only when the reactor sets `allow_addresses = true` (default false).
- AP calls `Notifier::request` / `emit_webhook` inside the reactor checkpoint
  transaction; it never writes NT's jobs directly.
- Dedupe key `reaction:{reactor}:{event_id}:{index}`. Source `application`.
- A malformed guest reply halts the reactor (retry with backoff, alert),
  whatever its `on_error` setting. A well-formed effect that fails validation
  is recorded as rejected and the reactor advances.
- Callers resolve recipients and membership through the Directory before
  opening their transaction; no port call inside a transaction waits on
  another pooled connection.
- For requests from sources `application` (reactions) and `api`, NT checks that
  every user recipient is a member of the named tenant through
  `Directory::is_member` and rejects with `recipient_not_in_tenant`. Identity's
  own requests are exempt, since an invitation targets a user who is not a
  member yet.
- Reactions never re-fire on projection rebuild (AP §C.5 stands).

### R4a. Dedupe keys and purged secrets

- Dedupe keys supplied over HTTP (source `api`) are namespaced by the calling
  principal: `api:{subject}:{key}`.
- An outbox dedupe key is at most 160 bytes, so derived job keys
  (`source:dedupe_key`) stay under the 200-byte job key limit. Longer keys are
  refused at `append`, never at relay.
- Replaying or reading a record whose secret was nulled returns 409
  `secret_purged` in every module.

### R5. Permission scopes (owner answer ID5)

ID adds application-level grants so apps without organizations work:

- Scopes: `Application` (the whole app), `Tenant(id)` (an organization), and
  `Resource { tenant: Option<id>, kind, id }`; a resource without a tenant is
  an application-level resource, so per-type grants work on
  application-scoped content. K renames `ResourceScope::Global` to
  `Application`.
- Role assignments can be made at application scope. API keys and service
  accounts can be application-scoped or tenant-scoped; a key never exceeds its
  owner's grants.
- `platform_admin` stays the break-glass grant that satisfies every check.
- Every check the designs called "global" (AP admin API and global routes, CT
  application-scoped content, NT admin and `notifications.send`, TE
  `system.status.read`) is an `Application` scope check.

### R6. Route permissions

AP §C.4 owns the syntax and enforcement:

```toml
[[commands]]
tag = "create-task"
permission = "tasks.create"
scope = "tenant"            # "application" | "tenant"
resource = "task"           # optional, from ID
# or: access = "public" | "authenticated"  (xor permission)
```

The same keys apply to `[[queries]]`, except `resource`: it names the
command's stream, so it is for commands only.

- Enforcement happens in the host before any guest call (AP-C2). ID PR 2 drops
  its `RouteAuthorizer`.
- The Rust type is K's `ResourceScope`.
- A principal acting for another actor gets 403 `actor_mismatch` everywhere.

### R7. Directory

One batch port in K, added to FW's port table:

```rust
users(ids) -> contacts           // email, display name, locale; no phone numbers
tenant_members(tenant, roles?)   // bounded page
application_members(roles?)      // bounded page
is_member(user, tenant) -> bool
```

ID implements it. It returns verified addresses only, so token mail
(verification, email change) goes to an explicit `Address` recipient instead.
NT's `RecipientDirectory` is replaced by it. Phone numbers are
not stored on users in v1 (owner answer ID10); SMS goes to explicit numbers.

### R8. Errors

- K grows a registry mapping error codes to HTTP statuses. Modules register
  their codes at startup; an unregistered permanent code stays 422. A code has
  one status in every module: `validation_failed` is 422 and
  `revision_conflict` 412.
- Codes are unprefixed lower snake case. Canonical names: `unauthenticated`
  (401), `unauthorized` (403, replacing `forbidden` and `permission_denied`),
  `actor_mismatch` (403), `tenant_required` (403), `body_limit` (413),
  `rate_limited` (429 with `Retry-After`), `overloaded` (503 with
  `Retry-After`).
- FW's error shape gains optional `details` for field-level validation errors:
  `{error: {code, classification, details?}}`.

### R9. Telemetry names

TE's `names.rs` is the single catalog of metric, span, attribute and log event
names. Other designs reference it instead of defining names:

- Metric durations in seconds; log fields keep `duration_ms`. Jobs metrics are `merezha.job.*` with TE's state names.
- Notification metrics use `topic` as the label (topics are a declared, bounded
  set); `template` is not a label.
- Every block attribute is prefixed by its module (`identity.*`, `content.*`,
  `notify.*`, `job.*`) so TE's allowlist keeps it; host attributes use
  `merezha.*`, and OpenTelemetry semantic-convention keys (`http.route`,
  `error.type`, `user.id`) keep their standard names. TE adds the content rows.
- Session ids never appear in telemetry. User ids are off by default; tenant ids
  are on (owner answer TE1).
- Log event names follow TE (`job.dead_lettered`, not `jobs.dead_lettered`).

### R10. Trace context, readiness, status

- `merezha_app.events` and the jobs tables get `trace_parent` and `trace_state`
  columns in AP's A2 and B1 migrations. `append()` takes the trace context from
  the current span; job retries continue the original trace.
- Readiness may wait for a warm-up (AP `warm_readiness`). After warm-up,
  projection lag never affects readiness. Job lag counts only when
  `jobs.readiness.max_lag_ms` is set (unset by default).
- TE's `/v1/admin/status` reads AP's application-status and queue data rather
  than querying the tables again.

### R11. Domain events and webhooks

- Event type names are lower snake case: `<module>.<aggregate>_<past verb>`,
  for example `identity.user_registered`, `content.entry_published`.
- AP defines the event envelope, starting from ID §8's.
- NT subscribes to module events through AP subscriptions. Modules never call
  `emit_webhook` for their own domain events.
- Webhook endpoints can be registered per tenant or at application level (owner
  answer NT6), so application-scoped content can deliver webhooks.

### R12. Configuration

- K adds a top-level `[jobs]` section and a duration type that accepts
  `"500ms"`, `"30s"`, `"15m"`, `"7d"` as well as integer milliseconds in keys
  ending `_ms`.
- One instance id: `app.instance_id` (default: hostname plus process id).
  `[app]` also takes an optional `version`, TE's `service.version`.
- A module is enabled by the presence of its `[modules.<name>]` section; designs
  drop their `enabled` keys.
- AP uses K's `[runtime] manifest` and `[application] path`, and extends
  K's `[application]` with its `projections`, `reactors`, `background` and
  `append` tables.
- TE extends K's `[telemetry]` (`log_level`, `format`) instead of adding
  `[telemetry.stderr]`. Logs go to stderr (owner answer P3).

### R13. Principal and blob storage

- K adopts ID's Principal: `user`, `Arc<GrantSnapshot>` for roles and
  permissions per scope, and `SessionFacts` (session id, authentication time,
  method) kept out of serialisation and telemetry.
- System principals use `sys_<name>` subjects everywhere (reactors are
  `sys_reactor_<name>`).
- K adopts CT's `BlobStore`: streaming put and get, `head`, delete, and signed
  URLs where the backend supports them.

### R14. Dependencies and platform floor

- `sha2` 0.11 everywhere (K bumps from 0.10); `hmac` 0.13.
- One time crate: `jiff`. `croner` is used with its `jiff` feature, so cron
  needs no `chrono`. `chrono` appears only inside the OAuth/OIDC provider
  adapter, because `oauth2` and `openidconnect` require it, and never in a
  public type.
- Workspace `jiff` pin: `default-features = false`, features `std`, `serde`,
  `tz-system`, `tzdb-bundle-always`, so IANA zones resolve on Linux too.
- PostgreSQL 14 minimum, CI on 16 (owner answer P2). The Contremaitre
  environment may keep 17.

### R15. Framework amendments (owner answer P1)

FW records these as accepted deviations:

- ID: the loopback-only development cookie drops `__Host-` and `Secure`;
  password policy is length plus breached-password check, no composition rules.
- CT: index builds with `CREATE INDEX CONCURRENTLY` run outside the migration
  transaction through a declared non-transactional migration step, and gate
  readiness until done. K2 adds that step to the migration runner: one
  statement, no transaction, recorded only after it succeeds, and safe to
  retry (`IF NOT EXISTS`, invalid index dropped and rebuilt).
- TE: `/health/startup`, an early listener that answers health before modules
  start, admin status under `/v1/admin/status`, logs on stderr.
- K/AP: the Directory port (R7), the error registry and `details` (R8).

## Deferred from v1

The PR #15 review flagged these as beyond the owner's scope. They stay in the
designs as later slices, outside the v1 delivery order: notification quiet
hours, scheduled publishing in content, and content model migrations with
`convert` operations (v1 supports additive model changes and refuses the rest).
"Additive" means the change rewrites no stored data: new types and fields,
tightened validation, and `access`, `filterable` or `sortable` changes activate
in v1; adding `unique` to an existing field does not. During a rolling deploy an
old instance serves the old `access` rules until it drains. Later slices:
NT-N9 (quiet hours), CT-C8 (scheduled publishing), CT-C10 (model migrations).

## Owner answers

Platform: P1 accept the amendments. P2 PostgreSQL 14 minimum, CI on 16.
P3 logs on stderr.

Identity:

- ID1 automatic OAuth account linking by verified email: off.
- ID2 minimum password length 12, configurable down to 8.
- ID3 sessions: 7 days idle, 30 days absolute, 10-minute reauthentication window.
- ID4 personal organization at sign-up: no.
- ID5 application-level roles plus application-scoped API keys and service
  accounts (R5).
- ID6 TOTP: no. ID7 passkey-only sign-up: no.
- ID8 registration mode default: open, configurable to invite-only or closed.
- ID9 OAuth authorization server for agents and MCP clients: separate decision
  later.
- ID10 phone numbers on users: not in v1.

Notifications:

- NT1 an SMS send that is ambiguous after a crash is not resent.
- NT2 marketing sends through Brevo transactional: opt-in only, capped at 1,000
  targets per send.
- NT3 no open or click tracking. NT4 no egress proxy in v1. NT5 per-tenant
  templates deferred.
- NT6 application-level webhooks: yes, through an application-level endpoint
  registry (R11).

Content:

- CT1 delivery access has no default: every content type must declare
  `access` (`public`, `authenticated`, or a permission). A type without it fails
  model validation.
- CT2 no presigned uploads in v1. CT3 no image transformations in the host.
  CT4 no EXIF/GPS stripping in v1. CT5 full-text search later. CT6 no guest
  hooks on entries. CT7 batch publish only, no named releases.

Application platform:

- AP1 about 300 to 2,000 commits per second per application is acceptable.
- AP2 no SQLite import: SQLite stays a development and test store.
- AP3 reactors cannot read projection state.
- AP4 the HTTP adapter fills in missing correlation and message ids.

Telemetry: TE1 user ids off in traces by default, tenant ids on.

Decided internally, with the designs' recommendations: receipts kept forever;
roles not stored in event metadata; no per-key FIFO jobs; asynchronous rebuild
on PostgreSQL; reactors start at head; reactor `on_error = halt`; partitions by
tenant only; dead letters kept 30 days; `jobs.workers = false` included; webhook
effects target registered endpoints only; Brevo idempotency verified in a
sandbox before NT-N3; no CSS inlining; strict push host allowlist;
per-recipient rate limit optional and unset; per-type content permissions
through ID resource scope; changing `localized` or `scope` on a live content
type unsupported; 100 revisions kept per entry variant; one content model file;
`deunicode` accepted; `OTEL_*` environment variables ignored; sampling ratio
1.0; `app.id` under `[app]`; Grafana JSON plus Prometheus rules; collector mTLS,
exemplars deferred; one telemetry provider set per application.

## Delivery order

Slice ids are the designs' own; AP-C* and CT-C* disambiguate the two C series.

- **K** PR #14, the kernel.
- **K2** contract alignment in K: R1 Outbox shape, R2 Notifier, R5 scopes, R7
  Directory, R8 error registry, R12 `[jobs]`, durations, `app.instance_id`,
  `app.version` and the `[application]` and `[telemetry]` extensions, R13 Principal and
  BlobStore, R14 `sha2`, and the R15 non-transactional migration step.
- **Stage 1**
  - First: AP-A1, AP-B1 (with R10 columns), TE-T1.
  - Then: AP-A2, AP-B2, AP-B3, AP-A3, TE-T2 to TE-T5; ID PR 1 (needs AP-B3),
    CT-C1, NT-N1. (AP-A4, the SQLite import, is dropped by AP2.)
- **Stage 2**
  - First: ID PR 2 (Directory implementation, application scope).
  - Then: NT-N2 and NT-N3 (production email for identity), NT-N5, NT-N4, NT-N6,
    NT-N7; ID PR 3; CT-C6, then CT-C2 to CT-C5, CT-C7; the identity and
    content parts of NT-N8. TE-T6 metric rows land inside each of these PRs.
- **Stage 3**: AP-C1, AP-C2, AP-C3 with NT-N8's reaction part; CT-C9, TE-T7;
  `merezha new` and the example application.

Ownership to avoid duplicated work: jobs gauges belong to TE-T5, not AP-B2;
route enforcement belongs to AP-C2, not ID PR 2; TE's status endpoint reads AP's
admin data.
