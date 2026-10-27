# Operating the notifications block

This guide is for the people who deploy and run a Merezha application with
`[modules.notifications]`. The reference for every key and route is
[platform.md](../architecture/platform.md#notifications-block); the design
is [notifications.md](../design/notifications.md). This page says what to
set up before production, what to watch, and what to do when something
goes wrong.

## Before the first deploy

Run the startup checks against the production configuration. They report
every problem at once and print no secret:

```sh
merezha notifications check --config merezha.toml
```

Exit 0 is valid (warnings such as `security_mail_disabled` are listed),
2 is invalid. `merezha run` makes the same checks and refuses to start on
the same errors. Among them: security topics cannot be capped or left
without a default channel, and identity's token mail must keep `email`
among its default channels.

**Keys.** `secret_key` (base64 of at least 32 bytes, from `env` or `file`,
never a literal) derives the unsubscribe token key, the SMS status URL key
and the webhook secret key. Losing it breaks every unsubscribe link already
sent and every stored webhook secret. Keep it in the secret store with the
database backups.

**Production refusals.** With `http.public = true` startup fails on a
`capture` driver, `egress.allow_private_networks`, `webhooks.allow_http`,
`templates.allow_http_links`, a non-https `brevo.base_url`, and template
previews without `preview_in_public`. These are development settings.

**Queues.** Queue settings live only under `[jobs.queues.<name>]`.
Start from:

```toml
[jobs.queues]
"notify.expand" = { concurrency = 4 }
"notify.email" = { concurrency = 8, max_attempts = 10, backoff_base_ms = 5000, backoff_max_ms = 3600000 }
"notify.sms" = { concurrency = 4, max_attempts = 6, backoff_base_ms = 5000, backoff_max_ms = 3600000 }
"notify.push" = { concurrency = 16, max_attempts = 6, backoff_base_ms = 5000, backoff_max_ms = 3600000 }
"notify.webhook" = { concurrency = 16, max_attempts = 10, backoff_base_ms = 1000, backoff_max_ms = 86400000 }
"notify.maintenance" = { concurrency = 1 }
```

Startup refuses a `notify.webhook` `backoff_max_ms` below 24 hours (the
webhook schedule's last step), a queue lease shorter than three provider
timeouts, and a `notify.maintenance` `timeout_ms` below 5 seconds
(`timeout_too_short`): the `retention` task starts no batch but a step's
first within 3 seconds of its deadline.

## Email through Brevo

1. Create a Brevo API key and authenticate the sending domain in Brevo.
   Set `[modules.notifications.email] driver = "brevo"` and `from`, and
   `[modules.notifications.brevo] api_key = { env = "BREVO_API_KEY" }`.
2. Set `public_base_url` to the public origin of the API. Unsubscribe
   links, the SMS status URL and the Brevo callback use it.
3. Choose a long random `brevo.webhook_token` and register the status
   callback once per environment:

   ```sh
   merezha notifications brevo register-webhook --config merezha.toml
   ```

   It prints the Brevo webhook id; the key and the token are never
   printed. Set `brevo.webhook_allowed_cidrs` to Brevo's published ranges
   (`1.179.112.0/20`, `172.246.240.0/20` at the time of writing).
4. Switch open and click tracking off in the Brevo console. The block
   stores no engagement data (NT3) and drops those events.
5. Check Brevo's behaviour on a real account before production use
   (issue #63; it needs an account key, so it is still open). The block
   assumes:
   - a reused `idempotencyKey` gets HTTP 400 `duplicate_parameter`, and a
     refused request does not take the key;
   - the `messageId` of the send response and the `message-id` /
     `messageId` of the status reports are written the same way (in
     particular with or without `<>` around email message ids). The block
     compares them exactly: a report whose id is not the current send's is
     taken for a report about an earlier send of a redelivered message and
     moves no status, so a format mismatch would stop every report from
     applying (issue #68);
   - for SMS: `recipient` without a leading `+`, the status report field
     names (`msg_status`, `messageId`, `ts_event`), `tag` as an array, and
     one space between `organisationPrefix` and the text.

   Design §12, "What could not be verified", lists them all.

Bounces, spam complaints, `invalid` and unsubscribes write suppressions.
`GET /v1/notifications/suppressions` lists them and an operator with
`notifications.suppressions.manage` adds or removes manual ones. An
`invalid` suppression blocks every topic, security mail included.

A 401, 402 or 403 from Brevo (a revoked key, no credits, an account under
validation) keeps mail `retrying` without spending attempts, logs one
`notification.provider_account_error` per run of refusals and counts
`merezha.notification.provider_account_errors`. Fix the account; queued
mail goes out on the next try, or fails `provider_account_unresolved` at
the notification's `expires_at` (24 hours after the first refusal when it
has none).

## SMS through Brevo

`sms.driver = "brevo"` needs `sender` and `public_base_url`. Users carry no
phone numbers in v1: SMS goes only to numbers the caller passes. A send that
may have reached Brevo before a crash is never resent (NT1): the delivery
becomes `unknown`, and a later status report can still move it to
`delivered` or `failed`. Do not redeliver an `unknown` SMS without checking
Brevo's logs first.

## Web Push

Generate a VAPID key pair once and keep the private key as a secret:

```sh
merezha notifications vapid generate
```

Changing the key makes every existing browser subscription unusable: they
are no longer targeted, and `push_expire` deletes them after 7 days unused.
Browsers subscribe again with the new public key. Only hosts in
`push.allowed_hosts` are accepted at subscribe.

During a rolling deploy of a new key, a push for a subscription of the
other key may be picked up by an instance that cannot sign for it. That
instance starts no attempt and retries the job (`notification.retry_scheduled`,
`vapid_key_mismatch`), so an instance holding the subscription's key sends
it; on the job's last attempt the push is skipped `no_subscription`. Each
such retry spends one of the `notify.push` queue's attempts, so keep the
rollout short (with two instances, about 98 % of these pushes go out within
6 attempts). The inbox keeps the durable copy either way.

## Webhooks

`[modules.notifications.webhooks]` turns outgoing webhooks on. Tenants
register endpoints under `/v1/notifications/tenants/{tenant}/webhooks`
(`notifications.webhooks.manage` in that organization), operators register
application-level ones under `/v1/notifications/webhooks`. Application
event types are declared in `webhooks.event_types`; content's
`content.entry_published`, `content.entry_unpublished` and
`content.entry_deleted` reach endpoints through the block's AP
subscription, so a publish in an organization reaches that organization's
endpoints and a publish of application content the application-level ones.

- **Receivers** verify the Standard Webhooks signature over the raw body,
  deduplicate on `webhook-id` (stable across retries and redeliveries) and
  reject timestamps more than 5 minutes off. During a secret rotation
  window two signatures are sent.
- **Retries** follow the Standard Webhooks schedule, about 3 days in all.
  A 410 disables the endpoint at once (`gone`). An endpoint failing for
  `auto_disable_after` (5 days) with at least 10 failed attempts is
  disabled (`failing`), logged `notification.webhook_endpoint_disabled`,
  and the tenant's `alert_roles` get `notifications.webhook_disabled`.
  Application-level endpoints alert `application_alert_roles`, which is
  empty by default: set it to the roles your operators hold, or watch the
  log event. Re-enabling an endpoint does not resend cancelled
  deliveries; redeliver them one by one.
- **Egress guard.** Webhook and push requests go only to public addresses,
  checked on the resolved IPs with redirects and proxies off. Add ranges
  with `egress.deny_cidrs`. There is no egress proxy in v1 (NT4).
- **Envelope size.** An event's envelope travels through the outbox, whose
  records are capped at 64 KiB, so `webhooks.max_payload_bytes` defaults
  to 65,536 and cannot be set higher; a larger event is refused
  `data_too_large` at `emit_webhook` (issue #76).
- **Audit.** Endpoint changes are kept 400 days in
  `merezha_notify.webhook_audit`. Managers read them, newest first, with
  `GET /v1/notifications/tenants/{tenant}/webhook-audit` (or
  `/v1/notifications/webhook-audit` for the application registry), 50 per
  page by default.
- **Rotating `secret_key`.** Move the old key to `secret_key_previous`,
  set the new one, deploy, and let the daily `retention` task reseal the
  webhook secrets (or trigger the `notify.maintenance.retention` schedule
  through the jobs admin API). Remove `secret_key_previous` only when
  `GET /v1/notifications/webhook-secrets/status` answers
  `previous_key_removable: true`. Unsubscribe tokens signed with the
  previous key keep working while it is configured.

## Deleted organizations

When identity deletes an organization, the block (subscribed to
`identity.organization_deleted`) records the tenant in
`merezha_notify.tenants_deleted` and purges, in short transactions, its
webhook endpoints (one audit row `delete` by `sys_notifications` each) and
webhook events with their deliveries, the push subscriptions made in it,
its inbox items, and its notifications with their deliveries, attempts and
provider events. Logs: `notification.organization_deleted` at the mark,
`notification.organization_purged` when the purge finished. From the mark
on, nothing of the tenant is written: its content events reach no
endpoint, its notifications expand to nothing (`notification.rejected`,
`tenant_deleted`), and creating an endpoint in its registry or a push
subscription in it answers 404. A send already past its first transaction
(one email, push or webhook request) may still go out. Users' preferences
and the endpoints' audit rows are kept. A purge job that dead-letters
leaves the rows at rest, still refused; replay it through the jobs admin
API.

## Rate cap per target

A transactional or marketing topic can set `max_per_target_per_hour` (1 to
10,000, unset by default). Security topics refuse it at startup: security
mail is always sent, so nobody can use up a cap on someone else's address
and silence their password reset.
Past that many deliveries of the topic to one target (an address, a phone
number, a push subscription or a user's inbox, per channel) within an
hour, further deliveries are `skipped` / `target_rate_limited`. It is a
guard against application bugs that loop, not a product quota; a sudden
rise of `merezha.notification.deliveries` with `notify.reason =
target_rate_limited` points at such a bug. Redelivery is never capped.

```toml
[modules.notifications.topics."tasks.assigned"]
category = "transactional"
channels = ["inbox", "push", "email"]
max_per_target_per_hour = 20
```

## Data kept, and for how long

| Data | Kept | Setting |
| --- | --- | --- |
| Notification `data` and recipient names | until `data_after_settled` after every delivery settled (`purge_data`, every 5 minutes) | `retention.data_after_settled` (`7d`) |
| Secret data (sign-in links, codes) | only in flagged outbox and job payloads, nulled at relay and at the job's end; never in the inbox | none |
| Notifications, deliveries, attempts | until `deliveries` after settling (`retention`, daily, chaining more runs while a backlog remains) | `retention.deliveries` (`90d`), at least `data_after_settled` |
| Provider events | until `deliveries` after receipt (`retention`) | `retention.deliveries` |
| Webhook bodies | `webhook_deliveries`, then nulled; a redelivery answers 410 `event_body_expired` | `retention.webhook_deliveries` (`30d`) |
| Webhook events, deliveries, attempts | until `deliveries`, once no delivery is open | `retention.deliveries` |
| Webhook audit rows | 400 days | none |
| Inbox items | `inbox.retention` (`180d`), archived ones `inbox.archived_retention` (`30d`) | `[modules.notifications.inbox]` |
| Captures (development) | the newest `captures` rows | `retention.captures` (`1000`) |
| A deleted organization's rows | purged when identity deletes it; the tenant id stays in `tenants_deleted` | none |

`retention` deletes in short transactions and, when a run's deadline
comes before the backlog is gone, queues another run at once, so a large
backlog (after raising traffic or shortening a setting) drains within
minutes rather than over days. Each step always runs its first batch, and
a run that deleted nothing queues no other. A repeating
`notification.maintenance_step_failed` with `task = retention` means it is
not keeping up.

A delivery whose job was lost (a handler panic, a dead letter nobody
replayed) would stay open, and so would its notification, which
`retention` then never deletes. `reclaim_stuck` (every 5 minutes) settles
every delivery still `queued`, `sending` or `retrying` whose `run_at` and
`updated_at` are both more than 24 hours old: `failed` / `job_lost`, or
`unknown` / `job_lost` when a send may have gone out (never resent by
itself), applying the provider events that waited for it. Webhook
deliveries open and due for as long become `failed` / `job_lost`. A rise
of `notify.reason = job_lost` means jobs are dying: look at the dead
letters of the `notify.*` queues.

A notification deleted by `retention` frees its `(source, dedupe_key)`: the
same key requested again afterwards makes a new notification. Identity's
keys name single-use tokens, so this matters only for callers that reuse
keys over months.

Dead jobs are kept 30 days by the jobs runtime. A dead job that carried
secret data cannot be replayed with it, and redelivering a notification
that had secret data answers 409 `secret_purged`: the user asks identity for
a new link instead.

## What to watch

Metrics (names from the telemetry catalog, labels from closed sets):

- `merezha.notification.deliveries` by `notify.status` and `notify.reason`:
  failures, bounces, `retries_exhausted`, `target_rate_limited`.
- `merezha.notification.latency`: request until accepted, per channel.
- `merezha.notification.provider_account_errors`: Brevo refuses the
  account; page someone.
- `merezha.webhook.attempts` by `notify.class` and
  `merezha.webhook.endpoints_disabled`.
- `merezha.notification.egress_blocked`: a tenant pointed an endpoint at a
  private address, or DNS changed under one.
- `merezha.webhook.secrets.previous_key` and `merezha.webhook.secrets.unreadable`
  during a key rotation.
- `merezha.inbox.sse.connections` and `merezha.inbox.sse.rejected` against
  `sse.max_streams` and `sse.max_streams_per_user`.
- The jobs runtime's queue depth and dead letters for the `notify.*`
  queues.

Log events worth an alert: `notification.provider_account_error`,
`notification.webhook_endpoint_disabled`, `notification.sse_listen_lost`
without a following `notification.sse_listen_restored`,
`notification.maintenance_step_failed` and
`notification.template_reload_failed`. No log line, span or metric holds an
address, a subject, a body, template data, secret data, a token or a URL
path.

## Development and CI

The default email driver is `capture`: the whole pipeline runs and the
rendered messages land in `merezha_notify.captures`, listed on loopback by
`GET /v1/notifications/dev/captures`. A plain-http development front end
needs `templates.allow_http_links = true`. CI sets
`MEREZHA_NOTIFY_NO_EGRESS=1`, which refuses every provider, push and
webhook destination that is not loopback.
