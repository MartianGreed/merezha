# Project scaffold and example application

Status: proposed 2026-10-27. Conforms to [the framework architecture](../architecture/framework.md)
("Project shape", stage 3 of "Delivery order") and to
[the cross-block decisions](cross-block-decisions.md), which win where this document differs.
Every configuration key below is one of [platform.md](../architecture/platform.md)'s, or of
[notifications §7.2](notifications.md#72-modulesnotifications), which platform.md names as the
reference for that block's keys. This design adds no configuration key. It adds four commands
(`merezha new`, `merezha build`, `merezha componentize`, `merezha version`), two crates and one
project file (`.merezha-version`), and it asks the content block for one behaviour change
(section 9, SC-Q4).

Stage 3 has three items left: `merezha new`, an example application that uses every block, and
an operations guide. This document designs the first two. The notifications part of the
operations guide is NT-N8's (PR #77, `docs/operations/notifications.md` once it merges); the
generated README points to it and nothing here repeats it.

The requirements other designs already placed on the scaffold:

| Source | Requirement | Where it is met |
| --- | --- | --- |
| [framework.md](../architecture/framework.md), Project shape | `merezha.toml`, runtime and application manifests and guest crates in the project repository; the prebuilt host runs it; blocks switched on in configuration | 3.1 |
| [notifications.md §7.2](notifications.md#72-modulesnotifications) | `merezha new` writes the block's `[jobs.queues]` settings | 3.2 |
| [notifications.md §11, N8](notifications.md#11-delivery-slices) | the end-to-end example application, from guest event to reaction to inbox item to SSE event, stays with stage 3 | 5.10, SC-S5 |
| [content.md §12, CT1](content.md#12-decisions) | `merezha new` writes `access = "public"` on scaffolded site types | 3.6, 5.5 |
| [telemetry.md §6](telemetry.md#6-local-development-and-production-backends) | the scaffold writes `ops/telemetry/lgtm.Dockerfile` and copies the dashboards into `ops/telemetry/` | 3.10, SC-S4 |
| [platform.md, Limits and follow-ups](../architecture/platform.md#limits-and-follow-ups) | reactors get a process test with a Wasm reactor component; the example wires a reactor with an `ArchiveTask` route | 5.7, 5.10 |

## Decision summary

| Id | Topic | Decision |
| --- | --- | --- |
| SC-D1 | Command | `merezha new NAME [--dir PATH] [--template starter\|teamboard] [--blocks LIST] [--merezha-rev REV \| --merezha-path PATH] [--no-secrets]`. It never overwrites: the target must not exist or must be an empty directory. It never touches the network and runs no subprocess. |
| SC-D2 | Templates | Two templates. `starter` (default) is a small notes application whose blocks are chosen with `--blocks`. `teamboard` is the example application with every block; it takes no `--blocks`. |
| SC-D3 | Embedding | A build script in the new crate `merezha-scaffold` walks `crates/scaffold/templates/` and emits one `include_bytes!` table. No new dependency. `include_dir` was checked and rejected (2.7). |
| SC-D4 | Rendering | Plain files are copied as they are. Files ending `.tmpl` get a closed set of `%%name%%` substitutions. `merezha.toml` and its siblings are concatenated from per-block fragments. There is no template engine. Template manifests are stored as `Cargo.toml.tmpl`, so Cargo never sees a template as a package; CI compiles the rendered output. |
| SC-D5 | Two config files | `merezha.toml` is the local development configuration. `merezha.production.toml` is the production starting point. Both share the manifests, the content model and the templates. Module settings in nested tables cannot be overridden from the environment (`nested_override_unsupported`), so one file cannot serve both. |
| SC-D6 | Secrets | Every secret is `{ env = "NAME" }` in both files. `merezha new` writes generated development secrets to `.env` (mode 0600, created after the `.gitignore` that lists it) and a committed `.env.example` without values. `--no-secrets` writes no `.env`. |
| SC-D7 | Guest build | `merezha build` runs `cargo build --target wasm32-unknown-unknown --release` on the project workspace, then componentizes every service of the runtime manifest into `components/`. The componentize code moves from `services/componentize` into the host library crate `merezha-guest-build`, which both use. |
| SC-D8 | SDK pin | The project depends on `merezha-domain` by git URL and full commit `rev`, the revision the running `merezha` binary was built from. `--merezha-path` writes a path dependency instead. Publishing to crates.io is SC-Q2. |
| SC-D9 | Local stack | `.contremaitre.yaml` declares managed PostgreSQL 17 (`db`) and, with telemetry, the LGTM container (`otel`) built from `ops/telemetry/lgtm.Dockerfile`. The host runs natively through `scripts/dev.sh`. No mail container: the capture driver is the local mail sink. |
| SC-D10 | Example | `teamboard`, a team task board: organizations, invitations, roles, API keys, tenant streams, a partitioned projection, a reactor with notify, webhook and command effects, help articles and organization briefs in the CMS, inbox with SSE, email, push and OTLP telemetry. |
| SC-D11 | Example location and sync | `examples/teamboard/` holds the committed output of `merezha new teamboard --template teamboard --merezha-path . --no-secrets`. CI regenerates it and fails on any difference, then builds it and runs the end-to-end test against it. |
| SC-D12 | End-to-end test | `crates/cli/tests/teamboard.rs` runs the real `merezha run` binary on the example's Wasm components and real PostgreSQL, with `FakeBrevo` for email, `FakePushService` for push and `WebhookReceiver` for webhooks, across two organizations. |
| SC-D13 | Telemetry ownership | TE-T7 keeps `deploy/telemetry/` and `merezha telemetry export`. The scaffold's copy into `ops/telemetry/`, the example's Contremaitre manifest and its verification profile move from TE-T7 to SC-S4. |
| SC-D14 | Front end origin | Development: the front end's dev server proxies `/v1` to the host, so the browser sees one origin and CORS stays off. Production: `[http.cors]` with the front end's exact origin and `allow_credentials = true`. |

## 1. Language

| Term | Meaning |
| --- | --- |
| Template | A named project layout embedded in the binary: `starter` or `teamboard`. |
| Block selection | The set of blocks a starter project enables: any of `identity`, `notifications`, `content`, `telemetry`. The kernel, jobs and the PostgreSQL application are always on. |
| Fragment | A file that becomes part of a generated file, kept or dropped by the block selection. |
| Source pin | The merezha revision (or local path) a project's guests and host are built from, recorded in `.merezha-version`. |
| Golden example | `examples/teamboard/`, the committed output of the `teamboard` template. |

## 2. `merezha new`

### 2.1 Synopsis

```sh
merezha new NAME [--dir PATH] [--template starter|teamboard] [--blocks LIST]
                 [--merezha-rev REV | --merezha-path PATH] [--no-secrets]
```

| Argument | Default | Meaning |
| --- | --- | --- |
| `NAME` | required | The application id: `[app] id`, the Contremaitre `project`, `[storage] application` and the README title. `^[a-z]([a-z0-9-]{0,38}[a-z0-9])?$`: a lowercase DNS label of at most 40 bytes, the stricter of `[app] id` (63 bytes) and a Contremaitre name (40). `merezha` is refused, since it would share the Contremaitre environments of the merezha repository. |
| `--dir PATH` | `./NAME` | Where the project goes. Created when missing; its parent must exist and is canonicalized first, so a parent reached through a symbolic link (`/tmp` on macOS) works. |
| `--template` | `starter` | `starter` or `teamboard` (section 5). |
| `--blocks LIST` | `identity,notifications,content,telemetry` | Comma-separated block selection for `starter`, or `none`. Refused with `teamboard`. |
| `--merezha-rev REV` | the binary's own revision | A 40-hex commit the guests' `merezha-domain` git dependency and `.merezha-version` name. |
| `--merezha-path PATH` | none | A merezha checkout, written as a path dependency relative to the project (the golden example and local merezha work use it). Exclusive with `--merezha-rev`. |
| `--no-secrets` | off | Write no `.env`; only `.env.example`. |

`merezha new --help` lists the templates and blocks. There is no `--force`.

### 2.2 Block selection

The rules follow what the blocks refuse at startup, so a generated project always starts:

| Rule | Why | Refusal |
| --- | --- | --- |
| `identity` needs `notifications` | identity stops at start with `notifications_required` unless `dev_print_links` runs on a private loopback listener, which production cannot use | `block_requires_notifications` |
| `content` needs `identity` | without identity there is no principal: on a private listener the kernel's loopback allow-all policy admits every management call, and on any other listener the deny-all policy answers every management route `401 unauthenticated`, so nobody could edit content in production; `scope = "organization"` types also fail `content_model_invalid` without identity | `block_requires_identity` |
| `telemetry` stands alone | the stderr log stream is always on; the block adds the OTLP settings, `ops/telemetry/` and the `otel` companion | none |
| `none` | runtime, jobs and the PostgreSQL application only: routes declare `access = "public"` and the project is loopback-only | none |

What changes with the selection is in 3.14.

### 2.3 Refusals

Every check runs before anything is written. Invalid input prints
`{"status":"invalid","error":{"code"}}` on stdout and exits 2:

| Code | When |
| --- | --- |
| `invalid_arguments` | an unknown flag, a repeated flag, a missing value |
| `invalid_name` | `NAME` outside the pattern above, or `merezha` |
| `unknown_template`, `unknown_block` | a template or block not in the lists |
| `block_requires_notifications`, `block_requires_identity` | 2.2 |
| `blocks_fixed_by_template` | `--blocks` with `teamboard` |
| `conflicting_source` | both `--merezha-rev` and `--merezha-path` |
| `invalid_rev` | not 40 lowercase hex characters |
| `invalid_merezha_path` | the path holds no `crates/domain/Cargo.toml` naming `merezha-domain` |
| `source_rev_unknown` | the binary does not know its revision (2.9) and neither flag was given |
| `target_not_empty`, `target_not_directory` | the target exists and holds anything, or is a file |
| `target_is_symlink` | the target itself is a symbolic link (the parent is canonicalized, so links above the target are fine) |
| `target_is_working_directory` | the canonical target is the current working directory: the rename would replace it and leave the shell in a deleted directory. Run `merezha new` from the parent instead |
| `parent_missing` | the target's parent directory does not exist |

### 2.4 Writing the project

1. Create a staging directory beside the target, `.NAME.merezha-new-<16 hex>`, mode 0700, with
   `create_dir` (fails if it exists).
2. Write every file with `create_new` (`O_EXCL`), so no existing file is ever replaced. Regular
   files get mode `0644 & !umask`, scripts under `scripts/` `0755 & !umask`. `.gitignore` is
   written before `.env`, and `.env` gets mode 0600 set at creation through
   `OpenOptionsExt::mode`, then checked with `metadata`.
3. Set the staging directory to `0755 & !umask` and `rename` it onto the target. POSIX `rename`
   replaces an empty directory and fails on a non-empty one, so a target someone filled after the
   checks makes the command fail instead of mixing files.
4. On any failure the staging directory is removed and the report is
   `{"status":"failed","error":{"code"}}` with exit 1 (`write_failed`, `rename_failed`).

The project appears whole or not at all. Windows is not supported in v1, like Contremaitre and
the CI matrix.

### 2.5 Output

One JSON object on stdout, the convention of `merezha content diff` and `merezha identity
platform-admin`:

```json
{"status":"created","dir":"/home/ada/acme","template":"starter",
 "blocks":["identity","notifications","content","telemetry"],
 "merezha":{"rev":"0123456789abcdef0123456789abcdef01234567"},
 "secrets":["MEREZHA_IDENTITY_SECRET_KEY","MEREZHA_NOTIFY_SECRET_KEY","MEREZHA_VAPID_PRIVATE_KEY"],
 "files":47,
 "next":["cd /home/ada/acme","scripts/dev.sh"]}
```

`secrets` lists the names written to `.env`, never a value. `merezha new` emits no telemetry
events: it reads no configuration and installs no subscriber, like `notifications vapid generate`.

### 2.6 Offline behaviour

`merezha new` needs no network, no Cargo, no Git and no Contremaitre: templates and the canonical
`wit/service.wit` are in the binary, and secrets come from the operating system's CSPRNG. It does
not run `git init`; the README's first step does.

The generated project's first `cargo build` needs crates.io and, with a git source pin, GitHub.
For an offline machine use `--merezha-path` to a local checkout and `cargo build --offline` with
a warm Cargo cache (or `cargo vendor`). The README says so.

### 2.7 Embedding the templates

`crates/scaffold/build.rs` walks `templates/`, refuses symbolic links, absolute or `..` paths,
files over 1 MiB and `.tmpl` files that are not UTF-8, and writes `$OUT_DIR/templates.rs`, a
sorted `&[(&str, &[u8])]` of `include_bytes!` entries. It prints
`cargo::rerun-if-changed=templates` (Cargo scans the directory recursively) and
`cargo::rerun-if-changed=../../wit/service.wit`, which it embeds too.

Checked with `cargo info` and the crates.io API on 2026-10-27:

| Option | Version | MSRV | Licence | Verdict |
| --- | --- | --- | --- | --- |
| build script with `include_bytes!` | none | n/a | n/a | chosen: no dependency, and edits to a template rebuild the binary |
| `include_dir` (+ `include_dir_macros`) | 0.7.4, released 2024-06-17 | 1.64 | MIT | rejected: without its `nightly` feature the macro does not track the directory, so an edited template leaves a stale binary unless a build script is added anyway |
| `rust-embed` | 8.13.0 | 1.80.0 | MIT | rejected: built for serving folders at run time, more than a fixed table needs |
| `tar` archive built at compile time | 0.4.46 | 1.63 | MIT OR Apache-2.0 | rejected: an archive step plus a reader, with nothing gained over the table |

The componentizer (SC-D7) is the one new direct dependency:

| Crate | Version | MSRV | Licence | Features | Used by |
| --- | --- | --- | --- | --- | --- |
| `wit-component` | 0.258.3 | 1.85.0 | Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT | default | `merezha-guest-build`: encoding and the import and export checks |
| `wit-parser` | 0.258.3 | 1.85.0 | Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT | default | `merezha-guest-build`: the canonical WIT |

Both are already in `Cargo.lock` at these versions, pulled by Wasmtime 49's bindgen, so the
lockfile gains no crate. `deny.toml` allows every licence. Wasmtime 49 decodes components with
`wasmparser` 0.258.3, the version these encode with. `services/componentize` moves from 0.262.0
to 0.258.3 with them (`services/Cargo.lock` keeps 0.262.0 for `wit-bindgen` 0.63 on the wasm32
target), which is an API port and not a version bump: `wit_component::targets`
takes three arguments in 0.258.3 and the current code passes four, so the call is rewritten
and its tests rerun.

### 2.8 Rendering

Each template directory holds a `template.toml` index:

```toml
name = "starter"
description = "A small notes application"
blocks = ["identity", "notifications", "content", "telemetry"]   # selectable; teamboard: fixed

[[file]]
path = "merezha.toml"
fragments = ["config/00-kernel.toml.tmpl", "config/10-identity.toml.tmpl",
             "config/20-notifications.toml.tmpl", "config/30-content.toml.tmpl",
             "config/40-telemetry.toml.tmpl"]

[[file]]
path = "content.toml"
source = "content.toml"
when = ["content"]

[[file]]
path = "application.toml"
source = "application.public.toml.tmpl"
unless = ["identity"]
```

- A `[[file]]` with `fragments` is the concatenation of the fragments whose block is selected
  (a fragment named `NN-<block>.*` belongs to that block; `00-kernel` always applies). Files not
  listed are copied as they are, under the same relative path, unless a `when` or `unless`
  drops them.
- Substitutions in `.tmpl` sources are a closed set: `%%app_id%%`, `%%app_name%%` (the id),
  `%%merezha_dependency%%` (the TOML inline table of 2.9), `%%merezha_rev%%` and
  `%%merezha_version%%`. A `%%` sequence that is not one of them fails the build script, so a
  typo never reaches a user. The delimiter avoids `{{ }}`, which minijinja templates and
  Contremaitre references use.
- Every `Cargo.toml` in a template is stored as `Cargo.toml.tmpl` and renamed when rendered.
  Cargo therefore never treats a template as a package: not in this repository's workspace, and
  not when a project's git dependency makes Cargo scan this repository for `merezha-domain`
  (which would otherwise find a second `domain` and `events` package and warn about duplicates).
- A unit test renders every template with every block selection allowed by 2.2 (eight starter
  selections and `teamboard`) and loads each result as 3.14 describes. The guest code is compiled
  from the rendered output (SC-S3), never in place.

### 2.9 Source pin

`crates/cli/build.rs` (SC-S1) records the revision the binary is built from, in this order (SC-S2
reads it through `merezha_cli::guest::source_rev`, or moves the script into `crates/scaffold`):

1. `MEREZHA_SOURCE_REV` from the build environment, which the release and CI builds set.
2. Else `git rev-parse HEAD` in the crate's directory, kept only when the repository is merezha's
   (`git rev-parse --show-prefix` is the crate's place, `crates/cli/`, and the build script is
   tracked, so a copy inside another repository records nothing) and `git --no-optional-locks
   status --porcelain --untracked-files=no` prints nothing. `--no-optional-locks` stops `git
   status` from refreshing `.git/index` as a side effect; a refreshed index would be newer than
   the script's last run, and Cargo would rerun the script, rebuild `merezha-scaffold` and relink
   `merezha` on every build. Untracked files are ignored on purpose: Cargo leaves an
   untracked `.cargo-ok` in every git checkout it builds, so counting untracked files would mark
   every `cargo install --git ... --rev` build dirty. A modified tracked file still counts.
3. Else none. A dirty or unknown tree gives none, so `merezha new` asks for `--merezha-rev` or
   `--merezha-path` (`source_rev_unknown`) rather than pinning a revision that is not the source.

The script prints `cargo::rerun-if-env-changed=MEREZHA_SOURCE_REV` and
`cargo::rerun-if-changed` for the git directory's `HEAD`, the loose ref `HEAD` points to,
`packed-refs` and `index`, each only when the file exists: Cargo treats a missing path as always
changed, and a branch whose ref `git gc` or `git pack-refs` moved into `packed-refs` has no loose
file (`packed-refs` then covers it). For such a branch it also watches the loose ref's nearest
existing parent directory under `refs/`, since the next commit writes the loose file there without
touching `packed-refs`. It finds them with `git rev-parse --git-path`, so linked worktrees work. A
commit, a branch switch or staging an edit reruns the script.

**Which revisions are authoritative.** Only two kinds of build record a revision that can be
trusted: one with `MEREZHA_SOURCE_REV` set (release and CI builds), and a fresh build of a clean
checkout, which is what `cargo install --git ... --rev` always is. In a development checkout the
recorded revision is advisory: an unstaged edit to a source file, say under `crates/runtime/src`,
touches neither `HEAD` nor the index, so the script does not rerun and the binary keeps
reporting the clean revision it saw last. Rerunning the script on every source change would
rebuild and relink on every edit, which is worse. Development builds pin with `--merezha-path`
anyway (SC-Q10). SC-S2's tests therefore use fresh builds: `cargo install --git
file://<checkout> --rev <sha> merezha-cli` records `<sha>` despite `.cargo-ok`; a fresh build
(empty target directory) of a checkout with a modified tracked file records none; and building
twice in a row without changes does not rerun the script.

`%%merezha_dependency%%` renders as one of:

```toml
merezha-domain = { git = "https://github.com/MartianGreed/merezha", rev = "0123456789abcdef0123456789abcdef01234567" }
merezha-domain = { path = "../crates/domain" }   # --merezha-path, relative to the project's Cargo.toml
```

`.merezha-version` records the same pin for scripts and CI:

```toml
# The merezha revision this project builds against. scripts/dev.sh and CI install and check it.
rev = "0123456789abcdef0123456789abcdef01234567"
# or: path = ".."
```

With a `path` pin there is no revision to compare: section 4.3 says what `scripts/dev.sh` and CI
check then, and SC-Q10 asks the owner to confirm it.

## 3. The generated project

### 3.1 Tree

The `starter` with every block. Bracketed names are the block a file needs.

```
acme/
  .contremaitre.yaml          PostgreSQL 17 (db); with telemetry the LGTM container (otel)
  .env                        generated development secrets, mode 0600, ignored (not with --no-secrets)
  .env.example                every variable both configs read, without values
  .gitignore
  .merezha-version            the source pin (2.9)
  .github/workflows/ci.yml    3.13
  README.md                   3.12
  rust-toolchain.toml         Rust 1.98.0 with the wasm32-unknown-unknown target
  Cargo.toml                  the Cargo workspace: guests/* and smoke
  merezha.toml                local development configuration (3.2)
  merezha.production.toml     production starting point (3.3)
  runtime.toml                runtime manifest (3.4)
  application.toml            routes, projections, reactors (3.5)
  content.toml                [content] content model (3.6)
  notifications/templates/    [notifications] project templates (3.7)
  wit/service.wit             the canonical WIT, byte for byte the repository's
  guests/
    events/                   shared event types (rlib)
    domain/                   the decider: validate and decide (cdylib + rlib)
    view/                     the projection and its queries (cdylib + rlib)
    notifier/                 the reactor (cdylib + rlib)
  smoke/                      native scenario driver over HTTP (std and serde_json only)
  scripts/dev.sh              start the stack and the host (3.9)
  scripts/smoke.sh            build, start on an ephemeral port, run smoke, stop
  scripts/ci.sh               every CI check; the generated workflow and this repository's both run it
  ops/telemetry/              [telemetry] lgtm.Dockerfile, dashboards.yaml, dashboards/, rules/ (3.10)
```

`components/` (written by `merezha build`) and `target/` are ignored, not generated.

### 3.2 `merezha.toml`

The whole development file of the starter with every block, as it is generated. Each comment
names the reason for a value that differs from the default.

```toml
# Local development. Production starts from merezha.production.toml.
# Secrets come from the environment: scripts/dev.sh loads .env.
[app]
id = "acme"
name = "acme"

[http]
listen = "127.0.0.1:8080"          # loopback: a private listener; the front end reaches it through its dev server proxy

[database]
url = { env = "DATABASE_URL" }
migrate = true
pool_size = 24                     # the recommended 23 (see below) plus one spare

[runtime]
manifest = "runtime.toml"

[application]
path = "application.toml"

[application.background]
max_concurrent = 2

# Queue settings live only here (R1). The notify.* lines are notifications §7.2's.
[jobs.queues]
"notify.expand" = { concurrency = 4 }
"notify.email" = { concurrency = 8, max_attempts = 10, backoff_base_ms = 5000, backoff_max_ms = 3600000 }
"notify.sms" = { concurrency = 4, max_attempts = 6, backoff_base_ms = 5000, backoff_max_ms = 3600000 }
"notify.push" = { concurrency = 16, max_attempts = 6, backoff_base_ms = 5000, backoff_max_ms = 3600000 }
"notify.webhook" = { concurrency = 16, max_attempts = 10, backoff_base_ms = 1000, backoff_max_ms = 86400000 }
"notify.maintenance" = { concurrency = 1 }
application = { concurrency = 2 }  # reactor command effects (application.command)
identity = { concurrency = 2 }
content = { concurrency = 2 }

[modules.identity]
app_origin = "http://localhost:5173"   # your front end's dev server; loopback is allowed here only
secret_key = { env = "MEREZHA_IDENTITY_SECRET_KEY" }
registration = "open"                  # owner answer ID8; "invite_only" or "closed"

[modules.identity.password]

[modules.identity.magic_links]

[modules.identity.organizations]
user_can_create = false                # the starter is an application without organizations

[modules.identity.rbac.builtin_grants]
user = ["notes.read", "notes.write"]   # every signed-in user may keep notes

[modules.identity.rbac.application_roles.operator]
name = "Operator"
description = "Runs the application: status, jobs, application and webhooks"
permissions = ["system.status.read", "jobs.admin.read", "application.admin.read",
               "notifications.webhooks.read", "notifications.webhooks.manage"]

[modules.notifications]
secret_key = { env = "MEREZHA_NOTIFY_SECRET_KEY" }
public_base_url = "http://127.0.0.1:8080"
egress = { allow_private_networks = true }   # local webhook receivers; refused with http.public

[modules.notifications.templates]
dir = "notifications/templates"
default_locale = "en"
locales = ["en"]
app_name = "acme"
app_base_url = "http://localhost:5173"
allow_http_links = true                # identity's http dev links; refused with http.public

[modules.notifications.email]
driver = "capture"                     # GET /v1/notifications/dev/captures; refused with http.public
from = "acme <no-reply@example.com>"

# To try real mail in development, switch the driver to "brevo" and uncomment.
# sandbox = true makes Brevo accept and drop every message (X-Sib-Sandbox: drop).
# [modules.notifications.brevo]
# api_key = { env = "BREVO_API_KEY" }
# webhook_token = { env = "BREVO_WEBHOOK_TOKEN" }
# sandbox = true

[modules.notifications.sms]
driver = "disabled"                    # no phone numbers on users in v1 (ID10)

[modules.notifications.push]
vapid_private_key = { env = "MEREZHA_VAPID_PRIVATE_KEY" }
vapid_subject = "mailto:ops@example.com"

[modules.notifications.webhooks]
allow_http = true                      # local receivers; refused with http.public
alert_roles = []                       # no organizations in the starter
application_alert_roles = ["operator"]
event_types = { "notes.note_shared" = { description = "A note was shared" } }

[modules.notifications.topics."notes.shared"]
category = "transactional"
channels = ["inbox", "email", "push"]
default_channels = ["inbox", "email"]
description = "Someone shared a note with you"

[modules.content]
model = "content.toml"

[telemetry]
log_level = "info"
# OTLP stays off unless an endpoint is set (R12). scripts/dev.sh sets
# MEREZHA_TELEMETRY__ENDPOINT when the otel companion runs.

[telemetry.resource]
environment = "local"
```

The pool size comes from platform.md's formula ("Application storage", Connections):
`limits.max_concurrent + background.max_concurrent + 2 + number of job queues + 2`, here
8 + 2 + 2 + 9 + 2 = 23, which is what the host recommends. 24 keeps one connection spare. Below
the recommendation the host logs the startup warning `db.pool_undersized`, which is emitted today
but missing from `names.rs`; TE-T7 adds it to the catalog. The queue concurrencies sum to 55 of
the 256 allowed. `notify.email` keeps `backoff_max_ms` at one hour, above the five minutes a Brevo
driver needs, and every provider queue keeps the default 30 s lease, three times the 10 s provider
timeouts, so switching to Brevo needs no queue change.

**The front end in development (SC-D14).** A browser front end on `http://localhost:5173` calling
`http://127.0.0.1:8080` is cross-origin, and CORS exists only through `[http.cors]`. Worse,
`localhost` and `127.0.0.1` are different sites, so a `SameSite=Lax` session cookie would not go
with a cross-site fetch even with CORS on. The development file therefore writes no `[http.cors]`,
and the README configures the front end's dev server to proxy `/v1` to `http://127.0.0.1:8080`
(for Vite: `server.proxy = { "/v1": "http://127.0.0.1:8080" }`). The browser then sees one origin,
`http://localhost:5173`, which is `app_origin`: the loopback development cookie, the `Origin`
check and the CSRF header work as they will in production, and nothing about CORS has to be
learned twice. Scripts and `curl` call `127.0.0.1:8080` directly with bearer sessions.

The README warns not to add the dev server to `http.trusted_proxies`, tempting as it looks for a
proxy. With any trusted proxy the listener is no longer private (`!behind_proxy` is part of the
private loopback test in `crates/identity/src/config.rs`), so the loopback development cookie
and `dev_print_links` are no longer allowed and identity's `http://localhost:5173` origin fails
`insecure_origin` at startup. Nothing is gained either: the client
address only feeds rate limits, which in development can share one address.

### 3.3 `merezha.production.toml`

The same file with these differences, and nothing a public listener refuses:

| Key | Development | Production |
| --- | --- | --- |
| `http.listen`, `http.public`, `http.trusted_proxies` | `127.0.0.1:8080`, absent, absent | `0.0.0.0:8080`, `true`, `["10.0.0.0/8"]` (replace with the proxy's addresses) |
| `database.tls` | absent (`disable`) | `"require"` |
| `[http.cors]` | absent (off) | `allowed_origins = ["https://app.example.invalid"]`, `allow_credentials = true`: the front end at `app.` calls the API at `api.`, a different origin of the same site, so the `SameSite=Lax` cookie goes along and only CORS is needed |
| `modules.identity.app_origin` | `http://localhost:5173` | `https://app.example.invalid` |
| `modules.identity.api_base_url` | absent (`app_origin`, through the dev proxy) | `https://api.example.invalid`, the host's own origin, from which OAuth callbacks are derived |
| `modules.notifications.public_base_url` | `http://127.0.0.1:8080` | `https://api.example.invalid` |
| `modules.notifications.egress` | `allow_private_networks = true` | absent |
| `templates.app_base_url`, `templates.allow_http_links` | `http://localhost:5173`, `true` | `https://app.example.invalid`, absent |
| `templates.preview` | absent (`true`) | `false` (a public listener refuses it without `preview_in_public`) |
| `email.driver` | `capture` | `brevo`, with `[modules.notifications.brevo]` `api_key = { env = "BREVO_API_KEY" }`, `webhook_token = { env = "BREVO_WEBHOOK_TOKEN" }`, `webhook_allowed_cidrs = ["1.179.112.0/20", "172.246.240.0/20"]` |
| `webhooks.allow_http` | `true` | absent |
| `[telemetry]` | no endpoint | `endpoint = "http://127.0.0.1:4318"`: a collector agent beside the host, TE §6's recommended topology |
| `telemetry.resource.environment` | `local` | `production` |

The `.invalid` names cannot resolve, so a deployment that forgot to set them fails visibly
instead of mailing links to someone's domain. The generated CI prints a warning annotation for
each `.invalid` host left in the production file. Top-level module keys such as `app_origin`
can also be set at deploy time with `MEREZHA_MODULES__IDENTITY__APP_ORIGIN`.

### 3.4 `runtime.toml`

```toml
version = 1

[limits]
memory_bytes = 16777216
fuel = 200000000
max_input_bytes = 262144
max_output_bytes = 262144
timeout_ms = 2000
max_concurrent_invocations = 16   # at least limits.max_concurrent 8 + background 2

[[services]]
id = "domain"
path = "components/domain.wasm"
application_protocol = 2

[[services]]
id = "view"
path = "components/view.wasm"
application_protocol = 2

[[services]]
id = "notifier"
path = "components/notifier.wasm"
application_protocol = 2
```

A service `id` is also its package name and its component's file stem: `merezha build` relies on
that (4.1).

### 3.5 `application.toml`

With identity (`application.toml.tmpl`):

```toml
version = 2

[storage]
backend = "postgres"
application = "acme"

[limits]
max_stream_events = 4096
max_state_bytes = 262144          # the notes projection is one state document
max_concurrent = 8

[[permissions]]
name = "notes.read"
description = "Read your notes and the notes shared with you"

[[permissions]]
name = "notes.write"
description = "Create, edit, share and archive your notes"

[[commands]]
tag = "CreateNote"
service = "domain"
aggregate = "Note"
permission = "notes.write"

[[commands]]
tag = "EditNote"
service = "domain"
aggregate = "Note"
permission = "notes.write"

[[commands]]
tag = "ShareNote"
service = "domain"
aggregate = "Note"
permission = "notes.write"

[[commands]]
tag = "ArchiveNote"
service = "domain"
aggregate = "Note"
permission = "notes.write"

[[queries]]
tag = "ListNotes"
service = "view"
projection = "notes"
permission = "notes.read"

[[queries]]
tag = "GetNote"
service = "view"
projection = "notes"
permission = "notes.read"

[[projections]]
name = "notes"
service = "view"
initial = { notes = {} }

[[reactors]]
name = "notifier"
service = "notifier"
events = ["NoteShared"]
effects = ["notify", "webhook"]
```

Every route is application-scoped, so `cross_tenant_query` does not apply. The routes check
permissions and the guests check ownership. Without identity, `application.public.toml.tmpl`
has no `[[permissions]]` and every route says `access = "public"`.

### 3.6 `content.toml`

```toml
version = 1
locales = [{ code = "en", default = true }]

[types.page]
kind = "collection"
access = "public"                  # CT1: merezha new writes access = "public" on site types
title_field = "title"
fields = [
  { name = "title", type = "string", required = true, max_length = 200, sortable = true },
  { name = "slug", type = "slug", from = "title", required = true },
  { name = "body", type = "rich_text", styles = ["normal", "h2", "h3"], marks = ["strong", "em", "code"], annotations = ["link"], lists = ["bullet", "number"] },
]
```

No `asset` field: a local media root must be absolute today (`invalid_path`), and an absolute path
in a committed file would name one developer's disk. SC-Q4 proposes the fix.

### 3.7 Notification templates

```
notifications/templates/notes.shared/
  template.toml        description, required = ["note_id", "title"], [preview] note_id and title
  en/subject.txt       {{ title }} was shared with you
  en/body.txt          plain text with links.app + "/notes/" + note_id
  en/body.html         the same, escaped by minijinja's HTML auto-escape
  en/inbox.toml        title, body, link
  en/push.toml         title, body, url
```

Identity's topics keep the block's embedded `en` and `fr` defaults.

### 3.8 Guest workspace

`rust-toolchain.toml`:

```toml
[toolchain]
channel = "1.98.0"
targets = ["wasm32-unknown-unknown"]
components = ["clippy", "rustfmt"]
profile = "minimal"
```

`Cargo.toml` (from `Cargo.toml.tmpl`):

```toml
[workspace]
resolver = "3"
members = ["guests/events", "guests/domain", "guests/view", "guests/notifier", "smoke"]

[workspace.package]
version = "0.1.0"
edition = "2024"
rust-version = "1.98.0"
publish = false

[workspace.dependencies]
merezha-domain = { git = "https://github.com/MartianGreed/merezha", rev = "0123456789abcdef0123456789abcdef01234567" }
events = { path = "guests/events" }
serde = { version = "1.0.228", features = ["derive"] }
serde_json = "1.0.154"
wit-bindgen = { version = "=0.63.0", default-features = false, features = ["macros", "realloc"] }

[profile.release]
opt-level = "s"
lto = true
codegen-units = 1
panic = "abort"
strip = true
```

The versions match the services workspace's (`serde_json` there also enables `raw_value`, which
the generated guests do not need); the first build's `Cargo.lock` fixes the exact ones.
`wit-bindgen` is pinned exactly (`=0.63.0`, as SC-S1's `merezha build` fixture does) because it
decides the component type embedded in every guest, which `merezha build` checks. Each
component crate follows
`services/task-notifier/Cargo.toml`: `crate-type = ["cdylib", "rlib"]`, `wit-bindgen` only
under `[target.'cfg(target_arch = "wasm32")'.dependencies]`, and a `#[cfg(target_arch =
"wasm32")]` module calling `wit_bindgen::generate!({ path: "../../wit", world: "service" })`.
The manifests are stored as `Cargo.toml.tmpl` (2.8) and the `wit/` directory is added at render
time, so the guests are compiled and tested from the rendered project: `scripts/check-scaffold.sh`
renders each case and runs the project's own `scripts/ci.sh` in it (SC-S3).

| Crate | Answers | Behaviour |
| --- | --- | --- |
| `events` | library | `NoteCreated {note_id, title, body, owner?}`, `NoteEdited {title?, body?}`, `NoteShared {note_id, title, with, by}`, `NoteArchived {}`, schema version 1, typed decode with `deny_unknown_fields` |
| `domain` | `validate`, `decide` | `CreateNote {title, body}` (title 1 to 120 characters without control characters, body at most 10,000) records the principal's subject as owner; `EditNote`, `ShareNote {with}` (a `usr_` id, not the owner, not already shared, at most 20 shares) and `ArchiveNote {}` need the owner (`not_owner`) and an open note (`note_archived`). Without a principal (no identity) notes have no owner and are open to every caller. |
| `view` | `project`, `query` | `notes` state `{notes: {id: {title, owner, shared_with, archived}}}`; `ListNotes {}` returns the notes the principal owns or that are shared with it, `GetNote {id}` one of them or `note_not_found` |
| `notifier` | `react` | for each `NoteShared`: `notify` topic `notes.shared` to `User { id: with }` with `{note_id, title}` and the topic's default channels, then `webhook` `notes.note_shared` with `{note_id}` |
| `smoke` | native binary | the scenario of 3.12 over a hand-written HTTP/1.1 client on `std::net` (no extra dependency); it never builds for wasm, since `merezha build` builds only the runtime manifest's packages |

Each guest crate has native tests in `tests/`: accept and reject cases for every command, a
projection walk, and the notifier's effects for a `NoteShared` and nothing for other events.

### 3.9 Local stack

`.contremaitre.yaml`:

```yaml
# Local stack for acme: managed PostgreSQL 17 and, with telemetry, Grafana LGTM.
# The merezha host runs on this machine: scripts/dev.sh.
version: 1
project: acme
services:
  db:
    kind: postgres
  otel:                                # [telemetry]
    build: ops/telemetry
    dockerfile: ops/telemetry/lgtm.Dockerfile
    port: 3000                         # Grafana, local HTTPS route
    http: true
```

The `otel` service declares no `ready` command, as TE §6 explains. There is no mail container:
Merezha speaks Brevo's HTTP API, not SMTP, so an SMTP sink such as Mailpit would receive nothing.
The capture driver is the local sink, read through `GET /v1/notifications/dev/captures` (loopback
peer and loopback `Host` only). The host runs natively rather than as a Contremaitre `app`
service, because there is no published `merezha` image yet (SC-Q1).

`scripts/dev.sh` does, in order:

1. `set -a; . ./.env; set +a` when `.env` exists.
2. Check `merezha version` against `.merezha-version` as 4.3's table says: a different pinned
   revision stops with the install command, an unknown one or a path pin only warns.
3. Unless `DATABASE_URL` is already set (a throwaway server, platform.md "Local PostgreSQL"):
   `contremaitre ensure --json`, `contremaitre proxy db ${DB_PORT:-15432}:5432 &`, and
   `DATABASE_URL=postgres://app:$(contremaitre exec db -- printenv POSTGRES_PASSWORD)@127.0.0.1:${DB_PORT:-15432}/app`.
4. With `ops/telemetry/` present: `contremaitre proxy otel ${OTLP_PORT:-14318}:4318 &` and
   `MEREZHA_TELEMETRY__ENDPOINT=http://127.0.0.1:${OTLP_PORT:-14318}`.
5. `merezha build`, then `merezha run --config merezha.toml` in the foreground; a trap stops the
   proxies when it exits.

### 3.10 Telemetry companion

With the telemetry block, `ops/telemetry/` holds what `merezha telemetry export --out
ops/telemetry` writes (TE-T7: `dashboards.yaml`, `dashboards/*.json`, `rules/*.yaml`) and
`lgtm.Dockerfile`, verbatim from TE §6:

```dockerfile
FROM grafana/otel-lgtm:0.38.0
# Dashboard provisioning path documented by the image README ("Add custom dashboards").
COPY dashboards/ /otel-lgtm/grafana/conf/provisioning/dashboards/merezha/
COPY dashboards.yaml /otel-lgtm/grafana/conf/provisioning/dashboards/merezha.yaml
```

`merezha new` calls the same embedded export TE-T7 adds, so the dashboards always match the host
version. The generated README shows how to refresh them after upgrading merezha (`merezha
telemetry export --out ops/telemetry`). The project's Contremaitre verification profile
`telemetry` runs inside `otel` and queries Tempo and Prometheus for a trace and the request
metric after one request, which is TE-T7's acceptance moved here (SC-D13).

### 3.11 Secrets and ignore files

`.env.example` (committed):

```sh
# Copy to .env (chmod 600) or let `merezha new` write it. Never commit .env.
# DATABASE_URL is filled by scripts/dev.sh from the Contremaitre environment when unset.
DATABASE_URL=
# At least 32 bytes: openssl rand -hex 32
MEREZHA_IDENTITY_SECRET_KEY=
# Base64 of 32 to 1024 bytes: openssl rand -base64 32
MEREZHA_NOTIFY_SECRET_KEY=
# merezha notifications vapid generate, the "private_key" field
MEREZHA_VAPID_PRIVATE_KEY=
# merezha.production.toml only
BREVO_API_KEY=
# At least 32 characters: openssl rand -base64 48
BREVO_WEBHOOK_TOKEN=
```

`.env` holds the generated values: `MEREZHA_IDENTITY_SECRET_KEY` from
`merezha_platform::context::random_token` (256 bits, hex), `MEREZHA_NOTIFY_SECRET_KEY` as base64
of 32 bytes from the same CSPRNG, `MEREZHA_VAPID_PRIVATE_KEY` from the generator behind
`merezha notifications vapid generate` (`jwt-simple`'s `ES256KeyPair::generate`, through
`web-push-native`, written as the base64url scalar). Every value is hex or base64, so
sourcing the file in a shell is safe. Brevo values are never generated: they come from the
provider. Variables for absent blocks are left out of both files.

`.gitignore`:

```
/target/
/components/
/.env
/.env.*
!/.env.example
/.merezha/
```

### 3.12 README

Sections, in order: what the project is and which blocks it enables; prerequisites (rustup, which
reads `rust-toolchain.toml`; the `merezha` host at the pinned revision, `cargo install --locked
--git https://github.com/MartianGreed/merezha --rev <rev> merezha-cli`, which needs a C compiler
and Perl for the vendored OpenSSL of passkeys; Contremaitre, or any PostgreSQL 14 or later);
`git init` and the first commit, which includes the generated `Cargo.lock` after the first build;
`scripts/dev.sh`; a walk through the API with `curl` (register, read the verification mail from
the captures, confirm, sign in with `"transport": "bearer"`, create, share and list notes, read
the inbox and open `inbox/stream`); the front end's dev server proxy for `/v1` (3.2) and the
production `[http.cors]` origin (3.3); where each thing lives (3.1); tests (`cargo test`,
`scripts/smoke.sh`); Brevo sandbox in development (3.2's commented block); the production
checklist (3.3's table, `merezha identity password-benchmark`, `merezha notifications brevo
register-webhook`, `merezha identity platform-admin grant`); and links to platform.md and the
operations guide at the pinned revision.

### 3.13 CI workflow

`.github/workflows/ci.yml`, one Linux job with a `postgres:17` service container, `permissions:
contents: read` and actions pinned by commit like this repository's. PostgreSQL 17 matches the
Contremaitre environment and this repository's own `host.yml`; R14 names 16 for CI, the floor
stays 14, and the generated SQL is all the blocks' own, which they keep within 14. The workflow
does two things:

1. Install the toolchain from `rust-toolchain.toml`; restore `~/.cargo/bin/merezha` from a cache
   keyed by `.merezha-version`, else `cargo install --locked --git ... --rev <rev> merezha-cli`.
2. Run `scripts/ci.sh`, which holds every check so the same script runs here and in this
   repository's `scaffold` workflow (SC-S3):
   1. `cargo fmt --all --check`; `cargo clippy --locked --workspace --all-targets -- -D
      warnings`; `cargo clippy --locked --target wasm32-unknown-unknown -p domain -p view -p
      notifier -- -D warnings`; `cargo test --locked --workspace`.
   2. `merezha build`.
   3. Throwaway secrets for this run (`openssl rand`), each printed as `::add-mask::<value>`
      before use so GitHub masks it in the log (outside Actions the line is inert), then `merezha
      notifications check --config merezha.toml` and `--config merezha.production.toml` with
      throwaway Brevo values. These checks run with `MEREZHA_NOTIFY_NO_EGRESS` unset: the
      production file's Brevo `base_url` is the default `https://api.brevo.com`, which that
      variable refuses (`egress_not_loopback`), and as a nested key it cannot be overridden.
      A warning annotation follows for each `.invalid` host left in the production file.
   4. `scripts/smoke.sh` against the service container's database, with
      `MEREZHA_NOTIFY_NO_EGRESS=1` set for the host it starts.

The workflow needs no repository secret.

### 3.14 What the block selection changes

| Block off | Effect |
| --- | --- |
| `identity` (so also `content`) | no `[modules.identity]`; `application.public.toml`; no `MEREZHA_IDENTITY_SECRET_KEY`; `application_alert_roles = []`, since no `operator` role exists; the README warns that the project serves only loopback; the smoke scenario calls the routes anonymously |
| `notifications` (so also `identity`, `content`) | no `[modules.notifications]`, no `notify.*` queues, no `notifications/`; the reactor keeps its `notify` effect, which the kernel's capture fallback logs as `notification.captured`; the `webhook` effect stays too, since the fallback records webhook events the same way |
| `content` | no `[modules.content]`, no `content.toml`, no `content` queue |
| `telemetry` | no `[telemetry.resource]` and no OTLP comment, no `ops/telemetry/`, no `otel` service; logs still go to stderr |

SC-S2's unit test loads every generated configuration through `host::configure` with every block
module registered, as `merezha run` does, with throwaway secrets and without
`MEREZHA_NOTIFY_NO_EGRESS` (which the production file's Brevo `base_url` would fail, as 3.13
explains): every key exists and every cross-check passes. It also parses the runtime manifest and `application.toml`,
validates `content.toml` through the content model loader and renders every template's preview
as `merezha notifications check` does. A key this design invents would fail that test.

## 4. Build and run

### 4.1 `merezha build` and `merezha componentize`

```sh
merezha build [--manifest runtime.toml] [--workspace Cargo.toml]
merezha componentize CORE.wasm COMPONENT.wasm
```

`merezha build`:

1. Parses the runtime manifest (the kernel's parser, without loading components) and takes each
   service's `path`. A path must be `components/<id>.wasm`, else `unsupported_component_path`.
   The service `id` names the Cargo package that builds it.
2. Runs `cargo metadata --format-version 1 --no-deps` on the workspace and finds, for each id,
   the package of that name and its `cdylib` target (`unknown_package`, `no_cdylib` otherwise).
   The target's name gives the artifact, `<target name with - as _>.wasm`, so a package that
   renames its library (`[lib] name = "task_domain"`) works.
3. Runs `cargo build --manifest-path <workspace> --release --target wasm32-unknown-unknown
   -p <id>...`, with `--locked` when `Cargo.lock` exists. Without one, Cargo writes it and the
   report says `"lockfile": "created"`. Cargo's output goes to stderr. `CARGO_TARGET_DIR` and
   the metadata's `target_directory` are honoured. Feature builds, such as the services
   workspace's `task-view-faulty`, are out of scope: build them with Cargo and `merezha
   componentize`.
4. Componentizes each artifact into `components/<id>.wasm` through `merezha-guest-build`,
   written to `<id>.wasm.partial` and renamed, as `services/componentize` does now. A component must import nothing and export
   exactly `merezha:service/handler@0.1.0` with the canonical types, and fit 16 MiB.
5. Prints `{"status":"built","components":[{"id","bytes"}],"lockfile"}` and exits 0; a Cargo
   failure prints `{"status":"failed","error":{"code":"cargo_failed"}}` (exit 1); a missing core
   module `component_source_missing`, a contract breach `component_rejected` with the service id
   (exit 1); an unreadable manifest or a missing wasm target `invalid_manifest` or
   `missing_wasm_target` (exit 2).

`merezha componentize` is step 4 alone. `services/componentize` keeps its package name
(`merezha-componentize`), binary and command line, and becomes a thin `main` over
`merezha-guest-build`, a path dependency on `../../crates/guest-build`. The host library takes a
different name precisely so that the two never collide. `scripts/build-services.sh` keeps its
commands (`cargo build -p merezha-componentize`, then `release/merezha-componentize`), but what
they compile changes: the services workspace now builds the host's `crates/guest-build` and its
`wit-component` 0.258.3, and `services/Cargo.lock` changes with it.

The two underlying steps stay usable without the wrapper: `cargo build --release --target
wasm32-unknown-unknown -p domain` and `merezha componentize
target/wasm32-unknown-unknown/release/domain.wasm components/domain.wasm`.

### 4.2 Run

```sh
merezha build
merezha run --config merezha.toml
```

Startup follows platform.md "Running a project". On the first start the migrations run
(`database.migrate = true`), content activates its model, identity syncs the `operator` role and
the reactor's row starts at the head. `scripts/smoke.sh` starts the host with `--listen
127.0.0.1:0`, reads the address from `listener.ready` on stderr, waits for `/health/ready`, runs
`cargo run --release -p smoke -- <address>` and stops the host with SIGTERM.

### 4.3 `merezha version`

`merezha version` prints `{"version":"0.1.0","rev":"<sha>"}` (`rev` is `null` when unknown) and
exits 0. It reads no configuration. `scripts/dev.sh` and the generated CI compare it with
`.merezha-version`:

| Pin | `merezha version` `rev` | Check |
| --- | --- | --- |
| `rev` | the same | passes |
| `rev` | another revision | fails with the `cargo install` command for the pinned one |
| `rev` | `null` (a host built from a dirty tree) | warns and continues: nothing to compare |
| `path` | any | compares with `git -C <path> rev-parse HEAD` when the path is a git checkout with no modified tracked file, and warns on a difference; otherwise warns that the host cannot be checked. It never fails, since a path pin is for working on merezha itself (SC-Q10) |

### 4.4 SDK version

The guest SDK is `merezha-domain`, which today lives only in this repository with `serde` and
`serde_json` as its only dependencies. The options:

| Option | For | Against |
| --- | --- | --- |
| Path | no network | needs a merezha checkout next to every project |
| Git `rev` (chosen) | exact, immutable, matches the host binary's revision; works with Cargo's workspace inheritance | first build fetches the repository from GitHub; Cargo cannot check semver compatibility |
| crates.io release | normal Cargo workflow, offline cache, semver | needs a publishing decision, a release process and the WIT shipped inside the crate; the names `merezha`, `merezha-domain`, `merezha-cli` and `merezha-sdk` were free on 2026-10-27 |

The scaffold writes the git `rev` of the running binary (2.9). The guest protocol version 2
handshake (`describe`) already refuses a guest from an older SDK, so a mismatch fails at startup
rather than at a command. Whether to publish is SC-Q2.

## 5. Example application: `teamboard`

### 5.1 Product

A team task board. People register, create an organization, invite teammates, create tasks,
assign them and complete them. The assignee gets an inbox item over SSE, an email and a push; the
creator hears when a task is done; the team's integrations get a signed webhook; done tasks are
archived seven days later by a command the reactor schedules. Help articles are public CMS
content, and each organization keeps project briefs that only its members read. It reuses the
task board vocabulary of `services/task-*`, but its guests are its own crates, so the example is
a project like any generated one.

### 5.2 Domain

Aggregate `Task`, one stream per task, tenant streams `{org}:Task/{id}` (AP §C.4).

| Command | Payload | Accepted when | Emits |
| --- | --- | --- | --- |
| `CreateTask` | `{title}` | the stream is empty; title as the starter's | `TaskCreated {task_id, title, creator}` |
| `AssignTask` | `{assignee}` (`usr_` id) | the task is open and the assignee changes; the creator, the current assignee or a principal with role `admin` or `owner` | `TaskAssigned {task_id, title, assignee, assigned_by}` |
| `CompleteTask` | `{}` | open; the creator, the assignee, `admin` or `owner` | `TaskCompleted {task_id, title, creator, assignee?, completed_by}` |
| `ArchiveTask` | `{}` | completed and not archived | `TaskArchived {task_id}` |

Rejections are `not_allowed`, `task_closed`, `already_assigned`, `task_not_found`,
`invalid_title` and `validation_failed`. Roles come from `Principal::has_role` at the route's
scope. The guest cannot tell whether the assignee is a member; the notifications block can, and
the reactor's effect is refused then (5.7).

Projection `board`, `partition = "tenant"`: per organization, `{tasks: {id: {title, creator,
assignee, status, completed_at}}}`. Queries `ListTasks {status?}`, `GetTask {id}` and `MyTasks
{}` (tasks assigned to the principal).

### 5.3 Routes and permissions

| Route | Declaration | Who holds it |
| --- | --- | --- |
| `CreateTask` | `permission = "tasks.create"`, `scope = "tenant"` | `member` (through `builtin_grants.member`), `admin`, `owner` |
| `AssignTask` | `permission = "tasks.assign"`, `scope = "tenant"`, `resource = "task"` | `admin`, `owner`, or a resource grant on one task |
| `CompleteTask` | `permission = "tasks.complete"`, `scope = "tenant"`, `resource = "task"` | `member`, `admin`, `owner` |
| `ArchiveTask` | `permission = "tasks.archive"`, `scope = "tenant"` | the reactor's `sys_reactor_notifier` (its `permissions`), `admin`, `owner` |
| `ListTasks`, `GetTask` | `permission = "tasks.read"`, `scope = "tenant"` | `member`, `admin`, `owner`, the `support` application role, tenant API keys listing it |
| `MyTasks` | `access = "authenticated"`, `scope = "tenant"` | any principal in the organization |

`[modules.identity.rbac.builtin_grants] member = ["tasks.read", "tasks.create",
"tasks.complete"]`. The declared application roles (R5):

| Role | Permissions |
| --- | --- |
| `support` | `identity.users.read`, `identity.organization.enter`, `tasks.read` |
| `editor` | `content.entries.read`, `content.entries.write`, `content.entries.publish`, `content.assets.read` |
| `integrations` | `notifications.webhooks.read`, `notifications.webhooks.manage` |
| `operator` | `system.status.read`, `jobs.admin.read`, `application.admin.read` |

### 5.4 Identity features

| Feature | How the example uses it |
| --- | --- |
| Registration and verification | open (ID8); `require_verified_email` stays on; the verification link arrives by email |
| Password sign-in, sessions | `[modules.identity.password]`; browsers use the cookie, scripts `"transport": "bearer"` |
| Magic links | `[modules.identity.magic_links]` |
| Organizations, invitations | `organizations.user_can_create = true` (default); owners invite with roles; `identity.invitation` mail |
| Roles | built-in `owner`, `admin`, `member`, the four declared application roles, a resource grant on one task |
| API keys, service accounts | an organization service account `ci-bot` holding `member`, with one key listing only `tasks.read` and one listing `tasks.read` and `tasks.create`: a key's power is its list intersected with its owner's grants |
| Passkeys | `[modules.identity.passkeys]` on (`rp_id` defaults to `localhost` in development); exercised by identity's own suite, not by 5.10 |
| OAuth, OIDC | a commented `[modules.identity.oauth.providers.github]` block with `client_id = "Iv1.replace-me"` and `client_secret = { env = "GITHUB_CLIENT_SECRET" }`. Its redirect URI is `{api_base_url}/v1/identity/oauth/github/callback`: in development `api_base_url` defaults to `app_origin`, which reaches the host through the dev server proxy (SC-D14); production sets `api_base_url` to the API origin (3.3) |
| Break-glass | `merezha identity platform-admin grant` for the operator who reads `/v1/admin/status` |
| `new_sign_in` | `notify_new_sign_in = true`, so security notices reach the inbox (NT-N8) |

### 5.5 Content

```toml
version = 1
locales = [{ code = "en", default = true }]

[types.help_article]
kind = "collection"
access = "public"                  # CT1: scaffolded site types are public
title_field = "title"
fields = [
  { name = "title", type = "string", required = true, max_length = 200, sortable = true },
  { name = "slug", type = "slug", from = "title", required = true },
  { name = "topic", type = "enum", values = ["getting-started", "tasks", "teams"], required = true, filterable = true },
  { name = "body", type = "rich_text", styles = ["normal", "h2", "h3"], marks = ["strong", "em", "code"], annotations = ["link", "entry_link"], lists = ["bullet", "number"] },
]

[types.site_settings]
kind = "singleton"
access = "public"
fields = [{ name = "site_name", type = "string", required = true }]

[types.project_brief]
kind = "collection"
scope = "organization"
access = "authenticated"           # members of that organization only (C9)
title_field = "title"
fields = [
  { name = "title", type = "string", required = true, max_length = 200 },
  { name = "body", type = "rich_text", styles = ["normal", "h2"], marks = ["strong", "em"], lists = ["bullet"] },
]
```

Editors with the `editor` role write help articles at application scope; organization owners and
admins write briefs through `/v1/content/manage` with their active organization. Publishing
either emits `content.entry_published`, which NT-N8 (PR #77) delivers to webhooks through AP
subscriptions: help articles to the application-level registry, briefs to the organization's.
If SC-Q4 is accepted, `help_article` gains `{ name = "cover", type = "asset", accept =
["image/*"] }` and the configuration a relative `[modules.content.media] local = { root =
".merezha/media" }`.

### 5.6 Notifications

| Topic or event | Category, channels (defaults) | Sent by | Data |
| --- | --- | --- | --- |
| `tasks.assigned` | transactional; `inbox`, `push`, `email` (`inbox`, `push`, `email`) | the reactor, to the assignee | `task_id`, `title` |
| `tasks.completed` | transactional; `inbox`, `email` (`inbox`) | the reactor, to the creator when someone else completed it | `task_id`, `title` |
| identity topics | security and transactional, the block's embedded templates | identity | identity's |
| `notifications.webhook_disabled` | the block's | NT-N7's auto-disable, to `owner` and `admin` (`alert_roles`) or `integrations` (`application_alert_roles`) | `endpoint_id` |
| webhook `tasks.task_completed` | `event_types` | the reactor | `task_id` |
| webhook `content.entry_published` and siblings | content's | content, through NT-N8's subscription | content's |

The inbox and its SSE stream need no configuration. Push is on with the generated VAPID key.
Email uses the capture driver in development, `FakeBrevo` in the end-to-end test, the Brevo
sandbox when a developer uncomments 3.2's block, and Brevo in production. SMS stays
`disabled`: users carry no phone numbers in v1 (ID10), and sending to explicit numbers would need
`allow_addresses = true` on the reactor, which the example keeps off.

### 5.7 Reactor

```toml
[[reactors]]
name = "notifier"
service = "notifier"
events = ["TaskAssigned", "TaskCompleted"]
effects = ["notify", "webhook", "command"]
commands = ["ArchiveTask"]
permissions = ["tasks.archive"]
```

| Event | Effects, in order |
| --- | --- |
| `TaskAssigned` | `notify` `tasks.assigned` to `User { id: assignee }` |
| `TaskCompleted` | `notify` `tasks.completed` to `User { id: creator }` unless the creator completed it; `webhook` `tasks.task_completed`; `command` `ArchiveTask` on the task's stream with `delay_ms = 604800000` |

An assignee who is not a member of the organization makes the notify effect a rejection
(`recipient_not_in_tenant`, R4) recorded in `merezha_app.reactor_rejections`; the checkpoint
advances and later events still notify.

### 5.8 Telemetry

Development exports OTLP to the `otel` companion through `scripts/dev.sh`. The overview dashboard
TE-T7 ships shows `http.server.request.duration`, `merezha.command.duration`,
`merezha.reactor.effects`, `merezha.reactor.lag.events`, `merezha.notification.deliveries`,
`merezha.inbox.sse.connections` and `merezha.webhook.deliveries`. A command's trace runs from the
`POST /v1/application/commands` server span through `command {tag}` and `guest {service}
{operation}`; the reaction is a `react {reactor}` root linked to it, and the delivery jobs
continue that trace through `notify {step}`. The README tells the operator to look for
`reactor.effect_rejected` and `reactor.halted` in the logs, and `merezha.job.dead_lettered` on
the dashboard.

### 5.9 Where it lives and how it stays in sync

The template `crates/scaffold/templates/teamboard/` is the source. `examples/teamboard/` holds
its committed output, generated with `--merezha-path .` from the repository root and
`--no-secrets`, plus the `Cargo.lock` of its first build. Both exist for different readers: the
template is what `merezha new` ships; the example is what people browse and what the end-to-end
test runs.

`scripts/check-scaffold.sh` regenerates the example into `.scaffold-check/teamboard` (ignored,
at the same depth below the root as `examples/teamboard`, so the relative path dependency is the
same) and runs `diff -r --exclude Cargo.lock --exclude target --exclude components` against
`examples/teamboard`. Any difference fails with the command that regenerates it,
`scripts/regenerate-examples.sh`.

### 5.10 End-to-end test

`crates/cli/tests/teamboard.rs` runs the real `merezha` binary on a scratch copy of
`examples/teamboard` with components from `MEREZHA_TEST_EXAMPLE_COMPONENTS` (the directory
`merezha build` wrote), a `TestDatabase`, an ephemeral loopback port and
`MEREZHA_NOTIFY_NO_EGRESS=1`. Without the variable it prints an explicit skip, like
`TestDatabase`; `MEREZHA_TEST_REQUIRE_EXAMPLE=1` turns the skip into a failure, and CI sets it.
The scratch copy edits three things with the `toml` crate:

- the email driver becomes `brevo`, with `base_url` pointing at a `FakeBrevo` and throwaway
  `api_key` and `webhook_token`;
- `[modules.notifications.push] allowed_hosts = ["127.0.0.1"]`, so a `FakePushService` on
  loopback is a valid endpoint (the development file already sets
  `egress.allow_private_networks = true`, which also lets `http` and any port through, as
  notifications' own push tests do);
- the telemetry endpoint points at a fake OTLP HTTP receiver, an axum router that counts
  `POST /v1/traces`, `/v1/metrics` and `/v1/logs`.

Every wait polls and returns as soon as its condition holds, under a 30 s ceiling, so nothing
depends on speed.

1. Alice registers; the verification link comes from `FakeBrevo`'s recorded request; she
   confirms and signs in with a bearer session. `merezha identity platform-admin grant` makes her
   the operator.
2. Alice creates organization Acme and invites Bob as `member`; Bob registers, verifies and
   accepts the invitation from the `identity.invitation` mail. Dave registers and creates his own
   organization, Globex, and a Globex task.
3. Bob subscribes to push with `FakePushService::subscription` (`POST
   /v1/notifications/push/subscriptions`, Acme active) and opens `GET
   /v1/notifications/inbox/stream` with his bearer and `Merezha-Organization`. The stream
   answers `hello`.
4. Alice sends `CreateTask`, then `AssignTask` to Bob. Within the ceiling Bob's stream yields an
   `item` event whose `InboxItem` has topic `tasks.assigned`, Acme as `tenant_id` and the task
   title, then `unread` with `{"unread": 1}`. `FakeBrevo` holds one `tasks.assigned` email to
   Bob, and `FakePushService` received one push that decrypts to a payload with topic
   `tasks.assigned` under a valid VAPID token for the generated key.
5. Tenant isolation. Acme's `ListTasks` does not contain Dave's Globex task, and `GetTask` on
   its id from Acme answers `task_not_found`: a tenant query reads only Acme's partition of the
   `board` projection, which never saw Globex's events. Dave's `ListTasks` in Globex shows only his task. Bob, with Globex named in
   `Merezha-Organization`, gets `404 not_found`, since he is not a member.
6. Alice assigns an Acme task to Dave. `GET /v1/application/reactors/notifier/rejections` lists
   `recipient_not_in_tenant` for it, and the next assignment to Bob still reaches his stream.
7. Alice, Acme's `owner`, registers an Acme webhook endpoint on a `WebhookReceiver`; `owner`'s
   `*` expands over the whole catalog, which includes `notifications.webhooks.manage` once the
   notifications block adds its permissions (`crates/cli/src/lib.rs`). Bob completes his task: the
   receiver verifies one `tasks.task_completed` signature, Alice's inbox holds `tasks.completed`,
   and `GET /v1/jobs/admin/queues` shows one delayed job on queue `application`.
8. Alice creates the Acme service account `ci-bot` with the `member` role and two keys: one
   listing only `tasks.read`, one listing `tasks.read` and `tasks.create`. The first lists Acme's
   tasks and gets `403 unauthorized` on `CreateTask`, since a key's power is its list; the second
   creates a task.
9. Alice registers an application-level endpoint (`POST
   /v1/notifications/webhooks`, `event_types = ["content.*"]`) on a second `WebhookReceiver`. She
   passes as `platform_admin` (break-glass, audited), not as `operator`, whose permissions hold
   no webhook permission; the `integrations` role is the everyday route.
   She gives Carol the `editor` role. Carol creates and publishes a help article: anonymous
   `GET /v1/content/help_article/by-slug/{slug}` answers 200, and the application-level endpoint
   receives one signed `content.entry_published`. Alice publishes an Acme brief: Bob reads it
   under `/v1/content/o/{acme}`, an anonymous caller gets 401, and Dave, whose tenant is Globex,
   gets 403.
10. `GET /v1/admin/status` as Alice shows the reactor `active` with `lag_events` 0 and
    `telemetry.signals.traces.exported` above 0; the fake receiver got traces, metrics and logs.
11. Every stderr line's `event` resolves through `merezha_telemetry::names::event`, and no line
    contains a password, a token, a session id or a verification link.
12. SIGTERM: the stream ends with the drain, and the host exits 0.

Steps 3 and 4 are NT-N8's "Tests" line with the Wasm reactor component, which PR #77's
in-process test could not use.

## 6. Security

**Secrets.** Both configuration files reference every secret with `{ env }`; neither ever holds a
literal (`secret_literal` would refuse it anyway). `.env` is the only file holding values: it is
created with mode 0600 inside a staging directory of mode 0700, after the `.gitignore` that
ignores it, and the report names the variables without their values. The identity and notification
keys come from the operating system CSPRNG through `merezha-platform`'s `Random`; the VAPID key
from `jwt-simple`'s `ES256KeyPair::generate`, as `notifications vapid generate` does. Nothing is printed, logged or
written to a committed file. `.env.example` has empty values only. The generated CI makes
throwaway secrets per run, so the repository needs none. Brevo keys and OAuth client secrets are
the provider's and never generated.

**File permissions.** Project files follow the umask from 0644 (0755 for `scripts/`). The
scaffold never widens a mode and never writes outside the target: template paths are checked at
build time (no `..`, no absolute paths, no symbolic links), the target must not be a symbolic
link (its parent is canonicalized), the target cannot be the working directory, and every file
is created with `O_EXCL`.

**Safe defaults in the generated configuration.**

| Setting | Default written | Reason |
| --- | --- | --- |
| Listener | `127.0.0.1:8080`, not public | A private listener: no exposure, development cookies allowed only here |
| CORS | off in development, where the front end's dev server proxies `/v1` (SC-D14); in production one exact origin with credentials, never `*` | Only the application's own front end may call with cookies |
| Cookies | identity's defaults: `__Host-` prefix, `Secure`, `HttpOnly`, `SameSite=Lax`; the loopback development cookie drops `__Host-` and `Secure` only on the private listener (R15) | |
| Registration | `open`, written out with a comment naming `invite_only` and `closed` | Owner answer ID8; written so it is a visible choice |
| Email verification | required | Unverified accounts cannot sign in |
| Development-only switches | `capture`, `allow_http_links`, `allow_http`, `allow_private_networks`, `preview` | Present only in `merezha.toml`; a public listener refuses each one (`refused_when_public`), so the development file cannot be deployed public by mistake |
| Production | Brevo with `webhook_allowed_cidrs` set to Brevo's ranges, TLS to the database, `preview = false`, `.invalid` placeholders | Fails closed until edited |
| Reactors | `allow_addresses` off | Guests cannot send to arbitrary addresses |
| Content | every type declares `access` (CT1); only site types are `public` | |

**What the scaffold refuses.** Everything in 2.3: overwriting anything, symbolic-link targets, the working directory,
names that would collide with the merezha repository's Contremaitre project, block selections
that would not start, and a source pin it cannot vouch for.

**Supply chain.** The SDK is pinned by full commit, never a branch. The first build writes
`Cargo.lock`, which the README says to commit and CI uses with `--locked`. Generated workflows
pin actions by commit and ask only for `contents: read`.

## 7. Delivery slices

Each slice is one pull request with documentation exercised as written, an independent review
and green CI. Tests that start hosts use real PostgreSQL through `TestDatabase` (explicit skip
when `MEREZHA_TEST_DATABASE_URL` is unset; CI sets it and `MEREZHA_TEST_REQUIRE_DATABASE=1`).
`merezha new`, `merezha build` and `merezha version` emit no telemetry events, so no slice adds
catalog rows.

| Slice | Scope | Depends on |
| --- | --- | --- |
| SC-S1 Guest build in the host | library crate `merezha-guest-build` (`crates/guest-build`: the componentize and contract checks of `services/componentize`, ported to `wit-component` 0.258.3), `merezha componentize`, `merezha build` with the `cargo metadata` lookup, `merezha version` with the source revision of 2.9; `services/componentize` as a thin binary over the library | main |
| SC-S2 The `merezha new` command | crate `merezha-scaffold` (build script, `template.toml` index, fragments, substitutions, the `Cargo.toml.tmpl` rule), `merezha new` with every refusal of 2.3, staging and rename, secrets into `.env`, the JSON report; the starter's configuration files only: both `merezha*.toml`, `runtime.toml`, `application.toml`, `content.toml`, the notification templates, `.env.example`, `.gitignore`, `.merezha-version` | SC-S1 |
| SC-S3 The runnable starter | the starter's guest crates and `smoke` crate, `wit/`, `rust-toolchain.toml`, `.contremaitre.yaml` with `db`, `scripts/dev.sh`, `scripts/smoke.sh`, `scripts/ci.sh`, the generated workflow and README; this repository's `scripts/check-scaffold.sh` and `scaffold` workflow for the starter | SC-S2 |
| SC-S4 Telemetry companion | `ops/telemetry/` from TE-T7's embedded export, the `otel` service, OTLP wiring in `scripts/dev.sh`, the Contremaitre `telemetry` verification profile; telemetry.md's T7 row and §6 amended for SC-D13 | SC-S3, TE-T7 |
| SC-S5 The example application | the `teamboard` template, `examples/teamboard/` golden output, `scripts/regenerate-examples.sh`, the golden diff in `scripts/check-scaffold.sh`, `crates/cli/tests/teamboard.rs`, platform.md's AP-C3 follow-up line closed | SC-S3, NT-N8 (PR #77); SC-S4 for its `otel` service; CT relative media root if SC-Q4 is accepted |

**SC-S1 acceptance.** On a fixture workspace the test generates (packages `domain`, `renamed`
with `[lib] name = "renamed_lib"`, and a package with no `cdylib`), `merezha build` writes
`components/domain.wasm` and `components/renamed.wasm`, and the second proves the `cargo
metadata` lookup. The written components load in the runtime and answer the version 2
`describe`. A package missing from the workspace fails `unknown_package`, one without a `cdylib`
`no_cdylib`. A core module importing a WASI interface, exporting a second interface, or over
16 MiB fails `component_rejected` naming the service and leaves no file in `components/`. A
missing wasm target fails `missing_wasm_target` before Cargo runs. `merezha version` prints the
revision `MEREZHA_SOURCE_REV` set at build time. `scripts/check-services.sh` passes with the
services workspace building `merezha-guest-build`.
*Tests:* `crates/guest-build/tests/` takes over `services/componentize/tests/guard.rs` (on the
ported three-argument `targets`); `crates/cli/tests/build.rs` builds the fixture workspace
(skipped with a message when the wasm target is missing; the `services` workflow installs it and
runs it with `MEREZHA_TEST_REQUIRE_WASM_TARGET=1`, as the `scaffold` workflow will);
`crates/cli/tests/source_rev.rs` runs 2.9's fresh-build cases on a probe crate carrying
`crates/cli/build.rs`, which SC-S2's `source_rev.rs` then only extends to `merezha new`.

**SC-S2 acceptance.** For every block selection of 2.2, the generated configuration loads through
`host::configure` with every module registered, for both files (3.14), and `content.toml` and the
notification templates validate. Each refusal of 2.3 has a test that also proves nothing was
written, including a target filled between the checks and the rename, `--dir` under a symbolic
link parent (`/tmp` on macOS, accepted) and the current directory (`target_is_working_directory`).
`.env` is 0600 and `git check-ignore .env` succeeds in the generated tree after `git init`. No
generated file outside `.env` contains a generated secret. Source pin, on fresh builds only
(2.9): `cargo install --git file://<checkout> --rev <sha> merezha-cli` produces a binary whose
`merezha new` pins `<sha>` although the checkout holds Cargo's untracked `.cargo-ok`; a fresh
build of a checkout with a modified tracked file records none and `merezha new` refuses with
`source_rev_unknown`; a second `cargo build` with nothing changed does not rerun the build
script (its `rerun-if-changed` paths all exist and `git status` leaves the index alone). Rendering with an unknown `%%name%%` fails
the build. No file under `crates/scaffold/templates/` is named `Cargo.toml`.
*Tests:* `crates/scaffold/tests/render.rs` (the matrix, the index rules, placeholder failures,
build-time path checks), `crates/scaffold/tests/source_rev.rs` (the three source pin cases on a
temporary git repository, each in an empty target directory), `crates/cli/tests/new.rs` (process tests of the command, its JSON
report and its refusals).
*As built (SC-S2):* the revision stays recorded by `crates/cli/build.rs`;
the CLI hands it to `merezha-scaffold`, whose std-only `src/pin.rs` picks the
pin, and `crates/cli/tests/source_rev.rs` carries that file in its probe
crate for the three source pin cases. The configuration matrix is in
`crates/cli/tests/new.rs`, which has every block registered; `render.rs`
keeps the rendering rules. The staging directory (0700) holds a `project`
directory created at `0755 & !umask`, which is what is renamed onto the
target, so the umask is never read or changed. The index also has `[[tree]]`
(a directory kept by `when`/`unless`), `[[secret]]` (the variables `.env`
gets, by block), `fixed` and `next`; fragments are concatenated in the
listed order and every `NN-kernel.*` applies. `.gitignore` and `.env.example`
are stored as `gitignore` and `env/*.example`; `.env` and `.merezha-version`
are written by the command and no template may produce them. The SPDX header
of SC-Q9 is written by the renderer (`render::LICENSE`) rather than stored
in each template. `wit/service.wit` is embedded with SC-S3, which renders it.
`merezha notifications check` now registers every block, as `merezha run`
does: before, any project with identity or content failed it with
`unknown_module`.

**SC-S3 acceptance.** For the starter with all blocks, with `identity,notifications` and with
`none`, `scripts/check-scaffold.sh` renders the project into `.scaffold-check/<case>` with
`--merezha-path .` and runs the project's own `scripts/ci.sh` in it with `MEREZHA_BIN` pointing at
the host just built: formatting, both Clippy runs, `cargo test`, `merezha build`, both
`notifications check` runs and `scripts/smoke.sh` against the workflow's PostgreSQL all pass. A
fresh project pinned with `git = "file://<checkout>"` and the checkout's `rev` builds offline
without Cargo's "skipping duplicate package" warning, proving the `Cargo.toml.tmpl` rule. `actionlint` passes on the generated
workflow.
*Tests:* the guest crates' native tests; the `scaffold` workflow itself.

**SC-S4 acceptance.** `contremaitre ensure --json` builds `otel` from the generated Dockerfile;
`scripts/dev.sh` exports to it; the `telemetry` verification profile finds a trace in Tempo and
`http.server.request.duration` in Prometheus after one request. `merezha new` writes the same
dashboards `merezha telemetry export` does (a byte comparison test). CI cannot run Contremaitre,
so the profile is run by hand on macOS and its output goes in the pull request.
*Tests:* the byte comparison in `crates/scaffold/tests/render.rs`; `promtool check rules` on the
generated `rules/` beside TE-T7's own check.

**SC-S5 acceptance.** The golden diff is empty on main and fails when a template file changes
without regenerating. `crates/cli/tests/teamboard.rs` passes all twelve steps of 5.10.
`scripts/check-scaffold.sh` runs `examples/teamboard/scripts/ci.sh` as it does the starter's, and
the example's `smoke` scenario covers the invitation, the SSE item and tenant isolation.
*Tests:* the end-to-end test; the example's guest crates' native tests (assignment rules, roles,
partitioned projection walk, the notifier's effect lists including the self-completion case).

**Existing scripts and CI.**

| File | Change | Slice |
| --- | --- | --- |
| `services/componentize/` | keeps its package name `merezha-componentize`, its binary and its command line; its library code moves to `crates/guest-build`, and the binary becomes a thin `main` over it (path dependency). `services/Cargo.lock` gains `wit-component` and `wit-parser` 0.258.3 for it (0.262.0 stays for `wit-bindgen`) and drops the old dev-only `wat` | SC-S1 |
| `scripts/build-services.sh` | its commands stay (`cargo build -p merezha-componentize`, then `release/merezha-componentize`); what they compile now includes `crates/guest-build` | SC-S1 |
| `scripts/check-services.sh` | unchanged; it covers the moved code through the build above | SC-S1 |
| `Cargo.toml` | members gain `crates/guest-build` and `crates/scaffold` | SC-S1, SC-S2 |
| `.github/workflows/services.yml` | paths gain `crates/guest-build/**`; a step runs `crates/cli/tests/build.rs` and `source_rev.rs` with the wasm target required; `MEREZHA_SOURCE_REV` is the commit (also in `host.yml`) | SC-S1 |
| `.github/workflows/scaffold.yml` (new) | Linux, Rust 1.98.0 with the wasm target, a `postgres:17` service: build the host, `scripts/check-scaffold.sh` (S3), then the golden diff, `merezha build` on `examples/teamboard` and `cargo test -p merezha-cli --test teamboard` with `MEREZHA_TEST_EXAMPLE_COMPONENTS` and `MEREZHA_TEST_REQUIRE_EXAMPLE=1` (S5). Triggered by `crates/**`, `examples/**`, `wit/**`, `scripts/check-scaffold.sh`, `Cargo.*` and itself | SC-S3, SC-S5 |
| `.github/workflows/host.yml` | sets `MEREZHA_SOURCE_REV` (SC-S1); it runs the new crates' unit tests through `--workspace`, and `teamboard.rs` skips there | |
| `scripts/check-scaffold.sh`, `scripts/regenerate-examples.sh` (new) | 5.9 and SC-S3 | SC-S3, SC-S5 |
| `.gitignore` | `/.scaffold-check/` | SC-S3 |
| `README.md` | "Start a project" with `merezha new`, before "Platform host" | SC-S3 |
| `docs/architecture/platform.md` | the four commands; "Limits and follow-ups" AP-C3 line closed | SC-S1, SC-S2, SC-S5 |
| `docs/design/telemetry.md` | T7 row and §6: the scaffold copy, the example's manifest and the verification profile move to SC-S4 | SC-S4 |
| `docs/design/cross-block-decisions.md` | "Delivery order", stage 3: `merezha new` and the example application name SC-S1 to SC-S5 | SC-S2 |

## 8. Decisions

**SC-D1, the command.** *Context:* the framework names `merezha new` and nothing else. *Decision:*
the synopsis of 2.1, no `--force`, no network, no subprocess, the project written whole through
a staging directory. *Alternatives:* an interactive prompt (rejected: agents and CI cannot answer
it, and every choice fits a flag); running `git init` and `cargo build` (rejected: they need
network and toolchains, and a failure would leave a half-made project the user did not ask for).

**SC-D2, two templates.** *Context:* the program needs a starting point and an example that uses
every block. *Decision:* `starter` is the smallest project that still has a domain, a view and a
reactor; `teamboard` is the full example. *Alternatives:* one template with every block (rejected:
a new project should not start with organizations, roles and briefs it may not want);
`--example` as a separate flag (rejected: a template name says the same thing).

**SC-D3, embedding.** See 2.7. *Consequence:* a template edit rebuilds `merezha-scaffold` and
nothing else.

**SC-D4, rendering without an engine.** *Context:* the generated files differ by block selection
and by five values. *Decision:* fragments plus closed substitutions. *Alternatives:* minijinja,
already a workspace dependency for notification templates (rejected: conditionals inside
`merezha.toml` are harder to read than one fragment per block, and `{{ }}` would collide with the
notification templates the scaffold copies).

**SC-D5, two configuration files.** *Context:* development and production differ in nested module
keys (`email.driver`, `templates.allow_http_links`, `webhooks.allow_http`, `egress`), which no
environment variable or flag can set. *Decision:* `merezha.toml` and `merezha.production.toml`,
both checked by the generated CI. *Consequence:* topics and roles are written twice; SC-Q3 asks
whether the kernel should layer files.

**SC-D6, secrets.** *Context:* a project must start right after `merezha new`, and secrets must
never reach Git. *Decision:* generated values in an ignored 0600 `.env`, `{ env }` references in
both files. *Alternatives:* `{ file }` references to an ignored secrets directory (rejected:
platforms inject environment variables, and the same names then work everywhere); only
`.env.example` (rejected: the first run would need five manual steps, offered instead as
`--no-secrets`).

**SC-D7, the build.** *Context:* a guest needs a core module build and a componentize step;
today the second lives in the services workspace. *Decision:* the host gains `merezha build` and
`merezha componentize` on the shared library `merezha-guest-build`, and finds each service's
artifact through `cargo metadata` rather than a naming rule. *Alternatives:* a componentize crate copied into each
project (rejected: two copies of the contract check drift); `cargo install merezha-componentize`
(rejected: a second tool to pin and install for every project).

**SC-D8, the SDK pin.** See 4.4.

**SC-D9, the local stack.** *Context:* the owner chose Contremaitre over Docker Compose. *Decision:*
managed PostgreSQL 17 (R14 lets the environment keep 17 above the floor of 14) and the LGTM
container; the capture driver as mail sink; the host native. *Alternatives:* Mailpit (rejected: no
SMTP driver exists to feed it); the host as a Contremaitre `app` built from source in a
Dockerfile (rejected for now: every environment would compile Wasmtime; SC-Q1).

**SC-D10, the example's product.** *Context:* "using every block" has to be concrete. *Decision:*
the team task board of section 5, since tasks already have a domain, a view and a reactor in
`services/`, and teams bring organizations, roles, keys and tenant content naturally. SMS is the
one channel it leaves off, for ID10's reason.

**SC-D11, golden example.** *Context:* the example must not drift from what users get.
*Decision:* the template is the source and `examples/teamboard/` its checked output.
*Alternatives:* only the template (rejected: nothing to browse and nothing stable to test against);
only `examples/` with the template generated from it (rejected: placeholders would have to be
reverse-substituted).

**SC-D12, the end-to-end test.** *Context:* `FakeBrevo`, `WebhookReceiver`, `TestDatabase` and the
catalog are host test kits a generated project cannot depend on. *Decision:* the full test lives
in `crates/cli/tests/`, and each generated project carries a dependency-free `smoke` scenario. The
test covers two organizations, so tenant isolation, a stage 3 guarantee, is checked on the Wasm
components and not only in AP-C2's suites.

**SC-D13, telemetry ownership.** *Context:* TE-T7 listed the scaffold's copy and the example's
Contremaitre manifest before this design existed. *Decision:* TE-T7 keeps the artifacts, the
export command and the operations guide section; the scaffold slices take the rest, and SC-S4
amends telemetry.md.

**SC-D14, the front end's origin.** *Context:* the development front end (`localhost:5173`) and
the host (`127.0.0.1:8080`) are different sites, and production usually serves `app.` and `api.`
as different origins of one site. *Decision:* in development, a dev server proxy for `/v1`, so
there is one origin and the development cookie works; in production, `[http.cors]` with the exact
front end origin and credentials. *Alternatives:* CORS in development too (rejected: the session
cookie still would not cross from `localhost` to `127.0.0.1`, and `localhost:8080` as the listen
address would make the cookie depend on name resolution); no CORS in production (rejected for
the default: serving front end and API from one origin behind a proxy works too, and the README
says to delete `[http.cors]` then, but separate origins are the common deployment).

## 9. Open questions

Each has a recommended answer; the slices assume it until the owner decides.

| Id | Question | Recommendation |
| --- | --- | --- |
| SC-Q1 | How is the prebuilt `merezha` host distributed? Nothing is released today. | Now: `cargo install --locked --git ... --rev`, which the scaffold and generated CI already use. With the first tag: a release workflow publishing Linux x86_64 and aarch64 and macOS aarch64 binaries with SHA-256 checksums, then an OCI image for the PaaS. That image would also let Contremaitre run the host as an `app` service. |
| SC-Q2 | Publish `merezha-domain` to crates.io, or keep git revisions? | Keep git revisions until guest protocol 2 has shipped in a tagged release; then publish `merezha-domain` with the WIT inside it, and switch the scaffold to a version requirement. The four names were free on 2026-10-27. |
| SC-Q3 | Should the kernel layer configuration files (a base plus an environment file), so topics and roles are written once? | Not in v1: two files, both checked in CI. Open a platform design item for `--config base.toml --config production.toml` with the same precedence rules, and move the scaffold to it when it lands. |
| SC-Q4 | Should content accept a relative `media.local.root` (and `spool_dir`), resolved against the configuration directory like `model` and `templates.dir`? | Yes, still created 0700 and refused when it is a symbolic link or open to others. It is a content change (content.md §6, CT follow-up), not a new key; it lets committed configurations use media, and the example then gains a cover image. |
| SC-Q5 | Should `merezha new` write `.env` with generated secrets by default? | Yes, mode 0600 and ignored, with `--no-secrets` for those who manage secrets elsewhere. |
| SC-Q6 | Is GitHub Actions the only generated CI? | Yes for v1, matching this repository. A `--ci none` flag can come with a second provider. |
| SC-Q7 | Should the starter turn on Web Push and webhooks, which a first project may not need? | Yes. The VAPID key is generated, neither section does work until a client subscribes or an endpoint is registered, and both give the reactor and the README something to show. Turning one on later is cheap too, since `database.migrate = true` applies the block's tables at the next start; the reason to start with them on is that the starter teaches the whole notification path. Delete the section to turn one off; with `[modules.notifications.webhooks]` gone, the notifier's `webhook` effect becomes a recorded rejection (`webhooks_disabled`) until it is removed from `effects`. |
| SC-Q8 | Once `teamboard` lands there are three task boards: `services/task-*` with `examples/task-board`, the reactor test fixtures, and `teamboard`. Retire the old ones? | Retire `services/task-notifier` once SC-S5 merges: `teamboard`'s notifier supersedes it, and no host test runs it as a component (the reactor tests use in-process guests). Keep `services/task-domain`, `task-view`, `task-events` and `examples/task-board`: they drive `merezha serve` with memory and SQLite storage (`scripts/task-board-demo.sh`, the restart demo), a host path `teamboard` does not cover. Their docs then point to `teamboard` as the framework example. |
| SC-Q9 | Which licence do generated projects carry? Everything `merezha new` copies is this repository's code, under MIT, which asks for its notice in substantial copies: the templates, `wit/service.wit` (embedded from the repository root) and, with telemetry, TE-T7's dashboards and rules. | Dual-license every copied file as `MIT OR MIT-0` with a per-file SPDX header where the format allows comments (Rust, TOML, YAML, shell, WIT) and a `LICENSE-MIT-0` beside the JSON dashboards, so the generated project may take MIT-0 and owes no notice. That covers `crates/scaffold/templates/`, `wit/service.wit` and `deploy/telemetry/`. `crates/scaffold` itself stays `MIT`; if it ever declares `MIT AND MIT-0`, `deny.toml`'s allow list needs `MIT-0`. Generated projects get no `LICENSE` file and no `license` key (`publish = false`); the README asks the owner to choose one. If the owner prefers not to relicense, narrow the claim: the README then says the copied files keep their MIT notice. |
| SC-Q10 | How do `scripts/dev.sh` and CI check a `path` pin, where there is no revision to compare with `merezha version`? | As 4.3's table says: compare with `git -C <path> rev-parse HEAD` when the path is a git checkout with no modified tracked file, and only warn, on a difference or when it cannot tell. Path pins are for working on merezha itself (the golden example, local changes), where the host is built from the same checkout; in this repository's CI the host and the example come from one commit, so nothing needs to fail there. |

## Cross-block dependencies

| Need | Owner | What the scaffold assumes |
| --- | --- | --- |
| `[jobs.queues]` settings for the notification queues | NT §7.2 | 3.2 writes them verbatim |
| Content events on webhooks through AP subscriptions | NT-N8 (PR #77) | the example's content webhooks (5.5) |
| `deploy/telemetry/` and `merezha telemetry export` | TE-T7 | SC-S4 embeds the same export |
| `db.pool_undersized` in the catalog | TE-T7 (in progress) | 3.2 names the warning; it is emitted today but not yet in `names.rs` |
| Relative media root | CT, if SC-Q4 is accepted | the example's cover image |
| `notifications vapid generate`'s key generator as a library call | NT-N6 | `merezha new` reuses it for `.env` |
| `random_token` and the CSPRNG | K (`merezha-platform`) | identity and notification secrets |
| Route permissions, tenant streams, partitioned projections, reactors | AP-C2, AP-C3 | `application.toml` of both templates |
| The guest SDK and protocol version 2 | AP-C1 | the generated guests |
