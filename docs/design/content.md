# Content block design

Status: proposed 2026-10-23, aligned 2026-10-24 with
[the cross-block decisions](cross-block-decisions.md), which win where this
document differs. Conforms to [the framework architecture](../architecture/framework.md)
with the amendment recorded there (R15). Needs from other blocks are listed
under [cross-block dependencies](#cross-block-dependencies).

The content block is a headless CMS API in the Merezha host: content types in
a TOML file in the project repository, entries with drafts, publishing,
revisions and per-locale variants, and media in a `BlobStore` with
local and S3-compatible drivers. There is no admin UI. Editors use a frontend
the project builds on the management API; sites read the delivery API. It
ships as `merezha-content` (`crates/content`), a platform `Module` with schema
`merezha_content` and routes under `/v1/content/`.

Scheduled publishing and model migrations (`[[migrations]]`, `convert`) are
deferred from v1 ([cross-block decisions](cross-block-decisions.md#deferred-from-v1)).
They are designed under [later slices](#later-slices-outside-the-v1-order);
the rest of this document describes v1 without them.

## What the established CMSs taught us

| System | What we take | What we leave |
| --- | --- | --- |
| Contentful | Separate delivery and management APIs. Version-based optimistic locking on every write. Locale fallback chains. Thin webhooks. Scheduled actions. | Field-level localization on every field, environments, a vendor rich text format, the `includes` sidecar for links. |
| Sanity | Portable Text as an open rich text format. `_key` on every array item so clients can diff and patch lists. Drafts kept apart from the published document. | GROQ, a custom query language, and the content lake. |
| Strapi v5 | Document-level i18n, where each locale is a full variant with its own draft and published state. Components and dynamic zones. Explicit `populate`, off by default. Batch publish ("releases"). | Database tables per content type, which turn every model change into DDL. |
| Payload | Schema as code in the project repository. Population depth limits. Revision retention per document. Globals as singletons. | Arbitrary access-control functions in the schema and the Lexical-specific rich text. |
| Directus | Singletons as a first-class kind. | Database-first modelling, where the SQL schema is the content model. |

The main lesson is cost. Mapping content types to SQL tables makes migrations
painful, and arbitrary filters over arbitrary fields make queries slow. Here
entry data is validated JSONB, and only fields declared `filterable` or
`sortable` can be filtered or sorted, each with a known index.

## 1. Language

| Term | Meaning |
| --- | --- |
| Content model | The whole set of content types, components and locales for one application, declared in `content.toml` with an integer `version`. |
| Content type | A named schema for entries, such as `article`. It has a kind, a tenancy scope, a delivery access rule and an ordered list of fields. |
| Collection | A content type with any number of entries. |
| Singleton | A content type with at most one entry per scope, such as `site_settings`. Its routes take no entry id. |
| Component | A named, reusable group of fields embedded inside entries, such as `seo`. A component has no identity and no lifecycle of its own. |
| Field | A named, typed slot in a content type or component, with validation rules. |
| Entry | One content item of a content type, identified by a UUIDv7. It owns one variant per locale. |
| Locale | A BCP 47 code configured in the model, such as `fr-BE`. Exactly one locale is the default. Each non-default locale may name a fallback locale. |
| Variant | The per-locale part of an entry: one draft, an optional published revision and its revision history. A non-localized type has only the default-locale variant. |
| Draft | The current editable data of a variant. Every save of a draft creates a new revision. Drafts may be incomplete: `required` is checked at publish time. |
| Revision | An immutable, numbered snapshot of a variant's data. Revision numbers start at 1 and increase by one per save, per variant. The draft is always the latest revision. |
| Published version | The revision of a variant that the delivery API serves. Publishing points the variant at a revision; it does not copy or create a revision. |
| Status | Derived per variant: `draft` (never published or unpublished), `published` (the published revision is the draft revision), `changed` (published, with a newer draft). |
| Shared field | A field with `localized = false` in a localized type. The default-locale variant owns its value; other variants read it. |
| Slug | A URL segment field, lowercase ASCII words joined by hyphens, unique per type, scope and locale. |
| Reference | A field value pointing at another entry, with cardinality one or many and a list of allowed target types. |
| Asset | An uploaded file with metadata (filename, content type, size, SHA-256, image dimensions). Its bytes are a blob. |
| Blob | The stored bytes, addressed by content hash. Two assets with the same bytes in the same scope share one blob. |
| Scope | The tenancy boundary of an entry or asset, `app` or `org:<organization id>`. |
| Stage | `published` (delivery default) or `draft` (preview). |

## 2. Content model definition

### File and loading

`merezha.toml` points at the model file:

```toml
[modules.content]                # the section's presence enables the module
model = "content.toml"
```

The path follows the runtime manifest's rules: relative to `merezha.toml`, no
parent traversal, a regular file inside the project. The host validates the
model before listening and reports every problem at once with a TOML path, for
example `content_model_invalid: types.article.fields[2].max_length must be
between 1 and 4096` or `content_model_invalid: types.article.access is
required`.
The canonical JSON form of the model (sorted keys, defaults filled in) hashed
with SHA-256 is its fingerprint.

### Example

```toml
version = 3

locales = [
  { code = "en", default = true },
  { code = "fr", fallback = "en" },
  { code = "fr-BE", fallback = "fr" },
]

[components.seo]
description = "Search and social metadata"
fields = [
  { name = "title", type = "string", max_length = 70 },
  { name = "description", type = "text", max_length = 160 },
  { name = "image", type = "asset", accept = ["image/*"] },
]

[components.quote]
fields = [
  { name = "text", type = "text", required = true },
  { name = "attribution", type = "string" },
]

[types.author]
access = "public"
title_field = "name"
fields = [
  { name = "name", type = "string", required = true, sortable = true },
  { name = "avatar", type = "asset", accept = ["image/png", "image/jpeg", "image/webp"], localized = false },
]

[types.article]
kind = "collection"
scope = "application"
localized = true
access = "public"
title_field = "title"
fields = [
  { name = "title", type = "string", required = true, max_length = 200, sortable = true },
  { name = "slug", type = "slug", from = "title", required = true },
  { name = "category", type = "enum", values = ["news", "guide", "release"], required = true, filterable = true },
  { name = "published_on", type = "date", sortable = true },
  { name = "author", type = "reference", to = ["author"], cardinality = "one", localized = false, filterable = true },
  { name = "tags", type = "list", of = { type = "string", max_length = 40 }, max_items = 20, filterable = true },
  { name = "body", type = "rich_text", styles = ["normal", "h2", "h3", "blockquote"], marks = ["strong", "em", "code"], annotations = ["link", "entry_link"], lists = ["bullet", "number"], embeds = ["asset", "quote"] },
  { name = "sections", type = "blocks", components = ["quote", "seo"], max_items = 30 },
  { name = "seo", type = "component", component = "seo" },
  { name = "reading_minutes", type = "integer", min = 1, max = 600, sortable = true },
]

[types.site_settings]
kind = "singleton"
access = "public"
fields = [
  { name = "site_name", type = "string", required = true },
  { name = "price_floor", type = "decimal", precision = 10, scale = 2 },
]

[types.partner_brief]
access = { permission = "content.delivery.read" }
fields = [{ name = "title", type = "string", required = true }]
```

`fields` is an array because field order is part of the model: it is the
order of `fields` in API responses and in the model endpoint that a frontend
uses to build its forms. TOML tables are maps and would lose it. Inline tables
must fit on one line in TOML 1.0. Long field definitions can use
`[[types.article.fields]]` array tables instead, which parse to the same model.

### Type-level keys

| Key | Values | Default | Notes |
| --- | --- | --- | --- |
| `kind` | `collection`, `singleton` | `collection` | |
| `scope` | `application`, `organization` | `application` | See section 8. `organization` needs identity with organizations enabled. |
| `localized` | bool | `true` | `false` means the type has only the default-locale variant. |
| `access` | `"public"`, `"authenticated"`, `{ permission = "<name>" }` | none, required | Who reads published entries through delivery (owner answer CT1). `public`: anyone, anonymous included. `authenticated`: any principal, and for organization types one whose tenant is the path's organization. A permission: checked in the type's resource scope (section 5); it must exist in the RBAC catalog. A missing or unknown value fails model validation. |
| `title_field` | field name | none | Must be a `string` field. Used in revision listings, referrer listings and slug generation defaults. |
| `description` | string, at most 500 chars | none | Documentation only, returned by the model endpoint. |
| `fields` | array | required | 1 to 100 fields. |

Type and component names match `^[a-z][a-z0-9_]{0,47}$`. The names `manage`,
`assets`, `model`, `o` and `health` are reserved because they share the
delivery path namespace. Field names match `^[a-z][a-z0-9_]{0,63}$`; `id`,
`type`, `locale`, `sys` and anything starting with `_` are reserved.

Model limits: 64 types, 32 components, 32 locales, 100 fields per type or
component, nesting depth 4, 8 `unique` and 16 `filterable` or `sortable`
fields per type.

### Common field keys

| Key | Applies to | Default | Meaning |
| --- | --- | --- | --- |
| `name` | all | required | Field name. |
| `type` | all | required | One of the field types below. |
| `description` | all | none | Documentation only. |
| `required` | all | `false` | Value must be present and non-empty at publish time. Empty string, empty list and empty rich text count as absent. |
| `localized` | all | type's `localized` | `false` makes it a shared field owned by the default-locale variant. Only meaningful in a localized type; not allowed inside components (a component inherits the containing field's setting). |
| `default` | scalar types | none | Value filled in when a new variant is created without the field. |
| `unique` | `string`, `slug`, `integer`, `enum` | `false`, `true` for `slug` | Unique per type, scope and locale (per type and scope for shared fields), enforced on drafts and on published rows by database indexes. Can be set on a new field only; adding it to an existing field is refused in v1 (see schema changes). |
| `filterable` | scalars, `reference`, lists of scalars | `false` | Field may appear in equality, `in` and `has` filters. |
| `sortable` | `string`, `slug`, `integer`, `float`, `decimal`, `date`, `datetime`, `enum` | `false` | Field may be sorted on and used with range filters. Implies `filterable`. |

`unique`, `filterable` and `sortable` are only allowed on top-level fields of a
content type. Fields inside components cannot be filtered in v1.

### Field types

| Type | JSON value | Type-specific keys | Rules |
| --- | --- | --- | --- |
| `string` | string | `min_length`, `max_length` (default 255, max 4096), `pattern` | Single line: no CR or LF. Length counts Unicode scalar values. |
| `text` | string | `min_length`, `max_length` (default 10000, max 100000) | Multi-line plain text. |
| `rich_text` | Portable Text array | `styles`, `marks`, `annotations`, `lists`, `embeds`, `max_blocks` (default 500, max 5000), `max_text_length` (default 100000) | See below. |
| `integer` | JSON integer | `min`, `max` | Must decode as an integer, so `1.0` is rejected. JSON `-0` is the integer 0; `-0.0` and `-0e0` are accepted as 0 too, because the JSON parser cannot tell them apart from `-0`. Range is ±9,007,199,254,740,991, the same safe range as application values. |
| `float` | JSON number | `min`, `max` | Finite binary64. Decimal lexical precision is not preserved, as in the application JSON policy. |
| `decimal` | string | `precision` (default 18, max 28), `scale` (default 2, at most `precision`), `min`, `max` as decimal strings | Matches `^-?(0|[1-9][0-9]*)(\.[0-9]+)?$`. Stored normalized to exactly `scale` fractional digits: `"10"` becomes `"10.00"`, more digits than `scale` is an error, not rounding. Use it for money. |
| `boolean` | `true` or `false` | none | |
| `datetime` | string | `min`, `max` | RFC 3339 with an offset. At most 3 fractional digits. Stored as UTC `YYYY-MM-DDTHH:MM:SS.sssZ`, so the stored strings sort in time order. |
| `date` | string | `min`, `max` | `YYYY-MM-DD`, a calendar date with no zone. |
| `enum` | string | `values` (1 to 200 identifiers matching `^[a-z0-9][a-z0-9_-]{0,63}$`) | |
| `slug` | string | `from` (a sibling `string` field; defaults to the type's `title_field` for top-level slugs), `max_length` (default 96, max 200) | Matches `^[a-z0-9]+(?:-[a-z0-9]+)*$`. When a save leaves it absent and `from` is set, the host transliterates the source field with `deunicode`, lowercases it, replaces runs of other characters with `-` and truncates at a word boundary. The host never changes an existing slug. |
| `reference` | `{"id": "<uuid>", "type": "<type>"}` or an array of them | `to` (1 to 16 types), `cardinality` (`one`, `many`), `min_items`, `max_items` (default 100, max 1000), `require_published` (default `false`) | Writes may omit `type`; the host fills it. The target must exist, not be in the trash, be one of the `to` types, and be in the same scope or in the application scope. With `require_published`, publishing fails while a target has no published variant in the same locale chain. |
| `asset` | `{"id": "<uuid>", "alt": "..."}` or an array of them | `accept` (MIME types or `image/*` patterns), `cardinality`, `min_items`, `max_items` | `alt` is optional, at most 500 chars. Alt text belongs to the field value, so each entry and locale can describe the same image differently. |
| `component` | object | `component` | Validated against the component's fields. |
| `list` | array | `of` (an inline field definition of a scalar type or `component`), `min_items`, `max_items` (default 100, max 1000) | Items of component lists carry `_key`. |
| `blocks` | array of `{"_type": "<component>", "_key": "...", ...fields}` | `components` (1 to 32), `min_items`, `max_items` | A tagged union of components, the "dynamic zone" for page builders. |
| `json` | any JSON | `max_bytes` (default 16384, max 262144) | Opaque. Not filterable. Integers outside the safe range are rejected, as everywhere. |

`pattern` uses the linear-time `regex` crate, at most 1024 bytes of source,
anchored as `^(?:...)$` and compiled at startup with a 1 MiB size limit on
the program and on the lazy DFA, so ordinary patterns such as `.{1,255}` or
`\w{5}` fit. Any string containing U+0000 is
rejected, `json` fields included, because `jsonb` cannot store it. `_key`
values match `^[A-Za-z0-9_-]{1,32}$` with at least one non-digit character
(an all-digit key would read as an array index in error paths), are unique within their array, and are
assigned randomly (12 characters) when a write omits them. They identify list
items across revisions and appear in reference paths.

### Rich text format: Portable Text

`rich_text` stores a [Portable Text](https://github.com/portabletext/portabletext)
array. I picked it over ProseMirror JSON and Lexical for four reasons:

1. It is a flat array of blocks holding spans, so the host validates it in one
   pass with a fixed depth bound. ProseMirror documents are trees of any depth.
2. It does not tie the API to an editor. ProseMirror node names come from each
   editor's schema (Tiptap and Remirror differ), and Lexical nodes are
   versioned classes of one editor.
3. Maintained renderers exist for HTML, React, Vue, Svelte, PHP, Python and Go,
   plus client-side converters from ProseMirror and HTML.
4. Its `_key` and `_type` conventions are the ones the `blocks` field uses.

Tables and nested blocks are not in the spec. Tables become an embedded
component and nested lists use `level`.

Accepted structure, everything else rejected with `rich_text_invalid` and a
JSON pointer:

```json
[
  { "_type": "block", "_key": "a1", "style": "h2", "markDefs": [],
    "children": [{ "_type": "span", "_key": "a1s", "text": "Release notes", "marks": [] }] },
  { "_type": "block", "_key": "b2", "style": "normal", "listItem": "bullet", "level": 1,
    "markDefs": [{ "_type": "link", "_key": "l1", "href": "https://example.com/changelog" },
                 { "_type": "entry_link", "_key": "l2", "id": "0192f7a0-5d1c-7c3e-9b1a-6f0e2c4d8a11", "type": "article" }],
    "children": [{ "_type": "span", "_key": "b2a", "text": "See the ", "marks": [] },
                 { "_type": "span", "_key": "b2b", "text": "changelog", "marks": ["strong", "l1"] }] },
  { "_type": "asset", "_key": "c3", "id": "0192f7a1-0b44-7aa2-8c55-1d3e9f20a7b4", "alt": "Team photo" },
  { "_type": "quote", "_key": "d4", "text": "Ship small.", "attribution": "The team" }
]
```

- `style` comes from the field's `styles` (`normal`, `h1` to `h6`,
  `blockquote`). `listItem` comes from `lists` (`bullet`, `number`), with
  `level` 1 to 6.
- `marks` holds decorators from the field's `marks` (`strong`, `em`, `code`,
  `underline`, `strike`) or the `_key` of a `markDefs` entry in the same block.
- `markDefs` are `link` (`href` with scheme `https`, `http` or `mailto`, at most
  2048 bytes) or `entry_link` (reference rules, targets limited by
  `entry_link_to`, default every type).
- Embedded objects are `asset` or a component listed in `embeds`, with the
  component's fields at the top level. `max_text_length` bounds total span
  text and `max_blocks` the block count.

### Write-time and publish-time validation

A draft save checks every rule except `required` and `min_items`, so editors
can save half-finished work. Publishing checks all of them, plus
`require_published`. Both report every violation at once:

```json
{ "error": { "code": "validation_failed", "classification": "permanent", "details": [
  { "path": "/fields/title", "code": "required" },
  { "path": "/fields/sections/k7Qa/text", "code": "max_length" },
  { "path": "/fields/author", "code": "reference_not_found" } ] } }
```

Paths are JSON pointers built from field names and `_key`s, never from user
text. Detail codes: `required`, `type_mismatch`, `min_length`, `max_length`,
`pattern`, `min`, `max`, `precision`, `scale`, `not_in_enum`, `min_items`,
`max_items`, `unknown_field`, `field_not_localized`, `duplicate_key`,
`invalid_character`, `datetime_precision`, `reference_not_found`,
`reference_type`, `reference_scope`, `reference_unpublished`,
`asset_not_found`, `asset_type`, `rich_text_invalid`, `unique`, `json_too_large`.
Since C9 `reference_scope` stays in the list but is never reported: a
target outside the scopes an entry may reference is `reference_not_found`
(section 8).

### Schema changes

`version` must increase whenever the fingerprint changes, or startup fails
with `content_model_version_unchanged`. At startup the host diffs the active
model in the database against the file and classifies every change. v1
activates only changes that rewrite no stored data and refuses the rest:

| Change | Class | What happens |
| --- | --- | --- |
| New type, component, optional field or locale | additive | Activated. |
| New `required` field | additive | Activated. Existing drafts are unaffected; the next publish of each entry needs the field. `default` covers new variants only; existing variants are not backfilled. |
| New field declared `unique`, the slug of a new type included | additive | Activated after its unique indexes are built (section 3). No host can write the field before activation, so the build cannot meet a duplicate. |
| Relaxed rule: higher `max`, lower `min`, wider `pattern`, new enum value, `required` removed, new `to` target, new `accept` type | additive | Activated. |
| Tightened rule: lower `max_length`, narrower range, `pattern` added, `required` added on an existing field | tightening | Activated. Rules apply at the next write. The host counts drafts and published rows that would fail and logs `content.model_tightened` with counts per field. Published content keeps being served. |
| `access` changed | access | Activated. Applies to the next request on a current host; the generation bump changes every ETag of the scope. |
| `filterable` or `sortable` added or removed, `unique` removed | index | Activated. New indexes are built before activation, unneeded ones dropped after it (section 3). |
| `unique` added to an existing field | refused | Startup fails with `content_model_breaking`. |
| Field removed or renamed, field type changed, enum value removed, type removed or renamed, `localized` changed, `scope` changed, `kind` changed, locale removed, default locale changed | refused | Startup fails with `content_model_breaking`, listing each change. |

`unique` on an existing field is refused because it cannot be made safe
without stopping writes: values may already collide, and between a duplicate
check and the end of a concurrent index build any writer can add a duplicate
that fails the build. The v1 path is a new `unique` field, which activates
safely, filled through the management API, where each colliding write fails
with a `unique` detail. A refused change is fixed in the file, never in the
database: the active model and the hosts running it are untouched.
`[[migrations]]` blocks are not accepted in v1 (`content_model_invalid`); the
later slice C10 designs them.

The table leaves some keys open; C2 classifies them by the same rule (does
stored data still fit, unrewritten?). Removing a component, changing a
field's `component`, `cardinality` or decimal `scale` (stored decimals are
normalized to it), and removing a `blocks` component are refused, like a
removed enum value. Removing a `to` target, an `accept` type or rich text
`styles`, `marks`, `annotations`, `lists` or `embeds`, setting
`entry_link_to`, and lowering `precision`, `max_blocks`, `max_text_length`
or `max_bytes` are tightening; a changed `pattern` counts as tightening
because a wider one cannot be told apart. Descriptions, defaults, slug
`from`, `title_field`, field, locale and enum value order, and a new locale
fallback are additive. Any key the classifier does not know is refused.
The tightening count validates at most 100,000 drafts and published rows
per type, against save-time and publish-time rules respectively.

With `database.migrate = true` one host activates a pending model under a
session advisory lock held on a connection detached from the pool (as the
kernel migration runner does), so a cancelled or failed activation closes the
session and releases the lock. It builds the indexes the new model adds (section 3), then in
one transaction marks the active `models` row `superseded`, inserts the new
one as `active`, syncs `locales`, bumps the affected generations and appends
`content.model_activated`. After that it drops the indexes the new model no
longer needs; the `published` fallback rows that a new locale or a changed
fallback affects are rebuilt in the background. Readiness stays false
until the indexes and the activation are done, not the rebuild. A lost
session loses its lock too, so the run reopens one (twice at most),
retakes the lock and plans again, keeping each index's attempt count.
With `database.migrate = false`, a pending model change fails readiness
with `content_model_pending`.

Decision (C4 review, 2026-10-24): readiness is not gated on the fallback
rebuild. The activation transaction records a pending row in
`merezha_content.rebuilds` (migration 3) when it inserts a locale or
changes a fallback, or when an older rebuild is still pending (that one
turns `superseded`: the new one covers every entry), and appends the
content job `content.published_rebuild`, registered through
`guard_jobs`. Any current host runs it: batches of 500 live entries with
a published variant, in id order and share-locked (saves and their
reference locks pass; publishes and deletes wait), each re-materializing
its entries, bumping the generations it touched and persisting the
cursor under the rebuild row's `FOR UPDATE`, so two runs never repeat a
batch; logged as `content.rebuild_batch` and `content.rebuild_completed`.
The job's payload names its version: a run of a superseded or finished
rebuild succeeds without work. A run does at least one batch, and one
stopped by its deadline or a cancel appends a continuation job keyed by
its job id (a retried run finds it there and goes on). A host dying
mid-batch leaves the committed cursor for the retried job, and a module
start enqueues a pending rebuild of its model again, keyed by the cursor
(a dead-lettered job, say; a duplicate beside a live one is harmless,
since runs serialize on the cursor row). Until it is
done, delivery may serve the previous materialization for the affected
locales, eventually consistent like the mixed state of a rolling deploy.
`merezha content diff` reports a pending rebuild as
`content_rebuild_pending`; readiness does not.

### Mixed model versions

A rolling deploy runs hosts with different models side by side. Each host
keeps its loaded model, and every transaction that writes content (saves,
publishes, deletes, asset changes, job handlers) starts with:

```sql
SELECT 1 FROM merezha_content.models
 WHERE version = $loaded_version AND state = 'active'
   FOR SHARE;
```

No row means a newer model is active. The transaction rolls back, the request
fails with `content_model_superseded` (503, transient, `Retry-After: 1`) so
the client retries on a current host, the host stops leasing content job
kinds (a job already leased is released with its attempt refunded, as in a
drain), and the host's readiness turns false with `content_model_outdated`
so the load balancer drains it. The shared lock makes activation's update of
that row wait for writes in flight, and a write that waited on activation
finds no row, so nothing validated against a superseded model commits after
activation. Every content write shares one row lock, which costs multixact
churn; at CMS write rates that is acceptable, and the operations guide notes
it. This holds for additive changes too: an old host would otherwise
publish entries without a newly required field. Reads keep the loaded model
until the host is drained, so an old host answers 404 for a new type and
applies the previous `access` until the deploy completes. A host starting
with an older model than the active one fails readiness with
`content_model_outdated`.

`merezha content diff --config merezha.toml` prints the classified diff
against the configured database without changing anything, and exits non-zero
on refused changes. Projects run it in CI.

## 3. Storage

### JSONB or columns

Entry field data is `jsonb`. System attributes are real columns: ids, type,
scope, locale, slug, revision numbers, timestamps, deletion state. A model
change then never runs DDL on content tables, which is where Directus and
early Strapi migrations hurt. The host validates every value before writing,
so the database does not need column types to keep data well-formed. Filters
and sorts only touch declared fields, each with a derived expression index,
and equality filters share one GIN index per table. The cost is that ad-hoc
SQL over content needs JSON operators, which is acceptable because no other
block reads these tables.

### Migration 1: `merezha_content`

```sql
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
```

The module's migrations go through the kernel runner (`merezha.migrations`,
checksums, one transaction each). Nothing here or in the derived indexes needs
more than PostgreSQL 14, the platform floor; CI runs 16 (R14). Operators can
set `lz4` compression on `revisions.data` where the server supports it.

Migration 2 (C3) is a declared non-transactional step (R15) for the
default management list, `-sys.updated_at`, and its reverse:

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS variants_by_update ON merezha_content.variants
  (scope, content_type, locale, draft_updated_at DESC, entry_id DESC) WHERE NOT deleted
```

`variants_list` stays from migration 1, unused by C3's lists. Sorts that
remain without a matching index: `sys.created_at` (an `entries` column),
and every sort of two or more keys, which reads the first key's index
order and sorts the rest. Delivery lists over `published` (C5) choose
their tie-breaker the same way, except after `sys.published_at`, whose
tie-breaker follows `published_list` (`published_at DESC, entry_id`).

Migration 3 (C4) adds `rebuilds (version integer PRIMARY KEY REFERENCES
models, after_entry uuid, state text CHECK (state IN ('pending', 'done')),
updated_at timestamptz NOT NULL)`, the resume cursor of the fallback
rebuild (section 2).

### Derived indexes

The model decides which indexes exist beyond the static ones. For every
`sortable` field the host derives two partial B-tree indexes on both
`variants` and `published`, one per sort direction: the ascending one
below, and one with `<value> DESC, entry_id DESC` (the `IS NULL` column
stays ascending, so nulls are last both ways). The list's `entry_id`
tie-breaker follows the last sort key's direction, so a one-key sort in
either direction is one index scan (C3):

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS content_ix_3f9a0c1d2e4b5a69
  ON merezha_content.published
  (scope, locale, ((data->'reading_minutes') IS NULL), (((data->>'reading_minutes'))::bigint), entry_id)
  WHERE content_type = 'article';
```

Casts: `::bigint` for `integer`, `::float8` for `float`, `::numeric` for
`decimal`, plain text for the rest. `datetime` stays text: a text to
`timestamptz` cast is not immutable and cannot be indexed, and the fixed-width
UTC format already sorts in time order. Each `unique` field, slugs included,
gets a partial unique index on `variants` (not deleted; `locale` in the key,
or `locale = <default>` in the predicate for shared fields) and on `published`
(`source_locale = locale`).

`CREATE INDEX CONCURRENTLY` cannot run in a transaction. This is an accepted
framework amendment (R15): the kernel runner gains a declared
non-transactional migration step, and content registers its index
reconciliation as one, run in two passes under the activation advisory lock.
Each name is `content_ix_` plus 16 hex characters of the definition's hash,
so the step computes the expected sets from the models and compares them
with `pg_indexes`. Before activation it builds every missing index of the
pending model and rebuilds those a crashed build left invalid
(`pg_index.indisvalid = false`), leaving the active model's indexes in place.
After activation it drops the `content_ix_` indexes the active model does not
need. Building first means no host writes a new `unique` field during its
build, since the active model rejects it as `unknown_field`, so the build
sees only absent values.

A failed build is dropped. A transient failure (lock or statement timeout,
lost connection) is retried twice with backoff. A unique violation (not
expected in v1, where only new fields gain `unique`) or a third failure stops
activation: the host logs `content.index_failed` with the index name and
field, readiness reports `index_build_failed` with the same, and startup
fails. Nothing retries in a loop, the pending model stays inactive, and hosts
on the active model keep serving and writing. The operator fixes the cause
and redeploys. Readiness stays false until every expected index exists and is
valid; with `database.migrate = false`, a missing one fails readiness with
`index_build_pending` instead of being built. Unique violations at write time
map back to their field through the index name.

### Scope predicates

Every statement on `entries`, `variants`, `published` and `revisions` carries
the scope and content type the route resolved, never an id alone: the type
from the path, the scope from the type's declaration and `Principal.tenant`
(section 8). `entries`, `variants` and `published` filter on their own
`scope` and `content_type` columns; `revisions` statements join `variants` on
`(entry_id, locale)` with both predicates. The composite foreign keys keep
the copied columns equal to the entry's. A row of another scope or type is
`entry_not_found`, as if absent, so a client-supplied id or a guessed revision
number never reaches it. `refs`, `assets` and `blobs` statements filter on
`scope`; only the `if_unreferenced` and `asset_in_use` checks count referrers
in every scope, without returning them. C3 ships these predicates with the
first write path; C9 adds organization types without changing a statement.

### Lock order

Content transactions take their locks in this order, so they wait on each
other rather than deadlock:

1. The loaded model's `models` row, `FOR SHARE` (the version check).
2. Entries: the entry a save or delete addresses (`FOR SHARE` for a save,
   `FOR UPDATE` for publish, unpublish and deletes), a batch's entries
   first, in id order. The rebuild takes the rebuild row `FOR UPDATE`,
   then its batch's entries `FOR SHARE` in id order.
3. The default variant `FOR SHARE` (a non-default save), then the
   addressed variant (`FOR UPDATE`, or its compare-and-swap update).
4. Reference targets, `FOR KEY SHARE` in id order, then
   `require_published` targets `FOR SHARE` in id order.

Steps 2 and 4 can still cross (a publish of A requiring B while a publish
of B requires A); PostgreSQL then aborts one transaction, which answers
`database_conflict` (409, conflict, retryable).

### Optimistic concurrency

Every write to a variant is a compare-and-swap on `draft_revision`, after the
model version check (section 2):

```sql
UPDATE merezha_content.variants
   SET draft_revision = draft_revision + 1, draft_data = $6, draft_slug = $7,
       draft_updated_at = $8, draft_updated_by = $9
 WHERE entry_id = $1 AND locale = $2 AND scope = $3 AND content_type = $4
   AND draft_revision = $5 AND NOT deleted
RETURNING draft_revision;
```

Zero rows is `revision_conflict` (412) when the variant exists under the same
predicates, `entry_not_found` otherwise. HTTP carries the revision as a strong
ETag, `"r12"`. `PUT`, `PATCH`, publish, unpublish, restore and
variant delete require `If-Match` (missing: `precondition_required`, 428);
creating a variant uses `If-None-Match: *`. The same transaction writes the
revision, the variant's `draft` refs, retention pruning, shared-field copies
and the outbox record of the event (`Outbox::append(tx, ...)`, R1). No network
or blob call happens inside it.

## 4. Workflow

A variant moves between `draft`, `published` and `changed` as described in
the language table. Deleting an entry moves it to the trash from any state,
undelete returns it as drafts, and purge or retention removes it.

### Saving

`POST` creates the entry and its first variant at revision 1. A client may
supply `id` for imports; an existing id is `entry_exists` (409). `PUT`
replaces the variant's fields. `PATCH` takes an RFC 7396 merge patch over
`fields` (`application/merge-patch+json`), which replaces lists whole. Each
save is one revision, with no autosave coalescing in v1, so autosaving
frontends should debounce.

### Publishing

`POST .../publish` with `If-Match: "r12"` publishes revision 12 in one
transaction. The host locks the variant (`FOR UPDATE`, under the scope
predicates), checks the revision and runs publish-time validation against the
active model. It then sets `published_revision`, `published_at` and
`first_published_at`, rewrites the variant's `published` refs, re-materializes
the entry's `published` rows for every locale whose chain passes through this
variant (for the default variant, also the shared fields of every row), bumps
the scope's generation and appends `content.entry_published`. A body of
`{"revision": 9}` rolls the published version back without touching the
draft. Unpublish clears the pointer and refs, re-materializes (a locale that
fell back to this variant falls back further or disappears) and appends
`content.entry_unpublished`.

References to unpublished entries do not block publishing; delivery shows them
as missing unless the field sets `require_published`. Batch publish takes up
to `limits.max_batch` items and publishes all or none in one transaction,
which covers coordinated launches without a release object (owner answer
CT7).

### Revisions and restore

Revision reads return the stored data with its `sys.model_version`. v1 model
changes never remove or retype a field, so stored data always fits the active
model's shape, though a tightened rule may now reject it. Restore creates a new draft revision (`kind = restore`,
`restored_from = n`) and never publishes. It runs save-time validation
against the current model. Data that no longer passes (a value now over
`max_length`) still restores and the response lists the violations in
`warnings`; a `unique` conflict fails the restore.

Retention keeps the newest `revisions.retain` revisions per variant (100 by
default) plus the published revision.

### Localization

- Each variant has its own draft, published state, revisions and slug. The
  first `PUT` with `If-None-Match: *` for a locale creates it;
  the default variant need not exist first.
- Shared fields (`localized = false`) belong to the default variant. Writing
  one through another locale fails with `field_not_localized`. The host copies
  shared values into the other variants' `draft_data` on each default save and
  into the other `published` rows on each default publish. Until the default
  variant is published, other locales deliver without shared fields. Their
  `required` checks run when the default variant publishes.
- Fallback is per variant, not per field. Every chain implicitly ends at the
  default locale, and a non-localized type is served in every locale from its
  default variant. `fr-BE` serves the `fr-BE` variant if published, else `fr`,
  else `en`, and says which in `sys.source_locale`. `fallback=false` turns it
  off. Because `published` stores the resolved rows, a delivery query reads
  one indexed table. A fallback change in the model is additive and rebuilds
  those rows in batches at activation.
- `DELETE .../{id}/{locale}` removes a non-default variant with its history.
  The default variant goes only with the entry (`default_variant_required`).
- A slug lookup prefers the entry whose own variant has the slug, then fallback
  rows in chain order. Uniqueness applies to own slugs only.

### Soft delete and purge

Deleting an entry sets `deleted_at`, marks its variants `deleted`, removes its
`published` rows and refs and frees its unique
values (unique indexes skip deleted rows). It appends
`content.entry_unpublished` per live locale and one `content.entry_deleted`.
With `if_unreferenced=true` it fails with `entry_referenced` while anything
still points at it. Undelete restores drafts only; a unique value taken
meanwhile fails it with `unique` details.

The recurring job `content.trash_purge`, declared through
`JobsHandle::schedule` with `trash.purge_cron` (AP §B.7), hard-deletes entries
trashed longer than `trash.retention`, 100 per batch, cascading to variants,
revisions and refs, and appends `content.entry_purged`.
`purge=true` with `content.entries.purge` does it at once, for erasure
requests. Assets follow the same pattern (section 6).

## 5. APIs

### Conventions

- Management lives under `/v1/content/manage/`, delivery under
  `/v1/content/`, organization delivery under `/v1/content/o/{organization}/`.
- Bodies are bounded by `limits.max_request_bytes`, draft data by
  `limits.max_entry_bytes`. Errors use the platform shape
  `{error: {code, classification, details?}}` (R8); content registers its
  codes and statuses in the kernel's error registry at startup.
- The edge `Authenticator` supplies the principal. The `Authorizer` gets
  `(principal, permission, scope)` with K's `ResourceScope` (R5): `Application`
  for application-scoped types and assets, `Tenant(org)` for organization
  ones, and `Resource { tenant, kind: "content_type", id: "<type>" }` for
  per-type checks.
- Timestamps are RFC 3339 UTC with milliseconds. Every query sets
  `statement_timeout` from the request deadline, capped by
  `limits.query_timeout`.

### Permissions

The content block contributes these permissions to the RBAC catalog:

| Permission | Grants |
| --- | --- |
| `content.model.read` | Read the full content model through the management API. |
| `content.entries.read` | List and read drafts, revisions, referrers and trash. |
| `content.entries.write` | Create entries, save and patch drafts, restore revisions, delete a non-default variant. |
| `content.entries.publish` | Publish, unpublish and batch publish. |
| `content.entries.delete` | Move entries to the trash and undelete them. |
| `content.entries.purge` | Hard-delete entries immediately. |
| `content.assets.read` | List assets and read any asset file through the management API. |
| `content.assets.write` | Upload assets and edit their metadata. |
| `content.assets.delete` | Delete, undelete and purge assets, including `force` deletes. |
| `content.delivery.read` | Read published entries of types declaring `access = { permission = "content.delivery.read" }`. A ready-made permission so projects need not declare their own. |
| `content.preview` | Read drafts through the delivery API with `stage=draft`. |

Entry permissions and permission-based `access` are checked in the type's
resource scope, `Resource { tenant, kind: "content_type", id: "article" }`.
An `Application` grant satisfies it for application-scoped types and a
`Tenant` grant for organization ones, so application-level roles (R5) cover
whole-app editors and a resource grant lets a role publish `article` only.
Model reads, assets and preview are checked at `Application` or `Tenant(org)`
scope. Batch publish checks every item. Without identity, content starts only
on a loopback listener with the explicit `AllowAll` policy; otherwise startup
fails with `identity_required`.

### Entry JSON

Management, one variant (`GET .../entries/article/{id}/fr`, `ETag: "r12"`):

```json
{
  "sys": {
    "id": "0192f7a0-5d1c-7c3e-9b1a-6f0e2c4d8a11", "type": "article", "locale": "fr",
    "status": "changed", "draft_revision": 12, "published_revision": 9, "model_version": 3,
    "created_at": "2026-10-01T10:00:00.000Z", "created_by": "usr_0192f5c4...",
    "updated_at": "2026-10-22T16:41:07.512Z", "updated_by": "usr_0192f5c4...",
    "published_at": "2026-10-20T09:12:44.120Z", "first_published_at": "2026-10-02T08:00:00.000Z"
  },
  "fields": {
    "title": "Notes de version 1.4", "slug": "notes-de-version-1-4", "category": "release",
    "author": { "id": "0192f79f-...", "type": "author" }, "tags": ["release", "api"],
    "sections": [{ "_type": "quote", "_key": "k7Qa", "text": "Ship small." }],
    "reading_minutes": 4
  }
}
```

Delivery (`GET /v1/content/article/{id}?locale=fr-BE&populate=author`):

```json
{
  "sys": {
    "id": "0192f7a0-5d1c-7c3e-9b1a-6f0e2c4d8a11", "type": "article", "locale": "fr-BE",
    "source_locale": "fr", "revision": 9,
    "published_at": "2026-10-20T09:12:44.120Z", "first_published_at": "2026-10-02T08:00:00.000Z"
  },
  "fields": {
    "title": "Notes de version 1.4",
    "author": {
      "id": "0192f79f-...", "type": "author",
      "entry": { "sys": { "id": "0192f79f-...", "type": "author", "locale": "fr-BE", "source_locale": "en", "revision": 3 },
                 "fields": { "name": "Ada" } }
    },
    "seo": { "image": { "id": "0192f7a1-...", "alt": "Photo d'équipe",
                        "asset": { "filename": "team.jpg", "content_type": "image/jpeg", "width": 1600, "height": 900, "url": "..." } } }
  }
}
```

`sys` keeps system attributes apart from field names. Delivery always expands
asset metadata (one batched query). A reference is `{"id", "type"}`, gains
`entry` when populated, and becomes `{"id", "type", "missing": true}` when the
target is not visible. `url` is a path, absolute when
`delivery.public_base_url` is set.

### Management routes

All paths are under `/v1/content/manage`. `{type}` is a content type,
`{locale}` a configured locale.

| Method and path | Permission | Request | Success |
| --- | --- | --- | --- |
| `GET /model` | `content.model.read` | none | 200 `{version, fingerprint, locales, components, types}` in model order. |
| `GET /entries/{type}` | `content.entries.read` | query: `locale`, `status` (`draft`, `published`, `changed`, `any`), `filter[...]`, `sort`, `limit`, `cursor`, `fields` | 200 `{items: [variant], page: {next_cursor, limit}}` |
| `POST /entries/{type}` | `content.entries.write` | `{id?, locale?, fields}` | 201 variant, `ETag: "r1"`, `Location` |
| `GET /entries/{type}/{id}` | `content.entries.read` | none | 200 `{sys: {id, type, created_at, deleted_at}, variants: [{locale, status, draft_revision, published_revision, updated_at}]}` |
| `GET /entries/{type}/{id}/{locale}` | `content.entries.read` | `fields`, `populate` (drafts) | 200 variant, `ETag` |
| `PUT /entries/{type}/{id}/{locale}` | `content.entries.write` | `{fields}`, `If-Match` or `If-None-Match: *` | 200 or 201 variant, new `ETag` |
| `PATCH /entries/{type}/{id}/{locale}` | `content.entries.write` | merge patch over `{fields}`, `If-Match` | 200 variant |
| `DELETE /entries/{type}/{id}/{locale}` | `content.entries.write` | `If-Match` | 204 |
| `POST /entries/{type}/{id}/{locale}/publish` | `content.entries.publish` | `{revision?}`, `If-Match` | 200 variant |
| `POST /entries/{type}/{id}/{locale}/unpublish` | `content.entries.publish` | `If-Match` | 200 variant |
| `GET /entries/{type}/{id}/{locale}/revisions` | `content.entries.read` | `limit`, `cursor` | 200 `{items: [{revision, kind, restored_from, model_version, created_at, created_by, title, published}], page}` |
| `GET /entries/{type}/{id}/{locale}/revisions/{n}` | `content.entries.read` | none | 200 variant shape with that revision's fields |
| `POST /entries/{type}/{id}/{locale}/revisions/{n}/restore` | `content.entries.write` | `If-Match` | 200 variant plus `warnings` |
| `GET /entries/{type}/{id}/referrers` | `content.entries.read` | `stage`, `limit`, `cursor` | 200 `{items: [{id, type, locale, stage, path, title}], page}` |
| `DELETE /entries/{type}/{id}` | `content.entries.delete`, plus `content.entries.purge` with `purge=true` | `if_unreferenced`, `purge` | 204 |
| `POST /entries/{type}/{id}/undelete` | `content.entries.delete` | none | 200 entry summary |
| `GET /trash` | `content.entries.read` | `type`, `limit`, `cursor` | 200 `{items: [{id, type, deleted_at, deleted_by, title}], page}` |
| `GET /singletons/{type}/{locale}` | `content.entries.read` | as variant GET | 200 variant, or 404 `entry_not_found` before first save |
| `PUT`, `PATCH /singletons/{type}/{locale}` | `content.entries.write` | as above; the first `PUT` with `If-None-Match: *` creates the entry | 200 or 201 |
| `POST /singletons/{type}/{locale}/publish`, `/unpublish`; `GET /singletons/{type}/{locale}/revisions[/{n}]`; `POST .../revisions/{n}/restore` | as for entries | as for entries | as for entries |
| `POST /batch/publish` | `content.entries.publish` per item | `{items: [{type, id, locale, revision}]}` | 200 `{items: [variant]}` |
| `POST /batch/unpublish` | `content.entries.publish` per item | `{items: [{type, id, locale}]}` | 200 `{items: [variant]}` |
| `POST /assets` | `content.assets.write` | raw body, `Content-Type`, query `filename`, optional `title` | 201 asset |
| `GET /assets` | `content.assets.read` | `content_type` (exact or `image/*`), `filename_prefix`, `limit`, `cursor` | 200 `{items: [asset], page}` |
| `GET /assets/{id}` | `content.assets.read` | none | 200 asset with `usage: {draft, published, elsewhere}` counts (C9: `elsewhere` counts other scopes' refs) |
| `PATCH /assets/{id}` | `content.assets.write` | merge patch over `{title, focal_point}` | 200 asset |
| `GET /assets/{id}/file` | `content.assets.read` | `Range` | 200 or 206 bytes, any asset state except purged |
| `DELETE /assets/{id}` | `content.assets.delete` | `force`, `purge` | 204, or 409 `asset_in_use` |
| `POST /assets/{id}/undelete` | `content.assets.delete` | none | 200 asset |

Batch publish items carry `revision` in place of `If-Match`.

C3 ships the draft and revision routes of this table (list, create, entry
summary, variant read, `PUT`, `PATCH`, revisions, restore, referrers and
their singleton forms); the rest arrive with C4, C5 and C7. As built:

- `If-Match` names one strong `"r<n>"`. Any other value (`*`, a weak or
  listed tag) never matches the draft and is `revision_conflict`.
  `If-None-Match: *` sent with `If-Match` fails as RFC 9110 orders them:
  `variant_exists` when the variant exists.
- A `PUT` that leaves out a slug the variant has keeps it (the host never
  changes an existing slug); `PATCH` removes one only when it sends `null`.
- A non-localized type has only the default locale: any other locale on
  its routes or in `POST` is `locale_unknown`.
- Field rules and reference problems are reported together: the
  references of the fields that pass on their own are resolved even when
  others fail. A restore keeps them all as `warnings` (only a `unique`
  conflict fails it) and records the refs of the fields that resolve.
- A reference whose target lives in a scope the entry cannot reference
  was `reference_scope` in C3; C9 answers `reference_not_found` instead
  and dropped the lookup that told them apart (below).
- Repeated `filter[f][in]` keys are one clause (any value); repeated
  `filter[f][has]` keys must all be present. Other operators take one
  value per field. `sys.id` takes `eq`, `in` and `ne`; `sys.created_at`
  and `sys.updated_at` take `eq` and the range operators.
- Revisions, referrers and lists page with the cursor shape of section 5;
  each cursor is bound to its listing (the variant, or the entry and
  stage). Sort values travel in the exact text the list selects
  (timestamps `YYYY-MM-DDTHH:MM:SS.ffffffZ`, PostgreSQL's float8 and
  numeric output); any other form, or a value PostgreSQL refuses (SQLSTATE
  class 22), is `invalid_cursor`.
- A type the model lacks and an organization type the principal has no
  tenant for are both checked at `Application` scope first, so a caller
  without that grant gets the same refusal for both; past it they are
  `content_type_not_found` and `tenant_required`.
- Content job kinds register through `ContentState::guard_jobs`: an
  outdated host stops leasing them, and a job it still holds, or one that
  meets `content_model_superseded` mid-run, is given back with
  `JobFailure::release()` (available at once, attempt refunded).
- Two checks looked past the caller's scope in C3. C9 decided both
  (section 8, as built): a reference outside the reachable scopes is
  `reference_not_found` for every caller, so saves reveal nothing about
  other tenants' ids; a client id taken in any scope stays
  `entry_exists`, because ids are a global key and any other answer
  (a fresh id included) would tell the same thing.
- Referrers list the refs recorded in the target's scope; `title` comes
  from the referring variant's draft, or its `published` row for
  `stage=published`.
- Every route answers `private, no-store`; single-variant responses carry
  the strong revision ETag and `fields` in model order. `populate` on a
  draft read comes with C5's population; until then it is an unknown
  parameter (`invalid_query`).
- A save of a non-default variant share-locks the default variant while it
  reads the shared values, so a concurrent default save cannot be undone
  by a stale copy.

C4 ships publish, unpublish, the batches, the variant and entry deletes,
undelete and the trash. As built:

- Every publish, unpublish and delete locks the entry `FOR UPDATE` before
  its variant, and a batch locks its entries in id order first, so
  concurrent batches never deadlock. Each re-materializes all of the
  entry's `published` rows (one per configured locale whose chain reaches
  a published variant), a superset of the rows the change affects.
- `require_published` checks that the target has a `published` row in
  the referrer's locale (the chain resolved), after every item of the
  transaction is materialized, so one batch can publish a target with its
  referrers.
- Batch items are checked in order: a failure names its item in
  `details` (`/items/<n>` plus the item's own path), and a repeated
  `(type, id, locale)` is `duplicate_item`. `revision` is required on
  publish items and refused on unpublish items.
- Unpublishing a variant that is not published answers 200 without an
  event or a generation bump. Undeleting a live entry does too.
- Deleting clears the variants' published pointers (undelete restores
  drafts only) and removes the entry's refs; undelete records the drafts'
  refs again. The variant delete of a published variant appends
  `content.entry_unpublished` with reason `delete`.
- `purge=true` reaches trashed entries too. Purging a live entry appends
  what a delete does, then `content.entry_purged`.
- `GET /trash` without `type` lists the types the caller reads, each
  checked in its resource scope; organization types need the principal's
  tenant.
- The trash purge is the cron schedule `content.trash_purge`, in UTC.
- The tightening count reads only own `published` rows
  (`source_locale = locale`); fallback rows repeat their source.
- Reference resolution locks the target entries `FOR KEY SHARE` until the
  saving transaction commits, and `require_published` share-locks its
  target before reading `published`. So a delete, purge, publish or
  unpublish of a target waits for a save that references it and then
  sees its refs, a save waits for a delete in flight and then gets
  `reference_not_found`, and a referrer's publish waits for its target's
  unpublish and then gets `reference_unpublished`. Two publishes requiring
  each other may deadlock; PostgreSQL aborts one (`database_conflict`,
  409, retryable).
- A purge deletes the refs pointing at the entry too, in every scope, so
  an id created again inherits no stale referrers.
- A batch's transaction gets one `limits.query_timeout` per 50 items
  started; the request bound would be too short for 500 items.

Until identity lands, C3 relies on the host's policy: a private listener
allows every route, a public one denies every route (`unauthenticated`
without a principal). The `identity_required` startup refusal waits for
identity's `Authenticator`, so a public listener still starts.

### Delivery routes

| Method and path | Access | Query | Success |
| --- | --- | --- | --- |
| `GET /v1/content/model` | anyone; each type filtered by its `access` | none | 200 the model restricted to types the caller can read |
| `GET /v1/content/{type}` (collection) | the type's `access` | `locale`, `fallback`, `filter[...]`, `sort`, `limit`, `cursor`, `fields`, `populate`, `stage` | 200 `{items: [entry], page: {next_cursor, limit}}` |
| `GET /v1/content/{type}` (singleton) | same | `locale`, `fallback`, `fields`, `populate`, `stage` | 200 entry, or 404 |
| `GET /v1/content/{type}/{id}` | same | `locale`, `fallback`, `fields`, `populate`, `stage` | 200 entry |
| `GET /v1/content/{type}/by-slug/{slug}` | same | same | 200 entry |
| `GET /v1/content/assets/{id}/{filename}` | asset referenced by a published entry the caller can read | `Range` | 200 or 206 bytes, or 302 in redirect mode |
| Each of the above under `/v1/content/o/{organization}/...` | for organization-scoped types | same | same |

`stage=draft` needs `content.preview` and reads `variants` with the same
fallback, filters and population, resolving referenced drafts too. Draft
fallback is resolved per query (`DISTINCT ON` over the chain), so preview
lists are slower than delivery lists. Preview responses are
`private, no-store` without ETag. Delivery never distinguishes unpublished
from absent: both are `entry_not_found`.

C5 ships these routes, preview, population, the ETags and the model
endpoints (`GET /v1/content/model` and the management `GET /model`). As
built:

- Access is checked before anything is read, an unknown id answering
  like a known one. A type the model lacks is `content_type_not_found`,
  while a restricted one answers 401 or 403, as the acceptance criteria
  require; delivery does not hide which restricted type names exist (the
  restricted model endpoint does).
- Preview needs the type's `access` as well as `content.preview`; a
  preview grant never lifts a type's own rule. Population in preview
  needs both for every target type.
- `populate` segments name top-level fields of the types the previous
  segment reached. A segment expands every entry reference inside the
  field's value: a `reference`, references inside components, component
  lists and `blocks`, and a rich text field's `entry_link` annotations and
  embedded components. With `fields`, a path's first segment must be
  selected (`not_selected`). Populated entries are counted per reference
  (a target reached twice counts twice) against
  `limits.max_populated_entries`; depth is checked when the query is
  parsed (`invalid_query`, `/populate` `too_deep`).
- Every entry reference of a delivery response is checked: one whose
  target is unpublished, deleted, of a type the caller cannot read, in
  another scope, or of another type than the reference names becomes
  `missing`. Management draft reads mark only the references on populated
  paths, and populate the drafts of the types the caller holds
  `content.entries.read` on.
- The default delivery sort `-sys.published_at` takes `entry_id` against
  its direction, the order of the static `published_list` index, so
  either direction of `sys.published_at` is one index scan without a new
  migration; field sorts keep C3's rule.
- In preview, `sys.published_at` filters and sorts on the variant's own
  `published_at`, null (sorted last) for a draft never published.
- A by-slug lookup orders candidates by their source locale's place in
  the chain, which starts with the locale itself, so an own slug wins.
- The ETag hashes the route, the type, the scope, the sorted query pairs
  and an access class: whether a principal asked, and the set of types
  the caller reads on that path (anonymous callers too, since a policy
  may grant them a permission type). On an organization's path a type
  that can reference entries also carries the application generation,
  populated or not, because the `missing` checks read application rows.
  The generations are read first in the read's transaction, so the
  content is never older than its tag. No other scope's generation
  enters a tag.
- `delivery.cache_control_public` applies only without a principal, to a
  `public` type whose reachable reference targets the caller reads are
  all `public`; it is decided before reading, so a 304 and a 200 agree.
- `limits.max_response_bytes` (16 MiB by default, 64 KiB to 256 MiB)
  bounds a delivery body and a populated management read; past it the
  request fails `response_too_large` (400). Without it the bound would be
  `max_page_size` × `max_entry_bytes` plus `max_populated_entries` ×
  `max_entry_bytes`, gigabytes at the ceilings.
- The delivery model endpoint keeps the types the caller reads (on an
  organization's path its organization types and the application types),
  the components they embed and only reference targets among them; the
  management one needs `content.model.read` in the principal's tenant,
  or at `Application` without one.
- Asset metadata expansion, asset serving and `delivery.public_base_url`
  arrive with C7; until then asset fields are delivered as stored.
- Delivery's statements are registered with the TE-T4 registry
  (`merezha_content::STATEMENTS`); its lists run through the registry's
  `Database::query_text`, which C5 adds for text built per request with
  parameters.

### Filters

Syntax: `filter[<field>][<op>]=<value>`, repeated keys for `in` and `has`.
System fields use the `sys.` prefix: `sys.id`, `sys.created_at`,
`sys.updated_at` (management), `sys.published_at` (delivery). Clauses are
combined with AND; there is no OR in v1. At most `limits.max_filters` clauses
and 50 values per `in`.

| Operator | Field types | Requires | SQL shape |
| --- | --- | --- | --- |
| `eq`, `in` | scalars, single `reference` (by id) | `filterable` | `data @> '{"f": v}'` per value, OR within `in`, GIN index |
| `has` | `list` of scalars, `reference` with `many` | `filterable` | `data @> '{"f": [v]}'` (references match `{"id": v}`) |
| `ne` | scalars | `filterable` | `NOT (data @> ...)` after indexed predicates |
| `exists` (`true`, `false`) | all top-level fields | `filterable` | `data ? 'f'` |
| `lt`, `lte`, `gt`, `gte` | `integer`, `float`, `decimal`, `date`, `datetime`, `string` | `sortable` | typed expression, B-tree derived index |

Values parse by field type with the write rules (decimals normalize to the
field's scale). Unknown fields, missing flags and bad values are
`invalid_filter` with `details`.

### Sorting and pagination

`sort=-reading_minutes,title` takes up to `limits.max_sort_keys` sortable
fields or `sys.` timestamps; `entry_id` is always the final key. Nulls sort
last both ways, which is why index expressions start with `IS NULL`. Defaults
are `-sys.updated_at` (management) and `-sys.published_at` (delivery).

Pagination is keyset only, `limit` 25 by default and 100 at most.
`next_cursor` (null on the last page) is base64url of
`{"v":1,"q":"<16 hex of query fingerprint>","k":[last sort values],"id":"..."}`.
The fingerprint covers type, scope, stage, locale, fallback, filters and sort,
so a cursor reused on another query is `invalid_cursor`. There are no totals:
counting a filtered JSONB set on every page is the cost this design avoids.

### Field selection and population

`fields=title,slug,author` limits top-level fields; `sys` is always present.
`populate=author,body,body.author` expands entry references along dotted paths
(asset fields never need it), at most `limits.max_populate_depth` segments
(default 2, ceiling 3), one batched query per level in the same stage and
locale chain. Rich text populates its `entry_link` annotations. Past
`limits.max_populated_entries` the request fails with `populate_limit` rather
than truncating. A target the caller may not read, under its own type's
`access`, stays a `missing` reference. Populated entries carry all their
fields.

### Caching

Delivery responses for the published stage carry a weak ETag built without
running the query:

```text
W/"<8 hex of model fingerprint>-<scope generation>-<16 hex of SHA-256(canonical query, access class)>"
```

Every publish, unpublish, delete, rebuild, `access` change or asset metadata
change bumps the scope's row in `generations` inside the transaction that
makes the change. An organization response that populates application entries
includes both generations. On a matching `If-None-Match` the host reads those
rows and answers 304 without touching entries. The price is one contended row
update per publish in a scope, which is fine at CMS write rates.

`Cache-Control` is `delivery.cache_control_public` for `access = "public"`
types read anonymously (default `public, max-age=60, stale-while-revalidate=300`)
and `delivery.cache_control_private` otherwise (default `private, no-cache`).
Responses vary on `Authorization` and `Cookie`. Management responses are always
`private, no-store`, with strong revision ETags on single variants.

### Error codes

| Code | HTTP | Classification | When |
| --- | --- | --- | --- |
| `unauthenticated` | 401 | permanent | The type's `access` or the route needs a principal and there is none. |
| `unauthorized` | 403 | permanent | Principal lacks the permission, or an `authenticated` organization type is read with another tenant. |
| `tenant_required` | 403 | permanent | Organization-scoped type and the principal has no active organization. |
| `content_type_not_found`, `entry_not_found`, `revision_not_found`, `asset_not_found` | 404 | permanent | Absent, pruned, of another scope or type, or not visible to this caller in this stage. |
| `locale_unknown` | 400 | permanent | Locale not configured or retired. |
| `invalid_query`, `invalid_filter`, `invalid_sort`, `invalid_cursor`, `populate_limit` | 400 | permanent | Query parameter problems, with `details`. |
| `invalid_json` | 400 | permanent | Body is not JSON, has duplicate keys at the top level, or has unknown top-level keys. |
| `validation_failed` | 422 | permanent | Field rules, with `details`. |
| `variant_exists`, `revision_conflict` | 412 | conflict | `If-None-Match: *` on an existing variant; `If-Match` not the draft revision. |
| `precondition_required` | 428 | permanent | Missing `If-Match`. |
| `entry_exists`, `entry_referenced`, `asset_in_use`, `default_variant_required`, `singleton_exists` | 409 | conflict | Client id taken; referrers exist with `if_unreferenced`; asset referenced without `force`; default variant deleted alone; second singleton entry. |
| `body_limit`, `asset_too_large` | 413 | permanent | Body or draft data too large; upload over `media.max_upload_bytes`. |
| `media_type_rejected` | 415 | permanent | Type not allowed, or the bytes do not match the declared type. |
| `range_not_satisfiable` | 416 | permanent | A ranged blob read starting at or past the end. |
| `overloaded` | 503 | transient | `media.max_concurrent_uploads` reached; `Retry-After: 1`. |
| `blob_unavailable` | 503 | transient | Blob store failure after its bounded retries. |
| `blob_backend_rejected` | 502 | permanent | The blob store answered 4xx (credentials, bucket, policy). |
| `blob_body_unreadable` | 503 | transient | The spooled file or body stream failed while it was stored. |
| `blob_length_mismatch`, `invalid_blob_attributes`, `invalid_blob_range`, `invalid_signed_url_ttl` | 422 | permanent | Blob port input refused before any I/O (internal errors of the caller). |
| `blob_too_large` | 413 | permanent | A blob body over 5 GiB. |
| `content_model_superseded` | 503 | transient | Write on a host whose model a newer one replaced; `Retry-After: 1` (section 2). |
| `timeout` | 504 | transient | Request deadline or statement timeout. |
| `internal` | 500 | transient | Anything else. Logged with the request id. |

Codes are unprefixed (R8); the cross-cutting ones keep their canonical
statuses. Startup and readiness codes: `content_model_invalid`,
`content_model_version_unchanged`, `content_model_outdated`,
`content_model_breaking`, `content_model_pending` (the domain term "content
model", not a module prefix), `identity_required`, `media_config_invalid`,
`index_build_pending`, `index_build_failed`, and the blob driver's
`blob_config_invalid`, `blob_root_unusable`, `blob_root_insecure`.
`merezha_blob::ERRORS` lists the blob codes for `Module::errors`.

## 6. Media

### BlobStore port

The framework places `BlobStore` in `merezha-platform`. Content owns its
shape, and the kernel adopts it in K2 (R13): streaming put and get, `head`,
idempotent delete, and signed URLs where the backend supports them.

```rust
use std::{io, ops::Range, path::PathBuf, time::Duration};
use bytes::Bytes;
use futures_util::{future::BoxFuture, stream::BoxStream};

pub struct BlobKey(String); // 1..1024 bytes, segments of [A-Za-z0-9._-], no '.' or '..'
pub enum BlobBody {
    Bytes(Bytes),
    File { path: PathBuf, len: u64 },                      // a spooled upload
    Stream { body: BoxStream<'static, io::Result<Bytes>>, len: Option<u64> },
}
pub struct BlobAttributes {
    pub content_type: String,
    pub content_disposition: String,
    pub cache_control: Option<String>,
}
pub struct BlobStat { pub size: u64, pub etag: Option<String> }
pub struct BlobRead {
    pub stat: BlobStat,
    pub range: Option<Range<u64>>,
    pub body: BoxStream<'static, io::Result<Bytes>>,
}
pub enum BlobError {
    NotFound,                  // permanent
    Invalid(&'static str),     // permanent, never reached the backend
    Rejected(&'static str),    // permanent, 4xx from the backend
    Unavailable(&'static str), // transient, after bounded retries
}

pub trait BlobStore: Send + Sync + 'static {
    fn put<'a>(&'a self, key: &'a BlobKey, body: BlobBody, attrs: &'a BlobAttributes)
        -> BoxFuture<'a, Result<BlobStat, BlobError>>;
    fn get<'a>(&'a self, key: &'a BlobKey, range: Option<Range<u64>>)
        -> BoxFuture<'a, Result<BlobRead, BlobError>>;
    fn head<'a>(&'a self, key: &'a BlobKey) -> BoxFuture<'a, Result<BlobStat, BlobError>>;
    fn delete<'a>(&'a self, key: &'a BlobKey) -> BoxFuture<'a, Result<(), BlobError>>; // idempotent
    fn signed_get_url<'a>(&'a self, key: &'a BlobKey, ttl: Duration)
        -> BoxFuture<'a, Result<Option<String>, BlobError>>; // None when unsupported
}
```

Boxed futures keep the trait object-safe without the `async-trait` macro.
Every body is written in parts as it arrives, so no put buffers a whole file.
Like Structure's `@structure-ai/storage`, the port has validated keys,
idempotent delete, typed transient and permanent failures, streaming bodies
and a head-based readiness probe. It drops `list` and per-object metadata,
because PostgreSQL tracks every blob. The serving policy (attachment by
default, an inline allowlist, `nosniff` always) sits in the content block's
serving code, one layer above the port.

### Driver crate: `object_store`

| Option | Version checked | Verdict |
| --- | --- | --- |
| `object_store` | 0.14.2, MSRV 1.85, MIT/Apache-2.0 | Chosen. |
| `aws-sdk-s3` | 1.155.0, MSRV 1.94.1, Apache-2.0 | Rejected. Full AWS SDK runtime (smithy, credential chain, its own HTTP stack), a large dependency tree and compile time, for four operations. |
| `rust-s3` | 0.38.0, no declared MSRV, MIT | Rejected. Single-maintainer crate with a history of breaking API changes and no declared MSRV. |

`object_store`, maintained by the Apache Arrow project, gives both drivers
behind one API: `LocalFileSystem`, and `AmazonS3` for AWS, MinIO, R2, Garage
and other S3-compatible stores. It streams multipart uploads
(`WriteMultipart`), serves byte ranges, sets type, disposition and cache
headers through `Attributes`, and signs GET URLs (`Signer`). The `aws` feature
uses reqwest 0.13 with rustls and pulls `aws-lc-rs`, so the build needs a C
compiler, as bundled SQLite already does. Its retries are bounded
(`max_retries = 3`, `retry_timeout = 10s`). That is a deliberate exception to
the platform's no-retry rule: operations on content-addressed keys are
idempotent.

The host creates `media.local.root` with mode 0700 if missing and refuses a
root that grants group or others any permission (mode `& 0o077`), at startup
and in readiness. That 0700 root is the access boundary: blob files and the
subdirectories below it follow the process umask. Keys never contain user input. The local
driver has no signed URLs, so `serve = "redirect"` with `driver = "local"`
fails startup with `media_config_invalid`.

The drivers live in `merezha-blob` (`crates/blob`, slice C6), so the kernel
stays free of `object_store`. `ObjectBlobStore::open` takes a
`DriverConfig` (`LocalConfig` or `S3Config`, the `media.local.*` and
`media.s3.*` keys) and fails `blob_config_invalid`, `blob_root_unusable` or
`blob_root_insecure`; it never contacts the bucket. As built:

- Every input is checked before any I/O: the key (with `key_prefix`, at
  most 1024 bytes), the attributes (1 to 1024 header-safe bytes), the range
  (`start < end`) and the signed URL lifetime (1 s to 7 days).
- A body of up to one 5 MiB part is one request. Larger bodies go up in
  parts of exactly 5 MiB, one part in flight, so a put holds about one part
  in memory. A declared length that does not match the bytes read fails
  `blob_length_mismatch`, a body over 5 GiB `blob_too_large`, a failing file
  or stream `blob_body_unreadable`; each aborts the multipart upload and
  stores nothing. A put cancelled mid-way leaves an incomplete upload on
  S3: give the bucket a lifecycle rule that aborts incomplete multipart
  uploads after a day.
- The local driver writes through a staging file and a rename, with fsync,
  so a put is atomic and durable. It stores bytes only: attributes are
  validated and dropped, because proxy serving takes them from the asset
  row.
- S3 requests are bounded by `media.s3.timeout` and retried 3 times within
  10 s (5xx, 408, 429, timeouts and connection failures). An operation that
  never gets an answer fails `blob_unavailable` after at most the timeout
  plus 10 s plus one backoff; a refused connection fails within the
  timeout. Other 4xx and every 3xx (a wrong region or endpoint) fail
  `blob_backend_rejected` at once, and so does a write to a missing bucket.
  Deletes
  are single `DELETE` requests, not bulk `POST /?delete`.
- A ranged read past the end fails `range_not_satisfiable` on both drivers;
  an end past the size is clamped and `BlobRead::range` reports what was
  served.
- `ObjectBlobStore::ready(deadline)` is the readiness probe below. On S3 it
  needs `s3:ListBucket`: without it a missing key answers 403, which reads as
  not ready. Locally it also fails when the root is gone or its mode no
  longer refuses group and others (object_store recreates a deleted root
  with the umask's mode on the next put).
- `ObjectBlobStore::with_max_bytes` lowers the 5 GiB cap, for declared
  lengths and for streams of unknown length alike.

### Keys

Blobs are content-addressed per scope at
`content/<scope tag>/sha256/<hex[0..2]>/<hex[2..4]>/<hex>`. `<scope tag>` is
`app`, or `org-` plus 32 hex characters of the organization id's SHA-256,
keeping ids out of bucket listings. Deduplication stays within a scope; across
organizations it would reveal that another tenant uploaded a file.

### Upload flow

Direct streaming upload through the host is the only upload path in v1:
`POST /v1/content/manage/assets?filename=team.jpg&title=Team` with the raw
bytes as body and `Content-Type: image/jpeg`.

1. Admission: beyond `media.max_concurrent_uploads` the host answers
   `overloaded` (503, `Retry-After: 1`). A `Content-Length` over
   `media.max_upload_bytes` fails before reading. The route uses its own body
   limit instead of the edge's.
2. The declared type, parameters stripped, must be in `media.allowed_types`.
   The filename is reduced to `[A-Za-z0-9._-]`, at most 200 bytes, with an
   extension matching the type.
3. The body streams into a `tempfile` in `media.spool_dir` while the host
   counts bytes and feeds SHA-256. Passing the limit aborts with
   `asset_too_large`.
4. `infer` sniffs the first 8 KiB. A recognized type must match the declared
   one (`image/jpg` counts as `image/jpeg`). If nothing is recognized, the
   declared type must be `text/plain`, `text/csv`, `application/json` or
   `image/svg+xml`, the file valid UTF-8, and SVG must have an `<svg` root.
   Otherwise `media_type_rejected`.
5. `imagesize` reads raster dimensions from the header without decoding
   pixels. More than 50,000 pixels on a side is rejected as malformed.
6. Blob claim, own transaction: insert a `pending` blob with
   `pending_until = now() + upload deadline`, or read the existing row. A
   `stored` row skips the put (dedupe). An `orphaned` row flips back to
   `stored` with a guarded update. A `deleting` row is retried for up to 2
   seconds, then `blob_unavailable`.
7. `BlobStore::put` from the spool file. Two uploads of the same bytes may
   both put; the key is the hash, so they write the same object.
8. One transaction sets the blob `stored`, inserts the asset and appends
   `content.asset_uploaded`. The response is 201 with the asset.

A crash between 6 and 8 leaves a `pending` row; cleanup removes it and the
object once the lease expires. The spool file is removed on every exit path,
and startup clears spool files older than one hour.

Direct upload beats presigned PUT URLs (not in v1, owner answer CT2) because
the host sees every byte before the asset exists. Size, type sniffing, hashing
and dimensions use one code path for both drivers, and the bucket needs no
CORS policy. The cost is that large files pass through the host, hence the
50 MiB default. The host stores bytes unchanged: no resizing or transcoding
(CT3; imgproxy or a CDN in front of redirect mode can use the dimensions and
`focal_point`) and no EXIF or GPS stripping (CT4).

### Asset JSON

```json
{ "id": "0192f7a1-0b44-7aa2-8c55-1d3e9f20a7b4", "filename": "team.jpg", "content_type": "image/jpeg",
  "size": 248113, "sha256": "9f2c...e01a", "width": 1600, "height": 900, "title": "Team",
  "focal_point": { "x": 0.5, "y": 0.35 }, "created_at": "2026-10-22T16:41:07.512Z", "created_by": "usr_0192f5c4...",
  "updated_at": "2026-10-22T16:41:07.512Z", "deleted_at": null,
  "url": "/v1/content/assets/0192f7a1-0b44-7aa2-8c55-1d3e9f20a7b4/team.jpg" }
```

`filename` and bytes are immutable; a new file is a new asset, so asset URLs
stay stable.

### Serving

Delivery serves an asset only if it is not deleted and a published variant of
a type the caller can read references it. Draft-only uploads stay unreachable
even with a guessed id. The filename segment must match, otherwise 404.

Proxy mode (`serve = "proxy"`, default) streams from the blob store with the
asset's `Content-Type`; `Content-Disposition: inline` only for
`media.inline_types`, else `attachment; filename="<filename>"` (SVG, HTML and
PDF are never inline on the API origin); `X-Content-Type-Options: nosniff` and
`Content-Security-Policy: default-src 'none'; sandbox` always; `ETag:
"<sha256>"`; and `media.asset_cache_control` (`private` when the referencing
type's `access` is not `public`). It is not `immutable`, because unpublishing
must take effect. A single byte range gets 206 for video seeking; multiple
ranges get the full body.

Redirect mode (S3 only) answers 302 to a presigned GET URL valid for
`media.signed_url_ttl`, with `Cache-Control: private, max-age=<ttl - 30 s>`.
Type and disposition come from attributes set at put time; a blob is shared
across assets, so that disposition is `inline` for inline types and a
filename-less `attachment` otherwise. Use it when the bucket has its own
domain or CDN, which also keeps user files off the API origin.

Management `GET /assets/{id}/file` always proxies with `attachment`.

### Deletion and orphan cleanup

- `DELETE /assets/{id}` fails with `asset_in_use` while refs point at it,
  unless `force=true`. Forced deletes leave references in place: delivery
  shows `"asset": null` and saves report `asset_not_found`.
- Deletion is soft. After `trash.retention` the purge job deletes the row
  and, in the same transaction, marks the blob `orphaned` if no other asset
  uses it.
- `content.blob_cleanup` (`media.cleanup_cron`, at most 1000 blobs per run)
  claims orphaned blobs older than `media.orphan_grace` and pending blobs
  with an expired lease by setting `deleting` (`FOR UPDATE SKIP LOCKED`),
  commits, calls `BlobStore::delete`, then deletes the row. A failed delete
  restores the previous state. The guarded state change makes an upload and
  cleanup racing on the same blob end with exactly one winner.

### As built (C7)

- The store is `merezha-blob`'s `ObjectBlobStore`, opened from
  `[modules.content.media]` and provided to every context through
  `Module::blob_store`; `merezha_blob::ERRORS` registers with content's
  codes and readiness adds `ObjectBlobStore::ready`. The S3 credentials
  are nested secrets read with `ModuleSection::secret_at` (`{ env }` or
  `{ file }` references), so the keys keep their `media.s3.*` names.
  `MEREZHA_*` overrides reach top-level module keys only; one naming a
  nested key fails `nested_override_unsupported`.
- The upload route bounds its own body: `Module::body_limits` lets the
  edge read it up to `media.max_upload_bytes` in place of
  `http.body_limit` and leaves a declared length to the route
  (`asset_too_large`). A wait of 30 s for the next bytes, or a body not
  done within the one-hour lease, fails `upload_incomplete` (400); a spool
  write failure is `spool_unavailable` (503). `title` is at most 500
  characters without control characters.
- `infer`'s text guesses (HTML, XML, shell scripts) are not signatures:
  text is judged as UTF-8, so an HTML file declared `text/plain` is stored
  as plain text and served as an attachment. Plain text is therefore not
  in the default `inline_types` (a deviation from section 9's default:
  the acceptance criterion requires the attachment); listing it serves
  such files inline as plain text, still with `nosniff` and the sandbox
  CSP. `allowed_types` must be types the sniffer can confirm
  (`media_type_unsupported` otherwise), so `text/html` cannot be allowed.
- A raster header that does not parse is malformed like an oversized
  side (`media_type_rejected`).
- The claim leases a `pending` row for an hour (`UPLOAD_LEASE`); the put
  must end two minutes before it does. The `orphaned` to `stored` flip
  happens in the transaction that inserts the asset (not in the claim),
  guarded with the row locked: from `stored` or `orphaned`, or from
  `pending` after this upload's own put. A row that cleanup took between
  the claim and that transaction sends the upload back to the claim (three
  claims at most, then `blob_unavailable`). An upload that inserted its
  row and then fails moves it to `deleting` while its lease is still its
  own, deletes the object and the row, so a refused upload leaves nothing;
  a crash leaves the `pending` row for cleanup. When that object delete
  fails the row goes back to `pending` with its lease over, so the next
  upload of the bytes joins it and puts again.
- The claim's insert has no conflict target (`ON CONFLICT DO NOTHING`):
  the primary key and `storage_key` conflict together, and two identical
  uploads at once must both get the row, never a unique violation (review
  finding 1).
- Every step after a deletion claim is fenced on its lease, the token: the
  claim sets `deleting` with a ten-minute lease (`DELETE_LEASE`); right
  before each object delete the worker re-takes the row only while the
  lease is still its token, renewing it; the object delete is bounded by
  half the lease; the row delete and a restore match the renewed token. A
  worker whose lease expired and was taken over, or whose row went and
  came back with the same bytes, touches nothing (`Swept::Lost`), so a
  stale run can never delete a re-uploaded object (review finding 2). A
  batch stops taking new blobs once half a lease has passed. The same
  fence applies to an upload's give-up. An upload that meets a `deleting`
  row whose lease is over takes it over as its own claim and puts again.
- A purge marks the blob `orphaned` after locking it, in a new statement
  that sees assets committed while it waited, so it never orphans a blob
  an upload just reused. "Uses" counts trashed assets too, which an
  undelete may bring back.
- Cleanup also takes `deleting` rows whose delete lease expired: a crash
  between the claim and the object delete. The asset trash purge (assets trashed longer
  than `trash.retention`) runs at the start of `content.blob_cleanup`, not
  in `content.trash_purge`, so the purge that frees a blob and the
  cleanup that deletes it share one job and schedule.
- Delivery decides with the entries' own rule (C5's `delivery::access`,
  on the application path) for each type whose published variants
  reference the asset, and answers `asset_not_found` for every asset the
  caller may not read (anonymous callers included), never 401 or 403, so
  an id reveals nothing. Through a `public` type it uses `asset_cache_control`, else
  `private, no-cache`; responses vary on `Authorization` and `Cookie`. The
  ETag is the SHA-256 and a matching `If-None-Match` is 304. An `If-Range`
  other than that ETag, several ranges or a malformed one get the full
  body. Plain text and CSV are served with `; charset=utf-8`. Every asset
  response (200, 206, 304, 416) carries `nosniff` and the sandbox CSP.
- Redirect mode hands the bytes to the bucket, which serves them without
  `nosniff` or the CSP: give the bucket (or its CDN) its own domain, never
  the API's or the site's origin.
- The local root and `spool_dir` are refused (`blob_root_insecure`) when
  they are a symbolic link (read with `lstat`, never followed), owned by
  another user than the process's effective one, or open to group or
  others, so a directory planted in a shared `/tmp` is never used.
- On S3, `media.s3.timeout` bounds each request, and object reads per
  wait for bytes (a reqwest read timeout, no whole-response bound) so a
  proxied download of a large file outlasts it while it moves; a stall
  longer than the timeout still fails after the bounded retries.
- Management reads (`GET /assets/{id}`, `.../file`) see trashed assets;
  `PATCH` and a soft delete act on live ones only (`asset_not_found`).
  Undelete bumps the generation and appends no event (section 7 has none
  for it).
- Every statement is a TE-T4 `Statement` (`merezha_content::media::STATEMENTS`);
  the media sources hand no SQL to the driver (the workspace scan).
- C9 replaced the application-only seams: the asset routes are mounted
  a second time under `/v1/content/o/{organization}`, for that
  organization's assets, and their delivery is served there too
  (section 8, as built).

## 7. Events

Every event is an outbox record appended inside the transaction of the change
(R1): `OutboxTarget::Event { topic }` with the event type as topic, source
`content`, and the event id (a UUIDv7 minted once per change) as dedupe key.
The envelope (event id, type, version, time, correlation) is AP's (R11); the
trace context travels in the outbox record's columns (R10). `context.tenant`
is the organization for organization scope and unset for application scope.
Events carry ids and metadata, never field values or filenames, so no record
is flagged `contains_secret`; consumers fetch content through the APIs, which
apply access rules, and personal data stays out of webhook payloads and dead
letters.

| Event type (version 1) | Payload | Emitted by |
| --- | --- | --- |
| `content.entry_draft_saved` | `scope, type, id, locale, revision, actor` | create, save, patch, restore |
| `content.entry_published` | `scope, type, id, locale, revision, slug, published_at, first_publish, actor` | publish, batch |
| `content.entry_unpublished` | `scope, type, id, locale, actor, reason (unpublish, delete)` | unpublish, delete |
| `content.entry_deleted` | `scope, type, id, actor` | soft delete |
| `content.entry_undeleted` | `scope, type, id, actor` | undelete |
| `content.entry_purged` | `scope, type, id` | purge, retention |
| `content.asset_uploaded` | `scope, id, content_type, size, sha256, actor` | upload |
| `content.asset_updated` | `scope, id, actor` | metadata patch |
| `content.asset_deleted` | `scope, id, actor, forced` | delete |
| `content.model_activated` | `version, fingerprint, previous_version, change_classes` | activation |

`actor` is the principal subject and kind; system work uses its `sys_<name>`
subject (R13). Fallback rows (a `fr-BE` row now served from `fr`) emit no
events of their own; consumers that cache by locale invalidate on the entry
id.

Content never calls URLs and never calls `Notifier::emit_webhook` (R11).
Notifications subscribes to the `content.*` topics through AP subscriptions
(`JobsHandle::subscribe`) and delivers signed webhooks with its own retries
and dead-lettering: organization events to that organization's endpoints,
application events to application-level endpoints (NT6). A typical project
points its frontend's revalidation endpoint at `content.entry_published` and
`content.entry_unpublished`.

## 8. Multi-tenancy

Decision: each content type declares its scope.

- `scope = "application"` (default), stored as `'app'`: the marketing site,
  docs or app-wide catalog. Most projects use only this.
- `scope = "organization"`, stored as `'org:<organization id>'`: each
  organization manages its own entries, such as a help center per customer.
  Requires identity with organizations.

Management takes the organization from `Principal.tenant`, so a session or API
key acts in its active organization and cannot name another
(`tenant_required` without one). Checks then run in `Tenant(org)` or a
resource scope inside it, so organization roles apply; application-scoped
types are checked in `Application` scope, so application-level roles and
application-scoped API keys apply (R5). Delivery puts it in the path,
`/v1/content/o/{organization}/{type}`, because anonymous readers have no
tenant. Content never reads identity tables; an unknown organization simply
has no entries. References and assets may stay within a scope or point from an
organization to the application scope, never the other way or across
organizations.

Uniqueness, slugs, singletons, generations and blob deduplication are per
scope. One database per application means no application column, and the
multi-application platform keeps that rule, so the layout survives it.

### As built (C9)

- Organization types load only beside identity:
  `ContentModule::with_organizations`, which `merezha run` sets when
  `[modules.identity]` is present; otherwise the model fails
  `content_model_invalid` at `types.<name>.scope`
  (`organizations_required`). Identity always has organizations (with
  `organizations.user_can_create = false` only platform admins create
  them), so no further switch exists.
- Management needed no new statement: C3's scope predicates already
  carried `org:<tenant>` from `Principal.tenant`. A tenant-pinned API key
  never reaches application types (identity refuses its key scope), and
  a principal without a tenant is checked at `Application` before
  `tenant_required`, as C3 ordered.
- Reference rules: an organization entry resolves targets in its own
  scope and the application's; an application entry in the application's
  only. Everything else, another organization's entry or asset included,
  is `reference_not_found` or `asset_not_found`, the answer an id that
  exists nowhere gets. Decision for the two cross-scope checks C3 left
  open: the existence lookup behind `reference_scope` is gone, for every
  caller, since it told any caller that another tenant owned a UUID; and
  a client-supplied id taken in another scope stays `entry_exists`. Ids
  are one primary key, so no answer can hide that the id is taken (a
  fresh id would say it too); the oracle confirms only an id the caller
  already holds, never its scope, type or data. Host-generated ids are
  UUIDv7 with 74 random bits, so they cannot be enumerated; imports that
  supply their own ids must use random UUIDs too, since a predictable
  scheme (ids derived from legacy integers) would be enumerable across
  tenants through `entry_exists`. `entry_referenced` and `asset_in_use` keep counting
  referrers in every scope, without returning them (section 3).
- Delivery on `/o/{organization}` (C5's routes) reads that organization's
  rows and the application's, nothing else: population and the `missing`
  checks are bound to the type's scope on the path, so a reference into
  another organization, even one planted behind validation, is only ever
  `missing`. Referrers list the target's scope only, so an application
  entry's organization referrers stay with their organizations.
- Assets: the path chooses the management scope, as it does for
  delivery. `/v1/content/manage/assets...` is the application scope,
  checked at `Application`, so a tenant-pinned key is refused there
  (key scope, 403). `/v1/content/o/{organization}/manage/assets...` is
  that organization's scope, checked at `Tenant(org)` for a principal
  whose active organization it is (403 otherwise, an application grant
  included). The active organization never moves an upload, so an
  application editor with an organization selected still manages the
  application assets its application entries use. The organization's
  upload route bounds its own body like the application's (a
  `RouteBodyLimit` path may now hold `{name}` segments). `usage` on
  `GET .../manage/assets/{id}` counts `draft` and `published` refs of
  the asset's own scope and `elsewhere`, the refs of every other scope
  (organization entries using an application asset), so the three
  together are what a delete without `force` is refused for
  (`asset_in_use`); other scopes' referrers are counted, never named.
  An organization's asset has the URL
  `/v1/content/o/{organization}/assets/{id}/{filename}`; that route
  serves the organization's assets and the application assets its
  published entries reference, under the referencing types' `access` on
  that path, counting only that organization's refs. Blob keys carry the
  `org-<32 hex>` tag, so identical bytes in two organizations are two
  blob rows and two objects. Asset events carry the organization as
  their tenant. When asset metadata enters delivery responses (issue
  #46), an organization response must use the organization's asset path
  and add the application generation for application assets.
- ETags keep C5's rule: an organization response hashes its
  organization's generation, plus the application's when the type can
  reference entries; another organization's publish changes no tag.
- Content never reads identity tables, so deleting an organization in
  identity leaves its content in place, unreachable through management
  (no principal has that tenant any more) but still served on its
  public delivery path, its assets included, until issue #56 lands.
  #56 purges an organization's content on `identity.organization_deleted`
  and must also give operators a purge they can run themselves (for
  organizations deleted before it, or a missed event). Until then an
  operator removes a deleted organization's public content by hand.

## 9. Configuration and telemetry

### `[modules.content]`

| Key | Type | Default | Range or rule |
| --- | --- | --- | --- |
| `model` | path | `"content.toml"` | Relative to `merezha.toml`, inside the project. |
| `limits.max_request_bytes` | int | 1048576 | 1024 to 8388608. Does not apply to uploads. |
| `limits.max_entry_bytes` | int | 262144 | 1024 to 1048576, canonical JSON of one variant's fields. |
| `limits.default_page_size` | int | 25 | 1 to `max_page_size`. |
| `limits.max_page_size` | int | 100 | 1 to 500. |
| `limits.max_filters` | int | 10 | 1 to 32. |
| `limits.max_sort_keys` | int | 2 | 1 to 4. |
| `limits.max_populate_depth` | int | 2 | 0 to 3. |
| `limits.max_populated_entries` | int | 500 | 1 to 2000. |
| `limits.max_batch` | int | 100 | 1 to 500. |
| `limits.query_timeout` | duration | `"5s"` | 100 ms to 30 s. |
| `revisions.retain` | int | 100 | 2 to 10000. |
| `trash.retention` | duration | `"30d"` | 1 day to 3650 days. |
| `trash.purge_cron` | cron | `"23 3 * * *"` | Validated by AP's cron parser (§B.7). |
| `delivery.public_base_url` | URL | none | `https` unless the listener is loopback. Makes asset and `Location` URLs absolute. |
| `delivery.cache_control_public` | string | `"public, max-age=60, stale-while-revalidate=300"` | Header-safe characters only. |
| `delivery.cache_control_private` | string | `"private, no-cache"` | |
| `media` | section | absent | Media is on when `[modules.content.media]` is present; absent rejects `asset` fields in the model. |
| `media.driver` | `local`, `s3` | `local` | |
| `media.local.root` | absolute path | required for `local` | Created 0700 if missing. |
| `media.s3.bucket` | string | required for `s3` | |
| `media.s3.region` | string | required for `s3` | |
| `media.s3.endpoint` | URL | AWS | No query or fragment. |
| `media.s3.path_style` | bool | `true` | Virtual-hosted style when `false`. |
| `media.s3.allow_http` | bool | `false` | Must be `true` for a plain HTTP MinIO endpoint. |
| `media.s3.access_key_id` | secret | required for `s3` | `{ env = "..." }` or `{ file = "..." }`. |
| `media.s3.secret_access_key` | secret | required for `s3` | |
| `media.s3.timeout` | duration | `"30s"` | 1 s to 300 s, per request. |
| `media.s3.key_prefix` | string | `""` | Key-safe characters. Prepended to every key. |
| `media.serve` | `proxy`, `redirect` | `proxy` | `redirect` needs `s3`. |
| `media.signed_url_ttl` | duration | `"5m"` | 60 s to 3600 s. |
| `media.max_upload_bytes` | int | 52428800 | 1024 to 5368709120. |
| `media.max_concurrent_uploads` | int | 4 | 1 to 64. |
| `media.allowed_types` | string list | png, jpeg, gif, webp, avif, svg+xml, pdf, mp4, webm, mpeg audio, ogg audio, plain text, csv | Bare MIME types. |
| `media.inline_types` | string list | png, jpeg, gif, webp, avif | Must be a subset of `allowed_types`. HTML, SVG, XML and PDF are refused. Plain text left the default in C7 (below). |
| `media.spool_dir` | absolute path | `<OS temp dir>/merezha-content` | Same filesystem rules as `local.root`. |
| `media.asset_cache_control` | string | `"public, max-age=3600"` | |
| `media.orphan_grace` | duration | `"1d"` | 1 hour to 30 days. |
| `media.cleanup_cron` | cron | `"47 3 * * *"` | |

Durations use K's duration type (R12): `"500ms"`, `"30s"`, `"15m"`, `"7d"`.
Queue settings for the `content` queue live under `[jobs.queues.content]`, not
here (R1). Unknown keys and invalid combinations fail startup with every error
at once, as the platform requires. The blob store contributes to readiness with a
`head` of the fixed key `content/readiness/probe`; found or not found both
count as ready, provided the bucket exists: on S3 a one-key list of
`content/readiness/` must also succeed, so a missing bucket (whose `head`
also answers 404) is not ready. Locally the root must be a directory with
mode `& 0o077` clear.

### Telemetry

TE's `names.rs` is the only catalog of names (R9). The rows below are what
content needs; TE adds them to the catalog in the slice that ships each
signal (TE-T6), and the catalog's spelling wins. Durations are in seconds.
Attributes carry the `content.` prefix so TE's allowlist keeps them:
`content.type`, `content.locale`, `content.stage`, `content.driver`,
`content.operation`, `content.outcome`, `content.code`, `content.job`; route
and status class come from TE's HTTP attributes. HTTP request durations come
from TE's server instrument, not a content metric. Entry ids appear only in
traces; tenant ids follow TE's default (on), user ids are off (TE1). Field
values, filenames, slugs and filter values never appear in any signal.

- Histograms: `merezha.content.populate.entries`, `merezha.content.query.rows`,
  `merezha.content.upload.bytes`, `merezha.content.blob.duration` (s).
- Counters: `merezha.content.delivery.not_modified`, `merezha.content.entries.saved`,
  `merezha.content.entries.published`, `merezha.content.entries.unpublished`,
  `merezha.content.validation.failures` (by detail code),
  `merezha.content.revision_conflicts`, `merezha.content.model_superseded_writes`,
  `merezha.content.upload.rejected`, `merezha.content.blobs.cleaned`. Blob errors are the `failed` series of
  `merezha.content.blob.duration`.
- Spans: TE's `content {operation}`, with operations `entry.save`,
  `entry.publish`, `materialize`, `delivery.query`, `populate`,
  `asset.upload`, `blob.put`, `blob.get`.
- Log events, in TE's `<area>.<noun>_<verb>` form: `content.model_loaded`,
  `content.model_activated`, `content.model_tightened`, `content.model_breaking`,
  `content.index_created`, `content.index_dropped`, `content.index_failed`,
  `content.blob_cleanup`, `content.spool_cleared`.

## 10. Crates

Versions checked with `cargo info` on 2026-10-23 with Rust 1.98.0. Each is
pinned in `[workspace.dependencies]`. Crates that declare no MSRV are compiled
and tested on 1.98.0 in their slice.

| Crate | Version | MSRV | License | Use |
| --- | --- | --- | --- | --- |
| `tokio-postgres` | 0.7.18 | 1.85 | MIT OR Apache-2.0 | Through `merezha-postgres`; features `with-serde_json-1`, `with-uuid-1`, `with-jiff-0_2`. |
| `deadpool-postgres` | 0.14.2 | 1.85 | MIT OR Apache-2.0 | Through `merezha-postgres`. |
| `postgres-types` | 0.2.14 | 1.85 | MIT OR Apache-2.0 | `jsonb` and `uuid` mapping. |
| `object_store` | 0.14.2 | 1.85 | MIT/Apache-2.0 | Blob drivers in `merezha-blob`; `default-features = false`, features `fs` and `aws`. It depends on `chrono` 0.4.45 unconditionally, as R14 allows; no `chrono` type appears in `merezha-blob`'s API. |
| `http` | 1.5.0 | 1.57.0 | MIT OR Apache-2.0 | `merezha-blob`: the `Method` that `object_store`'s signer takes; already locked through reqwest and axum. |
| `uuid` | 1.29.0 | 1.89.0 | Apache-2.0 OR MIT | UUIDv7 ids; workspace pin with `v4`, `v5`, `v7`, `serde`. |
| `jiff` | 0.2.38 | 1.70 | Unlicense OR MIT | The workspace's one time crate (R14), pinned with `default-features = false` and `std`, `serde`, `tz-system`, `tzdb-bundle-always`: strict RFC 3339 parsing, UTC normalization. |
| `rust_decimal` | 1.43.0 | 1.67.1 | MIT | `decimal` precision, scale and range checks (28 digits, hence the precision ceiling). |
| `regex` | 1.13.1 | 1.65 | MIT OR Apache-2.0 | `pattern` validation, linear time with a compiled size limit. |
| `sha2` | 0.11.1 | 1.88 | MIT OR Apache-2.0 | Blob hashes, model fingerprint, ETags, cursor fingerprints. The workspace version after K2 (R14). |
| `base64` | 0.23.1 | 1.71.0 | MIT OR Apache-2.0 | URL-safe cursors. |
| `infer` | 0.22.0 | 1.74 | MIT | Magic-byte type sniffing. |
| `imagesize` | 0.15.0 | none declared | MIT | Image dimensions from headers; default features off, enable `png`, `jpeg`, `gif`, `webp`, `heif` (covers AVIF). |
| `deunicode` | 1.6.2 | 1.66 | BSD-3-Clause | Slug transliteration. |
| `tempfile` | 3.27.0 (workspace) | | MIT OR Apache-2.0 | Upload spooling. |
| `bytes` | 1.12.1 | 1.57 | MIT | Blob bodies. |
| `futures-util` | 0.3.34 | 1.71 | MIT OR Apache-2.0 | `BoxFuture`, `BoxStream`. |
| `tokio-util` | 0.7.20 | 1.85 | MIT | Not used: C6 reads spooled files with `tokio::fs`, and C7 streams blob reads (`BlobRead::body`) straight into the response. |
| `serde`, `serde_json`, `toml`, `axum`, `tokio` | workspace pins | | | Model parsing, HTTP. |

Not used: `aws-sdk-s3`, `rust-s3` (section 6), `multer` (raw upload bodies),
`async-trait`, `image` and `img-parts` (no decoding or EXIF work in v1),
`jsonschema` (typed Rust validation gives better error paths), `chrono`
(confined to the adapters R14 names).

## 11. Delivery slices

Each slice is one pull request with tests against real PostgreSQL through
`TestDatabase::create()`, documentation exercised as written, an independent
review and green CI. CI runs PostgreSQL 16; the SQL stays within 14. S3 tests
run against a MinIO service container named by `MEREZHA_TEST_S3_ENDPOINT`,
`MEREZHA_TEST_S3_BUCKET` and credentials, and skip with an explicit message
when unset. Until ID PR 2 lands, tests use a table-driven fixture `Authorizer`
over K's scopes.

Dependencies follow the cross-block delivery order. v1 is C1 to C7 and C9;
C8 and C10 are [later slices](#later-slices-outside-the-v1-order) outside it.
Every write path appends outbox records, so only C1 runs without
`merezha-jobs`:

- C1: K2 (error `details`). Stage 1, second wave.
- C2, C3: C1 and AP-B1, because activation and every save append outbox
  records. C2 also needs the kernel runner's non-transactional step (R15).
- C4: C3 and AP-B3, for the trash purge cron. C5: C4.
- C6: K2's `BlobStore` trait. Stage 2 opens with C6, after ID PR 2.
- C7: C3, C6 and AP-B3, for the blob cleanup cron.
- C9: C5, C7 and ID PR 2's application and tenant scopes. Stage 3.

| Slice | Scope | Acceptance criteria | Test plan |
| --- | --- | --- | --- |
| C1 Content model | Crate `merezha-content`: parser, validator, canonical form, fingerprint, field value validators, Portable Text validator, slug generation. No database. | The section 2 example parses. Every rule in the field tables has an accepting and a rejecting case. A broken model reports every problem with its TOML path at once. A type without `access` fails model validation. A `[[migrations]]` block fails `content_model_invalid`. The fingerprint ignores key order and whitespace and changes on any semantic change. Details carry pointer paths and no user text. | Unit tests per field type: safe-integer bounds, `1.0` refused for integers, decimal normalization, U+0000, datetime precision, each rich text rejection, regex size limit. Golden output for a broken model. A type without `access`, with `access = "private"` or with an unknown permission fails at `types.<name>.access`; each of the three accepted forms parses. |
| C2 Schema and activation | Migration 1, locale sync, activation, diff classification, derived indexes, `merezha content diff`. | Fresh database activates version 1. Additive, tightening, access and index changes activate on restart. Each refused change, `unique` added to an existing field included, fails startup with `content_model_breaking` listing it and leaves the active model untouched. Same version with a new fingerprint fails. Indexes the new model adds are valid before its row turns active, including a new `unique` field's; unneeded ones are dropped after. An invalid index from a crashed build is rebuilt. A build failing three times stops startup with `index_build_failed` and the previous model still active. Readiness stays false until every expected index is valid. Two hosts starting together activate once. | Real PostgreSQL. Concurrent activation from two tasks. `pg_indexes` and `pg_index.indisvalid` assertions. A build killed mid-way leaves the host not ready, and the next start repairs it. An injected failure on every build attempt counts exactly three attempts. `EXPLAIN` shows a range filter on a sortable field using its derived index. |
| C3 Drafts and revisions | Create, read, `PUT`, `PATCH`, revisions, restore, retention, singletons, shared-field copying, management list with filters, sort and cursors, unique enforcement, reference validation, referrers, scope predicates, the model version check. | One revision per save. A stale `If-Match` gives 412 and writes nothing; a missing one gives 428. Retention never prunes the published revision. Shared fields written through `fr` fail `field_not_localized` and appear in every variant after a default save. Unique violations name the field. A second singleton fails. An id of another type, or of a row seeded in another scope, gives 404 `entry_not_found` on every entry route and changes nothing. A host whose model was superseded refuses every write with `content_model_superseded` and reports `content_model_outdated`. | HTTP tests over the axum router and a real database. 50 concurrent saves on one variant: exactly one wins per revision. A route-table-driven test sends every entry route an id of another type and an `org:` row seeded through SQL. Two hosts on versions 1 and 2: saves on host 1 started before, during and after host 2's activation either commit before activation commits or are refused. Cursor walk over 1000 entries with concurrent inserts: no duplicates, no gaps. Property test: random filters and sorts over seeded data match an in-memory evaluation. |
| C4 Publishing and deletion | Publish, unpublish, publish an older revision, batch, fallback materialization, generations, soft delete, undelete, purge, trash job, outbox events. Every `merezha-content` statement, and `merezha content diff`'s, registered with TE-T4's `Statement` registry, dynamic DDL through `batch_text`. | Publish makes the revision visible in its locale and in every locale falling back to it, in the transaction that appends `content.entry_published`. Unpublishing `fr` makes `fr-BE` fall back to `en`. Publish enforces `required` and `require_published`. One failing batch item rolls back the batch. Delete removes every delivery row and emits one `content.entry_unpublished` per live locale plus `content.entry_deleted`. Undelete restores drafts only. Purge removes only expired trash. `merezha_content::STATEMENTS` passes `check_registry` beside the kernel's, and the workspace scan (`crates/postgres/tests/statement_scan.rs`) finds no unregistered SQL in `crates/content/src` or `crates/cli/src/content.rs`. | Fault injection between materialization and commit leaves no rows and no outbox record. Each event lands as an `OutboxTarget::Event` record with the change's dedupe key. A fallback matrix over three locales and every publish state. Event count assertions per operation. Publish, unpublish, delete and undelete join C3's scope-predicate and superseded-model tests. |
| C5 Delivery API | Delivery routes, access modes, preview, `fields`, `populate`, by-slug, ETag and 304, cache headers, model endpoint. | Anonymous reads work on `public` types and get 401 on others. `authenticated` types serve any principal, and for organization types only one whose tenant is the path's organization (403 `unauthorized` otherwise). A permission type gives 403 without the permission, including to a principal granted it only on another type, and serves one granted it at `Application` scope or on that type. Unpublished and missing both give 404. Preview without `content.preview` gives 403. Population respects depth, the entry cap, stage, locale chain and target access. A publish changes the ETag; an unchanged scope answers 304 without reading entries. Preview is `no-store` without ETag. By-slug prefers an own-locale slug. | Seeded entries in three locales. A reference cycle at maximum depth terminates. A statement counter proves the 304 path reads only `generations`. `EXPLAIN` of the default list uses `published_list`. |
| C6 BlobStore | Platform trait, `object_store` local and S3 drivers, key validation, readiness probe, bounded retries. | Put from bytes, a file and a stream, get, ranged get, head, idempotent delete and attributes behave the same on both drivers. Invalid keys never reach a driver. A stopped MinIO yields `Unavailable` within the configured timeout. Signed URLs work and expire. A group-writable local root is refused. | One scenario suite run on a temp directory and on MinIO. A 20 MiB streamed put keeps memory near one part. |
| C7 Assets | Upload flow, sniffing, dimensions, dedupe, asset routes, proxy and redirect serving, asset refs, delete and `force`, cleanup job, upload admission, spool hygiene. | PNG bytes declared as JPEG give 415. HTML declared as text is served as `attachment` with `nosniff` and the sandbox CSP. Oversized uploads give 413 and leave no spool file or blob row. Identical bytes give two assets and one object. Delivery refuses a draft-only asset and serves it after publish. `Range` gives a correct 206. A referenced asset deletes only with `force`. | Local and MinIO drivers. Injected crash after put and before commit is cleaned after the lease. Cleanup and an upload of the same bytes run in parallel 200 times; every asset row's blob still exists. |
| C9 Organization scope | Organization types, tenant-derived management scope, `/o/{organization}` delivery, cross-scope reference rules, per-organization blob tags, docs, a content section in the stage 3 example. | A key of organization A cannot read or write B through any route. B's public delivery shows only B. Organization entries may reference application entries; the reverse is refused. Identical uploads in two organizations create two objects. | A two-organization matrix generated from the route table, so a new route cannot skip it. |

### Later slices, outside the v1 order

Both are deferred from v1 by the [cross-block decisions](cross-block-decisions.md#deferred-from-v1).
Each lands after C9 as its own pull request with its own migration, and keeps
the v1 rules: its writes run the model version check and the scope
predicates of section 3.

**C8 Scheduled publishing** (needs C4 and AP-B2, for delayed job dispatch).
`POST .../schedule` with `{"action": "publish", "at": "2026-11-02T08:00:00.000Z"}`
and `If-Match: "r12"` pins revision 12. Later draft edits do not change what
goes live; reschedule to pick them up. `at` must be in the future and within
`schedule.max_horizon` (default and ceiling 365 days, AP's delayed-job
ceiling; `schedule_invalid`, 422), and one pending schedule per variant and
action may exist (`schedule_conflict`, 409). The same transaction appends a
delayed job record (R1, AP §B.3):

```rust
outbox.append(tx, "content", &[OutboxRecord {
    dedupe_key: format!("schedule:{id}"),
    target: OutboxTarget::Job { queue: "content", kind: "content.schedule_run" },
    run_at: Some(at), ..
}], OnDuplicate::Fail)
```

The handler runs as the system principal `sys_content_scheduler` (R13). It
locks the schedule and returns at once unless it is pending (duplicate
delivery), publishes the pinned revision or unpublishes through the API code
path without `If-Match`, and marks it `done`. A permanent failure
(`validation_failed`, `entry_deleted`) marks it `failed`, appends
`content.scheduled_action_failed` and returns a permanent `JobFailure`;
transient errors, `content_model_superseded` included, retry under the
`[jobs.queues.content]` backoff. Authorization happens when the schedule is
created; a later role change does not cancel it.

The slice adds a `schedules` table keyed to `variants` with `ON DELETE
CASCADE` (`id`, `entry_id`, `locale`, `action`, pinned `revision`, `run_at`,
`state` in `pending`, `done`, `failed`, `cancelled`, `failure_code`), one
pending row per variant and action. Retention also keeps pinned revisions;
deleting an entry cancels its pending schedules. It adds the routes
`POST .../{locale}/schedule` for entries and singletons (`content.entries.publish`),
`GET /schedules` (`content.entries.read`) and `DELETE /schedules/{id}`
(`content.entries.publish`, state `cancelled`); `sys.schedules` on variants
and the `scheduled` list filter; `schedule_not_found` (404); `schedule_id`
and reason `schedule` on the publish events; and TE rows
`merezha.content.schedule.lag`, `merezha.content.schedule.failures`, the
`schedule.run` span and `content.schedule_failed`.

**C10 Model migrations** (needs C4). A `[[migrations]]` block names the
target version and ordered steps:

```toml
[[migrations]]
to = 4
steps = [
  { op = "rename_field", type = "article", from = "summary", to = "excerpt" },
  { op = "drop_field", type = "article", field = "legacy_id" },
  { op = "set_default", type = "article", field = "featured", value = false },
  { op = "map_enum", type = "article", field = "category", map = { release = "news" } },
  { op = "convert", type = "product", field = "price", to = "decimal" },
  { op = "rename_type", from = "post", to = "article" },
  { op = "drop_type", type = "promo" },
  { op = "retire_locale", code = "fr-BE" },
]
```

`convert` covers only well-defined conversions: `integer` to `float` or
`decimal`, `string` to `text`, `text` to `rich_text` (one block per
paragraph), `enum` to `string`, `one` to `many` for references and assets, and
a scalar to a `list` of it. `retire_locale` hides a locale from delivery and
keeps its variants readable for export. No step changes `localized`, `scope`
or `kind`. The run rewrites `variants.draft_data` and rebuilds `published` in
batches of 500 entries, one transaction each, with a resume cursor on the
`models` row (the slice adds `migration`, `migration_phase`,
`migration_after` and a `migrating` state). Steps are pure functions of the
old value, so a rerun batch gives the same result. Revisions are not
rewritten; reads and restores replay the stored steps from the revision's
model version.

The run supersedes the old model before the first batch, so the version
check refuses every write except the run's own batches until the new model is
active: writes pause for the run. The same pause lets C10 accept `unique` on an existing
field (check duplicates, then build, with no writer). C10 must still decide
what old hosts serve while `published` holds rows of both shapes: pause
delivery for migrated types, or have readers accept both.

| Slice | Scope | Acceptance criteria | Test plan |
| --- | --- | --- | --- |
| C8 Scheduled publishing | The `schedules` migration, schedule routes and job handler, retention of pinned revisions, schedule events and metrics. | A schedule publishes the pinned revision after later edits. Duplicate job delivery publishes once. A deleted entry ends the schedule `failed` with `entry_deleted` and emits `content.scheduled_action_failed`. Cancel prevents publication. A handler on a superseded host retries and publishes once on a current one. | Job runner with a test clock. |
| C10 Model migrations | `[[migrations]]` with resumable batches, the write pause, lazy revision migration, `unique` on existing fields. | Each migration op transforms drafts and published rows as specified. A killed run resumes to the same result. No write commits during a run. A revision two versions old restores as valid data. Readiness is false during a run. | Migration over 5000 entries killed between batches, compared with an uninterrupted run. |

## Cross-block dependencies

Owners and contracts are settled in [cross-block decisions](cross-block-decisions.md).

| Need | Owner | What content assumes |
| --- | --- | --- |
| Error registry and optional `details: [{path, code}]` | K2 (R8) | Content registers its codes at startup. Details carry codes and pointer paths, never user text. |
| `Outbox::append(tx, ...)` with event and job targets, `JobsHandle::schedule`, `[jobs.queues.content]` | AP-B1 to AP-B3 (R1) | Section 3, 4 and 7 records are written inside the change's transaction. |
| Non-transactional migration step and readiness gate | K2 runner, FW amendment (R15) | Derived index builds run through it (section 3). |
| `BlobStore` | content owns the shape, K2 adopts it (R13) | The trait in section 6; C6 ships the drivers. |
| `Application`, `Tenant` and `Resource` scopes, application-level roles and keys | ID PR 2 (R5) | Per-type grants on `Resource { kind: "content_type", id }`, with `tenant` `None` for application-scoped types (ID §5). |
| Content permissions in the catalog | ID | The table in section 5, plus permissions named in `access`. |
| Webhooks from `content.*` topics, per tenant and application-level | NT (R11, NT6) | NT subscribes through AP subscriptions; content only appends events. |
| Telemetry rows, attribute allowlist | TE `names.rs` (R9) | The section 9 rows. |
| Per-route body limit override for uploads | K edge | The upload route sets its own limit and streams the body. |
| Duration type, `[modules.content]` presence enables the module | K2 (R12) | Section 9. |

## 12. Decisions

The owner's answers of 2026-10-24 and the internal decisions that affect this
design, recorded in full in [cross-block decisions](cross-block-decisions.md):

- **CT1** No default delivery access. Every type declares `access`; a type
  without it fails model validation. `merezha new` writes `access = "public"`
  on scaffolded site types.
- **CT2** No presigned uploads in v1. A later presigned PUT needs a confirm
  call that runs the same sniffing.
- **CT3** No image transformations in the host. **CT4** No EXIF or GPS
  stripping in v1; stripping without re-encoding drops the orientation tag.
- **CT5** Full-text search later: `tsvector` over `searchable` fields with a
  derived GIN index per type and one text search configuration per locale.
- **CT6** No guest hooks on entries. Stage 3 reactions to content events cover
  side effects.
- **CT7** Batch publish only, no named releases.
- Deferred from v1: scheduled publishing (C8) and model migrations (C10). v1
  activates only model changes that rewrite no stored data, and refuses
  `unique` on an existing field.
- Error `details` are platform-wide (R8). Per-type permissions use identity's
  resource scope (R5). Changing `localized` or `scope` on a live type stays
  unsupported. 100 revisions are kept per variant, with no save coalescing.
  One `content.toml` per application. `deunicode` (BSD-3-Clause) is accepted.
