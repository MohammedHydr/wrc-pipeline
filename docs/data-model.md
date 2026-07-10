# Data Model

MongoDB database `wrc` (configurable via `MONGO_DB`). Four collections, each
with a single clear responsibility. All timestamps are timezone-aware UTC.

## Identity and hashing

Every document's stable identity is the tuple **`(source, body, identifier)`**,
not `identifier` alone. WRC reference numbers are globally unique in practice,
but different legal sources reuse identifiers (every court has its own
"Case No. 1"), so the full tuple is the correct model for the multi-source
scaling goal.

Two hashes are tracked per document:

* **`file_hash`** — SHA-256 of the *exact bytes* downloaded and stored in the
  landing object store (the auditable file identity).
* **`content_hash`** — SHA-256 of the *stable legal content*. For HTML this is
  the canonicalized decision text (page chrome / volatile attributes removed by
  `config/html_utils.py`), so cosmetic page changes don't look like new
  versions. For PDF/DOC/DOCX/RTF it equals `file_hash`.

The immutable **version** identity is `(source, body, identifier, content_hash)`.

---

## `landing_document_versions` — immutable landing zone (Requirements 5, 7, 8)

Append-only. One record per meaningful content version of a document. Written
by `MongoMetadataPipeline`; application code never updates or deletes rows here.
A changed document inserts a *new* version alongside the old one; the raw bytes
in MinIO are likewise append-only and never overwritten.

| Field | Type | Description |
|-------|------|-------------|
| `source` | string | Source netloc, e.g. `www.workplacerelations.ie` |
| `body` | string | Tribunal body (verbatim label) |
| `identifier` | string | Reference number, e.g. `ADJ-00054658` (preserved verbatim) |
| `title` | string | Listing headline |
| `description` | string | Parties / summary line |
| `published_date` | string (ISO date) | Decision date, `YYYY-MM-DD` |
| `doc_url` | string | Absolute URL of the document / detail page |
| `partition_date` | string (ISO date) | Start of the date window the record was found in |
| `partition_label` | string | `YYYY-MM-DD_YYYY-MM-DD` window label |
| `scraped_at` | string (ISO) | When the document was fetched |
| `file_ext` | string | `pdf` \| `doc` \| `docx` \| `rtf` \| `html` |
| `content_type` | string | Response `Content-Type` header |
| `file_hash` | string | SHA-256 of the exact stored bytes |
| `content_hash` | string | SHA-256 of stable legal content (drives change detection) |
| `file_path` | string | `s3://<landing-bucket>/body=…/partition=…/<identifier>/<file_hash>.<ext>` (bucket names the zone; no `raw/` prefix) |
| `size_bytes` | int | Exact byte size of the stored file |
| `schema_version` | int | Record schema version |
| `first_seen_at` | datetime | Insert-only; when this version was first stored |
| `first_run_id` | string | Insert-only; run that first stored this version |

**Indexes**

| Index | Type | Purpose |
|-------|------|---------|
| `(source, body, identifier, content_hash)` | **unique** | Immutable version identity — a re-run of unchanged content is a no-op, changed content inserts a new version |
| `(partition_date, body)` | plain | Transformation date-range query, per-body analytics |
| `(source, body, identifier)` | plain | All versions of one document |
| `(content_hash)` | plain | Reuse an object when content flips back to a prior version |

---

## `document_state` — mutable "latest version" pointer

Exactly one record per `(source, body, identifier)`. This is the only mutable
landing collection: it points at the current immutable version and records when
the document was last observed. Dedup checks read this pointer, never the
immutable version history.

| Field | Type | Description |
|-------|------|-------------|
| `source`, `body`, `identifier` | string | Natural key |
| `title`, `description`, `published_date`, `doc_url`, `partition_date`, `partition_label` | — | Latest listing metadata |
| `latest_version_id` | ObjectId | `_id` of the current `landing_document_versions` record |
| `latest_content_hash` | string | Content hash of the current version (change detection) |
| `latest_file_hash` | string | File hash of the current version |
| `latest_file_path` | string | Object key of the current version |
| `latest_size_bytes` | int | Size of the current version |
| `latest_file_ext`, `latest_content_type` | string | Current version file type |
| `first_seen_at` / `first_run_id` | datetime / string | Insert-only; document first observed |
| `last_seen_at` / `last_run_id` | datetime / string | Updated every run (even when unchanged) |

**Indexes**

| Index | Type | Purpose |
|-------|------|---------|
| `(source, body, identifier)` | **unique** | One state row per document |
| `(partition_date, body)` | plain | Date-range / per-body queries |

---

## `curated_documents` — curated zone (Transformation requirement v)

One record per `(source, body, identifier)`, derived from the current landing
version. HTML is reduced to decision content and re-hashed; PDF/DOC pass
through. Keeps full lineage back to the landing version it came from.

| Field | Type | Description |
|-------|------|-------------|
| `source`, `body`, `identifier` | string | Natural key (from the upsert filter) |
| `title`, `description`, `published_date`, `doc_url`, `partition_date`, `partition_label` | — | Carried from landing |
| `file_ext` | string | `html` for cleaned pages; original ext for pass-through |
| `file_path` | string | `s3://<curated-bucket>/body=…/partition=…/<identifier>.<ext>` — mirrors the landing layout (no `curated/` prefix); basename is `identifier.ext`. Curated is **latest-only**: a rewrite under a new key (e.g. ext HTML→PDF) deletes the superseded object |
| `file_hash` | string | SHA-256 of the **curated** bytes (recomputed for HTML) |
| `size_bytes` | int | Byte size of the curated file |
| `source_version_id` | ObjectId | Lineage: landing version this was derived from (drives skip-unchanged) |
| `source_content_hash` | string | Lineage: landing content hash |
| `source_file_path` | string | Lineage: landing object |
| `source_file_hash` | string | Lineage: landing file hash |
| `parser_version` | string | HTML-cleaner version; a bump re-transforms without touching landing |
| `run_id` | string | Transformation run that produced this record |
| `transformed_at` | datetime | When the curated record was written |

**Indexes**

| Index | Type | Purpose |
|-------|------|---------|
| `(source, body, identifier)` | **unique** | One curated record per document |
| `(partition_date)` | plain | Downstream date-range queries |

---

## `run_logs` — run summaries (Requirement 10)

Append-only. One document per crawl, mirroring the end-of-run JSON summary.

| Field | Type | Description |
|-------|------|-------------|
| `run_id` | string | Correlates with `*_run_id` on records and every JSON log line of the run |
| `spider`, `reason` | string | Spider name and close reason |
| `start_date`, `end_date`, `partition_size` | — | Run parameters |
| `partitions` | array | Per `(partition, body)`: `records_found`, `records_scraped`, `records_unchanged`, `records_failed`, `failures[]` (url, identifier, error, http status), listing failures, reconciliation flags |
| `totals` | object | `found`, `scraped`, `unchanged`, `failed`, listing failures, reconciled/complete partition counts |
| `finished_at` | string (ISO) | Run completion time |

**Reconciliation invariant:** for each `(partition, body)`,
`records_found == records_scraped + records_failed + listing_at_risk`, where
`records_unchanged` is the subset of `records_scraped` whose content matched a
previous run.

**Indexes**

| Index | Type | Purpose |
|-------|------|---------|
| `(finished_at desc)` | plain | "Latest run" query |
| `(run_id)` | plain | Fetch a specific run |

---

## Why an immutable version history plus a mutable pointer

Requirement 9 ("running twice must not create duplicate records or re-download
unchanged files") and the tip "don't delete/update Landing Zone data" pull in
slightly different directions: the first wants no duplicates, the second wants
full history. The versions + state split satisfies both — every distinct
content version is preserved forever in `landing_document_versions` (and its
own content-addressed object), while `document_state` gives the single
current-row view used for dedup and downstream reads. An unchanged re-run
inserts no version and uploads no object; it only moves `last_seen_at`.
