# Data Model

MongoDB database `wrc` (configurable via `MONGO_DB`). Three collections, each
with a single clear responsibility. All timestamps are timezone-aware UTC.

## Natural key

Every document's stable identity is the tuple **`(source, body, identifier)`**,
not `identifier` alone. WRC reference numbers are globally unique in practice,
but different legal sources reuse identifiers (every court has its own
"Case No. 1"), so the full tuple is the correct model for the multi-source
scaling goal. The immutable *version* identity is
`(source, body, identifier, file_hash)`, which is what the content-addressed
object keys encode.

---

## `landing_documents` — immutable landing zone (Requirement 5, 7, 8)

One document per `(source, body, identifier)`. Written by the Scrapy pipelines.
Re-runs upsert in place (audit fields preserved); the raw bytes in MinIO are
append-only and never overwritten.

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
| `file_ext` | string | `pdf` \| `doc` \| `docx` \| `rtf` \| `html` |
| `content_type` | string | Response `Content-Type` header |
| `file_hash` | string | **SHA-256** of the exact stored bytes |
| `file_path` | string | `s3://<landing-bucket>/raw/body=…/partition=…/<identifier>/<hash>.<ext>` |
| `size_bytes` | int | Exact byte size of the stored file |
| `first_scraped_at` | datetime | Insert-only; when the record was first seen |
| `first_run_id` | string | Insert-only; run that first created the record |
| `last_seen_at` | datetime | Updated every run the record is observed (even if unchanged) |
| `last_run_id` | string | Run that last touched the record |

**Indexes**

| Index | Type | Purpose |
|-------|------|---------|
| `(source, body, identifier)` | **unique** | Idempotency backbone — no duplicate records |
| `(partition_date)` | plain | Transformation date-range query, time analytics |
| `(body)` | plain | Per-body analytics |
| `(identifier)` | plain | Ad-hoc reference-number lookups |

---

## `curated_documents` — curated zone (Transformation requirement v)

One document per `(source, body, identifier)`, derived from a landing record.
HTML is reduced to decision content and re-hashed; PDF/DOC pass through. Keeps
full lineage back to the landing version it came from.

| Field | Type | Description |
|-------|------|-------------|
| `source`, `body`, `identifier` | string | Natural key (carried from landing) |
| `title`, `description`, `published_date`, `doc_url`, `partition_date`, `partition_label` | — | Carried from landing |
| `file_ext` | string | `html` for cleaned pages; original ext for pass-through |
| `file_path` | string | `s3://<curated-bucket>/curated/body=…/<identifier>.<ext>` (file **name** is `identifier.ext`) |
| `file_hash` | string | SHA-256 of the **curated** bytes (recomputed for HTML) |
| `size_bytes` | int | Byte size of the curated file |
| `source_file_path` | string | Lineage: landing object this was derived from |
| `source_file_hash` | string | Lineage: landing hash (drives change detection) |
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
| `partitions` | array | Per `(partition, body)`: `records_found`, `records_scraped`, `records_unchanged`, `records_failed`, `failures[]` (url, identifier, error, http status) |
| `totals` | object | `found`, `scraped`, `unchanged`, `failed` |
| `finished_at` | string (ISO) | Run completion time |

**Reconciliation invariant:** for each `(partition, body)`,
`records_found == records_scraped + records_failed`, where `records_unchanged`
is the subset of `records_scraped` whose bytes matched a previous run.

**Indexes**

| Index | Type | Purpose |
|-------|------|---------|
| `(finished_at desc)` | plain | "Latest run" query |
| `(run_id)` | plain | Fetch a specific run |

---

## Why upsert-in-place instead of a version table

At assessment scale (500–1000 docs) the landing collection is a *current-state*
view: one row per document, upserted, with the immutable history living in the
append-only object store (every changed version keeps its own content-addressed
key forever). This satisfies "no duplicate records" (Req 9) literally and keeps
queries simple. For the 1000×/50-source design, the same records would move to
an append-only `document_versions` collection keyed on
`(source, body, identifier, file_hash)` with a materialised "latest" view — see
`ARCHITECTURE.md`.
