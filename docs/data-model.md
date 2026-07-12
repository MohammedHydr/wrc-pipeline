# Data Model

MongoDB database `wrc` (configurable via `MONGO_DB`). Five collections, each
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
| `file_path` | string | `s3://<landing-bucket>/body=…/partition=…/<safe-identifier>/<file_hash>.<ext>` (bucket names the zone; no `raw/` prefix; the identifier segment is key-sanitised — e.g. `RP89/2008, MN99/2008` → `RP89-2008-MN99-2008` — while `identifier` stays verbatim) |
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
| `latest_etag` | string? | ETag observed on the artifact response (PDFs have them; dynamic HTML pages send none) — drives the next run's `If-None-Match` conditional re-fetch (304 → unchanged, zero bytes) |
| `latest_fetched_url` | string? | URL the artifact bytes actually came from (case page vs embedded PDF) — the ETag is only replayed against this exact URL |
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
| `file_path` | string | `s3://<curated-bucket>/body=…/partition=…/<safe-identifier>.<ext>` — mirrors the landing layout (no `curated/` prefix); basename is the key-sanitised `identifier.ext`. Curated is **latest-only**: a rewrite under a new key (e.g. ext HTML→PDF) deletes the superseded object |
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

## `enriched_decisions` — business layer (beyond spec)

One record per `(source, body, identifier)`, derived from the curated
artifact by deterministic BeautifulSoup/regex extraction (`transform/enrich.py`,
no ML). HTML decisions get `extraction_status: "extracted"`; legacy binary
scans get `"binary_source"` (would need OCR) so coverage stays measurable.
Binary-source records carry only the metadata, party split, and lineage
fields plus empty text fields (`paragraphs: []`, counts `0`); the extraction
fields (`adjudication_officer` through `is_anonymised`) appear on `extracted`
records.

| Field | Type | Description |
|-------|------|-------------|
| `source`, `body`, `identifier` | string | Natural key (unique index) |
| `title`, `description`, `published_date`, `partition_date`, `doc_url` | — | Carried from the curated record |
| `complainant` / `respondent` | string? | Split from the listing description ("A v B") |
| `adjudication_officer` | string? | From the labelled "Adjudication Officer:" line |
| `hearing_date` | string (ISO)? | From "Date of (Adjudication) Hearing:" |
| `acts_cited` | string[] | Statutes cited, e.g. `Unfair Dismissals Act 1977` |
| `complaint_references` | string[] | `CA-XXXXXXXX-XXX` references |
| `award_amounts_eur` / `award_max_eur` | number[] / number? | Monetary amounts found in the decision text |
| `outcome_signals` | string[] | Coarse phrases found ("well founded", "dismissed", …) — signals, not a classification |
| `sections_cited` | object[] | `{section, act}` pairs — statute-section facets |
| `practice_areas` | string[] | Taxonomy derived from acts (unfair_dismissal, equality_discrimination, …) |
| `cited_decisions` | string[] | Other decisions referenced — the precedent/citation graph |
| `received_date` | string (ISO)? | Earliest complaint date of receipt |
| `days_to_decision` | int? | Receipt → published decision (time-to-resolution metric) |
| `decision_type` | string? | decision \| recommendation \| determination \| correction_order |
| `outcome` | string? | upheld \| not_upheld \| mixed — deterministic phrase rules, never a guess |
| `self_represented` | bool | "Self-Represented" appears in the decision |
| `is_anonymised` | bool | Generic party descriptors ("A Worker v A Hotel") |
| `paragraphs` | string[] | Decision body split into block-level paragraphs (scripts/styles removed, whitespace normalised) — the analysis-ready text every other field is extracted from |
| `paragraph_count` / `text_length` | int | Size of the extracted text (coverage/quality signal; `0` for binary sources) |
| `extraction_status` | string | `extracted` \| `binary_source` |
| `source_file_path` / `source_file_hash` | string | Lineage to the exact curated artifact |
| `extraction_version` | string | Bump re-extracts without touching curated/landing |
| `run_id` / `enriched_at` | string / datetime | Enrichment run provenance |

**Indexes**

| Index | Type | Purpose |
|-------|------|---------|
| `(source, body, identifier)` | **unique** | One enriched record per document |
| `(partition_date)` | plain | Date-range queries |
| `(acts_cited)`, `(practice_areas)` | plain (multikey) | Filter case law by statute / practice area |
| `(adjudication_officer)` | plain | Per-officer analytics |
| `(cited_decisions)` | plain (multikey) | Reverse citation lookup ("who cites this decision?") |
| `(outcome)` | plain | Success-rate aggregations |

---

## `run_logs` — run summaries (Requirement 10)

Append-only. One document per crawl, mirroring the end-of-run JSON summary
(`wrc_spider.py:_build_summary()`).

| Field | Type | Description |
|-------|------|-------------|
| `run_id` | string | Correlates with `*_run_id` on records and every JSON log line of the run |
| `spider`, `reason` | string | Spider name and Scrapy close reason (`finished`, `shutdown`, …) |
| `start_date`, `end_date`, `partition_size` | string | Run parameters |
| `partitions` | array | One entry per `(partition, body)` — see breakdown below |
| `totals` | object | Run-wide sums: `found`, `scraped`, `unchanged`, `failed`, `parse_failed`, `skipped`, `unaccounted`, `listing_failures`, `records_listing_at_risk`, `reconciled_partitions`, `complete_partitions` |
| `finished_at` | string (ISO) | Run completion time |

Each `partitions[]` entry:

| Field | Description |
|-------|-------------|
| `partition`, `body` | The `(partition, body)` unit being reconciled |
| `records_found` | Result count the site reported for this search |
| `records_scraped` | Fully persisted (metadata + object) — success is counted only after the last pipeline stage |
| `records_unchanged` | Subset of `records_scraped` whose content matched a previous run (no re-store) |
| `records_failed` / `failures[]` | Download/validation/pipeline failures — each with `url`, `identifier`, `status`, `stage`, `error` (+ `error_type` for pipeline errors) |
| `records_parse_failed` / `parse_failures[]` | Listing cards missing identifier/link — recorded, never silently dropped |
| `records_skipped` | Fast-mode (`SKIP_EXISTING_IDENTIFIERS`) skips |
| `docs_enqueued` | Document requests actually enqueued |
| `records_unaccounted` | Enqueued requests that never reached a terminal state (silent loss — surfaced, never hidden) |
| `listing_failures[]` / `records_listing_at_risk` | Failed listing pages (`url`, `page`, `status`, `error`, `records_at_risk`) and the records they would have yielded |
| `count_parse_failed` | Result-count banner could not be parsed (found is then unknown) |
| `records_accounted`, `reconciled`, `complete` | The invariant check below, its verdict, and whether the partition also had zero failures |

**Reconciliation invariant** — every found record lands in exactly one bucket;
for each `(partition, body)`:

```
records_found == records_scraped + records_failed + records_parse_failed
               + records_skipped + records_listing_at_risk + records_unaccounted
```

`reconciled` is true when that equation holds; `complete` additionally
requires zero failures of any kind.

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
