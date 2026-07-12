# Architecture

```text
             Dagster job: wrc_pipeline — monthly partition grid

workplacerelations.ie
          │
          ▼
 scrape_landing_zone (Scrapy subprocess, all bodies per run)
          ├──► MongoDB: document_state, landing_document_versions,
          │             run_logs, failed_documents
          └──► MinIO: wrc-landing (immutable source artifacts)
          │
          ▼
 transform_to_curated
 HTML → relevant decision content · PDF/DOC/DOCX/RTF → byte-for-byte
 renamed to identifier.ext
          ├──► MongoDB: curated_documents
          └──► MinIO: wrc-curated (latest curated artifact)
          │
          ▼
 enrich_decisions — additional structured business layer; the assignment
 requirements end at curated, and enrichment failures are logged and
 skipped without failing the Dagster run
```

## Partitioning

The production Dagster grid is monthly: each selected month is one
independently observable, rerunnable run (one Scrapy subprocess, then
transform, then enrich). The spider fans out all configured WRC bodies
within that run and also accepts daily/weekly/monthly windows directly via
CLI/Launchpad. Its reconciliation unit is one `(date window × body)` cell;
every record stores `partition_date` (window start) separately from the
document's publication date. Monthly balances listing overhead, pagination
depth, and recovery scope — a failed month reruns without touching others.

## Rate Limiting and Retries

AutoThrottle is primary: 8 global requests, 4 per domain, 0.25s min delay,
target concurrency 4, 0.5s start delay, 15s max delay — all env-configurable.
RetryMiddleware allows up to four retries after the initial request for
network exceptions, 408/429, and selected 5xx, with a 60s download timeout;
non-retryable HTTP responses and validation failures are recorded, not
repeatedly retried. Request-level retries handle transient HTTP failures;
process/infra failures are handled by rerunning the corresponding Dagster
month. `ROBOTSTXT_OBEY=True`, cookies disabled (stateless GET flow), custom
identifiable `User-Agent`.

## Completeness and Failure Accounting

For every `(window × body)` cell, `scraped` increments only after object and
MongoDB metadata persistence complete; `unchanged` is a subset of `scraped`.
Document failures, listing/parse failures, skips, and unaccounted enqueued
requests all feed reconciliation; a cell is complete only when the
discovered count reconciles and there are no document failures, parse
failures, listing failures, count-parse failures, or unaccounted document
requests — each tracked as a separate condition. Scrapy application logs
are structured JSON and carry a `run_id`; relevant events additionally
include partition, body, URL, identifier, stage, HTTP status, and error.
Dagster captures the subprocess output and exposes op metadata in the UI.
Final per-cell/run counts persist in `run_logs`; document, validation,
pipeline, and parse failures also persist in `failed_documents`, whose
`retry_count` tracks repeat occurrences across reruns — a later
fully-listed run resolves failures that no longer recur.

## Idempotency and Landing Storage

`file_hash` (SHA-256 over stored bytes) identifies the object; `content_hash`
is the logical version identity — equal to `file_hash` for binary documents,
or a SHA-256 over canonical decision text (volatile chrome excluded) for
HTML. MongoDB enforces uniqueness on `document_state` `(source, body,
identifier)` (latest version pointer, object path, hashes, ETag) and on
`landing_document_versions` `(source, body, identifier, content_hash)`
(immutable version metadata). Landing keys are deterministic:
`body=<body>/partition=<partition_date>/<identifier>/<file_hash>.<ext>`.
On a `200`: an unseen identity is uploaded and versioned; a current-hash
match reuses the current object; a historical-hash match reuses that
version; a new hash uploads a new object and version. No landing version is
ever updated or deleted, and a document counts as scraped only after final
MongoDB persistence, which writes in a fixed order: object upload, then
`landing_document_versions` insert, then `document_state` upsert. A failure
between the first two steps leaves an object with no Mongo metadata at all;
a failure between the last two leaves an immutable version that
`document_state` does not yet point to as current. No automated
orphan/linkage reconciliation job exists yet. Stored ETags are replayed
only to the exact URL that issued them; a `304` avoids
re-transmitting the body, but SHA-256 remains the authoritative content
identity. `SKIP_EXISTING_IDENTIFIERS=true` skips known identifiers entirely,
trading edit detection for faster backfills.

## Transformation and Curated Storage

Reads the latest landing version referenced by `document_state` for the
requested range. PDF/DOC/DOCX/RTF are integrity-checked and copied
byte-for-byte; HTML is parsed with BeautifulSoup, stripped of navigation,
forms, and configured chrome, then serialized deterministically. Every
curated object is named `identifier.ext`; its path, SHA-256, size, parser
version, and lineage to the source landing version persist in
`curated_documents`. Curated storage is latest-only — a superseded curated
object may be deleted — while landing remains immutable.

## Configuration

Connection strings, collection/bucket names, partition size, body codes,
concurrency, delays, retry limits, timeouts, HTML selectors, parser, thread
counts, conditional-request and behavioral toggles are all typed
environment-backed settings (`config/settings.py`, `.env.example`). MongoDB
and MinIO run in Docker containers.

## Scaling to 50+ Sources

Each new source implements an adapter for discovery, document fetching, and
mapping into the shared metadata schema; hashing, storage keys, persistence,
lineage, reconciliation, retries, and logging stay shared. The Dagster grid
would move to `(source, month)` partitions for independent schedules,
backfills, and per-source concurrency limits, so source-month cells run on
separate workers without raising one domain's request rate. A distributed
Scrapy scheduler (e.g. scrapy-redis) would only be introduced once a single
source-month must be shared across multiple crawler workers. Landing-
boundary validation and source-scoped dead-letter reporting would isolate
malformed sources; local MongoDB/MinIO can move to managed MongoDB and
S3-compatible storage mainly through endpoint/credential/network config.
