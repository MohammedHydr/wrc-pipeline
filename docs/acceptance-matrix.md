# Acceptance Matrix — requirement → evidence

Every assessment requirement traced to its code path and verification. A
requirement is only "Done" when there is a code path **and** a test, run log,
or documented manual verification.

| # | Requirement | Code path | Verification |
|---|-------------|-----------|--------------|
| S0 | Scrapy is the framework | `scraper/wrc_scraper/` (Scrapy project: spider, items, pipelines, settings) | `scrapy crawl wrc` runs the pipeline |
| S1 | Fastest without getting blocked | `scraper/wrc_scraper/settings.py` — AutoThrottle + `DOWNLOAD_DELAY` floor + bounded per-domain concurrency + RetryMiddleware on 408/429/5xx + robots.txt; item pipelines run blocking Mongo/S3 I/O off the reactor (`deferToThread`) so downloads never stall | Settings sourced from env (`.env.example`); rationale + applied/rejected tips in `docs/performance.md`; `ARCHITECTURE.md` §Retries |
| S2 | Scrape each body with start/end date filters | `wrc_spider.py:start()` — fans out one GET search request per (body × partition) via `_search_url()` (the ASP.NET form redirects to a GET endpoint — see recon); bodies from `WRC_BODIES` env | JSON log `requesting search page` per (body, partition) |
| S3 | start_date/end_date inputs, time-period iteration, `partition_date` on every record | `wrc_spider.py:__init__` (`-a start_date= -a end_date=`), `config/common.py:iter_partitions()`, `partition_date` set in `parse_results()` | `tests/test_core.py::test_monthly_partitions_cover_range_without_gaps_or_overlaps` |
| S4 | Extract metadata (title, description, identifier, date, link, partition_date, …) | `wrc_spider.py:_extract_records()`; fields in `items.py` | Manual `scrapy shell` verification documented in README §7 |
| S5 | Metadata in NoSQL DB | `pipelines.py:MongoMetadataPipeline` → MongoDB `wrc.landing_document_versions` (+ `wrc.document_state`) | README §8 inspection commands |
| S6a | PDF/DOC stored as-is | `wrc_spider.py:parse_document()` + `pipelines.py:ObjectStoragePipeline` — raw `response.body` uploaded verbatim, after `_content_matches_ext()` magic-byte validation (wrong-content 200 → auditable failure, never stored under a lying extension). The search endpoint links only HTML case pages; legacy decision PDFs are embedded *inside* the page, so `_embedded_decision_pdf()` follows them (`FOLLOW_EMBEDDED_PDF`) and stores the PDF as the authoritative artifact; robots-forbidden legacy PDF paths (e.g. `/en/Equality_Tribunal_Import/`) fall back to storing the permitted HTML case page, so the record still succeeds | `tests/test_binary_document.py::test_parse_document_*`; `tests/test_spider.py::test_embedded_pdf_*`, `test_parse_document_follows_embedded_pdf_*`, `test_pdf_follow_failure_falls_back_to_storing_html_wrapper` |
| S6b | HTML pages stored as `.html` | `parse_document()` falls back to `ext="html"` for non-binary links | Same pipeline path |
| S7 | file path stored in metadata record | `pipelines.py` — `file_path` (`s3://bucket/key`) written by ObjectStoragePipeline, persisted by MongoMetadataPipeline | README §8 (`db.landing_document_versions.findOne()`) |
| S8 | file_hash stored in metadata record | `pipelines.py:HashAndDedupPipeline` — sha256 over exact bytes | `tests/test_core.py::test_sha256_is_deterministic_and_change_sensitive` |
| S9 | Idempotent re-runs; hash-based change detection | Unique Mongo index on natural key `(source, body, identifier)` + upserts; content-addressed keys `body=…/partition=…/<safe-identifier>/<hash>.<ext>` (identifier key-sanitised via `safe_identifier()` — raw value preserved in metadata); unchanged (same hash) → no re-upload | `ARCHITECTURE.md` §Deduplication; `tests/test_object_keys.py` (incl. `test_safe_identifier_*`); live rerun evidence in `wrc.run_logs` (`found=28, scraped=28, unchanged=28, failed=0`, zero new objects) |
| S10 | Structured JSON logs: partition, body, found vs scraped, failures w/ URL+error, end-of-run summary | `config/common.py:configure_json_logging()` (+ `run_id` on every line); `wrc_spider.py` log points + `_build_summary()`/`closed()` summary (persisted to `wrc.run_logs`) | Per-(partition, body) reconciliation into disjoint auditable buckets (scraped incl. unchanged, failed, parse_failed, skipped, listing-at-risk, unaccounted); `unchanged` reported as subset of scraped |
| I1 | NoSQL DB + object storage in Docker | `docker-compose.yml` — MongoDB 7 + MinIO with healthchecks + idempotent bucket bootstrap | `docker compose up -d` from clean checkout |
| I2 | Orchestrator with ingestion/transformation as separate dependent tasks | `orchestration/wrc_dagster/definitions.py` — `scrape_landing_zone >> transform_to_curated >> enrich_decisions` (Dagster job) + `wrc_monthly_incremental` schedule (previous calendar month) | README §5 trigger instructions; `dagster job execute` run logs |
| I3 | All config via env vars / config file, no hardcoded values | `config/settings.py` (pydantic-settings), `.env.example` documents every value | Grep for literals; all tunables flow from `Settings` |
| T1a | Transform: fetch metadata from Mongo by date range | `transform/transform.py:run_transformation()` — query on `partition_date` | `python -m transform.transform --start-date … --end-date …` |
| T1b | Fetch the referenced files from object storage | `transform.py` — `s3.get_object` on the landing `file_path` | Same run |
| T1c-i | PDF/DOC: no transformation | `PASSTHROUGH_EXTS` branch — bytes passed through unchanged | Code path + curated hash equals source hash |
| T1c-ii | HTML: BeautifulSoup relevant-content extraction + new file_hash | `config/html_utils.py:canonicalize_html()` (called by `transform.py`; strips nav/header/footer/script/…; ordered CSS selectors; deterministic serialize; transform re-hashes the output) | `tests/test_core.py::test_canonical_html_*`, `tests/test_html_utils.py::*` |
| T1c-iii | Rename ALL files to `identifier.ext` | `transform.py:_curated_object_key()` — object **name** is `{identifier}.{ext}` with the identifier key-sanitised (`safe_identifier()`: e.g. EAT's `RP89/2008, MN99/2008` → `RP89-2008-MN99-2008.pdf`, raw value kept in metadata), under a body+partition prefix that mirrors landing and prevents collisions | `tests/test_object_keys.py::test_curated_key_*`, `test_safe_identifier_*`; curated bucket listing (0 keys with spaces/extra path segments) |
| T1c-iv | Store in a new object-storage container | Separate bucket `S3_CURATED_BUCKET` (`wrc-curated`) | `docker-compose.yml` bootstrap + `ensure_bucket` |
| T1c-v | Metadata in a new NoSQL collection with new path + hash | `wrc.curated_documents` — new `file_path`, `file_hash`, `size_bytes`, lineage (`source_file_path`, `source_file_hash`), `parser_version` | README §8 |
| T2 | Additional transformation step for data quality (Tips: encouraged) | `transform/enrich.py` — deterministic BeautifulSoup/regex extraction of business fields (parties, acts cited, officer, hearing date, € awards, outcome) into `wrc.enriched_decisions`, with lineage + `extraction_version`; binary scans flagged `binary_source` so coverage stays measurable | `tests/test_enrich.py` (extraction incl. regression cases); `python -m transform.enrich`; third Dagster op |
| A1 | ARCHITECTURE.md — partition size, retries/rate limiting, dedup, 50+ sources; 1 page max | `ARCHITECTURE.md` (all four sections) | Visual check: one page |
| D1 | .MD run instructions | `README.md` — infra, scrape, transform, Dagster, tests, verification from clean checkout | Follow-through from clean checkout |
| D2 | Landing Zone never deleted/updated | Append-only keys (new hash → new key), upsert-only metadata, transform never writes to landing; guarded by `.claude/hooks/guard_dangerous_commands.py` | `ARCHITECTURE.md` §Deduplication |
| D3 | Found vs scraped accounting; every missing record logged with reason | `run_stats` per (partition, body): `found`, `scraped`, `unchanged`, `failed[]` (URL, identifier, error, HTTP status), plus `parse_failures[]` (unusable listing cards), `skipped` (fast-mode), `docs_enqueued` → `records_unaccounted` (silent losses). `parse_document()` wraps callbacks so an unhandled exception is a recorded `stage=parse` failure, not a lost record | Summary reconciliation `found == scraped + failed + parse_failed + skipped + listing_at_risk + unaccounted`; `tests/test_spider.py` covers clean / malformed-card / silent-loss / fast-mode / shortfall cases |

## Recon status

- **Site contract verified** via `scrapy shell` (see
  [recon notes](recon/wrc-search.md)): the ASP.NET form redirects to a GET
  endpoint, body codes read from the checkbox `value` attributes
  (Equality=1, EAT=2, Labour Court=3, WRC=15376), `pageNumber` pagination,
  and the `li.each-item` card structure. `tests/test_spider.py` locks the
  parsing against a fixture captured from the live markup.

## Known gaps (stated honestly)

- **Streaming:** documents are held in memory per item (Scrapy buffers
  responses); acceptable at 500–1000 docs of this size, revisit for 1000×.
  Memory is now bounded by `DOWNLOAD_MAXSIZE`/`WARNSIZE` (oversized responses
  become auditable failures rather than OOM). See [performance notes](performance.md).
- **Re-download of unchanged files (req 9):** the listing exposes no content
  hash, so the default mode re-fetches each document to hash-compare it — it
  never re-*stores* or duplicates unchanged content, but it does re-download.
  `SKIP_EXISTING_IDENTIFIERS=true` avoids the fetch for known identifiers at the
  cost of missing in-place edits. Disclosed in `ARCHITECTURE.md` §Deduplication.
- **Tests:** unit tests cover the pure-Python core plus spider parsing against
  a live-captured fixture. No containerised integration test yet; idempotency
  is verified end-to-end by re-running a range (README) — latest evidence in
  `wrc.run_logs`: same range twice → `found=28, scraped=28, unchanged=28,
  failed=0`, no new objects, no duplicate records.
