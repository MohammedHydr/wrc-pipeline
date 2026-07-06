# Acceptance Matrix — requirement → evidence

Every assessment requirement traced to its code path and verification. A
requirement is only "Done" when there is a code path **and** a test, run log,
or documented manual verification.

| # | Requirement | Code path | Verification |
|---|-------------|-----------|--------------|
| S0 | Scrapy is the framework | `scraper/wrc_scraper/` (Scrapy project: spider, items, pipelines, settings) | `scrapy crawl wrc` runs the pipeline |
| S1 | Fastest without getting blocked | `scraper/wrc_scraper/settings.py` — AutoThrottle + `DOWNLOAD_DELAY` floor + bounded per-domain concurrency + RetryMiddleware on 408/429/5xx + robots.txt | Settings sourced from env (`.env.example`); rationale in `ARCHITECTURE.md` §Retries |
| S2 | Scrape each body with start/end date filters | `wrc_spider.py:parse_search_form()` — fans out (body × partition) FormRequests; bodies from `WRC_BODIES` env | JSON log `submitting search` per (body, partition) |
| S3 | start_date/end_date inputs, time-period iteration, `partition_date` on every record | `wrc_spider.py:__init__` (`-a start_date= -a end_date=`), `config/common.py:iter_partitions()`, `partition_date` set in `parse_results()` | `tests/test_core.py::test_monthly_partitions_cover_range_without_gaps_or_overlaps` |
| S4 | Extract metadata (title, description, identifier, date, link, partition_date, …) | `wrc_spider.py:_extract_records()`; fields in `items.py` | Manual `scrapy shell` verification documented in README §7 |
| S5 | Metadata in NoSQL DB | `pipelines.py:MongoMetadataPipeline` → MongoDB `wrc.landing_documents` | README §8 inspection commands |
| S6a | PDF/DOC stored as-is | `wrc_spider.py:parse_document()` + `pipelines.py:ObjectStoragePipeline` — raw `response.body` uploaded verbatim | Byte-identical upload; hash computed over exact stored bytes |
| S6b | HTML pages stored as `.html` | `parse_document()` falls back to `ext="html"` for non-binary links | Same pipeline path |
| S7 | file path stored in metadata record | `pipelines.py` — `file_path` (`s3://bucket/key`) written by ObjectStoragePipeline, persisted by MongoMetadataPipeline | README §8 (`db.landing_documents.findOne()`) |
| S8 | file_hash stored in metadata record | `pipelines.py:HashAndDedupPipeline` — sha256 over exact bytes | `tests/test_core.py::test_sha256_is_deterministic_and_change_sensitive` |
| S9 | Idempotent re-runs; hash-based change detection | Unique Mongo index on natural key `(source, body, identifier)` + upserts; content-addressed keys `raw/…/<hash>.<ext>`; unchanged (same hash) → no re-upload | `ARCHITECTURE.md` §Deduplication; re-run manually per README |
| S10 | Structured JSON logs: partition, body, found vs scraped, failures w/ URL+error, end-of-run summary | `config/common.py:configure_json_logging()` (+ `run_id` on every line); `wrc_spider.py` log points + `closed()` summary (persisted to `wrc.run_logs`) | Reconciliation: `found == scraped + failed` per (partition, body); `unchanged` reported as subset of scraped |
| I1 | NoSQL DB + object storage in Docker | `docker-compose.yml` — MongoDB 7 + MinIO with healthchecks + idempotent bucket bootstrap | `docker compose up -d` from clean checkout |
| I2 | Orchestrator with ingestion/transformation as separate dependent tasks | `orchestration/wrc_airflow/dags/wrc_dag.py` — `scrape_landing_zone >> transform_to_curated` (Airflow) | README §5 trigger instructions |
| I3 | All config via env vars / config file, no hardcoded values | `config/settings.py` (pydantic-settings), `.env.example` documents every value | Grep for literals; all tunables flow from `Settings` |
| T1a | Transform: fetch metadata from Mongo by date range | `transform/transform.py:run_transformation()` — query on `partition_date` | `python -m transform.transform --start-date … --end-date …` |
| T1b | Fetch the referenced files from object storage | `transform.py` — `s3.get_object` on the landing `file_path` | Same run |
| T1c-i | PDF/DOC: no transformation | `PASSTHROUGH_EXTS` branch — bytes passed through unchanged | Code path + curated hash equals source hash |
| T1c-ii | HTML: BeautifulSoup relevant-content extraction + new file_hash | `transform.py:extract_relevant_html()` (strip nav/header/footer/script/…; ordered CSS selectors; re-hash) | `tests/test_core.py::test_extract_relevant_html_*` |
| T1c-iii | Rename ALL files to `identifier.ext` | `transform.py` — `out_key = f"curated/{identifier}.{ext}"` | Curated bucket listing |
| T1c-iv | Store in a new object-storage container | Separate bucket `S3_CURATED_BUCKET` (`wrc-curated`) | `docker-compose.yml` bootstrap + `ensure_bucket` |
| T1c-v | Metadata in a new NoSQL collection with new path + hash | `wrc.curated_documents` — new `file_path`, `file_hash`, `size_bytes`, lineage (`source_file_path`, `source_file_hash`), `parser_version` | README §8 |
| A1 | ARCHITECTURE.md — partition size, retries/rate limiting, dedup, 50+ sources; 1 page max | `ARCHITECTURE.md` (all four sections) | Visual check: one page |
| D1 | .MD run instructions | `README.md` — infra, scrape, transform, Airflow, tests, verification from clean checkout | Follow-through from clean checkout |
| D2 | Landing Zone never deleted/updated | Append-only keys (new hash → new key), upsert-only metadata, transform never writes to landing; guarded by `.claude/hooks/guard_dangerous_commands.py` | `ARCHITECTURE.md` §Deduplication |
| D3 | Found vs scraped accounting; every missing record logged with reason | `run_stats` per (partition, body): `found`, `scraped`, `unchanged`, `failed[]` (URL, identifier, error, HTTP status) | Summary reconciliation `found == scraped + failed` |

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
- **Tests:** unit tests cover the pure-Python core plus spider parsing against
  a live-captured fixture. No containerised integration test yet; idempotency
  is verified manually by re-running a range (README).
