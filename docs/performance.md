# Performance Notes

Where time actually goes in this pipeline, the optimizations applied, and — just
as important — the popular "fast spider" tips we deliberately **did not** apply
because they conflict with the non-negotiable politeness policy in `CLAUDE.md`.

## TL;DR — this pipeline is I/O-bound, not CPU-bound

Wall-clock time is dominated by **network and storage latency**, not parsing:

- **Scraper:** HTTP round-trips to `workplacerelations.ie` + AutoThrottle
  politeness delays.
- **Transform:** S3 `GET` → S3 `PUT` → Mongo upsert, per record.

So the biggest levers are **overlapping I/O (concurrency)** and **removing
per-request overhead** — not micro-optimizing the HTML parser (which is already
`lxml`, the fastest BeautifulSoup backend).

## Optimizations applied

| # | Change | Where | Effect |
|---|--------|-------|--------|
| 1 | Strip-tags removal in **one** DOM traversal instead of one walk per tag (`find_all(list(STRIP_TAGS))`) | `config/html_utils.py` | ~12× fewer tree walks in `_remove_tags`; byte-identical output |
| 2 | Scraper skips HTML serialization it never uses (`need_html=False`) | `config/html_utils.py`, `scraper/wrc_scraper/pipelines.py` | Skips attribute cleaning, text-node rewrite, serialize + encode on **every** HTML doc crawled; only `content_bytes` (the change-detection hash input) is produced |
| 3 | Transform and enrichment parallelized across records with a bounded thread pool | `transform/transform.py`, `transform/enrich.py`, `TRANSFORM_WORKERS` | Overlaps S3/Mongo latency — the single biggest wall-clock win; idempotent regardless of completion order |
| 4 | Cookie middleware disabled (`COOKIES_ENABLED=false`) | `scraper/wrc_scraper/settings.py` | Flow is stateless GET (recon-confirmed); removes per-request cookie overhead and avoids volatile tracking cookies |
| 5 | Download memory guard (`DOWNLOAD_MAXSIZE` / `DOWNLOAD_WARNSIZE`) | `scraper/wrc_scraper/settings.py` | Bounds memory for buffered document downloads; an oversized response becomes an **auditable failure** via the errback, never a silent loss or OOM |
| 6 | Conditional re-fetches (`If-None-Match` → 304) | `wrc_spider.py:_load_validators()/_conditional_headers()`, state `latest_etag` | Unchanged known documents cost a header exchange, **zero body bytes** — measured live (PDF w/ ETag → `304, size_download: 0`). Dynamic HTML pages send no validators (`no-cache`), so they re-fetch + hash-compare — provably the only change detection there. Toggle: `USE_CONDITIONAL_REQUESTS` |
| 7 | Streaming pass-through transform (`_spooled_copy`) | `transform/transform.py` | PDF/DOC bytes stream S3→spool→S3 while hashing (1 MiB chunks; ≤8 MiB spools in RAM, larger spills to disk) — transform memory bounded regardless of document size |

### Notes on #2 (`need_html`)

`canonicalize_html` produces two outputs: `content_bytes` (normalized visible
text, used for the content hash) and `html_bytes` (deterministic curated HTML).
The scraper's change-detection path only needs the hash, so `need_html=False`
returns `content_bytes` and skips building `html_bytes`. A regression test
(`tests/test_html_utils.py::test_need_html_false_matches_content_bytes_and_skips_html`)
asserts `content_bytes` is byte-identical to the full path, because whitespace
normalization is idempotent under the final text collapse. The transform still
calls the full path (`need_html=True`) to produce curated HTML.

### Notes on #3 (transform threads)

`pymongo` and `botocore` low-level clients are both thread-safe, so a single
Mongo/S3 client is shared across workers. Each record upserts its own
`(source, body, identifier)` key, so completion order never affects the result —
idempotency is preserved. Worker count is env-driven via `TRANSFORM_WORKERS`
(default 8); set to `1` for fully sequential execution. The enrichment step
(`transform/enrich.py`) uses the same pattern and the same setting.

## Already in place (good practice, no change needed)

- **AutoThrottle** enabled and tuned (adapts concurrency/delay to server latency).
- **`lxml`** BeautifulSoup backend — fastest available.
- **Pre-compiled module-level regexes** (`_WHITESPACE_RE`, `_BETWEEN_TAGS_RE`,
  `RESULT_COUNT_RE`).
- **DNS cache**, **HTTP compression**, **Telnet console off**.
- **Retry middleware** with an extended transient-status list.
- **HTTP cache** available for development (`HTTP_CACHE_ENABLED`) to avoid
  re-hitting the site while iterating on selectors.
- **`O(1)` fast-mode dedup** — `known_hashes` keyed by `(source, body, identifier)`.
- **Item payload released** after storage (`file_content = b""`) to free memory.

## Non-blocking item pipelines (applied)

The item pipelines (`HashAndDedupPipeline`, `ObjectStoragePipeline`,
`MongoMetadataPipeline`) perform `pymongo` and `boto3` calls that block. They
used to run on Scrapy's Twisted reactor thread, stalling all in-flight
downloads for the duration of every DB/S3 call. Each `process_item` now
returns `deferToThread(self._process, ...)`, so the blocking I/O runs on the
reactor's worker thread pool (sized by `REACTOR_THREADPOOL_MAXSIZE`) while
downloads keep progressing.

Design constraints that keep this safe:

- **Per-item ordering unchanged** — Scrapy awaits each pipeline's Deferred
  before handing the item to the next pipeline, so hash → store → metadata
  still runs strictly in order for any given item.
- **Race-free accounting** — worker threads never touch `run_stats` directly;
  the `scraped`/`unchanged` counters are marshalled to the reactor thread via
  `reactor.callFromThread(spider.note_scraped/-unchanged, key)`, so every
  `run_stats` mutation (spider callbacks, errbacks, signal handlers, counters)
  stays single-threaded. No locks needed.
- **Failure accounting intact** — an exception inside the threaded `_process`
  errbacks the Deferred; Scrapy fires `item_error`, which the spider already
  records as an auditable failure.
- **Client thread-safety** — `pymongo` and low-level `botocore` clients are
  thread-safe (the transform step already shares them across a thread pool).
- **Concurrent same-key writes** — the unique-index + `DuplicateKeyError`
  recovery path in `MongoMetadataPipeline` was designed for concurrent
  insertion races and covers them.

## Deliberately NOT done (conflicts with politeness)

`CLAUDE.md` makes politeness non-negotiable. The following common "make your
spider 10x faster" tips are **out of scope** here because they degrade
politeness or realism:

- Cranking `CONCURRENT_REQUESTS` / `CONCURRENT_REQUESTS_PER_DOMAIN` well beyond
  the configured bounds.
- Setting `DOWNLOAD_DELAY=0` or disabling **AutoThrottle**.
- Disabling `ROBOTSTXT_OBEY`.
- Disabling retries to "go faster".
- Stealth / anti-detection / TLS spoofing / CAPTCHA bypass.

With the pipelines now non-blocking, effective item parallelism is bounded by
`REACTOR_THREADPOOL_MAXSIZE` (default 10) and `CONCURRENT_ITEMS` (Scrapy
default 100) — the thread pool is the binding limit; raise it before touching
`CONCURRENT_ITEMS`.

## Scaling to 50+ sources

These are the right levers when the crawl fans out across **many domains**
(they do little for a single-domain crawl, so they are documented, not set):

- **`SCHEDULER_PRIORITY_QUEUE = "scrapy.pqueues.DownloaderAwarePriorityQueue"`** —
  balances the downloader across domains in a broad crawl.
- **`REACTOR_THREADPOOL_MAXSIZE`** (env-driven, default 10) — larger
  DNS-resolver thread pool when resolving many distinct hosts.
- **`JOBDIR`** (env-driven, default off) — persist scheduler/dupefilter state
  to resume an *interrupted* long backfill. Off for normal runs: a stale
  JOBDIR dupe-filters every document request on a fresh rerun, which would
  poison the found/scraped reconciliation.
- **Non-blocking item pipelines** — already applied (above), a prerequisite
  for per-domain concurrency to pay off.
- **Horizontal partitioning** — the `(partition, body)` fan-out is already the
  natural unit of parallelism; run partitions/bodies as independent workers or
  processes rather than pushing a single spider's request rate up.

## Tuning knobs (all env-driven, `config/settings.py`)

| Env var | Default | Purpose |
|---------|---------|---------|
| `CONCURRENT_REQUESTS` | 8 | Global in-flight request cap |
| `CONCURRENT_REQUESTS_PER_DOMAIN` | 4 | Per-domain politeness cap |
| `DOWNLOAD_DELAY` | 0.25 | Base delay (AutoThrottle adapts from here) |
| `AUTOTHROTTLE_TARGET_CONCURRENCY` | 4 | AutoThrottle target parallelism |
| `RETRY_TIMES` | 4 | Transient-failure retries |
| `COOKIES_ENABLED` | false | Cookie middleware (off — stateless flow) |
| `DOWNLOAD_MAXSIZE` | 268435456 | Max buffered download (bytes) |
| `DOWNLOAD_WARNSIZE` | 33554432 | Warn threshold (bytes) |
| `TRANSFORM_WORKERS` | 8 | Transform/enrichment thread-pool size |
| `HTTP_CACHE_ENABLED` | false | Dev-only response cache |
| `SKIP_EXISTING_IDENTIFIERS` | false | Fast incremental mode (skips change detection) |
