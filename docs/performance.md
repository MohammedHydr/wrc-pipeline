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
| 3 | Transform parallelized across records with a bounded thread pool | `transform/transform.py`, `TRANSFORM_WORKERS` | Overlaps S3/Mongo latency — the single biggest wall-clock win; idempotent regardless of completion order |
| 4 | Cookie middleware disabled (`COOKIES_ENABLED=false`) | `scraper/wrc_scraper/settings.py` | Flow is stateless GET (recon-confirmed); removes per-request cookie overhead and avoids volatile tracking cookies |
| 5 | Download memory guard (`DOWNLOAD_MAXSIZE` / `DOWNLOAD_WARNSIZE`) | `scraper/wrc_scraper/settings.py` | Bounds memory for buffered document downloads; an oversized response becomes an **auditable failure** via the errback, never a silent loss or OOM |

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
(default 8); set to `1` for fully sequential execution.

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

## Honest open item: don't block the reactor

The item pipelines (`HashAndDedupPipeline`, `ObjectStoragePipeline`,
`MongoMetadataPipeline`) perform **synchronous** `pymongo` and `boto3` calls
inside `process_item`, which runs on Scrapy's Twisted reactor thread. While a
blocking DB/S3 call runs, **no other downloads make progress**. This partly
offsets the configured request concurrency.

This is the highest-leverage remaining throughput improvement that stays within
the politeness policy. The fix is to move the blocking I/O off the reactor
(`twisted.internet.threads.deferToThread`, or async pipelines). It is a
non-trivial change with concurrency implications (client thread-safety, ordering,
failure accounting), so it is **flagged, not silently applied**. Revisit if crawl
throughput becomes the bottleneck.

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

Raising `CONCURRENT_ITEMS` is also not useful today: because the item pipelines
block the reactor (above), more concurrent items do not parallelize the actual
I/O. It becomes worthwhile only after the pipelines are made non-blocking.

## Scaling to 50+ sources

These are the right levers when the crawl fans out across **many domains**
(they do little for a single-domain crawl, so they are documented, not set):

- **`SCHEDULER_PRIORITY_QUEUE = "scrapy.pqueues.DownloaderAwarePriorityQueue"`** —
  balances the downloader across domains in a broad crawl.
- **`REACTOR_THREADPOOL_MAXSIZE`** — larger DNS-resolver thread pool when
  resolving many distinct hosts.
- **`JOBDIR`** — persist scheduler/dupefilter state for resumable long crawls.
- **Non-blocking item pipelines** (see open item) — required before per-domain
  concurrency actually pays off.
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
| `TRANSFORM_WORKERS` | 8 | Transform thread-pool size |
| `HTTP_CACHE_ENABLED` | false | Dev-only response cache |
| `SKIP_EXISTING_IDENTIFIERS` | false | Fast incremental mode (skips change detection) |
