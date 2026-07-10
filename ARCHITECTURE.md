# Architecture

```
                    ┌─────────────── Dagster job ────────────────┐
                    │                                            │
  workplacerelations.ie ──► Scrapy spider ──► Landing zone ──► Transform ──► Curated zone
       (per body ×             (wrc)          MinIO bucket +      script      MinIO bucket +
        per month)                            Mongo collection               Mongo collection
```

## Partition size (default: monthly)

The site holds ~63k records over ~30 years — roughly 100–300 records per month
in recent years. At 10 results/page a monthly window per body is ~5–30 listing
pages: large enough to amortise per-request overhead, small enough that a failed
partition retries cheaply in isolation, `partition_date` gives useful lineage,
and we never hit a pagination-depth limit. `PARTITION_SIZE` is configurable
(`daily`/`weekly`/`monthly`); drop to weekly for very dense backfills.

## Retries and rate limiting

- **AutoThrottle** adapts delay/concurrency to observed latency — "fastest
  without getting blocked": it speeds up while the server is healthy, backs off
  on slowdowns. Plus bounded per-domain concurrency (default 4) and a base delay.
- Scrapy `RetryMiddleware` (`RETRY_TIMES=4`) retries only transient failures
  (408/429/5xx + network errors); `robots.txt` is respected.
- Every failed record is captured with URL, identifier, stage, error and HTTP
  status in per-partition stats, logged as JSON, and rolled into the end-of-run
  summary — so `found = scraped + failed` is always reconcilable per (partition,
  body). Content that downloads but fails type validation is a failure, not a
  silent bad store.

## Deduplication / idempotency

- **Structural:** unique index on the natural key `(source, body, identifier)`
  (+ immutable version key incl. `content_hash`); all writes are upserts — a
  rerun cannot create duplicate records. The full tuple (not `identifier` alone)
  keeps the model correct across sources that reuse reference numbers.
- **Change detection:** each file is SHA-256 hashed. A matching hash marks the
  item `unchanged` — no re-upload, only `last_seen_at` moves.
- **Content-addressed keys** (`body=…/partition=…/<identifier>/<hash>.<ext>`; the
  bucket names the zone, so no redundant prefix): identical bytes map to the same
  key (re-upload is a no-op); a changed document gets a *new* key; landing is
  append-only. Curated mirrors the layout as `body=…/partition=…/<identifier>.<ext>`
  but is latest-only (superseded objects deleted on rewrite).
- **Honest trade-off:** the listing exposes no content hash, so detecting an
  *edited* document requires re-fetching it. Default mode re-fetches and
  hash-compares (never re-stores unchanged content); `SKIP_EXISTING_IDENTIFIERS`
  skips the fetch for known identifiers, trading in-place-edit detection for
  speed.

## Scaling to 50+ sources

1. **Source-plugin contract** — each source implements `discover(partition)` and
   `fetch(record)`; hashing/storage/metadata/logging are already source-agnostic
   (`source` on every record, keys namespaced).
2. **Dagster partitioned jobs** — a partition per (source, month) with
   per-partition retries, backfills and concurrency caps per source domain.
3. **Shared frontier** — scrapy-redis (or equal) so multiple workers crawl one
   source; per-domain politeness stays in config.
4. **Schema validation + dead-letter** at the landing boundary so one bad source
   can't poison the curated zone.
5. Swap MinIO/Mongo for managed S3/Atlas via the same env vars — no code change.
