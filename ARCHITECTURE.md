# Architecture

```
                    ┌─────────────── Airflow DAG ────────────────┐
                    │                                            │
  workplacerelations.ie ──► Scrapy spider ──► Landing zone ──► Transform ──► Curated zone
       (per body ×             (wrc)          MinIO bucket +      script      MinIO bucket +
        per month)                            Mongo collection               Mongo collection
```

## Why monthly partitions (default)

The site holds ~63k records over ~30 years, i.e. roughly 100–300 records per
month in recent years. With 10 results per page, a monthly window per body is
typically 5–30 listing pages — large enough to amortise the form-submission
overhead, small enough that (a) a failed partition can be retried cheaply and
in isolation, (b) `partition_date` gives useful lineage granularity, and
(c) we never risk hitting a pagination depth limit or an unstable, very long
result set. `PARTITION_SIZE` is configurable (`daily`/`weekly`/`monthly`);
for backfills of very dense ranges, drop to weekly.

## Retries and rate limiting

* **AutoThrottle** adapts delay/concurrency to observed server latency — this
  is the "fastest without getting blocked" mechanism: it speeds up while the
  server is healthy and backs off on slowdowns.
* Bounded per-domain concurrency (default 4) plus a small base delay.
* Scrapy `RetryMiddleware` with `RETRY_TIMES=4` on 408/429/5xx and network
  errors; `robots.txt` is respected by default.
* Every failed document download is captured with URL, identifier, error and
  HTTP status in the per-partition stats, logged as JSON, and included in the
  end-of-run summary persisted to Mongo — so `found = scraped + failed` is
  always accountable per partition/body.

## Deduplication / idempotency

* **Structural:** the landing collection has a **unique index on the natural
  key `(source, body, identifier)`** and all writes are upserts — re-running a
  range can never create duplicate records. The key is the full tuple rather
  than `identifier` alone so the model stays correct across multiple sources
  that reuse reference numbers.
* **Change detection:** each file is hashed (sha256). If the stored hash for
  an identifier matches, the item is marked `unchanged`: no upload happens and
  only `last_seen_at` moves.
* **Content-addressed storage keys**
  (`raw/body=…/partition=…/<identifier>/<hash>.<ext>`): identical bytes map to
  the same key (re-upload is a no-op), changed documents get a *new* key, and
  nothing in the landing zone is ever deleted or overwritten (append-only).
* Trade-off (stated honestly): the listing page exposes no content hash, so
  detecting a *changed* document requires re-fetching it. The default mode
  re-fetches and hash-compares (satisfies "don't re-download **unchanged**
  files" at the storage level and full change detection). For fast
  incremental runs, `SKIP_EXISTING_IDENTIFIERS=true` skips the HTTP fetch for
  known identifiers entirely, at the cost of missing in-place edits.

## Scaling to 50+ sources

1. **Extract a source contract:** each source becomes a plugin implementing
   `discover(partition) -> records` and `fetch(record) -> bytes`; the hashing,
   storage, metadata and logging layers are already source-agnostic (the
   `source` field is on every record, and bucket keys are namespaced).
2. **Airflow dynamic task mapping** instead of two tasks: one mapped task
   per (source, month) with per-partition retries, backfills via catchup, and
   per-partition observability; pools bound concurrency per source domain.
3. **Move the frontier out of process:** scrapy-redis (or equal) for a shared
   request queue so multiple workers can crawl one source, and per-domain
   politeness settings in config rather than code.
4. **Schema registry + validation** (e.g. pydantic models per source) at the
   landing boundary, and a dead-letter collection for records that fail
   validation, so one bad source can't poison the curated zone.
5. Swap MinIO/Mongo endpoints for managed S3/Atlas via the same env vars —
   no code change required.
