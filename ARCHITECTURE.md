# Architecture

```
                 ┌──────────────────────── Dagster job: wrc_pipeline ────────────────────────┐
                 │                                                                           │
workplacerelations.ie ─► scrape_landing_zone ─► transform_to_curated ─► enrich_decisions
  (per body × month)       Scrapy subprocess       HTML → content-only     structured fields
                           Mongo + MinIO           Mongo + MinIO           Mongo
                           (append-only landing)   (latest-only curated)   (enriched)
```

## Partition size (default: monthly, configurable)

The site holds ~63k records over ~30 years — roughly 100–300 per month per
body in recent years. At the site's fixed 10 results/page, a monthly window
per body is ~5–30 listing pages. That balances three forces: **failure
isolation** (a failed partition re-runs in seconds, not hours — retries
belong at the smallest safe boundary), **request amortisation** (daily
windows would multiply search requests ~30× for the same records), and
**pagination depth** (a yearly window on a dense body would page very deep,
where one mid-crawl failure costs the most). `(partition × body)` is also the
unit of reconciliation (`found = succeeded + unchanged + failed` is asserted
per unit) and matches the incremental schedule's monthly cadence.
`PARTITION_SIZE` is env-configurable (`daily`/`weekly`/`monthly`); every
record carries `partition_date`.

## Retries and rate limiting

- **AutoThrottle** is the primary rate controller — it adapts delay and
  concurrency to *observed* server latency (start 0.5 s, ceiling 15 s,
  target concurrency 4), which is "as fast as possible without getting
  blocked" by measurement rather than guesswork. Hard bounds back it up:
  4 concurrent requests per domain, 0.25 s base delay.
- **RetryMiddleware** (`RETRY_TIMES=4`) retries *transient* failures only —
  408/429/5xx and network errors, with a 60 s download timeout. Terminal 4xx
  and validation failures are recorded immediately and never retried:
  retrying a deterministic failure only hides it.
- The retry unit is the **request**, never the partition or run. What still
  fails after retries is captured with URL, stage, error class, and HTTP
  status in the JSON logs and the persisted run summary — failures are
  accounted for, never absorbed.
- `robots.txt` is obeyed, cookies are off (the flow is stateless GET), and
  the User-Agent is truthful. Conditional requests (`If-None-Match` on stored
  ETags) cut repeat-run load to a header exchange where the server supports it.

## Deduplication strategy

Three independent layers, so no single mechanism is load-bearing:

1. **Structural** — a unique Mongo index on the natural key
   `(source, body, identifier)` (plus `content_hash` on the immutable version
   collection) with atomic upserts: a rerun *cannot* create duplicates. The
   full tuple, not `identifier` alone, stays correct across sources that
   reuse reference numbers.
2. **Change detection** — every file is SHA-256-hashed over its exact bytes.
   Same hash → record marked `unchanged`, no re-upload, only `last_seen_at`
   advances. New hash → a new immutable landing version is *appended*;
   history is never overwritten.
3. **Content-addressed object keys** —
   `body=…/partition=…/<identifier>/<hash>.<ext>`: identical bytes map to the
   same key (re-upload is a no-op), changed bytes get a new key, and landing
   stays append-only by construction.

Honest trade-off: the listing exposes no content hash, so detecting an edited
document requires re-fetching it. The default re-fetches and hash-compares
(ETag/304 makes this near-free for static documents);
`SKIP_EXISTING_IDENTIFIERS=true` skips known identifiers entirely, trading
in-place-edit detection for speed on large backfills.

## Scaling to 50+ sources

1. **Source-plugin contract** — each source implements `discover(partition)`
   and `fetch(record)`; hashing, storage keys, metadata, idempotency, and
   logging are already source-agnostic (`source` is part of every natural key).
2. **Dagster partitioned jobs** — one partition per (source, month) with
   per-partition retries, backfills, alerting, and per-source concurrency
   caps, instead of one monolithic run.
3. **Distributed frontier** — scrapy-redis (or equivalent) to share the URL
   queue across workers; politeness stays per-domain in config, so scaling
   workers never scales pressure on any single site.
4. **Landing-boundary validation + dead-letter queue** so one malformed
   source degrades alone instead of poisoning the curated zone.
5. **Managed backends** — MinIO/Mongo swap for S3/Atlas via the existing env
   vars (same S3 API, same driver); no code change.
