# WRC Legal Documents Pipeline

Production-shaped scraping pipeline for decisions from
[workplacerelations.ie](https://www.workplacerelations.ie/en/search/?advance=true):
**Scrapy** ingests metadata + documents into an immutable **landing zone**
(MongoDB + MinIO), a **transformation** step produces a clean curated zone,
and an **enrichment** step extracts structured business fields — all
orchestrated as one dependent **Dagster** job:

```
scrape_landing_zone  >>  transform_to_curated  >>  enrich_decisions
      (Scrapy)              (BeautifulSoup)         (structured fields)
```

```
wrc-pipeline/
├── config/                 # env-driven settings + shared utils (logging, hashing, partitioning)
├── scraper/                # Scrapy project (spider + pipelines)
├── transform/              # transform.py (landing → curated), enrich.py (curated → enriched)
├── orchestration/          # Dagster job + monthly schedule
├── docker-compose.yml      # MongoDB + MinIO + mongo-express + bucket bootstrap
├── .env.example            # every configurable value (no hardcoded values anywhere)
├── ARCHITECTURE.md         # partition sizing, retries, dedup, scaling to 50+ sources
└── docs/                   # data model, recon evidence, acceptance matrix, performance notes
```

## Storage choices

- **MongoDB** (NoSQL DB) — decision records are semi-structured and vary by
  body/era; document storage fits without schema migrations. Unique indexes +
  atomic upserts give the idempotency guarantee at the database level.
- **MinIO** (object storage) — S3-compatible blob store running locally in
  Docker. The pipeline talks to it through `boto3` with the standard S3 API,
  so the same code deploys against AWS S3/GCS unchanged — local dev
  convenience without a proprietary storage layer.

Two zones, two buckets: `wrc-landing` (raw, append-only, versioned by content
hash — never mutated or deleted) and `wrc-curated` (cleaned, latest-only,
files renamed to `identifier.ext`).

## 1. Prerequisites

- Python **3.12+**
- **Docker** + docker compose

## 2. Install

```bash
git clone https://github.com/MohammedHydr/wrc-pipeline.git && cd wrc-pipeline

python -m venv .venv
source .venv/bin/activate        # Windows PowerShell: .venv\Scripts\Activate.ps1

pip install -r requirements.txt
```

## 3. Configure

```bash
cp .env.example .env             # Windows: copy .env.example .env
```

Every connection string, bucket name, partition size, delay, retry count, and
selector lives in `.env` (fully commented). Defaults work out of the box.

## 4. Start the infrastructure

```bash
docker compose up -d
```

This starts MongoDB, MinIO (buckets auto-created by a one-shot init
container), and a Mongo web UI:

| Service | URL | Default credentials |
|---|---|---|
| MinIO console | http://localhost:9001 | `minioadmin` / `minioadmin` |
| mongo-express | http://localhost:8081 | `admin` / `admin` |
| MongoDB (for Compass) | `mongodb://root:example@localhost:27018/wrc?authSource=admin&directConnection=true` | — |

## 5. Run via Dagster

```bash
# from the repo root, venv active
export PYTHONPATH=$PWD                    # PowerShell: $env:PYTHONPATH = $PWD
export DAGSTER_HOME=$PWD/.dagster_home    # optional — persists run history

dagster dev -m orchestration.wrc_dagster.definitions
```

Open **http://localhost:3000** → job **`wrc_pipeline`**. The job is
**monthly-partitioned** (grid starts at `DAGSTER_PARTITION_START_DATE`):
pick a partition in the **Launchpad** and its run config fills in
automatically, or use **Partitions → Launch backfill** to run a range of
months — each month becomes its own independently retryable run (one Scrapy
subprocess per month). For ad-hoc ranges or body filters, paste a run config
instead (examples below). Only the first op takes config; the date window
flows to transform and enrich automatically.

**Standard run — one month, all four bodies:**

```yaml
ops:
  scrape_landing_zone:
    config:
      start_date: "2024-01-01"
      end_date: "2024-01-31"
      partition: "monthly"
      bodies: ""                # empty = all four bodies from .env
```

**Backfill — six months, selected bodies:**

```yaml
ops:
  scrape_landing_zone:
    config:
      start_date: "2024-01-01"
      end_date: "2024-06-30"
      partition: "monthly"
      bodies: "Labour Court,Workplace Relations Commission"
```

**PDF-era run — legacy documents stored byte-for-byte:**

```yaml
ops:
  scrape_landing_zone:
    config:
      start_date: "2009-07-01"
      end_date: "2009-07-31"
      partition: "monthly"
      bodies: "Employment Appeals Tribunal"
```

Valid `partition`: `daily` | `weekly` | `monthly`. Valid `bodies` (comma-
separated, exact names): `Employment Appeals Tribunal`, `Equality Tribunal`,
`Labour Court`, `Workplace Relations Commission`. Config is validated before
Scrapy launches; a bad range fails fast with a clear message.

A built-in schedule **`wrc_monthly_incremental`** (06:00 on the 2nd,
Europe/Dublin) re-runs the job for the previous calendar month — idempotency
makes the recurring rerun safe by construction, turning the backfill into a
living feed.

### Which dates return PDFs — and why

Measured during recon ([docs/recon/wrc-search.md](docs/recon/wrc-search.md)):
the modern site renders every decision as a dynamic HTML page. Original
decision PDFs exist only in the earliest legacy imports, embedded *inside*
the case page:

| Body / era | Document type |
|---|---|
| Equality Tribunal ~2000–2003 | **PDF** embedded in the case page |
| Employment Appeals Tribunal (legacy imports) | **PDF** embedded in the case page |
| Equality Tribunal / EAT ~2010+, Labour Court, WRC (all years) | HTML only |

The spider fetches the case page and, when it detects an embedded decision
PDF (identifier-matched; site-chrome PDFs excluded), follows it and stores
the PDF **byte-for-byte** as the authoritative artifact (`FOLLOW_EMBEDDED_PDF`,
default on). Modern pages are stored as raw `.html` in landing and cleaned
only in curated.

## 6. What each op does

**`scrape_landing_zone`** — for every *body × partition*, queries the site's
GET search endpoint (discovered via recon — the ASP.NET advanced-search form
302-redirects to a plain GET, so no VIEWSTATE postbacks and no browser),
paginates, and for each record: writes metadata (`identifier`, `title`,
`description`, `published_date`, `doc_url`, `body`, `partition_date`, …) to
Mongo, downloads the document (PDF/DOC verbatim, HTML raw), computes its
**SHA-256 `file_hash`**, uploads it to
`s3://wrc-landing/body=…/partition=…/<identifier>/<hash>.<ext>`, and stores
`file_path` + `file_hash` on the record. Runs Scrapy in a subprocess so the
Twisted reactor never conflicts with Dagster.

**`transform_to_curated`** — PDF/DOC bytes pass through untouched (streamed,
never fully loaded in memory); HTML is reduced to the decision content only
(nav/header/footer/scripts stripped via BeautifulSoup) and **re-hashed**. All
files are renamed to `identifier.ext` and written to
`s3://wrc-curated/…`; curated metadata (new path, new hash, lineage to the
exact source object) goes to the `curated_documents` collection. The landing
zone is never touched.

**`enrich_decisions`** — the business layer, its own section below (§7).

Each op fails loudly: any failed transform/enrich record raises a visible
`dg.Failure` in the Dagster UI, and per-op metadata shows selected /
processed / skipped-unchanged / failed counts.

## 7. The enrichment layer (beyond the brief)

A third stage, **not required by the assessment**: deterministic
BeautifulSoup/regex extraction (no ML — every value is traceable to the text
that produced it) turning each curated HTML decision into a structured,
queryable record in `enriched_decisions`:

| Field(s) | What it captures | Why it is useful |
|---|---|---|
| `complainant`, `respondent`, `is_anonymised` | The parties, plus whether the decision is anonymised | Search by employer/employee; track repeat respondents; anonymisation-practice analysis |
| `acts_cited`, `sections_cited`, `practice_areas` | Statutes and exact sections cited, mapped to a practice-area taxonomy | Filter case law by legislation (e.g. Unfair Dismissals Act) or area (discrimination, working time, pay) |
| `cited_decisions`, `complaint_references` | Cross-references to other decisions and complaint numbers | A citation graph — find precedents and how often a decision is relied upon |
| `adjudication_officer`, `decision_type` | Who decided, and the decision category | Per-officer analytics; separates decisions from recommendations |
| `hearing_date`, `received_date`, `days_to_decision` | The case timeline | Measurable processing times — how long a complaint really takes |
| `award_amounts_eur`, `award_max_eur` | Every euro amount awarded, and the maximum | Compensation benchmarking: typical award for a given act/area/year |
| `outcome`, `outcome_signals` | Derived result: `upheld` / `not_upheld` / `mixed`, with the trigger phrases kept as evidence | Success-rate statistics by act, body, officer, or representation |
| `self_represented` | Whether the complainant appeared without representation | Does representation correlate with outcomes/awards? |
| `paragraphs`, `paragraph_count`, `text_length` | The decision body split into clean paragraphs | Ready-made chunks for full-text search, RAG/embedding pipelines, and summarisation — no re-parsing needed downstream |

**The `paragraphs` field** deserves emphasis: the cleaned HTML is walked at
block level (`p`, `li`, table cells, headings, …) with scripts/styles
removed and whitespace normalised, so each entry is one coherent unit of the
decision text. Everything else in the table above is extracted *from* these
paragraphs — and downstream consumers (search index, embeddings, an LLM
summariser) get analysis-ready text without ever parsing HTML again.

Each record also carries the core metadata forward (`title`, `description`,
`published_date`, `partition_date`, `doc_url`), the parties being parsed from
the record's description. Full lineage (`source_file_path`,
`source_file_hash`, `extraction_version`, `run_id`, `enriched_at`) makes any
field auditable back to the exact curated artifact it came from. Legacy
binary scans (PDF-era decisions) are recorded as
`extraction_status: binary_source` instead of being silently dropped, so text
coverage stays measurable. The step is idempotent — unchanged source hash +
same `extraction_version` → skip — and bumping the extraction version
re-enriches everything automatically on the next run.

The collection is indexed for the queries it exists to serve: a unique index
on the natural key `(source, body, identifier)` plus secondary indexes on
`partition_date`, `acts_cited`, `adjudication_officer`, `practice_areas`,
`cited_decisions`, and `outcome`.

**Business value:** this is the layer a legal-tech product is built on — case
search by party/act/outcome, award benchmarking, officer and timeline
analytics, a precedent citation graph, and LLM-ready paragraph chunks —
without any consumer ever touching raw HTML. Example queries:

```bash
docker exec -it wrc-mongo mongosh -u root -p example
> use wrc
// top 5 most-cited acts
> db.enriched_decisions.aggregate([{$unwind:'$acts_cited'},{$group:{_id:'$acts_cited',n:{$sum:1}}},{$sort:{n:-1}},{$limit:5}])
// how many decisions award money, and the average award
> db.enriched_decisions.aggregate([{$match:{award_max_eur:{$gt:0}}},{$group:{_id:null,n:{$sum:1},avg:{$avg:'$award_max_eur'}}}])
// success rate of upheld outcomes
> db.enriched_decisions.aggregate([{$group:{_id:'$outcome',n:{$sum:1}}}])
```

## 8. Idempotency — verify it yourself

Launch the same Launchpad config **twice**. The second run must report all
records as *unchanged/skipped*: no new Mongo documents (unique index on
`(source, body, identifier, content_hash)`), no re-uploaded objects
(hash-addressed keys), no curated rewrites (source hash unchanged → skip).

Extra: known documents are re-checked with conditional requests
(`If-None-Match` + stored ETag) — an unchanged legacy PDF answers **304 with
zero body bytes** re-downloaded.

```bash
docker exec -it wrc-mongo mongosh -u root -p example
> use wrc
> db.landing_document_versions.countDocuments()        // identical after rerun
> db.run_logs.find().sort({finished_at:-1}).limit(1)   // per-run summary: every found record in one terminal bucket, unaccounted = 0
> db.failed_documents.find({status:"pending"})         // dead-letter ledger; replay = re-run the partition
```

## 9. Logging

All logs are structured JSON and carry `run_id`, `partition`, `body`,
`identifier`/URL, event, and outcome. Every run ends with a reconciliation
summary (records found vs succeeded vs unchanged vs failed); every failure is
logged with URL, stage, error class, and HTTP status — and the summary is
also persisted to the `run_logs` collection.

## 10. Standalone (without Dagster)

```bash
export PYTHONPATH=$PWD
cd scraper && scrapy crawl wrc -a start_date=2024-01-01 -a end_date=2024-01-31
cd ..
python -m transform.transform --start-date 2024-01-01 --end-date 2024-01-31
python -m transform.enrich    --start-date 2024-01-01 --end-date 2024-01-31
```

## 11. Tests

```bash
pip install -r requirements-dev.txt   # pytest, ruff, mypy
pytest tests/ -q
```

- **Unit** — partitioning, HTML extraction, hashing, object keys, spider
  parsing against live-captured fixtures. No network, no DB.
- **Integration** (`tests/test_integration_idempotency.py`) — real pipelines
  against the compose stack in a throwaway DB/bucket, proving the rerun
  contract. Auto-skips when the stack is down.
- **Live smoke** (opt-in) — `WRC_LIVE_SMOKE=1 pytest tests/test_live_smoke.py`.
- **CI** (`.github/workflows/ci.yml`) — every push runs
  `ruff format --check`, `ruff check`, `mypy`, and the full test suite with
  MongoDB + MinIO up, so the integration idempotency proof runs on every
  commit. The live-site smoke test stays opt-in and out of CI.

## 12. Beyond the brief — notable extras

- **Automation** — the pipeline is not just runnable, it is *scheduled*: the
  `wrc_monthly_incremental` Dagster schedule fires at 06:00 on the 2nd of
  each month (Europe/Dublin, after the previous month is complete) and runs
  the full scrape → transform → enrich job for that month. Because every
  layer is idempotent, the recurring rerun is safe by construction — the
  one-off backfill becomes a self-updating dataset with zero manual steps.
- **Recon before code** — the search contract (GET endpoint, body codes,
  pagination, selectors, PDF locations) was measured and documented in
  `docs/recon/` instead of guessed; the spider needs no browser at all —
  the fastest polite way to scrape this site.
- **Versioned, append-only landing** — content-hash-addressed objects and an
  immutable `landing_document_versions` collection with a mutable
  `document_state` "latest" pointer: changed documents append a new version,
  history is never lost.
- **Conditional re-fetches** — stored ETags replayed as `If-None-Match`;
  unchanged documents cost a header exchange and **zero body bytes**. A
  `SKIP_EXISTING_IDENTIFIERS` toggle adds an even faster incremental mode
  that skips known identifiers entirely.
- **Run reconciliation as an invariant** — per (partition, body), every
  found record must land in exactly one terminal bucket (scraped — with
  `unchanged` as its subset — failed, parse-failed, skipped, listing-at-risk)
  and a cell is complete only when `unaccounted = 0`; the accounting is
  persisted to `run_logs`, so missing records can't hide.
- **Enrichment layer** — a third pipeline stage extracting structured
  business fields (parties, acts cited, awards, outcomes, citation graph),
  not required by the assessment.
- **Bounded-memory transform** — large PDFs stream S3 → spool → S3 while
  hashing in 1 MiB chunks; memory stays flat regardless of document size.
- **Subprocess-isolated Scrapy** — the Twisted reactor can't restart inside
  a long-lived process, so the Dagster op runs Scrapy as a subprocess and
  streams its logs into the Dagster UI.
- **Evidence-first documentation** — `docs/acceptance-matrix.md` maps every
  requirement to its code path and proof; `docs/data-model.md` documents
  every collection, field, and index.
- **Politeness** — AutoThrottle, bounded per-domain concurrency, jittered
  delays, retry middleware, truthful User-Agent. No stealth, no evasion.

## 13. Screenshots

**Dagster — `wrc_pipeline` run (scrape → transform → enrich):**

![Dagster DAG run](docs/screenshots/dagster-dag.png)

**MinIO — landing and curated buckets:**

![MinIO buckets](docs/screenshots/minio-buckets.png)

**MongoDB Compass — collections and a sample record:**

![MongoDB Compass](docs/screenshots/compass-collections.png)

## Further reading

- [ARCHITECTURE.md](ARCHITECTURE.md) — partition sizing, retries/rate
  limiting, deduplication, scaling to 50+ sources (1 page).
- [docs/data-model.md](docs/data-model.md) — every collection, field, index.
- [docs/acceptance-matrix.md](docs/acceptance-matrix.md) — requirement →
  evidence mapping.
- [docs/recon/wrc-search.md](docs/recon/wrc-search.md) — how the site
  actually works, measured.
