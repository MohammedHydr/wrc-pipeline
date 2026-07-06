# WRC Legal Documents Scraping Pipeline

Scrapes decisions and determinations (metadata + documents) from
[workplacerelations.ie](https://www.workplacerelations.ie/en/search/?advance=true)
using **Scrapy**, stores metadata in **MongoDB** and files in **MinIO**
(S3-compatible object storage), then runs a **transformation** step into a
curated zone. Orchestrated with **Airflow**.

```
wrc-pipeline/
├── config/                 # env-driven settings + shared utils (logging, hashing, partitioning)
├── scraper/                # Scrapy project (spider + pipelines)
├── transform/              # landing -> curated transformation script
├── orchestration/          # Airflow DAG (scrape -> transform)
├── docker-compose.yml      # MongoDB + MinIO (+ bucket bootstrap)
├── .env.example            # every configurable value
├── ARCHITECTURE.md
└── README.md
```

## 1. Prerequisites

* Python 3.10+
* Docker + docker compose

## 2. Setup

```bash
git clone <this-repo> && cd wrc-pipeline

python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env          # adjust if needed — no values are hardcoded

docker compose up -d          # starts MongoDB (27017) + MinIO (9000/9001)
                              # and creates the two buckets
```

MinIO console: http://localhost:9001 (minioadmin / minioadmin by default).

## 3. Run the scraper (standalone)

```bash
export PYTHONPATH=$PWD        # so the scraper can import config/
cd scraper
scrapy crawl wrc -a start_date=2024-01-01 -a end_date=2024-03-31
# optional overrides:
#   -a partition=weekly            (default from PARTITION_SIZE env)
#   -a bodies="Labour Court"       (default: all four bodies)
```

What happens:

* the date range is split into partitions (monthly by default) and, for each
  **body × partition**, the advanced-search form is submitted and paginated;
* every record's metadata (`identifier`, `title`, `description`,
  `published_date`, `doc_url`, `body`, `partition_date`, …) is upserted into
  `wrc.landing_documents` (unique index on `identifier` — no duplicates);
* the linked document is downloaded — PDF/DOC files verbatim, HTML pages as
  `.html` — hashed (sha256), and uploaded to `s3://wrc-landing/raw/...`;
  the resulting `file_path` and `file_hash` are stored on the metadata record;
* re-running the same range is **idempotent**: unchanged files (same hash)
  are not re-uploaded and records are updated in place, never duplicated;
* logs are JSON lines including the current partition, body, records
  found vs scraped, failed downloads (URL + error/status), and a final run
  summary (also persisted to `wrc.run_logs`).

## 4. Run the transformation (standalone)

```bash
cd <repo root>
python -m transform.transform --start-date 2024-01-01 --end-date 2024-03-31
```

* PDF/DOC files pass through untouched; HTML files are reduced to the
  relevant decision content (nav/header/footer/scripts stripped) via
  BeautifulSoup and re-hashed;
* every file is renamed to `<identifier>.<ext>` and written to
  `s3://wrc-curated/curated/`;
* curated metadata (new `file_path`, new `file_hash`, plus lineage to the
  source object/hash) is upserted into `wrc.curated_documents`;
* the landing zone is never modified; the step is idempotent (records whose
  source hash hasn't changed are skipped).

## 5. Run via Airflow (recommended)

**One-time setup** (run once after `pip install -r requirements.txt`):

```bash
export AIRFLOW_HOME=$(pwd)/airflow_home
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/orchestration/wrc_airflow/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export PYTHONPATH=$PWD

airflow db migrate
airflow users create \
    --username admin --firstname Admin --lastname User \
    --role Admin --email admin@example.com --password admin
```

**Start Airflow:**

```bash
export AIRFLOW_HOME=$(pwd)/airflow_home
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/orchestration/wrc_airflow/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export PYTHONPATH=$PWD
airflow standalone        # scheduler + webserver in one process
```

Open http://localhost:8080 (admin / admin), find the **wrc_pipeline** DAG,
click **Trigger DAG w/ config** and supply:

```json
{
    "run_start_date": "2024-01-01",
    "run_end_date":   "2024-03-31"
}
```

The `transform_to_curated` task only starts after `scrape_landing_zone`
succeeds — dependency is explicit in the DAG (`scrape >> transform`). Failed
transformation records raise a task error visible in the Airflow UI.

## 6. Configuration

Everything (connection strings, buckets, partition size, concurrency, delays,
retries, user agent, selectors, …) is set in `.env` — see `.env.example` for
the full list with comments. No hardcoded values.

Two toggles worth knowing:

* `SKIP_EXISTING_IDENTIFIERS=true` — fast incremental mode: identifiers
  already in Mongo are not re-downloaded at all (skips change detection).
* `HTML_CONTENT_SELECTORS` — ordered CSS selectors used to isolate the
  relevant content region of decision pages.

## 7. How the search works (verified via recon)

The advanced-search form is ASP.NET, but it redirects to a plain **GET**
endpoint, which the spider queries directly (no VIEWSTATE postbacks):

```
/en/search/?decisions=1&from=DD/MM/YYYY&to=DD/MM/YYYY&body=<code>&pageNumber=<n>
```

Body codes (from the checkbox `value` attributes) live in `WRC_BODY_CODES`:
Equality Tribunal=1, Employment Appeals Tribunal=2, Labour Court=3,
Workplace Relations Commission=15376. Result cards are `li.each-item`
(`span.refNO` = id, `h2.title a` = title/link, `span.date`,
`p.description@title`). Full details in
[docs/recon/wrc-search.md](docs/recon/wrc-search.md).

To re-verify against the live site if the markup ever changes:

```bash
cd scraper
export PYTHONPATH=..
scrapy shell "https://www.workplacerelations.ie/en/search/?decisions=1&from=01/01/2024&to=31/01/2024&body=3" -s "ITEM_PIPELINES={}"
>>> response.xpath("normalize-space(//*[contains(text(),'results')])").get()
>>> response.css("li.each-item span.refNO::text").getall()[:3]
```

## 8. Inspecting results

Collection schemas, field types, and indexes are documented in
[docs/data-model.md](docs/data-model.md). Quick look:

```bash
docker exec -it wrc-mongo mongosh -u root -p example
> use wrc
> db.landing_documents.countDocuments()
> db.landing_documents.findOne()
> db.landing_documents.getIndexes()          // shows the (source, body, identifier) unique index
> db.curated_documents.findOne()
> db.run_logs.find().sort({finished_at:-1}).limit(1)   // latest run summary
```

## 9. Tests

```bash
pip install -r requirements-dev.txt   # pytest, ruff, mypy
pytest tests/ -q
```

Covers date partitioning, HTML content extraction and hashing/idempotency
key logic (the pure-Python parts that don't need the live site).
