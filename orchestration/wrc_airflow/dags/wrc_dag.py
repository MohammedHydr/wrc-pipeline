"""Airflow DAG for the WRC scraping pipeline.

Two tasks with an explicit dependency:

    scrape_landing_zone  --->  transform_to_curated

The scraper runs via BashOperator because Scrapy's Twisted reactor cannot be
restarted inside a long-lived Python process; subprocess isolation makes
repeated Airflow task reruns reliable.

The transformation is invoked in-process via PythonOperator (it is pure I/O
over Mongo + S3 with no reactor dependency).

Setup (one-time, run from the project root):
    export AIRFLOW_HOME=$(pwd)/airflow_home
    export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/orchestration/wrc_airflow/dags
    airflow db migrate
    airflow users create --username admin --firstname Admin --lastname User \\
        --role Admin --email admin@example.com --password admin

Run:
    airflow standalone            # starts scheduler + webserver at http://localhost:8080
    # or separately:
    airflow scheduler &
    airflow webserver --port 8080

Then open http://localhost:8080, find the `wrc_pipeline` DAG, click
"Trigger DAG" and supply run config, e.g.:
    {
        "run_start_date": "2024-01-01",
        "run_end_date":   "2024-03-31"
    }
"""
from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Path of this file: <REPO_ROOT>/orchestration/wrc_airflow/dags/wrc_dag.py
# parents[0]=dags  parents[1]=wrc_airflow  parents[2]=orchestration  parents[3]=REPO_ROOT
REPO_ROOT = Path(__file__).resolve().parents[3]
SCRAPER_DIR = REPO_ROOT / "scraper"

_ENV_FILE = REPO_ROOT / ".env"

# Merge .env values into the subprocess environment (pydantic-settings honours
# env-var precedence, so real env vars still win over the file values).
def _build_env() -> dict[str, str]:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(REPO_ROOT)
    if _ENV_FILE.exists():
        for line in _ENV_FILE.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            env.setdefault(key.strip(), val.strip())
    return env


with DAG(
    dag_id="wrc_pipeline",
    description="Scrape WRC decisions into the landing zone, then transform to the curated zone.",
    schedule=None,       # manual / API trigger only — no automatic scheduling
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["wrc", "legal", "scraping"],
    params={
        # Named run_start_date / run_end_date to avoid shadowing Airflow's own
        # DAG-level start_date / end_date properties.
        "run_start_date": Param(
            "2024-01-01",
            type="string",
            description="Inclusive start date of the scrape/transform window (YYYY-MM-DD)",
        ),
        "run_end_date": Param(
            "2024-03-31",
            type="string",
            description="Inclusive end date of the scrape/transform window (YYYY-MM-DD)",
        ),
        "partition": Param(
            "",
            type="string",
            description="Override PARTITION_SIZE env var: monthly | weekly | daily (empty = use env default)",
        ),
    },
) as dag:

    # ---------------------------------------------------------------------- #
    # Task 1: scrape the landing zone
    # ---------------------------------------------------------------------- #
    scrape_landing_zone = BashOperator(
        task_id="scrape_landing_zone",
        # Jinja templating is supported natively in bash_command.
        # The {% if %} block appends the partition override only when supplied.
        bash_command=(
            f"cd {SCRAPER_DIR} && scrapy crawl wrc "
            "-a start_date={{ params.run_start_date }} "
            "-a end_date={{ params.run_end_date }}"
            "{% if params.partition %} -a partition={{ params.partition }}{% endif %}"
        ),
        env=_build_env(),
        doc_md=(
            "Run the Scrapy WRC spider for the configured date range. "
            "Writes raw document files to MinIO (landing bucket) and upserts "
            "metadata into MongoDB (landing_documents collection)."
        ),
    )

    # ---------------------------------------------------------------------- #
    # Task 2: transform landing → curated
    # ---------------------------------------------------------------------- #
    def _run_transform(**context) -> None:
        """Import and call run_transformation in-process."""
        params = context["params"]
        sys.path.insert(0, str(REPO_ROOT))
        from transform.transform import run_transformation  # noqa: PLC0415
        stats = run_transformation(
            params["run_start_date"],
            params["run_end_date"],
        )
        failed = stats.get("failed", [])
        if failed:
            # Surface failures as an Airflow task failure so they appear in the
            # UI and trigger retries / alerts according to DAG defaults.
            raise RuntimeError(
                f"{len(failed)} record(s) failed transformation: {failed}"
            )

    transform_to_curated = PythonOperator(
        task_id="transform_to_curated",
        python_callable=_run_transform,
        doc_md=(
            "For every landing record in the date window: pass through PDF/DOC "
            "files unchanged; strip boilerplate from HTML files via BeautifulSoup; "
            "rename all files to <identifier>.<ext>; upload to the curated MinIO "
            "bucket; upsert curated metadata into MongoDB (curated_documents)."
        ),
    )

    # Explicit dependency: transform only runs after scrape succeeds.
    scrape_landing_zone >> transform_to_curated
