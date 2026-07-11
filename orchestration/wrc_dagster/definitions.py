"""Dagster orchestration: scrape_landing_zone >> transform_to_curated >>
enrich_decisions, plus a monthly schedule (`wrc_monthly_incremental`) that
turns the backfill into a living feed. Mongo/MinIO come from docker compose;
Dagster, Scrapy and the transforms run natively."""

import os
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

import dagster as dg

from config.common import parse_cli_date
from transform.enrich import run_enrichment
from transform.transform import run_transformation


# wrc_dagster / orchestration / <repo root>
REPO_ROOT = Path(__file__).resolve().parents[2]
SCRAPER_DIR = REPO_ROOT / "scraper"


class WrcPipelineConfig(dg.Config):
    """Configuration exposed in the Dagster Launchpad."""

    start_date: str = "2024-01-01"
    end_date: str = "2024-01-31"
    partition: str = "monthly"

    # Empty means use all bodies configured in .env.
    bodies: str = ""


def _validate_config(config: WrcPipelineConfig) -> None:
    start = parse_cli_date(config.start_date)
    end = parse_cli_date(config.end_date)

    if start > end:
        raise dg.Failure(
            description=(
                f"start_date {config.start_date!r} must not be after "
                f"end_date {config.end_date!r}"
            )
        )

    allowed_partitions = {"daily", "weekly", "monthly"}

    if config.partition not in allowed_partitions:
        raise dg.Failure(
            description=(
                f"Unsupported partition {config.partition!r}. "
                f"Expected one of {sorted(allowed_partitions)}."
            )
        )


def _subprocess_environment() -> dict[str, str]:
    """Create the environment inherited by the Scrapy subprocess."""

    env = os.environ.copy()

    existing_pythonpath = env.get("PYTHONPATH")

    if existing_pythonpath:
        env["PYTHONPATH"] = f"{REPO_ROOT}{os.pathsep}{existing_pythonpath}"
    else:
        env["PYTHONPATH"] = str(REPO_ROOT)

    # Ensure predictable output when logs contain Unicode.
    env["PYTHONUTF8"] = "1"

    return env


@dg.op(
    name="scrape_landing_zone",
    description=(
        "Run the WRC Scrapy spider and persist immutable landing "
        "versions, current document state, and raw MinIO objects."
    ),
)
def scrape_landing_zone(
    context: dg.OpExecutionContext,
    config: WrcPipelineConfig,
) -> dict[str, str]:
    """Run Scrapy in a subprocess — the Twisted reactor can't restart inside
    a long-lived Dagster process."""

    _validate_config(config)

    command = [
        sys.executable,
        "-m",
        "scrapy",
        "crawl",
        "wrc",
        "-a",
        f"start_date={config.start_date}",
        "-a",
        f"end_date={config.end_date}",
        "-a",
        f"partition={config.partition}",
    ]

    if config.bodies.strip():
        command.extend(
            [
                "-a",
                f"bodies={config.bodies.strip()}",
            ]
        )

    context.log.info(
        "Starting Scrapy command: %s",
        subprocess.list2cmdline(command),
    )

    process = subprocess.Popen(
        command,
        cwd=str(SCRAPER_DIR),
        env=_subprocess_environment(),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )

    if process.stdout is None:
        process.kill()
        raise dg.Failure(description="Could not capture the Scrapy process output.")

    for line in process.stdout:
        clean_line = line.rstrip()

        if clean_line:
            context.log.info(clean_line)

    return_code = process.wait()

    if return_code != 0:
        raise dg.Failure(
            description=(
                f"Scrapy landing-zone task failed with exit code {return_code}."
            )
        )

    context.add_output_metadata(
        {
            "start_date": config.start_date,
            "end_date": config.end_date,
            "partition": config.partition,
            "bodies": config.bodies or "configured defaults",
            "scrapy_exit_code": return_code,
        }
    )

    return {
        "start_date": config.start_date,
        "end_date": config.end_date,
    }


@dg.op(
    name="transform_to_curated",
    description=(
        "Transform the current immutable landing versions and store "
        "normalized files and metadata in the curated zone."
    ),
)
def transform_to_curated(
    context: dg.OpExecutionContext,
    date_window: dict[str, str],
) -> dict:
    """Run the existing landing-to-curated transformation."""

    start_date = date_window["start_date"]
    end_date = date_window["end_date"]

    context.log.info(
        "Starting transformation for %s through %s",
        start_date,
        end_date,
    )

    stats = run_transformation(
        start_date,
        end_date,
        configure_logging=False,
    )

    failures = stats.get("failed", [])

    context.add_output_metadata(
        {
            "selected": stats.get("selected", 0),
            "transformed": stats.get("transformed", 0),
            "skipped_unchanged": stats.get(
                "skipped_unchanged",
                0,
            ),
            "failed": len(failures),
            "transformation_run_id": stats.get("run_id", ""),
        }
    )

    if failures:
        raise dg.Failure(
            description=(f"{len(failures)} transformation record(s) failed: {failures}")
        )

    context.log.info(
        "Transformation finished: selected=%s transformed=%s "
        "skipped_unchanged=%s failed=%s",
        stats.get("selected", 0),
        stats.get("transformed", 0),
        stats.get("skipped_unchanged", 0),
        len(failures),
    )

    # Carry the date window forward so downstream ops share the same range.
    return {**stats, "start_date": start_date, "end_date": end_date}


@dg.op(
    name="enrich_decisions",
    description=(
        "Extract structured business fields (parties, acts cited, officer, "
        "hearing date, award amounts, outcome signals) from curated HTML "
        "decisions into the enriched collection."
    ),
)
def enrich_decisions(
    context: dg.OpExecutionContext,
    transform_stats: dict,
) -> dict:
    """Run the curated-to-enriched structured extraction."""

    start_date = transform_stats["start_date"]
    end_date = transform_stats["end_date"]

    context.log.info(
        "Starting enrichment for %s through %s",
        start_date,
        end_date,
    )

    stats = run_enrichment(
        start_date,
        end_date,
        configure_logging=False,
    )

    failures = stats.get("failed", [])

    context.add_output_metadata(
        {
            "selected": stats.get("selected", 0),
            "extracted": stats.get("extracted", 0),
            "binary_source": stats.get("binary_source", 0),
            "skipped_unchanged": stats.get("skipped_unchanged", 0),
            "failed": len(failures),
            "enrichment_run_id": stats.get("run_id", ""),
        }
    )

    if failures:
        raise dg.Failure(
            description=(f"{len(failures)} enrichment record(s) failed: {failures}")
        )

    return stats


@dg.job(
    description=(
        "Scrape WRC decisions into the immutable landing zone, transform "
        "their latest versions into the curated zone, then extract "
        "structured business fields into the enriched collection."
    )
)
def wrc_pipeline():
    enrich_decisions(transform_to_curated(scrape_landing_zone()))


@dg.schedule(
    cron_schedule="0 6 2 * *",  # 06:00 on the 2nd — previous month is complete
    job=wrc_pipeline,
    execution_timezone="Europe/Dublin",
)
def wrc_monthly_incremental(
    context: dg.ScheduleEvaluationContext,
) -> dg.RunRequest:
    """Scrape/transform/enrich the previous calendar month. Idempotency makes
    the recurring rerun safe: known records are observed, never duplicated."""
    today = context.scheduled_execution_time.date()
    prev_end = today.replace(day=1) - timedelta(days=1)
    prev_start = prev_end.replace(day=1)

    return dg.RunRequest(
        run_config={
            "ops": {
                "scrape_landing_zone": {
                    "config": {
                        "start_date": prev_start.isoformat(),
                        "end_date": prev_end.isoformat(),
                        "partition": "monthly",
                        "bodies": "",
                    }
                }
            }
        }
    )


defs = dg.Definitions(
    jobs=[wrc_pipeline],
    schedules=[wrc_monthly_incremental],
)
