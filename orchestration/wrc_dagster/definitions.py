"""Dagster orchestration for the WRC ingestion pipeline.

Execution order:

    scrape_landing_zone
            |
            v
    transform_to_curated

MongoDB and MinIO run through the existing Docker Compose stack.
Dagster, Scrapy, and the transformation run natively on Windows.
"""

import os
import subprocess
import sys
from pathlib import Path

import dagster as dg

from config.common import parse_cli_date
from transform.transform import run_transformation


# definitions.py:
# parents[0] = wrc_dagster
# parents[1] = orchestration
# parents[2] = repository root
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
        env["PYTHONPATH"] = (
            f"{REPO_ROOT}{os.pathsep}{existing_pythonpath}"
        )
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
    """Execute Scrapy in a subprocess.

    Subprocess isolation avoids Twisted reactor lifecycle problems and keeps
    Scrapy logging separate from the Dagster process.
    """

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
        raise dg.Failure(
            description="Could not capture the Scrapy process output."
        )

    for line in process.stdout:
        clean_line = line.rstrip()

        if clean_line:
            context.log.info(clean_line)

    return_code = process.wait()

    if return_code != 0:
        raise dg.Failure(
            description=(
                "Scrapy landing-zone task failed with exit code "
                f"{return_code}."
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
            description=(
                f"{len(failures)} transformation record(s) failed: "
                f"{failures}"
            )
        )

    context.log.info(
        "Transformation finished: selected=%s transformed=%s "
        "skipped_unchanged=%s failed=%s",
        stats.get("selected", 0),
        stats.get("transformed", 0),
        stats.get("skipped_unchanged", 0),
        len(failures),
    )

    return stats


@dg.job(
    description=(
        "Scrape WRC decisions into the immutable landing zone, "
        "then transform their latest versions into the curated zone."
    )
)
def wrc_pipeline():
    transform_to_curated(
        scrape_landing_zone()
    )


defs = dg.Definitions(
    jobs=[wrc_pipeline],
)