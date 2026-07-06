"""Shared utilities used by both the scraper and the transformation script:

* structured (JSON) logging setup
* date-partition generation
* file hashing
* Mongo / S3 client factories
"""

from __future__ import annotations

import hashlib
import logging
import sys
import uuid
from datetime import date, datetime, timedelta
from typing import Iterator, Optional, Tuple

import boto3
from botocore.config import Config as BotoConfig
from pymongo import MongoClient
from pythonjsonlogger import jsonlogger

from config.settings import Settings


# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #
def new_run_id() -> str:
    """Short unique id correlating every log line of one pipeline run."""
    return uuid.uuid4().hex[:12]


class _RunIdFilter(logging.Filter):
    """Injects the run_id into every log record so all JSON lines carry it."""

    def __init__(self, run_id: str) -> None:
        super().__init__()
        self.run_id = run_id

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "run_id"):
            record.run_id = self.run_id
        return True


def configure_json_logging(
    level: str = "INFO", run_id: Optional[str] = None
) -> logging.Logger:
    """Configure the root logger to emit one JSON object per line to stdout.

    Fields: timestamp, level, name, message, run_id + any `extra={...}` keys,
    which is how we attach partition / body / counters to every log record.
    """
    handler = logging.StreamHandler(sys.stdout)
    formatter = jsonlogger.JsonFormatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s",
        rename_fields={"asctime": "timestamp", "levelname": "level"},
    )
    handler.setFormatter(formatter)
    if run_id:
        handler.addFilter(_RunIdFilter(run_id))

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level.upper())
    return root


# --------------------------------------------------------------------------- #
# Date partitioning
# --------------------------------------------------------------------------- #
def _add_month(d: date) -> date:
    if d.month == 12:
        return date(d.year + 1, 1, 1)
    return date(d.year, d.month + 1, 1)


def iter_partitions(
    start: date, end: date, size: str = "monthly"
) -> Iterator[Tuple[date, date]]:
    """Yield inclusive `(window_start, window_end)` pairs covering [start, end].

    `size` is one of: daily | weekly | monthly. Monthly windows are aligned to
    calendar months (the first/last window may be partial), which makes
    partition_date values stable and human-readable across runs.
    """
    if start > end:
        raise ValueError(f"start ({start}) must be <= end ({end})")

    size = size.lower()
    cur = start
    while cur <= end:
        if size == "daily":
            win_end = cur
            nxt = cur + timedelta(days=1)
        elif size == "weekly":
            win_end = min(cur + timedelta(days=6), end)
            nxt = win_end + timedelta(days=1)
        elif size == "monthly":
            first_of_next = _add_month(date(cur.year, cur.month, 1))
            win_end = min(first_of_next - timedelta(days=1), end)
            nxt = win_end + timedelta(days=1)
        else:
            raise ValueError(f"Unknown partition size: {size!r}")
        yield cur, min(win_end, end)
        cur = nxt


def parse_cli_date(value: str) -> date:
    """Parse dates given as YYYY-MM-DD or DD-MM-YYYY (both accepted)."""
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    raise ValueError(
        f"Could not parse date {value!r}; expected YYYY-MM-DD or DD-MM-YYYY"
    )


# --------------------------------------------------------------------------- #
# Hashing
# --------------------------------------------------------------------------- #
def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# --------------------------------------------------------------------------- #
# Key normalisation
# --------------------------------------------------------------------------- #
def slug(value: str) -> str:
    """Lower-case, filesystem/object-key-safe form of a label (e.g. a body
    name) for use in deterministic storage-key prefixes. Source values are
    preserved verbatim in metadata; only keys are normalised."""
    return "".join(c.lower() if c.isalnum() else "-" for c in value).strip("-")


# --------------------------------------------------------------------------- #
# Client factories
# --------------------------------------------------------------------------- #
def get_mongo_client(settings: Settings) -> MongoClient:
    return MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=10_000)


def get_s3_client(settings: Settings):
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name=settings.s3_region,
        config=BotoConfig(
            s3={"addressing_style": "path"},
            retries={"max_attempts": 5, "mode": "standard"},
        ),
    )


def ensure_bucket(s3_client, bucket: str) -> None:
    """Create the bucket if it does not exist (idempotent)."""
    existing = {b["Name"] for b in s3_client.list_buckets().get("Buckets", [])}
    if bucket not in existing:
        s3_client.create_bucket(Bucket=bucket)
