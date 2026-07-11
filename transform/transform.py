"""Landing -> curated transformation.

Fetches landing metadata for a date range, cleans HTML down to the decision
content (binaries pass through), renames every file to <identifier>.<ext>,
and writes objects + metadata to the curated bucket/collection. Landing is
never modified; reruns skip records whose source hash hasn't changed.

Usage:
    python -m transform.transform --start-date 2024-01-01 --end-date 2024-06-30
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import IO, Any, Iterator

from pymongo import ASCENDING

from config.common import (
    configure_json_logging,
    ensure_bucket,
    get_mongo_client,
    get_s3_client,
    new_run_id,
    parse_cli_date,
    safe_identifier,
    sha256_bytes,
    slug,
)
from config.settings import get_settings
from config.html_utils import canonicalize_html

logger = logging.getLogger("transform")

PASSTHROUGH_EXTS = {"pdf", "doc", "docx", "rtf"}

# Bump on cleaner/naming changes: stale curated records re-derive next run.
PARSER_VERSION = "1.2"


def run_transformation(
    start_date: str,
    end_date: str,
    *,
    configure_logging: bool = True,
) -> dict:
    cfg = get_settings()
    run_id = new_run_id()
    if configure_logging:
        configure_json_logging(
            cfg.log_level,
            run_id=run_id,
        )

    start = parse_cli_date(start_date)
    end = parse_cli_date(end_date)

    mongo = get_mongo_client(cfg)
    database = mongo[cfg.mongo_db]
    landing = database[cfg.mongo_landing_collection]
    state = database[cfg.mongo_state_collection]
    curated = database[cfg.mongo_curated_collection]

    curated.create_index(
        [("source", ASCENDING), ("body", ASCENDING), ("identifier", ASCENDING)],
        unique=True,
        name="uq_source_body_identifier",
    )
    curated.create_index([("partition_date", ASCENDING)])

    s3 = get_s3_client(cfg)
    ensure_bucket(s3, cfg.s3_curated_bucket)

    query = {"partition_date": {"$gte": start.isoformat(), "$lte": end.isoformat()}}
    stats: dict[str, Any] = {
        "run_id": run_id,
        "selected": 0,
        "transformed": 0,
        "skipped_unchanged": 0,
        "failed": [],
    }

    # Mongo cursors aren't thread-safe; the records are small, so materialize.
    records = list(state.find(query))
    stats["selected"] = len(records)

    # I/O-bound work — threads overlap the S3/Mongo latency. Clients are
    # thread-safe, and each record upserts its own key, so order is irrelevant.
    with ThreadPoolExecutor(max_workers=cfg.transform_workers) as executor:
        futures = [
            executor.submit(
                _process_record,
                current_state,
                s3=s3,
                landing=landing,
                curated=curated,
                cfg=cfg,
                run_id=run_id,
            )
            for current_state in records
        ]

        for future in as_completed(futures):
            result = future.result()
            outcome = result["outcome"]
            if outcome == "transformed":
                stats["transformed"] += 1
            elif outcome == "skipped":
                stats["skipped_unchanged"] += 1
            else:
                stats["failed"].append(
                    {"identifier": result["identifier"], "error": result["error"]}
                )

    logger.info("transformation summary", extra={"summary": stats})
    mongo.close()
    return stats


def _process_record(
    current_state: dict,
    *,
    s3,
    landing,
    curated,
    cfg,
    run_id: str,
) -> dict[str, Any]:
    """Transform one landing version. Returns an outcome dict instead of
    raising, so one bad record never kills the pool."""

    identifier = current_state.get("identifier", "unknown")

    try:
        version_id = current_state.get("latest_version_id")

        if version_id is None:
            raise ValueError("Document state has no latest_version_id")

        record = landing.find_one({"_id": version_id})

        if record is None:
            raise ValueError(f"Landing version {version_id} does not exist")

        nat_key = {
            "source": record.get("source"),
            "body": record.get("body"),
            "identifier": record["identifier"],
        }
        existing = curated.find_one(
            nat_key,
            {"source_version_id": 1, "parser_version": 1, "file_path": 1},
        )
        if (
            existing
            and existing.get("source_version_id") == version_id
            and existing.get("parser_version") == PARSER_VERSION
        ):
            logger.info(
                "curated record up to date; skipping",
                extra={"identifier": identifier},
            )
            return {"outcome": "skipped", "identifier": identifier}

        bucket, key = _parse_s3_uri(record["file_path"])
        obj = s3.get_object(Bucket=bucket, Key=key)
        ext = str(record["file_ext"]).lower()

        out_key = _curated_object_key(
            record["body"], record["partition_date"], identifier, ext
        )

        if ext in PASSTHROUGH_EXTS:
            # Stream S3 -> spool -> S3 while hashing; memory stays bounded
            # no matter how large the document is.
            content_type = record.get("content_type") or "application/octet-stream"
            with _spooled_copy(obj["Body"]) as (spool, downloaded_hash, size_bytes):
                if downloaded_hash != record["file_hash"]:
                    raise ValueError(
                        "Landing object hash mismatch for "
                        f"{identifier}: metadata={record['file_hash']} "
                        f"downloaded={downloaded_hash}"
                    )
                new_hash = downloaded_hash
                spool.seek(0)
                s3.upload_fileobj(
                    spool,
                    cfg.s3_curated_bucket,
                    out_key,
                    ExtraArgs={"ContentType": content_type},
                )

        elif ext == "html":
            # HTML needs the whole DOM anyway; verify integrity before parsing.
            raw = obj["Body"].read()
            downloaded_hash = sha256_bytes(raw)
            if downloaded_hash != record["file_hash"]:
                raise ValueError(
                    "Landing object hash mismatch for "
                    f"{identifier}: metadata={record['file_hash']} "
                    f"downloaded={downloaded_hash}"
                )

            canonical = canonicalize_html(
                raw,
                selectors=cfg.html_selector_list,
                parser=cfg.html_parser,
                drop_selectors=cfg.html_drop_selector_list,
                min_text_chars=cfg.html_min_text_chars,
            )

            out_bytes = canonical.html_bytes
            new_hash = sha256_bytes(out_bytes)
            size_bytes = len(out_bytes)
            content_type = "text/html; charset=utf-8"

            logger.info(
                "extracted relevant HTML content",
                extra={
                    "identifier": identifier,
                    "selector_used": canonical.selector_used,
                    "fallback_used": canonical.fallback_used,
                    "extracted_text_length": canonical.text_length,
                    "parser_used": canonical.parser_used,
                },
            )

            s3.put_object(
                Bucket=cfg.s3_curated_bucket,
                Key=out_key,
                Body=out_bytes,
                ContentType=content_type,
            )

        else:
            raise ValueError(f"Unsupported landing file extension: {ext!r}")

        new_path = f"s3://{cfg.s3_curated_bucket}/{out_key}"

        curated_doc = {
            **{
                k: record.get(k)
                for k in (
                    "identifier",
                    "title",
                    "description",
                    "published_date",
                    "doc_url",
                    "body",
                    "partition_date",
                    "partition_label",
                    "source",
                )
            },
            "file_ext": ext,
            "file_path": new_path,
            "file_hash": new_hash,
            "size_bytes": size_bytes,
            # Lineage to the exact landing version this came from.
            "source_version_id": version_id,
            "source_content_hash": record["content_hash"],
            "source_file_path": record["file_path"],
            "source_file_hash": record["file_hash"],
            "parser_version": PARSER_VERSION,
            "run_id": run_id,
            "transformed_at": datetime.now(timezone.utc),
        }
        curated.update_one(nat_key, {"$set": curated_doc}, upsert=True)

        # Curated is latest-only: drop the object the old record pointed at if
        # the key moved. Best-effort — a failed delete is a stray object, not
        # a correctness problem. Landing is never touched here.
        old_path = existing.get("file_path") if existing else None
        if old_path and old_path != new_path:
            try:
                old_bucket, old_key = _parse_s3_uri(old_path)
                s3.delete_object(Bucket=old_bucket, Key=old_key)
                logger.info(
                    "removed superseded curated object",
                    extra={
                        "identifier": identifier,
                        "old_path": old_path,
                        "new_path": new_path,
                    },
                )
            except Exception as exc:  # noqa: BLE001 - best-effort cleanup
                logger.warning(
                    "could not remove superseded curated object",
                    extra={
                        "identifier": identifier,
                        "old_path": old_path,
                        "error": str(exc),
                    },
                )

        logger.info(
            "transformed document",
            extra={
                "identifier": identifier,
                "file_path": new_path,
                "file_hash": new_hash,
            },
        )
        return {"outcome": "transformed", "identifier": identifier}
    except Exception as exc:  # noqa: BLE001 - log & continue per record
        logger.error(
            "transformation failed",
            extra={"identifier": identifier, "error": str(exc)},
        )
        return {"outcome": "failed", "identifier": identifier, "error": str(exc)}


# 1 MiB read chunks; files over 8 MiB spill from RAM to disk.
_STREAM_CHUNK_BYTES = 1024 * 1024
_SPOOL_MAX_BYTES = 8 * 1024 * 1024


@contextmanager
def _spooled_copy(stream) -> Iterator[tuple[IO[bytes], str, int]]:
    """Stream an S3 body into a spooled temp file, hashing along the way.
    Yields (spool at EOF, sha256 hexdigest, size)."""
    digest = hashlib.sha256()
    size = 0
    with tempfile.SpooledTemporaryFile(max_size=_SPOOL_MAX_BYTES) as spool:
        for chunk in stream.iter_chunks(_STREAM_CHUNK_BYTES):
            digest.update(chunk)
            spool.write(chunk)
            size += len(chunk)
        yield spool, digest.hexdigest(), size


def _curated_object_key(
    body: str, partition_date: str, identifier: str, ext: str
) -> str:
    """Curated key: landing's body=/partition= layout, but the basename is
    <identifier>.<ext> (rename requirement), key-sanitised so identifiers
    like ``RP89/2008`` can't inject path segments. Latest-only, so no
    content-hash segment."""

    return (
        f"body={slug(body)}/partition={partition_date}/"
        f"{safe_identifier(identifier)}.{ext}"
    )


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {uri!r}")

    without_scheme = uri.removeprefix("s3://")
    bucket, separator, key = without_scheme.partition("/")

    if not separator or not bucket or not key:
        raise ValueError(f"Invalid S3 URI: {uri!r}")

    return bucket, key


def main() -> None:
    parser = argparse.ArgumentParser(description="WRC landing->curated transform")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    args = parser.parse_args()
    run_transformation(args.start_date, args.end_date)


if __name__ == "__main__":
    main()
