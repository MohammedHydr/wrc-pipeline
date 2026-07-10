"""Landing-zone -> curated-zone transformation.

Given a start and end date (matched against `partition_date`):

1. fetch metadata records from the Mongo landing collection,
2. download each referenced file from the landing bucket,
3. transform:
     * pdf/doc/docx/rtf  -> passed through unchanged,
     * html              -> reduced to the relevant content only
                            (navigation, headers, footers, scripts, forms
                            stripped) and re-hashed,
4. rename EVERY file to `<identifier>.<ext>`,
5. upload to the curated bucket,
6. upsert the curated metadata record (new file_path + file_hash) into a
   separate Mongo collection.

The landing zone is never modified. The step is idempotent: curated records
are keyed on `identifier` and re-uploading identical bytes to the same key is
a no-op; if the source hash has not changed since the last transformation,
the record is skipped entirely.

Usage:
    python -m transform.transform --start-date 2024-01-01 --end-date 2024-06-30
"""

from __future__ import annotations

import argparse
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

from pymongo import ASCENDING

from config.common import (
    configure_json_logging,
    ensure_bucket,
    get_mongo_client,
    get_s3_client,
    new_run_id,
    parse_cli_date,
    sha256_bytes,
    slug,
)
from config.settings import get_settings
from config.html_utils import canonicalize_html

logger = logging.getLogger("transform")

PASSTHROUGH_EXTS = {"pdf", "doc", "docx", "rtf"}

# Bump when the HTML-cleaning logic changes: records transformed with an older
# parser version are re-transformed on the next run (landing stays untouched).
PARSER_VERSION = "1.1"


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
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

    # Same natural key as landing: one curated record per (source, body,
    # identifier). partition_date supports downstream date-range queries.
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

    # Materialize the cursor before fanning out: a Mongo cursor is not safe to
    # iterate from multiple threads, whereas the MongoClient itself is. State
    # records are small metadata documents, so holding the selection in memory
    # is cheap.
    records = list(state.find(query))
    stats["selected"] = len(records)

    # The step is I/O-bound (S3 GET/PUT + Mongo per record); worker threads
    # overlap that latency. pymongo and botocore low-level clients are both
    # thread-safe, so the same `s3` / collection handles are shared across
    # workers. Each record upserts its own (source, body, identifier) key, so
    # results stay idempotent regardless of completion order.
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
    """Transform one landing version into a curated record.

    Returns an outcome dict — ``{"outcome": "transformed"|"skipped"|"failed",
    "identifier": ..., "error": ...}`` — instead of raising, so a single bad
    record never aborts the pool. Shares thread-safe Mongo/S3 handles with the
    caller.
    """

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
        raw = obj["Body"].read()
        downloaded_hash = sha256_bytes(raw)

        if downloaded_hash != record["file_hash"]:
            raise ValueError(
                "Landing object hash mismatch for "
                f"{identifier}: metadata={record['file_hash']} "
                f"downloaded={downloaded_hash}"
            )
        ext = str(record["file_ext"]).lower()

        if ext in PASSTHROUGH_EXTS:
            out_bytes = raw
            new_hash = downloaded_hash
            content_type = record.get("content_type") or "application/octet-stream"

        elif ext == "html":
            canonical = canonicalize_html(
                raw,
                selectors=cfg.html_selector_list,
                parser=cfg.html_parser,
                drop_selectors=cfg.html_drop_selector_list,
                min_text_chars=cfg.html_min_text_chars,
            )

            out_bytes = canonical.html_bytes
            new_hash = sha256_bytes(out_bytes)
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

        else:
            raise ValueError(f"Unsupported landing file extension: {ext!r}")

        # File NAME is <identifier>.<ext> (rename requirement), namespaced by
        # body + partition so the key can never collide across bodies/partitions
        # that reuse an identifier (consistent with the (source, body,
        # identifier) natural key and the landing layout).
        out_key = _curated_object_key(
            record["body"], record["partition_date"], identifier, ext
        )
        s3.put_object(
            Bucket=cfg.s3_curated_bucket,
            Key=out_key,
            Body=out_bytes,
            ContentType=content_type,
        )
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
            "size_bytes": len(out_bytes),
            # Lineage back to the exact landing version this was derived from
            "source_version_id": version_id,
            "source_content_hash": record["content_hash"],
            "source_file_path": record["file_path"],
            "source_file_hash": record["file_hash"],
            "parser_version": PARSER_VERSION,
            "run_id": run_id,
            "transformed_at": datetime.now(timezone.utc),
        }
        curated.update_one(nat_key, {"$set": curated_doc}, upsert=True)

        # Curated is a latest-only view: if this rewrite lands under a new key
        # (e.g. the source ext changed HTML -> PDF, or the slug changed), delete
        # the object the previous curated record pointed at so no orphan is left
        # behind. Best-effort — the curated record already points at new_path, so
        # a failed cleanup is a stray object, not a correctness problem. (Only
        # curated is mutable; landing is never touched here.)
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


def _curated_object_key(
    body: str, partition_date: str, identifier: str, ext: str
) -> str:
    """Key for one curated object.

    Mirrors the landing layout (``body=`` / ``partition=`` Hive prefix) so both
    zones are navigable the same way, but the basename is ``<identifier>.<ext>``
    as the rename requirement mandates. Curated is a *latest-only* derived view
    (one object per logical document), so — unlike landing — there is no
    content-hash version segment and a superseded object is deleted on rewrite.
    """

    return f"body={slug(body)}/partition={partition_date}/{identifier}.{ext}"


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
