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
from datetime import datetime, timezone

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

STRIP_TAGS = [
    "script",
    "style",
    "nav",
    "header",
    "footer",
    "form",
    "button",
    "noscript",
    "iframe",
    "aside",
]
PASSTHROUGH_EXTS = {"pdf", "doc", "docx", "rtf"}

# Bump when the HTML-cleaning logic changes: records transformed with an older
# parser version are re-transformed on the next run (landing stays untouched).
PARSER_VERSION = "1.0"

# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def run_transformation(start_date: str, end_date: str) -> dict:
    cfg = get_settings()
    run_id = new_run_id()
    configure_json_logging(cfg.log_level, run_id=run_id)

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
    stats = {
        "run_id": run_id,
        "selected": 0,
        "transformed": 0,
        "skipped_unchanged": 0,
        "failed": [],
    }

    for current_state in state.find(query):
        stats["selected"] += 1

        identifier = current_state.get("identifier", "unknown")

        try:
            version_id = current_state.get("latest_version_id")

            if version_id is None:
                raise ValueError(
                    "Document state has no latest_version_id"
                )

            record = landing.find_one(
                {"_id": version_id}
            )

            if record is None:
                raise ValueError(
                    f"Landing version {version_id} does not exist"
                )

            nat_key = {
                "source": record.get("source"),
                "body": record.get("body"),
                "identifier": record["identifier"],
            }
            existing = curated.find_one(
                nat_key,
                {"source_file_hash": 1, "parser_version": 1},
            )
            if (
                existing
                and existing.get("source_file_hash") == record["file_hash"]
                and existing.get("parser_version") == PARSER_VERSION
            ):
                stats["skipped_unchanged"] += 1
                logger.info(
                    "curated record up to date; skipping",
                    extra={"identifier": identifier},
                )
                continue

            bucket, key = _parse_s3_uri(record["file_path"])
            obj = s3.get_object(Bucket=bucket, Key=key)
            raw = obj["Body"].read()

            ext = record["file_ext"]
            if ext in PASSTHROUGH_EXTS:
                out_bytes = raw  # requirement 1.c.i
                new_hash = record["file_hash"]
                content_type = record.get("content_type") or "application/octet-stream"
            else:
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
                ext = "html"

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

            # Requirement: file NAME becomes <identifier>.<ext>. We keep that
            # basename but namespace the prefix by body so the key can never
            # collide across bodies that reuse an identifier (consistent with
            # the (source, body, identifier) natural key).
            out_key = f"curated/body={slug(record['body'])}/{identifier}.{ext}"
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
                "source_file_path": record["file_path"],
                "source_file_hash": record["file_hash"],
                "parser_version": PARSER_VERSION,
                "run_id": run_id,
                "transformed_at": datetime.now(timezone.utc),
            }
            curated.update_one(nat_key, {"$set": curated_doc}, upsert=True)
            stats["transformed"] += 1
            logger.info(
                "transformed document",
                extra={
                    "identifier": identifier,
                    "file_path": new_path,
                    "file_hash": new_hash,
                },
            )
        except Exception as exc:  # noqa: BLE001 - log & continue per record
            stats["failed"].append({"identifier": identifier, "error": str(exc)})
            logger.error(
                "transformation failed",
                extra={"identifier": identifier, "error": str(exc)},
            )

    logger.info("transformation summary", extra={"summary": stats})
    mongo.close()
    return stats


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    without_scheme = uri.removeprefix("s3://")
    bucket, _, key = without_scheme.partition("/")
    return bucket, key


def main() -> None:
    parser = argparse.ArgumentParser(description="WRC landing->curated transform")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    args = parser.parse_args()
    run_transformation(args.start_date, args.end_date)


if __name__ == "__main__":
    main()
