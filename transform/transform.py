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

from bs4 import BeautifulSoup, FeatureNotFound
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
# HTML cleaning
# --------------------------------------------------------------------------- #
def extract_relevant_html(
    raw: bytes, selectors: list[str], parser: str = "lxml"
) -> bytes:
    """Return a minimal HTML document containing only the decision content.

    Strategy: try each configured CSS selector in order and take the first
    match that contains a meaningful amount of text; fall back to <body> with
    boilerplate tags stripped. Data-quality extras: collapse whitespace-only
    nodes and drop tag attributes that only matter for styling/JS.

    `parser` is the BeautifulSoup backend ("lxml" by default; "html.parser" is
    a dependency-free fallback). We fall back to html.parser automatically if
    the configured parser isn't installed, so the step never hard-fails on a
    missing optional dependency.
    """
    try:
        soup = BeautifulSoup(raw, parser)
    except FeatureNotFound:
        logger.warning(
            "configured HTML parser unavailable; falling back",
            extra={"requested_parser": parser, "fallback": "html.parser"},
        )
        soup = BeautifulSoup(raw, "html.parser")

    for tag in soup(STRIP_TAGS):
        tag.decompose()

    node = None
    for sel in selectors:
        candidate = soup.select_one(sel)
        if candidate and len(candidate.get_text(strip=True)) > 200:
            node = candidate
            break
    if node is None:
        node = soup.body or soup

    # drop presentational / behavioural attributes for cleaner downstream text
    for tag in [node, *node.find_all(True)]:
        if hasattr(tag, "attrs"):
            for attr in ("class", "style", "onclick", "id", "role"):
                tag.attrs.pop(attr, None)

    title = soup.title.string.strip() if soup.title and soup.title.string else ""
    html = (
        "<!DOCTYPE html>\n<html><head><meta charset='utf-8'>"
        f"<title>{title}</title></head><body>{node.decode()}</body></html>"
    )
    return html.encode("utf-8")


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
    landing = mongo[cfg.mongo_db][cfg.mongo_landing_collection]
    curated = mongo[cfg.mongo_db][cfg.mongo_curated_collection]
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

    for record in landing.find(query):
        stats["selected"] += 1
        identifier = record["identifier"]
        nat_key = {
            "source": record.get("source"),
            "body": record.get("body"),
            "identifier": identifier,
        }
        try:
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
                out_bytes = extract_relevant_html(  # requirement 1.c.ii
                    raw, cfg.html_selector_list, cfg.html_parser
                )
                new_hash = sha256_bytes(out_bytes)
                content_type = "text/html; charset=utf-8"
                ext = "html"

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
