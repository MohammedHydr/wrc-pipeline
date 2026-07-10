"""Curated-zone -> enriched-zone structured extraction (business layer).

Decision pages are semi-structured: labelled fields such as "Adjudication
Officer:", "Date of Adjudication Hearing:", the Acts a complaint was brought
under, complaint reference numbers, and monetary awards ("EUR x,xxx" / "€x,xxx")
appear as recognisable text patterns. This step turns each curated HTML
document into one queryable record — deterministic BeautifulSoup/regex
extraction only, no ML and no re-scraping.

For each curated record in the requested `partition_date` range:

* HTML documents  -> fields extracted into `enriched_decisions`
                     (extraction_status="extracted"),
* binary documents (legacy PDF/DOC scans) -> a stub record with
  extraction_status="binary_source", so text coverage stays measurable
  (these would need OCR — out of scope, recorded honestly).

Idempotent: records are keyed on the same natural key as curated
(`source, body, identifier`); a record whose curated file_hash and
EXTRACTION_VERSION are unchanged is skipped. The curated zone is read-only
here; landing is never touched.

Usage:
    python -m transform.enrich --start-date 2024-01-01 --end-date 2024-06-30
"""

from __future__ import annotations

import argparse
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Optional

from bs4 import BeautifulSoup
from pymongo import ASCENDING

from config.common import (
    configure_json_logging,
    get_mongo_client,
    get_s3_client,
    new_run_id,
    parse_cli_date,
)
from config.settings import get_settings

logger = logging.getLogger("enrich")

# Bump when extraction logic changes: stale records are re-extracted on the
# next run (curated and landing stay untouched).
EXTRACTION_VERSION = "1.2"

# "Employment Equality Act, 1998" / "Industrial Relations Act 1969" /
# "Employment Equality Acts, 1998 - 2015" / "Civil Law and Criminal Law
# (Miscellaneous Provisions) Act 2020". The name must be a run of
# Capitalised words (plus of/and/the connectors, optionally parenthesised)
# ending in Act/Acts, so a lowercase sentence prefix ("…insurable for all
# purposes under the …") can never be swallowed into the act name. Applied
# per text line (element boundary), so page headings cannot glue onto an
# act citation from a neighbouring element.
_ACT_RE = re.compile(
    r"\b([A-Z][A-Za-z()'’]*(?:\s+(?:\(?[A-Z][A-Za-z()'’]*|of|and|the)){0,7}?"
    r"\s+Acts?)[,]?\s+(\d{4}(?:\s*[-–]\s*\d{4})?)"
)
_OFFICER_RE = re.compile(r"Adjudication Officer:\s*(.+)", re.IGNORECASE)
_HEARING_RE = re.compile(
    r"Date of (?:Adjudication )?Hearing:\s*(\d{2}/\d{2}/\d{4})", re.IGNORECASE
)
_COMPLAINT_REF_RE = re.compile(r"\bCA-\d{8}-\d{3}\b")
_AWARD_RE = re.compile(r"(?:€|EUR\s?)\s?([\d,]+(?:\.\d{1,2})?)")
# "Complainant v Respondent" in listing descriptions ("V", "v", "-v-", "vs").
_PARTIES_RE = re.compile(r"\s+-?[Vv][Ss]?\.?-?\s+")
# Coarse outcome phrases, most specific first; all matches are kept as signals
# rather than pretending a single classification.
_OUTCOME_PHRASES = (
    "not well founded",
    "well founded",
    "not upheld",
    "upheld",
    "succeeds",
    "dismissed",
    "discrimination did not occur",
    "discrimination occurred",
)


# Elements that start a new logical text line. Inline tags (a, b, em, …) do
# not, so a citation containing inline markup stays on one line.
_BLOCK_TAGS = frozenset(
    "p li td th caption h1 h2 h3 h4 h5 h6 tr table ul ol dl dt dd "
    "div section article blockquote br hr".split()
)


def _block_lines(soup: BeautifulSoup) -> list[str]:
    """Text of the page as one line per block element.

    Whitespace inside each block is collapsed, so a sentence wrapped across
    source-HTML lines stays one line, while headings/cells/paragraphs remain
    separate lines. This is what makes the line-anchored patterns safe: they
    can neither be split by source wrapping nor glued to a neighbouring
    element's text.
    """
    lines: list[str] = []
    buffer: list[str] = []

    def flush() -> None:
        if buffer:
            lines.append(" ".join(buffer))
            buffer.clear()

    for element in soup.descendants:
        if isinstance(element, str):
            text = " ".join(element.split())
            if text:
                buffer.append(text)
        elif getattr(element, "name", None) in _BLOCK_TAGS:
            flush()
    flush()
    return lines


def extract_decision_fields(html: bytes | str, parser: str = "lxml") -> dict[str, Any]:
    """Extract structured business fields from one curated decision page.

    Pure and deterministic. Every field is optional: a pattern that does not
    match yields None/[] rather than a guess.
    """
    soup = BeautifulSoup(html, parser)
    lines = _block_lines(soup)
    flat = " ".join(lines)

    officer: Optional[str] = None
    hearing_date: Optional[str] = None
    for line in lines:
        if officer is None:
            m = _OFFICER_RE.search(line)
            if m and m.group(1).strip():
                officer = m.group(1).strip()
        if hearing_date is None:
            m = _HEARING_RE.search(line)
            if m:
                try:
                    hearing_date = (
                        datetime.strptime(m.group(1), "%d/%m/%Y").date().isoformat()
                    )
                except ValueError:
                    hearing_date = None

    acts: list[str] = []
    # Per line, not on `flat`: joining lines with spaces would let a heading
    # from a neighbouring element glue onto the start of an act citation.
    for line in lines:
        for name, years in _ACT_RE.findall(line):
            act = f"{name.strip()} {years.strip()}"
            if act not in acts:
                acts.append(act)

    awards = []
    for amount in _AWARD_RE.findall(flat):
        try:
            awards.append(float(amount.replace(",", "")))
        except ValueError:
            continue

    lowered = flat.lower()
    outcome_signals = [p for p in _OUTCOME_PHRASES if p in lowered]

    return {
        "adjudication_officer": officer,
        "hearing_date": hearing_date,
        "acts_cited": acts,
        "complaint_references": sorted(set(_COMPLAINT_REF_RE.findall(flat))),
        "award_amounts_eur": awards,
        "award_max_eur": max(awards) if awards else None,
        "outcome_signals": outcome_signals,
        "text_length": len(flat),
    }


def split_parties(description: Optional[str]) -> dict[str, Optional[str]]:
    """Split a listing description like "A Worker v An Employer" into parties."""
    if not description:
        return {"complainant": None, "respondent": None}
    parts = _PARTIES_RE.split(description, maxsplit=1)
    if len(parts) == 2 and all(p.strip() for p in parts):
        return {"complainant": parts[0].strip(), "respondent": parts[1].strip()}
    return {"complainant": None, "respondent": None}


def run_enrichment(
    start_date: str,
    end_date: str,
    *,
    configure_logging: bool = True,
) -> dict:
    cfg = get_settings()
    run_id = new_run_id()
    if configure_logging:
        configure_json_logging(cfg.log_level, run_id=run_id)

    start = parse_cli_date(start_date)
    end = parse_cli_date(end_date)

    mongo = get_mongo_client(cfg)
    database = mongo[cfg.mongo_db]
    curated = database[cfg.mongo_curated_collection]
    enriched = database[cfg.mongo_enriched_collection]

    enriched.create_index(
        [("source", ASCENDING), ("body", ASCENDING), ("identifier", ASCENDING)],
        unique=True,
        name="uq_source_body_identifier",
    )
    enriched.create_index([("partition_date", ASCENDING)])
    enriched.create_index([("acts_cited", ASCENDING)])
    enriched.create_index([("adjudication_officer", ASCENDING)])

    s3 = get_s3_client(cfg)

    query = {"partition_date": {"$gte": start.isoformat(), "$lte": end.isoformat()}}
    records = list(curated.find(query))

    stats: dict[str, Any] = {
        "run_id": run_id,
        "selected": len(records),
        "extracted": 0,
        "binary_source": 0,
        "skipped_unchanged": 0,
        "failed": [],
    }

    with ThreadPoolExecutor(max_workers=cfg.transform_workers) as executor:
        futures = [
            executor.submit(
                _enrich_record, rec, s3=s3, enriched=enriched, cfg=cfg, run_id=run_id
            )
            for rec in records
        ]
        for future in as_completed(futures):
            result = future.result()
            outcome = result["outcome"]
            if outcome in ("extracted", "binary_source", "skipped_unchanged"):
                stats[outcome] += 1
            else:
                stats["failed"].append(
                    {"identifier": result["identifier"], "error": result["error"]}
                )

    logger.info("enrichment summary", extra={"summary": stats})
    mongo.close()
    return stats


def _enrich_record(
    record: dict,
    *,
    s3,
    enriched,
    cfg,
    run_id: str,
) -> dict[str, Any]:
    """Enrich one curated record; returns an outcome dict, never raises."""
    identifier = record.get("identifier", "unknown")
    try:
        nat_key = {
            "source": record.get("source"),
            "body": record.get("body"),
            "identifier": record["identifier"],
        }

        existing = enriched.find_one(
            nat_key, {"source_file_hash": 1, "extraction_version": 1}
        )
        if (
            existing
            and existing.get("source_file_hash") == record["file_hash"]
            and existing.get("extraction_version") == EXTRACTION_VERSION
        ):
            return {"outcome": "skipped_unchanged", "identifier": identifier}

        base = {
            **nat_key,
            "title": record.get("title"),
            "description": record.get("description"),
            "published_date": record.get("published_date"),
            "partition_date": record.get("partition_date"),
            "doc_url": record.get("doc_url"),
            **split_parties(record.get("description")),
            # Lineage to the exact curated artifact this was derived from.
            "source_file_path": record.get("file_path"),
            "source_file_hash": record.get("file_hash"),
            "extraction_version": EXTRACTION_VERSION,
            "run_id": run_id,
            "enriched_at": datetime.now(timezone.utc),
        }

        if str(record.get("file_ext", "")).lower() == "html":
            bucket_and_key = record["file_path"].removeprefix("s3://")
            bucket, _, key = bucket_and_key.partition("/")
            raw = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            fields = extract_decision_fields(raw, parser=cfg.html_parser)
            doc = {**base, **fields, "extraction_status": "extracted"}
            outcome = "extracted"
        else:
            # Legacy binary scans (PDF/DOC) would need OCR; record the gap
            # honestly instead of silently shrinking coverage.
            doc = {**base, "extraction_status": "binary_source"}
            outcome = "binary_source"

        enriched.update_one(nat_key, {"$set": doc}, upsert=True)
        return {"outcome": outcome, "identifier": identifier}
    except Exception as exc:  # noqa: BLE001 - log & continue per record
        logger.error(
            "enrichment failed",
            extra={"identifier": identifier, "error": str(exc)},
        )
        return {"outcome": "failed", "identifier": identifier, "error": str(exc)}


def main() -> None:
    parser = argparse.ArgumentParser(description="WRC curated->enriched extraction")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    args = parser.parse_args()
    run_enrichment(args.start_date, args.end_date)


if __name__ == "__main__":
    main()
