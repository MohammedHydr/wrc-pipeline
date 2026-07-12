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

EXTRACTION_VERSION = "2.1"

_ACT_RE = re.compile(
    r"\b([A-Z][A-Za-z()'’]*(?:\s+(?:\(?[A-Z][A-Za-z()'’]*|of|and|the)){0,7}?"
    r"\s+Acts?)[,]?\s+(\d{4}(?:\s*[-–]\s*\d{4})?)"
)
_OFFICER_RE = re.compile(r"Adjudication Officer:\s*(.+)", re.IGNORECASE)
_HEARING_RE = re.compile(
    r"Date of (?:Adjudication )?Hearing:\s*(\d{2}/\d{2}/\d{4})",
    re.IGNORECASE,
)
_COMPLAINT_REF_RE = re.compile(r"\bCA-\d{8}-\d{3}\b")
_AWARD_RE = re.compile(r"(?:€|EUR\s?)\s?([\d,]+(?:\.\d{1,2})?)")
_PARTIES_RE = re.compile(r"\s+-?[Vv][Ss]?\.?-?\s+")
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
_POSITIVE_NEGATED_PAIRS = (
    ("well founded", "not well founded"),
    ("upheld", "not upheld"),
)
_POSITIVE_PHRASES = ("succeeds", "discrimination occurred")
_NEGATIVE_PHRASES = ("dismissed", "discrimination did not occur")
_DECISION_CITATION_RES = (
    re.compile(r"\bADJ-\d{6,8}\b"),
    re.compile(r"\bLCR\d{4,6}\b"),
    re.compile(r"\bDEC-[A-Z]{1,2}\d{4}-\d{1,4}\b"),
    re.compile(r"\b[A-Z]{2,3}\d{1,5}/\d{4}\b"),
    re.compile(r"\b(?:EDA|UDD|PWD|TED|HSD|MWD|FTD|AWD|CD|DWT)\d{2,5}\b"),
    re.compile(r"\bIR\s*-\s*SC\s*-\s*\d{8}\b"),
)
_SECTION_RE = re.compile(
    r"[Ss]ections?\s+(\d+[A-Z]?(?:\(\d+\))?(?:\([a-z]\))?)\s+of\s+the\s+"
)
_RECEIPT_AFTER_REF_RE = re.compile(r"CA-\d{8}-\d{3}\s+(\d{2}/\d{2}/\d{4})")
_RECEIPT_LABELLED_RE = re.compile(
    r"Date of Receipt:?\s*(\d{2}/\d{2}/\d{4})",
    re.IGNORECASE,
)
_DECISION_TYPES = (
    ("correction order", "correction_order"),
    ("adjudication officer decision", "decision"),
    ("adjudication officer recommendation", "recommendation"),
    ("determination", "determination"),
    ("recommendation", "recommendation"),
    ("decision", "decision"),
)
_GENERIC_PARTY_RE = re.compile(
    r"^(?:An?\s+[A-Z]|Anonymised\b|Employee\b|Employer\b|Worker\b)"
)
_PRACTICE_AREA_KEYWORDS = (
    ("Unfair Dismissals", "unfair_dismissal"),
    ("Employment Equality", "equality_discrimination"),
    ("Equal Status", "equality_discrimination"),
    ("Payment of Wages", "pay_wages"),
    ("National Minimum Wage", "pay_wages"),
    ("Organisation of Working Time", "working_time"),
    ("Redundancy Payments", "redundancy"),
    ("Minimum Notice", "notice_terms"),
    ("Terms of Employment", "notice_terms"),
    ("Industrial Relations", "industrial_relations"),
    ("Safety, Health and Welfare", "health_safety"),
    ("Protected Disclosures", "whistleblowing"),
    ("Maternity Protection", "family_leave"),
    ("Parental Leave", "family_leave"),
    ("Paternity Leave", "family_leave"),
    ("Carer's Leave", "family_leave"),
    ("Fixed-Term", "atypical_work"),
    ("Part-Time", "atypical_work"),
    ("Temporary Agency", "atypical_work"),
    ("Employment Permits", "employment_permits"),
)
_BLOCK_TAGS = frozenset(
    "p li td th caption h1 h2 h3 h4 h5 h6 tr table ul ol dl dt dd "
    "div section article blockquote br hr".split()
)
_IGNORED_TAGS = ("script", "style", "noscript", "template", "svg")


def _block_lines(soup: BeautifulSoup) -> list[str]:
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


def extract_decision_fields(
    html: bytes | str,
    parser: str = "lxml",
) -> dict[str, Any]:
    soup = BeautifulSoup(html, parser)

    for tag in soup.find_all(_IGNORED_TAGS):
        tag.decompose()

    paragraphs = _block_lines(soup)
    flat = " ".join(paragraphs)

    officer: Optional[str] = None
    hearing_date: Optional[str] = None

    for line in paragraphs:
        if officer is None:
            match = _OFFICER_RE.search(line)
            if match and match.group(1).strip():
                officer = match.group(1).strip()

        if hearing_date is None:
            match = _HEARING_RE.search(line)
            if match:
                try:
                    hearing_date = (
                        datetime.strptime(match.group(1), "%d/%m/%Y").date().isoformat()
                    )
                except ValueError:
                    hearing_date = None

    acts: list[str] = []
    for line in paragraphs:
        for name, years in _ACT_RE.findall(line):
            act = f"{name.strip()} {years.strip()}"
            if act not in acts:
                acts.append(act)

    awards: list[float] = []
    for amount in _AWARD_RE.findall(flat):
        try:
            awards.append(float(amount.replace(",", "")))
        except ValueError:
            continue

    lowered = flat.lower()
    outcome_signals = [phrase for phrase in _OUTCOME_PHRASES if phrase in lowered]

    sections: list[dict[str, str]] = []
    for line in paragraphs:
        for match in _SECTION_RE.finditer(line):
            act_match = _ACT_RE.search(line, match.end())
            if act_match:
                entry = {
                    "section": match.group(1),
                    "act": (
                        f"{act_match.group(1).strip()} {act_match.group(2).strip()}"
                    ),
                }
                if entry not in sections:
                    sections.append(entry)

    complaint_refs = sorted(set(_COMPLAINT_REF_RE.findall(flat)))

    cited: list[str] = []
    for pattern in _DECISION_CITATION_RES:
        for ref in pattern.findall(flat):
            normalised = " ".join(ref.split())
            if normalised not in cited:
                cited.append(normalised)

    receipt_dates: list[str] = []
    for pattern in (_RECEIPT_LABELLED_RE, _RECEIPT_AFTER_REF_RE):
        for raw in pattern.findall(flat):
            try:
                receipt_dates.append(
                    datetime.strptime(raw, "%d/%m/%Y").date().isoformat()
                )
            except ValueError:
                continue

    received_date = min(receipt_dates) if receipt_dates else None

    return {
        "adjudication_officer": officer,
        "hearing_date": hearing_date,
        "acts_cited": acts,
        "sections_cited": sections,
        "practice_areas": derive_practice_areas(acts),
        "complaint_references": complaint_refs,
        "cited_decisions": cited,
        "received_date": received_date,
        "decision_type": _derive_decision_type(lowered),
        "self_represented": (
            "self-represented" in lowered or "self represented" in lowered
        ),
        "award_amounts_eur": awards,
        "award_max_eur": max(awards) if awards else None,
        "outcome_signals": outcome_signals,
        "outcome": derive_outcome(lowered),
        "paragraphs": paragraphs,
        "paragraph_count": len(paragraphs),
        "text_length": len(flat),
    }


def derive_outcome(lowered_text: str) -> Optional[str]:
    positive = any(
        lowered_text.count(positive_phrase) > lowered_text.count(negative_phrase)
        for positive_phrase, negative_phrase in _POSITIVE_NEGATED_PAIRS
    ) or any(phrase in lowered_text for phrase in _POSITIVE_PHRASES)

    negative = any(
        negative_phrase in lowered_text
        for _, negative_phrase in _POSITIVE_NEGATED_PAIRS
    ) or any(phrase in lowered_text for phrase in _NEGATIVE_PHRASES)

    if positive and negative:
        return "mixed"
    if positive:
        return "upheld"
    if negative:
        return "not_upheld"
    return None


def derive_practice_areas(acts: list[str]) -> list[str]:
    areas: list[str] = []

    for act in acts:
        for keyword, area in _PRACTICE_AREA_KEYWORDS:
            if keyword in act and area not in areas:
                areas.append(area)

    return areas


def _derive_decision_type(lowered_text: str) -> Optional[str]:
    for phrase, decision_type in _DECISION_TYPES:
        if phrase in lowered_text:
            return decision_type

    return None


def is_anonymised(
    complainant: Optional[str],
    respondent: Optional[str],
) -> bool:
    return any(
        _GENERIC_PARTY_RE.match(party) is not None
        for party in (complainant, respondent)
        if party
    )


def _days_between(
    start_iso: Optional[str],
    end_iso: Optional[str],
) -> Optional[int]:
    if not start_iso or not end_iso:
        return None

    try:
        start = datetime.strptime(start_iso, "%Y-%m-%d").date()
        end = datetime.strptime(end_iso, "%Y-%m-%d").date()
    except ValueError:
        return None

    delta = (end - start).days
    return delta if delta >= 0 else None


def split_parties(
    description: Optional[str],
) -> dict[str, Optional[str]]:
    if not description:
        return {
            "complainant": None,
            "respondent": None,
        }

    parts = _PARTIES_RE.split(description, maxsplit=1)

    if len(parts) == 2 and all(part.strip() for part in parts):
        return {
            "complainant": parts[0].strip(),
            "respondent": parts[1].strip(),
        }

    return {
        "complainant": None,
        "respondent": None,
    }


def run_enrichment(
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
    curated = database[cfg.mongo_curated_collection]
    enriched = database[cfg.mongo_enriched_collection]

    enriched.create_index(
        [
            ("source", ASCENDING),
            ("body", ASCENDING),
            ("identifier", ASCENDING),
        ],
        unique=True,
        name="uq_source_body_identifier",
    )
    enriched.create_index([("partition_date", ASCENDING)])
    enriched.create_index([("acts_cited", ASCENDING)])
    enriched.create_index([("adjudication_officer", ASCENDING)])
    enriched.create_index([("practice_areas", ASCENDING)])
    enriched.create_index([("cited_decisions", ASCENDING)])
    enriched.create_index([("outcome", ASCENDING)])

    s3 = get_s3_client(cfg)

    query = {
        "partition_date": {
            "$gte": start.isoformat(),
            "$lte": end.isoformat(),
        }
    }
    records = list(curated.find(query))

    stats: dict[str, Any] = {
        "run_id": run_id,
        "selected": len(records),
        "extracted": 0,
        "binary_source": 0,
        "skipped_unchanged": 0,
        "failed": [],
    }

    with ThreadPoolExecutor(
        max_workers=cfg.transform_workers,
    ) as executor:
        futures = [
            executor.submit(
                _enrich_record,
                record,
                s3=s3,
                enriched=enriched,
                cfg=cfg,
                run_id=run_id,
            )
            for record in records
        ]

        for future in as_completed(futures):
            result = future.result()
            outcome = result["outcome"]

            if outcome in (
                "extracted",
                "binary_source",
                "skipped_unchanged",
            ):
                stats[outcome] += 1
            else:
                stats["failed"].append(
                    {
                        "identifier": result["identifier"],
                        "error": result["error"],
                    }
                )

    logger.info(
        "enrichment summary",
        extra={"summary": stats},
    )
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
    identifier = record.get("identifier", "unknown")

    try:
        nat_key = {
            "source": record.get("source"),
            "body": record.get("body"),
            "identifier": record["identifier"],
        }

        existing = enriched.find_one(
            nat_key,
            {
                "source_file_hash": 1,
                "extraction_version": 1,
            },
        )

        if (
            existing
            and existing.get("source_file_hash") == record["file_hash"]
            and existing.get("extraction_version") == EXTRACTION_VERSION
        ):
            return {
                "outcome": "skipped_unchanged",
                "identifier": identifier,
            }

        base = {
            **nat_key,
            "title": record.get("title"),
            "description": record.get("description"),
            "published_date": record.get("published_date"),
            "partition_date": record.get("partition_date"),
            "doc_url": record.get("doc_url"),
            **split_parties(record.get("description")),
            "source_file_path": record.get("file_path"),
            "source_file_hash": record.get("file_hash"),
            "extraction_version": EXTRACTION_VERSION,
            "run_id": run_id,
            "enriched_at": datetime.now(timezone.utc),
        }

        if str(record.get("file_ext", "")).lower() == "html":
            bucket_and_key = record["file_path"].removeprefix("s3://")
            bucket, _, key = bucket_and_key.partition("/")
            raw = s3.get_object(
                Bucket=bucket,
                Key=key,
            )["Body"].read()

            fields = extract_decision_fields(
                raw,
                parser=cfg.html_parser,
            )

            doc = {
                **base,
                **fields,
                "is_anonymised": is_anonymised(
                    base.get("complainant"),
                    base.get("respondent"),
                ),
                "days_to_decision": _days_between(
                    fields.get("received_date"),
                    record.get("published_date"),
                ),
                "extraction_status": "extracted",
            }
            outcome = "extracted"
        else:
            doc = {
                **base,
                "paragraphs": [],
                "paragraph_count": 0,
                "text_length": 0,
                "extraction_status": "binary_source",
            }
            outcome = "binary_source"

        enriched.update_one(
            nat_key,
            {"$set": doc},
            upsert=True,
        )

        return {
            "outcome": outcome,
            "identifier": identifier,
        }
    except Exception as exc:
        logger.error(
            "enrichment failed",
            extra={
                "identifier": identifier,
                "error": str(exc),
            },
        )
        return {
            "outcome": "failed",
            "identifier": identifier,
            "error": str(exc),
        }


def main() -> None:
    parser = argparse.ArgumentParser(description="WRC curated-to-enriched extraction")
    parser.add_argument(
        "--start-date",
        required=True,
    )
    parser.add_argument(
        "--end-date",
        required=True,
    )
    args = parser.parse_args()

    run_enrichment(
        args.start_date,
        args.end_date,
    )


if __name__ == "__main__":
    main()
