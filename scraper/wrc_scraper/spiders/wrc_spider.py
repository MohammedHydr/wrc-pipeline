"""Spider for the WRC "Decisions and Determinations" search.

The ASP.NET search form 302-redirects to a plain GET endpoint (recon:
docs/recon/wrc-search.md), so we query that directly — no VIEWSTATE, no
browser. Flow: one page-1 request per (body x partition), paginate from the
"Shows 1 to 10 of N results" total, extract each result card, then fetch its
document (binaries verbatim, case pages as HTML). Every discovered record
ends in exactly one auditable state — the run summary reconciles
found = scraped + failed + parse_failed + skipped + listing losses.
"""

from __future__ import annotations

import json
import math
import re
from collections import defaultdict
from datetime import date, datetime, timezone
from typing import AsyncIterator, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import scrapy
from itemadapter import ItemAdapter
from scrapy import signals
from scrapy.http import Response

from config.common import (
    configure_json_logging,
    get_mongo_client,
    iter_partitions,
    new_run_id,
    parse_cli_date,
)
from config.settings import get_settings
from wrc_scraper.items import WrcDocumentItem

BINARY_EXTENSIONS = {"pdf", "doc", "docx", "rtf"}

DATE_RE = re.compile(r"^\d{2}/\d{2}/\d{4}$")
RESULT_COUNT_RE = re.compile(
    r"Shows?\s+\d+\s+to\s+\d+\s+of\s+([\d,]+)\s+results", re.IGNORECASE
)


class WrcSpider(scrapy.Spider):
    name = "wrc"

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #
    def __init__(
        self,
        start_date: str,
        end_date: str,
        bodies: Optional[str] = None,
        partition: Optional[str] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.cfg = get_settings()
        self.run_id = new_run_id()
        configure_json_logging(self.cfg.log_level, run_id=self.run_id)

        self.start_date: date = parse_cli_date(start_date)
        self.end_date: date = parse_cli_date(end_date)
        self.partition_size = (partition or self.cfg.partition_size).lower()
        # Netloc is part of the natural key; compute once so spider and
        # pipelines agree.
        self.source: str = urlparse(self.cfg.wrc_base_url).netloc
        self.body_codes: Dict[str, str] = self.cfg.body_code_map
        self.per_page: int = self.cfg.wrc_results_per_page
        self.bodies: List[str] = (
            [b.strip() for b in bodies.split(",")] if bodies else self.cfg.bodies_list
        )
        self.pdf_exclude: List[str] = self.cfg.embedded_pdf_exclude_list
        self.pdf_path_markers: List[str] = self.cfg.embedded_pdf_path_marker_list

        self.partitions: List[Tuple[date, date]] = list(
            iter_partitions(self.start_date, self.end_date, self.partition_size)
        )

        # Per-(partition, body) counters. "scraped" is only incremented once a
        # document is fully persisted (by MongoMetadataPipeline), never at
        # download time; "unchanged" is its subset. Everything else exists so
        # the close-time reconciliation can account for every found record —
        # unusable cards, fast-mode skips, requests that never reached a
        # terminal state, and records lost with a failed listing page.
        self.run_stats: Dict[Tuple[str, str], Dict] = defaultdict(
            lambda: {
                "found": None,
                "scraped": 0,
                "unchanged": 0,
                "failed": [],
                "parse_failures": [],
                "skipped": 0,
                "docs_enqueued": 0,
                "listing_failures": [],
                "count_parse_failed": False,
            }
        )
        # (source, body, identifier) -> file_hash, for fast-mode skip.
        self.known_hashes: Dict[Tuple[str, str, str], str] = {}
        # (source, body, identifier) -> (etag, fetched_url). ETags are only
        # replayed against the exact URL they were observed on.
        self.validators: Dict[Tuple[str, str, str], Tuple[str, str]] = {}

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        # Each preload is a full collection scan — only pay for what's on.
        if spider.cfg.skip_existing_identifiers:
            spider._load_known_hashes()
        if spider.cfg.use_conditional_requests:
            spider._load_validators()

        crawler.signals.connect(
            spider._on_item_error,
            signal=signals.item_error,
        )
        crawler.signals.connect(
            spider._on_item_dropped,
            signal=signals.item_dropped,
        )

        return spider

    def _load_known_hashes(self) -> None:
        """Preload known identifiers for fast mode. Non-fatal on failure."""
        try:
            client = get_mongo_client(self.cfg)
            coll = client[self.cfg.mongo_db][self.cfg.mongo_landing_collection]
            self.known_hashes = {
                (d.get("source", ""), d.get("body", ""), d["identifier"]): d.get(
                    "file_hash", ""
                )
                for d in coll.find(
                    {}, {"source": 1, "body": 1, "identifier": 1, "file_hash": 1}
                )
            }
            client.close()
            self.logger.info(
                "loaded known identifiers from mongo",
                extra={"count": len(self.known_hashes)},
            )
        except Exception as exc:  # noqa: BLE001 - deliberately broad, non-fatal
            self.logger.warning(
                "could not preload identifiers from mongo",
                extra={"error": str(exc)},
            )

    def _load_validators(self) -> None:
        """Preload stored ETags so re-fetches can 304 instead of re-download.
        Non-fatal: without them the crawl degrades to re-fetch + hash-compare."""
        try:
            client = get_mongo_client(self.cfg)
            coll = client[self.cfg.mongo_db][self.cfg.mongo_state_collection]
            self.validators = {
                (d.get("source", ""), d.get("body", ""), d["identifier"]): (
                    d["latest_etag"],
                    d.get("latest_fetched_url") or "",
                )
                for d in coll.find(
                    {"latest_etag": {"$nin": [None, ""]}},
                    {
                        "source": 1,
                        "body": 1,
                        "identifier": 1,
                        "latest_etag": 1,
                        "latest_fetched_url": 1,
                    },
                )
            }
            client.close()
            self.logger.info(
                "loaded etag validators from mongo",
                extra={"count": len(self.validators)},
            )
        except Exception as exc:  # noqa: BLE001 - deliberately broad, non-fatal
            self.logger.warning(
                "could not preload etag validators from mongo",
                extra={"error": str(exc)},
            )

    def _conditional_headers(
        self, nat_key: Tuple[str, str, str], url: str
    ) -> Optional[Dict[str, str]]:
        """If-None-Match headers when we hold an ETag for this exact URL.
        The URL match matters: a legacy record's ETag belongs to its embedded
        PDF, not the case page the listing links to."""
        validator = self.validators.get(nat_key)
        if validator and validator[0] and validator[1] == url:
            return {"If-None-Match": validator[0]}
        return None

    # ------------------------------------------------------------------ #
    # Step 1: fan out (body x partition) page-1 search requests
    # ------------------------------------------------------------------ #
    async def start(self) -> AsyncIterator[scrapy.Request]:
        for body in self.bodies:
            code = self.body_codes.get(body)
            if code is None:
                self.logger.error(
                    "no body code configured; skipping body",
                    extra={"body": body, "known": list(self.body_codes)},
                )
                continue
            for win_start, win_end in self.partitions:
                partition_label = f"{win_start.isoformat()}_{win_end.isoformat()}"
                url = self._search_url(win_start, win_end, code, page=1)
                self.logger.info(
                    "requesting search page",
                    extra={
                        "body": body,
                        "partition": partition_label,
                        "from": win_start.strftime(self.cfg.wrc_date_format),
                        "to": win_end.strftime(self.cfg.wrc_date_format),
                        "page": 1,
                    },
                )
                yield scrapy.Request(
                    url,
                    callback=self.parse_results,
                    errback=self.on_request_failed,
                    dont_filter=True,
                    meta={
                        "body": body,
                        "body_code": code,
                        "partition_date": win_start.isoformat(),
                        "partition_label": partition_label,
                        "win_start": win_start,
                        "win_end": win_end,
                        "page": 1,
                    },
                )

    def _search_url(self, win_start: date, win_end: date, code: str, page: int) -> str:
        frm = win_start.strftime(self.cfg.wrc_date_format)
        to = win_end.strftime(self.cfg.wrc_date_format)
        # Drop any stale query (e.g. "?advance=true") — no double-"?" URLs.
        base = self.cfg.search_url.split("?", 1)[0]
        return f"{base}?decisions=1&from={frm}&to={to}&body={code}&pageNumber={page}"

    # ------------------------------------------------------------------ #
    # Step 2: parse result pages + fan out remaining pages
    # ------------------------------------------------------------------ #
    def parse_results(self, response: Response):
        meta = response.meta
        key = (meta["partition_label"], meta["body"])

        records = list(self._extract_records(response))

        if meta["page"] == 1:
            total = self._extract_total(response)

            if total is None and not records:
                # No count text and no cards: genuinely zero results.
                total = 0
                self.run_stats[key]["found"] = 0
                self.run_stats[key]["count_parse_failed"] = False

                self.logger.info(
                    "search returned no records",
                    extra={
                        "partition": meta["partition_label"],
                        "body": meta["body"],
                        "records_found": 0,
                        "url": response.url,
                    },
                )

            elif total is None:
                # Cards exist but the count didn't parse — a real problem
                # that must not masquerade as zero.
                self.run_stats[key]["found"] = None
                self.run_stats[key]["count_parse_failed"] = True

                self.logger.error(
                    "could not parse total result count",
                    extra={
                        "partition": meta["partition_label"],
                        "body": meta["body"],
                        "url": response.url,
                        "visible_result_cards": len(records),
                    },
                )

            else:
                self.run_stats[key]["found"] = total
                self.run_stats[key]["count_parse_failed"] = False

                self.logger.info(
                    "search results",
                    extra={
                        "partition": meta["partition_label"],
                        "body": meta["body"],
                        "records_found": total,
                    },
                )

            if total is not None and total > 0:
                pages = math.ceil(total / self.per_page)

                for page in range(2, pages + 1):
                    url = self._search_url(
                        meta["win_start"],
                        meta["win_end"],
                        meta["body_code"],
                        page,
                    )

                    yield scrapy.Request(
                        url,
                        callback=self.parse_results,
                        errback=self.on_request_failed,
                        dont_filter=True,
                        meta={
                            "body": meta["body"],
                            "body_code": meta["body_code"],
                            "partition_date": meta["partition_date"],
                            "partition_label": meta["partition_label"],
                            "win_start": meta["win_start"],
                            "win_end": meta["win_end"],
                            "page": page,
                        },
                    )

        for rec in records:
            identifier = rec["identifier"]
            nat_key = (
                self.source,
                meta["body"],
                identifier,
            )

            if self.cfg.skip_existing_identifiers and nat_key in self.known_hashes:
                self.run_stats[key]["skipped"] += 1
                self.logger.info(
                    "skipping existing identifier (fast mode)",
                    extra={
                        "identifier": identifier,
                        **_ctx(meta),
                    },
                )
                continue

            item = WrcDocumentItem(
                identifier=identifier,
                title=rec["title"],
                description=rec["description"],
                published_date=rec["published_date"],
                doc_url=rec["doc_url"],
                body=meta["body"],
                partition_date=meta["partition_date"],
                partition_label=meta["partition_label"],
                source=self.source,
                scraped_at=datetime.now(timezone.utc).isoformat(),
            )

            ext = _url_extension(rec["doc_url"])

            # Counted before yielding: a request that later vanishes without
            # callback or errback still shows up in the reconciliation.
            self.run_stats[key]["docs_enqueued"] += 1

            doc_meta = {
                "item": item,
                "ext_hint": ext,
                **_ctx(meta),
            }
            headers = self._conditional_headers(nat_key, rec["doc_url"])
            if headers:
                # 304 is a success here — keep it away from HttpError.
                doc_meta["handle_httpstatus_list"] = [304]

            yield scrapy.Request(
                rec["doc_url"],
                callback=self.parse_document,
                errback=self.on_document_failed,
                headers=headers,
                meta=doc_meta,
                dont_filter=False,
            )

    def _extract_total(self, response: Response) -> Optional[int]:
        """Parse "Shows 1 to 10 of N results" into N (None if not found)."""
        text = " ".join(response.css("::text").getall())
        m = RESULT_COUNT_RE.search(text)
        return int(m.group(1).replace(",", "")) if m else None

    def _extract_records(self, response: Response):
        """Yield result-card dicts from a listing page (`li.each-item`:
        span.refNO, h2.title a, span.date, p.description@title)."""
        # Responses without a request (tests) still need failure accounting.
        meta = response.request.meta if response.request is not None else {}
        key = (meta.get("partition_label", "?"), meta.get("body", "?"))
        cards = response.css("li.each-item")
        for card in cards:
            identifier = (card.css("span.refNO::text").get() or "").strip()
            href = (
                card.css("div.bottom-ref a::attr(href)").get()
                or card.css("h2.title a::attr(href)").get()
            )
            if not identifier or not href:
                # The site's total counts this card, so it must land in an
                # auditable bucket — not silently disappear.
                entry = {
                    "url": response.url,
                    "identifier": identifier or None,
                    "href": href,
                    "stage": "listing_parse",
                    "error": "result card missing identifier or link",
                }
                self.run_stats[key]["parse_failures"].append(entry)
                self.logger.error(
                    "result card missing identifier or link; recorded as parse failure",
                    extra={**entry, "partition": key[0], "body": key[1]},
                )
                continue

            title = (card.css("h2.title a::text").get() or identifier).strip()
            # The title attribute holds the clean description; collapse the
            # site's embedded newlines between parties.
            description = " ".join(
                (card.css("p.description::attr(title)").get() or "").split()
            )

            date_txt = (card.css("span.date::text").get() or "").strip()
            published = None
            if DATE_RE.match(date_txt):
                try:
                    published = (
                        datetime.strptime(date_txt, "%d/%m/%Y").date().isoformat()
                    )
                except ValueError:
                    published = None

            yield {
                "identifier": identifier,
                "title": title,
                "description": description,
                "published_date": published,
                "doc_url": urljoin(response.url, href),
            }

    def _embedded_decision_pdf(
        self, response: Response, identifier: str
    ) -> Optional[str]:
        """URL of a legacy decision PDF embedded in an HTML case page, or None.

        A link qualifies when its filename contains the identifier (Equality's
        ``DEC-E2000-14.pdf``) or sits under a known legacy import path (EAT's
        GUID-named PDFs). Site-chrome PDFs are excluded; first match wins.
        """
        norm_id = _norm(identifier)
        for href in response.css("a::attr(href)").getall():
            if not href:
                continue
            low = href.lower()
            if not urlparse(low).path.endswith(".pdf"):
                continue
            if any(chrome in low for chrome in self.pdf_exclude):
                continue
            if norm_id and norm_id in _norm(urlparse(low).path):
                return urljoin(response.url, href)
            if any(marker in low for marker in self.pdf_path_markers):
                return urljoin(response.url, href)
        return None

    # ------------------------------------------------------------------ #
    # Step 3: download the document itself
    # ------------------------------------------------------------------ #
    def parse_document(self, response: Response):
        item: WrcDocumentItem = response.meta["item"]

        # 304: stored artifact is current, no body was sent. Pipelines resolve
        # hashes/path from state and count it as unchanged.
        if response.status == 304:
            self.logger.info(
                "document not modified (304); stored artifact is current",
                extra={
                    "identifier": item["identifier"],
                    "url": response.url,
                    **_ctx(response.meta),
                },
            )
            item["not_modified"] = True
            yield item
            return

        # An exception here would never reach the errback and the record would
        # vanish from run_stats — wrap it so it becomes an auditable failure.
        try:
            content_type = (
                (response.headers.get("Content-Type") or b"").decode("latin-1").lower()
            )
            ext = response.meta.get("ext_hint")
            if not ext:
                if "pdf" in content_type:
                    ext = "pdf"
                elif "msword" in content_type or "officedocument" in content_type:
                    ext = "docx" if "officedocument" in content_type else "doc"
                else:
                    ext = "html"

            # Legacy case pages embed the real decision as a PDF; follow it
            # and store that instead of the wrapper. `pdf_followed` stops the
            # fetched PDF from being re-scanned.
            if (
                ext == "html"
                and self.cfg.follow_embedded_pdf
                and not response.meta.get("pdf_followed")
            ):
                pdf_url = self._embedded_decision_pdf(response, item["identifier"])
                if pdf_url:
                    self.logger.info(
                        "following embedded decision PDF",
                        extra={
                            "identifier": item["identifier"],
                            "pdf_url": pdf_url,
                            **_ctx(response.meta),
                        },
                    )
                    pdf_meta = {
                        "item": item,
                        "ext_hint": "pdf",
                        "pdf_followed": True,
                        # If the PDF fetch fails (robots.txt forbids the legacy
                        # import paths), the errback stores this permitted
                        # wrapper instead — the record still succeeds.
                        "wrapper_body": response.body,
                        "wrapper_content_type": content_type,
                        **_ctx(response.meta),
                    }
                    nat_key = (self.source, item["body"], item["identifier"])
                    pdf_headers = self._conditional_headers(nat_key, pdf_url)
                    if pdf_headers:
                        pdf_meta["handle_httpstatus_list"] = [304]
                    yield scrapy.Request(
                        pdf_url,
                        callback=self.parse_document,
                        errback=self.on_document_failed,
                        headers=pdf_headers,
                        meta=pdf_meta,
                        dont_filter=False,
                    )
                    # The PDF request produces this record's terminal state.
                    return

            body = response.body

            # A 200 can still be the wrong payload (a .pdf link answering with
            # an HTML interstitial) — verify magic bytes before storing.
            if not _content_matches_ext(ext, body):
                self._record_document_failure(
                    response,
                    item,
                    stage="validation",
                    error=(
                        f"downloaded content does not match expected type {ext!r} "
                        "(magic-byte mismatch)"
                    ),
                )
                return

            item["file_content"] = body
            item["file_ext"] = ext
            item["content_type"] = content_type
            # Validator for the next run's conditional re-fetch, tied to the
            # exact URL the bytes came from.
            etag = (response.headers.get("ETag") or b"").decode("latin-1").strip()
            item["etag"] = etag or None
            item["fetched_url"] = response.url
        except Exception as exc:  # noqa: BLE001 - keep the record auditable
            self._record_document_failure(
                response,
                item,
                stage="parse",
                error=f"unhandled exception parsing document: {exc!r}",
            )
            return

        # "scraped" is counted by MongoMetadataPipeline after full persistence,
        # never here — a later stage failure must not look like a success.
        yield item

    def note_scraped(self, key: Tuple[str, str]) -> None:
        """Count one fully persisted document. Pipelines marshal this back to
        the reactor thread, so run_stats stays single-threaded — no locks."""
        self.run_stats[key]["scraped"] += 1

    def note_unchanged(self, key: Tuple[str, str]) -> None:
        """Count one unchanged document (same contract as note_scraped)."""
        self.run_stats[key]["unchanged"] += 1

    def _record_document_failure(
        self,
        response: Response,
        item: "WrcDocumentItem",
        *,
        stage: str,
        error: str,
    ) -> None:
        """A document that downloaded but is unusable — record it so the
        found = scraped + failed reconciliation holds."""
        meta = response.meta
        key = (meta.get("partition_label", "?"), meta.get("body", "?"))
        entry = {
            "url": response.url,
            "identifier": item.get("identifier"),
            "status": response.status,
            "stage": stage,
            "error": error,
        }
        self.run_stats[key]["failed"].append(entry)
        self.logger.error(
            "document validation failed",
            extra={**entry, "partition": key[0], "body": key[1]},
        )

    # ------------------------------------------------------------------ #
    # Error handling
    # ------------------------------------------------------------------ #
    def on_document_failed(self, failure):
        request = failure.request
        meta = request.meta
        status = getattr(getattr(failure.value, "response", None), "status", None)
        key = (meta.get("partition_label", "?"), meta.get("body", "?"))

        # PDF follow that carried a fallback payload: the PDF is unreachable
        # (usually robots.txt), but the case page it came from is permitted and
        # already downloaded — store that instead of failing the record.
        wrapper_body = meta.get("wrapper_body")
        if wrapper_body:
            item: WrcDocumentItem = meta["item"]
            self.logger.warning(
                "embedded decision PDF unavailable; storing HTML case page "
                "as fallback artifact",
                extra={
                    "pdf_url": request.url,
                    "identifier": item.get("identifier"),
                    "error": failure.getErrorMessage(),
                    "status": status,
                    "partition": key[0],
                    "body": key[1],
                },
            )
            item["file_content"] = wrapper_body
            item["file_ext"] = "html"
            item["content_type"] = (
                meta.get("wrapper_content_type") or "text/html; charset=utf-8"
            )
            yield item
            return

        entry = {
            "url": request.url,
            "identifier": meta.get("item", {}).get("identifier"),
            "error": failure.getErrorMessage(),
            "status": status,
        }
        self.run_stats[key]["failed"].append(entry)
        self.logger.error(
            "document download failed",
            extra={**entry, "partition": key[0], "body": key[1]},
        )

    def on_request_failed(self, failure):
        request = failure.request
        meta = request.meta

        status = getattr(
            getattr(failure.value, "response", None),
            "status",
            None,
        )

        key = (
            meta.get("partition_label", "?"),
            meta.get("body", "?"),
        )

        page = int(meta.get("page") or 1)
        found = self.run_stats[key]["found"]

        # How many records this failed listing page put at risk. Page 1 means
        # the total itself is unknown.
        if page == 1:
            records_at_risk = None
        elif found is not None:
            page_offset = (page - 1) * self.per_page
            records_at_risk = min(
                self.per_page,
                max(found - page_offset, 0),
            )
        else:
            records_at_risk = self.per_page

        entry = {
            "url": request.url,
            "page": page,
            "status": status,
            "stage": "listing",
            "error": failure.getErrorMessage(),
            "records_at_risk": records_at_risk,
        }

        self.run_stats[key]["listing_failures"].append(entry)

        self.logger.error(
            "listing-page request failed",
            extra={
                **entry,
                "partition": key[0],
                "body": key[1],
            },
        )

    # ------------------------------------------------------------------ #
    # Pipeline-stage failure accounting (item_error / item_dropped)
    # ------------------------------------------------------------------ #
    def _item_failure_context(self, item):
        adapter = ItemAdapter(item)

        key = (
            adapter.get("partition_label") or "?",
            adapter.get("body") or "?",
        )

        return adapter, key

    def _on_item_error(
        self,
        item,
        response,
        spider,
        failure,
    ) -> None:
        """Record exceptions raised inside the item pipelines."""

        adapter, key = self._item_failure_context(item)

        error_type = (
            failure.type.__name__
            if getattr(failure, "type", None)
            else type(failure.value).__name__
        )

        entry = {
            "identifier": adapter.get("identifier"),
            "url": (adapter.get("doc_url") or getattr(response, "url", None)),
            "status": getattr(response, "status", None),
            "stage": "pipeline",
            "error_type": error_type,
            "error": failure.getErrorMessage(),
        }

        self.run_stats[key]["failed"].append(entry)

        self.logger.error(
            "item pipeline failed",
            extra={
                **entry,
                "partition": key[0],
                "body": key[1],
            },
        )

    def _on_item_dropped(
        self,
        item,
        response,
        exception,
        spider,
    ) -> None:
        """Record items intentionally dropped by a pipeline."""

        adapter, key = self._item_failure_context(item)

        entry = {
            "identifier": adapter.get("identifier"),
            "url": (adapter.get("doc_url") or getattr(response, "url", None)),
            "status": getattr(response, "status", None),
            "stage": "pipeline",
            "error_type": type(exception).__name__,
            "error": str(exception),
        }

        self.run_stats[key]["failed"].append(entry)

        self.logger.error(
            "item dropped by pipeline",
            extra={
                **entry,
                "partition": key[0],
                "body": key[1],
            },
        )

    # ------------------------------------------------------------------ #
    # Step 4: end-of-run summary
    # ------------------------------------------------------------------ #
    def _reconcile_partition(self, partition: str, body: str, stats: Dict) -> Dict:
        """One (partition, body)'s counters as an auditable summary. Pure, so
        the reconciliation invariant is unit-testable."""
        document_failures = stats["failed"]
        parse_failures = stats["parse_failures"]
        listing_failures = stats["listing_failures"]

        known_listing_losses = sum(
            failure.get("records_at_risk") or 0 for failure in listing_failures
        )

        unknown_listing_loss = any(
            failure.get("records_at_risk") is None for failure in listing_failures
        )

        # Enqueued requests that never reached a terminal state = silent loss
        # (dropped at shutdown, dupe-filtered, …). Surfaced, never hidden.
        doc_unaccounted = max(
            stats["docs_enqueued"] - stats["scraped"] - len(document_failures),
            0,
        )

        # Every found record must land in exactly one bucket.
        accounted_records = (
            stats["scraped"]
            + len(document_failures)
            + len(parse_failures)
            + stats["skipped"]
            + known_listing_losses
            + doc_unaccounted
        )

        reconciled = (
            stats["found"] is not None
            and not unknown_listing_loss
            and accounted_records == stats["found"]
        )

        complete = (
            reconciled
            and not document_failures
            and not parse_failures
            and not listing_failures
            and not doc_unaccounted
            and not stats["count_parse_failed"]
        )

        return {
            "partition": partition,
            "body": body,
            "records_found": stats["found"],
            "records_scraped": stats["scraped"],
            "records_unchanged": stats["unchanged"],
            "records_failed": len(document_failures),
            "failures": document_failures,
            "records_parse_failed": len(parse_failures),
            "parse_failures": parse_failures,
            "records_skipped": stats["skipped"],
            "docs_enqueued": stats["docs_enqueued"],
            "records_unaccounted": doc_unaccounted,
            "listing_failures": listing_failures,
            "records_listing_at_risk": known_listing_losses,
            "count_parse_failed": stats["count_parse_failed"],
            "records_accounted": accounted_records,
            "reconciled": reconciled,
            "complete": complete,
        }

    def _build_summary(self, reason: str) -> Dict:
        """Full run summary from run_stats. Pure (no I/O)."""
        partition_summaries = [
            self._reconcile_partition(partition, body, stats)
            for (partition, body), stats in sorted(self.run_stats.items())
        ]

        return {
            "spider": self.name,
            "run_id": self.run_id,
            "reason": reason,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "partition_size": self.partition_size,
            "partitions": partition_summaries,
            "totals": {
                "found": sum(stats["found"] or 0 for stats in self.run_stats.values()),
                "scraped": sum(stats["scraped"] for stats in self.run_stats.values()),
                "unchanged": sum(
                    stats["unchanged"] for stats in self.run_stats.values()
                ),
                "failed": sum(
                    len(stats["failed"]) for stats in self.run_stats.values()
                ),
                "parse_failed": sum(
                    len(stats["parse_failures"]) for stats in self.run_stats.values()
                ),
                "skipped": sum(stats["skipped"] for stats in self.run_stats.values()),
                "unaccounted": sum(
                    summary["records_unaccounted"] for summary in partition_summaries
                ),
                "listing_failures": sum(
                    len(stats["listing_failures"]) for stats in self.run_stats.values()
                ),
                "records_listing_at_risk": sum(
                    failure.get("records_at_risk") or 0
                    for stats in self.run_stats.values()
                    for failure in stats["listing_failures"]
                ),
                "reconciled_partitions": sum(
                    1
                    for partition_summary in partition_summaries
                    if partition_summary["reconciled"]
                ),
                "complete_partitions": sum(
                    1
                    for partition_summary in partition_summaries
                    if partition_summary["complete"]
                ),
            },
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }

    def closed(self, reason: str) -> None:
        """Build, log, and persist the final run summary."""

        summary = self._build_summary(reason)

        # Silent losses deserve a loud log line, not just a summary field.
        for partition_summary in summary["partitions"]:
            if partition_summary["records_unaccounted"]:
                self.logger.error(
                    "document requests unaccounted for; possible silent loss",
                    extra={
                        "partition": partition_summary["partition"],
                        "body": partition_summary["body"],
                        "docs_enqueued": partition_summary["docs_enqueued"],
                        "records_scraped": partition_summary["records_scraped"],
                        "records_failed": partition_summary["records_failed"],
                        "records_unaccounted": partition_summary["records_unaccounted"],
                    },
                )

        self.logger.info(
            "run summary",
            extra={"summary": summary},
        )

        try:
            client = get_mongo_client(self.cfg)

            run_logs = client[self.cfg.mongo_db][self.cfg.mongo_run_log_collection]

            run_logs.create_index([("finished_at", -1)])
            run_logs.create_index([("run_id", 1)])

            run_logs.insert_one(json.loads(json.dumps(summary)))

            client.close()

        except Exception as exc:  # noqa: BLE001
            self.logger.warning(
                "could not persist run summary",
                extra={"error": str(exc)},
            )


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _url_extension(url: str) -> Optional[str]:
    path = urlparse(url).path.lower()
    for ext in BINARY_EXTENSIONS:
        if path.endswith(f".{ext}"):
            return ext
    return None


def _norm(value: str) -> str:
    """Strip case and separators so DEC-E2000-14 matches dec-e2000-14.pdf."""
    return re.sub(r"[^a-z0-9]", "", value.lower())


# Magic bytes for binaries stored verbatim. HTML has no reliable signature.
_DOCUMENT_MAGIC: Dict[str, Tuple[bytes, ...]] = {
    "docx": (b"PK\x03\x04",),  # OOXML zip container
    "doc": (b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1",),  # OLE2 compound file
    "rtf": (b"{\\rtf",),
}


def _content_matches_ext(ext: Optional[str], content: bytes) -> bool:
    """Does the payload plausibly match its extension? PDFs are matched
    leniently (%PDF within the first 1 KiB — some carry a preamble); other
    binaries must start with their signature; HTML/unknown always pass."""
    if ext == "pdf":
        return b"%PDF" in content[:1024]

    signatures = _DOCUMENT_MAGIC.get(ext or "")
    if signatures is None:
        return True

    return any(content.startswith(signature) for signature in signatures)


def _ctx(meta: dict) -> dict:
    return {
        "partition_label": meta.get("partition_label"),
        "partition_date": meta.get("partition_date"),
        "body": meta.get("body"),
    }
