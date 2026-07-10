"""Spider for the Workplace Relations "Decisions and Determinations" search.

Strategy
--------
The advanced-search form is an ASP.NET WebForms page, but submitting it
**redirects to a plain GET search endpoint** (confirmed via `scrapy shell`
recon — see docs/recon/wrc-search.md). We therefore skip the brittle VIEWSTATE
form-post entirely and query that endpoint directly:

    GET /en/search/?decisions=1&from=DD/MM/YYYY&to=DD/MM/YYYY&body=<code>&pageNumber=<n>

1. For each (body x date-partition), request page 1 of the results.
2. On page 1, read the total result count ("Shows 1 to 10 of N results") and
   fan out requests for the remaining pages (10 results per page).
3. For every result card (`li.each-item`) extract listing metadata, then
   request the linked document:
      * .pdf / .doc / .docx / .rtf  -> stored verbatim
      * anything else (case .html)  -> the HTML page is fetched and stored
4. Items flow through pipelines: hash -> dedup -> object storage -> MongoDB.

Structured JSON logs include: current partition, body, records found vs
successfully scraped, failed downloads (URL + error/status), and an
end-of-run summary (also persisted to Mongo).

Selectors are anchored on the stable result-card structure discovered in
recon: `li.each-item` with `span.refNO` (identifier), `h2.title a`
(title + link), `span.date` (DD/MM/YYYY) and `p.description@title` (parties).
If the markup changes, re-verify with `scrapy shell` and update the fixtures
in tests/.
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
        # Source netloc is part of every record's natural key (source, body,
        # identifier); compute it once so the spider and the fast-mode dedup
        # set agree with what MongoMetadataPipeline persists.
        self.source: str = urlparse(self.cfg.wrc_base_url).netloc
        self.body_codes: Dict[str, str] = self.cfg.body_code_map
        self.per_page: int = self.cfg.wrc_results_per_page
        self.bodies: List[str] = (
            [b.strip() for b in bodies.split(",")] if bodies else self.cfg.bodies_list
        )
        # Legacy embedded-decision-PDF detection (see _embedded_decision_pdf).
        self.pdf_exclude: List[str] = self.cfg.embedded_pdf_exclude_list
        self.pdf_path_markers: List[str] = self.cfg.embedded_pdf_path_marker_list

        self.partitions: List[Tuple[date, date]] = list(
            iter_partitions(self.start_date, self.end_date, self.partition_size)
        )

        # run statistics keyed by (partition_label, body). "scraped" counts
        # documents that were FULLY PERSISTED (hashed, stored, metadata
        # written) — incremented by MongoMetadataPipeline, not at download
        # time, so a failure in any pipeline stage is never counted as a
        # success. "unchanged" is the subset of scraped whose content hash
        # matched a previous run. Item failures at any stage (download,
        # canonicalization, storage, metadata) are appended to "failed", so
        # every discovered record ends in exactly one auditable state:
        # found == scraped(succeeded, incl. unchanged) + failed.
        self.run_stats: Dict[Tuple[str, str], Dict] = defaultdict(
            lambda: {
                "found": None,
                "scraped": 0,
                "unchanged": 0,
                "failed": [],
                # Listing cards discovered but unusable (missing identifier/link):
                # counted in "found" by the site total, so they must reach an
                # auditable state here rather than being silently dropped.
                "parse_failures": [],
                # Records intentionally not fetched this run (fast-mode skip of an
                # already-known identifier). Counted so reconciliation still holds.
                "skipped": 0,
                # Document requests actually enqueued for download. Reconciled at
                # close against terminal states (scraped + failed); any positive
                # remainder is a silent loss (e.g. a callback-level exception or a
                # request dropped at shutdown) that would otherwise vanish.
                "docs_enqueued": 0,
                "listing_failures": [],
                "count_parse_failed": False,
            }
        )
        # (source, body, identifier) -> file_hash, for fast-mode skip
        self.known_hashes: Dict[Tuple[str, str, str], str] = {}

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        # Only pay the full landing-collection scan when fast mode will actually
        # consult it; the default (hash-compare) mode never reads known_hashes.
        if spider.cfg.skip_existing_identifiers:
            spider._load_known_hashes()

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
        """Preload (source, body, identifier)->hash from Mongo to support the
        optional `skip_existing_identifiers` fast mode. Failure is non-fatal."""
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
        """Build the GET search URL. Dates keep literal slashes (as the site
        emits them); everything is sourced from config."""
        frm = win_start.strftime(self.cfg.wrc_date_format)
        to = win_end.strftime(self.cfg.wrc_date_format)
        # Drop any stale query on the configured search path (e.g. a leftover
        # "?advance=true") so we never build a double-"?" URL.
        base = self.cfg.search_url.split("?", 1)[0]
        return f"{base}?decisions=1&from={frm}&to={to}&body={code}&pageNumber={page}"

    # ------------------------------------------------------------------ #
    # Step 2: parse result pages + fan out remaining pages
    # ------------------------------------------------------------------ #
    def parse_results(self, response: Response):
        meta = response.meta
        key = (meta["partition_label"], meta["body"])

        # Extract once so we can distinguish an empty result set from a broken
        # total-count parser.
        records = list(self._extract_records(response))

        if meta["page"] == 1:
            total = self._extract_total(response)

            if total is None and not records:
                # Successful search page with no result cards means zero records.
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
                # Cards exist but the total-count text was not understood.
                # This is a real parsing problem and must not be reported as zero.
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

            # Request remaining pages only when a positive total was parsed.
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

        # Continue processing the records already extracted above.
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

            # Count the request as enqueued *before* yielding it: closed()
            # reconciles docs_enqueued against terminal states so a request that
            # is later dropped without a callback/errback (silent loss) is still
            # surfaced rather than quietly reducing the scraped count.
            self.run_stats[key]["docs_enqueued"] += 1

            yield scrapy.Request(
                rec["doc_url"],
                callback=self.parse_document,
                errback=self.on_document_failed,
                meta={
                    "item": item,
                    "ext_hint": ext,
                    **_ctx(meta),
                },
                dont_filter=False,
            )

    def _extract_total(self, response: Response) -> Optional[int]:
        """Parse "Shows 1 to 10 of N results" into N (None if not found)."""
        text = " ".join(response.css("::text").getall())
        m = RESULT_COUNT_RE.search(text)
        return int(m.group(1).replace(",", "")) if m else None

    def _extract_records(self, response: Response):
        """Extract result cards from the listing.

        Each card is `<li class="each-item">` with a stable structure:
          * identifier   -> span.refNO
          * title + link -> h2.title a
          * date         -> span.date (DD/MM/YYYY)
          * description  -> p.description@title (parties)
        """
        # A response is only guaranteed to carry meta when tied to a request;
        # fall back to placeholders so parse-failure accounting still works.
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
                # The card counts toward the site's total ("of N results") but we
                # cannot build a document identity from it. Record it as an
                # auditable parse failure so `found` still reconciles to
                # scraped + failed + parse_failures + ... instead of silently
                # dropping the record and leaving an unexplained shortfall.
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
            # Description lives in the title attribute (clean, no child markup);
            # collapse the embedded newlines the site uses between parties.
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
        """Return the absolute URL of a legacy decision PDF embedded in an HTML
        case page, or None.

        A link qualifies when its filename contains the record identifier (e.g.
        Equality's ``DEC-E2000-14.pdf``) or it sits under a known legacy import
        path (e.g. EAT's GUID-named PDFs under ``/eat_import/``). Site-chrome
        PDFs (cookie policy, info guides) are excluded. The first match wins.
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

        # A callback that raises here is only logged to Scrapy's
        # `spider_exceptions` stat and never reaches errback, so the record would
        # vanish from run_stats (neither scraped nor failed). Wrap the body so any
        # unexpected error becomes an auditable document failure instead.
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

            # For a legacy case page the decision is a PDF embedded in the HTML
            # wrapper, not the wrapper itself. Follow that PDF and store it as
            # the authoritative artifact; the wrapper is only an index. Guarded
            # by `pdf_followed` so the fetched PDF (ext=pdf) is never re-scanned.
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
                    yield scrapy.Request(
                        pdf_url,
                        callback=self.parse_document,
                        errback=self.on_document_failed,
                        meta={
                            "item": item,
                            "ext_hint": "pdf",
                            "pdf_followed": True,
                            **_ctx(response.meta),
                        },
                        dont_filter=False,
                    )
                    # The follow-up PDF request produces this record's terminal
                    # state; the wrapper contributes neither scraped nor failed.
                    return

            body = response.body

            # Don't trust the URL extension / Content-Type alone: a 200 response
            # can still be the wrong payload (e.g. a `.pdf` link that returns an
            # HTML "not found" interstitial). Verify the bytes match the expected
            # type via magic bytes; a mismatch is recorded as an auditable failure
            # rather than being stored under a lying extension.
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
        except Exception as exc:  # noqa: BLE001 - keep the record auditable
            self._record_document_failure(
                response,
                item,
                stage="parse",
                error=f"unhandled exception parsing document: {exc!r}",
            )
            return

        # NOTE: "scraped" is NOT incremented here. Success is counted by
        # MongoMetadataPipeline once the document is fully persisted, so a
        # failure in hashing/storage/metadata never masquerades as a success.
        yield item

    def _record_document_failure(
        self,
        response: Response,
        item: "WrcDocumentItem",
        *,
        stage: str,
        error: str,
    ) -> None:
        """Record a document that downloaded (HTTP-wise) but is unusable, so it
        still ends in exactly one auditable state: found = scraped + failed."""
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
        entry = {
            "url": request.url,
            "identifier": meta.get("item", {}).get("identifier"),
            "error": failure.getErrorMessage(),
            "status": status,
        }
        key = (meta.get("partition_label", "?"), meta.get("body", "?"))
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

        if page == 1:
            # The total result count is unavailable.
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
        """Record exceptions raised by hashing, storage, or metadata pipelines."""

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
        """Turn one (partition, body)'s raw counters into an auditable summary.

        Pure: no logging or I/O, so the reconciliation invariant can be unit
        tested directly. Every discovered record must land in exactly one bucket.
        """
        document_failures = stats["failed"]
        parse_failures = stats["parse_failures"]
        listing_failures = stats["listing_failures"]

        known_listing_losses = sum(
            failure.get("records_at_risk") or 0 for failure in listing_failures
        )

        unknown_listing_loss = any(
            failure.get("records_at_risk") is None for failure in listing_failures
        )

        # Document requests we enqueued but that never reached a terminal state
        # (scraped, unchanged-counted-as-scraped, or a recorded failure). A
        # positive value is a silent loss — e.g. a request dropped by the
        # dupefilter/at shutdown, or a callback exception that never surfaced —
        # and is reported so it can never quietly shrink the scraped count.
        doc_unaccounted = max(
            stats["docs_enqueued"] - stats["scraped"] - len(document_failures),
            0,
        )

        # Every discovered record must land in exactly one auditable bucket:
        # scraped (incl. unchanged) + document failures + unusable listing cards
        # + intentionally-skipped + records lost on failed listing pages +
        # silently-lost document requests.
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
        """Assemble the full run summary from run_stats. Pure (no I/O)."""
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

        # Escalate any silent document losses so they are visible in logs, not
        # only in the persisted summary.
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
    """Lowercase and strip all non-alphanumerics, so an identifier can be
    matched inside a filename regardless of separators/case
    (``DEC-E2000-14`` -> ``dece200014`` matches ``dec-e2000-14.pdf``)."""
    return re.sub(r"[^a-z0-9]", "", value.lower())


# Leading "magic bytes" for the binary types we store verbatim. HTML (and any
# other/unknown ext) has no reliable signature, so it is not signature-checked.
_DOCUMENT_MAGIC: Dict[str, Tuple[bytes, ...]] = {
    "docx": (b"PK\x03\x04",),  # OOXML is a zip container
    "doc": (b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1",),  # OLE2 compound file
    "rtf": (b"{\\rtf",),
}


def _content_matches_ext(ext: Optional[str], content: bytes) -> bool:
    """True if ``content`` plausibly matches ``ext`` by its magic bytes.

    PDFs are matched leniently (``%PDF`` anywhere in the first 1 KiB, as some
    files carry a short preamble); other binaries must start with their exact
    signature. HTML and unknown extensions have no signature and always pass —
    HTML content is validated later by the canonicalizer's text-length gate.
    """
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
