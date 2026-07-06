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
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import scrapy
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

        self.partitions: List[Tuple[date, date]] = list(
            iter_partitions(self.start_date, self.end_date, self.partition_size)
        )

        # run statistics keyed by (partition_label, body). "scraped" counts
        # successfully downloaded documents; "unchanged" (incremented by the
        # dedup pipeline) is the subset of those whose hash matched a previous
        # run, so every discovered record ends in exactly one auditable state:
        # found == scraped(succeeded, incl. unchanged) + failed.
        self.run_stats: Dict[Tuple[str, str], Dict] = defaultdict(
            lambda: {"found": None, "scraped": 0, "unchanged": 0, "failed": []}
        )
        # (source, body, identifier) -> file_hash, for fast-mode skip
        self.known_hashes: Dict[Tuple[str, str, str], str] = {}

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        spider._load_known_hashes()
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
    def start_requests(self) -> Iterable[scrapy.Request]:
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

        if meta["page"] == 1:
            total = self._extract_total(response)
            self.run_stats[key]["found"] = total
            self.logger.info(
                "search results",
                extra={
                    "partition": meta["partition_label"],
                    "body": meta["body"],
                    "records_found": total,
                },
            )
            # Fan out the remaining pages now that we know the total.
            if total:
                pages = math.ceil(total / self.per_page)
                for page in range(2, pages + 1):
                    url = self._search_url(
                        meta["win_start"], meta["win_end"], meta["body_code"], page
                    )
                    yield scrapy.Request(
                        url,
                        callback=self.parse_results,
                        errback=self.on_request_failed,
                        dont_filter=True,
                        meta={**meta, "page": page},
                    )

        for rec in self._extract_records(response):
            identifier = rec["identifier"]
            nat_key = (self.source, meta["body"], identifier)
            if self.cfg.skip_existing_identifiers and nat_key in self.known_hashes:
                self.logger.info(
                    "skipping existing identifier (fast mode)",
                    extra={"identifier": identifier, **_ctx(meta)},
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
            yield scrapy.Request(
                rec["doc_url"],
                callback=self.parse_document,
                errback=self.on_document_failed,
                meta={"item": item, "ext_hint": ext, **_ctx(meta)},
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
        cards = response.css("li.each-item")
        for card in cards:
            identifier = (card.css("span.refNO::text").get() or "").strip()
            href = (
                card.css("div.bottom-ref a::attr(href)").get()
                or card.css("h2.title a::attr(href)").get()
            )
            if not identifier or not href:
                self.logger.warning(
                    "result card missing identifier or link; skipped",
                    extra={"identifier": identifier or None, "href": href},
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

    # ------------------------------------------------------------------ #
    # Step 3: download the document itself
    # ------------------------------------------------------------------ #
    def parse_document(self, response: Response):
        item: WrcDocumentItem = response.meta["item"]
        content_type = (
            response.headers.get("Content-Type", b"").decode("latin-1").lower()
        )
        ext = response.meta.get("ext_hint")
        if not ext:
            if "pdf" in content_type:
                ext = "pdf"
            elif "msword" in content_type or "officedocument" in content_type:
                ext = "docx" if "officedocument" in content_type else "doc"
            else:
                ext = "html"

        item["file_content"] = response.body
        item["file_ext"] = ext
        item["content_type"] = content_type

        key = (item["partition_label"], item["body"])
        self.run_stats[key]["scraped"] += 1
        yield item

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
        self.logger.error(
            "request failed",
            extra={
                "url": failure.request.url,
                "error": failure.getErrorMessage(),
            },
        )

    # ------------------------------------------------------------------ #
    # Step 4: end-of-run summary
    # ------------------------------------------------------------------ #
    def closed(self, reason: str):
        summary = {
            "spider": self.name,
            "run_id": self.run_id,
            "reason": reason,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "partition_size": self.partition_size,
            "partitions": [
                {
                    "partition": p,
                    "body": b,
                    "records_found": s["found"],
                    "records_scraped": s["scraped"],
                    "records_unchanged": s["unchanged"],
                    "records_failed": len(s["failed"]),
                    "failures": s["failed"],
                }
                for (p, b), s in sorted(self.run_stats.items())
            ],
            "totals": {
                "found": sum(s["found"] or 0 for s in self.run_stats.values()),
                "scraped": sum(s["scraped"] for s in self.run_stats.values()),
                "unchanged": sum(s["unchanged"] for s in self.run_stats.values()),
                "failed": sum(len(s["failed"]) for s in self.run_stats.values()),
            },
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }
        self.logger.info("run summary", extra={"summary": summary})
        try:
            client = get_mongo_client(self.cfg)
            run_logs = client[self.cfg.mongo_db][self.cfg.mongo_run_log_collection]
            # Indexes for the common observability queries: latest run, by id.
            run_logs.create_index([("finished_at", -1)])
            run_logs.create_index([("run_id", 1)])
            run_logs.insert_one(json.loads(json.dumps(summary)))
            client.close()
        except Exception as exc:  # noqa: BLE001
            self.logger.warning(
                "could not persist run summary", extra={"error": str(exc)}
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


def _ctx(meta: dict) -> dict:
    return {
        "partition_label": meta.get("partition_label"),
        "partition_date": meta.get("partition_date"),
        "body": meta.get("body"),
    }
