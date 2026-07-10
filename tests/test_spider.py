"""Unit tests for the spider's pure parsing logic, using a fixture captured
from the live WRC results page (no network, no DB)."""

from datetime import date

from scrapy.http import HtmlResponse, Request

from scraper.wrc_scraper.items import WrcDocumentItem
from scraper.wrc_scraper.spiders import WrcSpider
from scraper.wrc_scraper.spiders import wrc_spider as wrc_spider_mod

# Two result cards + the count line, mirroring the exact live markup observed
# via `scrapy shell` (li.each-item / span.refNO / h2.title a / span.date /
# p.description@title). The description carries the site's embedded newlines.
RESULTS_HTML = """
<html><body>
  <div class="search-info">Shows 1 to 10 of 45 results</div>
  <ul class="results">
    <li class="each-item clearfix">
      <div class="row">
        <div class="col-sm-9">
          <h2 class="title" title="LCR22912"><a href="/en/cases/2024/february/lcr22912.html" title="LCR22912">LCR22912</a></h2>
        </div>
        <div class="col-sm-3"><span class="date">30/01/2024</span></div>
      </div>
      <p class="fullpath" title="/en/cases/2024/february/lcr22912.html"></p>
      <p class="description" title="SONOMA VALLEY
AND


A WORKER">SONOMA VALLEY AND A WORKER</p>
      <div class="row bottom-ref">
        <div class="col-sm-9 ref"><span>Ref no: </span><span class="refNO">LCR22912</span></div>
        <div class="col-sm-3 link"><a class="btn btn-primary" href="/en/cases/2024/february/lcr22912.html">View Page</a></div>
      </div>
    </li>
    <li class="each-item clearfix">
      <div class="row">
        <div class="col-sm-9">
          <h2 class="title" title="ADJ-00047352"><a href="/en/cases/2024/january/adj-00047352.html">ADJ-00047352</a></h2>
        </div>
        <div class="col-sm-3"><span class="date">15/01/2024</span></div>
      </div>
      <p class="description" title="A Worker v An Employer">A Worker v An Employer</p>
      <div class="row bottom-ref">
        <div class="col-sm-9 ref"><span>Ref no: </span><span class="refNO">ADJ-00047352</span></div>
        <div class="col-sm-3 link"><a class="btn btn-primary" href="/en/cases/2024/january/adj-00047352.html">View Page</a></div>
      </div>
    </li>
  </ul>
</body></html>
"""

SEARCH_URL = (
    "https://www.workplacerelations.ie/en/search/"
    "?decisions=1&from=01/01/2024&to=31/01/2024&body=3&pageNumber=1"
)


def _spider() -> WrcSpider:
    # Direct construction (no from_crawler) avoids the Mongo preload.
    return WrcSpider(start_date="2024-01-01", end_date="2024-01-31")


def _response() -> HtmlResponse:
    return HtmlResponse(
        url=SEARCH_URL,
        body=RESULTS_HTML.encode("utf-8"),
        encoding="utf-8",
        request=Request(SEARCH_URL),
    )


def test_extract_total_parses_result_count():
    assert _spider()._extract_total(_response()) == 45


def test_extract_records_pulls_clean_metadata():
    records = list(_spider()._extract_records(_response()))
    assert len(records) == 2

    first = records[0]
    assert first["identifier"] == "LCR22912"
    assert first["title"] == "LCR22912"
    # embedded newlines in the description are collapsed to single spaces
    assert first["description"] == "SONOMA VALLEY AND A WORKER"
    assert first["published_date"] == "2024-01-30"
    assert first["doc_url"] == (
        "https://www.workplacerelations.ie/en/cases/2024/february/lcr22912.html"
    )

    assert records[1]["identifier"] == "ADJ-00047352"
    assert records[1]["published_date"] == "2024-01-15"


def test_extract_records_skips_card_without_identifier():
    html = (
        "<html><body><li class='each-item'>"
        "<h2 class='title'><a href='/en/cases/x.html'>X</a></h2>"
        "</li></body></html>"
    )
    resp = HtmlResponse(url=SEARCH_URL, body=html.encode(), encoding="utf-8")
    assert list(_spider()._extract_records(resp)) == []


def test_extract_records_records_malformed_card_as_parse_failure():
    # A card with no ref number cannot become a document identity, but it still
    # counts toward the site's total; it must be recorded, not silently dropped.
    html = (
        "<html><body><li class='each-item'>"
        "<h2 class='title'><a href='/en/cases/x.html'>X</a></h2>"
        "</li></body></html>"
    )
    request = Request(
        SEARCH_URL,
        meta={"partition_label": "2024-01-01_2024-01-31", "body": "Labour Court"},
    )
    resp = HtmlResponse(
        url=SEARCH_URL, body=html.encode(), encoding="utf-8", request=request
    )
    spider = _spider()
    records = list(spider._extract_records(resp))

    assert records == []
    key = ("2024-01-01_2024-01-31", "Labour Court")
    parse_failures = spider.run_stats[key]["parse_failures"]
    assert len(parse_failures) == 1
    assert parse_failures[0]["stage"] == "listing_parse"
    assert parse_failures[0]["identifier"] is None


def test_search_url_uses_config_and_literal_date_slashes():
    url = _spider()._search_url(date(2024, 1, 1), date(2024, 1, 31), "3", 2)
    assert url == (
        "https://www.workplacerelations.ie/en/search/"
        "?decisions=1&from=01/01/2024&to=31/01/2024&body=3&pageNumber=2"
    )


def _stats(**overrides):
    """A run_stats entry with all buckets zeroed, then overridden."""
    base = {
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
    base.update(overrides)
    return base


def _summary_for(**stats):
    spider = _spider()
    spider.run_stats[("2022-04-01_2022-04-30", "Workplace Relations Commission")] = (
        _stats(**stats)
    )
    return spider._build_summary("finished")["partitions"][0]


def test_clean_partition_reconciles_and_completes():
    p = _summary_for(found=2, scraped=2, docs_enqueued=2)
    assert p["reconciled"] and p["complete"]
    assert p["records_accounted"] == 2
    assert p["records_unaccounted"] == 0


def test_malformed_card_reconciles_but_is_incomplete():
    # December 2022 signature: a listing card with no identifier/link.
    p = _summary_for(
        found=3,
        scraped=2,
        docs_enqueued=2,
        parse_failures=[{"stage": "listing_parse", "error": "missing id"}],
    )
    assert p["reconciled"]  # 2 scraped + 1 parse_failure == 3 found
    assert not p["complete"]  # but not clean
    assert p["records_parse_failed"] == 1
    assert p["records_unaccounted"] == 0


def test_silent_document_loss_is_surfaced_as_unaccounted():
    # April 2022 signature: a well-formed record enqueued for download that
    # never reached a terminal state (no scrape, no recorded failure).
    p = _summary_for(found=3, scraped=2, docs_enqueued=3)
    assert p["records_unaccounted"] == 1
    assert p["reconciled"]  # the gap is now explicitly accounted
    assert not p["complete"]


def test_fast_mode_skip_keeps_partition_reconciled():
    p = _summary_for(found=5, scraped=3, docs_enqueued=3, skipped=2)
    assert p["reconciled"]
    assert p["records_skipped"] == 2


def test_unexplained_shortfall_fails_reconciliation():
    # A record lost before it was ever enqueued (not skipped, not a parse
    # failure) leaves the denominator short and must NOT reconcile.
    p = _summary_for(found=3, scraped=2, docs_enqueued=2)
    assert not p["reconciled"]
    assert p["records_accounted"] == 2


def _document_response(ext_hint, body, identifier="ADJ-1", url="http://x/y"):
    key = ("2022-04-01_2022-04-30", "Workplace Relations Commission")
    item = WrcDocumentItem(
        identifier=identifier, body=key[1], partition_label=key[0], doc_url=url
    )
    request = Request(
        url,
        meta={
            "item": item,
            "ext_hint": ext_hint,
            "partition_label": key[0],
            "body": key[1],
        },
    )
    return key, HtmlResponse(url=url, body=body, request=request, encoding="utf-8")


# Legacy case pages: an HTML wrapper that links to the authoritative decision
# PDF, plus the two site-chrome PDFs present on every page.
EQUALITY_CASE_HTML = b"""
<html><body>
  <a href="/en/privacy-policy/cookie_policy.pdf">Cookie policy</a>
  <a href="/en/Publications_Forms/Decisions_Information_Guide.pdf">Guide</a>
  <a href="/en/Equality_Tribunal_Import/Database-of-Decisions/2000/DEC-E2000-14.pdf">
     Download the decision</a>
</body></html>
"""

EAT_CASE_HTML = b"""
<html><body>
  <a href="/en/privacy-policy/cookie_policy.pdf">Cookie policy</a>
  <a href="/en/eat_import/2008/12/674e1a97-d24e-4513-b48d-ad46343febb5.pdf">Download</a>
</body></html>
"""

MODERN_CASE_HTML = b"""
<html><body>
  <a href="/en/privacy-policy/cookie_policy.pdf">Cookie policy</a>
  <h1>ADJ-00036994</h1><p>Full decision text lives here in HTML.</p>
</body></html>
"""


def test_embedded_pdf_matches_identifier_and_skips_chrome():
    spider = _spider()
    _, resp = _document_response(None, EQUALITY_CASE_HTML, identifier="DEC-E2000-14")
    url = spider._embedded_decision_pdf(resp, "DEC-E2000-14")
    assert url.endswith("/Database-of-Decisions/2000/DEC-E2000-14.pdf")


def test_embedded_pdf_matches_legacy_import_path_for_guid_named_file():
    spider = _spider()
    _, resp = _document_response(None, EAT_CASE_HTML, identifier="31482")
    url = spider._embedded_decision_pdf(resp, "31482")
    assert url.endswith("/eat_import/2008/12/674e1a97-d24e-4513-b48d-ad46343febb5.pdf")


def test_embedded_pdf_returns_none_for_modern_html_only_page():
    spider = _spider()
    _, resp = _document_response(None, MODERN_CASE_HTML, identifier="ADJ-00036994")
    assert spider._embedded_decision_pdf(resp, "ADJ-00036994") is None


def test_parse_document_follows_embedded_pdf_instead_of_storing_wrapper():
    spider = _spider()
    _, resp = _document_response(None, EQUALITY_CASE_HTML, identifier="DEC-E2000-14")
    out = list(spider.parse_document(resp))
    # One follow-up request to the PDF, and NO item for the HTML wrapper.
    assert len(out) == 1
    req = out[0]
    assert isinstance(req, Request)
    assert req.url.endswith("/DEC-E2000-14.pdf")
    assert req.meta["ext_hint"] == "pdf"
    assert req.meta["pdf_followed"] is True
    assert req.meta["item"]["identifier"] == "DEC-E2000-14"


def test_parse_document_stores_html_when_no_embedded_pdf():
    spider = _spider()
    _, resp = _document_response(None, MODERN_CASE_HTML, identifier="ADJ-00036994")
    out = list(spider.parse_document(resp))
    assert len(out) == 1
    assert out[0]["file_ext"] == "html"
    assert out[0]["file_content"] == MODERN_CASE_HTML


def test_parse_document_stores_followed_pdf_bytes():
    spider = _spider()
    # The followed PDF response carries pdf_followed=True so it is not re-scanned.
    _, resp = _document_response(
        "pdf", b"%PDF-1.7\nlegacy decision\n%%EOF", identifier="DEC-E2000-14"
    )
    resp.meta["pdf_followed"] = True
    out = list(spider.parse_document(resp))
    assert len(out) == 1
    assert out[0]["file_ext"] == "pdf"
    assert out[0]["file_content"].startswith(b"%PDF")


def test_parse_document_validation_mismatch_is_recorded():
    spider = _spider()
    key, response = _document_response("pdf", b"not a pdf at all")
    assert list(spider.parse_document(response)) == []
    failures = spider.run_stats[key]["failed"]
    assert len(failures) == 1
    assert failures[0]["stage"] == "validation"


def test_parse_document_unhandled_exception_is_recorded_not_silent(monkeypatch):
    # An exception in the callback would otherwise only hit Scrapy's
    # spider_exceptions stat and vanish from run_stats. It must become a
    # recorded document failure so the record stays auditable.
    spider = _spider()
    key, response = _document_response("html", b"<html>ok</html>")

    def boom(*_args, **_kwargs):
        raise RuntimeError("kaboom")

    monkeypatch.setattr(wrc_spider_mod, "_content_matches_ext", boom)
    assert list(spider.parse_document(response)) == []
    failures = spider.run_stats[key]["failed"]
    assert len(failures) == 1
    assert failures[0]["stage"] == "parse"
    assert "kaboom" in failures[0]["error"]


def test_body_code_map_parses_configured_pairs():
    codes = _spider().cfg.body_code_map
    assert codes["Labour Court"] == "3"
    assert codes["Workplace Relations Commission"] == "15376"
    assert codes["Equality Tribunal"] == "1"
    assert codes["Employment Appeals Tribunal"] == "2"


class _FakeFailure:
    """Minimal stand-in for a twisted Failure passed to an errback."""

    def __init__(self, request, message="Forbidden by robots.txt"):
        self.request = request
        self.value = Exception(message)
        self._message = message

    def getErrorMessage(self) -> str:
        return self._message


def test_pdf_follow_failure_falls_back_to_storing_html_wrapper():
    # robots.txt forbids the legacy Equality_Tribunal_Import PDF paths; the
    # errback must store the permitted HTML case page instead of failing.
    spider = _spider()
    key = ("2000-01-01_2000-01-31", "Equality Tribunal")
    item = WrcDocumentItem(
        identifier="DEC-E2000-14", body=key[1], partition_label=key[0]
    )
    request = Request(
        "http://x/Equality_Tribunal_Import/DEC-E2000-14.pdf",
        meta={
            "item": item,
            "ext_hint": "pdf",
            "pdf_followed": True,
            "wrapper_body": EQUALITY_CASE_HTML,
            "wrapper_content_type": "text/html",
            "partition_label": key[0],
            "body": key[1],
        },
    )
    out = list(spider.on_document_failed(_FakeFailure(request)))
    assert len(out) == 1
    assert out[0]["file_ext"] == "html"
    assert out[0]["file_content"] == EQUALITY_CASE_HTML
    # Not a failure: the wrapper item proceeds through the pipelines and is
    # counted as scraped once fully persisted.
    assert spider.run_stats[key]["failed"] == []


def test_document_failure_without_fallback_payload_is_recorded():
    spider = _spider()
    key = ("2022-04-01_2022-04-30", "Workplace Relations Commission")
    item = WrcDocumentItem(identifier="ADJ-1", body=key[1], partition_label=key[0])
    request = Request(
        "http://x/y.html",
        meta={"item": item, "partition_label": key[0], "body": key[1]},
    )
    out = list(spider.on_document_failed(_FakeFailure(request, "504 timeout")))
    assert out == []
    failures = spider.run_stats[key]["failed"]
    assert len(failures) == 1
    assert failures[0]["identifier"] == "ADJ-1"
    assert "504" in failures[0]["error"]
