"""Unit tests for the spider's pure parsing logic, using a fixture captured
from the live WRC results page (no network, no DB)."""

from datetime import date

from scrapy.http import HtmlResponse, Request

from wrc_scraper.spiders.wrc_spider import WrcSpider

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


def test_search_url_uses_config_and_literal_date_slashes():
    url = _spider()._search_url(date(2024, 1, 1), date(2024, 1, 31), "3", 2)
    assert url == (
        "https://www.workplacerelations.ie/en/search/"
        "?decisions=1&from=01/01/2024&to=31/01/2024&body=3&pageNumber=2"
    )


def test_body_code_map_parses_configured_pairs():
    codes = _spider().cfg.body_code_map
    assert codes["Labour Court"] == "3"
    assert codes["Workplace Relations Commission"] == "15376"
    assert codes["Equality Tribunal"] == "1"
    assert codes["Employment Appeals Tribunal"] == "2"
