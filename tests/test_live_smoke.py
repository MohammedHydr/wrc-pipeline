"""Opt-in live smoke test of the WRC search contract.

Skipped by default so the suite never depends on the live site (per the
testing conventions). Enable explicitly before an important run/submission:

    WRC_LIVE_SMOKE=1 pytest tests/test_live_smoke.py -v

One polite GET against a tiny historical window verifies the assumptions the
spider is built on: the GET search endpoint answers 200, the result-count
sentence parses, and result cards still expose identifier + link. If this
fails, re-run the recon in docs/recon/wrc-search.md before crawling.
"""

from __future__ import annotations

import os
import urllib.request

import pytest
from parsel import Selector

from config.settings import get_settings
from scraper.wrc_scraper.spiders.wrc_spider import RESULT_COUNT_RE

pytestmark = pytest.mark.skipif(
    os.environ.get("WRC_LIVE_SMOKE") != "1",
    reason="live-site smoke test; set WRC_LIVE_SMOKE=1 to run",
)


def test_search_endpoint_contract_still_holds():
    cfg = get_settings()
    # Small, stable historical window (Labour Court, January 2024).
    url = (
        f"{cfg.search_url.split('?', 1)[0]}"
        "?decisions=1&from=01/01/2024&to=31/01/2024&body=3&pageNumber=1"
    )
    request = urllib.request.Request(url, headers={"User-Agent": cfg.user_agent})
    with urllib.request.urlopen(request, timeout=30) as response:
        assert response.status == 200
        html = response.read().decode("utf-8", "replace")

    selector = Selector(text=html)

    # "Shows 1 to 10 of N results" still parses (pagination math depends on it).
    text = " ".join(selector.css("::text").getall())
    match = RESULT_COUNT_RE.search(text)
    assert match, "result-count sentence not found — re-run recon"
    assert int(match.group(1).replace(",", "")) > 0

    # Result cards still expose identifier and a document link.
    cards = selector.css("li.each-item")
    assert cards, "no li.each-item result cards — markup changed"
    first = cards[0]
    assert (first.css("span.refNO::text").get() or "").strip()
    assert (
        first.css("h2.title a::attr(href)").get()
        or first.css("div.bottom-ref a::attr(href)").get()
    )
