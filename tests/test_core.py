"""Unit tests for the pure-Python parts of the pipeline
(no network, no databases required)."""
from datetime import date

import pytest

from config.common import iter_partitions, parse_cli_date, sha256_bytes
from transform.transform import extract_relevant_html


# --------------------------------------------------------------------------- #
# Partitioning
# --------------------------------------------------------------------------- #
def test_monthly_partitions_cover_range_without_gaps_or_overlaps():
    parts = list(iter_partitions(date(2024, 1, 15), date(2024, 4, 10), "monthly"))
    assert parts == [
        (date(2024, 1, 15), date(2024, 1, 31)),
        (date(2024, 2, 1), date(2024, 2, 29)),
        (date(2024, 3, 1), date(2024, 3, 31)),
        (date(2024, 4, 1), date(2024, 4, 10)),
    ]


def test_weekly_and_daily_partitions():
    weekly = list(iter_partitions(date(2024, 1, 1), date(2024, 1, 15), "weekly"))
    assert weekly[0] == (date(2024, 1, 1), date(2024, 1, 7))
    assert weekly[-1][1] == date(2024, 1, 15)

    daily = list(iter_partitions(date(2024, 1, 1), date(2024, 1, 3), "daily"))
    assert len(daily) == 3


def test_invalid_range_raises():
    with pytest.raises(ValueError):
        list(iter_partitions(date(2024, 2, 1), date(2024, 1, 1)))


def test_parse_cli_date_accepts_both_orders():
    assert parse_cli_date("2024-01-05") == date(2024, 1, 5)
    assert parse_cli_date("05-01-2024") == date(2024, 1, 5)


# --------------------------------------------------------------------------- #
# Hashing
# --------------------------------------------------------------------------- #
def test_sha256_is_deterministic_and_change_sensitive():
    a = sha256_bytes(b"hello")
    assert a == sha256_bytes(b"hello")
    assert a != sha256_bytes(b"hello!")


# --------------------------------------------------------------------------- #
# HTML content extraction
# --------------------------------------------------------------------------- #
SAMPLE_HTML = b"""
<html><head><title>IR - SC - 00001595</title>
<script>alert('x')</script></head>
<body>
  <nav><a href="/">Home</a><a href="/search">Search</a></nav>
  <header>Workplace Relations Commission</header>
  <div id="main" class="wrapper" style="color:red">
    <h1>ADJUDICATION OFFICER Recommendation</h1>
    <p>%s</p>
    <table><tr><td>Worker</td><td>Employer</td></tr></table>
  </div>
  <footer>Contact us | Privacy</footer>
  <form><button>Search</button></form>
</body></html>
""" % (b"Decision body text. " * 30)


def test_extract_relevant_html_strips_boilerplate_and_keeps_content():
    out = extract_relevant_html(SAMPLE_HTML, ["#main", "main", "article"])
    text = out.decode()
    assert "ADJUDICATION OFFICER Recommendation" in text
    assert "Decision body text." in text
    assert "Worker" in text                      # table content preserved
    for boilerplate in ("nav", "footer", "alert('x')", "Contact us", "Home"):
        assert boilerplate not in text or f"<{boilerplate}" not in text
    assert "<nav" not in text and "<footer" not in text and "<script" not in text
    assert 'style="color:red"' not in text       # presentational attrs dropped


def test_extract_relevant_html_falls_back_to_body():
    html = b"<html><body><p>" + b"fallback content " * 40 + b"</p></body></html>"
    out = extract_relevant_html(html, ["#does-not-exist"])
    assert b"fallback content" in out
