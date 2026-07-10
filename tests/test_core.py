"""Unit tests for the pure-Python parts of the pipeline.

These tests require no network, MongoDB, or object storage.
"""

from datetime import date

import pytest

from config.common import iter_partitions, parse_cli_date, sha256_bytes
from config.html_utils import canonicalize_html


# --------------------------------------------------------------------------- #
# Partitioning
# --------------------------------------------------------------------------- #
def test_monthly_partitions_cover_range_without_gaps_or_overlaps() -> None:
    parts = list(
        iter_partitions(
            date(2024, 1, 15),
            date(2024, 4, 10),
            "monthly",
        )
    )

    assert parts == [
        (date(2024, 1, 15), date(2024, 1, 31)),
        (date(2024, 2, 1), date(2024, 2, 29)),
        (date(2024, 3, 1), date(2024, 3, 31)),
        (date(2024, 4, 1), date(2024, 4, 10)),
    ]


def test_weekly_and_daily_partitions() -> None:
    weekly = list(
        iter_partitions(
            date(2024, 1, 1),
            date(2024, 1, 15),
            "weekly",
        )
    )

    assert weekly == [
        (date(2024, 1, 1), date(2024, 1, 7)),
        (date(2024, 1, 8), date(2024, 1, 14)),
        (date(2024, 1, 15), date(2024, 1, 15)),
    ]

    daily = list(
        iter_partitions(
            date(2024, 1, 1),
            date(2024, 1, 3),
            "daily",
        )
    )

    assert daily == [
        (date(2024, 1, 1), date(2024, 1, 1)),
        (date(2024, 1, 2), date(2024, 1, 2)),
        (date(2024, 1, 3), date(2024, 1, 3)),
    ]


def test_invalid_range_raises() -> None:
    with pytest.raises(ValueError):
        list(
            iter_partitions(
                date(2024, 2, 1),
                date(2024, 1, 1),
            )
        )


def test_parse_cli_date_accepts_supported_formats() -> None:
    assert parse_cli_date("2024-01-05") == date(2024, 1, 5)
    assert parse_cli_date("05-01-2024") == date(2024, 1, 5)
    assert parse_cli_date("05/01/2024") == date(2024, 1, 5)


def test_parse_cli_date_rejects_invalid_value() -> None:
    with pytest.raises(ValueError):
        parse_cli_date("not-a-date")


# --------------------------------------------------------------------------- #
# Hashing
# --------------------------------------------------------------------------- #
def test_sha256_is_deterministic_and_change_sensitive() -> None:
    original_hash = sha256_bytes(b"hello")

    assert original_hash == sha256_bytes(b"hello")
    assert original_hash != sha256_bytes(b"hello!")


# --------------------------------------------------------------------------- #
# HTML content extraction
# --------------------------------------------------------------------------- #
SAMPLE_HTML = b"""
<html>
  <head>
    <title>IR - SC - 00001595</title>
    <script nonce="volatile-request-id">alert('x')</script>
  </head>

  <body>
    <nav>
      <a href="/">Home</a>
      <a href="/search">Search</a>
    </nav>

    <header>Workplace Relations Commission</header>

    <div
      id="main"
      class="wrapper"
      style="color:red"
      data-request-id="volatile-123"
    >
      <h1>ADJUDICATION OFFICER Recommendation</h1>

      <p>%s</p>

      <table class="styled-table">
        <tr>
          <th colspan="2">Parties</th>
        </tr>
        <tr>
          <td>Worker</td>
          <td>Employer</td>
        </tr>
      </table>
    </div>

    <footer>Contact us | Privacy</footer>

    <form>
      <button>Search</button>
    </form>
  </body>
</html>
""" % (b"Decision body text. " * 30)


def test_canonical_html_strips_boilerplate_and_keeps_content() -> None:
    result = canonicalize_html(
        SAMPLE_HTML,
        selectors=["#main", "main", "article"],
        parser="lxml",
        min_text_chars=200,
    )

    output = result.html_bytes.decode("utf-8")

    assert "ADJUDICATION OFFICER Recommendation" in output
    assert "Decision body text." in output
    assert "Worker" in output
    assert "Employer" in output
    assert 'colspan="2"' in output

    assert "<nav" not in output
    assert "<footer" not in output
    assert "<header" not in output
    assert "<script" not in output
    assert "<form" not in output
    assert "<button" not in output

    assert "Contact us" not in output
    assert "alert('x')" not in output
    assert 'style="color:red"' not in output
    assert 'class="wrapper"' not in output
    assert "volatile-request-id" not in output
    assert "volatile-123" not in output

    assert result.selector_used == "#main"
    assert result.fallback_used is False
    assert result.text_length >= 200
    assert result.parser_used == "lxml"


def test_canonical_html_falls_back_to_body() -> None:
    raw = (
        b"<html><head><title>Fallback</title></head><body>"
        b"<p>" + b"fallback decision content " * 40 + b"</p></body></html>"
    )

    result = canonicalize_html(
        raw,
        selectors=["#does-not-exist"],
        parser="lxml",
        min_text_chars=200,
    )

    output = result.html_bytes.decode("utf-8")

    assert "fallback decision content" in output
    assert result.selector_used is None
    assert result.fallback_used is True

    # The canonicalizer must not create <body><body>...</body></body>.
    assert output.count("<body>") == 1
    assert output.count("</body>") == 1


def test_canonical_html_is_stable_when_page_chrome_changes() -> None:
    first = b"""
    <html>
      <head>
        <title>Decision 1</title>
        <script nonce="request-1">trackingOne()</script>
      </head>
      <body>
        <header>Header generated for request 1</header>
        <main id="dynamic-1" class="layout-a">
          <h1>ADJ-00000001</h1>
          <p>%s</p>
        </main>
        <footer>Footer generated for request 1</footer>
      </body>
    </html>
    """ % (b"The legal decision content remains unchanged. " * 20)

    second = b"""
    <html>
      <head>
        <title>Decision 1</title>
        <script nonce="request-2">trackingTwo()</script>
      </head>
      <body>
        <header>Completely different website header</header>
        <main id="dynamic-999" class="layout-b" onclick="track()">
          <h1>ADJ-00000001</h1>
          <p>%s</p>
        </main>
        <footer>Completely different website footer</footer>
      </body>
    </html>
    """ % (b"The legal decision content remains unchanged. " * 20)

    first_result = canonicalize_html(
        first,
        selectors=["main"],
        min_text_chars=200,
    )
    second_result = canonicalize_html(
        second,
        selectors=["main"],
        min_text_chars=200,
    )

    assert first_result.content_bytes == second_result.content_bytes
    assert first_result.html_bytes == second_result.html_bytes

    assert sha256_bytes(first_result.content_bytes) == sha256_bytes(
        second_result.content_bytes
    )
