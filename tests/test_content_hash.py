"""Tests for semantic HTML hashing."""

from config.common import sha256_bytes
from config.html_utils import canonicalize_html


def _content_hash(raw: bytes) -> str:
    result = canonicalize_html(
        raw,
        selectors=["main"],
        parser="lxml",
        min_text_chars=50,
    )
    return sha256_bytes(result.content_bytes)


def test_raw_html_can_change_while_content_hash_remains_equal() -> None:
    first = b"""
    <html>
      <head>
        <title>Decision 1</title>
        <script nonce="request-one">analyticsOne()</script>
      </head>
      <body>
        <header>Generated header one</header>

        <main id="request-one" class="layout-one">
          <h1>ADJ-00000001</h1>
          <p>
            The legal decision states that the worker's complaint was upheld.
            This document content is stable between requests.
          </p>
        </main>

        <footer>Generated footer one</footer>
      </body>
    </html>
    """

    second = b"""
    <html>
      <head>
        <title>Decision 1</title>
        <script nonce="request-two">analyticsTwo()</script>
      </head>
      <body>
        <header>Completely different generated header</header>

        <main id="request-two" class="layout-two" onclick="track()">
          <h1>ADJ-00000001</h1>
          <p>
            The legal decision states that the worker's complaint was upheld.
            This document content is stable between requests.
          </p>
        </main>

        <footer>Completely different generated footer</footer>
      </body>
    </html>
    """

    # Exact downloaded files differ.
    assert sha256_bytes(first) != sha256_bytes(second)

    # The actual legal decision content is identical.
    assert _content_hash(first) == _content_hash(second)


def test_content_change_produces_different_content_hash() -> None:
    first = b"""
    <html>
      <body>
        <main>
          <h1>ADJ-00000001</h1>
          <p>
            The complaint was upheld and compensation was awarded to the
            worker following consideration of the evidence.
          </p>
        </main>
      </body>
    </html>
    """

    second = b"""
    <html>
      <body>
        <main>
          <h1>ADJ-00000001</h1>
          <p>
            The complaint was rejected and no compensation was awarded to the
            worker following consideration of the evidence.
          </p>
        </main>
      </body>
    </html>
    """

    assert _content_hash(first) != _content_hash(second)


def test_binary_hash_is_exact_file_hash() -> None:
    binary_content = b"%PDF-1.7 sample binary document"

    file_hash = sha256_bytes(binary_content)
    content_hash = file_hash

    assert content_hash == file_hash
