"""Deterministic HTML extraction and canonicalization.

This module is shared by:

* the scraper, which uses ``content_bytes`` to detect meaningful document
  changes without being affected by volatile page chrome; and
* the transformation step, which stores ``html_bytes`` in the curated zone.

The exact raw response remains untouched in the landing object store.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from html import escape
from typing import Any, Sequence

from bs4 import BeautifulSoup, Comment, FeatureNotFound, NavigableString, Tag

logger = logging.getLogger(__name__)


# Elements that cannot be part of the legal decision itself or that commonly
# introduce volatile values between requests.
STRIP_TAGS: tuple[str, ...] = (
    "script",
    "style",
    "noscript",
    "nav",
    "header",
    "footer",
    "form",
    "button",
    "iframe",
    "aside",
    "template",
    "svg",
)

# Preserve only structural attributes that have meaning in legal tables/lists.
# Styling, JavaScript, generated IDs and tracking attributes are deliberately
# removed to keep canonical output deterministic.
PRESERVED_ATTRIBUTES: dict[str, frozenset[str]] = {
    "td": frozenset({"colspan", "rowspan", "headers"}),
    "th": frozenset({"colspan", "rowspan", "scope", "headers"}),
    "ol": frozenset({"start", "reversed"}),
    "li": frozenset({"value"}),
}

_WHITESPACE_RE = re.compile(r"\s+")
_BETWEEN_TAGS_RE = re.compile(r">\s+<")


@dataclass(frozen=True, slots=True)
class CanonicalHtmlResult:
    """Result returned by :func:`canonicalize_html`.

    ``html_bytes``
        Deterministic, minimal HTML suitable for curated storage.

    ``content_bytes``
        Normalized visible decision text suitable for semantic change
        detection. Page scripts, navigation and dynamic attributes cannot
        affect it.

    ``selector_used``
        CSS selector that successfully identified the document content.

    ``fallback_used``
        True when no configured selector matched and ``body`` was used.

    ``text_length``
        Length of normalized visible document text.

    ``parser_used``
        BeautifulSoup parser that successfully parsed the input.
    """

    html_bytes: bytes
    content_bytes: bytes
    selector_used: str | None
    fallback_used: bool
    text_length: int
    parser_used: str


class HtmlCanonicalizationError(ValueError):
    """Raised when an HTML document cannot produce meaningful content."""


def canonicalize_html(
    raw: bytes,
    *,
    selectors: Sequence[str],
    parser: str = "lxml",
    drop_selectors: Sequence[str] = (),
    min_text_chars: int = 200,
    need_html: bool = True,
) -> CanonicalHtmlResult:
    """Extract and deterministically serialize relevant document content.

    Processing order:

    1. Parse the raw HTML.
    2. Remove scripts, navigation, forms, comments and configured page chrome.
    3. Select the first configured content container containing enough text.
    4. Remove volatile attributes.
    5. Normalize text and whitespace.
    6. Return both canonical HTML and canonical visible text.

    Args:
        raw:
            Exact HTML response bytes.
        selectors:
            Ordered CSS selectors. The first meaningful match is used.
        parser:
            Preferred BeautifulSoup parser.
        drop_selectors:
            Additional CSS selectors for source-specific page chrome.
        min_text_chars:
            Minimum normalized text length for a selector to be accepted.
        need_html:
            When True (default) the deterministic curated HTML is built and
            returned in ``html_bytes``. When False — the scraper's change-
            detection path, which only needs ``content_bytes`` for the content
            hash — attribute cleaning, text-node rewriting and serialization are
            skipped and ``html_bytes`` is empty. ``content_bytes`` is identical
            either way (whitespace normalization is idempotent under the final
            text collapse).

    Raises:
        HtmlCanonicalizationError:
            When input is empty or no meaningful content can be produced.
        ValueError:
            When ``min_text_chars`` is less than one.
    """

    if not raw:
        raise HtmlCanonicalizationError("Cannot canonicalize empty HTML content")

    if min_text_chars < 1:
        raise ValueError("min_text_chars must be at least 1")

    soup, parser_used = _parse_html(raw, parser)

    title = _normalize_text(soup.title.get_text(" ", strip=True) if soup.title else "")

    _remove_comments(soup)
    _remove_tags(soup)
    _remove_selected_elements(soup, drop_selectors)

    node, selector_used = _select_content_node(
        soup=soup,
        selectors=selectors,
        min_text_chars=min_text_chars,
    )

    fallback_used = node is None
    if node is None:
        node = soup.body or soup

    if not need_html:
        # Change-detection path: only the visible-text hash is needed, so skip
        # attribute cleaning, text-node rewriting and serialization entirely.
        canonical_text = _normalize_text(node.get_text(" ", strip=True))
        if not canonical_text:
            raise HtmlCanonicalizationError(
                "HTML document contains no meaningful visible content"
            )

        return CanonicalHtmlResult(
            html_bytes=b"",
            content_bytes=canonical_text.encode("utf-8"),
            selector_used=selector_used,
            fallback_used=fallback_used,
            text_length=len(canonical_text),
            parser_used=parser_used,
        )

    _clean_attributes(node)
    _normalize_text_nodes(node)

    canonical_text = _normalize_text(node.get_text(" ", strip=True))
    if not canonical_text:
        raise HtmlCanonicalizationError(
            "HTML document contains no meaningful visible content"
        )

    body_html = _serialize_contents(node)
    canonical_html = (
        "<!doctype html>"
        "<html>"
        "<head>"
        '<meta charset="utf-8">'
        f"<title>{escape(title)}</title>"
        "</head>"
        f"<body>{body_html}</body>"
        "</html>"
    )

    return CanonicalHtmlResult(
        html_bytes=canonical_html.encode("utf-8"),
        content_bytes=canonical_text.encode("utf-8"),
        selector_used=selector_used,
        fallback_used=fallback_used,
        text_length=len(canonical_text),
        parser_used=parser_used,
    )


def _parse_html(raw: bytes, parser: str) -> tuple[BeautifulSoup, str]:
    """Parse HTML, falling back to Python's built-in parser."""

    try:
        return BeautifulSoup(raw, parser), parser
    except FeatureNotFound:
        fallback = "html.parser"
        logger.warning(
            "configured HTML parser unavailable; using fallback",
            extra={
                "requested_parser": parser,
                "fallback_parser": fallback,
            },
        )
        return BeautifulSoup(raw, fallback), fallback


def _remove_comments(soup: BeautifulSoup) -> None:
    """Remove comments because they may contain build or request metadata."""

    for comment in soup.find_all(string=lambda value: isinstance(value, Comment)):
        comment.extract()


def _remove_tags(soup: BeautifulSoup) -> None:
    """Remove globally irrelevant and volatile elements.

    ``find_all`` accepts a list of names and matches any of them in a single
    tree traversal, so all ``STRIP_TAGS`` are removed in one pass instead of
    one full walk per tag name.
    """

    for tag in soup.find_all(list(STRIP_TAGS)):
        tag.decompose()


def _remove_selected_elements(
    soup: BeautifulSoup,
    selectors: Sequence[str],
) -> None:
    """Remove configured source-specific page-chrome elements."""

    for selector in selectors:
        clean_selector = selector.strip()
        if not clean_selector:
            continue

        try:
            matches = soup.select(clean_selector)
        except Exception as exc:  # BeautifulSoup delegates to soupsieve
            logger.warning(
                "invalid HTML drop selector ignored",
                extra={
                    "selector": clean_selector,
                    "error": str(exc),
                },
            )
            continue

        for element in matches:
            element.decompose()


def _select_content_node(
    *,
    soup: BeautifulSoup,
    selectors: Sequence[str],
    min_text_chars: int,
) -> tuple[Tag | None, str | None]:
    """Return the first selector match with enough normalized text."""

    for selector in selectors:
        clean_selector = selector.strip()
        if not clean_selector:
            continue

        try:
            candidate = soup.select_one(clean_selector)
        except Exception as exc:
            logger.warning(
                "invalid HTML content selector ignored",
                extra={
                    "selector": clean_selector,
                    "error": str(exc),
                },
            )
            continue

        if candidate is None:
            continue

        text = _normalize_text(candidate.get_text(" ", strip=True))
        if len(text) >= min_text_chars:
            return candidate, clean_selector

    return None, None


def _clean_attributes(node: Tag) -> None:
    """Remove volatile attributes while retaining legal table structure."""

    elements = [node, *node.find_all(True)]

    for element in elements:
        allowed = PRESERVED_ATTRIBUTES.get(
            element.name or "",
            frozenset(),
        )

        cleaned_attributes: dict[str, Any] = {}
        for attribute in sorted(allowed):
            if attribute in element.attrs:
                cleaned_attributes[attribute] = element.attrs[attribute]

        # Replacing the complete mapping also removes generated IDs, classes,
        # inline styles, event handlers, nonces and tracking attributes.
        element.attrs = cleaned_attributes


def _normalize_text_nodes(node: Tag) -> None:
    """Normalize textual whitespace before serialization."""

    for text_node in list(node.find_all(string=True)):
        if isinstance(text_node, Comment):
            text_node.extract()
            continue

        normalized = _normalize_text(str(text_node))

        if not normalized:
            text_node.extract()
            continue

        # Spaces around normalized text prevent adjacent inline elements from
        # joining words together. Serialization removes inter-tag whitespace
        # afterward, while visible-text extraction remains correct.
        text_node.replace_with(NavigableString(f" {normalized} "))


def _serialize_contents(node: Tag) -> str:
    """Serialize only the selected node's children.

    Using ``decode_contents`` avoids creating a nested ``<body>`` when the
    fallback node itself is the original body element.
    """

    serialized = node.decode_contents(formatter="minimal")
    serialized = _BETWEEN_TAGS_RE.sub("><", serialized)
    return serialized.strip()


def _normalize_text(value: str) -> str:
    """Collapse all Unicode whitespace into single spaces."""

    return _WHITESPACE_RE.sub(" ", value).strip()
