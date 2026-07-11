"""Deterministic HTML extraction and canonicalization.

Shared by the scraper (``content_bytes`` -> change-detection hash immune to
page chrome) and the transform (``html_bytes`` -> curated HTML). The raw
response always stays untouched in landing.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from html import escape
from typing import Any, Sequence

from bs4 import BeautifulSoup, Comment, FeatureNotFound, NavigableString, Tag

logger = logging.getLogger(__name__)


# Never part of the decision text, or volatile between requests.
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

# Keep only attributes with structural meaning in legal tables/lists;
# styling, IDs and tracking attributes go, so output stays deterministic.
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
    """html_bytes = deterministic curated HTML; content_bytes = normalized
    visible text (the change-detection hash input). The rest is diagnostics:
    which selector matched, whether the body fallback kicked in, text length,
    and which parser actually ran."""

    html_bytes: bytes
    content_bytes: bytes
    selector_used: str | None
    fallback_used: bool
    text_length: int
    parser_used: str


class HtmlCanonicalizationError(ValueError):
    """The document cannot produce meaningful content."""


def canonicalize_html(
    raw: bytes,
    *,
    selectors: Sequence[str],
    parser: str = "lxml",
    drop_selectors: Sequence[str] = (),
    min_text_chars: int = 200,
    need_html: bool = True,
) -> CanonicalHtmlResult:
    """Strip chrome, pick the first content selector with enough text, and
    return canonical HTML + canonical visible text.

    ``need_html=False`` is the scraper's change-detection path: it skips
    attribute cleaning and serialization and returns empty ``html_bytes``
    (``content_bytes`` is byte-identical either way).
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
    """Comments can carry build/request metadata — drop them."""

    for comment in soup.find_all(string=lambda value: isinstance(value, Comment)):
        comment.extract()


def _remove_tags(soup: BeautifulSoup) -> None:
    """Drop all STRIP_TAGS in one tree traversal (find_all takes a list)."""

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
    """First selector match with enough normalized text."""

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
    """Strip volatile attributes, keeping legal table structure."""

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

        # Replacing the whole mapping drops IDs, classes, styles, handlers…
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

        # Pad with spaces so adjacent inline elements can't join words;
        # serialization strips inter-tag whitespace afterwards.
        text_node.replace_with(NavigableString(f" {normalized} "))


def _serialize_contents(node: Tag) -> str:
    """Serialize only the node's children — decode_contents avoids a nested
    <body> when the fallback node is the body itself."""

    serialized = node.decode_contents(formatter="minimal")
    serialized = _BETWEEN_TAGS_RE.sub("><", serialized)
    return serialized.strip()


def _normalize_text(value: str) -> str:
    """Collapse all Unicode whitespace into single spaces."""

    return _WHITESPACE_RE.sub(" ", value).strip()
