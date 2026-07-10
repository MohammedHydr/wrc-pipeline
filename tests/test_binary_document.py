from collections import defaultdict

from scrapy.http import Request, Response

from wrc_scraper.items import WrcDocumentItem
from wrc_scraper.spiders.wrc_spider import WrcSpider


def _make_item(ext_hint: str | None = None) -> WrcDocumentItem:
    return WrcDocumentItem(
        identifier="TEST-DOC-001",
        title="Test",
        description="Binary document test",
        published_date="2013-03-01",
        doc_url="https://example.test/document",
        body="Employment Appeals Tribunal",
        partition_date="2013-03-01",
        partition_label="2013-03-01_2013-03-31",
        source="example.test",
        scraped_at="2026-07-06T00:00:00+00",
    )


def _make_response(
    item: WrcDocumentItem, body: bytes, content_type: str, ext_hint: str | None
) -> Response:
    request = Request(
        url="https://example.test/document",
        meta={
            "item": item,
            "ext_hint": ext_hint,
            "partition_label": item["partition_label"],
            "body": item["body"],
        },
    )
    return Response(
        url=request.url,
        request=request,
        body=body,
        headers={"Content-Type": content_type},
    )


def _bare_spider() -> WrcSpider:
    """A spider instance without running __init__, but with the attributes
    parse_document touches on the failure path."""
    spider = object.__new__(WrcSpider)
    spider.run_stats = defaultdict(  # type: ignore[attr-defined]
        lambda: {"found": None, "scraped": 0, "unchanged": 0, "failed": []}
    )
    return spider


def test_parse_document_detects_and_preserves_pdf() -> None:
    pdf_bytes = b"%PDF-1.7\nfake-test-document\n%%EOF"

    item = _make_item()
    response = _make_response(item, pdf_bytes, "application/pdf", ext_hint=None)

    results = list(WrcSpider.parse_document(_bare_spider(), response))

    assert len(results) == 1
    result = results[0]
    assert result["file_ext"] == "pdf"
    assert result["file_content"] == pdf_bytes
    assert result["content_type"] == "application/pdf"


def test_parse_document_rejects_wrong_content_for_binary_extension() -> None:
    """A `.pdf` link that actually returns an HTML error page (HTTP 200) must
    not be stored as a PDF; it is recorded as an auditable failure instead."""
    html_masquerading_as_pdf = b"<!doctype html><html><body>Not found</body></html>"

    item = _make_item()
    response = _make_response(
        item, html_masquerading_as_pdf, "text/html", ext_hint="pdf"
    )
    spider = _bare_spider()

    results = list(WrcSpider.parse_document(spider, response))

    # No item is emitted for storage.
    assert results == []

    # The record is accounted as a failure with a reason (found = scraped + failed).
    key = (item["partition_label"], item["body"])
    failures = spider.run_stats[key]["failed"]
    assert len(failures) == 1
    assert failures[0]["identifier"] == "TEST-DOC-001"
    assert failures[0]["stage"] == "validation"
    assert failures[0]["status"] == 200


def test_parse_document_accepts_valid_docx_signature() -> None:
    docx_bytes = b"PK\x03\x04" + b"\x00" * 32  # zip/OOXML magic

    item = _make_item()
    response = _make_response(
        item,
        docx_bytes,
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ext_hint="docx",
    )

    results = list(WrcSpider.parse_document(_bare_spider(), response))

    assert len(results) == 1
    assert results[0]["file_ext"] == "docx"
    assert results[0]["file_content"] == docx_bytes
