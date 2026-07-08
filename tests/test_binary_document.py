from scrapy.http import Request, Response

from wrc_scraper.items import WrcDocumentItem
from wrc_scraper.spiders.wrc_spider import WrcSpider


def test_parse_document_detects_and_preserves_pdf() -> None:
    pdf_bytes = b"%PDF-1.7\nfake-test-document\n%%EOF"

    item = WrcDocumentItem(
        identifier="TEST-PDF-001",
        title="Test PDF",
        description="Binary document test",
        published_date="2013-03-01",
        doc_url="https://example.test/document",
        body="Employment Appeals Tribunal",
        partition_date="2013-03-01",
        partition_label="2013-03-01_2013-03-31",
        source="example.test",
        scraped_at="2026-07-06T00:00:00+00",
    )

    request = Request(
        url="https://example.test/document",
        meta={
            "item": item,
            "ext_hint": None,
        },
    )

    response = Response(
        url=request.url,
        request=request,
        body=pdf_bytes,
        headers={
            "Content-Type": "application/pdf",
        },
    )

    # parse_document does not depend on initialized spider state.
    spider = object.__new__(WrcSpider)

    results = list(
        WrcSpider.parse_document(spider, response)
    )

    assert len(results) == 1

    result = results[0]

    assert result["file_ext"] == "pdf"
    assert result["file_content"] == pdf_bytes
    assert result["content_type"] == "application/pdf"