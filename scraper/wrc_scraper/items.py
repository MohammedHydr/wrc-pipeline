"""Item definition for one scraped decision/determination record."""

import scrapy


class WrcDocumentItem(scrapy.Item):
    # --- listing metadata ---------------------------------------------------
    identifier = scrapy.Field()  # e.g. "ADJ-00054658"
    title = scrapy.Field()  # e.g. "ADJ-00054658" (headline text)
    description = scrapy.Field()  # e.g. "Declan Holden V Ger Brennan Construction"
    published_date = scrapy.Field()  # ISO date string, e.g. "2025-07-17"
    doc_url = scrapy.Field()  # absolute URL of the document / page
    body = scrapy.Field()  # tribunal body the record belongs to
    partition_date = scrapy.Field()  # start of the date window, e.g. "2025-07-01"
    partition_label = scrapy.Field()  # "2025-07-01_2025-07-31"
    source = scrapy.Field()  # constant "workplacerelations.ie"
    scraped_at = scrapy.Field()  # UTC ISO timestamp

    # --- document payload (populated after the download request) -------------
    file_content = scrapy.Field()  # raw bytes
    file_ext = scrapy.Field()  # "pdf" | "doc" | "docx" | "html"
    content_type = scrapy.Field()  # response Content-Type header

    # --- computed in pipelines -----------------------------------------------
    file_hash = scrapy.Field()  # SHA-256 of the exact downloaded bytes
    content_hash = scrapy.Field()  # SHA-256 of stable document content
    file_path = scrapy.Field()  # s3://bucket/key of the stored object
    size_bytes = scrapy.Field()  # exact byte size of the stored file
    unchanged = scrapy.Field()  # bool: True if hash matched previous run
    known_version = scrapy.Field()  # bool: content matches an OLDER stored version
