"""Central configuration for the WRC scraping pipeline.

Every connection string, storage path, partition size and scraping parameter
is read from environment variables (or a `.env` file) via pydantic-settings.
Nothing is hardcoded in application code — see `.env.example` for defaults.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import List

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = PROJECT_ROOT / ".env"


class Settings(BaseSettings):
    """Typed, validated application settings."""

    model_config = SettingsConfigDict(
        env_file=ENV_FILE,
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ------------------------------------------------------------------ #
    # Target site
    # ------------------------------------------------------------------ #
    wrc_base_url: str = Field(default="https://www.workplacerelations.ie")
    # The advanced-search form redirects to this GET endpoint; we query it
    # directly (discovered via `scrapy shell` recon — see docs/recon).
    wrc_search_path: str = Field(default="/en/search/")
    # Bodies to scrape. Comma separated in env.
    wrc_bodies: str = Field(
        default=(
            "Employment Appeals Tribunal,Equality Tribunal,"
            "Labour Court,Workplace Relations Commission"
        )
    )
    # Body -> numeric `body=` query code, as submitted by the site's Body
    # checkboxes (the checkbox `value` attributes). Discovered via recon; kept
    # in config (not hardcoded in code) so a site change is a one-line edit.
    wrc_body_codes: str = Field(
        default=(
            "Employment Appeals Tribunal=2,Equality Tribunal=1,"
            "Labour Court=3,Workplace Relations Commission=15376"
        )
    )
    # Results shown per listing page (drives pagination math).
    wrc_results_per_page: int = Field(default=10)
    # Date format used by the site's from/to query params.
    wrc_date_format: str = Field(default="%d/%m/%Y")

    # ------------------------------------------------------------------ #
    # Partitioning
    # ------------------------------------------------------------------ #
    # monthly | weekly | daily
    partition_size: str = Field(default="monthly")

    # ------------------------------------------------------------------ #
    # Scrapy politeness / anti-blocking
    # ------------------------------------------------------------------ #
    concurrent_requests: int = Field(default=8)
    concurrent_requests_per_domain: int = Field(default=4)
    download_delay: float = Field(default=0.25)
    autothrottle_enabled: bool = Field(default=True)
    autothrottle_start_delay: float = Field(default=0.5)
    autothrottle_max_delay: float = Field(default=15.0)
    autothrottle_target_concurrency: float = Field(default=4.0)
    retry_times: int = Field(default=4)
    download_timeout: int = Field(default=60)
    robotstxt_obey: bool = Field(default=True)
    # The WRC search + document flow is stateless GET (recon: no session
    # cookie is required), so the cookie middleware is pure overhead and can
    # set volatile tracking cookies. Disabling it drops that per-request cost.
    cookies_enabled: bool = Field(default=False)
    # Memory guard: documents are buffered in memory before hashing/storage, so
    # cap the download size. A response over the max is cancelled and recorded
    # as an auditable document failure (never silently lost); warnsize only
    # logs. Bytes; raise via env if a legitimately larger document appears.
    download_maxsize: int = Field(default=268_435_456)  # 256 MiB
    download_warnsize: int = Field(default=33_554_432)  # 32 MiB
    user_agent: str = Field(
        default=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/125.0 Safari/537.36 "
            "legal-research-pipeline/1.0"
        )
    )
    http_cache_enabled: bool = Field(default=False)
    # Persist scheduler + dupefilter state so an *interrupted* long backfill
    # can resume where it stopped (scrapy JOBDIR). Empty = off (default):
    # a leftover JOBDIR from a completed run would dupe-filter every document
    # request on the next run and poison the found/scraped reconciliation, so
    # it must only be set while resuming, then cleared.
    jobdir: str = Field(default="")
    # Twisted reactor thread pool (DNS resolution etc.). Scrapy's default is
    # 10; matters when crawling many distinct hosts (50+ sources), harmless
    # on a single-domain crawl with the DNS cache on.
    reactor_threadpool_maxsize: int = Field(default=10, ge=1)

    # ------------------------------------------------------------------ #
    # MongoDB (metadata store)
    # ------------------------------------------------------------------ #
    mongo_uri: str = Field(...)
    mongo_db: str = Field(default="wrc")
    mongo_landing_collection: str = Field(default="landing_document_versions")
    mongo_state_collection: str = Field(default="document_state")
    mongo_curated_collection: str = Field(default="curated_documents")
    mongo_enriched_collection: str = Field(default="enriched_decisions")
    mongo_run_log_collection: str = Field(default="run_logs")

    # ------------------------------------------------------------------ #
    # Object storage (MinIO / any S3-compatible endpoint)
    # ------------------------------------------------------------------ #
    s3_endpoint_url: str = Field(default="http://localhost:9000")
    s3_access_key: str = Field(default="minioadmin")
    s3_secret_key: str = Field(default="minioadmin")
    s3_region: str = Field(default="us-east-1")
    s3_landing_bucket: str = Field(default="wrc-landing")
    s3_curated_bucket: str = Field(default="wrc-curated")

    # ------------------------------------------------------------------ #
    # Behaviour toggles
    # ------------------------------------------------------------------ #
    # If True, documents whose identifier already exists in Mongo are not
    # re-downloaded at all (fast incremental mode; change-detection is then
    # skipped for those records). If False (default), documents are
    # re-fetched, hashed, and only re-uploaded/updated when the hash differs.
    skip_existing_identifiers: bool = Field(default=False)

    # The WRC search endpoint links every result to an HTML case page, but the
    # earliest legacy imports (Equality Tribunal ~2000-2003, EAT legacy) embed
    # the authoritative decision as a PDF *inside* that page. When True, the
    # spider follows that embedded decision PDF and stores it byte-for-byte
    # instead of the HTML wrapper. Set False to always store the HTML page.
    follow_embedded_pdf: bool = Field(default=True)
    # PDF links present as site chrome on every page (never a decision); matched
    # case-insensitively against the link and skipped when following.
    embedded_pdf_exclude: str = Field(
        default="cookie_policy.pdf,decisions_information_guide.pdf"
    )
    # Path substrings that positively identify a legacy decision PDF whose
    # filename does not contain the identifier (e.g. EAT's GUID-named PDFs under
    # /eat_import/). An identifier match is always accepted regardless.
    embedded_pdf_path_markers: str = Field(default="_import/,database-of-decisions")

    # Transformation
    html_content_selectors: str = Field(
        # Ordered, comma-separated CSS selectors tried in turn to isolate the
        # "relevant content" region of a decision page.
        default="#main, main, article, .article-body, .content, #content"
    )
    # BeautifulSoup backend. "lxml" (C-based, fast, lenient) is the default and
    # is already installed as a Scrapy dependency; "html.parser" (pure-Python,
    # no extra dep) is a portable fallback if lxml is unavailable.
    html_parser: str = Field(default="lxml")
    html_drop_selectors: str = Field(
        default=(
            ".cookie-banner,.cookie-consent,.cookies,"
            ".breadcrumb,.breadcrumbs,"
            ".social-share,.share-tools,"
            ".document-actions,.page-actions,"
            ".return-to-search,.print-controls"
        )
    )

    html_min_text_chars: int = Field(default=200, ge=1)

    # Number of worker threads the transform step uses to process records. The
    # step is I/O-bound (S3 GET/PUT + Mongo per record), so threads overlap that
    # latency. pymongo and botocore low-level clients are both thread-safe, so a
    # single client is shared across workers. Set to 1 for fully sequential.
    transform_workers: int = Field(default=8, ge=1)

    # Logging
    log_level: str = Field(default="INFO")

    # ------------------------------------------------------------------ #
    # Convenience accessors
    # ------------------------------------------------------------------ #
    @property
    def bodies_list(self) -> List[str]:
        return [b.strip() for b in self.wrc_bodies.split(",") if b.strip()]

    @property
    def body_code_map(self) -> dict[str, str]:
        """Parse `wrc_body_codes` ("Name=code,Name=code") into {name: code}."""
        out: dict[str, str] = {}
        for pair in self.wrc_body_codes.split(","):
            if "=" in pair:
                name, _, code = pair.partition("=")
                out[name.strip()] = code.strip()
        return out

    @property
    def html_selector_list(self) -> List[str]:
        return [s.strip() for s in self.html_content_selectors.split(",") if s.strip()]

    @property
    def html_drop_selector_list(self) -> List[str]:
        return [
            selector.strip()
            for selector in self.html_drop_selectors.split(",")
            if selector.strip()
        ]

    @property
    def embedded_pdf_exclude_list(self) -> List[str]:
        return [
            x.strip().lower() for x in self.embedded_pdf_exclude.split(",") if x.strip()
        ]

    @property
    def embedded_pdf_path_marker_list(self) -> List[str]:
        return [
            x.strip().lower()
            for x in self.embedded_pdf_path_markers.split(",")
            if x.strip()
        ]

    @property
    def search_url(self) -> str:
        return self.wrc_base_url.rstrip("/") + self.wrc_search_path


@lru_cache
def get_settings() -> Settings:
    # mongo_uri (and other required fields) are populated from the environment
    # / .env by pydantic-settings, which mypy cannot see at the call site.
    return Settings()  # type: ignore[call-arg]
