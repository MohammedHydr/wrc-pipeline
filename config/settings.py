"""Central configuration: every tunable comes from env vars / `.env` via
pydantic-settings. Nothing is hardcoded — see `.env.example` for the full
list with rationale per value."""

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
    # The advanced-search form redirects here (recon: docs/recon).
    wrc_search_path: str = Field(default="/en/search/")
    wrc_bodies: str = Field(
        default=(
            "Employment Appeals Tribunal,Equality Tribunal,"
            "Labour Court,Workplace Relations Commission"
        )
    )
    # Body -> numeric body= code, from the site's checkbox value attributes.
    # In config so a site change is a one-line edit.
    wrc_body_codes: str = Field(
        default=(
            "Employment Appeals Tribunal=2,Equality Tribunal=1,"
            "Labour Court=3,Workplace Relations Commission=15376"
        )
    )
    # Listing page size; drives pagination math.
    wrc_results_per_page: int = Field(default=10)
    wrc_date_format: str = Field(default="%d/%m/%Y")

    # ------------------------------------------------------------------ #
    # Partitioning: monthly | weekly | daily
    # ------------------------------------------------------------------ #
    partition_size: str = Field(default="monthly")
    # First month of Dagster's partition/backfill grid.
    dagster_partition_start_date: str = Field(default="2024-01-01")

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
    # Stateless GET flow — the cookie middleware is pure overhead.
    cookies_enabled: bool = Field(default=False)
    # Downloads buffer in memory; over max = auditable failure, never OOM.
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
    # Resume an *interrupted* backfill. Keep empty otherwise: a stale JOBDIR
    # dupe-filters every document request and breaks reconciliation.
    jobdir: str = Field(default="")
    # Reactor thread pool: DNS + the pipelines' blocking Mongo/S3 I/O.
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
    # Dead-letter ledger: one row per failed record, replayed by re-running
    # the partition (failed records are never persisted, so they re-fetch).
    mongo_failed_collection: str = Field(default="failed_documents")

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
    # True = skip identifiers already in Mongo entirely (fast, but no change
    # detection for them). False = re-fetch + hash-compare (requirement 9).
    skip_existing_identifiers: bool = Field(default=False)

    # Replay stored ETags as If-None-Match; 304 = unchanged with zero body
    # bytes. Only the static PDFs send ETags (measured); the dynamic HTML
    # pages are no-cache, so they re-fetch + hash-compare. SHA-256 remains
    # the content identity; set False to distrust ETags entirely.
    use_conditional_requests: bool = Field(default=True)

    # The earliest legacy imports embed the authoritative decision as a PDF
    # inside the HTML case page; follow and store that instead of the wrapper.
    follow_embedded_pdf: bool = Field(default=True)
    # Chrome PDFs present on every page — never a decision.
    embedded_pdf_exclude: str = Field(
        default="cookie_policy.pdf,decisions_information_guide.pdf"
    )
    # Legacy import paths whose PDF filenames don't contain the identifier
    # (EAT's GUID names). An identifier match is always accepted regardless.
    embedded_pdf_path_markers: str = Field(default="_import/,database-of-decisions")

    # ------------------------------------------------------------------ #
    # Transformation
    # ------------------------------------------------------------------ #
    # Ordered selectors tried in turn to isolate the decision content.
    html_content_selectors: str = Field(
        default="#main, main, article, .article-body, .content, #content"
    )
    # lxml is fast and already a Scrapy dependency; html.parser is the
    # pure-Python fallback.
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

    # Transform/enrich thread pools — the work is S3/Mongo I/O-bound and the
    # clients are thread-safe. 1 = fully sequential.
    transform_workers: int = Field(default=8, ge=1)

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
    # Required fields (mongo_uri) come from the env; mypy can't see that.
    return Settings()  # type: ignore[call-arg]
