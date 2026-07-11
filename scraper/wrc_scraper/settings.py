"""Scrapy settings. Every tunable comes from config.settings (env-driven).

"Fastest without getting blocked" = AutoThrottle rides the server's real
latency between a polite floor (DOWNLOAD_DELAY) and bounded per-domain
concurrency; retries cover transient statuses only; robots.txt is obeyed.
Trade-offs and rejected "fast spider" tips: docs/performance.md.
"""

from config.settings import get_settings

_cfg = get_settings()

BOT_NAME = "wrc_scraper"

SPIDER_MODULES = ["wrc_scraper.spiders"]
NEWSPIDER_MODULE = "wrc_scraper.spiders"

USER_AGENT = _cfg.user_agent
ROBOTSTXT_OBEY = _cfg.robotstxt_obey

# --- Throughput vs politeness ------------------------------------------------
CONCURRENT_REQUESTS = _cfg.concurrent_requests
CONCURRENT_REQUESTS_PER_DOMAIN = _cfg.concurrent_requests_per_domain
DOWNLOAD_DELAY = _cfg.download_delay
DOWNLOAD_TIMEOUT = _cfg.download_timeout

AUTOTHROTTLE_ENABLED = _cfg.autothrottle_enabled
AUTOTHROTTLE_START_DELAY = _cfg.autothrottle_start_delay
AUTOTHROTTLE_MAX_DELAY = _cfg.autothrottle_max_delay
AUTOTHROTTLE_TARGET_CONCURRENCY = _cfg.autothrottle_target_concurrency
AUTOTHROTTLE_DEBUG = False

# --- Resilience ---------------------------------------------------------------
RETRY_ENABLED = True
RETRY_TIMES = _cfg.retry_times
RETRY_HTTP_CODES = [429, 500, 502, 503, 504, 522, 524, 408]

COMPRESSION_ENABLED = True
DNSCACHE_ENABLED = True

# Stateless GET flow — the cookie middleware is pure overhead.
COOKIES_ENABLED = _cfg.cookies_enabled

# Memory guard: over max = auditable failure via errback; warnsize logs.
DOWNLOAD_MAXSIZE = _cfg.download_maxsize
DOWNLOAD_WARNSIZE = _cfg.download_warnsize

# Dev-only response cache for iterating on selectors without re-hitting the site.
HTTPCACHE_ENABLED = _cfg.http_cache_enabled
HTTPCACHE_EXPIRATION_SECS = 3600
HTTPCACHE_DIR = "httpcache"

# Resume an interrupted backfill only — a stale JOBDIR dupe-filters document
# requests and breaks the found/scraped reconciliation on a fresh rerun.
JOBDIR = _cfg.jobdir or None

# DNS + the pipelines' blocking Mongo/S3 I/O run on this pool.
REACTOR_THREADPOOL_MAXSIZE = _cfg.reactor_threadpool_maxsize

# --- Pipelines ----------------------------------------------------------------
ITEM_PIPELINES = {
    "wrc_scraper.pipelines.HashAndDedupPipeline": 100,
    "wrc_scraper.pipelines.ObjectStoragePipeline": 200,
    "wrc_scraper.pipelines.MongoMetadataPipeline": 300,
}

# --- Misc ----------------------------------------------------------------------
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"
TELNETCONSOLE_ENABLED = False

LOG_LEVEL = _cfg.log_level
