"""Scrapy settings.

All tunables come from `config.settings.Settings` (env vars / .env file),
so nothing here is hardcoded. The politeness strategy for "fastest without
getting blocked" is:

* AutoThrottle: dynamically adapts concurrency/delay to observed latency,
  so we go as fast as the server comfortably allows and back off when it
  slows down or errors.
* Bounded per-domain concurrency + a small base delay.
* Retry middleware with an extended status list (incl. 429) — Scrapy's
  RetryMiddleware applies exponential-ish backoff via redownload slots.
* HTTP compression + DNS cache to cut per-request overhead.
* robots.txt respected by default (configurable).
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

# The WRC flow is stateless GET, so the cookie middleware is unnecessary
# overhead (and avoids storing volatile tracking cookies). See recon notes.
COOKIES_ENABLED = _cfg.cookies_enabled

# Memory guard for buffered document downloads. Exceeding the max cancels the
# download (recorded as an auditable failure via the errback); warnsize logs.
DOWNLOAD_MAXSIZE = _cfg.download_maxsize
DOWNLOAD_WARNSIZE = _cfg.download_warnsize

# Optional local HTTP cache — useful during development to avoid re-hitting
# the site while iterating on selectors.
HTTPCACHE_ENABLED = _cfg.http_cache_enabled
HTTPCACHE_EXPIRATION_SECS = 3600
HTTPCACHE_DIR = "httpcache"

# --- Pipelines ----------------------------------------------------------------
ITEM_PIPELINES = {
    "wrc_scraper.pipelines.HashAndDedupPipeline": 100,
    "wrc_scraper.pipelines.ObjectStoragePipeline": 200,
    "wrc_scraper.pipelines.MongoMetadataPipeline": 300,
}

# Resume an interrupted long backfill (scheduler + dupefilter state). Off by
# default: a stale JOBDIR would dupe-filter every document request on a fresh
# rerun and break the found/scraped reconciliation. Set only while resuming.
JOBDIR = _cfg.jobdir or None

# Reactor thread pool (DNS etc.). Scrapy default 10; raise for many-host
# crawls (50+ sources) — negligible on this single-domain crawl.
REACTOR_THREADPOOL_MAXSIZE = _cfg.reactor_threadpool_maxsize

# --- Misc ----------------------------------------------------------------------
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"
TELNETCONSOLE_ENABLED = False

LOG_LEVEL = _cfg.log_level
