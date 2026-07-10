"""Shared pytest configuration.

Adds the repository root and Scrapy project directory to Python's import path
so tests use the same package name as the Scrapy application: ``wrc_scraper``.
"""

from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRAPER_ROOT = PROJECT_ROOT / "scraper"


for path in (PROJECT_ROOT, SCRAPER_ROOT):
    path_string = str(path)

    if path_string not in sys.path:
        sys.path.insert(0, path_string)
