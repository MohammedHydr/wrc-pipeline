"""Object-key layout for landing and curated zones.

The bucket already names the zone, so keys carry no redundant ``raw/``/
``curated/`` prefix; both zones share the Hive ``body=``/``partition=`` layout.
Landing is versioned by content hash (append-only); curated is latest-only and
named ``<identifier>.<ext>`` per the rename requirement.
"""

from itemadapter import ItemAdapter

from config.common import slug
from transform.transform import _curated_object_key
from wrc_scraper.items import WrcDocumentItem
from wrc_scraper.pipelines import _landing_object_key


def _adapter() -> ItemAdapter:
    return ItemAdapter(
        WrcDocumentItem(
            identifier="ADJ-00021349",
            body="Workplace Relations Commission",
            partition_date="2022-04-01",
            file_hash="a" * 64,
            file_ext="html",
        )
    )


def test_landing_key_layout_has_no_zone_prefix_and_is_versioned():
    key = _landing_object_key(_adapter())
    assert not key.startswith("raw/")
    assert key.startswith(f"body={slug('Workplace Relations Commission')}/")
    assert "/partition=2022-04-01/" in key
    assert "/ADJ-00021349/" in key
    assert key.endswith(f"{'a' * 64}.html")  # content hash -> immutable version


def test_curated_key_mirrors_landing_but_names_by_identifier():
    key = _curated_object_key(
        "Workplace Relations Commission", "2022-04-01", "ADJ-00021349", "pdf"
    )
    assert not key.startswith("curated/")
    assert key == (
        f"body={slug('Workplace Relations Commission')}"
        "/partition=2022-04-01/ADJ-00021349.pdf"
    )


def test_landing_key_changes_with_content_hash_only():
    a = _adapter()
    b = _adapter()
    b["file_hash"] = "b" * 64
    # Same natural identity, different content -> different immutable object.
    assert _landing_object_key(a) != _landing_object_key(b)
