"""Object-key layout for landing and curated zones.

The bucket already names the zone, so keys carry no redundant ``raw/``/
``curated/`` prefix; both zones share the Hive ``body=``/``partition=`` layout.
Landing is versioned by content hash (append-only); curated is latest-only and
named ``<identifier>.<ext>`` per the rename requirement.
"""

from itemadapter import ItemAdapter

from config.common import safe_identifier, slug
from transform.transform import _curated_object_key
from wrc_scraper.items import WrcDocumentItem
from wrc_scraper.pipelines import _landing_object_key


def _adapter(identifier: str = "ADJ-00021349") -> ItemAdapter:
    return ItemAdapter(
        WrcDocumentItem(
            identifier=identifier,
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


def test_safe_identifier_sanitizes_hostile_source_values():
    # Real EAT identifier: slashes would inject extra key path segments.
    assert safe_identifier("RP89/2008, MN99/2008") == "RP89-2008-MN99-2008"
    # Real WRC identifier: internal spaces collapse to single separators.
    assert safe_identifier("IR - SC - 00001595") == "IR-SC-00001595"
    # Clean identifiers pass through untouched.
    assert safe_identifier("ADJ-00021349") == "ADJ-00021349"
    assert safe_identifier("DEC-E2000-14") == "DEC-E2000-14"


def test_landing_key_identifier_segment_is_sanitized():
    key = _landing_object_key(_adapter(identifier="RP89/2008, MN99/2008"))
    # body= / partition= / <identifier> / <hash>.<ext> — exactly four segments,
    # i.e. the raw identifier's slashes did not create extra path levels.
    assert key.count("/") == 3
    assert "/RP89-2008-MN99-2008/" in key
    assert " " not in key


def test_curated_key_basename_is_sanitized_identifier_dot_ext():
    key = _curated_object_key(
        "Employment Appeals Tribunal", "2008-12-01", "RP89/2008, MN99/2008", "pdf"
    )
    assert key.count("/") == 2
    assert key.endswith("/RP89-2008-MN99-2008.pdf")
    assert " " not in key
