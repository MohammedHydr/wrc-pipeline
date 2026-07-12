"""Integration test: idempotency against the real compose stack.

Runs the three item pipelines (hash -> object storage -> metadata) end-to-end
against the *running* MongoDB + MinIO containers, in an isolated throwaway
database and bucket, and asserts the idempotency contract:

* same content twice  -> one landing version, one object, marked unchanged;
* changed content     -> a NEW version + NEW object, the old object untouched
                         (landing stays append-only), state repointed.

The test needs no network beyond localhost and no live WRC site. If the
compose stack is not running it SKIPS (it never fails a laptop-only run);
with `docker compose up -d` it makes the idempotency proof reproducible in CI.
"""

from __future__ import annotations

import uuid
from collections import defaultdict
from types import SimpleNamespace

import pytest

from config.common import ensure_bucket, get_mongo_client, get_s3_client
from config.settings import get_settings
from wrc_scraper import pipelines as pl
from wrc_scraper.items import WrcDocumentItem

PDF_V1 = b"%PDF-1.7\noriginal decision text\n%%EOF"
PDF_V2 = b"%PDF-1.7\namended decision text\n%%EOF"


def _stack_available(cfg) -> bool:
    try:
        client = get_mongo_client(cfg)
        client.admin.command("ping")
        client.close()
        get_s3_client(cfg).list_buckets()
        return True
    except Exception:  # noqa: BLE001 - any failure means "stack not running"
        return False


def _item(content: bytes) -> WrcDocumentItem:
    return WrcDocumentItem(
        identifier="IT-0001",
        title="Integration Test Decision",
        description="A Worker v An Employer",
        published_date="2024-01-15",
        doc_url="http://localhost/it-0001.pdf",
        body="Labour Court",
        partition_date="2024-01-01",
        partition_label="2024-01-01_2024-01-31",
        source="integration.test",
        scraped_at="2024-01-31T00:00:00+00:00",
        file_content=content,
        file_ext="pdf",
        content_type="application/pdf",
        fetched_url="http://localhost/it-0001.pdf",
    )


@pytest.fixture()
def stack(monkeypatch):
    cfg = get_settings()
    if not _stack_available(cfg):
        pytest.skip("compose stack (MongoDB + MinIO) is not running")

    run_ns = uuid.uuid4().hex[:10]
    test_db = f"wrc_it_{run_ns}"
    test_bucket = f"wrc-it-{run_ns}"

    client = get_mongo_client(cfg)
    database = client[test_db]
    s3 = get_s3_client(cfg)
    ensure_bucket(s3, test_bucket)

    # Same unique indexes the pipelines create in open_spider — they are what
    # the DuplicateKeyError idempotency path relies on.
    database["versions"].create_index(
        [("source", 1), ("body", 1), ("identifier", 1), ("content_hash", 1)],
        unique=True,
    )
    database["state"].create_index(
        [("source", 1), ("body", 1), ("identifier", 1)], unique=True
    )

    # Pipelines wired to the throwaway namespace (bypasses open_spider, which
    # reads the cached prod settings).
    hasher = pl.HashAndDedupPipeline()
    hasher.cfg = cfg
    hasher.state = database["state"]
    hasher.versions = database["versions"]

    store = pl.ObjectStoragePipeline()
    store.cfg = cfg.model_copy(update={"s3_landing_bucket": test_bucket})
    store.s3 = s3

    meta = pl.MongoMetadataPipeline()
    meta.cfg = cfg
    meta.versions = database["versions"]
    meta.state = database["state"]

    # Execute the reactor-marshalled counters inline (no reactor in tests).
    monkeypatch.setattr(pl.reactor, "callFromThread", lambda fn, *a: fn(*a))

    spider = SimpleNamespace(
        run_id="it-run",
        run_stats=defaultdict(lambda: {"scraped": 0, "unchanged": 0}),
    )
    spider.note_scraped = lambda key: spider.run_stats[key].__setitem__(
        "scraped", spider.run_stats[key]["scraped"] + 1
    )
    spider.note_unchanged = lambda key: spider.run_stats[key].__setitem__(
        "unchanged", spider.run_stats[key]["unchanged"] + 1
    )

    def run_pipelines(item):
        for stage in (hasher, store, meta):
            item = stage._process(item, spider)
        return item

    try:
        yield SimpleNamespace(
            run=run_pipelines,
            db=database,
            s3=s3,
            bucket=test_bucket,
            spider=spider,
        )
    finally:
        client.drop_database(test_db)
        client.close()
        objects = s3.list_objects_v2(Bucket=test_bucket).get("Contents", [])
        for obj in objects:
            s3.delete_object(Bucket=test_bucket, Key=obj["Key"])
        s3.delete_bucket(Bucket=test_bucket)


def _bucket_keys(stack) -> list[str]:
    listing = stack.s3.list_objects_v2(Bucket=stack.bucket)
    return sorted(o["Key"] for o in listing.get("Contents", []))


def test_rerun_of_unchanged_content_creates_nothing_new(stack):
    first = stack.run(_item(PDF_V1))
    assert first["unchanged"] is False
    assert stack.db["versions"].count_documents({}) == 1
    keys_after_first = _bucket_keys(stack)
    assert len(keys_after_first) == 1

    second = stack.run(_item(PDF_V1))
    assert second["unchanged"] is True
    # No duplicate metadata, no new object, state still points at v1.
    assert stack.db["versions"].count_documents({}) == 1
    assert stack.db["state"].count_documents({}) == 1
    assert _bucket_keys(stack) == keys_after_first
    key = ("2024-01-01_2024-01-31", "Labour Court")
    assert stack.spider.run_stats[key]["scraped"] == 2
    assert stack.spider.run_stats[key]["unchanged"] == 1


def test_changed_content_appends_new_version_and_object(stack):
    stack.run(_item(PDF_V1))
    keys_v1 = _bucket_keys(stack)

    changed = stack.run(_item(PDF_V2))
    assert changed["unchanged"] is False

    # Append-only landing: v1's object and version record both survive.
    assert stack.db["versions"].count_documents({}) == 2
    keys_now = _bucket_keys(stack)
    assert len(keys_now) == 2
    assert set(keys_v1).issubset(set(keys_now))

    # The mutable state pointer moved to the new version.
    state = stack.db["state"].find_one({"identifier": "IT-0001"})
    assert state["latest_file_hash"] == changed["file_hash"]
    assert state["latest_file_path"].endswith(f"{changed['file_hash']}.pdf")
