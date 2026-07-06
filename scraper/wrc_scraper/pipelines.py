"""Item pipelines.

Order (see settings.ITEM_PIPELINES):

1. HashAndDedupPipeline   – computes sha256, compares with the hash stored in
                            Mongo for the same natural key and marks the item
                            `unchanged` (idempotency / change detection).
2. ObjectStoragePipeline  – uploads the raw bytes to the landing bucket under
                            a content-addressed key. Unchanged files are NOT
                            re-uploaded; changed files get a *new* key (the
                            landing zone is append-only — nothing is deleted
                            or overwritten).
3. MongoMetadataPipeline  – upserts the metadata record keyed on the natural
                            key (unique index), so re-running a date range can
                            never create duplicates.

Natural key
-----------
A record's stable identity is the tuple ``(source, body, identifier)`` — NOT
``identifier`` alone. WRC reference numbers happen to be globally unique, but
keying on the full tuple is the correct model for a multi-source pipeline
(different legal sources reuse identifiers such as "Case No. 1") and matches
the immutable-version identity ``(source, body, identifier, file_hash)`` used
for the content-addressed object keys.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from itemadapter import ItemAdapter
from pymongo import ASCENDING

from config.common import (
    ensure_bucket,
    get_mongo_client,
    get_s3_client,
    sha256_bytes,
    slug,
)
from config.settings import get_settings

logger = logging.getLogger(__name__)


def _natural_key(adapter) -> dict:
    """The stable identity filter for a document: (source, body, identifier)."""
    return {
        "source": adapter["source"],
        "body": adapter["body"],
        "identifier": adapter["identifier"],
    }


class HashAndDedupPipeline:
    def open_spider(self, spider):
        self.cfg = get_settings()
        self.client = get_mongo_client(self.cfg)
        self.coll = self.client[self.cfg.mongo_db][self.cfg.mongo_landing_collection]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        content = adapter.get("file_content")
        if not content:
            raise ValueError(f"Item {adapter.get('identifier')} has no content")

        adapter["file_hash"] = sha256_bytes(content)
        adapter["size_bytes"] = len(content)

        existing = self.coll.find_one(
            _natural_key(adapter), {"file_hash": 1, "file_path": 1}
        )
        adapter["unchanged"] = bool(
            existing and existing.get("file_hash") == adapter["file_hash"]
        )
        if adapter["unchanged"]:
            # keep the already-stored path; storage pipeline will skip upload
            adapter["file_path"] = existing.get("file_path")
            key = (adapter["partition_label"], adapter["body"])
            spider.run_stats[key]["unchanged"] += 1
            logger.info(
                "document unchanged since last run",
                extra={
                    "identifier": adapter["identifier"],
                    "file_hash": adapter["file_hash"],
                },
            )
        return item


class ObjectStoragePipeline:
    def open_spider(self, spider):
        self.cfg = get_settings()
        self.s3 = get_s3_client(self.cfg)
        ensure_bucket(self.s3, self.cfg.s3_landing_bucket)

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if adapter.get("unchanged"):
            return item  # nothing to upload

        # Content-addressed key: identical bytes -> identical key, so even a
        # concurrent duplicate upload is a harmless no-op. Changed documents
        # get a new key; old versions remain (append-only landing zone).
        key = (
            f"raw/body={slug(adapter['body'])}/"
            f"partition={adapter['partition_date']}/"
            f"{adapter['identifier']}/"
            f"{adapter['file_hash']}.{adapter['file_ext']}"
        )
        self.s3.put_object(
            Bucket=self.cfg.s3_landing_bucket,
            Key=key,
            Body=adapter["file_content"],
            ContentType=adapter.get("content_type") or "application/octet-stream",
        )
        adapter["file_path"] = f"s3://{self.cfg.s3_landing_bucket}/{key}"
        logger.info(
            "stored document",
            extra={
                "identifier": adapter["identifier"],
                "file_path": adapter["file_path"],
                "bytes": len(adapter["file_content"]),
            },
        )
        return item


class MongoMetadataPipeline:
    def open_spider(self, spider):
        self.cfg = get_settings()
        self.client = get_mongo_client(self.cfg)
        self.coll = self.client[self.cfg.mongo_db][self.cfg.mongo_landing_collection]
        # Natural-key unique index => structural guarantee of no duplicate
        # records across re-runs (the idempotency backbone).
        self.coll.create_index(
            [("source", ASCENDING), ("body", ASCENDING), ("identifier", ASCENDING)],
            unique=True,
            name="uq_source_body_identifier",
        )
        # Serves the transformation's date-range query and per-body analytics.
        self.coll.create_index([("partition_date", ASCENDING)])
        self.coll.create_index([("body", ASCENDING)])
        # Ad-hoc lookups by reference number (non-unique: uniqueness is only
        # guaranteed within a source+body).
        self.coll.create_index([("identifier", ASCENDING)])

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        doc = {
            k: adapter.get(k)
            for k in (
                "identifier",
                "title",
                "description",
                "published_date",
                "doc_url",
                "body",
                "partition_date",
                "partition_label",
                "source",
                "file_ext",
                "content_type",
                "file_hash",
                "file_path",
                "size_bytes",
            )
        }
        now = datetime.now(timezone.utc)
        # Audit / lineage: last_seen_at + last_run_id prove the record was
        # observed on this run even when the bytes are unchanged; the *_at and
        # first_run_id fields are written once (insert only) and never mutated.
        doc["last_seen_at"] = now
        doc["last_run_id"] = spider.run_id
        update = {
            "$set": doc,
            "$setOnInsert": {
                "first_scraped_at": now,
                "first_run_id": spider.run_id,
            },
        }
        self.coll.update_one(_natural_key(adapter), update, upsert=True)
        # drop the payload before the item is logged/serialised further
        adapter["file_content"] = b""
        return item
