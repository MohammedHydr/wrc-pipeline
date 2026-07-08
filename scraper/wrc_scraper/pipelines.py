"""Scrapy item pipelines for WRC document ingestion.

Pipeline order:

1. HashAndDedupPipeline
   Calculates:
   * file_hash: SHA-256 of the exact downloaded bytes.
   * content_hash: SHA-256 of stable legal content.

   It compares the calculated content hash with the latest document state.

2. ObjectStoragePipeline
   Stores new or changed raw files in the landing object-storage bucket.
   Unchanged files are not uploaded again.

3. MongoMetadataPipeline
   Inserts immutable document versions into the landing collection and
   updates a separate mutable state collection that points to the latest
   version.

Collections:

landing_document_versions
    Immutable version history. Application code never updates or deletes
    records in this collection.

document_state
    One mutable record per logical document. It points to the latest immutable
    landing version and records when the document was most recently observed.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from itemadapter import ItemAdapter
from pymongo import ASCENDING
from pymongo.errors import DuplicateKeyError

from config.common import (
    ensure_bucket,
    get_mongo_client,
    get_s3_client,
    sha256_bytes,
    slug,
)
from config.html_utils import canonicalize_html
from config.settings import get_settings

logger = logging.getLogger(__name__)


def _natural_key(adapter: ItemAdapter) -> dict[str, Any]:
    """Return the stable identity of one logical legal document."""

    return {
        "source": adapter["source"],
        "body": adapter["body"],
        "identifier": adapter["identifier"],
    }


def _version_key(adapter: ItemAdapter) -> dict[str, Any]:
    """Return the immutable identity of one document version."""

    return {
        **_natural_key(adapter),
        "content_hash": adapter["content_hash"],
    }


class HashAndDedupPipeline:
    """Calculate hashes and detect unchanged documents."""

    def open_spider(self) -> None:
        self.cfg = get_settings()
        self.client = get_mongo_client(self.cfg)
        database = self.client[self.cfg.mongo_db]

        # Deduplication checks the mutable latest-version pointer rather than
        # searching or updating immutable landing versions.
        self.state = database[self.cfg.mongo_state_collection]
        # Read-only lookup used to recognise content that flipped back to a
        # previously stored version (avoids uploading an orphan raw object).
        self.versions = database[self.cfg.mongo_landing_collection]

    def close_spider(self) -> None:
        self.client.close()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        content = adapter.get("file_content")
        if not content:
            raise ValueError(
                f"Item {adapter.get('identifier')} has no downloaded content"
            )

        file_ext = str(adapter.get("file_ext") or "").lower()

        # Exact hash of the bytes downloaded from WRC.
        raw_file_hash = sha256_bytes(content)

        adapter["file_hash"] = raw_file_hash
        adapter["size_bytes"] = len(content)

        if file_ext == "html":
            canonical = canonicalize_html(
                content,
                selectors=self.cfg.html_selector_list,
                parser=self.cfg.html_parser,
                drop_selectors=self.cfg.html_drop_selector_list,
                min_text_chars=self.cfg.html_min_text_chars,
            )

            adapter["content_hash"] = sha256_bytes(canonical.content_bytes)

            logger.info(
                "calculated canonical HTML content hash",
                extra={
                    "identifier": adapter["identifier"],
                    "file_hash": adapter["file_hash"],
                    "content_hash": adapter["content_hash"],
                    "selector_used": canonical.selector_used,
                    "fallback_used": canonical.fallback_used,
                    "text_length": canonical.text_length,
                    "parser_used": canonical.parser_used,
                },
            )
        else:
            # For binary documents, exact file bytes define the version.
            adapter["content_hash"] = raw_file_hash

        existing_state = self.state.find_one(
            _natural_key(adapter),
            {
                "latest_content_hash": 1,
                "latest_file_hash": 1,
                "latest_file_path": 1,
                "latest_size_bytes": 1,
            },
        )

        unchanged = bool(
            existing_state
            and existing_state.get("latest_content_hash") == adapter["content_hash"]
        )

        adapter["unchanged"] = unchanged
        adapter["known_version"] = False
        
        if not unchanged:
            # Content differs from the CURRENT state, but it may match an
            # OLDER immutable version (content flipped A -> B -> back to A).
            # Reuse that version's stored object instead of uploading a raw
            # file no version record would ever reference; the metadata
            # pipeline's DuplicateKeyError path then repoints state to it.
            prior_version = self.versions.find_one(
                _version_key(adapter),
                {"file_hash": 1, "file_path": 1, "size_bytes": 1},
            )
            if prior_version is not None:
                adapter["known_version"] = True
                adapter["file_hash"] = prior_version["file_hash"]
                adapter["file_path"] = prior_version["file_path"]
                adapter["size_bytes"] = prior_version.get(
                    "size_bytes", adapter["size_bytes"]
                )
                logger.info(
                    "content matches a prior stored version; reusing object",
                    extra={
                        "identifier": adapter["identifier"],
                        "content_hash": adapter["content_hash"],
                        "file_path": adapter["file_path"],
                    },
                )

        if unchanged:
            # Preserve metadata for the exact raw file already stored.
            adapter["file_hash"] = existing_state["latest_file_hash"]
            adapter["file_path"] = existing_state["latest_file_path"]
            adapter["size_bytes"] = existing_state.get(
                "latest_size_bytes",
                adapter["size_bytes"],
            )

            key = (
                adapter["partition_label"],
                adapter["body"],
            )
            spider.run_stats[key]["unchanged"] += 1

            logger.info(
                "document content unchanged since last run",
                extra={
                    "identifier": adapter["identifier"],
                    "file_hash": adapter["file_hash"],
                    "content_hash": adapter["content_hash"],
                    "file_path": adapter["file_path"],
                },
            )

        return item


class ObjectStoragePipeline:
    """Store new raw files in the immutable landing bucket."""

    def open_spider(self) -> None:
        self.cfg = get_settings()
        self.s3 = get_s3_client(self.cfg)

        ensure_bucket(
            self.s3,
            self.cfg.s3_landing_bucket,
        )

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        if adapter.get("unchanged") or adapter.get("known_version"):
            # Unchanged content, or content identical to an older stored
            # version — the raw object for these bytes already exists.
            return item

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
            ContentType=(adapter.get("content_type") or "application/octet-stream"),
        )

        adapter["file_path"] = f"s3://{self.cfg.s3_landing_bucket}/{key}"

        logger.info(
            "stored raw document version",
            extra={
                "identifier": adapter["identifier"],
                "file_path": adapter["file_path"],
                "file_hash": adapter["file_hash"],
                "content_hash": adapter["content_hash"],
                "bytes": len(adapter["file_content"]),
            },
        )

        return item


class MongoMetadataPipeline:
    """Insert immutable versions and maintain latest-version state."""

    def open_spider(self) -> None:
        self.cfg = get_settings()
        self.client = get_mongo_client(self.cfg)

        database = self.client[self.cfg.mongo_db]

        self.versions = database[self.cfg.mongo_landing_collection]
        self.state = database[self.cfg.mongo_state_collection]

        # One immutable record for every meaningful content version.
        # Binary content_hash equals file_hash. HTML content_hash represents
        # canonical legal content.
        self.versions.create_index(
            [
                ("source", ASCENDING),
                ("body", ASCENDING),
                ("identifier", ASCENDING),
                ("content_hash", ASCENDING),
            ],
            unique=True,
            name="uq_landing_document_version",
        )

        self.versions.create_index(
            [
                ("partition_date", ASCENDING),
                ("body", ASCENDING),
            ],
            name="partition_body_1",
        )

        self.versions.create_index(
            [
                ("source", ASCENDING),
                ("body", ASCENDING),
                ("identifier", ASCENDING),
            ],
            name="document_identity_1",
        )

        self.versions.create_index(
            [("content_hash", ASCENDING)],
            name="content_hash_1",
        )

        # Exactly one mutable state record for each logical document.
        self.state.create_index(
            [
                ("source", ASCENDING),
                ("body", ASCENDING),
                ("identifier", ASCENDING),
            ],
            unique=True,
            name="uq_document_state",
        )

        self.state.create_index(
            [
                ("partition_date", ASCENDING),
                ("body", ASCENDING),
            ],
            name="state_partition_body_1",
        )

    def close_spider(self) -> None:
        self.client.close()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        now = datetime.now(timezone.utc)
        natural_key = _natural_key(adapter)

        if adapter.get("unchanged"):
            # The immutable landing version is not touched. Only mutable
            # operational state records that it was observed again.
            self.state.update_one(
                natural_key,
                {
                    "$set": {
                        "last_seen_at": now,
                        "last_run_id": spider.run_id,
                    }
                },
            )

            adapter["file_content"] = b""
            self._count_scraped(adapter, spider)
            return item

        if adapter.get("known_version"):
            stored_version = self.versions.find_one(
                _version_key(adapter)
            )

            if stored_version is None:
                raise RuntimeError(
                    "Item was marked as a known historical version, "
                    "but that immutable version could not be found: "
                    f"{adapter.get('identifier')}"
                )

            version_id = stored_version["_id"]

            logger.info(
                "reusing immutable landing version",
                extra={
                    "identifier": adapter["identifier"],
                    "version_id": str(version_id),
                    "content_hash": adapter["content_hash"],
                    "file_path": stored_version["file_path"],
                },
            )

        else:
            version_doc = {
                key: adapter.get(key)
                for key in (
                    "identifier",
                    "title",
                    "description",
                    "published_date",
                    "doc_url",
                    "body",
                    "partition_date",
                    "partition_label",
                    "source",
                    "scraped_at",
                    "file_ext",
                    "content_type",
                    "file_hash",
                    "content_hash",
                    "file_path",
                    "size_bytes",
                )
            }

            version_doc.update(
                {
                    "schema_version": 1,
                    "first_seen_at": now,
                    "first_run_id": spider.run_id,
                }
            )

            stored_version = version_doc

            try:
                result = self.versions.insert_one(version_doc)
                version_id = result.inserted_id

                logger.info(
                    "inserted immutable landing version",
                    extra={
                        "identifier": adapter["identifier"],
                        "version_id": str(version_id),
                        "content_hash": adapter["content_hash"],
                        "file_hash": adapter["file_hash"],
                    },
                )

            except DuplicateKeyError:
                # Genuine concurrent insertion race.
                stored_version = self.versions.find_one(
                    _version_key(adapter)
                )

                if stored_version is None:
                    raise

                version_id = stored_version["_id"]

                logger.info(
                    "landing version already inserted concurrently",
                    extra={
                        "identifier": adapter["identifier"],
                        "version_id": str(version_id),
                        "content_hash": adapter["content_hash"],
                    },
                )

        state_doc = {
            "source": adapter["source"],
            "body": adapter["body"],
            "identifier": adapter["identifier"],
            "title": adapter.get("title"),
            "description": adapter.get("description"),
            "published_date": adapter.get("published_date"),
            "doc_url": adapter.get("doc_url"),
            "partition_date": adapter.get("partition_date"),
            "partition_label": adapter.get("partition_label"),
            "latest_version_id": version_id,
            "latest_content_hash": stored_version["content_hash"],
            "latest_file_hash": stored_version["file_hash"],
            "latest_file_path": stored_version["file_path"],
            "latest_size_bytes": stored_version["size_bytes"],
            "latest_file_ext": stored_version["file_ext"],
            "latest_content_type": stored_version.get("content_type"),
            "last_seen_at": now,
            "last_run_id": spider.run_id,
        }

        self.state.update_one(
            natural_key,
            {
                "$set": state_doc,
                "$setOnInsert": {
                    "first_seen_at": now,
                    "first_run_id": spider.run_id,
                },
            },
            upsert=True,
        )

        adapter["file_content"] = b""
        self._count_scraped(adapter, spider)
        return item

    @staticmethod
    def _count_scraped(adapter: ItemAdapter, spider) -> None:
        """Count success only here: the document is now fully persisted
        (hashed, object stored, metadata + state written). Counting at
        download time would let pipeline-stage failures pose as successes."""
        key = (adapter["partition_label"], adapter["body"])
        spider.run_stats[key]["scraped"] += 1
