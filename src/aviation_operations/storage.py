from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any

from .models import DOMAIN_RECORD_TYPE


class DataStore:
    """Owns structured tables, idempotent writes, rejection records and batch state."""

    def __init__(self, database: str) -> None:
        self.conn = sqlite3.connect(database)
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.executescript("""
        CREATE TABLE IF NOT EXISTS batches (
            batch_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            status TEXT NOT NULL,
            processed INTEGER NOT NULL DEFAULT 0,
            accepted INTEGER NOT NULL DEFAULT 0,
            rejected INTEGER NOT NULL DEFAULT 0,
            error TEXT,
            started_at TEXT NOT NULL,
            finished_at TEXT
        );
        CREATE TABLE IF NOT EXISTS raw_manifests (
            digest TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            content_type TEXT NOT NULL,
            byte_count INTEGER NOT NULL,
            path TEXT,
            captured_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS records (
            record_key TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            raw_hash TEXT NOT NULL,
            first_seen_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS rejected_records (
            rejection_id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id INTEGER NOT NULL,
            source TEXT,
            error TEXT NOT NULL,
            raw_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(batch_id) REFERENCES batches(batch_id)
        );
        """)
        self.conn.commit()

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    def start_batch(self, source: str) -> int:
        cursor = self.conn.execute(
            "INSERT INTO batches(source, status, started_at) VALUES (?, 'running', ?)",
            (source, self._now()),
        )
        self.conn.commit()
        return int(cursor.lastrowid)

    def save_manifest(self, digest: str, source: str, content_type: str, byte_count: int, path: str | None) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO raw_manifests(digest, source, content_type, byte_count, path, captured_at) VALUES (?, ?, ?, ?, ?, ?)",
            (digest, source, content_type, byte_count, path, self._now()),
        )
        self.conn.commit()

    def upsert(self, record: DOMAIN_RECORD_TYPE) -> bool:
        now = self._now()
        cursor = self.conn.execute(
            """INSERT INTO records(record_key, source, observed_at, payload_json, raw_hash, first_seen_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(record_key) DO UPDATE SET source=excluded.source, observed_at=excluded.observed_at,
                 payload_json=excluded.payload_json, raw_hash=excluded.raw_hash, updated_at=excluded.updated_at
               WHERE records.raw_hash <> excluded.raw_hash""",
            (record.record_key, record.source, record.observed_at, json.dumps(record.payload, ensure_ascii=False, sort_keys=True), record.raw_hash, now, now),
        )
        self.conn.commit()
        return cursor.rowcount > 0

    def reject(self, batch_id: int, raw: Any, source: str | None, error: str) -> None:
        self.conn.execute(
            "INSERT INTO rejected_records(batch_id, source, error, raw_json, created_at) VALUES (?, ?, ?, ?, ?)",
            (batch_id, source, error, json.dumps(raw, ensure_ascii=False, sort_keys=True), self._now()),
        )
        self.conn.commit()

    def finish_batch(self, batch_id: int, status: str, processed: int, accepted: int, rejected: int, error: str | None = None) -> None:
        self.conn.execute(
            "UPDATE batches SET status=?, processed=?, accepted=?, rejected=?, error=?, finished_at=? WHERE batch_id=?",
            (status, processed, accepted, rejected, error, self._now(), batch_id),
        )
        self.conn.commit()

    def counts(self) -> dict[str, int]:
        return {
            "records": int(self.conn.execute("SELECT COUNT(*) FROM records").fetchone()[0]),
            "rejected": int(self.conn.execute("SELECT COUNT(*) FROM rejected_records").fetchone()[0]),
            "batches": int(self.conn.execute("SELECT COUNT(*) FROM batches").fetchone()[0]),
            "raw_manifests": int(self.conn.execute("SELECT COUNT(*) FROM raw_manifests").fetchone()[0]),
        }

    def close(self) -> None:
        self.conn.close()
