from __future__ import annotations

import sqlite3
import unittest

from app.engine.schema import compare_and_swap_state, initialize_engine_schema
from app.engine.state_machine import EngineState, InvalidStateTransition, require_transition


LEGACY_SCHEMA = """
CREATE TABLE messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_chat_id TEXT NOT NULL,
    source_message_id INTEGER NOT NULL,
    dest_chat_id TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    next_retry_at TEXT,
    file_unique_key TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    source_topic_id INTEGER,
    dest_topic_id INTEGER,
    media_group_id TEXT,
    source_message_ids TEXT NOT NULL,
    dest_message_ids TEXT,
    media_type TEXT,
    file_size INTEGER,
    caption TEXT,
    verified_at TEXT
);
"""


class PhaseOneSchemaTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(LEGACY_SCHEMA)
        self.conn.execute(
            """
            INSERT INTO messages(
                source_chat_id, source_message_id, dest_chat_id, status,
                file_unique_key, created_at, updated_at, source_message_ids
            ) VALUES('source', 1, 'dest', 'pending', 'key', 'now', 'now', '[1]')
            """
        )
        self.conn.commit()

    def tearDown(self) -> None:
        self.conn.close()

    def test_schema_upgrade_is_idempotent_and_preserves_legacy_row(self) -> None:
        initialize_engine_schema(self.conn)
        initialize_engine_schema(self.conn)

        row = self.conn.execute("SELECT * FROM messages WHERE id = 1").fetchone()
        self.assertEqual(row["status"], "pending")
        self.assertEqual(row["engine_state"], "planned")
        self.assertEqual(row["state_version"], 0)

        version = self.conn.execute(
            "SELECT value FROM engine_schema_meta WHERE key = 'engine_schema_version'"
        ).fetchone()
        self.assertEqual(version["value"], "1")

    def test_invalid_transition_is_rejected(self) -> None:
        with self.assertRaises(InvalidStateTransition):
            require_transition(EngineState.PLANNED, EngineState.COMMITTED)

    def test_compare_and_swap_rejects_stale_version(self) -> None:
        initialize_engine_schema(self.conn)
        first = compare_and_swap_state(
            self.conn,
            job_id=1,
            expected_state=EngineState.PLANNED,
            target_state=EngineState.LEASED,
            expected_version=0,
        )
        stale = compare_and_swap_state(
            self.conn,
            job_id=1,
            expected_state=EngineState.PLANNED,
            target_state=EngineState.LEASED,
            expected_version=0,
        )

        self.assertTrue(first)
        self.assertFalse(stale)
        row = self.conn.execute("SELECT engine_state, state_version FROM messages WHERE id = 1").fetchone()
        self.assertEqual(row["engine_state"], "leased")
        self.assertEqual(row["state_version"], 1)

    def test_lease_token_is_unique(self) -> None:
        initialize_engine_schema(self.conn)
        self.conn.execute(
            """
            INSERT INTO messages(
                source_chat_id, source_message_id, dest_chat_id, status,
                file_unique_key, created_at, updated_at, source_message_ids,
                engine_state, lease_token
            ) VALUES('source', 2, 'dest', 'pending', 'key2', 'now', 'now', '[2]', 'leased', 'token')
            """
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute("UPDATE messages SET lease_token = 'token' WHERE id = 1")


if __name__ == "__main__":
    unittest.main()
