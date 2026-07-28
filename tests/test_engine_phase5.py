from __future__ import annotations

import os
import sqlite3
import time
from pathlib import Path

from app.engine.verification import (
    cleanup_committed_job,
    commit_verified_job,
    initialize_verification_schema,
    reap_stale_directories,
    register_temp_artifact,
    verify_job,
)


def make_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(
        """
        CREATE TABLE messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            engine_state TEXT NOT NULL,
            verified_at TEXT,
            state_version INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            lease_token TEXT,
            lease_expires_at TEXT
        );
        CREATE TABLE publish_intents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER,
            state TEXT NOT NULL,
            destination_message_ids TEXT
        );
        INSERT INTO messages(engine_state, updated_at) VALUES('verifying', 'now');
        INSERT INTO publish_intents(job_id, state, destination_message_ids)
        VALUES(1, 'acknowledged', '[901]');
        """
    )
    initialize_verification_schema(conn)
    return conn


def test_commit_requires_all_verification_layers() -> None:
    conn = make_db()
    failed = verify_job(
        conn,
        job_id=1,
        publish_intent_id=1,
        expected_item_count=2,
        observed_item_count=1,
        content_confidence=0.99,
    )
    assert failed.acknowledged is True
    assert failed.structural_ok is False
    assert failed.verified is False
    assert commit_verified_job(conn, job_id=1) is False

    passed = verify_job(
        conn,
        job_id=1,
        publish_intent_id=1,
        expected_item_count=1,
        observed_item_count=1,
        content_confidence=0.99,
    )
    assert passed.verified is True
    assert commit_verified_job(conn, job_id=1) is True
    assert conn.execute("SELECT engine_state FROM messages WHERE id = 1").fetchone()[0] == "committed"


def test_cleanup_happens_only_after_verified_commit(tmp_path) -> None:
    conn = make_db()
    artifact = tmp_path / "job-1"
    artifact.mkdir()
    (artifact / "media.bin").write_bytes(b"data")
    register_temp_artifact(conn, job_id=1, path=str(artifact))

    assert cleanup_committed_job(conn, job_id=1) == 0
    assert artifact.exists()

    verify_job(
        conn,
        job_id=1,
        publish_intent_id=1,
        expected_item_count=1,
        observed_item_count=1,
        content_confidence=1.0,
    )
    assert commit_verified_job(conn, job_id=1) is True
    assert cleanup_committed_job(conn, job_id=1) == 1
    assert not artifact.exists()
    assert conn.execute("SELECT engine_state FROM messages WHERE id = 1").fetchone()[0] == "done"


def test_ambiguous_job_retains_temp_data(tmp_path) -> None:
    conn = make_db()
    artifact = tmp_path / "ambiguous"
    artifact.mkdir()
    register_temp_artifact(conn, job_id=1, path=str(artifact))
    conn.execute("UPDATE messages SET engine_state = 'needs_reconciliation' WHERE id = 1")
    conn.execute("UPDATE publish_intents SET state = 'needs_reconciliation' WHERE id = 1")
    conn.commit()

    assert cleanup_committed_job(conn, job_id=1) == 0
    assert artifact.exists()


def test_reaper_never_deletes_active_lease_or_unresolved_intent(tmp_path) -> None:
    conn = make_db()
    now_future = "2999-01-01T00:00:00+00:00"
    active = tmp_path / "active"
    unresolved = tmp_path / "unresolved"
    orphan = tmp_path / "orphan"
    for path in (active, unresolved, orphan):
        path.mkdir()
        os.utime(path, (1, 1))

    register_temp_artifact(conn, job_id=1, path=str(active))
    conn.execute(
        "UPDATE messages SET engine_state='committed', lease_token='live', lease_expires_at=? WHERE id=1",
        (now_future,),
    )
    conn.commit()
    summary = reap_stale_directories(conn, root=str(tmp_path), older_than_epoch=time.time())
    assert active.exists()
    assert unresolved.exists() is False
    assert orphan.exists() is False
    assert summary.retained == 1

    unresolved.mkdir()
    os.utime(unresolved, (1, 1))
    register_temp_artifact(conn, job_id=1, path=str(unresolved))
    conn.execute("UPDATE messages SET lease_token=NULL, lease_expires_at=NULL WHERE id=1")
    conn.execute("UPDATE publish_intents SET state='needs_reconciliation' WHERE id=1")
    conn.commit()
    summary = reap_stale_directories(conn, root=str(tmp_path), older_than_epoch=time.time())
    assert unresolved.exists()
    assert summary.retained >= 1
