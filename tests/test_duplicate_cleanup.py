from pathlib import Path

from app.db import Database, utc_now
from app.duplicate_cleanup import (
    mark_duplicate_delivery_deleted,
    plan_duplicate_delivery_cleanup,
)
from app.skip_policy import is_expected_skip_reason


def make_db(tmp_path: Path) -> Database:
    db = Database(tmp_path / "queue.db")
    db.initialize()
    return db


def add_verified_delivery(
    db: Database,
    *,
    source: str,
    source_message_id: int,
    destination: str,
    destination_message_ids: list[int],
    fingerprint: str = "telegram-file-unique-id",
    topic_id: int | None = None,
    verified: bool = True,
) -> int:
    assert db.enqueue_message(
        source_chat_id=source,
        source_message_id=source_message_id,
        dest_chat_id=destination,
        file_unique_key=fingerprint,
        source_message_ids=[source_message_id],
        source_topic_id=None,
        dest_topic_id=topic_id,
        media_group_id=None,
        media_type="video",
        file_size=2048,
        caption=None,
    )
    row = db.query_one(
        "SELECT id FROM messages WHERE source_chat_id = ? AND source_message_id = ? AND dest_chat_id = ?",
        (source, source_message_id, destination),
    )
    assert row is not None
    job_id = int(row["id"])
    assert db.set_status(
        job_id,
        "copied",
        dest_message_ids=destination_message_ids,
        verified_at=utc_now() if verified else None,
    )
    return job_id


def test_cleanup_targets_exact_copies_with_saved_destination_ids(tmp_path: Path) -> None:
    db = make_db(tmp_path)
    try:
        kept_job = add_verified_delivery(
            db,
            source="-1001",
            source_message_id=1,
            destination="-2001",
            destination_message_ids=[10],
        )
        duplicate_job = add_verified_delivery(
            db,
            source="-1002",
            source_message_id=2,
            destination="-2001",
            destination_message_ids=[20],
        )
        # Same media in another destination or forum topic is intentionally kept.
        add_verified_delivery(
            db,
            source="-1003",
            source_message_id=3,
            destination="-2002",
            destination_message_ids=[30],
        )
        add_verified_delivery(
            db,
            source="-1004",
            source_message_id=4,
            destination="-2001",
            destination_message_ids=[40],
            topic_id=77,
        )
        # Text fallbacks are never eligible.  A completed delivery without a
        # later strong-verification timestamp is still safe: its exact
        # destination message ID was recorded when Telegram accepted the send.
        add_verified_delivery(
            db,
            source="-1005",
            source_message_id=5,
            destination="-2001",
            destination_message_ids=[50],
            fingerprint="messages:-1005:5",
        )
        unverified_duplicate_job = add_verified_delivery(
            db,
            source="-1006",
            source_message_id=6,
            destination="-2001",
            destination_message_ids=[60],
            verified=False,
        )

        plan = plan_duplicate_delivery_cleanup(db)
        assert plan.group_count == 1
        assert plan.delivery_count == 2
        assert plan.message_count == 2
        assert [candidate.job_id for candidate in plan.candidates] == [duplicate_job, unverified_duplicate_job]
        assert [candidate.dest_message_ids for candidate in plan.candidates] == [(20,), (60,)]
        assert all(candidate.kept_job_id == kept_job for candidate in plan.candidates)
        assert all(candidate.kept_dest_message_ids == (10,) for candidate in plan.candidates)
    finally:
        db.close()


def test_cleanup_retains_a_skip_record_so_the_media_is_not_sent_again(tmp_path: Path) -> None:
    db = make_db(tmp_path)
    try:
        kept_job = add_verified_delivery(
            db,
            source="-1001",
            source_message_id=1,
            destination="-2001",
            destination_message_ids=[10],
        )
        duplicate_job = add_verified_delivery(
            db,
            source="-1002",
            source_message_id=2,
            destination="-2001",
            destination_message_ids=[20],
            verified=False,
        )

        candidate = plan_duplicate_delivery_cleanup(db).candidates[0]
        assert mark_duplicate_delivery_deleted(db, candidate)
        row = db.query_one("SELECT status, last_error FROM messages WHERE id = ?", (duplicate_job,))
        assert row is not None
        assert row["status"] == "skipped"
        assert is_expected_skip_reason(row["last_error"])

        # Even if the retained delivery record is no longer available, a future
        # scan still sees the cleanup record as an anti-duplicate marker.
        db.execute("UPDATE messages SET status = 'failed' WHERE id = ?", (kept_job,))
        existing = db.find_duplicate_media_delivery(
            source_chat_id="-1099",
            source_message_id=99,
            dest_chat_id="-2001",
            dest_topic_id=None,
            file_unique_key="telegram-file-unique-id",
        )
        assert existing is not None
        assert existing["status"] == "skipped"
    finally:
        db.close()


def test_cleanup_skips_ambiguous_overlapping_destination_ids(tmp_path: Path) -> None:
    db = make_db(tmp_path)
    try:
        add_verified_delivery(
            db,
            source="-1001",
            source_message_id=1,
            destination="-2001",
            destination_message_ids=[10, 11],
        )
        add_verified_delivery(
            db,
            source="-1002",
            source_message_id=2,
            destination="-2001",
            destination_message_ids=[11, 12],
        )
        plan = plan_duplicate_delivery_cleanup(db)
        assert plan.delivery_count == 0
        assert plan.group_count == 0
    finally:
        db.close()
