from pathlib import Path

from app.db import Database
from app.media_finder import (
    MediaDescriptor,
    build_fingerprint,
    compare_descriptors,
    duplicate_groups,
    find_by_reference,
    find_matches,
    index_existing_queue,
    index_media,
    initialize_media_finder,
    media_finder_stats,
    parse_telegram_reference,
)


def make_db(tmp_path: Path) -> Database:
    db = Database(tmp_path / "queue.db")
    db.initialize()
    initialize_media_finder(db)
    return db


def video(**overrides: object) -> MediaDescriptor:
    values = {
        "media_type": "video",
        "file_size": 10_000_000,
        "duration": 120,
        "width": 1920,
        "height": 1080,
        "mime_type": "video/mp4",
        "file_name": "example.mp4",
        "telegram_file_unique_id": "telegram-unique-1",
        "thumbnail_hash": "thumb-abc",
    }
    values.update(overrides)
    return MediaDescriptor(**values)


def test_fingerprint_is_stable_and_does_not_depend_on_filename() -> None:
    first = video(file_name="original-name.mp4")
    renamed = video(file_name="renamed-copy.mp4")
    assert build_fingerprint(first) == build_fingerprint(renamed)


def test_changed_stable_metadata_changes_fingerprint() -> None:
    assert build_fingerprint(video()) != build_fingerprint(video(duration=121))


def test_confidence_explains_exact_match() -> None:
    confidence, reasons, differences = compare_descriptors(video(), video())
    assert confidence == 100.0
    assert "Same Telegram file unique ID" in reasons
    assert "Same duration" in reasons
    assert not differences


def test_confidence_allows_small_size_variation_but_reports_resolution_change() -> None:
    confidence, reasons, differences = compare_descriptors(
        video(telegram_file_unique_id=None, thumbnail_hash=None),
        video(
            telegram_file_unique_id=None,
            thumbnail_hash=None,
            file_size=10_050_000,
            width=1280,
            height=720,
        ),
    )
    assert 55.0 <= confidence < 95.0
    assert "Similar file size" in reasons or "Same file size" in reasons
    assert "Different width" in differences
    assert "Different height" in differences


def test_index_and_find_exact_duplicate(tmp_path: Path) -> None:
    db = make_db(tmp_path)
    try:
        fingerprint_id = index_media(
            db,
            source_chat_id="-100123",
            source_message_id=77,
            descriptor=video(),
            metadata={"caption": "ignored by fingerprint"},
        )
        results = find_matches(db, video())
        assert len(results) == 1
        assert results[0].fingerprint_id == fingerprint_id
        assert results[0].confidence >= 99.0
        assert results[0].is_duplicate is True
        assert "Same stable fingerprint" in results[0].reasons
    finally:
        db.close()


def test_upsert_preserves_one_location_record(tmp_path: Path) -> None:
    db = make_db(tmp_path)
    try:
        first_id = index_media(
            db,
            source_chat_id="-100123",
            source_message_id=77,
            descriptor=video(),
        )
        second_id = index_media(
            db,
            source_chat_id="-100123",
            source_message_id=77,
            descriptor=video(duration=121),
        )
        assert first_id == second_id
        assert db.query_one("SELECT COUNT(*) AS count FROM media_fingerprints")["count"] == 1
    finally:
        db.close()


def test_duplicate_group_and_dashboard_stats(tmp_path: Path) -> None:
    db = make_db(tmp_path)
    try:
        descriptor = video()
        index_media(db, source_chat_id="-1001", source_message_id=1, descriptor=descriptor)
        index_media(db, source_chat_id="-1002", source_message_id=2, descriptor=descriptor)
        groups = duplicate_groups(db)
        assert len(groups) == 1
        assert groups[0]["copies"] == 2

        find_matches(db, descriptor)
        stats = media_finder_stats(db)
        assert stats["indexed"] == 2
        assert stats["unique_fingerprints"] == 1
        assert stats["duplicate_records"] == 1
        assert stats["duplicate_rate"] == 50.0
        assert stats["match_history"] == 2
        assert stats["average_confidence"] >= 99.0
    finally:
        db.close()


def test_parse_and_find_telegram_reference(tmp_path: Path) -> None:
    assert parse_telegram_reference("https://t.me/mychannel/123") == ("mychannel", 123)
    assert parse_telegram_reference("https://t.me/c/123456/88?single") == ("123456", 88)
    assert parse_telegram_reference("42") == ("", 42)
    assert parse_telegram_reference("not a link") is None

    db = make_db(tmp_path)
    try:
        index_media(db, source_chat_id="@mychannel", source_message_id=123, descriptor=video())
        found = find_by_reference(db, "https://t.me/mychannel/123")
        assert found is not None
        assert found["source_message_id"] == 123
    finally:
        db.close()


def test_backfill_existing_queue_is_idempotent(tmp_path: Path) -> None:
    db = make_db(tmp_path)
    try:
        assert db.enqueue_message(
            source_chat_id="-1001",
            source_message_id=10,
            dest_chat_id="-2001",
            file_unique_key="queue-key-10",
            source_message_ids=[10],
            source_topic_id=None,
            dest_topic_id=None,
            media_group_id=None,
            media_type="video",
            file_size=2048,
            caption="caption is not fingerprint input",
        )
        assert index_existing_queue(db) == 1
        assert index_existing_queue(db) == 0
        stats = media_finder_stats(db)
        assert stats["indexed"] == 1
    finally:
        db.close()


def test_limited_backfill_advances_to_unindexed_queue_items(tmp_path: Path) -> None:
    db = make_db(tmp_path)
    try:
        for message_id in (10, 11):
            assert db.enqueue_message(
                source_chat_id="-1001",
                source_message_id=message_id,
                dest_chat_id="-2001",
                file_unique_key=f"queue-key-{message_id}",
                source_message_ids=[message_id],
                source_topic_id=None,
                dest_topic_id=None,
                media_group_id=None,
                media_type="video",
                file_size=2048,
                caption=None,
            )
        assert index_existing_queue(db, limit=1) == 1
        assert index_existing_queue(db, limit=1) == 1
        assert media_finder_stats(db)["indexed"] == 2
    finally:
        db.close()


def test_private_telegram_link_uses_numeric_chat_id_prefix(tmp_path: Path) -> None:
    db = make_db(tmp_path)
    try:
        index_media(db, source_chat_id="-100123456", source_message_id=88, descriptor=video())
        found = find_by_reference(db, "https://t.me/c/123456/88")
        assert found is not None
        assert found["source_chat_id"] == "-100123456"
    finally:
        db.close()
