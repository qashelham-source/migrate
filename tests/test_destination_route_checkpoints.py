from app.db import Database
from app.scanner import build_scan_plan, destination_route_baseline


def test_new_destination_bootstraps_history_without_reusing_old_route(tmp_path):
    db = Database(tmp_path / "migration.sqlite3")
    db.initialize()

    source = "-100111"
    old_destination = "-100222"
    new_destination = "-100333"

    db.enqueue_message(
        source_chat_id=source,
        source_message_id=100,
        dest_chat_id=old_destination,
        file_unique_key="media:old",
        source_message_ids=[100],
        source_topic_id=None,
        dest_topic_id=None,
        media_group_id=None,
        media_type="video",
        file_size=1,
        caption=None,
    )
    db.set_scan_checkpoint(source, None, 100, "incremental")

    # Re-running initialize is the production upgrade path. It seeds the old
    # delivery route but deliberately leaves a brand-new destination unseeded.
    db.initialize()
    assert db.get_destination_scan_checkpoint(source, None, old_destination, None)
    assert db.get_destination_scan_checkpoint(source, None, new_destination, None) is None

    old_checkpoint = db.get_destination_scan_checkpoint(
        source, None, old_destination, None
    )
    baseline, route_bootstrap = destination_route_baseline(
        scan_mode="incremental",
        route_checkpoints=[
            int(old_checkpoint["last_scanned_message_id"]),
            None,
        ],
    )

    assert baseline is None
    assert route_bootstrap is True

    plan = build_scan_plan(
        configured_start=1,
        configured_end=None,
        latest_message_id=100,
        checkpoint=baseline,
        queue_highwater=None,
        scan_mode="incremental",
    )
    assert plan is not None
    assert plan.start_id == 1
    assert plan.end_id == 100


def test_existing_destination_routes_share_the_oldest_checkpoint(tmp_path):
    db = Database(tmp_path / "migration.sqlite3")
    db.initialize()

    source = "-100111"
    first_destination = "-100222"
    second_destination = "-100333"
    db.set_destination_scan_checkpoint(
        source, None, first_destination, None, 100, "incremental"
    )
    db.set_destination_scan_checkpoint(
        source, None, second_destination, None, 80, "incremental"
    )

    first = db.get_destination_scan_checkpoint(source, None, first_destination, None)
    second = db.get_destination_scan_checkpoint(source, None, second_destination, None)
    baseline, route_bootstrap = destination_route_baseline(
        scan_mode="incremental",
        route_checkpoints=[
            int(first["last_scanned_message_id"]),
            int(second["last_scanned_message_id"]),
        ],
    )

    assert baseline == 80
    assert route_bootstrap is False

    db.set_scan_checkpoint(source, None, 100, "incremental")
    assert db.reset_scan_checkpoints(source) == 1
    assert db.get_destination_scan_checkpoint(source, None, first_destination, None) is None
    assert db.get_destination_scan_checkpoint(source, None, second_destination, None) is None
