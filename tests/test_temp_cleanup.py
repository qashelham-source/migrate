from __future__ import annotations

from app.temp_cleanup import reap_abandoned_active_job_dirs


def test_startup_reaper_deletes_only_abandoned_job_directories(tmp_path) -> None:
    active = tmp_path / "downloads" / "active"
    abandoned_one = active / "job-101"
    abandoned_two = active / "job-202"
    preserved = active / "manual-notes"
    outside = tmp_path / "outside"
    abandoned_one.mkdir(parents=True)
    abandoned_two.mkdir()
    preserved.mkdir()
    outside.mkdir()
    (abandoned_one / "first.bin").write_bytes(b"abc")
    (abandoned_two / "second.bin").write_bytes(b"12345")
    (preserved / "keep.txt").write_text("keep", encoding="utf-8")
    (outside / "keep.txt").write_text("outside", encoding="utf-8")
    (active / "job-303").symlink_to(outside, target_is_directory=True)

    summary = reap_abandoned_active_job_dirs(active)

    assert summary.scanned == 2
    assert summary.deleted == 2
    assert summary.freed_bytes == 8
    assert summary.failed == 0
    assert not abandoned_one.exists()
    assert not abandoned_two.exists()
    assert preserved.exists()
    assert (outside / "keep.txt").exists()
