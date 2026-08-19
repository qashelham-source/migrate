from app.admin_bot import (
    _duplicates_menu,
    _duplicates_text_from_data,
    _finder_menu,
    _finder_result_text,
    _finder_text_from_stats,
)


def _callbacks(markup: object) -> set[str]:
    return {
        str(button.callback_data)
        for row in markup.inline_keyboard
        for button in row
        if button.callback_data
    }


def test_finder_menu_routes_to_real_actions() -> None:
    callbacks = _callbacks(_finder_menu())
    assert {"finder:index", "finder:search", "finder:view"}.issubset(callbacks)


def test_duplicate_menu_routes_to_real_actions() -> None:
    callbacks = _callbacks(_duplicates_menu())
    assert {
        "duplicates:index",
        "duplicates:view",
        "duplicates:destination:scan",
        "duplicates:destination:preview",
    }.issubset(callbacks)


def test_finder_stats_and_result_are_operator_readable() -> None:
    text = _finder_text_from_stats(
        {
            "indexed": 12,
            "unique_fingerprints": 10,
            "duplicate_records": 2,
            "duplicate_rate": 16.7,
            "match_history": 4,
        },
        indexed_now=3,
    )
    assert "Indexed media: 12" in text
    assert "3 new media queue item(s) indexed" in text

    result = _finder_result_text(
        "https://t.me/c/123456/88",
        {
            "source_chat_id": "-100123456",
            "source_message_id": 88,
            "media_type": "video",
            "file_size": 2048,
            "file_name": "sample.mp4",
        },
    )
    assert "Original media found" in result
    assert "Message ID: 88" in result


def test_duplicate_text_shows_groups() -> None:
    text = _duplicates_text_from_data(
        {
            "indexed": 5,
            "duplicate_records": 2,
            "duplicate_rate": 40.0,
        },
        [
            {
                "copies": 3,
                "original_chat_id": "-1001",
                "original_message_id": 9,
                "locations": "-1001:9,-1002:20,-1003:30",
            }
        ],
    )
    assert "3 copies" in text
    assert "Original: -1001/9" in text
