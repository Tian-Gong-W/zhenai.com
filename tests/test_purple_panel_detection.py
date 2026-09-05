"""Regression checks for real-time purple visibility detection.

Run with: python3 tests/test_purple_panel_detection.py
"""

import importlib.util
import os
import tempfile
from pathlib import Path

SOURCE = Path(__file__).resolve().parents[1] / "app.py"
BASE_NOW = 1_800_000_000.0


def moment(moment_type, seconds):
    return {"moment": {"type": moment_type, "createTime": int(seconds * 1000)}}


with tempfile.TemporaryDirectory() as temp_dir:
    os.environ["ZHENAI_BASE_DIR"] = temp_dir
    spec = importlib.util.spec_from_file_location("purple_panel_detection_test", SOURCE)
    service = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(service)
    crawler = object.__new__(service.ZhenaiCrawler)

    recent_certification = BASE_NOW - 26 * 3600
    # Certification and avatar records alone must never be treated as a visible purple panel.
    assert crawler.has_purple_panel(recent_certification * 1000, [
        moment(6, recent_certification), moment(4, recent_certification + 60)
    ], now=BASE_NOW) is False

    # A daily type-0 post after certification and within the real-time 48-hour window qualifies.
    assert crawler.has_purple_panel(recent_certification * 1000, [
        moment(6, recent_certification), moment(0, recent_certification + 60)
    ], now=BASE_NOW) is True

    # type-1 is also a daily post and qualifies.
    assert crawler.has_purple_panel(recent_certification * 1000, [
        moment(6, recent_certification), moment(1, recent_certification + 60)
    ], now=BASE_NOW) is True

    # Old posts, even type 0/1, are before the latest certification and must not qualify.
    assert crawler.has_purple_panel(recent_certification * 1000, [
        moment(1, recent_certification - 180 * 86400), moment(6, recent_certification)
    ], now=BASE_NOW) is False

    # Other types are not daily posts for this rule.
    assert crawler.has_purple_panel(recent_certification * 1000, [
        moment(6, recent_certification), moment(2, recent_certification + 60),
        moment(8, recent_certification + 120), moment(12, recent_certification + 180)
    ], now=BASE_NOW) is False

    # Certification outside the trailing 48-hour window must not qualify.
    old_certification = BASE_NOW - 49 * 3600
    assert crawler.has_purple_panel(old_certification * 1000, [
        moment(6, old_certification), moment(0, old_certification + 60)
    ], now=BASE_NOW) is False

    # When multiple certification cards exist, parsing uses the latest one.
    original_time = service.time.time
    service.time.time = lambda: BASE_NOW
    parsed = crawler.parse_moments_times([
        moment(6, BASE_NOW - 9 * 86400), moment(1, BASE_NOW - 8 * 86400),
        moment(6, recent_certification), moment(0, recent_certification + 60),
    ])
    service.time.time = original_time
    assert parsed[3] is True

os.environ.pop("ZHENAI_BASE_DIR", None)
print("PASS: purple visibility requires a post-certification type-0/1 daily moment within 48 hours")
