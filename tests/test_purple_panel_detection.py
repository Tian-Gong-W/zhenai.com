"""Regression checks for the certification-only purple eligibility rule.

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
    window_start = BASE_NOW - service.CERTIFICATION_WINDOW_HOURS * 3600

    # A recent certification without any daily post qualifies.
    assert crawler.has_purple_panel(recent_certification * 1000, [
        moment(6, recent_certification), moment(4, recent_certification + 60)
    ], now=BASE_NOW) is True

    # Recent type-0 and type-1 posts do not disqualify the member.
    assert crawler.has_purple_panel(recent_certification * 1000, [
        moment(6, recent_certification), moment(0, recent_certification + 60),
        moment(1, recent_certification + 120),
    ], now=BASE_NOW) is True

    # A daily type-0/1 post older than the real-time 48-hour window disqualifies.
    assert crawler.has_purple_panel(recent_certification * 1000, [
        moment(1, window_start - 1), moment(6, recent_certification)
    ], now=BASE_NOW) is False

    # System/profile cards are not daily posts and cannot disqualify.
    assert crawler.has_purple_panel(recent_certification * 1000, [
        moment(2, window_start - 86400), moment(4, window_start - 86400),
        moment(5, window_start - 86400), moment(6, recent_certification),
        moment(7, window_start - 86400), moment(8, window_start - 86400),
        moment(12, window_start - 86400),
    ], now=BASE_NOW) is True

    # Certification outside the trailing 48-hour window never qualifies.
    old_certification = BASE_NOW - 49 * 3600
    assert crawler.has_purple_panel(old_certification * 1000, [
        moment(6, old_certification)
    ], now=BASE_NOW) is False

    # Parsing uses the latest certification card rather than an earlier one.
    original_time = service.time.time
    service.time.time = lambda: BASE_NOW
    parsed = crawler.parse_moments_times([
        moment(6, BASE_NOW - 9 * 86400), moment(1, BASE_NOW - 8 * 86400),
        moment(6, recent_certification),
    ])
    service.time.time = original_time
    assert parsed[3] is False

os.environ.pop("ZHENAI_BASE_DIR", None)
print("PASS: purple eligibility requires recent certification and no pre-window type-0/1 daily moments")
