"""Regression check for purple visibility detection.

Run with: python3 tests/test_purple_panel_detection.py
"""

import importlib.util
import os
import tempfile
from pathlib import Path

SOURCE = Path(__file__).resolve().parents[1] / "app.py"
with tempfile.TemporaryDirectory() as temp_dir:
    os.environ["ZHENAI_BASE_DIR"] = temp_dir
    spec = importlib.util.spec_from_file_location("purple_panel_detection_test", SOURCE)
    service = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(service)

    crawler = object.__new__(service.ZhenaiCrawler)
    crawler.get_show_moments = lambda object_id: {"contents": [], "momentCount": 2}
    assert crawler.has_purple_panel("member-1", cert_ts=1) is False

    # A certification card plus an avatar change must not be treated as a visible purple panel.
    moments = [
        {"moment": {"type": 6, "createTime": 1_700_000_000_000}},
        {"moment": {"type": 4, "createTime": 1_700_000_100_000}},
    ]
    _, _, _, panel_visible = crawler.parse_moments_times(moments, object_id="member-1")
    assert panel_visible is False

    crawler.get_show_moments = lambda object_id: {"contents": [{"id": "visible"}], "momentCount": 1}
    assert crawler.has_purple_panel("member-1", cert_ts=1) is True

    crawler.get_show_moments = lambda object_id: None
    assert crawler.has_purple_panel("member-1", cert_ts=1) is False

os.environ.pop("ZHENAI_BASE_DIR", None)
print("PASS: purple visibility uses non-empty show-moments contents, not cards or momentCount")
