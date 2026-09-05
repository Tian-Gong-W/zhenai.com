"""Regression check for concurrent dashboard log streams.

Run with: python3 tests/test_dashboard_stream_broadcast.py
"""

import importlib.util
import os
import tempfile
import threading
from pathlib import Path

SOURCE = Path(__file__).resolve().parents[1] / "app.py"
with tempfile.TemporaryDirectory() as temp_dir:
    os.environ["ZHENAI_BASE_DIR"] = temp_dir
    os.environ["DASHBOARD_ACCESS_TOKEN"] = "test-token"
    spec = importlib.util.spec_from_file_location("dashboard_stream_broadcast_test", SOURCE)
    service = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(service)
    service.init_db()

    streams = []
    for _ in range(2):
        with service.app.test_request_context("/stream"):
            streams.append(iter(service.stream.__wrapped__().response))

    results = [None, None]

    def read_one(index):
        results[index] = next(streams[index])

    readers = [threading.Thread(target=read_one, args=(index,), daemon=True) for index in range(2)]
    for reader in readers:
        reader.start()
    service.emit("BROADCAST_CHECK")
    for reader in readers:
        reader.join(timeout=2)
    assert all(result and '"msg": "BROADCAST_CHECK"' in result for result in results), results

    service.clear_live_log_history()
    service.emit("REFRESH_HISTORY_CHECK")
    with service.app.test_request_context("/stream"):
        refreshed_stream = iter(service.stream.__wrapped__().response)
    refreshed_event = next(refreshed_stream)
    assert '"msg": "REFRESH_HISTORY_CHECK"' in refreshed_event, refreshed_event

for name in ("ZHENAI_BASE_DIR", "DASHBOARD_ACCESS_TOKEN"):
    os.environ.pop(name, None)
print("PASS: broadcast and refresh history behavior")
