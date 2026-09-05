"""Regression check for Telegram response failure logging.

Run with: python3 tests/test_telegram_delivery_logging.py
"""

import importlib.util
import os
import tempfile
from pathlib import Path

SOURCE = Path(__file__).resolve().parents[1] / "app.py"


class FakeResponse:
    ok = False
    status_code = 401

    @staticmethod
    def json():
        return {"ok": False, "description": "Unauthorized"}


with tempfile.TemporaryDirectory() as temp_dir:
    os.environ["ZHENAI_BASE_DIR"] = temp_dir
    spec = importlib.util.spec_from_file_location("telegram_delivery_logging_test", SOURCE)
    service = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(service)

    emitted = []
    service.emit = emitted.append
    assert service.telegram_response_ok(FakeResponse(), "sendMessage") is False
    assert emitted == ["Telegram sendMessage 发送失败：HTTP 401 · Unauthorized"]

os.environ.pop("ZHENAI_BASE_DIR", None)
print("PASS: Telegram API failures produce a redacted actionable log message")
