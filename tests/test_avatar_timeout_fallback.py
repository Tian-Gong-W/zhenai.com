"""Regression check for avatar download timeouts.

Run with: python3 tests/test_avatar_timeout_fallback.py
"""

import importlib.util
import os
import tempfile
from pathlib import Path

import requests

SOURCE = Path(__file__).resolve().parents[1] / "app.py"
with tempfile.TemporaryDirectory() as temp_dir:
    os.environ["ZHENAI_BASE_DIR"] = temp_dir
    os.environ["DASHBOARD_ACCESS_TOKEN"] = "test-token"
    spec = importlib.util.spec_from_file_location("avatar_timeout_fallback_test", SOURCE)
    service = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(service)
    service.init_db()

    original_get = service.requests.get
    service.requests.get = lambda *args, **kwargs: (_ for _ in ()).throw(requests.ReadTimeout("avatar timed out"))
    assert service.download_avatar("https://photo.zastatic.com/avatar.jpg") is None
    service.requests.get = original_get

for name in ("ZHENAI_BASE_DIR", "DASHBOARD_ACCESS_TOKEN"):
    os.environ.pop(name, None)
print("PASS: avatar timeout returns no-avatar fallback instead of aborting card delivery")
