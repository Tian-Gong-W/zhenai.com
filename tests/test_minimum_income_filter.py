"""Regression checks for the 3,000-yuan monthly income filter.

Run with: python3 tests/test_minimum_income_filter.py
"""

import importlib.util
import os
import tempfile
from pathlib import Path

SOURCE = Path(__file__).resolve().parents[1] / "dahai.py"

with tempfile.TemporaryDirectory() as temp_dir:
    os.environ["ZHENAI_BASE_DIR"] = temp_dir
    spec = importlib.util.spec_from_file_location("minimum_income_filter_test", SOURCE)
    service = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(service)

    assert service.DEFAULT_SEARCH_CONFIG["minimum_income"] == 3000
    assert service.income_meets_minimum("3000元以下", 3000) is False
    assert service.income_meets_minimum("3001-5000元", 3000) is True
    assert service.income_meets_minimum("5001-8000元", 3000) is True
    assert service.income_meets_minimum("50000元以上", 3000) is True
    assert service.income_meets_minimum("无固定收入", 3000) is False
    assert service.income_meets_minimum("", 3000) is False
    assert service.income_meets_minimum("3000元以下", 0) is True

os.environ.pop("ZHENAI_BASE_DIR", None)
print("PASS: only salary bands that can meet 3,000 yuan qualify")
