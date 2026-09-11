"""Regression checks for pre-launch dashboard controls.

Run with: python3 tests/test_dashboard_run_config.py
"""

import importlib.util
import json
import os
import tempfile
from pathlib import Path

SOURCE = Path(__file__).resolve().parents[1] / "app.py"
SOURCE_TEXT = SOURCE.read_text(encoding="utf-8")

assert "本次新增记录" not in SOURCE_TEXT
assert "累计入库" not in SOURCE_TEXT
assert 'grid-template-rows: min-content minmax(0, 1fr) auto;' in SOURCE_TEXT
assert 'background: rgba(237,233,254,.88);' in SOURCE_TEXT
assert ".dashboard::after" in SOURCE_TEXT
assert "center / cover no-repeat" in SOURCE_TEXT
assert "center / contain no-repeat" in SOURCE_TEXT
assert '<span class="log-heading">运行日志</span>' in SOURCE_TEXT
assert 'min-width: 112px; min-height: 38px;' in SOURCE_TEXT
assert "请至少选择 15 个省份" in SOURCE_TEXT

with tempfile.TemporaryDirectory() as temp_dir:
    os.environ["ZHENAI_BASE_DIR"] = temp_dir
    os.environ["DASHBOARD_ACCESS_TOKEN"] = "test-dashboard-token"
    spec = importlib.util.spec_from_file_location("dashboard_run_config_test", SOURCE)
    service = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(service)
    service.init_db()

    assert "福建" not in service.selectable_provinces()
    assert len(service.selectable_provinces()) == len(service.PROVINCE_MAP) - 1
    assert service.default_run_filter_config()["minimum_income"] == 3000
    assert service.default_run_filter_config()["push_window_hours"] == 48
    assert service.MIN_SELECTED_PROVINCES == 15

    selected_provinces = service.selectable_provinces()[:15]
    config = service.validate_run_filter_config({
        "provinces": list(reversed(selected_provinces)),
        "minimum_income": 8000,
        "push_window_hours": 24,
    })
    assert config == {"provinces": selected_provinces, "minimum_income": 8000, "push_window_hours": 24}
    service.save_run_filter_config(config)
    assert service.get_run_filter_config() == config

    for invalid in (
        {"provinces": [], "minimum_income": 3000, "push_window_hours": 48},
        {"provinces": ["福建"], "minimum_income": 3000, "push_window_hours": 48},
        {"provinces": service.selectable_provinces()[:14], "minimum_income": 3000, "push_window_hours": 48},
        {"provinces": ["北京"], "minimum_income": 4000, "push_window_hours": 48},
        {"provinces": ["北京"], "minimum_income": 3000, "push_window_hours": 36},
    ):
        try:
            service.validate_run_filter_config(invalid)
            raise AssertionError(f"invalid config accepted: {invalid}")
        except ValueError:
            pass

    captured_payload = {}

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {"isError": False, "data": {"list": [], "hasNext": False}}

    crawler = service.ZhenaiCrawler(token="token", seedtoken="")
    crawler.session.post = lambda _url, data, headers: captured_payload.update(data) or FakeResponse()
    crawler.search_members(page=2, page_size=20, age_begin=40, age_end=65, work_city=110100, gender=1, salary_begin=-1)
    assert captured_payload["salaryBegin"] == -1
    assert captured_payload["salaryEnd"] == -1

    service._launch_crawl = lambda: (True, "")
    with service.app.test_client() as client:
        with client.session_transaction() as session:
            session["dashboard_access"] = True
        response = client.get("/api/run-config")
        assert response.status_code == 200
        result = response.get_json()
        assert "福建" not in result["provinces"]
        assert result["income_options"] == [0, 3000, 5000, 8000, 12000]
        assert result["push_window_options"] == [24, 48]

        response = client.post("/start", json={
            "provinces": selected_provinces, "minimum_income": 12000, "push_window_hours": 24,
        })
        assert response.status_code == 200 and response.get_json()["ok"] is True
        assert service.get_run_filter_config() == {
            "provinces": selected_provinces, "minimum_income": 12000, "push_window_hours": 24,
        }

        assert service.income_meets_minimum("3001-5000元", 8000) is False
        assert service.income_meets_minimum("8001-12000元", 8000) is True
        assert service.income_meets_minimum("50000元以上", 8000) is True

os.environ.pop("ZHENAI_BASE_DIR", None)
os.environ.pop("DASHBOARD_ACCESS_TOKEN", None)
print("PASS: dashboard persists allowed province, income, and group push window selections")
