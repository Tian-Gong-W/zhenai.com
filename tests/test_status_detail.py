"""Regression check for detailed dashboard/Telegram runtime status."""
import importlib.util
import os
import tempfile
import time
from pathlib import Path

source = Path(__file__).resolve().parents[1] / "app.py"
with tempfile.TemporaryDirectory() as temp_dir:
    os.environ["ZHENAI_BASE_DIR"] = temp_dir
    os.environ["DASHBOARD_ACCESS_TOKEN"] = "test-token"
    spec = importlib.util.spec_from_file_location("status_detail_test", source)
    service = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(service)
    service.init_db()
    service.save_run_filter_config({"provinces": ["广东"], "minimum_income": 0, "push_window_hours": 24})
    service.set_state("city_progress", "广州")
    service.set_state("runtime_current_page", "7")
    now = time.time()
    service.set_state("runtime_started_at", str(now - 120))
    service.set_state("runtime_last_progress_at", str(now - 5))
    service.set_state("last_log_at", str(now - 3))
    service.current_stats["total"] = 12
    service.crawl_running.set()
    status = service.build_runtime_status()
    assert status["health"] == "正常"
    assert status["city"] == "广州"
    assert status["page"] == 7
    assert status["new_records"] == 12
    text = service.format_runtime_status()
    for expected in ("当前城市：广州", "当前页：第 7 页", "本轮新增：12 条", "最近进度："):
        assert expected in text, (expected, text)

os.environ.pop("ZHENAI_BASE_DIR", None)
os.environ.pop("DASHBOARD_ACCESS_TOKEN", None)
print("PASS: detailed runtime status includes city, page, new records, and health")
