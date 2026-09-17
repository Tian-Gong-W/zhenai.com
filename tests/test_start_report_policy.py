"""Startup reports are manual-only and are never auto-deleted."""
import importlib.util
import os
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

for filename in ("dahai.py", "laoying.py"):
    with tempfile.TemporaryDirectory() as temp_dir:
        os.environ["ZHENAI_BASE_DIR"] = temp_dir
        spec = importlib.util.spec_from_file_location(f"startup_policy_{filename}", ROOT / filename)
        service = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(service)
        service.init_db()
        service.save_run_filter_config({
            "provinces": ["广东"], "age_begin": 40, "age_end": 65,
            "minimum_income": 0, "push_window_hours": 48,
        })

        sent_urls = []
        original_post = service.requests.post
        service.TG_BOT_TOKEN = "test-token"
        service.TG_CHAT_ID = ""
        service.GROUP_CHAT_ID = "test-group"
        service.TG_PRIVATE_PUSH_ENABLED = False
        service.requests.post = lambda url, **kwargs: sent_urls.append(url)
        service.send_start_report(40, 65)
        service.requests.post = original_post
        assert len(sent_urls) == 1 and sent_urls[0].endswith("/sendMessage")
        assert all("deleteMessage" not in url for url in sent_urls)

        started_targets = []
        original_thread = service.threading.Thread
        original_worker = service.crawl_worker
        original_reporter = service.send_start_report

        class FakeThread:
            def __init__(self, target, args=(), daemon=False, name=None):
                self.target = target
            def start(self):
                started_targets.append(self.target)

        service.threading.Thread = FakeThread
        service.crawl_worker = lambda *args: None
        service.send_start_report = lambda *args: None
        service._launch_crawl(auto_started=True)
        assert service.send_start_report not in started_targets
        service.crawl_running.clear()
        started_targets.clear()
        service._launch_crawl(auto_started=False)
        assert service.send_start_report in started_targets
        service.threading.Thread = original_thread
        service.crawl_worker = original_worker
        service.send_start_report = original_reporter

os.environ.pop("ZHENAI_BASE_DIR", None)
print("PASS: startup reports are manual-only and never auto-deleted")
