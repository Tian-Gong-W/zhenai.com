"""Group-only Telegram controls replace the removed web dashboard and private control path."""
import importlib.util
import os
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

for filename in ("dahai.py", "laoying.py"):
    with tempfile.TemporaryDirectory() as temp_dir:
        os.environ["ZHENAI_BASE_DIR"] = temp_dir
        spec = importlib.util.spec_from_file_location(f"group_menu_{filename}", ROOT / filename)
        service = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(service)
        service.init_db()
        service.TG_BOT_TOKEN = "test-token"
        service.GROUP_CHAT_ID = "-100123"
        if filename != "dahai.py":
            service.TG_GROUP_CONTROL_USER_ID = "8400495055"
        assert service.group_control_authorized(
            {"type": "supergroup", "id": "-100123"}, {"id": "8400495055"}
        )
        assert not service.group_control_authorized(
            {"type": "private", "id": "8400495055"}, {"id": "8400495055"}
        )
        if filename == "dahai.py":
            assert service.group_control_authorized(
                {"type": "supergroup", "id": "-100123"}, {"id": "other"}
            )
        else:
            assert not service.group_control_authorized(
                {"type": "supergroup", "id": "-100123"}, {"id": "other"}
            )
        assert service.group_command_name("/start_collect@TopMan_ZiYuan_bot") == "/start_collect"
        assert service.group_command_name("/start_collect@Other_bot") == ""
        assert service.parse_control_command("启动") == ("start", None)
        assert service.parse_control_command("停止") == ("stop", None)
        assert service.parse_control_command("状态") == ("status", None)
        assert service.parse_control_command("统计") == ("stats", None)
        assert service.parse_control_command("帮助") == ("help", None)

        calls = []
        original_post = service.requests.post
        service.requests.post = lambda url, **kwargs: calls.append((url, kwargs)) or type("R", (), {"ok": True})()
        service.install_group_command_menu()
        service.requests.post = original_post
        assert calls and calls[0][0].endswith("/setMyCommands")
        payload = calls[0][1]["json"]
        assert payload["scope"] == {"type": "chat", "chat_id": "-100123"}
        assert {item["command"] for item in payload["commands"]} == {
            "start_collect", "stop_collect", "status", "stats", "help"
        }
        help_text = service.group_control_help()
        assert "发送「启动」" in help_text
        assert "发送「停止」" in help_text
        assert "发送「状态」" in help_text
        assert "发送「统计」" in help_text
        assert "发送「帮助」" in help_text

        starts = []
        original_thread = service.threading.Thread
        original_worker = service.crawl_worker
        original_reporter = service.send_start_report
        class FakeThread:
            def __init__(self, target, args=(), daemon=False, name=None):
                self.target = target
            def start(self):
                starts.append(self.target)
        service.threading.Thread = FakeThread
        service.crawl_worker = lambda *args: None
        service.send_start_report = lambda *args: None
        service._launch_crawl(auto_started=True)
        assert service.send_start_report not in starts
        service.crawl_running.clear()
        starts.clear()
        service._launch_crawl(auto_started=False)
        assert service.send_start_report in starts
        service.threading.Thread = original_thread
        service.crawl_worker = original_worker
        service.send_start_report = original_reporter

os.environ.pop("ZHENAI_BASE_DIR", None)
print("PASS: group menu controls are authorized, scoped, and manual-start reports remain one-time")
