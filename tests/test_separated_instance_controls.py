"""Test keyword-only start/stop controls and optional target group ID filtering."""
import importlib.util
import os
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

for filename, expected_tag in (
    ("dahai.py", "【大海】"),
    ("laoying.py", "【老鹰】"),
):
    with tempfile.TemporaryDirectory() as temp_dir:
        os.environ["ZHENAI_BASE_DIR"] = temp_dir
        spec = importlib.util.spec_from_file_location(f"keyword_ctrl_{filename}", ROOT / filename)
        service = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(service)
        service.init_db()
        service.TG_BOT_TOKEN = "test-token"
        service.GROUP_CHAT_ID = "-100123456789"
        service.TG_GROUP_CONTROL_USER_ID = "8400495055"

        # 1. Direct keyword parse tests (without any instance names)
        assert service.parse_control_command("启动") == ("start", None)
        assert service.parse_control_command("群启动") == ("start", None)
        assert service.parse_control_command("/start") == ("start", None)
        assert service.parse_control_command("/start_collect") == ("start", None)

        assert service.parse_control_command("停止") == ("stop", None)
        assert service.parse_control_command("关闭") == ("stop", None)
        assert service.parse_control_command("群关闭") == ("stop", None)
        assert service.parse_control_command("/stop") == ("stop", None)
        assert service.parse_control_command("/stop_collect") == ("stop", None)

        assert service.parse_control_command("状态") == ("status", None)
        assert service.parse_control_command("统计") == ("stats", None)

        # 2. Keyword + target group ID tests ("启动加群ID" / "停止加群ID")
        assert service.parse_control_command("启动 -100123456789") == ("start", "-100123456789")
        assert service.parse_control_command("启动-100123456789") == ("start", "-100123456789")
        assert service.parse_control_command("停止 -100123456789") == ("stop", "-100123456789")
        assert service.parse_control_command("关闭 -100123456789") == ("stop", "-100123456789")

        # 3. Chat ID matching logic
        assert service.chat_ids_match("-100123456789", service.GROUP_CHAT_ID)
        assert service.chat_ids_match("123456789", service.GROUP_CHAT_ID)
        assert not service.chat_ids_match("-100999999999", service.GROUP_CHAT_ID)

        # 4. Bot username mention filter
        assert service.parse_control_command("/start_collect@TopMan_ZiYuan_bot") == ("start", None)
        assert service.parse_control_command("/start_collect@Other_bot") == (None, None)

        # 5. Tag identity
        assert service.INSTANCE_TAG == expected_tag

os.environ.pop("ZHENAI_BASE_DIR", None)
print("PASS: keyword-only start/stop and group ID targeting work correctly")
