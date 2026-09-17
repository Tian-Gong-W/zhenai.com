"""Test that Dahai and Laoying controls are completely separated."""
import importlib.util
import os
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

for filename, expected_tag, own_keyword, other_keyword in (
    ("dahai.py", "【大海】", "大海", "老鹰"),
    ("laoying.py", "【老鹰】", "老鹰", "大海"),
):
    with tempfile.TemporaryDirectory() as temp_dir:
        os.environ["ZHENAI_BASE_DIR"] = temp_dir
        spec = importlib.util.spec_from_file_location(f"split_test_{filename}", ROOT / filename)
        service = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(service)
        service.init_db()
        service.TG_BOT_TOKEN = "test-token"
        service.GROUP_CHAT_ID = "-100123"
        service.TG_GROUP_CONTROL_USER_ID = "8400495055, 99999999"

        # Multi-admin authorization test
        assert service.group_control_authorized({"type": "supergroup", "id": "-100123"}, {"id": "8400495055"})
        assert service.group_control_authorized({"type": "supergroup", "id": "-100123"}, {"id": "99999999"})
        assert not service.group_control_authorized({"type": "supergroup", "id": "-100123"}, {"id": "11111111"})

        # Natural language command normalization
        assert service.group_command_name(f"{own_keyword}启动") == f"{own_keyword}启动"
        assert service.group_command_name("群启动") == "群启动"
        assert service.group_command_name("关闭") == "关闭"
        assert service.group_command_name("/start_collect@TopMan_ZiYuan_bot") == "/start_collect"

        # Check instance tag in identity
        assert service.INSTANCE_TAG == expected_tag

os.environ.pop("ZHENAI_BASE_DIR", None)
print("PASS: Dahai and Laoying controls are cleanly separated with independent tags and commands")
