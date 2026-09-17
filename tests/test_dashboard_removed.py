"""The crawler services are Telegram-only and must not expose an HTTP dashboard."""
from pathlib import Path

root = Path(__file__).resolve().parents[1]
for filename in ("dahai.py", "laoying.py"):
    source = (root / filename).read_text(encoding="utf-8")
    compile(source, filename, "exec")
    for forbidden in (
        "from flask import", "Flask(", "@app.route", "app.run(",
        "http://localhost:7860", "DASHBOARD_ACCESS_TOKEN",
    ):
        assert forbidden not in source, (filename, forbidden)
    for required in ("def telegram_control_reporter", "def group_push_reporter", "def _launch_crawl"):
        assert required in source, (filename, required)

print("PASS: Dahai and Laoying contain Telegram crawler control without a web dashboard")
