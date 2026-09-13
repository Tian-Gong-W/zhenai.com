import importlib.util
import os
import tempfile
import time
from datetime import datetime
from pathlib import Path

source = Path(__file__).resolve().parents[1] / 'app.py'
with tempfile.TemporaryDirectory() as temp_dir:
    os.environ['ZHENAI_BASE_DIR'] = temp_dir
    os.environ['DASHBOARD_ACCESS_TOKEN'] = 'test-token'
    spec = importlib.util.spec_from_file_location('beijing_timestamp_test', source)
    service = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(service)
    service.init_db()
    timestamp = int(datetime(2026, 9, 13, 12, 0, tzinfo=service.BEIJING_TZ).timestamp() * 1000)
    latest, cert, earliest, purple = service.ZhenaiCrawler.parse_moments_times(None, [
        {'text': '已完成人脸认证', 'moment': {'type': 6, 'createTime': timestamp}},
    ], check_purple=False)
    assert cert == '2026-09-13 12:00:00', cert

os.environ.pop('ZHENAI_BASE_DIR', None)
os.environ.pop('DASHBOARD_ACCESS_TOKEN', None)
print('PASS: certification timestamps are formatted in Asia/Shanghai')
