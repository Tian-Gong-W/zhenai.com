"""Ensure each production instance has its own valid service entry file."""
from pathlib import Path

root = Path(__file__).resolve().parents[1]
for filename, instance_name in (("dahai.py", "dahai"), ("laoying.py", "laoying")):
    source = (root / filename).read_text(encoding="utf-8")
    compile(source, filename, "exec")
    assert f'INSTANCE_NAME = "{instance_name}"' in source
    assert "def upsert_member(record):" in source
    assert "def append_to_csv(" not in source
    assert "def ensure_csv_fieldnames(" not in source

assert not (root / "app.py").exists()
print("PASS: Dahai and Laoying are independent service entry sources")
