import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location("service", Path(__file__).parents[1] / "app.py")
service = importlib.util.module_from_spec(spec)
spec.loader.exec_module(service)

assert service.income_meets_minimum("5001-8000元", 8000) is False
assert service.income_meets_minimum("8001-12000元", 8000) is True
assert service.income_meets_minimum("12001-20000元", 12000) is True
assert service.income_meets_minimum("3000元以下", 3000) is False
assert service.income_meets_minimum("5001-8000元", 0) is True
print("PASS: salary-band lower-bound filtering")
