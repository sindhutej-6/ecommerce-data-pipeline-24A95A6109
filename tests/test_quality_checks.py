import json
from pathlib import Path

REPORT = Path("data/processed/monitoring_report.json")


def test_quality_report_generated():
    assert REPORT.exists(), "Monitoring report not generated"


def test_quality_score_present():
    report = json.load(open(REPORT))
    assert "data_quality" in report["checks"]
    assert "quality_score" in report["checks"]["data_quality"]


def test_null_detection():
    report = json.load(open(REPORT))
    assert "null_violations" in report["checks"]["data_quality"]


def test_referential_integrity_check():
    report = json.load(open(REPORT))
    assert report["checks"]["data_quality"]["orphan_records"] == 0
