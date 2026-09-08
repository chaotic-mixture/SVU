import json

from scripts.demo import build


def test_synthetic_input_to_serializable_report(tmp_path):
    result = build(tmp_path / "fresh" / "reports")
    assert result["status"] == "SIMULATED"
    assert result["icati"][0] == 100
    assert json.loads((tmp_path / "fresh/reports/demo.json").read_text())["domain"]["relative_baseline"] == [100.] * len(result["icati"])
    assert "SIMULATED" in (tmp_path / "fresh/reports/demo.html").read_text(encoding="utf-8")
