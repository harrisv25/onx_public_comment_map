from pathlib import Path
import json
import pytest
from scripts.build_map_assets import main as build_main

def test_build_map_assets_passthrough(tmp_path: Path, monkeypatch, capsys):
    # Arrange: fake input FeatureCollection
    src = tmp_path / "in.geojson"
    dst = tmp_path / "out.json"
    gj = {
        "type": "FeatureCollection",
        "features": [
            {"type":"Feature","geometry":{"type":"Point","coordinates":[0,0]},"properties":{"title":"T"}}]
    }
    src.write_text(json.dumps(gj), encoding="utf-8")

    # Patch argv for the CLI
    monkeypatch.setenv("PYTHONIOENCODING", "utf-8")
    monkeypatch.setattr("sys.argv", ["build_map_assets.py", "--in", str(src), "--out", str(dst)])

    # Act
    build_main()

    # Assert
    out = json.loads(dst.read_text(encoding="utf-8"))
    assert out["type"] == "FeatureCollection"
    assert out["features"][0]["properties"]["title"] == "T"

def test_build_map_assets_rejects_non_featurecollection(tmp_path: Path, monkeypatch):
    src = tmp_path / "bad.json"
    dst = tmp_path / "out.json"
    src.write_text(json.dumps({"type":"NotAFeatureCollection"}), encoding="utf-8")
    monkeypatch.setattr("sys.argv", ["build_map_assets.py", "--in", str(src), "--out", str(dst)])

    with pytest.raises(SystemExit):
        build_main()
