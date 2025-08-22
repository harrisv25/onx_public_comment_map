import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

from scripts.standardize import compute_status, load_and_normalize, to_geojson_points


def test_compute_status_rules():
    today = date(2025, 8, 21)
    # active: today inside window
    assert compute_status("2025-08-01", "2025-08-31", today) == "active"
    # upcoming: start within 30 days in future
    assert compute_status("2025-09-05", None, today) == "upcoming"
    # closed: end within last 30 days
    assert compute_status(None, "2025-08-10", today) == "closed"
    # unknown: no dates
    assert compute_status(None, None, today) == "unknown"


def test_load_and_normalize_merges(tmp_path: Path):
    blm = tmp_path / "blm.csv"
    usfs = tmp_path / "usfs.csv"

    blm.write_text(
        "project_id,agency,title,description,office_or_unit,comment_start_date,comment_end_date,source_url,state,geometry_type,geom_source,scrape_confidence,last_checked_utc\n"
        "BLM-111,BLM,BLM Title,Desc A,FO A,2025-08-10,2025-09-05,https://blm/111,CO,,admin-centroid,0.8,2025-08-20T00:00:00Z\n",
        encoding="utf-8",
    )
    usfs.write_text(
        "project_id,agency,title,description,office_or_unit,comment_start_date,comment_end_date,source_url,state,geometry_type,geom_source,scrape_confidence,last_checked_utc\n"
        "USFS-222,USFS,USFS Title,Desc B,Forest B,,2025-09-25,https://usfs/222,CO,,,0.6,2025-08-20T00:00:00Z\n",
        encoding="utf-8",
    )

    df = load_and_normalize([str(blm), str(usfs)])
    assert set(df["project_id"]) == {"BLM-111", "USFS-222"}
    # computed statuses exist
    assert "comment_status" in df.columns
    # BLM row active (depending on today's date this may vary; just assert non-empty)
    assert df.loc[df["project_id"] == "BLM-111", "comment_status"].iloc[0] in {"active","upcoming","closed","unknown"}


def test_to_geojson_points_structure(tmp_path: Path):
    # build a tiny df
    df = pd.DataFrame([{
        "project_id":"BLM-111","agency":"BLM","title":"T",
        "description":"D","office_or_unit":"O","comment_status":"active",
        "comment_start_date":"2025-08-10","comment_end_date":"2025-09-05",
        "source_url":"https://x","state":"CO","geometry_type":"","geom_source":"",
        "scrape_confidence":"0.8","last_checked_utc":"2025-08-21T00:00:00Z"
    }])
    gj = to_geojson_points(df)
    assert gj["type"] == "FeatureCollection"
    assert len(gj["features"]) == 1
    feat = gj["features"][0]
    assert feat["geometry"]["type"] == "Point"
    assert feat["geometry"]["coordinates"] == [0, 0]
    assert feat["properties"]["project_id"] == "BLM-111"
