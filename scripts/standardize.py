#!/usr/bin/env python3
"""
Finalize BLM + USFS into a minimal, map-ready dataset

What this does (in plain English):
- Takes the BLM CSV and the USFS-enriched CSV and reduces them to a shared,
  compact schema that the web map can load without thinking.
- Normalizes dates to ISO, picks a reasonable project name, and ensures we have
  clean WGS84 coordinates (and converts from Web Mercator if needed).
- Writes both a CSV (for analysis) and a tiny GeoJSON (for the frontend map).

Why minimal?
- The goal here is to be lightweight and unambiguous for visualization:
  project_name, source, start_date, end_date, notes, longitude, latitude.
  You can always join back to richer tables upstream if you need more fields.

Outputs:
- CSV:     project_name, source, start_date, end_date, notes, longitude, latitude, geometry_wkt
- GeoJSON: FeatureCollection of Points with the same minimal properties

Usage:
  python scripts/finalize_opportunities.py \
    data/interim/blm_public_comment.csv \
    data/processed/usfs_public_comment_with_geom.csv \
    --csv data/standardized/final_opportunities.csv \
    --geojson data/standardized/final_opportunities.geojson
"""

from __future__ import annotations
import argparse
import json
import math
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd

# ---- CRS detection + conversion (EPSG:3857 -> EPSG:4326) ----
R_MERC = 6378137.0  # Web Mercator sphere radius used for conversion heuristics

def looks_like_3857(lon_str: str, lat_str: str) -> bool:
    """
    Quick-and-dirty check: if values are way outside lon/lat ranges, they’re probably 3857 meters.
    """
    try:
        x = float(lon_str)
        y = float(lat_str)
    except Exception:
        return False
    degish = (-180 <= x <= 180) and (-90 <= y <= 90)
    mercish = (abs(x) > 1000) or (abs(y) > 1000)
    return (not degish) and mercish

def merc3857_to_wgs84(x: float, y: float) -> Tuple[float, float]:
    """
    Convert Web Mercator meters (EPSG:3857) to lon/lat in degrees (EPSG:4326).
    This is good enough for points destined for a web map.
    """
    lon = (x / R_MERC) * (180.0 / math.pi)
    lat = (2.0 * math.atan(math.exp(y / R_MERC)) - math.pi / 2.0) * (180.0 / math.pi)
    # Clamp to valid ranges just in case.
    lat = max(min(lat, 90.0), -90.0)
    lon = max(min(lon, 180.0), -180.0)
    return lon, lat

def to_iso(d: str) -> str:
    """
    Normalize any date-like value to YYYY-MM-DD. Return empty string if unsure.
    """
    if not d or pd.isna(d):
        return ""
    try:
        return pd.to_datetime(d).date().isoformat()
    except Exception:
        return ""

def first_nonempty(row: pd.Series, candidates: list[str]) -> str:
    """
    Given a list of candidate columns, return the first one that has a non-blank string.
    """
    for c in candidates:
        if c in row and str(row[c]).strip():
            return str(row[c]).strip()
    return ""

def clean_text(s: str) -> str:
    """
    Tidy trailing punctuation/whitespace so popups look nice.
    """
    if not s:
        return ""
    s = str(s).strip()
    return s.rstrip(". \t\r\n")

def detect_source(df: pd.DataFrame) -> str:
    """
    Try to auto-detect whether a DataFrame is BLM or USFS flavored.

    We look at:
    - Column names (BLM often has project_id/lat/lon; USFS has unit/comment_* fields)
    - URL hints (eplanning.blm.gov for BLM)
    """
    cols = {c.lower() for c in df.columns}
    if {"project_id","state","latitude","longitude"} <= cols:
        u = "".join(df.get("url","").astype(str).head(5)).lower()
        if "eplanning.blm.gov" in u or "blm.gov" in u:
            return "BLM"
    if "unit" in cols or {"comment_start","comment_end"} & cols:
        return "USFS"
    sample = df.head(1).to_dict("records")
    if sample:
        u = (sample[0].get("url") or sample[0].get("source_url") or "").lower()
        if "eplanning.blm.gov" in u or "blm.gov" in u:
            return "BLM"
    return "USFS"

def map_rows_to_final(df: pd.DataFrame, source_hint: Optional[str]=None) -> List[dict]:
    """
    Convert an input table (BLM or USFS) to our minimal row schema.
    """
    source = source_hint or detect_source(df)
    rows: List[dict] = []

    for _, r in df.iterrows():
        if source == "BLM":
            # BLM: we often only have an ID and maybe description; treat ID as project_name.
            pid = first_nonempty(r, ["project_id", "ProjectID", "ID"])
            notes = first_nonempty(r, ["description", "Summary", "ProjectDescription", "ProjectSummary"])
            cs = to_iso(first_nonempty(r, ["comment_start", "start_date", "PublicCommentStartDate"]))
            ce = to_iso(first_nonempty(r, ["comment_end", "PublicCommentEndDate"]))

            # Coordinates can be degrees or 3857 meters depending on where they came from.
            lon_raw = first_nonempty(r, ["longitude", "Longitude", "X"])
            lat_raw = first_nonempty(r, ["latitude", "Latitude", "Y"])
            lon = lat = None
            if lon_raw and lat_raw:
                if looks_like_3857(lon_raw, lat_raw):
                    try:
                        lon, lat = merc3857_to_wgs84(float(lon_raw), float(lat_raw))
                    except Exception:
                        pass
                else:
                    try:
                        lon = float(lon_raw); lat = float(lat_raw)
                    except Exception:
                        pass

            if lon is not None and lat is not None:
                rows.append({
                    "project_name": pid,
                    "source": "BLM",
                    "start_date": cs,
                    "end_date": ce,
                    "notes": notes.strip(),
                    "longitude": lon,
                    "latitude": lat,
                })

        else:  # USFS
            # USFS: we usually have a human-readable name and sometimes a description.
            name = first_nonempty(r, ["name", "title"])
            notes = first_nonempty(r, ["location_desc", "notes", "description"])
            cs = to_iso(first_nonempty(r, ["comment_start", "start_date", "comment_start_date", "expected_comment_start"]))
            ce = to_iso(first_nonempty(r, ["comment_end", "comment_end_date", "expected_comment_end"]))

            lon_raw = first_nonempty(r, ["longitude"])
            lat_raw = first_nonempty(r, ["latitude"])
            lon = lat = None
            if lon_raw and lat_raw:
                try:
                    lon = float(lon_raw); lat = float(lat_raw)
                except Exception:
                    pass

            if lon is not None and lat is not None:
                rows.append({
                    "project_name": clean_text(name) or "Unnamed project",
                    "source": "USFS",
                    "start_date": cs,
                    "end_date": ce,
                    "notes": notes.strip(),
                    "longitude": lon,
                    "latitude": lat,
                })

    return rows

def to_geojson(min_rows: List[dict]) -> dict:
    """
    Convert our minimal rows into a small, standards-compliant GeoJSON FeatureCollection.
    """
    feats = []
    for r in min_rows:
        lon = float(r["longitude"])
        lat = float(r["latitude"])
        props = {
            "project_name": r.get("project_name", ""),
            "source":       r.get("source", ""),
            "start_date":   r.get("start_date", ""),
            "end_date":     r.get("end_date", ""),
            "notes":        r.get("notes", ""),
        }
        feats.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": props,
        })
    return {"type": "FeatureCollection", "features": feats}

def main():
    """
    CLI entrypoint:
    - Read one or more CSVs (BLM + USFS).
    - Map each to the minimal schema.
    - Write CSV + GeoJSON to the requested locations.
    """
    ap = argparse.ArgumentParser()
    ap.add_argument("inputs", nargs="+", help="Input CSV files (BLM + USFS enriched)")
    ap.add_argument("--csv", required=True, help="Output CSV path")
    ap.add_argument("--geojson", required=True, help="Output GeoJSON path")
    args = ap.parse_args()

    all_rows: List[dict] = []
    for p in args.inputs:
        df = pd.read_csv(p, dtype=str).fillna("")
        rows = map_rows_to_final(df)
        all_rows.extend(rows)

    # Final DataFrame for the CSV
    out = pd.DataFrame(all_rows, columns=[
        "project_name", "source", "start_date", "end_date", "notes", "longitude", "latitude"
    ])

    # Handy WKT column for quick spatial sanity checks in downstream tools.
    def mk_wkt(row):
        return f"POINT ({row['longitude']} {row['latitude']})"
    out["geometry_wkt"] = out.apply(mk_wkt, axis=1)

    # Ensure output directories exist
    Path(args.csv).parent.mkdir(parents=True, exist_ok=True)
    Path(args.geojson).parent.mkdir(parents=True, exist_ok=True)

    # Write CSV
    out.to_csv(args.csv, index=False)

    # Write GeoJSON
    gj = to_geojson(all_rows)
    Path(args.geojson).write_text(json.dumps(gj, indent=2), encoding="utf-8")

    print(f"[OK] Wrote CSV -> {args.csv}")
    print(f"[OK] Wrote GeoJSON -> {args.geojson}")

if __name__ == "__main__":
    main()
