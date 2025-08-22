#!/usr/bin/env python3
"""
Standardize BLM + USFS scraped CSVs into one dataset and export GeoJSON/CSV.

- Merges multiple input CSVs.
- Harmonizes columns into a shared schema.
- Computes `comment_status` based on start/end dates and `today`.
- Emits GeoJSON FeatureCollection (Points for now; geometry handled later).

Usage:
  python scripts/standardize.py data/interim/*.csv \
    -o data/standardized/opportunities.geojson \
    --csv data/standardized/opportunities.csv
"""
from __future__ import annotations
import argparse
import json
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd


SCHEMA_ORDER = [
    "project_id",
    "agency",
    "title",
    "description",
    "office_or_unit",
    "comment_status",
    "comment_start_date",
    "comment_end_date",
    "source_url",
    "state",
    "geometry_type",
    "geom_source",
    "scrape_confidence",
    "last_checked_utc",
]

# some scrapers emit `geometry` too; keep it but we won't parse complex objects here
OPTIONAL_COLS = {"geometry"}

REQUIRED_BASE = set(SCHEMA_ORDER) - {"comment_status"}  

def compute_status(start: Optional[str], end: Optional[str], today: Optional[date] = None) -> str:
    """
    Status rules:
    - active   : today ∈ [start, end]
    - upcoming : today < start ≤ today+30d
    - closed   : today-30d < end < today
    - else     : unknown
    """
    if today is None:
        today = date.today()

    try:
        s = pd.to_datetime(start).date() if start else None
    except Exception:
        s = None
    try:
        e = pd.to_datetime(end).date() if end else None
    except Exception:
        e = None

    try:
        if s and e and s <= today <= e:
            return "active"
        if s and today < s <= (today + timedelta(days=30)):
            return "upcoming"
        if e and (today - timedelta(days=30)) < e < today:
            return "closed"
    except Exception:
        pass
    return "unknown"


def load_and_normalize(paths: Iterable[str]) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    for p in paths:
        df = pd.read_csv(p, dtype=str).fillna("")
        # ensure all expected columns exist (create blanks if missing)
        for col in REQUIRED_BASE | OPTIONAL_COLS:
            if col not in df.columns:
                df[col] = ""
        # subset + order (comment_status filled later)
        df = df[[c for c in (SCHEMA_ORDER + list(OPTIONAL_COLS)) if c in df.columns]]
        frames.append(df)

    if not frames:
        return pd.DataFrame(columns=SCHEMA_ORDER + list(OPTIONAL_COLS))

    merged = pd.concat(frames, ignore_index=True)
    # compute status
    merged["comment_status"] = merged.apply(
        lambda r: compute_status(r.get("comment_start_date"), r.get("comment_end_date")), axis=1
    )
    # enforce output order
    for col in SCHEMA_ORDER:
        if col not in merged.columns:
            merged[col] = ""
    return merged[SCHEMA_ORDER + [c for c in OPTIONAL_COLS if c in merged.columns]]


def to_geojson_points(df: pd.DataFrame) -> dict:
    """
    Build a minimal GeoJSON FeatureCollection with Point(0,0) placeholders.
    (We will replace geometry later with admin centroids or real extents.)
    """
    features = []
    for _, r in df.iterrows():
        props = {k: ("" if pd.isna(r.get(k)) else r.get(k)) for k in SCHEMA_ORDER}
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [0, 0]},
            "properties": props,
        })
    return {"type": "FeatureCollection", "features": features}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("inputs", nargs="+", help="Input CSV files (e.g., data/interim/*.csv)")
    ap.add_argument("-o", "--out", required=True, help="Output GeoJSON path")
    ap.add_argument("--csv", default=None, help="Optional: also write a merged CSV")
    args = ap.parse_args()

    df = load_and_normalize(args.inputs)
    gj = to_geojson_points(df)

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(gj, indent=2), encoding="utf-8")

    if args.csv:
        Path(args.csv).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(args.csv, index=False)


if __name__ == "__main__":
    main()
