# scripts/enrich_with_district_geoms.py
import json
import re
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests
from shapely.ops import unary_union

# ------------ Config ------------
INPUT_CSV = "data/interim/usfs_public_comment.csv"
OUT_CSV   = "data/processed/usfs_public_comment_with_geom.csv"

# USFS EDW Ranger Districts - National extent (layer 0)
LAYER_BASE = "https://apps.fs.usda.gov/arcx/rest/services/EDW/EDW_RangerDistricts_01/MapServer/0"

# Optional aliases for tricky names (left = normalized unit name, right = actual EDW name)
ALIASES = {
    "bears ears ranger district": "hahns peak/bears ears ranger district",
    # add more here if needed, e.g. "leadville rd": "leadville ranger district"
}

# ------------ REST loader (robust) ------------
def _get_json(url, params=None, timeout=60):
    r = requests.get(url, params=params or {}, timeout=timeout)
    r.raise_for_status()
    return r.json()

def _layer_info():
    return _get_json(LAYER_BASE, {"f": "json"})

def _query_geojson(params):
    q = dict(params)
    q["f"] = "geojson"
    geojson = _get_json(f"{LAYER_BASE}/query", q)
    return gpd.read_file(json.dumps(geojson))

def _query_json(params):
    q = dict(params)
    q["f"] = "json"
    return _get_json(f"{LAYER_BASE}/query", q)

def load_ranger_districts():
    info = _layer_info()
    max_count = int(info.get("maxRecordCount", 1000))
    supports_pagination = bool(info.get("supportsPagination", False))
    chunks = []

    if supports_pagination:
        offset = 0
        while True:
            gdf = _query_geojson({
                "where": "1=1",
                "outFields": "*",
                "outSR": "4326",
                "returnGeometry": "true",
                "resultOffset": offset,
                "resultRecordCount": max_count,
            })
            if gdf.empty:
                break
            chunks.append(gdf)
            if len(gdf) < max_count:
                break
            offset += max_count
    else:
        oid_resp = _query_json({"where": "1=1", "returnIdsOnly": "true"})
        oids = oid_resp.get("objectIds") or []
        if oids:
            for i in range(0, len(oids), max_count):
                subset = ",".join(map(str, oids[i:i+max_count]))
                gdf = _query_geojson({
                    "objectIds": subset,
                    "outFields": "*",
                    "outSR": "4326",
                    "returnGeometry": "true",
                })
                if not gdf.empty:
                    chunks.append(gdf)
        else:
            gdf = _query_geojson({
                "where": "1=1",
                "outFields": "*",
                "outSR": "4326",
                "returnGeometry": "true",
            })
            if gdf.empty:
                raise RuntimeError("No features returned from USFS Ranger Districts layer.")
            chunks.append(gdf)

    gdf_all = pd.concat(chunks, ignore_index=True) if chunks else gpd.GeoDataFrame()
    if gdf_all.empty:
        raise RuntimeError("No features retrieved from USFS Ranger Districts layer.")

    name_field = next((f for f in ["DISTRICTNAME", "RDNAME", "NAME"] if f in gdf_all.columns), None)
    if not name_field:
        raise RuntimeError(f"District name field not found. Columns: {list(gdf_all.columns)}")

    gdf_all["unit_name"] = gdf_all[name_field].astype(str)
    gdf_all["unit_lc"] = gdf_all["unit_name"].str.strip().str.lower()
    gdf_all = gdf_all.set_crs(4326, allow_override=True)
    return gdf_all[["unit_name", "unit_lc", "geometry"]]

# ------------ Matching helpers ------------
def normalize_unit_text(unit: str | None) -> list[str]:
    """
    Normalize SOPA 'unit' strings to EDW district names (list to support multi-units).
    """
    if not unit or pd.isna(unit):
        return []
    parts = [p.strip() for p in unit.split(",") if p.strip()]
    cleaned = []
    for p in parts:
        seg = p.split("/")[-1]          # drop 'East Zone/' etc.
        seg = re.sub(r"\s+", " ", seg).strip()
        seg = seg.rstrip(" .;:")        # remove trailing punctuation
        seg = re.sub(r"\bRD\b\.?$", "Ranger District", seg, flags=re.IGNORECASE)
        seg = re.sub(r"\bRanger Districts\b", "Ranger District", seg, flags=re.IGNORECASE)
        if not re.search(r"ranger district$", seg, flags=re.IGNORECASE):
            if re.search(r"\bdistrict$", seg, flags=re.IGNORECASE):
                seg = re.sub(r"\bdistrict$", "Ranger District", seg, flags=re.IGNORECASE)
            else:
                seg = f"{seg} Ranger District"
        # alias fixups (compare in lowercase)
        seg_lc = seg.lower()
        if seg_lc in ALIASES:
            seg = ALIASES[seg_lc]
        cleaned.append(seg)
    return cleaned

def compute_centroids_csv(csv_path: str, districts_gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Reads SOPA CSV, matches 'unit' to district polygons (union if multiple),
    computes centroid lon/lat (x/y). Returns a plain DataFrame (no geometry).
    """
    df = pd.read_csv(csv_path)
    lut = districts_gdf[["unit_lc", "geometry", "unit_name"]].copy()

    centroids_x, centroids_y, matched_units = [], [], []

    for _, r in df.iterrows():
        units = normalize_unit_text(r.get("unit"))
        unit_geoms = []
        matched_list = []
        for u in units:
            key = u.strip().lower()
            m = lut[lut["unit_lc"] == key]
            if not m.empty:
                unit_geoms.extend(list(m.geometry))
                matched_list.extend(list(m["unit_name"]))
        if unit_geoms:
            geom = unary_union(unit_geoms)
            c = geom.centroid
            centroids_x.append(float(c.x))
            centroids_y.append(float(c.y))
            matched_units.append(";".join(matched_list) if matched_list else None)
        else:
            centroids_x.append(None)
            centroids_y.append(None)
            matched_units.append(None)

    out = df.copy()
    # Fill or create lon/lat columns from centroids
    out["longitude"] = out.get("longitude", pd.Series([None]*len(out))).where(out.get("longitude", pd.Series([None]*len(out))).notna(), pd.Series(centroids_x))
    out["latitude"]  = out.get("latitude",  pd.Series([None]*len(out))).where(out.get("latitude",  pd.Series([None]*len(out))).notna(), pd.Series(centroids_y))
    out["matched_units"] = matched_units
    return out

# ------------ Main ------------
def main():
    Path(OUT_CSV).parent.mkdir(parents=True, exist_ok=True)

    print("[INFO] Loading USFS Ranger Districts...")
    districts = load_ranger_districts()
    print(f"[INFO] District features: {len(districts)}")

    print("[INFO] Computing centroids from matched districts...")
    out_df = compute_centroids_csv(INPUT_CSV, districts)

    # Quick report
    matched = int(out_df["longitude"].notna().sum())
    print(f"[INFO] Centroids available for {matched} / {len(out_df)} rows")

    out_df.to_csv(OUT_CSV, index=False)
    print(f"[INFO] Wrote CSV -> {OUT_CSV}")
    print("[DONE]")

if __name__ == "__main__":
    main()
