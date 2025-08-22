#!/usr/bin/env python3
"""
Build web map assets.

Reads a standardized GeoJSON and writes a web-ready JSON (currently pass-through)
to `webmap/data.json`. This keeps the web bundle stable
even if the upstream schema changes a bit over time.
"""
from __future__ import annotations
import argparse
import json
from pathlib import Path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", required=True, help="Input GeoJSON path")
    ap.add_argument("--out", dest="out_path", required=True, help="Output JSON path (e.g., webmap/data.json)")
    args = ap.parse_args()

    in_path = Path(args.in_path)
    out_path = Path(args.out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Pass-through for now (kept as a hook for future transforms)
    data = json.loads(in_path.read_text(encoding="utf-8"))

    # Minimal validation
    if not isinstance(data, dict) or data.get("type") != "FeatureCollection":
        raise SystemExit("Input must be a GeoJSON FeatureCollection")

    out_path.write_text(json.dumps(data), encoding="utf-8")
    print(f"[ok] wrote {out_path}")


if __name__ == "__main__":
    main()
