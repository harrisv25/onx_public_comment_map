#!/usr/bin/env python3
"""
USFS SOPA scraper (Colorado-focused).

What it does
------------
- Option A (--auto-co): discovers ALL Colorado National Forest/Grassland SOPA
  listing pages from the official state index and scrapes each.
- Option B (--sopa Forest=URL ...): scrape one or more specific SOPA listing URLs.
- Parses each unit's main table and extracts:
    title, link, free-text, and up to two dates from the row text.
- Emits a CSV (one row per project) in the shared schema used by the pipeline.

Example
-------
python scripts/usfs_sopa_scrape.py --state CO \
  --auto-co \
  -o data/interim/usfs.csv

or, manual selection:

python scripts/usfs_sopa_scrape.py --state CO \
  --sopa "ArapahoRoosevelt=https://www.fs.usda.gov/sopa/forest-level.php?110210" \
         "GMUG=https://www.fs.usda.gov/sopa/forest-level.php?110206" \
  -o data/interim/usfs.csv
"""
from __future__ import annotations

import argparse
import csv
import re
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dparse

# ----------------------------- Constants -------------------------------------

# Official USFS SOPA state index for Colorado.
STATE_CO_URL = "https://www.fs.usda.gov/sopa/state-level.php?co="

# Month-name date tokens like "Aug 20, 2025"
DATE_ANY_RE = re.compile(
    r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},\s*\d{4}",
    re.IGNORECASE,
)


# ----------------------------- Data Types ------------------------------------

@dataclass
class RowResult:
    project_id: str
    agency: str
    title: str
    description: str
    office_or_unit: str
    comment_start_date: Optional[str]
    comment_end_date: Optional[str]
    source_url: str
    state: str
    geometry_type: str
    geometry: str
    geom_source: str
    scrape_confidence: float
    last_checked_utc: str


# ----------------------------- Helpers ---------------------------------------

def parse_dates_from_text(text: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract up to two dates from free text.
    - If one date: interpret as an 'end' date (common "Comments due <date>").
    - If two or more: earliest = start, latest = end.
    """
    full_matches = [m.group(0) for m in DATE_ANY_RE.finditer(text or "")]
    parsed = []
    for s in full_matches:
        try:
            parsed.append(dparse.parse(s, fuzzy=True))
        except Exception:
            # ignore unparseable tokens
            pass

    if not parsed:
        return None, None
    if len(parsed) == 1:
        return None, parsed[0].date().isoformat()

    parsed.sort()
    return parsed[0].date().isoformat(), parsed[-1].date().isoformat()


def discover_colorado_sopa_units() -> List[Tuple[str, str]]:
    """
    Parse the Colorado SOPA state page and return (label, url) tuples
    for every Forest/Grassland unit listed there.
    """
    r = requests.get(STATE_CO_URL, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")

    units: List[Tuple[str, str]] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "forest-level.php?" in href:
            url = urljoin(STATE_CO_URL, href)
            label = a.get_text(strip=True) or "USFS Unit (CO)"
            units.append((label, url))

    # de-dup (some pages can repeat links)
    seen = set()
    deduped: List[Tuple[str, str]] = []
    for label, url in units:
        key = (label, url)
        if key not in seen:
            deduped.append((label, url))
            seen.add(key)
    return deduped


def scrape_sopa_listing(url: str, state: str, office_label: str) -> List[RowResult]:
    """
    Scrape a single SOPA unit listing page (usually per Forest/Grassland).
    Returns a list of RowResult (one per project row found).
    """
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")

    # Heuristic: the first <table> usually contains the listing.
    table = soup.find("table")
    if not table:
        return []

    results: List[RowResult] = []
    for tr in table.select("tr")[1:]:  # skip header row
        text = tr.get_text(" ", strip=True)
        if not text:
            continue

        a = tr.find("a")
        title = a.get_text(strip=True) if a else "USFS Project"
        href = a.get("href") if a else url

        start, end = parse_dates_from_text(text)

        # Build a pseudo-stable project_id:
        # Prefer a suffix from the link target when present; else hash title.
        if a and a.get("href"):
            tail = a.get("href").rstrip("/").split("/")[-1]
            project_id = f"USFS-{tail[-12:]}"
        else:
            project_id = f"USFS-{abs(hash(title)) % (10**10)}"

        confidence = 0.6 if (start or end) else 0.4

        results.append(RowResult(
            project_id=project_id,
            agency="USFS",
            title=title,
            description=text[:1000],
            office_or_unit=office_label,   
            comment_start_date=start,
            comment_end_date=end,
            source_url=href,
            state=state,
            geometry_type="",
            geometry="",
            geom_source="",
            scrape_confidence=confidence,
            last_checked_utc=datetime.now(timezone.utc).isoformat(),
        ))

    return results


# ----------------------------- CLI / Main ------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--state", default="CO", help="Two-letter state code (default: CO)")
    ap.add_argument("-o", "--out", required=True, help="Output CSV path")
    ap.add_argument(
        "--sopa",
        nargs="+",
        help="Manual SOPA sources as pairs of ForestLabel=URL (e.g., 'WhiteRiver=https://...'). "
             "Can be combined with --auto-co.",
    )
    ap.add_argument(
        "--auto-co",
        action="store_true",
        help="Discover all Colorado SOPA Forest/Grassland unit pages automatically.",
    )
    args = ap.parse_args()

    pairs: List[Tuple[str, str]] = []

    if args.auto_co:
        pairs.extend(discover_colorado_sopa_units())

    if args.sopa:
        for pair in args.sopa:
            if "=" not in pair:
                raise SystemExit(f"--sopa must be ForestLabel=URL, got: {pair}")
            label, url = pair.split("=", 1)
            pairs.append((label, url))

    if not pairs:
        raise SystemExit("No SOPA sources provided. Use --auto-co or --sopa Forest=URL ...")

    all_rows: List[RowResult] = []
    for label, url in pairs:
        try:
            all_rows.extend(scrape_sopa_listing(url, args.state, office_label=label))
        except Exception as exc:
            # be resilient: capture which unit failed, continue others
            print(f"[warn] Failed to scrape {label} ({url}): {exc}")

    fieldnames = list(RowResult.__annotations__.keys())
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in all_rows:
            w.writerow(asdict(r))


if __name__ == "__main__":
    main()
