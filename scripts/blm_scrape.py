"""
BLM ePlanning public-comment finder (Colorado-only)

What this does (in plain English):
- Opens the BLM ePlanning search UI with a Colorado filter applied.
- Scrolls the results to trigger lazy-loading and harvests project IDs from links.
- For each project, visits a handful of tabs (510, 570, 565, 5101), grabs the page text,
  and looks for anything that reads like "public comment".
- If we see public comment language, we try to pull out a date and a state,
  and we optionally ask the BLM ArcGIS service for a lat/lon.
- Finally, we write a light CSV with the bits we care about so the rest of the pipeline
  can pick it up.

Notes & guardrails:
- This is meant to be quick and resilient, not perfect parsing.
- We treat any detected date as both start/end for now (conservative default).
- If ArcGIS gives us a coordinate, we trust it; otherwise we leave lat/lon blank.
"""

import re
import csv
import json
import requests
from datetime import datetime
from playwright.sync_api import sync_playwright


def discover_ids():
    """
    Walk the ePlanning UI search page (Colorado filter) and collect project IDs.

    Approach:
    - Load the search page with a prebuilt JSON filter in the query string.
    - Scroll a few times to trigger lazy-loading of more results.
    - Scrape all <a> anchors and regex out /project/<ID> patterns.

    Returns:
        list[str]: Sorted list of project IDs (strings like "123456").
    """
    url = "https://eplanning.blm.gov/eplanning-ui/search?filterSearch=" + \
          '{"states":["CO"],"offices":null,"projectTypes":null,"programs":null,"years":null,"open":false,"active":true}'

    ids = set()

    # Playwright does the heavy lifting here because the page is JS-driven.
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(url)

        # Gentle scrolling: we nudge the page a few times and give it a beat to load more rows.
        for _ in range(10):
            page.mouse.wheel(0, 2000)
            page.wait_for_timeout(800)

        # Grab every anchor href on the page and look for /eplanning-ui/project/<digits>
        hrefs = page.eval_on_selector_all("a", "els => els.map(e => e.href)")
        for h in hrefs:
            m = re.search(r"/eplanning-ui/project/(\d{6,})", h)
            if m:
                ids.add(m.group(1))

        browser.close()

    return sorted(ids)


def extract_date(text):
    """
    Find the first date in the text and normalize it to ISO (YYYY-MM-DD).

    We accept two formats:
    - Long form: "Month DD, YYYY"
    - Short form: "MM/DD/YYYY"

    If we can’t parse anything, we just return None.

    Args:
        text (str): Any blob of text from the page.

    Returns:
        str | None: ISO date string if found, else None.
    """
    patterns = [
        r"\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}\b",
        r"\b\d{1,2}/\d{1,2}/\d{4}\b"
    ]
    for pat in patterns:
        match = re.search(pat, text)
        if match:
            raw = match.group(0)
            # Try Month DD, YYYY
            try:
                return datetime.strptime(raw, "%B %d, %Y").date().isoformat()
            except ValueError:
                # Fall back to MM/DD/YYYY
                try:
                    return datetime.strptime(raw, "%m/%d/%Y").date().isoformat()
                except ValueError:
                    continue
    return None


def extract_state(text):
    """
    Try to pull a state name if the page includes a "State:" label.
    Default to "Colorado" when in doubt (since we filter to CO in discover_ids).

    Args:
        text (str): Page text.

    Returns:
        str: A state string, usually "Colorado".
    """
    m = re.search(r"\b(State|STATE):?\s+(.*?)\b", text)
    if m:
        return m.group(2).strip()
    return "Colorado"


def query_arcgis_for_lat_lon(pid):
    """
    Ask the BLM ArcGIS service if it knows the lat/lon for this project.

    Endpoint:
        https://eplanning.blm.gov/arcgisfed/rest/services/Proj_Loc_FO/BLM_ePlan_Proj_Loc/MapServer/4/query

    Strategy:
    - Query where projectID=<pid>, pull the first feature's attributes.
    - If X_match/Y_match are present, treat them as lon/lat.

    Args:
        pid (str): Project ID.

    Returns:
        tuple[float|None, float|None]: (lat, lon) if available, else (None, None).
    """
    url = "https://eplanning.blm.gov/arcgisfed/rest/services/Proj_Loc_FO/BLM_ePlan_Proj_Loc/MapServer/4/query"
    params = {
        "f": "json",
        "outFields": "*",
        "returnGeometry": "false",
        "spatialRel": "esriSpatialRelIntersects",
        "where": f"projectID={pid}"
    }
    try:
        r = requests.get(url, params=params)
        r.raise_for_status()
        data = r.json()
        features = data.get("features", [])
        if features:
            attr = features[0].get("attributes", {})
            lat = attr.get("Y_match")
            lon = attr.get("X_match")
            print(f"[ARCGIS] Project {pid} lat/lon: {lat}, {lon}")
            return lat, lon
    except Exception as e:
        # Not fatal — we can still write a row without coordinates.
        print(f"[ERROR] Failed to query ArcGIS for {pid}: {e}")
    return None, None


def scrape_projects(ids):
    """
    Given a bunch of project IDs, visit a few useful tabs and look for public comment hints.

    Tabs we visit:
    - 510 (often the overview)
    - 570, 565, 5101 (these tend to hold notices or supporting info)

    Behavior:
    - We concatenate the text from all visited tabs and scan it once.
    - If the text contains "public comment", we try to extract a date and state.
    - If ArcGIS has coordinates, we use them.

    Args:
        ids (list[str]): Project IDs from discover_ids().

    Returns:
        list[dict]: Lightweight records ready to be written to CSV.
    """
    records = []
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()

        for pid in ids:
            print(f"[INFO] Scraping project {pid}")
            full_text = ""

            # Walk through a few known tabs that often host relevant info.
            for tab in ("510", "570", "565", "5101"):
                url = f"https://eplanning.blm.gov/eplanning-ui/project/{pid}/{tab}"
                page.goto(url)
                page.wait_for_timeout(800)  # small pause to allow content to render

                try:
                    full_text += page.inner_text("body") + "\n"
                except:
                    # Some tabs may not load or may block text extraction; we skip quietly.
                    continue

            # Pull a date (if any) and make a best-guess at the state
            start_date = extract_date(full_text)
            state = extract_state(full_text)
            lat, lon = None, None

            # If there's no hint of public comment, we bail early for this project.
            if "public comment" in full_text.lower():
                # Optional: override coords with ArcGIS location if available.
                arcgis_lat, arcgis_lon = query_arcgis_for_lat_lon(pid)
                if arcgis_lat and arcgis_lon:
                    lat, lon = arcgis_lat, arcgis_lon

                # We keep the schema compact; downstream steps can enrich further.
                record = {
                    "project_id": pid,
                    "state": state,
                    "latitude": lat,
                    "longitude": lon,
                    "start_date": start_date,
                    "comment_start": start_date,  # conservative: same as start_date if only one date is known
                    "comment_end": start_date,    # conservative: same as start_date if only one date is known
                    "confidence": 0.8,            # soft signal — we saw “public comment” language
                    "url": f"https://eplanning.blm.gov/eplanning-ui/project/{pid}/510"
                }
                print("Project with comment:", record)
                records.append(record)

        browser.close()

    return records


def save_to_csv(records, path="data/interim/blm_public_comment.csv"):
    """
    Write our minimalist records to a CSV.

    Columns:
        project_id, state, latitude, longitude,
        start_date, comment_start, comment_end,
        confidence, url

    Args:
        records (list[dict]): Output of scrape_projects().
        path (str): Where to write the CSV (directories should exist).
    """
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "project_id", "state", "latitude", "longitude",
            "start_date", "comment_start", "comment_end",
            "confidence", "url"
        ])
        writer.writeheader()
        writer.writerows(records)
    print(f"[INFO] Saved {len(records)} records to {path}")


if __name__ == "__main__":
    # 1) Find Colorado project IDs from the search UI
    ids = discover_ids()
    print("Found IDs:", ids)

    # 2) Visit each project and look for public comment indicators
    records = scrape_projects(ids)

    # 3) Dump a simple CSV for the rest of the pipeline to consume
    save_to_csv(records)
