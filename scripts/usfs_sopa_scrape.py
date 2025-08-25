"""
USFS SOPA public-comment finder (Colorado-only)

What this does (in plain English):
- Walks each Colorado forest’s SOPA report (HTML) and looks for rows that reference
  a "Comment Period Public Notice".
- When we find relevant rows, we extract dates (start/end or best guess) and a name/ID if present.
- We also download the monthly SOPA PDF for the same forest and do a quick scan for
  "public comment" language as a safety net.
- Output is a lightweight CSV the rest of the pipeline can consume.

Notes & guardrails:
- SOPA PDFs list *all* projects for the forest’s administrative unit; delineations
  between projects aren’t always clean in text, so we take a conservative approach.
- Date parsing accepts both long-form (“Month DD, YYYY”) and month-year (“MM/YYYY”)
  and we pick reasonable defaults when we only have a single date.
- We default state to “Colorado” because this script is scoped to CO forests.
- If we can’t resolve a precise project name/ID from a row, we still log
  something useful (with confidence and a snippet) rather than dropping it.

Known rough edges (tracked in README):
- Multiple project delineations inside a single SOPA PDF paragraph can blur together.
- HTML rows occasionally omit a direct “project=” link; we fall back to “unknown”.
"""

import os
import re
import csv
import time
import requests
import tempfile
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from pathlib import Path
from PyPDF2 import PdfReader
import argparse

# Colorado forests we care about (name, forest_id).
# Forest IDs map directly to the SOPA report URLs below.
FORESTS_CO = [
    ("Arapaho and Roosevelt NFs & Pawnee NG", "110202"),
    ("Grand Mesa, Uncompahgre and Gunnison NFs", "110204"),
    ("Medicine Bow-Routt NFs", "110206"),
    ("Pike and San Isabel NFs & Comanche and Cimarron NGs", "110208"),
    ("Rio Grande NF", "110209"),
    ("San Juan NF", "110210"),
    ("White River NF", "110212"),
    ("Cimarron National Grassland", "110701"),
    ("Manti-La Sal NF", "110504"),
    ("Comanche National Grassland", "110802"),
    ("Pawnee National Grassland", "110902"),
]

# These are “current month” style links. Pinning to a specific cycle keeps the run reproducible.
#  Todo: Make the month dynamic
SOPA_HTML = "https://www.fs.usda.gov/sopa/components/reports/sopa-{forest_id}-2025-07.html"
SOPA_PDF  = "https://www.fs.usda.gov/sopa/components/reports/sopa-{forest_id}-2025-07.pdf"


def extract_date_range(text):
    """
    Scrape plausible dates out of a blob of SOPA text and return a best-effort window.

    How we think about dates:
    - Long dates like "July 15, 2025" are ideal.
    - We also accept "07/2025" as a coarse month-level signal (we treat it as the 1st).
    - If we only see one date:
        * If it’s in the past: interpret as comment_end.
        * If it’s in the future: interpret as comment_start.
    - Some rows say "Comment Period Public Notice" without explicit open/close dates.
      In those cases we take the first long-form date as an expected start and assume
      a 30-day window for expected_end.

    Returns:
        tuple[str|None, str|None, str|None, str|None, str|None]:
        (start_date, comment_start, comment_end, expected_comment_start, expected_comment_end)
        All values are ISO ("YYYY-MM-DD") or None if unknown.
    """
    date_regex  = r"\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}"
    short_regex = r"\b(?:0[1-9]|1[0-2])/\d{4}\b"

    long_dates = re.findall(date_regex, text)
    short_dates = re.findall(short_regex, text)

    parsed_dates = []
    today = datetime.today().date()

    # Parse long-form dates first.
    for d in long_dates:
        try:
            parsed = datetime.strptime(d, "%B %d, %Y").date()
            parsed_dates.append(parsed)
        except Exception:
            continue

    # Parse month/year dates (we anchor to day=1).
    for d in short_dates:
        try:
            parsed = datetime.strptime(d, "%m/%Y").date().replace(day=1)
            parsed_dates.append(parsed)
        except Exception:
            continue

    # Deduplicate + sort to make reasoning simpler.
    parsed_dates = sorted(set(parsed_dates))

    comment_start = comment_end = expected_start = expected_end = None

    # If the row explicitly looks like a public notice, assume a 30-day expectation window.
    if "Comment Period Public Notice" in text:
        notice_match = re.search(date_regex, text)
        if notice_match:
            try:
                expected_start = datetime.strptime(notice_match.group(), "%B %d, %Y").date()
                expected_end = expected_start + timedelta(days=30)
            except Exception:
                pass

    # If we got actual parsed dates, pick the first two as start/end where possible.
    if parsed_dates:
        if len(parsed_dates) == 1:
            if parsed_dates[0] < today:
                comment_end = parsed_dates[0]
            else:
                comment_start = parsed_dates[0]
        elif len(parsed_dates) >= 2:
            comment_start = parsed_dates[0]
            comment_end = parsed_dates[1]

    # A single "start_date" that callers can lean on (prefers real start, else expected, else end).
    start_date = comment_start or expected_start or comment_end or expected_end

    return (
        start_date.isoformat() if start_date else None,
        comment_start.isoformat() if comment_start else None,
        comment_end.isoformat() if comment_end else None,
        expected_start.isoformat() if expected_start else None,
        expected_end.isoformat() if expected_end else None
    )


def parse_html_report(forest_id, debug=False):
    """
    Download and parse the SOPA HTML report for a given forest.

    Strategy:
    - Fetch the HTML report for this forest_id.
    - Iterate table rows, sniff for "comment period public notice".
    - Pull a project name from the first cell and try to find a "project=<id>" link.

    Args:
        forest_id (str): The numeric forest ID in the SOPA URL.
        debug (bool): If True, echo raw row text to help with tuning.

    Returns:
        list[dict]: Lightweight project-like records with date fields + notes.
    """
    url = SOPA_HTML.format(forest_id=forest_id)
    try:
        r = requests.get(url)
        if "Schedule of Proposed Actions" not in r.text:
            print(f"[WARN] No HTML SOPA report found for {forest_id}")
            return []
    except Exception as e:
        print(f"[ERROR] Request failed for {url}: {e}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    projects = []

    for row in soup.select("tr"):
        cells = row.find_all("td")
        if not cells or len(cells) < 2:
            continue

        # Flatten row text to make keyword tests easier.
        text = row.get_text(separator=" ", strip=True)
        if debug:
            print(f"[DEBUG] HTML row text: {text.lower()}")

        if "comment period public notice" in text.lower():
            start, c_start, c_end, expected_start, expected_end = extract_date_range(text)

            # Try to recover a project ID from any hyperlink in the row.
            href = row.find("a")
            project_id = None
            if href and "project=" in href.get("href", ""):
                m = re.search(r"project=(\d+)", href["href"])
                project_id = m.group(1) if m else "unknown"

            # First cell usually contains the project title.
            name = cells[0].get_text(strip=True) if cells else "unknown"

            projects.append({
                "project_id": project_id or "unknown",
                "name": name,
                "state": "Colorado",
                "latitude": None,
                "longitude": None,
                "start_date": start,
                "comment_start": c_start,
                "comment_end": c_end,
                "expected_comment_start": expected_start,
                "expected_comment_end": expected_end,
                "confidence": 0.7,   # HTML rows are usually cleaner than PDF blobs
                "notes": text,
                "url": url
            })

    return projects


def download_pdf(forest_id):
    """
    Grab the monthly SOPA PDF for a forest and stash it in a temp folder.

    Returns:
        Path | None: Path to the downloaded PDF if it looks valid, else None.
    """
    url = SOPA_PDF.format(forest_id=forest_id)
    tmpdir = Path(tempfile.gettempdir())
    pdf_path = tmpdir / f"sopa_{forest_id}.pdf"

    try:
        r = requests.get(url)
        # Quick sanity checks: status 200 and PDF magic bytes somewhere near the start.
        if r.status_code != 200 or b"%PDF" not in r.content[:1024]:
            print(f"[WARN] No PDF SOPA report found for {forest_id}")
            return None

        with open(pdf_path, "wb") as f:
            f.write(r.content)
        return pdf_path
    except Exception as e:
        print(f"[ERROR] Failed to download PDF for {forest_id}: {e}")
        return None


def parse_pdf_report(forest_id, pdf_path, debug=False):
    """
    Do a light pass over the SOPA PDF text and look for "public comment" mentions.

    Caveat:
    - The PDF aggregates many projects; text extraction sometimes smashes boundaries.
      We record what we can with a short snippet and conservative confidence.

    Returns:
        list[dict]: PDF-derived “maybe projects” with expected/actual date hints.
    """
    projects = []
    try:
        if not pdf_path.exists():
            raise FileNotFoundError(f"{pdf_path} does not exist")

        reader = PdfReader(str(pdf_path))
        text = "\n".join(page.extract_text() or "" for page in reader.pages)

        if "public comment" in text.lower():
            start, c_start, c_end, expected_start, expected_end = extract_date_range(text)

            # Keep a small snippet anchored around the first “public comment” we saw.
            # This helps with manual QA without dumping the whole PDF into the CSV.
            idx = text.lower().find("public comment")
            snippet = text[idx:][:500] if idx >= 0 else text[:500]

            projects.append({
                "project_id": "unknown",
                "name": "unknown",
                "state": "Colorado",
                "latitude": None,
                "longitude": None,
                "start_date": start,
                "comment_start": c_start,
                "comment_end": c_end,
                "expected_comment_start": expected_start,
                "expected_comment_end": expected_end,
                "confidence": 0.6,   # PDF text is noisier than HTML rows
                "notes": snippet,
                "url": str(pdf_path)
            })
    except Exception as e:
        print(f"[ERROR] PDF parse failed for {forest_id}: {e}")

    return projects


def run_scraper(debug_html=False):
    """
    Drive the whole SOPA collection flow:
    - For each forest: parse HTML rows, then try the PDF as a backstop.
    - Sleep briefly between forests to be polite.

    Args:
        debug_html (bool): Echo row text during HTML parse if True.

    Returns:
        list[dict]: All harvested records across forests.
    """
    all_records = []

    for name, forest_id in FORESTS_CO:
        print(f"[INFO] Scraping forest: {name}")

        # 1) HTML report (usually the cleanest signals)
        html_records = parse_html_report(forest_id, debug=debug_html)
        all_records.extend(html_records)

        # Small pause between requests so we’re good citizens.
        time.sleep(1)

        # 2) Monthly PDF (catch-all)
        pdf_path = download_pdf(forest_id)
        if pdf_path:
            pdf_records = parse_pdf_report(forest_id, pdf_path, debug=debug_html)
            all_records.extend(pdf_records)

    return all_records


def save_to_csv(records, path="data/interim/usfs_public_comment.csv"):
    """
    Write everything to a consistent CSV so downstream steps don’t have to guess.

    Columns:
        project_id, name, state, latitude, longitude,
        start_date, comment_start, comment_end,
        expected_comment_start, expected_comment_end,
        confidence, notes, url
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "project_id", "name", "state", "latitude", "longitude",
            "start_date", "comment_start", "comment_end",
            "expected_comment_start", "expected_comment_end",
            "confidence", "notes", "url"
        ])
        writer.writeheader()
        writer.writerows(records)
    print(f"[INFO] Saved {len(records)} records to {path}")


if __name__ == "__main__":
    # Optional debugging to print raw HTML row text (useful when tuning regex).
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug-html", action="store_true",
                        help="Print raw HTML row text and PDF snippets for debugging")
    args = parser.parse_args()

    # 1) Collect records across all CO forests.
    records = run_scraper(debug_html=args.debug_html)

    # 2) Save the lot to a predictable path for the rest of the pipeline.
    save_to_csv(records)
