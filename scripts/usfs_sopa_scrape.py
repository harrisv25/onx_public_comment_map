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

SOPA_HTML = "https://www.fs.usda.gov/sopa/components/reports/sopa-{forest_id}-2025-07.html"
SOPA_PDF = "https://www.fs.usda.gov/sopa/components/reports/sopa-{forest_id}-2025-07.pdf"

def extract_date_range(text):
    date_regex = r"\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}"
    short_regex = r"\b(?:0[1-9]|1[0-2])/\d{4}\b"

    long_dates = re.findall(date_regex, text)
    short_dates = re.findall(short_regex, text)

    parsed_dates = []
    today = datetime.today().date()

    for d in long_dates:
        try:
            parsed = datetime.strptime(d, "%B %d, %Y").date()
            parsed_dates.append(parsed)
        except Exception:
            continue

    for d in short_dates:
        try:
            parsed = datetime.strptime(d, "%m/%Y").date().replace(day=1)
            parsed_dates.append(parsed)
        except Exception:
            continue

    parsed_dates = sorted(set(parsed_dates))

    comment_start = comment_end = expected_start = expected_end = None

    if "Comment Period Public Notice" in text:
        notice_match = re.search(date_regex, text)
        if notice_match:
            try:
                expected_start = datetime.strptime(notice_match.group(), "%B %d, %Y").date()
                expected_end = expected_start + timedelta(days=30)
            except:
                pass

    if parsed_dates:
        if len(parsed_dates) == 1:
            if parsed_dates[0] < today:
                comment_end = parsed_dates[0]
            else:
                comment_start = parsed_dates[0]
        elif len(parsed_dates) >= 2:
            comment_start = parsed_dates[0]
            comment_end = parsed_dates[1]

    start_date = comment_start or expected_start or comment_end or expected_end

    return (
        start_date.isoformat() if start_date else None,
        comment_start.isoformat() if comment_start else None,
        comment_end.isoformat() if comment_end else None,
        expected_start.isoformat() if expected_start else None,
        expected_end.isoformat() if expected_end else None
    )

def parse_html_report(forest_id, debug=False):
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

        text = row.get_text(separator=" ", strip=True)
        if debug:
            print(f"[DEBUG] HTML row text: {text.lower()}")

        if "comment period public notice" in text.lower():
            start, c_start, c_end, expected_start, expected_end = extract_date_range(text)
            href = row.find("a")
            project_id = None
            if href and "project=" in href.get("href", ""):
                m = re.search(r"project=(\d+)", href["href"])
                project_id = m.group(1) if m else "unknown"
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
                "confidence": 0.7,
                "notes": text,
                "url": url
            })
    return projects

def download_pdf(forest_id):
    url = SOPA_PDF.format(forest_id=forest_id)
    tmpdir = Path(tempfile.gettempdir())
    pdf_path = tmpdir / f"sopa_{forest_id}.pdf"
    try:
        r = requests.get(url)
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
    projects = []
    try:
        if not pdf_path.exists():
            raise FileNotFoundError(f"{pdf_path} does not exist")

        reader = PdfReader(str(pdf_path))
        text = "\n".join(page.extract_text() or "" for page in reader.pages)

        if "public comment" in text.lower():
            start, c_start, c_end, expected_start, expected_end = extract_date_range(text)
            snippet = text[text.lower().find("public comment"):][:500]
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
                "confidence": 0.6,
                "notes": snippet,
                "url": str(pdf_path)
            })
    except Exception as e:
        print(f"[ERROR] PDF parse failed for {forest_id}: {e}")
    return projects

def run_scraper(debug_html=False):
    all_records = []
    for name, forest_id in FORESTS_CO:
        print(f"[INFO] Scraping forest: {name}")
        html_records = parse_html_report(forest_id, debug=debug_html)
        all_records.extend(html_records)
        time.sleep(1)

        pdf_path = download_pdf(forest_id)
        if pdf_path:
            pdf_records = parse_pdf_report(forest_id, pdf_path, debug=debug_html)
            all_records.extend(pdf_records)
    return all_records

def save_to_csv(records, path="data/interim/usfs_public_comment.csv"):
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
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug-html", action="store_true", help="Print raw HTML or PDF content for debugging")
    args = parser.parse_args()

    records = run_scraper(debug_html=args.debug_html)
    save_to_csv(records)
