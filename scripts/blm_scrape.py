"""
Revised USFS SOPA Scraper to mirror the BLM script structure and data model.
Focuses on active projects with public comment periods and consistent date handling.
"""
import csv
import re
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime

# Hardcoded list of Colorado SOPA forest IDs and names
FORESTS = {
    "110202": "Arapaho and Roosevelt NFs & Pawnee NG",
    "110204": "Grand Mesa, Uncompahgre and Gunnison NFs",
    "110206": "Medicine Bow-Routt NFs",
    "110208": "Pike and San Isabel NFs & Comanche and Cimarron NGs",
    "110209": "Rio Grande NF",
    "110210": "San Juan NF",
    "110212": "White River NF",
    "110701": "Cimarron National Grassland",
    "110504": "Manti-La Sal NF",
    "110802": "Comanche National Grassland",
    "110902": "Pawnee National Grassland"
}

BASE_URL = "https://www.fs.usda.gov/sopa/"
OUTPUT_CSV = "data/interim/usfs_public_comment.csv"

# --- Helpers ---
def extract_dates(text):
    pattern = r"(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}"
    matches = re.findall(pattern, text)
    parsed = []
    for m in matches:
        try:
            dt = datetime.strptime(m, "%B %d, %Y").date().isoformat()
            parsed.append(dt)
        except:
            continue
    return parsed[:2]  # comment_start, comment_end

# --- Main scraping ---
def scrape_sopa():
    records = []
    for fid, fname in FORESTS.items():
        print(f"[INFO] Scraping forest: {fname}")
        forest_url = f"{BASE_URL}forest-level.php?{fid}"
        try:
            res = requests.get(forest_url)
            soup = BeautifulSoup(res.text, "html.parser")
            report_link = soup.find("a", string=re.compile("Current SOPA Report", re.I))
            if not report_link:
                print(f"[WARN] No report link found for {fname}")
                continue

            href = soup.find("a", string="html")
            if not href or not href.get("href"):
                print(f"[WARN] No HTML SOPA report found for {fname}")
                continue

            report_url = urljoin(BASE_URL, href["href"])
            report_res = requests.get(report_url)
            report_soup = BeautifulSoup(report_res.text, "html.parser")

            for row in report_soup.select("table.reportTable tr.reportRow"):
                cols = row.find_all("td")
                if len(cols) < 4:
                    continue

                project_name = cols[0].get_text(strip=True)
                project_status = cols[1].get_text(strip=True)
                comment_info = cols[3].get_text(strip=True)
                dates = extract_dates(comment_info)

                if "comment" in comment_info.lower():
                    record = {
                        "project_id": None,
                        "state": "Colorado",
                        "latitude": None,
                        "longitude": None,
                        "start_date": dates[0] if dates else None,
                        "comment_start": dates[0] if dates else None,
                        "comment_end": dates[1] if len(dates) > 1 else dates[0] if dates else None,
                        "confidence": 0.8,
                        "url": report_url,
                        "forest": fname,
                        "project_name": project_name,
                        "project_status": project_status
                    }
                    print("[MATCH] Public comment project:", record)
                    records.append(record)

            time.sleep(0.8)
        except Exception as e:
            print(f"[ERROR] Failed to scrape {fname}: {e}")
    return records

def save_to_csv(records, path=OUTPUT_CSV):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "project_id", "state", "latitude", "longitude",
            "start_date", "comment_start", "comment_end",
            "confidence", "url", "forest", "project_name", "project_status"
        ])
        writer.writeheader()
        writer.writerows(records)
    print(f"[INFO] Saved {len(records)} records to {path}")

if __name__ == "__main__":
    recs = scrape_sopa()
    save_to_csv(recs)
