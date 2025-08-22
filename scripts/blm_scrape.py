#!/usr/bin/env python3
"""
BLM ePlanning participation scraper (browser-only, CO-focused).
- Discovers project IDs via Playwright-rendered search UI.
- Scrapes Public Participation tabs (510/570/565) with the browser.
- Extracts likely public comment windows using keyword-aware heuristics.

Run:
  python scripts/blm_scrape.py --state CO --discover-co --debug -o data/interim/blm.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote

from bs4 import BeautifulSoup  # only used for occasional text cleanup if needed
from dateutil import parser as dparse

# -------------------- Config --------------------
PARTICIPATION_PATHS: Tuple[str, ...] = ("510", "570", "565")

# ID patterns (permissive; handles absolute and relative URLs, with/without trailing tab)
PROJECT_ID_RE = re.compile(r"/eplanning-ui/project/(\d{6,})(?:/|$)")
PROJECT_HREF_RE = re.compile(r"/eplanning-ui/project/\d{6,}(?:/\d{3})?")

# Date patterns
COMMENT_DUE_RE = re.compile(
    r"(?:comments?\s+(?:are\s+due|due\s+by|must\s+be\s+postmarked\s+by)\s*)([A-Za-z]{3,9}\s+\d{1,2},\s*\d{4})",
    re.IGNORECASE,
)
DATE_ANY_RE = re.compile(
    r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},\s*\d{4}",
    re.IGNORECASE,
)

@dataclass
class ScrapeResult:
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
    notes_tabs: str
    notes_examples: str

# -------------------- Helpers --------------------
def _search_url_for_state(state: str = "CO", active: bool = True, open_only: bool = False) -> str:
    filt = {
        "states": [state],
        "offices": None,
        "projectTypes": None,
        "programs": None,
        "years": None,
        "open": bool(open_only),
        "active": bool(active),
    }
    return f"https://eplanning.blm.gov/eplanning-ui/search?filterSearch={quote(json.dumps(filt))}"

def _extract_project_id_from_url(url: str) -> Optional[str]:
    if not url:
        return None
    m = PROJECT_ID_RE.search(url)
    return m.group(1) if m else None

# -------------------- Playwright discovery --------------------
def discover_project_ids(state: str = "CO", active: bool = True,
                         open_only: bool = False, debug: bool = False) -> List[str]:
    from playwright.sync_api import sync_playwright

    ids: set[str] = set()
    search_url = _search_url_for_state(state, active=active, open_only=open_only)

    def harvest_from_page(page) -> None:
        # A) Anchor hrefs
        anchors = page.locator("a[href*='/eplanning-ui/project/']")
        hrefs = anchors.evaluate_all("els => els.map(e => e.getAttribute('href'))")
        if debug:
            print(f"[harvest:anchors] hrefs={len(hrefs) if hrefs else 0}")
            if hrefs:
                print("  sample:", hrefs[:5])
        for h in hrefs or []:
            pid = _extract_project_id_from_url(h)
            if pid:
                ids.add(pid)
        # B) Full HTML regex (covers non-anchor renderings)
        html = page.content()
        for m in PROJECT_HREF_RE.finditer(html):
            pid = _extract_project_id_from_url(m.group(0))
            if pid:
                ids.add(pid)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        page.set_default_timeout(35_000)

        if debug:
            print(f"[discover:browser] go to {search_url}")
        page.goto(search_url, wait_until="domcontentloaded")
        page.wait_for_timeout(1500)
        harvest_from_page(page)

        # Try “Show More”/scroll a few times
        for _ in range(10):
            before = len(ids)
            try:
                btns = page.locator("button:has-text('Show More'), button:has-text('More'), a:has-text('More')")
                if btns.count() > 0:
                    btns.first.click()
                    page.wait_for_timeout(900)
            except Exception:
                pass
            page.mouse.wheel(0, 2200)
            page.wait_for_timeout(900)
            harvest_from_page(page)
            if len(ids) == before:
                break

        if debug:
            print(f"[discover:browser] collected {len(ids)} ids")

        context.close()
        browser.close()

    return sorted(ids)

# -------------------- Browser tab scraping --------------------
def _page_inner_text(page) -> str:
    try:
        return page.locator("body").inner_text(timeout=5_000)
    except Exception:
        try:
            return page.evaluate("document.body && document.body.innerText || ''")
        except Exception:
            return ""

def fetch_all_participation_text_browser(page, project_id: str, debug: bool = False) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for tab in PARTICIPATION_PATHS:
        url = f"https://eplanning.blm.gov/eplanning-ui/project/{project_id}/{tab}"
        try:
            if debug:
                print(f"[tab] goto {url}")
            page.goto(url, wait_until="domcontentloaded", timeout=35_000)
            page.wait_for_timeout(1000)
            txt = _page_inner_text(page)
            if debug:
                print(f"[tab] {tab} text_len={len(txt)}")
            if txt.strip():
                out[tab] = txt
        except Exception as exc:
            if debug:
                print(f"[tab] error {url}: {exc}")
    return out

# -------------------- Date parsing (comment-aware) --------------------
def parse_dates_from_text(text: str, debug: bool = False) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract a likely public comment window from page text.
    Heuristics:
      - Only consider clauses with participation keywords.
      - Prefer explicit 'comments due by ...'
      - Ignore absurd years (<2000 or >current+2)
      - Pick the tightest clause window.
    """
    if not text:
        return None, None

    KW = re.compile(r"(comment|public|scoping|feedback|submit|due|open|close|participation|input)", re.I)
    CURRENT_YEAR = datetime.now().year
    MIN_YEAR, MAX_YEAR = 2000, CURRENT_YEAR + 2

    def year_ok(dt: datetime) -> bool:
        return MIN_YEAR <= dt.year <= MAX_YEAR

    clauses = re.split(r"(?<=[\.\!\?])\s+|\n+", text)
    clauses = [c.strip() for c in clauses if c.strip()]
    relevant = [c for c in clauses if KW.search(c)]
    if debug:
        print(f"[dates] relevant clauses: {len(relevant)} / {len(clauses)}")

    # explicit "comments due by ..."
    for c in relevant:
        m = COMMENT_DUE_RE.search(c)
        if m:
            try:
                d = dparse.parse(m.group(1), fuzzy=True)
                if year_ok(d):
                    if debug:
                        print(f"[dates] explicit due-by -> {d.date().isoformat()}")
                    return None, d.date().isoformat()
            except Exception:
                pass

    def extract_dates(s: str) -> list[datetime]:
        out = []
        for m in DATE_ANY_RE.finditer(s):
            try:
                d = dparse.parse(m.group(0), fuzzy=True)
                if year_ok(d):
                    out.append(d)
            except Exception:
                continue
        return out

    candidates: list[Tuple[Optional[str], Optional[str], int]] = []
    for c in relevant:
        ds = extract_dates(c)
        if len(ds) >= 2:
            ds.sort()
            s, e = ds[0].date().isoformat(), ds[-1].date().isoformat()
            candidates.append((s, e, len(c)))
        elif len(ds) == 1 and re.search(r"(due|close|deadline|by)", c, re.I):
            e = ds[0].date().isoformat()
            candidates.append((None, e, len(c)))

    if not candidates:
        if debug:
            print("[dates] no candidates after filtering")
        return None, None

    candidates.sort(key=lambda t: t[2])  # shortest clause wins
    best_s, best_e, _ = candidates[0]
    if debug:
        print(f"[dates] picked window -> start={best_s}, end={best_e}")
    return best_s, best_e

# -------------------- Build result row --------------------
def _scrape_project_from_texts(project_id: str, state: str,
                               tab_texts: Dict[str, str], debug: bool = False) -> ScrapeResult:
    tab_presence = []
    tab_snippets = []
    combined_text_parts: List[str] = []

    primary_url = None
    title = f"BLM Project {project_id}"

    for tab in PARTICIPATION_PATHS:
        txt = tab_texts.get(tab)
        has_tab = txt is not None
        tab_presence.append(f"{tab}:{str(has_tab).lower()}")
        if not txt:
            continue
        if primary_url is None or tab == "510":
            primary_url = f"https://eplanning.blm.gov/eplanning-ui/project/{project_id}/{tab}"
        combined_text_parts.append(txt)
        tab_snippets.append(f"{tab}:{txt[:160].replace('\n',' ')}")

    combined_text = " ".join(combined_text_parts)
    start, end = parse_dates_from_text(combined_text, debug=debug)
    confidence = 0.8 if (start or end) else (0.5 if combined_text else 0.2)
    if debug:
        print(f"[scrape] {project_id}: start={start}, end={end}, conf={confidence}")

    return ScrapeResult(
        project_id=f"BLM-{project_id}",
        agency="BLM",
        title=title,
        description=combined_text[:1000],
        office_or_unit="",
        comment_start_date=start,
        comment_end_date=end,
        source_url=primary_url or f"https://eplanning.blm.gov/eplanning-ui/project/{project_id}/510",
        state=state,
        geometry_type="",
        geometry="",
        geom_source="",
        scrape_confidence=confidence,
        last_checked_utc=datetime.now(timezone.utc).isoformat(),
        notes_tabs=";".join(tab_presence),
        notes_examples=" | ".join(tab_snippets)[:500],
    )

def scrape_projects_with_browser(ids: List[str], state: str, debug: bool = False) -> List[ScrapeResult]:
    from playwright.sync_api import sync_playwright

    results: List[ScrapeResult] = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        page.set_default_timeout(35_000)

        for pid in ids:
            if debug:
                print(f"[scrape] project {pid}")
            tab_texts = fetch_all_participation_text_browser(page, pid, debug=debug)
            res = _scrape_project_from_texts(pid, state, tab_texts, debug=debug)
            results.append(res)

        context.close()
        browser.close()
    return results

# -------------------- CLI --------------------
def main() -> None:
    ap = argparse.ArgumentParser()
    mx = ap.add_mutually_exclusive_group(required=True)
    mx.add_argument("--ids", nargs="+", help="Manual BLM project IDs (e.g., 2033900 2027547)")
    mx.add_argument("--discover-co", action="store_true", help="Discover ALL Colorado BLM projects (browser)")

    ap.add_argument("--state", default="CO", help="Two-letter state code (default: CO)")
    ap.add_argument("--open-only", action="store_true", help="Use 'open' filter in discovery page")
    ap.add_argument("--debug", action="store_true", help="Verbose logging")
    ap.add_argument("--ids-file", help="Optional: text file with one BLM project ID per line")
    ap.add_argument("-o", "--out", required=True, help="Output CSV path")
    args = ap.parse_args()

    # Choose IDs
    if args.discover_co:
        ids = discover_project_ids(state=args.state, active=True, open_only=args.open_only, debug=args.debug)
    elif args.ids_file:
        with open(args.ids_file, "r", encoding="utf-8") as fh:
            ids = [ln.strip() for ln in fh if ln.strip() and not ln.strip().startswith("#")]
    else:
        ids = args.ids

    if not ids:
        raise SystemExit("No BLM project IDs discovered or provided.")

    # Scrape
    rows = scrape_projects_with_browser(ids, args.state, debug=args.debug)

    # Write CSV
    fieldnames = list(ScrapeResult.__annotations__.keys())
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(asdict(r))

if __name__ == "__main__":
    main()
