import re
from scripts.blm_scrape import (
    parse_dates_from_text,
    fetch_all_participation_html,
    scrape_project,
    discover_project_ids,
    _search_url_for_state,
)

HTML_510_WITH_END = """
<html><head><title>Sample BLM Project</title></head>
<body>
<h2>Public Participation</h2>
<p>Comments are due by September 15, 2025.</p>
</body></html>
"""

HTML_570_WITH_RANGE = """
<html><head><title>Ignore This Title</title></head>
<body>
<p>Public scoping from Aug 20, 2025 through Sep 10, 2025.</p>
</body></html>
"""

# Added: stub for a tab with no dates
HTML_565_EMPTY = """
<html><head><title>Other Tab</title></head>
<body>
<p>No dates here.</p>
</body></html>
"""

# Mocked HTML for discovery from the ePlanning search page
HTML_SEARCH = """
<html>
  <body>
    <a href="/eplanning-ui/project/2033900/510">Project A</a>
    <a href="/eplanning-ui/project/2027547/570">Project B</a>
    <a href="/eplanning-ui/project/2033900/565">Dup Different Tab</a>
  </body>
</html>
"""

def test_discover_project_ids_parses_unique_ids(requests_mock):
    url = _search_url_for_state("CO", active=True, open_only=False)
    requests_mock.get(url, text=HTML_SEARCH, status_code=200)
    ids = discover_project_ids("CO", active=True, open_only=False)
    assert ids == ["2027547", "2033900"]

def test_discovery_then_scrape_tabs(requests_mock):
    # mock search discovery
    url = _search_url_for_state("CO", active=True, open_only=False)
    requests_mock.get(url, text=HTML_SEARCH, status_code=200)

    # mock tabs for 2033900
    base_203 = "https://eplanning.blm.gov/eplanning-ui/project/2033900"
    requests_mock.get(f"{base_203}/510", text=HTML_510_WITH_END, status_code=200)
    requests_mock.get(f"{base_203}/570", text=HTML_570_WITH_RANGE, status_code=200)
    requests_mock.get(f"{base_203}/565", status_code=404)

    # mock tabs for 2027547
    base_202 = "https://eplanning.blm.gov/eplanning-ui/project/2027547"
    requests_mock.get(f"{base_202}/510", status_code=404)
    requests_mock.get(f"{base_202}/570", text=HTML_570_WITH_RANGE, status_code=200)
    requests_mock.get(f"{base_202}/565", status_code=404)

    ids = discover_project_ids("CO", active=True, open_only=False)
    # scrape one to validate merge logic via discovery
    r = scrape_project(ids[1], "CO")  # pick '2033900'
    assert r.comment_start_date == "2025-08-20"
    assert r.comment_end_date == "2025-09-15"
    assert r.source_url.endswith("/510")

def test_parse_dates_from_text_end_only():
    s, e = parse_dates_from_text("Comments are due by September 15, 2025.")
    assert s is None and e == "2025-09-15"

def test_parse_dates_from_text_range():
    s, e = parse_dates_from_text("Scoping Aug 20, 2025 through Sep 10, 2025.")
    assert s == "2025-08-20" and e == "2025-09-10"

def test_fetch_all_participation_html(requests_mock):
    base = "https://eplanning.blm.gov/eplanning-ui/project/2033900"
    # 510 fails, 570 succeeds, 565 succeeds
    requests_mock.get(f"{base}/510", status_code=404)
    requests_mock.get(f"{base}/570", text=HTML_570_WITH_RANGE, status_code=200)
    requests_mock.get(f"{base}/565", text=HTML_565_EMPTY, status_code=200)

    html_map = fetch_all_participation_html("2033900")
    assert html_map["510"] is None
    assert "through" in html_map["570"]
    assert "No dates here" in html_map["565"]

def test_scrape_project_merges_tabs_and_picks_primary_510_when_available(requests_mock):
    base = "https://eplanning.blm.gov/eplanning-ui/project/2029163"
    requests_mock.get(f"{base}/510", text=HTML_510_WITH_END, status_code=200)
    requests_mock.get(f"{base}/570", text=HTML_570_WITH_RANGE, status_code=200)
    requests_mock.get(f"{base}/565", status_code=404)

    result = scrape_project("2029163", "CO")
    # primary URL should be 510 because it's available
    assert result.source_url.endswith("/510")
    # dates should merge: start from 570, end from either 570 or explicit 510 (explicit end wins if later)
    assert result.comment_start_date == "2025-08-20"
    assert result.comment_end_date == "2025-09-15"
    assert "510:true" in result.notes_tabs and "570:true" in result.notes_tabs
    assert result.scrape_confidence == 0.8

def test_scrape_project_no_dates_lowers_confidence(requests_mock):
    base = "https://eplanning.blm.gov/eplanning-ui/project/2027000"
    requests_mock.get(f"{base}/510", text="<html><title>T</title><body>No dates</body></html>", status_code=200)
    requests_mock.get(f"{base}/570", status_code=404)
    requests_mock.get(f"{base}/565", status_code=404)

    result = scrape_project("2027000", "CO")
    assert result.comment_start_date is None and result.comment_end_date is None
    assert result.scrape_confidence == 0.5  # we had some text but no dates
