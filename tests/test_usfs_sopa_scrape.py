import pytest
from scripts import usfs_sopa_scrape


def test_parse_dates_from_text_single_end():
    text = "Comments must be received by October 15, 2025."
    s, e = usfs_sopa_scrape.parse_dates_from_text(text)
    assert s is None
    assert e == "2025-10-15"


def test_parse_dates_from_text_range():
    text = "Open: August 1, 2025 through September 15, 2025."
    s, e = usfs_sopa_scrape.parse_dates_from_text(text)
    assert s == "2025-08-01"
    assert e == "2025-09-15"


@pytest.mark.parametrize("bad_text", [
    "", "No dates here", "Deadline TBD"
])
def test_parse_dates_from_text_none(bad_text):
    s, e = usfs_sopa_scrape.parse_dates_from_text(bad_text)
    assert s is None and e is None


def test_discover_colorado_sopa_units(requests_mock):
    # Mock the Colorado state index page with two forest links
    html = """
    <html><body>
    <a href="forest-level.php?110210">Arapaho & Roosevelt</a>
    <a href="forest-level.php?110215">White River</a>
    </body></html>
    """
    requests_mock.get(usfs_sopa_scrape.STATE_CO_URL, text=html)

    units = usfs_sopa_scrape.discover_colorado_sopa_units()
    assert ("Arapaho & Roosevelt", "https://www.fs.usda.gov/sopa/forest-level.php?110210") in units
    assert ("White River", "https://www.fs.usda.gov/sopa/forest-level.php?110215") in units


def test_scrape_sopa_listing_parses_rows(requests_mock):
    # Mock a SOPA unit page with a simple table
    html = """
    <html><body>
    <table>
      <tr><th>Header</th></tr>
      <tr><td><a href="https://example.com/proj123">Trail Restoration</a> - Comments due September 15, 2025</td></tr>
    </table>
    </body></html>
    """
    url = "https://www.fs.usda.gov/sopa/forest-level.php?110210"
    requests_mock.get(url, text=html)

    rows = usfs_sopa_scrape.scrape_sopa_listing(url, state="CO", office_label="TestForest")
    assert len(rows) == 1
    r = rows[0]
    assert r.title == "Trail Restoration"
    assert r.comment_end_date == "2025-09-15"
    assert r.office_or_unit == "TestForest"
    assert r.state == "CO"
