.ONESHELL:

# Use the venv's Python explicitly (Windows-friendly)
PY := ./.venv/Scripts/python.exe

# Default: build data + web bundle
all: setup data map

# ---- ENV SETUP ----
setup:
	@if [ ! -f ".venv/Scripts/python.exe" ]; then \
		py -m venv .venv || python -m venv .venv; \
	fi
	$(PY) -m pip install -r requirements.txt
	@if [ -f requirements-dev.txt ]; then \
		$(PY) -m pip install -r requirements-dev.txt; \
	fi
	@mkdir -p data/raw data/interim data/standardized webmap

# ---- DATA PIPELINE ----

# BLM: discover ALL relevant Colorado projects (no manual IDs)
# (Next step: I'll patch blm_scrape.py to implement --discover-co and optional --open-only.)
blm:
	$(PY) scripts/blm_scrape.py --state CO --discover-co \
	  -o data/interim/blm.csv

# USFS: auto-discover ALL Colorado Forest/Grassland SOPA pages
usfs:
	$(PY) scripts/usfs_sopa_scrape.py --state CO --auto-co \
	  -o data/interim/usfs.csv

# Merge + compute status -> GeoJSON + CSV
standardize: blm usfs
	$(PY) scripts/standardize.py data/interim/*.csv \
	  -o data/standardized/opportunities.geojson \
	  --csv data/standardized/opportunities.csv

data: standardize

# ---- WEB MAP ----
map: data
	$(PY) scripts/build_map_assets.py \
	  --in data/standardized/opportunities.geojson \
	  --out webmap/data.json
	@echo "✅ Map assets built -> open webmap/index.html"

# ---- QA / DEV ----
test:
	$(PY) -m pytest -q

serve: map
	$(PY) -m http.server 8000
