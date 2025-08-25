# -------------------------------
# Config
# -------------------------------
PY ?= python

DATA_INTERIM   := data/interim
DATA_PROCESSED := data/processed
DATA_STD       := data/standardized
DOCS_DATA      := docs/data

BLM_CSV        := $(DATA_INTERIM)/blm_public_comment.csv
USFS_CSV       := $(DATA_INTERIM)/usfs_public_comment.csv
USFS_ENRICHED  := $(DATA_PROCESSED)/usfs_public_comment_with_geom.csv

FINAL_CSV      := $(DATA_STD)/final_opportunities.csv
FINAL_GEOJSON  := $(DATA_STD)/final_opportunities.geojson
PUBLISH_GEOJSON := $(DOCS_DATA)/final_opportunities.geojson

# Default: run whole pipeline to published final GeoJSON
.PHONY: all
all: publish

# 1) Run BLM scraper → interim CSV
$(BLM_CSV):
	@mkdir -p $(DATA_INTERIM)
	$(PY) scripts/blm_scrape.py

# 2) Run USFS scraper → interim CSV
$(USFS_CSV):
	@mkdir -p $(DATA_INTERIM)
	$(PY) scripts/usfs_sopa_scrape.py

# 3) Enrich USFS CSV with ranger district geoms
$(USFS_ENRICHED): $(USFS_CSV)
	@mkdir -p $(DATA_PROCESSED)
	$(PY) scripts/enrich_with_district_geoms.py --csv-out $(USFS_ENRICHED)

# 4) Standardize + finalize into final_opportunities.* (drops intermediates)
$(FINAL_CSV) $(FINAL_GEOJSON): $(BLM_CSV) $(USFS_ENRICHED)
	@mkdir -p $(DATA_STD)
	$(PY) scripts/standardize.py \
		$(BLM_CSV) $(USFS_ENRICHED) \
		--csv $(FINAL_CSV) \
		--geojson $(FINAL_GEOJSON)

# 5) Copy final GeoJSON into docs/ for GitHub Pages
$(PUBLISH_GEOJSON): $(FINAL_GEOJSON)
	@mkdir -p $(DOCS_DATA)
	cp $(FINAL_GEOJSON) $(PUBLISH_GEOJSON)

.PHONY: publish
publish: $(FINAL_CSV) $(FINAL_GEOJSON) $(PUBLISH_GEOJSON)
	@echo "[OK] Final outputs ready:"
	@echo "  - $(FINAL_CSV)"
	@echo "  - $(FINAL_GEOJSON)"
	@echo "  - Published web copy → $(PUBLISH_GEOJSON)"

.PHONY: clean
clean:
	@rm -f $(FINAL_CSV) $(FINAL_GEOJSON) $(PUBLISH_GEOJSON)
