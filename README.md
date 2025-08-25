# Public Comment Opportunities on BLM & USFS Lands – Colorado

## Overview
This project identifies and maps **public comment opportunities** on Bureau of Land Management (BLM) and United States Forest Service (USFS) managed lands in Colorado.  

It was completed as part of the **Senior Geospatial Analyst Candidate Project** for onX. The workflow demonstrates data sourcing, automation, standardization, and geospatial visualization.  

👉 **[Interactive Map (GitHub Pages)](https://<your-username>.github.io/<your-repo-name>/)**  

---

## Objectives
- Collect and standardize data on public comment opportunities from **BLM ePlanning** and **USFS SOPA** sites.  
- Automate extraction from both structured (CSV/JSON) and unstructured (HTML/PDF) sources.  
- Enrich records with geospatial context (ranger district geometries, centroids).  
- Provide a clean, minimal dataset for mapping.  
- Visualize results in an **interactive web map** for public engagement and advocacy.  

---

## Deliverables
- **Interactive Map** – projects symbolized by status (active, upcoming, closed).  
- **Data Pipeline** – automated scripts for scraping, parsing, enrichment, and standardization.  
- **Standardized Dataset** – final CSV and GeoJSON of opportunities.  
- **Documentation** – this README (user + technical instructions).  
- **Summary One-Pager** – highlights workflow, methods, and insights.  

---

## Repository Structure
```
├── scripts/
│   ├── blm_scrape.py                # Scrapes BLM ePlanning tabs (510, 570, 565, 5101) 
│   ├── usfs_sopa_scrape.py          # Scrapes USFS SOPA HTML + PDFs for comment dates
│   ├── enrich_with_district_geoms.py # Matches USFS projects to district polygons, adds centroids
│   ├── standardize.py               # Cleans and aligns schema across sources
│   ├── finalize_opportunities.py    # Outputs final CSV + GeoJSON for map
├── data/
│   ├── interim/                     # Intermediate raw scrapes
│   ├── processed/                   # Enriched outputs
│   ├── standardized/                # Final clean outputs
├── requirements.txt                 # Runtime dependencies
├── requirements-dev.txt             # Dev/test dependencies
├── Makefile                         # Task shortcuts (scrape, enrich, standardize)
└── README.md                        # Documentation
```

---

## Setup Instructions

### 1. Clone & Install
```bash
git clone https://github.com/<your-username>/<your-repo-name>.git
cd <your-repo-name>
python -m venv .venv
source .venv/bin/activate   # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
```

### 2. Run Data Collection
Scrape **BLM** and **USFS** opportunities:
```bash
python scripts/blm_scrape.py
python scripts/usfs_sopa_scrape.py
```
Outputs saved under `data/interim/`.

### 3. Enrich with Ranger District Geometry
For USFS projects, add geospatial context:
```bash
python scripts/enrich_with_district_geoms.py
```
Saves `data/processed/usfs_public_comment_with_geom.csv`.

### 4. Standardize & Finalize
Merge BLM + USFS into a single dataset:
```bash
python scripts/finalize_opportunities.py   data/interim/blm_public_comment.csv   data/processed/usfs_public_comment_with_geom.csv   --csv data/standardized/final_opportunities.csv   --geojson data/standardized/final_opportunities.geojson
```

### 5. Launch Map
The web map loads from the standardized GeoJSON. To preview locally:
```bash
python -m http.server 8000
```
Then open [http://localhost:8000](http://localhost:8000) in your browser.

---

## Map Usage
- **Pan/zoom** to explore Colorado.  
- **Click project markers** to view details:  
  - Name & agency (BLM/USFS)  
  - Comment period dates  
  - Notes & description  
  - Source link  

Projects are symbolized by status:  
- 🟢 Active  
- 🟡 Upcoming  
- 🔴 Closed  

---

## Key Insights
- **Automation** is necessary: BLM and USFS present inconsistent schemas and formats.  
- **PDF parsing** was required for USFS SOPA, where public comment notices are sometimes only available as attachments.  
- **Geospatial enrichment** enables placing projects on the map even when precise lat/lon is not provided.  
- A **single standardized dataset** improves usability and clarity for advocacy groups.  

---

## Next Steps
- Expand coverage beyond Colorado.  
- Integrate **scheduled refresh** via GitHub Actions.  
- Add **polygon geometries** for planning areas, not just centroids.  
- Enable **filtering by agency/project type** in the web map.  

---

## Author
**Vance Harris**  
[Portfolio](https://vanceharris.com) · [GitHub](https://github.com/harrisv25) · [LinkedIn](https://www.linkedin.com/in/vanceharris)
