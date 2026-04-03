# Scrapers

Small Python scrapers for collecting multimodal image-question datasets from public science and education sources.

## Included scrapers

- `cdc/cdc_phil_scraper.py`: collects public-domain image records from the CDC Public Health Image Library.
- `nasa/nasa_apod_scraper.py`: collects Astronomy Picture of the Day image records from NASA.
- `nasa/nasa_spaceplace_scraper.py`: collects image-based records from NASA Space Place articles.
- `nih/niaid_bioart_scraper.py`: collects public NIH BioArt image records.
- `plos/plos_research_figure_scraper.py`: collects peer-reviewed PLOS article figures with questions that combine figure legends and abstracts.
- `wikipedia/wikipedia_biology_scraper.py`: collects biology-related image records from Wikipedia.

## Output files

Each scraper writes JSON into `dataset/`. The generated questions are intended to use both the image and the accompanying source text, rather than simple "what is shown?" identification prompts.

## Setup

1. Create a virtual environment and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install requests beautifulsoup4
```

2. Copy the example environment file and fill in any values you need:

```bash
cp .env.example .env
```

- `NASA_API_KEY` is optional. If it is not set, the APOD scraper falls back to NASA's `DEMO_KEY`, which has stricter rate limits.
- `SCRAPER_CONTACT_EMAIL` is optional but recommended for polite request headers.

## Running the scrapers

```bash
python3 cdc/cdc_phil_scraper.py
python3 nasa/nasa_apod_scraper.py
python3 nasa/nasa_spaceplace_scraper.py
python3 nih/niaid_bioart_scraper.py
python3 plos/plos_research_figure_scraper.py
python3 wikipedia/wikipedia_biology_scraper.py
```
