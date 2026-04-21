# Scrapers

Small Python scrapers for collecting multimodal image-question datasets from public science and education sources.

## Included scrapers

- `cdc/cdc_phil_scraper.py`: collects public-domain image records from the CDC Public Health Image Library.
- `jeopardy/jeopardy_scraper.py`: collects archived Jeopardy! clues and responses into a quiz dataset.
- `jeopardy/jeopardy_show_*_visual_scraper.py`: collect J-Archive clues from specific shows that include linked visual media. The working Jeopardy batch targets shows 7103 through 7124.
- `kensquiz/kensquiz_scraper.py`: collects Ken's Quiz road-sign handout questions and cropped image tiles.
- `kensquiz/kensquiz_handout_scraper.py`: collects Ken's Quiz pub quiz handout picture rounds and cropped image tiles.
- `nasa/nasa_apod_scraper.py`: collects Astronomy Picture of the Day image records from NASA.
- `nasa/nasa_spaceplace_scraper.py`: collects image-based records from NASA Space Place articles.
- `nih/niaid_bioart_scraper.py`: collects public NIH BioArt image records.
- `plos/plos_research_figure_scraper.py`: collects peer-reviewed PLOS article figures with questions that combine figure legends and abstracts.
- `quizbowl/quizbowl_picture_rounds_scraper.py`: collects quizbowl-style picture rounds from curated Wikipedia/Wikimedia Commons pages.
- `sporcle/sporcle_scraper.py`: collects image-backed trivia prompts from a Sporcle slideshow quiz.
- `sporcle/sporcle_*_scraper.py`: collect specific Sporcle slideshow quizzes with image-backed prompts. The working Sporcle batch currently targets 13 quizzes, including the actor series plus `Broken Bones by X-Ray` and `Animals with David Attenborough`.
- `wikipedia/wikipedia_biology_scraper.py`: collects biology-related image records from Wikipedia.

## Output files

Each scraper writes JSON into `dataset/`. On each run, remote media is downloaded into `dataset/images/<source>/`, the main `image_url` or `media_url` field is rewritten to the local file path, and the original remote URL is preserved in `source_image_url` or `source_media_url`. The quiz scrapers preserve their imported question text, and the rest of the generated questions are intended to use both the image and the accompanying source text rather than simple "what is shown?" identification prompts.

## Setup

1. Create a virtual environment and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install requests beautifulsoup4 pdfplumber pdf2image pillow
```

2. Copy the example environment file and fill in any values you need:

```bash
cp .env.example .env
```

- `NASA_API_KEY` is optional. If it is not set, the APOD scraper falls back to NASA's `DEMO_KEY`, which has stricter rate limits.
- `SCRAPER_CONTACT_EMAIL` is optional but recommended for polite request headers.
- `kensquiz/kensquiz_scraper.py` requires Poppler so `pdf2image` can rasterize the handout PDFs.

## Running the scrapers

```bash
python3 cdc/cdc_phil_scraper.py
python3 jeopardy/jeopardy_scraper.py
python3 jeopardy/jeopardy_show_7124_visual_scraper.py
python3 kensquiz/kensquiz_scraper.py
python3 kensquiz/kensquiz_handout_scraper.py
python3 nasa/nasa_apod_scraper.py
python3 nasa/nasa_spaceplace_scraper.py
python3 nih/niaid_bioart_scraper.py
python3 plos/plos_research_figure_scraper.py
python3 quizbowl/quizbowl_picture_rounds_scraper.py
python3 sporcle/sporcle_scraper.py
python3 wikipedia/wikipedia_biology_scraper.py
```

Use additional Jeopardy visual-clue scrapers by running the matching file in `jeopardy/`, for example `python3 jeopardy/jeopardy_show_7116_visual_scraper.py`.
Use additional Sporcle slideshow scrapers by running the matching file in `sporcle/`, for example `python3 sporcle/sporcle_actors_through_three_decades_on_tv_iv_scraper.py`.
