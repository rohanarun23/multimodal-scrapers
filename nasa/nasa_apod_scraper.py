import json
import os
import time
from datetime import date, datetime, timedelta

import requests
from requests import Response


# API endpoint and output path for the generated dataset.
API_URL = "https://api.nasa.gov/planetary/apod"
OUTPUT_JSON = "dataset/nasa_apod_questions.json"
API_KEY = os.getenv("NASA_API_KEY", "DEMO_KEY").strip() or "DEMO_KEY"
CONTACT_EMAIL = os.getenv("SCRAPER_CONTACT_EMAIL", "your-email@example.com").strip() or "your-email@example.com"
START_DATE = "2024-01-01"
END_DATE = date.today().isoformat()

MAX_RETRIES = 4
REQUEST_DELAY_SECONDS = 0.5
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
IDENTIFYING_QUESTION = "What astronomical object or phenomenon is shown in this NASA image?"
DATE_CHUNK_DAYS = 120
MAX_RETRY_WAIT_SECONDS = 8
DEMO_KEY_CHUNK_DAYS = 7

# Reuse one session so headers and connection pooling stay consistent.
SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": f"NASAAPODScraper/1.0 (contact: {CONTACT_EMAIL})",
        "Accept": "application/json",
    }
)


def make_request(url: str, *, params=None, timeout: int = 30) -> Response:
    # Retry temporary network and rate-limit errors before failing.
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = SESSION.get(url, params=params, timeout=timeout)
        except requests.RequestException as exc:
            last_error = exc
            if attempt == MAX_RETRIES:
                break

            wait_seconds = attempt * 2
            print(f"Request failed for {url}: {exc}. Retrying in {wait_seconds}s.")
            time.sleep(wait_seconds)
            continue

        if response.status_code in RETRY_STATUS_CODES:
            if attempt == MAX_RETRIES:
                response.raise_for_status()

            retry_after = response.headers.get("Retry-After")
            if retry_after and retry_after.isdigit():
                wait_seconds = min(float(retry_after), MAX_RETRY_WAIT_SECONDS)
            else:
                wait_seconds = min(attempt * 2, MAX_RETRY_WAIT_SECONDS)
            print(f"Retryable response {response.status_code} for {response.url}. Waiting {wait_seconds:.1f}s.")
            response.close()
            time.sleep(wait_seconds)
            continue

        response.raise_for_status()
        time.sleep(REQUEST_DELAY_SECONDS)
        return response

    raise last_error


def clean_text(text: str | None) -> str:
    return " ".join((text or "").split()).strip()


def parse_iso_date(value: str, label: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"{label} must be in YYYY-MM-DD format; got {value!r}") from exc


def iter_date_chunks(start_date: str, end_date: str):
    # NASA's demo key is heavily rate-limited, so request smaller ranges.
    current = parse_iso_date(start_date, "NASA_APOD_START_DATE")
    end = parse_iso_date(end_date, "NASA_APOD_END_DATE")
    chunk_days = DEMO_KEY_CHUNK_DAYS if API_KEY == "DEMO_KEY" else DATE_CHUNK_DAYS

    while current <= end:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end)
        yield current.isoformat(), chunk_end.isoformat()
        current = chunk_end + timedelta(days=1)


def is_valid_item(item: dict) -> bool:
    # Keep only records that point to a usable image and have a label.
    if item.get("media_type") != "image":
        return False

    image_url = item.get("hdurl") or item.get("url")
    if not image_url:
        return False

    lower = image_url.lower()
    if not lower.endswith((".jpg", ".jpeg", ".png", ".webp")):
        return False

    return bool(clean_text(item.get("title")))


def build_record(item: dict, index: int) -> dict:
    image_url = item.get("hdurl") or item.get("url")

    return {
        "id": f"nasa_apod_{index:03d}",
        "question": IDENTIFYING_QUESTION,
        "answer": clean_text(item.get("title")),
        "source": "nasa_apod",
        "source_url": item.get("url"),
        "image_url": image_url,
        "date": item.get("date"),
        "copyright": clean_text(item.get("copyright")) or None,
        "context": clean_text(item.get("explanation")),
    }


def fetch_apod_items() -> list[dict]:
    # Collect records in chunks to stay within API limits.
    items = []

    for chunk_start, chunk_end in iter_date_chunks(START_DATE, END_DATE):
        print(f"Fetching APOD chunk {chunk_start} to {chunk_end}")
        params = {
            "api_key": API_KEY,
            "start_date": chunk_start,
            "end_date": chunk_end,
        }

        try:
            response = make_request(API_URL, params=params, timeout=60)
        except requests.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code == 429 and API_KEY == "DEMO_KEY":
                raise RuntimeError(
                    "NASA DEMO_KEY rate limit exceeded. "
                    "Use a smaller date range, wait for the limit to reset, or set NASA_API_KEY to a personal key."
                ) from exc
            raise
        data = response.json()

        if isinstance(data, dict):
            data = [data]

        items.extend(data)

    return items


def main():
    print(f"Fetching APOD items from {START_DATE} to {END_DATE}")
    items = fetch_apod_items()

    # Build the final dataset with stable sequential IDs.
    dataset = []
    for item in items:
        if not is_valid_item(item):
            continue

        dataset.append(build_record(item, len(dataset) + 1))

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(dataset, f, indent=2, ensure_ascii=False)

    print(f"Done. Saved {len(dataset)} items to {OUTPUT_JSON}")


if __name__ == "__main__":
    main()
