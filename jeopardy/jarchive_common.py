import json
import re
from pathlib import Path
import sys
from urllib.parse import parse_qs, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from asset_localization import download_assets


BASE_URL = "https://www.j-archive.com"
IMAGES_DIR = ROOT_DIR / "dataset/images/jeopardy"
REQUEST_TIMEOUT_SECONDS = 60
MEDIA_EXTENSIONS = (".jpg", ".jpeg", ".png", ".gif", ".webp", ".mp3", ".wav")
VALID_MEDIA_CONTENT_PREFIXES = ("image/", "audio/")

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": "JArchiveScraper/1.0",
        "Accept-Language": "en-US,en;q=0.9",
    }
)


def clean_text(value: str | None) -> str:
    return " ".join((value or "").split()).strip()


def parse_show_metadata(soup: BeautifulSoup) -> tuple[str | None, str | None]:
    heading = clean_text(soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else "")
    match = re.match(r"Show #(?P<show>\d+)\s*-\s*(?P<air_date>.+)$", heading)
    if not match:
        return None, None

    return match.group("show"), match.group("air_date")


def extract_answer(answer_cell: BeautifulSoup | None) -> str | None:
    if not answer_cell:
        return None

    answer_tag = answer_cell.find("em", class_="correct_response")
    if not answer_tag:
        return None

    answer = clean_text(answer_tag.get_text(" ", strip=True))
    return answer or None


def extract_media_urls(clue_cell: BeautifulSoup, page_url: str) -> list[str]:
    media_urls = []

    for link in clue_cell.find_all("a", href=True):
        href = clean_text(link["href"])
        if not href or href.startswith("#") or href.lower().startswith("javascript:"):
            continue

        full_url = urljoin(page_url, href)
        full_url = normalize_media_url(full_url)
        lower = full_url.lower()

        if "/media/" not in lower and not lower.endswith(MEDIA_EXTENSIONS):
            continue

        if full_url not in media_urls:
            media_urls.append(full_url)

    return media_urls


def normalize_media_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.netloc in {"j-archive.com", "www.j-archive.com"} and parsed.scheme == "http":
        return parsed._replace(scheme="https").geturl()
    return url


def is_valid_media_url(url: str) -> bool:
    try:
        response = SESSION.get(url, timeout=REQUEST_TIMEOUT_SECONDS, stream=True)
        response.raise_for_status()
    except requests.RequestException:
        return False
    finally:
        if "response" in locals():
            response.close()

    content_type = response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
    if content_type:
        return content_type.startswith(VALID_MEDIA_CONTENT_PREFIXES)

    return url.lower().endswith(MEDIA_EXTENSIONS)


def filter_valid_media_urls(urls: list[str]) -> list[str]:
    valid_urls = []
    for url in urls:
        if is_valid_media_url(url):
            valid_urls.append(url)
        else:
            print(f"Skipping invalid media URL: {url}")
    return valid_urls


def scrape_jarchive_game(page_url: str, *, media_only: bool = False, validate_media_urls: bool = True) -> list[dict]:
    print(f"Downloading Jeopardy page: {page_url}")
    response = SESSION.get(page_url, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    show_number, air_date = parse_show_metadata(soup)
    game_id = parse_qs(urlparse(page_url).query).get("game_id", [None])[0]

    records = []
    clue_id = 1

    for clue_cell in soup.find_all("td", class_="clue_text"):
        cell_id = clean_text(clue_cell.get("id"))
        if not cell_id.startswith("clue_") or cell_id.endswith("_r"):
            continue

        question = clean_text(clue_cell.get_text(" ", strip=True))
        if not question:
            continue

        media_urls = extract_media_urls(clue_cell, page_url)
        if media_urls and validate_media_urls:
            media_urls = filter_valid_media_urls(media_urls)
        if media_only and not media_urls:
            continue

        answer_cell = soup.find("td", id=f"{cell_id}_r")
        answer = extract_answer(answer_cell)

        source = {
            "platform": "Jeopardy",
            "url": page_url,
        }
        if show_number:
            source["show_number"] = show_number
        if air_date:
            source["air_date"] = air_date
        if game_id:
            source["game_id"] = game_id
        if media_urls:
            source["media_urls"] = media_urls

        records.append(
            {
                "id": clue_id,
                "question": question,
                "answer": answer,
                "media_url": media_urls[0] if media_urls else None,
                "source": source,
            }
        )
        clue_id += 1

    return records


def save_records(records: list[dict], output_path: Path) -> None:
    for record in records:
        media_urls = list(record.get("source", {}).get("media_urls", []))
        if not media_urls:
            continue

        record["source_media_url"] = record.get("media_url")
        local_media_paths = download_assets(media_urls, IMAGES_DIR, f"{output_path.stem}_{record['id']}")
        if local_media_paths:
            record["media_url"] = local_media_paths[0]
            record["source"]["local_media_paths"] = local_media_paths

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w") as file:
        json.dump(records, file, indent=4)

    print(f"Saved {len(records)} clues to {output_path}")
