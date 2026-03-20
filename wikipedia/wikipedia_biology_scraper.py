import json
import os
import time
from urllib.parse import urlparse

import requests
from requests import Response


# Reuse one session so headers and connection pooling stay consistent.
CONTACT_EMAIL = os.getenv("SCRAPER_CONTACT_EMAIL", "your-email@example.com").strip() or "your-email@example.com"
SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": (
            "WikipediaScraper/1.0 "
            f"(contact: {CONTACT_EMAIL})"
        ),
        "Referer": "https://en.wikipedia.org/",
    }
)

REQUEST_DELAY_SECONDS = 1.0
MAX_RETRIES = 4
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
THUMBNAIL_WIDTH = 1200
IMAGEINFO_BATCH_SIZE = 50

# Target pages for the biology image dataset.
PAGES = [
    "Cell_(biology)",
    "Mitochondrion",
    "DNA",
    "Neuron",
    "Photosynthesis"
]

OUTPUT_JSON = "dataset/wikipedia_biology.json"


QUESTION_TEMPLATES = {
    "Cell_(biology)": "Identify the biological structure shown in the image.",
    "Mitochondrion": "Which organelle is shown in the image?",
    "DNA": "What biological molecule is shown in the image?",
    "Neuron": "What type of cell is shown in the image?",
    "Photosynthesis": "What biological process is illustrated in the image?"
}


ANSWER_MAP = {
    "Cell_(biology)": "Cell",
    "Mitochondrion": "Mitochondrion",
    "DNA": "DNA",
    "Neuron": "Neuron",
    "Photosynthesis": "Photosynthesis"
}


def make_request(url: str, *, params=None, timeout: int = 20, stream: bool = False, headers=None) -> Response:
    # Retry temporary request failures before raising an error.
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = SESSION.get(url, params=params, timeout=timeout, stream=stream, headers=headers)
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
            wait_seconds = float(retry_after) if retry_after and retry_after.isdigit() else attempt * 2
            print(f"Retryable response {response.status_code} for {response.url}. Waiting {wait_seconds:.1f}s.")
            response.close()
            time.sleep(wait_seconds)
            continue

        response.raise_for_status()
        time.sleep(REQUEST_DELAY_SECONDS)
        return response

    raise last_error


def get_wikipedia_images(page_title: str):
    # Get all image titles linked from a Wikipedia page.
    url = "https://en.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "titles": page_title,
        "prop": "images",
        "format": "json",
        "imlimit": "max"
    }

    response = make_request(
        url,
        params=params,
        timeout=20,
        headers={"Accept": "application/json"},
    )
    data = response.json()

    pages = data["query"]["pages"]
    page_data = next(iter(pages.values()))
    return page_data.get("images", [])


def get_image_infos(image_titles: list[str]) -> dict[str, dict]:
    # Fetch image URLs and licensing data in batches.
    url = "https://en.wikipedia.org/w/api.php"
    image_info_by_title = {}

    for start in range(0, len(image_titles), IMAGEINFO_BATCH_SIZE):
        batch = image_titles[start:start + IMAGEINFO_BATCH_SIZE]
        params = {
            "action": "query",
            "titles": "|".join(batch),
            "prop": "imageinfo",
            "iiprop": "url|extmetadata",
            "iiurlwidth": THUMBNAIL_WIDTH,
            "format": "json"
        }

        response = make_request(
            url,
            params=params,
            timeout=20,
            headers={"Accept": "application/json"},
        )
        data = response.json()

        for image_data in data["query"]["pages"].values():
            image_title = image_data.get("title")
            imageinfo = image_data.get("imageinfo", [])
            if not image_title or not imageinfo:
                continue

            info = imageinfo[0]
            metadata = info.get("extmetadata", {})
            image_info_by_title[image_title] = {
                "url": info.get("thumburl") or info.get("url"),
                "original_url": info.get("url"),
                "caption": metadata.get("ImageDescription", {}).get("value", ""),
                "license": metadata.get("LicenseShortName", {}).get("value", "")
            }

    return image_info_by_title


def is_valid_image(url: str) -> bool:
    # Skip non-image files and decorative assets.
    if not url:
        return False

    lower = urlparse(url).path.lower()

    valid_exts = (".jpg", ".jpeg", ".png", ".svg", ".webp")
    bad_keywords = ["icon", "logo", "symbol", "disambig"]

    if not lower.endswith(valid_exts):
        return False

    if any(word in lower for word in bad_keywords):
        return False

    return True


def main():
    # Build the dataset with sequential IDs across all target pages.
    dataset = []
    counter = 1

    for page in PAGES:
        print(f"Scraping page: {page}")
        try:
            images = get_wikipedia_images(page)
        except requests.RequestException as e:
            print(f"Failed to fetch images for page {page}: {e}")
            continue

        image_titles = [img.get("title") for img in images if img.get("title")]

        try:
            image_info_by_title = get_image_infos(image_titles)
        except requests.RequestException as e:
            print(f"Failed to fetch metadata for page {page}: {e}")
            continue

        for image_title in image_titles:
            info = image_info_by_title.get(image_title)

            if not info or not is_valid_image(info["url"]):
                continue

            item_id = f"wiki_bio_{counter:03d}"

            dataset.append({
                "id": item_id,
                "question": QUESTION_TEMPLATES[page],
                "answer": ANSWER_MAP[page],
                "source": "wikipedia",
                "source_page": page,
                "image_url": info["url"],
                "original_image_url": info["original_url"],
                "caption": info["caption"],
                "license": info["license"]
            })

            print(f"Saved {item_id}")
            counter += 1

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(dataset, f, indent=2, ensure_ascii=False)

    print(f"Done. Saved {len(dataset)} items to {OUTPUT_JSON}")


if __name__ == "__main__":
    main()
