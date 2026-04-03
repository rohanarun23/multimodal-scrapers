import json
import os
import re
import time
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests import Response


# Base settings for the scraper run and output dataset.
BASE_URL = "https://bioart.niaid.nih.gov"
OUTPUT_JSON = "dataset/niaid_bioart_questions.json"
START_ID = 400
END_ID = 670
MAX_ITEMS = 250
MAX_CONSECUTIVE_MISSES = 15

MAX_RETRIES = 2
REQUEST_DELAY_SECONDS = 0.05
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
MAX_RETRY_WAIT_SECONDS = 1.0

CONTACT_EMAIL = os.getenv("SCRAPER_CONTACT_EMAIL", "your-email@example.com").strip() or "your-email@example.com"
MIN_CONTEXT_LENGTH = 40
BIOMEDICAL_SIGNAL_TERMS = (
    "cell",
    "protein",
    "virus",
    "viral",
    "bacteria",
    "bacterial",
    "immune",
    "antibody",
    "dna",
    "rna",
    "gene",
    "genetic",
    "pathogen",
    "disease",
    "molecule",
    "molecular",
    "organ",
    "tissue",
    "lab",
    "microscope",
    "therapeutic",
    "therapeutics",
    "parasite",
    "fung",
    "vaccine",
    "plasma",
    "lymph",
)
QUESTION_TEMPLATES = (
    "Which NIAID BioArt concept is the strongest inference when you combine the image with this catalog clue: {clue}",
    "The visual structure and the catalog text point to the same biomedical concept. Which title best explains both? Clue: {clue}",
    "If you reconcile the image with this redacted BioArt clue, which named biomedical structure or concept is the best fit? {clue}",
    "Which BioArt answer can you defend only after using both the image and this catalog clue: {clue}",
)

# Reuse one session so request headers and retries stay consistent.
SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": f"NIAIDBioArtScraper/1.0 (contact: {CONTACT_EMAIL})",
        "Accept-Language": "en-US,en;q=0.9",
    }
)


def make_request(url: str, *, timeout: int = 30) -> Response:
    # Retry short-lived request and server failures before giving up.
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = SESSION.get(url, timeout=timeout)
        except requests.RequestException as exc:
            last_error = exc
            if attempt == MAX_RETRIES:
                break

            wait_seconds = min(attempt * 2, MAX_RETRY_WAIT_SECONDS)
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


def clean_text(text: Any) -> str:
    return " ".join(str(text or "").split()).strip()


def stable_index(*parts: str, count: int) -> int:
    return sum(ord(char) for part in parts for char in part) % count


def fetch_html(url: str) -> BeautifulSoup | None:
    # Return parsed HTML only for pages that appear to be real records.
    try:
        response = make_request(url, timeout=45)
    except requests.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else None
        if status_code in {404, 500, 502, 503, 504}:
            print(f"Skipping {url} after repeated server errors ({status_code}).")
            return None
        raise

    if response.status_code == 404:
        return None
    if response.url.rstrip("/").endswith("/404"):
        return None
    return BeautifulSoup(response.text, "html.parser")


def text_from_meta(soup: BeautifulSoup, *, property_name: str | None = None, name: str | None = None) -> str | None:
    attrs = {}
    if property_name:
        attrs["property"] = property_name
    if name:
        attrs["name"] = name

    tag = soup.find("meta", attrs=attrs)
    if not tag:
        return None
    return clean_text(tag.get("content"))


def extract_title(soup: BeautifulSoup) -> str | None:
    # Prefer social metadata, then fall back to visible headings.
    for meta_name in ("og:title", "twitter:title"):
        if meta_name.startswith("og:"):
            meta_title = text_from_meta(soup, property_name=meta_name)
        else:
            meta_title = text_from_meta(soup, name=meta_name)
        meta_title = clean_text(meta_title)
        if meta_title and "An official website of the United States government" not in meta_title:
            return meta_title

    bad_prefixes = ("BIO ART", "SOURCE", "BIOART-")
    bad_contains = (
        "Creation Date",
        "Submission Date",
        "Modify Date",
        "Licensing:",
        "An official website of the United States government",
        "Here's how you know",
    )

    for tag in soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6"]):
        text = clean_text(tag.get_text(" ", strip=True))
        if not text:
            continue
        if any(text.upper().startswith(prefix) for prefix in bad_prefixes):
            continue
        if any(fragment in text for fragment in bad_contains):
            continue
        if re.fullmatch(r"\d+", text):
            continue
        return text

    return None


def extract_image_url(soup: BeautifulSoup) -> str | None:
    # Check metadata first, then scan page images and downloadable links.
    candidates = [
        text_from_meta(soup, property_name="og:image"),
        text_from_meta(soup, name="twitter:image"),
    ]

    for candidate in candidates:
        if candidate and is_valid_image_url(candidate):
            return candidate

    for img in soup.find_all("img", src=True):
        src = clean_text(img.get("src"))
        alt = clean_text(img.get("alt"))
        classes = " ".join(img.get("class", []))
        if not src:
            continue
        if "logo" in src.lower() or "icon" in src.lower():
            continue
        if alt.lower() in {"image", "indicator 1", "indicator 2", "indicator 3"}:
            continue
        if "thumbnail" in classes.lower():
            continue

        if src.startswith("http"):
            candidate = src
        else:
            candidate = f"{BASE_URL}{src}" if src.startswith("/") else f"{BASE_URL}/{src}"

        if is_valid_image_url(candidate):
            return candidate

    for anchor in soup.find_all("a", href=True):
        href = clean_text(anchor.get("href"))
        if not href:
            continue

        candidate = urljoin(BASE_URL, href)
        if is_valid_image_url(candidate):
            return candidate

    return None


def is_valid_image_url(url: str) -> bool:
    lower = url.lower()
    if "/api/bioarts/" in lower and "/files/" in lower:
        return True

    if not lower.endswith((".jpg", ".jpeg", ".png", ".webp", ".svg")):
        return False

    bad_keywords = ("logo", "favicon", "icon", "sprite")
    return not any(keyword in lower for keyword in bad_keywords)


def find_label_value(soup: BeautifulSoup, label: str) -> str | None:
    # Match both "Label: value" text and label/value blocks in the page layout.
    pattern = re.compile(rf"^{re.escape(label)}\s*:\s*(.+)$", flags=re.IGNORECASE)

    for text_node in soup.find_all(string=True):
        text = clean_text(text_node)
        if not text:
            continue

        inline_match = pattern.match(text)
        if inline_match:
            value = clean_text(inline_match.group(1))
            if value:
                return value

        if text.lower() == label.lower():
            current = text_node.parent if getattr(text_node, "parent", None) else None
            if current:
                sibling = current.find_next_sibling()
                while sibling:
                    sibling_text = clean_text(sibling.get_text(" ", strip=True))
                    if sibling_text and sibling_text.lower() != label.lower():
                        return sibling_text
                    sibling = sibling.find_next_sibling()

            next_text = text_node.find_next(string=True)
            if next_text:
                value = clean_text(next_text)
                if value and value.lower() != label.lower():
                    return value

    return None


def extract_description(soup: BeautifulSoup) -> str | None:
    description = find_label_value(soup, "Description")
    if description and description.lower() != "description":
        return description
    return text_from_meta(soup, name="description")


def extract_license(soup: BeautifulSoup) -> str | None:
    return find_label_value(soup, "Licensing")


def extract_keywords(soup: BeautifulSoup) -> str | None:
    return find_label_value(soup, "Keywords")


def extract_category(soup: BeautifulSoup) -> str | None:
    return find_label_value(soup, "Category")


def extract_credit(soup: BeautifulSoup) -> str | None:
    return find_label_value(soup, "Credit")


def is_valid_record(soup: BeautifulSoup) -> bool:
    # Keep only records with a title, an image, and an allowed license.
    title = extract_title(soup)
    image_url = extract_image_url(soup)
    license_name = (extract_license(soup) or "").lower()

    if not title or not image_url:
        return False

    if license_name and "public domain" not in license_name and "cc-by" not in license_name:
        return False

    title_lower = title.lower()
    bad_keywords = ("template", "poster", "graph", "symbol")
    if any(keyword in title_lower for keyword in bad_keywords):
        return False

    return True


def build_context(soup: BeautifulSoup) -> str | None:
    # Combine the most useful descriptive fields into one context string.
    parts = []
    for value in (extract_category(soup), extract_description(soup), extract_keywords(soup)):
        text = clean_text(value)
        if text and text not in parts:
            parts.append(text)
    return " | ".join(parts) if parts else None


def has_biomedical_signal(title: str | None, category: str | None, context: str | None) -> bool:
    combined = clean_text(" ".join(part for part in (title, category, context) if part)).lower()
    return any(term in combined for term in BIOMEDICAL_SIGNAL_TERMS)


def answer_aliases(answer: str) -> list[str]:
    aliases = {clean_text(answer)}
    aliases = {alias for alias in aliases if alias}
    return sorted(aliases, key=len, reverse=True)


def redact_answer_from_text(text: str, answer: str) -> str:
    redacted = clean_text(text)

    for alias in answer_aliases(answer):
        pattern = re.compile(rf"(?<![A-Za-z0-9]){re.escape(alias)}(?![A-Za-z0-9])", flags=re.IGNORECASE)
        redacted = pattern.sub("this concept", redacted)

    return clean_text(redacted)


def build_question(answer: str, context: str | None) -> str:
    segments = [clean_text(segment) for segment in clean_text(context).split("|") if clean_text(segment)]
    clue = ""
    ordered_segments = []

    if len(segments) >= 2:
        ordered_segments.append(segments[1])
    if len(segments) >= 3:
        ordered_segments.append(segments[2])
    if segments:
        ordered_segments.append(segments[0])

    for index, segment in enumerate(ordered_segments):
        candidate = redact_answer_from_text(segment, answer)
        min_length = 20 if index == 0 else MIN_CONTEXT_LENGTH
        if len(candidate) >= min_length:
            clue = candidate
            break

    if not clue and context:
        clue = redact_answer_from_text(context, answer)

    if len(clue) >= MIN_CONTEXT_LENGTH:
        template = QUESTION_TEMPLATES[stable_index(answer, clue, count=len(QUESTION_TEMPLATES))]
        return template.format(clue=clue)

    return "Which NIAID BioArt concept can you infer only after combining the image with the catalog text?"


def build_record(entry_id: int, soup: BeautifulSoup, index: int) -> dict:
    source_url = f"{BASE_URL}/bioart/{entry_id}"
    title = extract_title(soup)
    context = build_context(soup)

    return {
        "id": f"niaid_bioart_{index:03d}",
        "question": build_question(title, context),
        "answer": title,
        "source": "niaid_bioart",
        "source_url": source_url,
        "entry_id": f"BIOART-{entry_id:06d}",
        "image_url": extract_image_url(soup),
        "license": extract_license(soup),
        "category": extract_category(soup),
        "credit": extract_credit(soup),
        "context": context,
    }


def main():
    print(f"Scanning NIH BioArt Source IDs {START_ID} to {END_ID}")

    # Track duplicate images and stop after a long run of missing pages.
    dataset = []
    seen_images = set()
    consecutive_misses = 0

    for entry_id in range(START_ID, END_ID + 1):
        if len(dataset) >= MAX_ITEMS:
            break
        if consecutive_misses >= MAX_CONSECUTIVE_MISSES:
            print(f"Stopping after {consecutive_misses} consecutive misses.")
            break

        url = f"{BASE_URL}/bioart/{entry_id}"
        print(f"Fetching {url}")

        soup = fetch_html(url)
        if soup is None:
            consecutive_misses += 1
            continue

        if not is_valid_record(soup):
            consecutive_misses = 0
            continue

        item = build_record(entry_id, soup, len(dataset) + 1)
        if not has_biomedical_signal(item["answer"], item["category"], item["context"]):
            consecutive_misses = 0
            continue
        if item["image_url"] in seen_images:
            consecutive_misses = 0
            continue

        seen_images.add(item["image_url"])
        dataset.append(item)
        consecutive_misses = 0
        print(f"Saved {item['id']}: {item['answer']}")

    with open(OUTPUT_JSON, "w", encoding="utf-8") as handle:
        json.dump(dataset, handle, indent=2, ensure_ascii=False)

    print(f"Done. Saved {len(dataset)} items to {OUTPUT_JSON}")


if __name__ == "__main__":
    main()
