import json
import os
import re
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup


# Base URL, starting pages, and output path for the Space Place scraper.
BASE_URL = "https://spaceplace.nasa.gov"
START_PAGES = [
    "https://spaceplace.nasa.gov/menu/earth/",
    "https://spaceplace.nasa.gov/menu/solar-system/",
]

OUTPUT_JSON = "dataset/nasa_spaceplace_questions.json"
CONTACT_EMAIL = os.getenv("SCRAPER_CONTACT_EMAIL", "your-email@example.com").strip() or "your-email@example.com"

# Reuse one session so request headers stay consistent.
SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": f"NASASpacePlaceScraper/1.0 (contact: {CONTACT_EMAIL})",
        "Accept-Language": "en-US,en;q=0.9",
    }
)
MAX_WORKERS = 8
IDENTIFYING_QUESTION = "What NASA Space Place topic is shown in this image?"


def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def fetch_html(url: str) -> BeautifulSoup:
    resp = SESSION.get(url, timeout=30)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")


def extract_article_links(index_url: str) -> list[str]:
    # Collect article-like links from a Space Place menu page.
    soup = fetch_html(index_url)
    links = set()

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        full_url = urljoin(index_url, href)

        if not full_url.startswith(BASE_URL):
            continue

        bad_patterns = [
            "/menu/",
            "/search/",
            ".pdf",
            "#",
            "mailto:",
        ]
        if any(bp in href for bp in bad_patterns):
            continue

        parsed = urlparse(full_url)
        path = parsed.path

        if path.count("/") >= 2 and path.endswith("/"):
            links.add(full_url)

    return sorted(links)


def should_skip_title(title: str) -> bool:
    # Exclude activity pages that do not map cleanly to a single answer.
    title = clean_text(title)
    lower = title.lower()

    bad_prefixes = (
        "get ",
        "make ",
        "build ",
        "play ",
        "color ",
        "draw ",
        "do ",
        "try ",
        "launch ",
    )
    bad_keywords = (
        "game",
        "activity",
        "experiment center",
        "video",
        "quiz",
        "puzzle",
        "bingo",
        "mask",
        "mobile",
        "fan",
        "cookies",
        "spacecraft!",
    )

    return lower.startswith(bad_prefixes) or any(keyword in lower for keyword in bad_keywords)


def strip_leading_article(text: str) -> str:
    return re.sub(r"^(a|an|the)\s+", "", text, flags=re.IGNORECASE).strip()


def derive_answer_from_title(title: str) -> str | None:
    # Convert common article title patterns into a short answer.
    title = clean_text(title).rstrip()
    lower = title.lower()

    if should_skip_title(title):
        return None

    patterns = [
        r"^All About (.+)$",
        r"^What Is (.+)\??$",
        r"^What Are (.+)\??$",
        r"^What causes (.+)\??$",
        r"^Why Are (.+)\??$",
        r"^Why Do We Care About (.+)\??$",
        r"^How Long is (.+)\??$",
        r"^How Does (.+) Work\??$",
        r"^The Mars Rovers:\s*(.+)$",
        r"^Voyager 1 and 2:\s*(.+)$",
    ]

    for pattern in patterns:
        match = re.match(pattern, title, flags=re.IGNORECASE)
        if match:
            answer = strip_leading_article(clean_text(match.group(1)).rstrip("!?"))
            return answer or None

    cleaned_title = title.rstrip("!?")
    if ":" in cleaned_title:
        answer = clean_text(cleaned_title.split(":")[-1]).rstrip("!?")
        if answer:
            return answer

    if lower.startswith(("what ", "why ", "how ", "where ")):
        return None

    return cleaned_title or None


def extract_title(soup: BeautifulSoup) -> str | None:
    # Prefer the page heading, then fall back to the document title.
    h1 = soup.find("h1")
    if h1:
        return clean_text(h1.get_text(" ", strip=True))

    if soup.title:
        title = clean_text(soup.title.get_text(" ", strip=True))
        title = re.sub(r"\s*\|\s*NASA.*$", "", title)
        return title

    return None


def extract_hero_image_url(soup: BeautifulSoup, page_url: str) -> str | None:
    # Score likely article images and return the best candidate.
    def is_bad_image(url: str) -> bool:
        lower = url.lower()
        bad_keywords = [
            "logo",
            "icon",
            "sprite",
            "social",
            "seal",
            "banner",
            "favicon",
            "resources/homepage",
            "nasa.png",
        ]
        return any(keyword in lower for keyword in bad_keywords)

    candidates = []

    article = soup.find("article")
    main = soup.find("main")
    containers = [container for container in (article, main, soup) if container]

    for container in containers:
        for img in container.find_all("img", src=True):
            src = img.get("data-src") or img.get("data-lazy-src") or img.get("src")
            if not src:
                continue

            full = urljoin(page_url, src.strip())
            lower = full.lower()

            if is_bad_image(lower):
                continue

            if not lower.endswith((".jpg", ".jpeg", ".png", ".webp")):
                continue

            score = 0
            if container in (article, main):
                score += 2
            if img.get("alt"):
                score += 1
            if "figure" in " ".join(img.get("class", [])):
                score += 1

            width = img.get("width")
            height = img.get("height")
            try:
                if width and height and int(width) >= 250 and int(height) >= 250:
                    score += 2
            except ValueError:
                pass

            candidates.append((score, full))

    if candidates:
        candidates.sort(key=lambda item: item[0], reverse=True)
        return candidates[0][1]

    for meta_attrs in (
        {"property": "og:image"},
        {"name": "twitter:image"},
    ):
        meta = soup.find("meta", attrs=meta_attrs)
        if meta and meta.get("content"):
            full = urljoin(page_url, meta["content"])
            if not is_bad_image(full):
                return full

    return None


def infer_subject_from_url(url: str) -> str:
    if "/earth/" in url:
        return "earth_science"
    if "/solar-system/" in url:
        return "astronomy"
    return "space_science"


def parse_article(article_url: str, item_id: str) -> dict | None:
    # Build one dataset record from a Space Place article page.
    soup = fetch_html(article_url)

    title = extract_title(soup)
    if not title:
        return None

    answer = derive_answer_from_title(title)
    if not answer:
        return None

    image_url = extract_hero_image_url(soup, article_url)
    if not image_url:
        return None

    first_p = None
    for p in soup.find_all("p"):
        txt = clean_text(p.get_text(" ", strip=True))
        lower = txt.lower()
        if len(txt) > 40 and "click here to download this video" not in lower:
            first_p = txt
            break

    return {
        "id": item_id,
        "question": IDENTIFYING_QUESTION,
        "answer": answer,
        "source": "nasa_space_place",
        "source_url": article_url,
        "subject": infer_subject_from_url(article_url),
        "image_url": image_url,
        "context": first_p,
    }


def main():
    # Scan each start page, parse articles in parallel, and write the dataset.
    dataset = []
    seen_urls = set()
    counter = 1

    for start_page in START_PAGES:
        print(f"\nScanning index: {start_page}")
        links = extract_article_links(start_page)
        print(f"Found {len(links)} candidate links")

        unique_links = [link for link in links if link not in seen_urls]
        seen_urls.update(unique_links)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_link = {}

            for offset, link in enumerate(unique_links, start=counter):
                item_id = f"nasa_q_{offset:03d}"
                print(f"Queueing {link}")
                future = executor.submit(parse_article, link, item_id)
                future_to_link[future] = (link, item_id)

            for future in as_completed(future_to_link):
                link, item_id = future_to_link[future]
                try:
                    item = future.result()
                except Exception as e:
                    print(f"Error parsing {link}: {e}")
                    item = None

                if item:
                    dataset.append(item)
                    print(f"Saved {item_id}: {item['question']} -> {item['answer']}")

        counter += len(unique_links)

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(dataset, f, indent=2, ensure_ascii=False)

    print(f"\nDone. Saved {len(dataset)} items to {OUTPUT_JSON}")


if __name__ == "__main__":
    main()
