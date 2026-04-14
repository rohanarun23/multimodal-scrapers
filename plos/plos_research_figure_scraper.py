import json
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import date
from pathlib import Path
from typing import Any

import requests
from requests import Response

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from asset_localization import download_asset


SEARCH_URL = "https://api.plos.org/search"
OUTPUT_JSON = "dataset/plos_research_figure_questions.json"
IMAGES_DIR = ROOT_DIR / "dataset/images/plos"
CONTACT_EMAIL = os.getenv("SCRAPER_CONTACT_EMAIL", "your-email@example.com").strip() or "your-email@example.com"

TARGET_JOURNALS = (
    "PLOS Biology",
    "PLOS Computational Biology",
    "PLOS Genetics",
    "PLOS Medicine",
    "PLOS Neglected Tropical Diseases",
    "PLOS Pathogens",
)
JOURNAL_PATH_BY_CODE = {
    "pbio": "plosbiology",
    "pcbi": "ploscompbiol",
    "pgen": "plosgenetics",
    "pmed": "plosmedicine",
    "pntd": "plosntds",
    "pone": "plosone",
    "ppat": "plospathogens",
}

START_DATE = os.getenv("PLOS_START_DATE", "2022-01-01")
END_DATE = os.getenv("PLOS_END_DATE", date.today().isoformat())
MAX_ARTICLES = int(os.getenv("PLOS_MAX_ARTICLES", "40"))
MAX_ITEMS = int(os.getenv("PLOS_MAX_ITEMS", "150"))
MAX_FIGURES_PER_ARTICLE = int(os.getenv("PLOS_MAX_FIGURES_PER_ARTICLE", "4"))
SEARCH_ROWS = int(os.getenv("PLOS_SEARCH_ROWS", "20"))

MAX_RETRIES = 3
REQUEST_DELAY_SECONDS = 0.2
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
MAX_RETRY_WAIT_SECONDS = 6.0
MIN_ABSTRACT_LENGTH = 140
MIN_CAPTION_LENGTH = 90

QUESTION_TEMPLATES = (
    "Which peer-reviewed PLOS study title best matches the figure when you combine this figure clue with the abstract clue? Figure: {figure_clue} Abstract: {abstract_clue}",
    "The figure and abstract come from the same research article. Which study topic best explains both pieces of evidence? Figure clue: {figure_clue} Abstract clue: {abstract_clue}",
    "If you reconcile the visual evidence with these redacted clues from the figure legend and abstract, which PLOS article title is the best fit? Figure: {figure_clue} Abstract: {abstract_clue}",
    "Which PLOS research answer can you justify only after using both the figure legend and the abstract? Figure clue: {figure_clue} Abstract clue: {abstract_clue}",
)


SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": f"PLOSFigureScraper/1.0 (contact: {CONTACT_EMAIL})",
        "Accept-Language": "en-US,en;q=0.9",
    }
)


def make_request(url: str, *, params: dict[str, Any] | None = None, timeout: int = 45, headers: dict[str, str] | None = None) -> Response:
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = SESSION.get(url, params=params, timeout=timeout, headers=headers)
        except requests.RequestException as exc:
            last_error = exc
            if attempt == MAX_RETRIES:
                break

            wait_seconds = min(float(attempt), MAX_RETRY_WAIT_SECONDS)
            print(f"Request failed for {url}: {exc}. Retrying in {wait_seconds:.1f}s.")
            time.sleep(wait_seconds)
            continue

        if response.status_code in RETRY_STATUS_CODES:
            if attempt == MAX_RETRIES:
                response.raise_for_status()

            retry_after = response.headers.get("Retry-After")
            if retry_after and retry_after.isdigit():
                wait_seconds = min(float(retry_after), MAX_RETRY_WAIT_SECONDS)
            else:
                wait_seconds = min(float(attempt), MAX_RETRY_WAIT_SECONDS)

            print(f"Retryable response {response.status_code} for {response.url}. Waiting {wait_seconds:.1f}s.")
            response.close()
            time.sleep(wait_seconds)
            continue

        response.raise_for_status()
        time.sleep(REQUEST_DELAY_SECONDS)
        return response

    raise last_error


def clean_text(value: Any) -> str:
    if isinstance(value, (list, tuple)):
        value = " ".join(clean_text(item) for item in value)
    return " ".join(str(value or "").split()).strip()


def split_sentences(text: str) -> list[str]:
    return [segment.strip() for segment in re.split(r"(?<=[.!?])\s+", clean_text(text)) if segment.strip()]


def stable_index(*parts: str, count: int) -> int:
    return sum(ord(char) for part in parts for char in part) % count


def build_search_query(start_date: str, end_date: str) -> str:
    journal_query = " OR ".join(f'journal:"{journal}"' for journal in TARGET_JOURNALS)
    return (
        f"({journal_query}) "
        'AND article_type:"research article" '
        f"AND publication_date:[{start_date}T00:00:00Z TO {end_date}T23:59:59Z]"
    )


def normalize_publication_date(value: Any) -> str | None:
    text = clean_text(value)
    match = re.search(r"\d{4}-\d{2}-\d{2}", text)
    return match.group(0) if match else (text or None)


def article_docs() -> list[dict]:
    docs = []
    start = 0
    query = build_search_query(START_DATE, END_DATE)

    while len(docs) < MAX_ARTICLES:
        rows = min(SEARCH_ROWS, MAX_ARTICLES - len(docs))
        params = {
            "q": query,
            "fl": "id,journal,publication_date",
            "wt": "json",
            "sort": "publication_date desc",
            "rows": rows,
            "start": start,
        }
        response = make_request(SEARCH_URL, params=params, timeout=60, headers={"Accept": "application/json"})
        payload = response.json()
        batch = payload.get("response", {}).get("docs", [])
        if not batch:
            break

        docs.extend(batch)
        start += len(batch)

    return docs[:MAX_ARTICLES]


def journal_code_from_doi(doi: str) -> str | None:
    match = re.search(r"10\.1371/journal\.([a-z0-9]+)\.", doi, flags=re.IGNORECASE)
    return match.group(1).lower() if match else None


def journal_path_from_doi(doi: str) -> str | None:
    code = journal_code_from_doi(doi)
    if not code:
        return None
    return JOURNAL_PATH_BY_CODE.get(code)


def article_xml_url(journal_path: str, doi: str) -> str:
    return f"https://journals.plos.org/{journal_path}/article/file?id={doi}&type=manuscript"


def article_html_url(journal_path: str, doi: str) -> str:
    return f"https://journals.plos.org/{journal_path}/article?id={doi}"


def figure_image_url(journal_path: str, figure_id: str) -> str:
    return f"https://journals.plos.org/{journal_path}/article/figure/image?size=large&id={figure_id}"


def local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def element_text(element: ET.Element | None) -> str:
    if element is None:
        return ""
    return clean_text(" ".join(text for text in element.itertext()))


def first_child_text(element: ET.Element, name: str) -> str | None:
    for child in element:
        if local_name(child.tag) == name:
            text = element_text(child)
            if text:
                return text
    return None


def first_matching_text(root: ET.Element, name: str, *, attr_name: str | None = None, attr_value: str | None = None) -> str | None:
    for element in root.iter():
        if local_name(element.tag) != name:
            continue
        if attr_name and clean_text(element.get(attr_name)) != clean_text(attr_value):
            continue

        text = element_text(element)
        if text:
            return text
    return None


def extract_article_title(root: ET.Element) -> str | None:
    return first_matching_text(root, "article-title")


def extract_abstract(root: ET.Element) -> str | None:
    for element in root.iter():
        if local_name(element.tag) != "abstract":
            continue
        if clean_text(element.get("abstract-type")).lower() in {"graphical", "teaser", "video", "summary"}:
            continue

        text = element_text(element)
        if len(text) >= MIN_ABSTRACT_LENGTH:
            return text
    return None


def extract_license(root: ET.Element) -> str | None:
    return first_matching_text(root, "license-p")


def figure_doi_from_node(fig: ET.Element) -> str | None:
    for element in fig.iter():
        if local_name(element.tag) != "object-id":
            continue
        value = element_text(element)
        if value and ".g" in value.lower():
            return value
    return None


def derive_figure_doi(article_doi: str, label: str) -> str | None:
    if not label:
        return None

    if label.strip().lower().startswith("s"):
        return None

    match = re.search(r"(\d+)", label)
    if not match:
        return None

    return f"{article_doi}.g{int(match.group(1)):03d}"


def extract_figures(root: ET.Element, article_doi: str, journal_path: str) -> list[dict]:
    figures = []

    for fig in root.iter():
        if local_name(fig.tag) != "fig":
            continue

        label = first_child_text(fig, "label")
        caption = first_child_text(fig, "caption")
        if not label or not caption or len(caption) < MIN_CAPTION_LENGTH:
            continue
        if label.strip().lower().startswith("s"):
            continue

        figure_doi = figure_doi_from_node(fig) or derive_figure_doi(article_doi, label)
        if not figure_doi:
            continue

        figures.append(
            {
                "figure_label": label,
                "figure_doi": figure_doi,
                "caption": caption,
                "image_url": figure_image_url(journal_path, figure_doi),
            }
        )

    figures.sort(key=lambda item: int(re.search(r"(\d+)", item["figure_label"]).group(1)) if re.search(r"(\d+)", item["figure_label"]) else 9999)
    return figures


def normalize_answer(answer: str) -> str:
    answer = clean_text(answer)
    return answer.rstrip(" .")


def answer_aliases(answer: str) -> list[str]:
    aliases = {normalize_answer(answer)}

    for delimiter in (":", " - ", " – ", " — "):
        if delimiter in answer:
            left, right = answer.split(delimiter, 1)
            aliases.add(normalize_answer(left))
            aliases.add(normalize_answer(right))

    aliases = {alias for alias in aliases if len(alias) >= 6}
    return sorted(aliases, key=len, reverse=True)


def redact_answer_from_text(text: str, answer: str) -> str:
    redacted = clean_text(text)

    for alias in answer_aliases(answer):
        pattern = re.compile(rf"(?<![A-Za-z0-9]){re.escape(alias)}(?![A-Za-z0-9])", flags=re.IGNORECASE)
        redacted = pattern.sub("this study", redacted)

    return clean_text(redacted)


def choose_clue(text: str, answer: str, *, min_length: int) -> str | None:
    changed_clues = []
    fallback_clues = []

    for sentence in split_sentences(text):
        clue = redact_answer_from_text(sentence, answer)
        if len(clue) < min_length:
            continue

        fallback_clues.append(clue)
        if clue != clean_text(sentence):
            changed_clues.append(clue)

    if changed_clues:
        return max(changed_clues, key=len)
    if fallback_clues:
        return max(fallback_clues, key=len)

    clue = redact_answer_from_text(text, answer)
    return clue if len(clue) >= min_length else None


def build_question(answer: str, caption: str, abstract: str) -> str | None:
    figure_clue = choose_clue(caption, answer, min_length=50)
    abstract_clue = choose_clue(abstract, answer, min_length=70)
    if not figure_clue or not abstract_clue:
        return None

    template = QUESTION_TEMPLATES[stable_index(answer, figure_clue, abstract_clue, count=len(QUESTION_TEMPLATES))]
    return template.format(figure_clue=figure_clue, abstract_clue=abstract_clue)


def fetch_article_xml(journal_path: str, doi: str) -> ET.Element | None:
    url = article_xml_url(journal_path, doi)

    try:
        response = make_request(url, timeout=60, headers={"Accept": "application/xml,text/xml;q=0.9,*/*;q=0.8"})
    except requests.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else None
        if status_code in {404, 500, 502, 503, 504}:
            print(f"Skipping {url} after repeated server errors ({status_code}).")
            return None
        raise

    try:
        return ET.fromstring(response.text)
    except ET.ParseError as exc:
        print(f"Skipping {url} after XML parse failure: {exc}")
        return None


def build_article_records(doc: dict) -> list[dict]:
    doi = clean_text(doc.get("id"))
    journal_path = journal_path_from_doi(doi)
    if not doi or not journal_path:
        return []

    root = fetch_article_xml(journal_path, doi)
    if root is None:
        return []

    title = normalize_answer(extract_article_title(root) or "")
    abstract = clean_text(extract_abstract(root))
    license_text = clean_text(extract_license(root))
    if not title or len(abstract) < MIN_ABSTRACT_LENGTH:
        return []

    figure_records = []
    for figure in extract_figures(root, doi, journal_path)[:MAX_FIGURES_PER_ARTICLE]:
        question = build_question(title, figure["caption"], abstract)
        if not question:
            continue

        figure_records.append(
            {
                "id": "",
                "question": question,
                "answer": title,
                "source": "plos_research",
                "source_url": article_html_url(journal_path, doi),
                "image_url": figure["image_url"],
                "article_doi": doi,
                "journal": clean_text(doc.get("journal")) or journal_path,
                "publication_date": normalize_publication_date(doc.get("publication_date")),
                "figure_label": figure["figure_label"],
                "figure_doi": figure["figure_doi"],
                "license": license_text or "CC BY",
                "caption": figure["caption"],
                "context": abstract,
            }
        )

    return figure_records


def main() -> None:
    print(
        f"Searching PLOS research articles from {START_DATE} to {END_DATE} "
        f"across {len(TARGET_JOURNALS)} journals"
    )

    dataset = []
    seen_images = set()

    for doc in article_docs():
        if len(dataset) >= MAX_ITEMS:
            break

        doi = clean_text(doc.get("id"))
        print(f"Fetching figures for {doi}")

        try:
            article_items = build_article_records(doc)
        except requests.RequestException as exc:
            print(f"Skipping article {doi} after request failure: {exc}")
            continue
        except Exception as exc:
            print(f"Skipping article {doi} after parse failure: {exc}")
            continue

        for item in article_items:
            if len(dataset) >= MAX_ITEMS:
                break
            if item["image_url"] in seen_images:
                continue

            item["id"] = f"plos_fig_{len(dataset) + 1:03d}"
            seen_images.add(item["image_url"])
            dataset.append(item)
            print(f"Saved {item['id']}: {item['figure_label']} -> {item['answer']}")

    for item in dataset:
        remote_image_url = item.get("image_url")
        if not remote_image_url:
            continue

        item["source_image_url"] = remote_image_url
        local_image_path = download_asset(remote_image_url, IMAGES_DIR, item["id"])
        if local_image_path:
            item["image_url"] = local_image_path

    with open(OUTPUT_JSON, "w", encoding="utf-8") as handle:
        json.dump(dataset, handle, indent=2, ensure_ascii=False)

    print(f"Done. Saved {len(dataset)} items to {OUTPUT_JSON}")


if __name__ == "__main__":
    main()
