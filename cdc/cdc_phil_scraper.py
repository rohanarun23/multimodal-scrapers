import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup
from requests import Response

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from asset_localization import download_asset


BASE_URL = "https://wwwn.cdc.gov/phil"
OUTPUT_JSON = "dataset/cdc_phil_questions.json"
IMAGES_DIR = ROOT_DIR / "dataset/images/cdc"
START_ID = int(os.getenv("CDC_PHIL_START_ID", "10000"))
END_ID = int(os.getenv("CDC_PHIL_END_ID", "26000"))
MAX_ITEMS = int(os.getenv("CDC_PHIL_MAX_ITEMS", "200"))
MAX_CONSECUTIVE_MISSES = int(os.getenv("CDC_PHIL_MAX_CONSECUTIVE_MISSES", "400"))

MAX_RETRIES = 3
REQUEST_DELAY_SECONDS = 0.0
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
MAX_RETRY_WAIT_SECONDS = 3.0
MIN_CONTEXT_LENGTH = 60
MAX_WORKERS = int(os.getenv("CDC_PHIL_MAX_WORKERS", "16"))
BATCH_SIZE = int(os.getenv("CDC_PHIL_BATCH_SIZE", "128"))

CONTACT_EMAIL = os.getenv("SCRAPER_CONTACT_EMAIL", "your-email@example.com").strip() or "your-email@example.com"

SIGNAL_TERMS = (
    "bacter",
    "virus",
    "viral",
    "fung",
    "parasite",
    "pathogen",
    "disease",
    "infection",
    "syndrome",
    "vaccine",
    "cell",
    "tissue",
    "blood",
    "microscop",
    "electron micro",
    "lesion",
    "organ",
    "lung",
    "brain",
    "liver",
    "intestin",
    "skin",
    "mosquito",
    "tick",
    "flea",
    "public health",
)

SPECIFIC_TERM_HINTS = (
    "abscess",
    "actinomycosis",
    "antibody",
    "anthrax",
    "bacter",
    "blastomycosis",
    "cell",
    "cholera",
    "chromoblastomycosis",
    "covid",
    "dengue",
    "disease",
    "ebola",
    "erysipelas",
    "erythema",
    "escherichia",
    "favus",
    "fever",
    "fingerprinting",
    "fung",
    "genome",
    "hepatitis",
    "histoplasma",
    "influenza",
    "karyotype",
    "laboratory",
    "leishmania",
    "leprosy",
    "lesion",
    "louse",
    "malaria",
    "mosquito",
    "mycobacterium",
    "nodosum",
    "parasite",
    "pathogen",
    "phenytoin",
    "plague",
    "plasma",
    "protein",
    "rabies",
    "salmonella",
    "sequencing",
    "serologic",
    "staphylococcus",
    "streptococcus",
    "syndrome",
    "testing",
    "tissue",
    "tick",
    "vaccine",
    "vibrio",
    "virus",
    "yersinia",
)

QUESTION_TEMPLATES = (
    "Which CDC PHIL subject is the strongest inference when you combine the image with this caption clue: {clue}",
    "The visual evidence and the CDC caption point to the same clinical, laboratory, or public-health subject. Which subject best explains both? Clue: {clue}",
    "If you reconcile the image with this CDC caption clue, which diagnosis, organism, procedure, or public-health topic is the best fit? {clue}",
    "Which CDC PHIL answer can you justify only after using both the image and this caption clue: {clue}",
)

GENERIC_ANSWER_PATTERNS = (
    r"^(?:close|left|right|dorsal|ventral|anterior|posterior|superior|inferior|medial|lateral|oblique|full-body|fundoscopic|intraoral|transaxial)\b",
    r"\bview of\b",
    r"\bpresence of\b",
    r"\bprocess of\b",
    r"\bsymptoms of\b",
    r"\bsamples of\b",
    r"\bpiece of\b",
    r"\bpieces of\b",
    r"\bnumber of\b",
    r"\bfront of\b",
    r"\bfa[cç]ade of\b",
    r"\bexterior of\b",
)

EXACT_PHRASE_HINTS = (
    "whole genome sequencing",
    "dna fingerprinting",
    "pulsed field gel electrophoresis",
    "antimicrobial susceptibility testing",
    "sars-cov-2 antibodies",
    "oral cholera vaccination",
    "rapid health assessment survey",
    "karyotype",
    "chromosomal analysis",
    "serologic test",
    "vaccinations",
    "measles",
    "rabies",
    "phenytoin",
    "penicillin",
    "human herpesvirus-8",
    "cytomegalovirus",
    "carter memorial laboratory",
    "bacterial dna sequencing",
    "ixodidae",
)

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": f"CDCPHILScraper/1.0 (contact: {CONTACT_EMAIL})",
        "Accept-Language": "en-US,en;q=0.9",
    }
)


def make_request(url: str, *, timeout: int = 30) -> Response:
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = SESSION.get(url, timeout=timeout)
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
    return " ".join(str(value or "").split()).strip()


def fetch_html(entry_id: int) -> BeautifulSoup | None:
    url = f"{BASE_URL}/Details.aspx?pid={entry_id}"

    try:
        response = make_request(url, timeout=45)
    except requests.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else None
        if status_code in {404, 500, 502, 503, 504}:
            print(f"Skipping {url} after repeated server errors ({status_code}).")
            return None
        raise

    if "public health image library" not in response.text.lower():
        return None

    return BeautifulSoup(response.text, "html.parser")


def text_lines(soup: BeautifulSoup) -> list[str]:
    return [clean_text(line) for line in soup.get_text("\n").splitlines() if clean_text(line)]


def details_table_rows(soup: BeautifulSoup) -> dict[str, BeautifulSoup]:
    rows = {}

    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all("td", recursive=False)
            if len(cells) < 2:
                continue

            label = clean_text(cells[0].get_text(" ", strip=True)).rstrip(":")
            if label:
                rows[label] = cells[1]

    return rows


def find_label_value(soup: BeautifulSoup, label: str) -> str | None:
    rows = details_table_rows(soup)
    value_cell = rows.get(label)
    if value_cell is not None:
        value = clean_text(value_cell.get_text(" ", strip=True))
        if value:
            return value

    lines = text_lines(soup)
    inline_pattern = re.compile(rf"^{re.escape(label)}\s*:\s*(.+)$", flags=re.IGNORECASE)
    for line in lines:
        inline_match = inline_pattern.match(line)
        if inline_match:
            value = clean_text(inline_match.group(1))
            if value:
                return value
    return None


def extract_id(soup: BeautifulSoup) -> int | None:
    value = find_label_value(soup, "ID#")
    if value and value.isdigit():
        return int(value)
    return None


def extract_caption(soup: BeautifulSoup) -> str | None:
    return find_label_value(soup, "Caption")


def extract_copyright(soup: BeautifulSoup) -> str | None:
    return find_label_value(soup, "Copyright Restrictions")


def extract_content_provider(soup: BeautifulSoup) -> str | None:
    return find_label_value(soup, "Content Provider(s)")


def extract_creation_date(soup: BeautifulSoup) -> str | None:
    return find_label_value(soup, "Creation Date")


def is_valid_image_url(url: str) -> bool:
    lower = url.lower()
    if not lower.endswith((".jpg", ".jpeg", ".png", ".webp", ".gif", ".tif", ".tiff")):
        return False

    bad_keywords = ("logo", "icon", "banner", "sprite")
    return not any(keyword in lower for keyword in bad_keywords)


def extract_image_url(soup: BeautifulSoup) -> str | None:
    candidates = []

    for image_id in ("imgURL2", "imgURL1"):
        image = soup.find("img", id=image_id, src=True)
        if image:
            candidates.append(clean_text(image.get("src")))

    for anchor in soup.find_all("a", href=True):
        href = clean_text(anchor.get("href"))
        text = clean_text(anchor.get_text(" ", strip=True)).lower()
        if "resolution" not in text and "image" not in text:
            continue
        if href.startswith("/"):
            href = f"https://wwwn.cdc.gov{href}"
        elif href.startswith("Images") or href.startswith("images"):
            href = f"{BASE_URL}/{href.lstrip('/')}"
        candidates.append(href)

    for image in soup.find_all("img", src=True):
        src = clean_text(image.get("src"))
        if src.startswith("/"):
            src = f"https://wwwn.cdc.gov{src}"
        elif src.startswith("Images") or src.startswith("images"):
            src = f"{BASE_URL}/{src.lstrip('/')}"
        candidates.append(src)

    for candidate in candidates:
        if "phil_images" not in candidate.lower():
            continue
        if is_valid_image_url(candidate):
            return candidate

    return None


def has_signal(caption: str | None) -> bool:
    combined = clean_text(caption).lower()
    return any(term in combined for term in SIGNAL_TERMS)


def scientific_name_candidates(caption: str) -> list[str]:
    candidates = []
    seen = set()
    bad_prefixes = (
        "in this ",
        "included ",
        "please ",
        "under ",
        "this ",
        "these ",
        "image ",
        "figure ",
        "extracted ",
        "mixed ",
    )
    patterns = [
        r"\b([A-Z][a-z]+(?:-[A-Z][a-z]+)?\s[a-z][a-z0-9-]+(?:\s[a-z][a-z0-9-]+)?)\b",
        r"\b([A-Z]\.\s[a-z][a-z0-9-]+)\b",
    ]

    for pattern in patterns:
        for match in re.finditer(pattern, caption):
            candidate = clean_text(match.group(1).strip(" ,.;:"))
            candidate_lower = candidate.lower()
            if candidate_lower in seen:
                continue
            if candidate_lower.startswith(bad_prefixes):
                continue
            seen.add(candidate_lower)
            candidates.append(candidate)

    return candidates


def normalize_answer(answer: str) -> str:
    answer = clean_text(answer)
    answer = re.sub(
        r"\s+(bacterium|bacteria|virus|viruses|cell|cells|organism|organisms|gametocyte|trophozoite)$",
        "",
        answer,
        flags=re.IGNORECASE,
    )
    return clean_text(answer.strip(" ,.;:"))


def normalize_candidate(candidate: str) -> str:
    candidate = clean_text(candidate)
    candidate = re.sub(r"\s*\([^)]*arrowhead[^)]*\)", "", candidate, flags=re.IGNORECASE)
    candidate = re.sub(r"\s*\([^)]*see phil[^)]*\)", "", candidate, flags=re.IGNORECASE)
    candidate = re.sub(r"^(?:a|an|the)\s+", "", candidate, flags=re.IGNORECASE)
    candidate = re.sub(r"^(?:case of|diagnosis of)\s+", "", candidate, flags=re.IGNORECASE)

    for delimiter in (
        " due to ",
        " caused by ",
        " which ",
        " that ",
        " while ",
        " where ",
        " during ",
        " involving ",
        " revealing ",
        " highlighting ",
        " producing ",
        " allowing ",
        " located ",
        " placed ",
        " showing ",
        " demonstrating ",
        " containing ",
        " prepared ",
        " created ",
        " resulted ",
        " resulting ",
        " had been ",
        " was ",
        " were ",
    ):
        if delimiter in candidate.lower():
            candidate = candidate[:candidate.lower().index(delimiter)]
            break

    return normalize_answer(candidate)


def is_specific_answer(candidate: str) -> bool:
    if len(candidate) < 3:
        return False

    lower_candidate = candidate.lower()
    if lower_candidate in {"cdc", "public health", "clinical setting", "patient", "patients"}:
        return False

    if any(re.search(pattern, lower_candidate) for pattern in GENERIC_ANSWER_PATTERNS):
        return False

    generic_prefixes = (
        "this image",
        "these children",
        "this patient",
        "patient",
        "patients",
        "samples",
        "scientists",
        "public health scientists",
        "public health officials",
    )
    if lower_candidate.startswith(generic_prefixes):
        return False

    if len(candidate.split()) > 8:
        return False

    return True


def score_candidate(candidate: str, caption: str, base_score: int) -> tuple[int, int]:
    lower_candidate = candidate.lower()
    score = base_score

    if re.search(r"[A-Z][a-z]+\s[a-z][a-z0-9-]+", candidate):
        score += 8
    if any(hint in lower_candidate for hint in SPECIFIC_TERM_HINTS):
        score += 4
    if lower_candidate in caption.lower():
        score += 2
    if len(candidate.split()) <= 4:
        score += 1

    return score, len(candidate)


def pattern_candidates(caption: str) -> list[tuple[str, int]]:
    candidates = []

    patterns = (
        (r"\bdiagnosed as (?:a|an)? case of ([^,.]+)", 97),
        (r"\bdiagnosed as ([^,.]+)", 96),
        (r"\bdiagnosis of ([^,.]+) was made", 95),
        (r"\b(?:illness|condition) known as ([^,.]+)", 95),
        (r"\balso referred to as ([^,.]+)", 93),
        (r"\bconsistent with ([^,.]+)", 92),
        (r"\bassociated with ([^,.]+)", 91),
        (r"\bdue to what was determined to be a case of ([^,.]+)", 95),
        (r"\ba case of ([^,.]+)", 90),
        (r"\bcase of ([^,.]+)", 88),
        (r"\bsymptoms caused by (?:its|the) ([^,.]+) infection", 89),
        (r"\bdue to ([^,.]+ measles)", 89),
        (r"\badverse reaction to [^()]+\(([^)]+)\)", 90),
        (r"\bbelong to the tick family,\s*([A-Z][A-Za-z0-9-]+)", 88),
        (r"\bfront of (?:what was )?(?:the main building of )?(?:the )?([A-Z][^,.]+)", 86),
        (r"\b(?:test|testing) to identify the presence of ([^,.]+)", 91),
        (r"\btested for the presence of ([^,.]+)", 90),
        (r"\btested for ([^,.]+)", 88),
        (r"\bprepared to undergo,?\s+([^,.]+)", 87),
        (r"\bused to prepare [^,.]+ for ([^,.]+)", 86),
        (r"\bconducting a ([^,.]+)", 82),
        (r"\bnumbers of ([^,.]+?) particles", 83),
        (r"\bnumbers of [^,.]+,\s*([^,.]+?) virions", 83),
        (r"\bThis is a medical illustration of ([^,.]+)", 94),
        (r"\bThis image depicts an? ([^,.]+?)(?:, or [^,.]+,|, created by|, used to|, revealing|, highlighting|, producing|, allowing|, that had been|, which|\.|$)", 66),
        (r"\bThis image depicted an? ([^,.]+?)(?:, or [^,.]+,|, created by|, used to|, revealing|, highlighting|, producing|, allowing|, that had been|, which|\.|$)", 66),
        (r"\bThis image shows an? ([^,.]+?)(?:,|\.|$)", 64),
        (r"\bThis was an? ([^,.]+?)(?:,|\.|$)", 64),
    )

    for pattern, base_score in patterns:
        for match in re.finditer(pattern, caption, flags=re.IGNORECASE):
            candidate = normalize_candidate(match.group(1))
            if candidate and is_specific_answer(candidate):
                candidates.append((candidate, base_score))

    return candidates


def exact_phrase_candidates(caption: str) -> list[tuple[str, int]]:
    lower_caption = caption.lower()
    candidates = []

    for phrase in EXACT_PHRASE_HINTS:
        if phrase in lower_caption:
            start = lower_caption.index(phrase)
            candidate = normalize_candidate(caption[start:start + len(phrase)])
            if candidate and is_specific_answer(candidate):
                candidates.append((candidate, 85))

    return candidates


def choose_answer(caption: str) -> str | None:
    scientific_candidates = scientific_name_candidates(caption)
    scored_candidates = []

    for candidate in scientific_candidates:
        normalized = normalize_candidate(candidate)
        if normalized and is_specific_answer(normalized):
            scored_candidates.append((normalized, 100))

    scored_candidates.extend(pattern_candidates(caption))
    scored_candidates.extend(exact_phrase_candidates(caption))

    if not scored_candidates:
        return None

    ranked = sorted(
        {
            candidate: score_candidate(candidate, caption, base_score)
            for candidate, base_score in scored_candidates
        }.items(),
        key=lambda item: item[1],
        reverse=True,
    )
    return ranked[0][0]


def split_sentences(text: str) -> list[str]:
    return [segment.strip() for segment in re.split(r"(?<=[.!?])\s+", clean_text(text)) if segment.strip()]


def stable_index(*parts: str, count: int) -> int:
    return sum(ord(char) for part in parts for char in part) % count


def answer_aliases(answer: str) -> list[str]:
    aliases = {clean_text(answer)}

    if ":" in answer:
        left, right = answer.split(":", 1)
        aliases.add(clean_text(left))
        aliases.add(clean_text(right))

    aliases = {alias for alias in aliases if alias}
    return sorted(aliases, key=len, reverse=True)


def redact_answer_from_text(text: str, answer: str) -> str:
    redacted = clean_text(text)

    for alias in answer_aliases(answer):
        pattern = re.compile(rf"(?<![A-Za-z0-9]){re.escape(alias)}(?![A-Za-z0-9])", flags=re.IGNORECASE)
        redacted = pattern.sub("this subject", redacted)

    return clean_text(redacted)


def build_question(answer: str, context: str) -> str:
    clue_candidates = []

    for sentence in split_sentences(context):
        clue = redact_answer_from_text(sentence, answer)
        if len(clue) >= 45:
            clue_candidates.append(clue)

    clue = clue_candidates[0] if clue_candidates else redact_answer_from_text(context, answer)
    if len(clue) >= 45:
        template = QUESTION_TEMPLATES[stable_index(answer, clue, count=len(QUESTION_TEMPLATES))]
        return template.format(clue=clue)

    return (
        "Which CDC PHIL subject can you infer only after combining the image with the accompanying caption?"
    )


def is_valid_record(soup: BeautifulSoup) -> bool:
    caption = clean_text(extract_caption(soup))
    copyright_text = clean_text(extract_copyright(soup)).lower()
    image_url = extract_image_url(soup)

    if len(caption) < MIN_CONTEXT_LENGTH or not image_url:
        return False

    if "public domain" not in copyright_text and "no copyright restrictions" not in copyright_text:
        return False

    lower_caption = caption.lower()
    bad_keywords = ("logo", "chart", "graph", "poster", "seal", "map")
    if any(keyword in lower_caption for keyword in bad_keywords):
        return False

    return True


def build_record(entry_id: int, soup: BeautifulSoup, index: int) -> dict:
    caption = clean_text(extract_caption(soup))
    answer = choose_answer(caption)

    return {
        "id": f"cdc_phil_{index:03d}",
        "question": build_question(answer, caption) if answer else None,
        "answer": answer,
        "source": "cdc_phil",
        "source_url": f"{BASE_URL}/Details.aspx?pid={entry_id}",
        "entry_id": f"PHIL-{entry_id}",
        "image_url": extract_image_url(soup),
        "content_provider": extract_content_provider(soup),
        "creation_date": extract_creation_date(soup),
        "license": extract_copyright(soup),
        "context": caption,
    }


def process_entry(entry_id: int) -> dict | None:
    soup = fetch_html(entry_id)
    if soup is None or extract_id(soup) != entry_id:
        return None

    if not is_valid_record(soup):
        return None

    caption = clean_text(extract_caption(soup))
    if not has_signal(caption):
        return None

    item = build_record(entry_id, soup, 0)
    if not item["answer"]:
        return None

    return item


def iter_entry_batches() -> list[list[int]]:
    entry_ids = list(range(END_ID, START_ID - 1, -1))
    return [entry_ids[start:start + BATCH_SIZE] for start in range(0, len(entry_ids), BATCH_SIZE)]


def main() -> None:
    print(f"Scanning CDC PHIL IDs {END_ID} down to {START_ID} with {MAX_WORKERS} workers")

    dataset = []
    seen_images = set()
    consecutive_empty_batches = 0

    for batch_entry_ids in iter_entry_batches():
        if len(dataset) >= MAX_ITEMS:
            break
        if consecutive_empty_batches >= MAX_CONSECUTIVE_MISSES:
            print(f"Stopping after {consecutive_empty_batches} consecutive empty batches.")
            break

        print(f"Fetching batch {batch_entry_ids[0]} to {batch_entry_ids[-1]}")

        batch_items = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(process_entry, entry_id): entry_id for entry_id in batch_entry_ids}
            for future in as_completed(futures):
                entry_id = futures[future]
                try:
                    item = future.result()
                except requests.RequestException as exc:
                    print(f"Skipping {BASE_URL}/Details.aspx?pid={entry_id} after request failure: {exc}")
                    continue
                except Exception as exc:
                    print(f"Skipping {BASE_URL}/Details.aspx?pid={entry_id} after parse failure: {exc}")
                    continue

                if item:
                    batch_items.append(item)

        if not batch_items:
            consecutive_empty_batches += 1
            continue

        consecutive_empty_batches = 0
        batch_items.sort(key=lambda item: int(str(item["entry_id"]).split("-")[-1]), reverse=True)

        for item in batch_items:
            if len(dataset) >= MAX_ITEMS:
                break
            if item["image_url"] in seen_images:
                continue

            item["id"] = f"cdc_phil_{len(dataset) + 1:03d}"
            seen_images.add(item["image_url"])
            dataset.append(item)
            print(f"Saved {item['id']}: {item['answer']}")

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
