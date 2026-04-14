import ast
import json
import re
from pathlib import Path
import sys

import requests
from bs4 import BeautifulSoup

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from asset_localization import download_asset


IMAGES_DIR = ROOT_DIR / "dataset/images/sporcle"
REQUEST_TIMEOUT_SECONDS = 60

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/143.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }
)


def extract_json_literal(html: str, variable_name: str) -> list:
    match = re.search(rf"var {variable_name} = (\[.*?\]);", html, re.S)
    if not match:
        raise ValueError(f"Could not find JavaScript array for {variable_name}")

    return json.loads(match.group(1).replace("\\/", "/"))


def extract_optional_json_literal(html: str, variable_name: str) -> list:
    match = re.search(rf"var {variable_name} = (\[.*?\]);", html, re.S)
    if not match:
        return []

    return json.loads(match.group(1).replace("\\/", "/"))


def extract_dict_literal(html: str, variable_name: str) -> dict[str, str]:
    match = re.search(rf"var {variable_name} = (\{{.*?\}});", html, re.S)
    if not match:
        raise ValueError(f"Could not find JavaScript object for {variable_name}")

    return json.loads(match.group(1).replace("\\/", "/"))


def extract_double_quoted_assignment(html: str, assignment_name: str) -> str | None:
    match = re.search(rf"{assignment_name}\s*=\s*(\"(?:\\.|[^\"\\])*\")\s*;", html)
    if not match:
        return None

    return json.loads(match.group(1))


def extract_single_quoted_assignment(html: str, assignment_name: str) -> str | None:
    match = re.search(rf"{assignment_name}\s*=\s*('(?:\\.|[^'\\])*')\s*;", html)
    if not match:
        return None

    return ast.literal_eval(match.group(1))


def extract_embedded_description(html: str) -> str | None:
    match = re.search(r"description\s*:\s*('(?:\\.|[^'\\])*')", html)
    if match:
        return ast.literal_eval(match.group(1))

    soup = BeautifulSoup(html, "html.parser")
    meta = soup.find("meta", attrs={"name": "description"})
    if not meta:
        return None

    content = meta.get("content")
    if not content:
        return None

    return content.replace(" Play this fun quiz and test your trivia knowledge.", "").strip()


def extract_integer_assignment(html: str, assignment_name: str) -> int | None:
    match = re.search(rf"{assignment_name}\s*=\s*(\d+)\s*;", html)
    if not match:
        return None

    return int(match.group(1))


def decode_answer_variants(encoded_variants: list[str], cipher: dict[str, str]) -> list[str]:
    inverse_cipher = {value: key for key, value in cipher.items()}
    decoded_variants: list[str] = []
    seen_normalized: set[str] = set()

    for encoded_variant in encoded_variants:
        decoded = "".join(inverse_cipher.get(character, character) for character in encoded_variant).strip()
        if not decoded:
            continue

        normalized = decoded.casefold()
        if normalized in seen_normalized:
            continue

        seen_normalized.add(normalized)
        decoded_variants.append(decoded)

    return decoded_variants


def scrape_sporcle_quiz(page_url: str) -> list[dict]:
    print(f"Downloading Sporcle page: {page_url}")
    response = SESSION.get(page_url, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    html = response.text

    quiz_title = extract_double_quoted_assignment(html, r"Sporcle\.gameData\.name")
    quiz_description = extract_embedded_description(html)
    creator_handle = extract_single_quoted_assignment(html, r"var creatorHandle")
    encoded_game_id = extract_single_quoted_assignment(html, r"var encodedGameID")
    game_id = extract_integer_assignment(html, r"Sporcle\.gameData\.gameID")

    answers = extract_json_literal(html, "answers")
    pics = extract_optional_json_literal(html, "pics")
    image_sources = extract_optional_json_literal(html, "imgSources")
    extras = extract_optional_json_literal(html, "extras")
    cipher = extract_dict_literal(html, "asta")

    records = []
    total_items = max(len(answers), len(pics))

    for index in range(total_items):
        encoded_answer_variants = answers[index] if index < len(answers) else []
        accepted_answers = decode_answer_variants(encoded_answer_variants, cipher)
        answer = accepted_answers[0] if accepted_answers else None

        source = {
            "platform": "Sporcle",
            "url": page_url,
            "quiz_title": quiz_title,
            "quiz_type": "Slideshow",
        }
        if creator_handle:
            source["creator_handle"] = creator_handle
        if encoded_game_id:
            source["encoded_game_id"] = encoded_game_id
        if game_id is not None:
            source["game_id"] = game_id
        if index < len(image_sources) and image_sources[index]:
            source["image_source_url"] = image_sources[index]

        record = {
            "id": index + 1,
            "question": quiz_description or f"Identify the answer from the image in {quiz_title}.",
            "quiz_title": quiz_title,
            "media_url": pics[index] if index < len(pics) else None,
            "answer": answer,
            "source": source,
        }

        if len(accepted_answers) > 1:
            record["accepted_answers"] = accepted_answers

        if index < len(extras) and extras[index]:
            record["context"] = extras[index]

        records.append(record)

    return records


def save_records(records: list[dict], output_path: Path) -> None:
    for record in records:
        remote_media_url = record.get("media_url")
        if not remote_media_url:
            continue

        record["source_media_url"] = remote_media_url
        local_media_path = download_asset(remote_media_url, IMAGES_DIR, f"{output_path.stem}_{record['id']:03d}")
        if local_media_path:
            record["media_url"] = local_media_path

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w") as file:
        json.dump(records, file, indent=4)

    print(f"Saved {len(records)} Sporcle clues to {output_path}")


def scrape_and_save_quiz(page_url: str, output_path: Path) -> None:
    records = scrape_sporcle_quiz(page_url)
    save_records(records, output_path)
