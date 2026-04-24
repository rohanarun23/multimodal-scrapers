import json
import os
import re
from pathlib import Path
from urllib.parse import urlencode

import requests


ROOT_DIR = Path(__file__).resolve().parents[1]
OUTPUT_JSON = ROOT_DIR / "dataset/quizbowl_tossups.json"
API_BASE_URL = "https://www.qbreader.org/api"
REQUEST_TIMEOUT_SECONDS = 30
DEFAULT_SET_NAMES = [
    "2025 ACF Winter",
    "2024 ACF Fall",
    "2023 ACF Regionals",
]
DEFAULT_MAX_PACKETS_PER_SET = 2
CONTACT_EMAIL = os.getenv("SCRAPER_CONTACT_EMAIL", "rohanarun@users.noreply.github.com").strip()
if CONTACT_EMAIL == "your-email@example.com":
    CONTACT_EMAIL = "rohanarun@users.noreply.github.com"

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": f"QuizBowlTossupScraper/1.0 (contact: {CONTACT_EMAIL})",
        "Accept": "application/json",
        "Referer": "https://www.qbreader.org/",
    }
)


def clean_text(value: str) -> str:
    without_tags = re.sub(r"<[^>]+>", "", value or "")
    return " ".join(without_tags.replace("\xa0", " ").split())


def clean_answer(value: str) -> str:
    cleaned = clean_text(value)
    cleaned = re.sub(r"\s*<[^>]+>\s*$", "", cleaned)
    return cleaned.strip(" -")


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return slug or "quizbowl"


def get_json(path: str, params: dict[str, str | int]) -> dict:
    response = SESSION.get(f"{API_BASE_URL}/{path}", params=params, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.json()


def load_set_names() -> list[str]:
    configured = os.getenv("QUIZBOWL_SET_NAMES", "").strip()
    if not configured:
        return DEFAULT_SET_NAMES
    return [name.strip() for name in configured.split(",") if name.strip()]


def load_max_packets_per_set() -> int:
    raw_value = os.getenv("QUIZBOWL_MAX_PACKETS_PER_SET", str(DEFAULT_MAX_PACKETS_PER_SET)).strip()
    try:
        return max(1, int(raw_value))
    except ValueError as exc:
        raise ValueError(f"QUIZBOWL_MAX_PACKETS_PER_SET must be an integer, got {raw_value!r}") from exc


def build_packet_api_url(set_name: str, packet_number: int) -> str:
    query = urlencode(
        {
            "setName": set_name,
            "packetNumber": packet_number,
            "questionTypes": "tossups",
        }
    )
    return f"{API_BASE_URL}/packet?{query}"


def fetch_tossups_for_set(set_name: str, max_packets_per_set: int) -> list[dict]:
    num_packets = get_json("num-packets", {"setName": set_name}).get("numPackets", 0)
    if not num_packets:
        raise ValueError(f"No packets reported for set {set_name!r}")

    packet_limit = min(num_packets, max_packets_per_set)
    tossups: list[dict] = []

    for packet_number in range(1, packet_limit + 1):
        packet_response = get_json(
            "packet",
            {
                "setName": set_name,
                "packetNumber": packet_number,
                "questionTypes": "tossups",
            },
        )
        tossups.extend(packet_response.get("tossups", []))

    return tossups


def build_dataset() -> list[dict]:
    dataset = []
    set_names = load_set_names()
    max_packets_per_set = load_max_packets_per_set()

    for set_name in set_names:
        print(f"Fetching tossups from {set_name}")
        tossups = fetch_tossups_for_set(set_name, max_packets_per_set)

        for tossup in tossups:
            packet = tossup.get("packet", {})
            packet_number = packet.get("number")
            question_number = tossup.get("number")
            question_id = (
                f"{slugify(set_name)}_p{int(packet_number):02d}_q{int(question_number):02d}"
                if packet_number and question_number
                else tossup.get("_id", f"quizbowl_{len(dataset) + 1:04d}")
            )

            dataset.append(
                {
                    "id": question_id,
                    "question": clean_text(tossup.get("question_sanitized") or tossup.get("question", "")),
                    "answer": clean_answer(tossup.get("answer_sanitized") or tossup.get("answer", "")),
                    "source": "qbreader",
                    "source_url": build_packet_api_url(set_name, int(packet_number)) if packet_number else "",
                    "quiz_title": set_name,
                    "category": clean_text(tossup.get("category", "")),
                    "subcategory": clean_text(tossup.get("subcategory", "")),
                    "difficulty": tossup.get("difficulty"),
                    "packet_name": clean_text(packet.get("name", "")),
                    "packet_number": packet_number,
                    "question_number": question_number,
                    "qbreader_tossup_id": tossup.get("_id", ""),
                    "updated_at": tossup.get("updatedAt", ""),
                    "source_meta": {
                        "platform": "QB Reader",
                        "upstream_source": "quizbowl packets",
                        "set_name": clean_text(set_name),
                        "packet_name": clean_text(packet.get("name", "")),
                        "packet_number": packet_number,
                        "question_type": "tossup",
                        "standard": tossup.get("set", {}).get("standard"),
                        "year": tossup.get("set", {}).get("year"),
                    },
                }
            )

    return dataset


def main() -> None:
    dataset = build_dataset()
    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(dataset, indent=4), encoding="utf-8")
    print(f"Saved {len(dataset)} questions to {OUTPUT_JSON}")


if __name__ == "__main__":
    main()
