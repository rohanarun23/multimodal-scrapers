import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import requests

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from asset_localization import SESSION as ASSET_SESSION
from asset_localization import download_asset

OUTPUT_JSON = ROOT_DIR / "dataset/quizbowl_picture_rounds.json"
IMAGES_DIR = ROOT_DIR / "dataset/images/quizbowl"
WIKIPEDIA_API_URL = "https://en.wikipedia.org/w/api.php"
REQUEST_TIMEOUT_SECONDS = 30
REQUEST_DELAY_SECONDS = 0.75
DOWNLOAD_RETRIES = 4
THUMBNAIL_WIDTH = 1200
CONTACT_EMAIL = os.getenv("SCRAPER_CONTACT_EMAIL", "rohanarun@users.noreply.github.com").strip()
if CONTACT_EMAIL == "your-email@example.com":
    CONTACT_EMAIL = "rohanarun@users.noreply.github.com"

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": f"QuizBowlPictureRoundScraper/1.0 (contact: {CONTACT_EMAIL})",
        "Accept": "application/json",
        "Referer": "https://en.wikipedia.org/",
    }
)
ASSET_SESSION.headers.update({"User-Agent": f"QuizBowlPictureRoundScraper/1.0 (contact: {CONTACT_EMAIL})"})


@dataclass(frozen=True)
class PictureRoundItem:
    round_title: str
    question: str
    answer: str
    wikipedia_title: str
    accepted_answers: tuple[str, ...] = ()


PICTURE_ROUND_ITEMS = [
    PictureRoundItem(
        "Fine Arts Picture Round",
        "Identify this painting.",
        "Mona Lisa",
        "Mona Lisa",
        ("La Gioconda",),
    ),
    PictureRoundItem("Fine Arts Picture Round", "Identify this painting.", "The Starry Night", "The Starry Night"),
    PictureRoundItem("Fine Arts Picture Round", "Identify this painting.", "The Birth of Venus", "The Birth of Venus"),
    PictureRoundItem("Fine Arts Picture Round", "Identify this painting.", "Las Meninas", "Las Meninas"),
    PictureRoundItem(
        "Fine Arts Picture Round",
        "Identify this triptych.",
        "The Garden of Earthly Delights",
        "The Garden of Earthly Delights",
    ),
    PictureRoundItem("Fine Arts Picture Round", "Identify this fresco.", "The School of Athens", "The School of Athens"),
    PictureRoundItem("Fine Arts Picture Round", "Identify this painting.", "The Night Watch", "The Night Watch"),
    PictureRoundItem(
        "Fine Arts Picture Round",
        "Identify this painting.",
        "The Arnolfini Portrait",
        "Arnolfini Portrait",
        ("Portrait of Giovanni Arnolfini and his Wife",),
    ),
    PictureRoundItem(
        "Fine Arts Picture Round",
        "Identify this woodblock print.",
        "The Great Wave off Kanagawa",
        "The Great Wave off Kanagawa",
    ),
    PictureRoundItem(
        "Fine Arts Picture Round",
        "Identify this painting.",
        "Liberty Leading the People",
        "Liberty Leading the People",
    ),
    PictureRoundItem("Monuments and Sculpture Picture Round", "Identify this sculpture.", "David", "David (Michelangelo)"),
    PictureRoundItem("Monuments and Sculpture Picture Round", "Identify this sculpture.", "Venus de Milo", "Venus de Milo"),
    PictureRoundItem(
        "Monuments and Sculpture Picture Round",
        "Identify this sculpture group.",
        "Laocoon and His Sons",
        "Laocoon and His Sons",
    ),
    PictureRoundItem("Monuments and Sculpture Picture Round", "Identify this monument.", "Great Sphinx of Giza", "Great Sphinx of Giza"),
    PictureRoundItem("Monuments and Sculpture Picture Round", "Identify this building.", "Parthenon", "Parthenon"),
    PictureRoundItem("Monuments and Sculpture Picture Round", "Identify this building.", "Hagia Sophia", "Hagia Sophia"),
    PictureRoundItem("Monuments and Sculpture Picture Round", "Identify this basilica.", "Sagrada Familia", "Sagrada Familia"),
    PictureRoundItem("Monuments and Sculpture Picture Round", "Identify this amphitheatre.", "Colosseum", "Colosseum"),
    PictureRoundItem("Monuments and Sculpture Picture Round", "Identify this mausoleum.", "Taj Mahal", "Taj Mahal"),
    PictureRoundItem("Monuments and Sculpture Picture Round", "Identify this temple complex.", "Angkor Wat", "Angkor Wat"),
    PictureRoundItem("Science Picture Round", "Identify this molecule or structure.", "DNA", "DNA"),
    PictureRoundItem("Science Picture Round", "Identify this organelle.", "Mitochondrion", "Mitochondrion", ("Mitochondria",)),
    PictureRoundItem("Science Picture Round", "Identify this type of cell.", "Neuron", "Neuron"),
    PictureRoundItem("Science Picture Round", "Identify this scientific table.", "Periodic table", "Periodic table"),
    PictureRoundItem(
        "Science Picture Round",
        "Identify this astronomical diagram.",
        "Hertzsprung-Russell diagram",
        "Hertzsprung-Russell diagram",
    ),
    PictureRoundItem("Science Picture Round", "Identify this chronological scale.", "Geologic time scale", "Geologic time scale"),
    PictureRoundItem("Science Picture Round", "Identify this organ.", "Human brain", "Human brain", ("Brain",)),
    PictureRoundItem("Science Picture Round", "Identify this astronomical system.", "Solar System", "Solar System"),
    PictureRoundItem("Science Picture Round", "Identify this physics experiment.", "Double-slit experiment", "Double-slit experiment"),
    PictureRoundItem("Science Picture Round", "Identify this biological diagram type.", "Cladogram", "Cladogram"),
    PictureRoundItem("Geography Picture Round", "Identify this lake.", "Lake Baikal", "Lake Baikal"),
    PictureRoundItem("Geography Picture Round", "Identify this mountain.", "Mount Kilimanjaro", "Mount Kilimanjaro"),
    PictureRoundItem("Geography Picture Round", "Identify this strait.", "Strait of Gibraltar", "Strait of Gibraltar"),
    PictureRoundItem("Geography Picture Round", "Identify this river.", "Nile", "Nile"),
    PictureRoundItem("Geography Picture Round", "Identify this canyon.", "Grand Canyon", "Grand Canyon"),
    PictureRoundItem("Geography Picture Round", "Identify this rainforest.", "Amazon rainforest", "Amazon rainforest"),
    PictureRoundItem("Geography Picture Round", "Identify this desert.", "Sahara", "Sahara"),
    PictureRoundItem("Geography Picture Round", "Identify this reef system.", "Great Barrier Reef", "Great Barrier Reef"),
    PictureRoundItem("Geography Picture Round", "Identify this mountain range.", "Andes", "Andes"),
    PictureRoundItem("Geography Picture Round", "Identify this mountain range.", "Himalayas", "Himalayas"),
    PictureRoundItem("History and Artifacts Picture Round", "Identify this artifact.", "Rosetta Stone", "Rosetta Stone"),
    PictureRoundItem("History and Artifacts Picture Round", "Identify this Dead Sea Scroll artifact.", "Copper Scroll", "Copper Scroll"),
    PictureRoundItem("History and Artifacts Picture Round", "Identify this embroidered work.", "Bayeux Tapestry", "Bayeux Tapestry"),
    PictureRoundItem("History and Artifacts Picture Round", "Identify this archaeological army.", "Terracotta Army", "Terracotta Army"),
    PictureRoundItem("History and Artifacts Picture Round", "Identify this helmet.", "Sutton Hoo helmet", "Sutton Hoo helmet"),
    PictureRoundItem("History and Artifacts Picture Round", "Identify this charter.", "Magna Carta", "Magna Carta"),
    PictureRoundItem("History and Artifacts Picture Round", "Identify this law code.", "Code of Hammurabi", "Code of Hammurabi"),
    PictureRoundItem("History and Artifacts Picture Round", "Identify this bust.", "Nefertiti Bust", "Nefertiti Bust"),
    PictureRoundItem("History and Artifacts Picture Round", "Identify this figurine.", "Venus of Willendorf", "Venus of Willendorf"),
    PictureRoundItem("History and Artifacts Picture Round", "Identify this ancient artifact.", "Standard of Ur", "Standard of Ur"),
    PictureRoundItem("Literature Picture Round", "Identify this author.", "William Shakespeare", "William Shakespeare"),
    PictureRoundItem("Literature Picture Round", "Identify this author.", "Jane Austen", "Jane Austen"),
    PictureRoundItem("Literature Picture Round", "Identify this author.", "Leo Tolstoy", "Leo Tolstoy"),
    PictureRoundItem("Literature Picture Round", "Identify this author.", "Virginia Woolf", "Virginia Woolf"),
    PictureRoundItem("Literature Picture Round", "Identify this author.", "James Joyce", "James Joyce"),
    PictureRoundItem("Literature Picture Round", "Identify this poet.", "Homer", "Homer"),
    PictureRoundItem("Literature Picture Round", "Identify this poet.", "Dante Alighieri", "Dante Alighieri"),
    PictureRoundItem("Literature Picture Round", "Identify this author.", "Charles Dickens", "Charles Dickens"),
    PictureRoundItem("Literature Picture Round", "Identify this author.", "Mary Shelley", "Mary Shelley"),
    PictureRoundItem("Literature Picture Round", "Identify this author.", "Franz Kafka", "Franz Kafka"),
    PictureRoundItem("Classical Music Picture Round", "Identify this composer.", "Johann Sebastian Bach", "Johann Sebastian Bach"),
    PictureRoundItem("Classical Music Picture Round", "Identify this composer.", "Wolfgang Amadeus Mozart", "Wolfgang Amadeus Mozart"),
    PictureRoundItem("Classical Music Picture Round", "Identify this composer.", "Ludwig van Beethoven", "Ludwig van Beethoven"),
    PictureRoundItem("Classical Music Picture Round", "Identify this composer.", "Frederic Chopin", "Frederic Chopin"),
    PictureRoundItem("Classical Music Picture Round", "Identify this composer.", "Pyotr Ilyich Tchaikovsky", "Pyotr Ilyich Tchaikovsky"),
    PictureRoundItem("Classical Music Picture Round", "Identify this composer.", "Igor Stravinsky", "Igor Stravinsky"),
    PictureRoundItem("Classical Music Picture Round", "Identify this composer.", "Clara Schumann", "Clara Schumann"),
    PictureRoundItem("Classical Music Picture Round", "Identify this composer.", "Giuseppe Verdi", "Giuseppe Verdi"),
    PictureRoundItem("Classical Music Picture Round", "Identify this composer.", "Richard Wagner", "Richard Wagner"),
    PictureRoundItem("Classical Music Picture Round", "Identify this composer.", "Claude Debussy", "Claude Debussy"),
    PictureRoundItem("Math and Computing Picture Round", "Identify this fractal.", "Mandelbrot set", "Mandelbrot set"),
    PictureRoundItem("Math and Computing Picture Round", "Identify this surface.", "Mobius strip", "Mobius strip"),
    PictureRoundItem("Math and Computing Picture Round", "Identify this theorem.", "Pythagorean theorem", "Pythagorean theorem"),
    PictureRoundItem("Math and Computing Picture Round", "Identify this dynamical system.", "Lorenz system", "Lorenz system"),
    PictureRoundItem("Math and Computing Picture Round", "Identify this mathematical ratio.", "Golden ratio", "Golden ratio"),
    PictureRoundItem("Math and Computing Picture Round", "Identify this fractal.", "Sierpinski triangle", "Sierpinski triangle"),
    PictureRoundItem("Math and Computing Picture Round", "Identify this family of solids.", "Platonic solid", "Platonic solid"),
    PictureRoundItem("Math and Computing Picture Round", "Identify this diagram type.", "Euler diagram", "Euler diagram"),
    PictureRoundItem("Math and Computing Picture Round", "Identify this probability distribution.", "Normal distribution", "Normal distribution"),
    PictureRoundItem("Math and Computing Picture Round", "Identify this model of computation.", "Turing machine", "Turing machine"),
]


def request_json(params: dict) -> dict:
    response = SESSION.get(WIKIPEDIA_API_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    time.sleep(REQUEST_DELAY_SECONDS)
    return response.json()


def get_page_image_info(title: str) -> dict:
    page_data = request_json(
        {
            "action": "query",
            "format": "json",
            "formatversion": "2",
            "redirects": "1",
            "titles": title,
            "prop": "pageimages|info",
            "piprop": "thumbnail|original|name",
            "pithumbsize": str(THUMBNAIL_WIDTH),
            "inprop": "url",
        }
    )["query"]["pages"][0]

    if page_data.get("missing"):
        raise ValueError(f"Wikipedia page not found: {title}")

    image_title = page_data.get("pageimage")
    if not image_title:
        raise ValueError(f"No lead image found for {title}")

    image_page_title = f"File:{image_title}"
    image_data = request_json(
        {
            "action": "query",
            "format": "json",
            "formatversion": "2",
            "titles": image_page_title,
            "prop": "imageinfo",
            "iiprop": "url|extmetadata",
            "iiurlwidth": str(THUMBNAIL_WIDTH),
        }
    )["query"]["pages"][0]

    imageinfo = image_data.get("imageinfo", [{}])[0]
    metadata = imageinfo.get("extmetadata", {})
    thumbnail_url = page_data.get("thumbnail", {}).get("source")
    original_url = page_data.get("original", {}).get("source") or imageinfo.get("url")
    source_image_url = thumbnail_url or imageinfo.get("thumburl") or original_url

    return {
        "resolved_page_title": page_data.get("title", title),
        "page_url": page_data.get("fullurl"),
        "image_title": image_page_title,
        "source_image_url": source_image_url,
        "original_image_url": original_url,
        "license": metadata.get("LicenseShortName", {}).get("value", ""),
        "artist": metadata.get("Artist", {}).get("value", ""),
        "credit": metadata.get("Credit", {}).get("value", ""),
    }


def image_extension_ok(url: str) -> bool:
    suffix = Path(urlparse(url).path).suffix.lower()
    return suffix in {".gif", ".jpg", ".jpeg", ".png", ".svg", ".webp"}


def download_media(url: str, filename_base: str) -> str:
    for attempt in range(1, DOWNLOAD_RETRIES + 1):
        local_media = download_asset(url, IMAGES_DIR, filename_base)
        if local_media:
            time.sleep(REQUEST_DELAY_SECONDS)
            return local_media

        wait_seconds = attempt * 3
        print(f"Retrying media download in {wait_seconds}s: {url}")
        time.sleep(wait_seconds)

    raise ValueError(f"Failed to download image after retries: {url}")


def build_dataset() -> list[dict]:
    dataset = []

    for index, item in enumerate(PICTURE_ROUND_ITEMS, start=1):
        print(f"Fetching {item.wikipedia_title}")
        info = get_page_image_info(item.wikipedia_title)

        if not image_extension_ok(info["source_image_url"]):
            raise ValueError(f"Unsupported image URL for {item.wikipedia_title}: {info['source_image_url']}")

        local_media = download_media(
            info["source_image_url"],
            f"quizbowl_picture_round_{index:03d}",
        )

        accepted_answers = [item.answer, *item.accepted_answers]
        dataset.append(
            {
                "id": index,
                "question": item.question,
                "quiz_title": item.round_title,
                "media_url": local_media,
                "answer": item.answer,
                "source": {
                    "platform": "Wikipedia / Wikimedia Commons",
                    "url": info["page_url"],
                    "quiz_title": item.round_title,
                    "quiz_type": "Quiz Bowl Picture Round",
                    "wikipedia_title": info["resolved_page_title"],
                    "image_title": info["image_title"],
                    "license": info["license"],
                },
                "accepted_answers": accepted_answers,
                "source_media_url": info["source_image_url"],
                "original_media_url": info["original_image_url"],
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
