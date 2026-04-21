import json
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urljoin

import pdfplumber
import requests
from pdf2image import convert_from_path

BASE_URL = "https://www.kensquiz.co.uk"
OUTPUT_JSON = Path("dataset/kensquiz_pub_quiz_handouts.json")
IMAGES_DIR = Path("dataset/images/kensquiz_handouts")
REQUEST_TIMEOUT_SECONDS = 60
RENDER_DPI = 200
USER_AGENT = "Mozilla/5.0 (compatible; quiz-dataset-scraper/1.0)"


@dataclass(frozen=True)
class HandoutRound:
    slug: str
    quiz_title: str
    question_pdf_path: str
    answer_pdf_path: str


HANDOUT_ROUNDS = [
    HandoutRound(
        slug="signs_of_spring_2026",
        quiz_title="Signs of Spring",
        question_pdf_path="/wp-content/uploads/SOS2026Q.pdf",
        answer_pdf_path="/wp-content/uploads/SOS2026A.pdf",
    ),
    HandoutRound(
        slug="name_that_bear_2026",
        quiz_title="Name That Bear",
        question_pdf_path="/wp-content/uploads/NTB2026Q.pdf",
        answer_pdf_path="/wp-content/uploads/NTB2026A.pdf",
    ),
    HandoutRound(
        slug="luxury_brand_logos_2025",
        quiz_title="Luxury Brand Logos",
        question_pdf_path="/wp-content/uploads/LBLOGOSQ2025.pdf",
        answer_pdf_path="/wp-content/uploads/LBLOGOSA2025.pdf",
    ),
    HandoutRound(
        slug="christmas_day_dingbats_2025",
        quiz_title="Christmas Day Dingbats",
        question_pdf_path="/wp-content/uploads/CDD2025Q.pdf",
        answer_pdf_path="/wp-content/uploads/CDD2025A.pdf",
    ),
    HandoutRound(
        slug="us_state_flags_pictures",
        quiz_title="US State Flags",
        question_pdf_path="/wp-content/uploads/USSFQ.pdf",
        answer_pdf_path="/wp-content/uploads/USSFA.pdf",
    ),
    HandoutRound(
        slug="island_country_flags_2025",
        quiz_title="Flags of Island Countries",
        question_pdf_path="/wp-content/uploads/ISFLAGSQ.pdf",
        answer_pdf_path="/wp-content/uploads/ISFLAGSA.pdf",
    ),
    HandoutRound(
        slug="flags_with_animals_2025",
        quiz_title="Flags with Animals",
        question_pdf_path="/wp-content/uploads/FWA2025Q.pdf",
        answer_pdf_path="/wp-content/uploads/FWA2025A.pdf",
    ),
    HandoutRound(
        slug="famous_puppets_2024",
        quiz_title="Famous Puppets",
        question_pdf_path="/wp-content/uploads/FPQ2024.pdf",
        answer_pdf_path="/wp-content/uploads/FPA2024.pdf",
    ),
    HandoutRound(
        slug="euro_2024_team_badges",
        quiz_title="UEFA Euro 2024 Team Badges",
        question_pdf_path="/wp-content/uploads/EURO2024Q.pdf",
        answer_pdf_path="/wp-content/uploads/EURO2024A.pdf",
    ),
    HandoutRound(
        slug="african_nations_badges",
        quiz_title="African Nations Badges",
        question_pdf_path="/wp-content/uploads/ANLOGOQ.pdf",
        answer_pdf_path="/wp-content/uploads/ANLOGOA.pdf",
    ),
    HandoutRound(
        slug="hats_and_headgear_2023",
        quiz_title="Hats and Headgear",
        question_pdf_path="/wp-content/uploads/HATS2023Q.pdf",
        answer_pdf_path="/wp-content/uploads/HATS2023A.pdf",
    ),
    HandoutRound(
        slug="barbie_jobs",
        quiz_title="What's Barbie's Job?",
        question_pdf_path="/wp-content/uploads/BARBIEJQ.pdf",
        answer_pdf_path="/wp-content/uploads/BARBIEJA.pdf",
    ),
    HandoutRound(
        slug="royal_residences_2023",
        quiz_title="Royal Residences",
        question_pdf_path="/wp-content/uploads/ROYALR2023Q.pdf",
        answer_pdf_path="/wp-content/uploads/ROYALR2023A.pdf",
    ),
    HandoutRound(
        slug="uk_prime_ministers_pictures",
        quiz_title="British Prime Ministers",
        question_pdf_path="/wp-content/uploads/UKPMSQ.pdf",
        answer_pdf_path="/wp-content/uploads/UKPMSA.pdf",
    ),
    HandoutRound(
        slug="sports_clothing_logos",
        quiz_title="Sports Clothing Logos",
        question_pdf_path="/wp-content/uploads/SPORTLOGOQ.pdf",
        answer_pdf_path="/wp-content/uploads/SPORTLOGOA.pdf",
    ),
    HandoutRound(
        slug="who_lives_here",
        quiz_title="Who Lives Here?",
        question_pdf_path="/wp-content/uploads/WHITPIXQ.pdf",
        answer_pdf_path="/wp-content/uploads/WHITPIXA.pdf",
    ),
    HandoutRound(
        slug="wildflowers_1",
        quiz_title="Wildflowers 1",
        question_pdf_path="/wp-content/uploads/WILDF1Q.pdf",
        answer_pdf_path="/wp-content/uploads/WILDF1A.pdf",
    ),
    HandoutRound(
        slug="wildflowers_2",
        quiz_title="Wildflowers 2",
        question_pdf_path="/wp-content/uploads/WILDF2Q.pdf",
        answer_pdf_path="/wp-content/uploads/WILDF2A.pdf",
    ),
    HandoutRound(
        slug="fruit_and_veg_quiz",
        quiz_title="Ken's Fruit and Veg Quiz",
        question_pdf_path="/wp-content/uploads/KFANDVQ.pdf",
        answer_pdf_path="/wp-content/uploads/KFANDVA.pdf",
    ),
    HandoutRound(
        slug="covid_store_cupboard_quiz",
        quiz_title="Covid Store Cupboard Quiz",
        question_pdf_path="/wp-content/uploads/COVCQQ.pdf",
        answer_pdf_path="/wp-content/uploads/COVCQA.pdf",
    ),
    HandoutRound(
        slug="laundry_care_symbols",
        quiz_title="Laundry Care Symbols",
        question_pdf_path="/wp-content/uploads/CCSQ.pdf",
        answer_pdf_path="/wp-content/uploads/CCSA.pdf",
    ),
    HandoutRound(
        slug="british_food",
        quiz_title="British Food",
        question_pdf_path="/wp-content/uploads/BPNFQ.pdf",
        answer_pdf_path="/wp-content/uploads/BPNFA.pdf",
    ),
]


def download_pdf(url: str, out_path: Path) -> None:
    print(f"Downloading {url}")
    response = requests.get(
        url,
        timeout=REQUEST_TIMEOUT_SECONDS,
        headers={"User-Agent": USER_AGENT},
    )
    response.raise_for_status()

    content_type = response.headers.get("content-type", "")
    if "pdf" not in content_type.lower() and not response.content.startswith(b"%PDF"):
        raise ValueError(f"Expected a PDF from {url}, got {content_type!r}")

    out_path.write_bytes(response.content)


def clean_text(value: str) -> str:
    return " ".join(value.replace("\xa0", " ").split())


def clean_answer(value: str) -> str:
    return clean_text(value).strip(" .;-")


def extract_prompt(pdf_path: Path) -> str:
    with pdfplumber.open(pdf_path) as pdf:
        text = pdf.pages[0].extract_text() or ""

    lines = [clean_text(line) for line in text.splitlines() if clean_text(line)]
    for line in lines[1:]:
        if not re.fullmatch(r"[\d\s]+", line) and "kensquiz.co.uk" not in line.lower():
            return line

    return "Can you identify the pictures in this pub quiz handout round?"


def extract_answers(pdf_path: Path) -> list[str]:
    answers_by_number: dict[int, str] = {}

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            for table in page.extract_tables():
                for row in table:
                    for cell in row:
                        cell_text = clean_text(cell or "")
                        match = re.match(r"^(\d{1,2})[.)]?\s*(.+)$", cell_text, flags=re.S)
                        if match:
                            number = int(match.group(1))
                            if 1 <= number <= 40:
                                answers_by_number[number] = clean_answer(match.group(2))

        if answers_by_number:
            return [answers_by_number[number] for number in sorted(answers_by_number)]

        text = "\n".join(page.extract_text() or "" for page in pdf.pages)

    text = clean_text(text)
    text = re.sub(r"www\.kensquiz\.co\.uk.*$", "", text, flags=re.IGNORECASE)
    matches = list(re.finditer(r"(?<!\d)(\d{1,2})[.)]?\s*", text))

    for index, match in enumerate(matches):
        number = int(match.group(1))
        if number < 1 or number > 40:
            continue

        next_start = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        answer = clean_answer(text[match.end() : next_start])
        if answer:
            answers_by_number[number] = answer

    if not answers_by_number:
        raise ValueError(f"Could not extract numbered answers from {pdf_path}")

    return [answers_by_number[number] for number in sorted(answers_by_number)]


def extract_image_tiles(pdf_path: Path, round_slug: str) -> list[str]:
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)

    with pdfplumber.open(pdf_path) as pdf:
        page = pdf.pages[0]
        image_boxes = sorted(page.images, key=lambda image: (image["top"], image["x0"]))
        page_width = float(page.width)
        page_height = float(page.height)

    rendered_page = convert_from_path(
        pdf_path,
        dpi=RENDER_DPI,
        first_page=1,
        last_page=1,
    )[0]
    scale_x = rendered_page.width / page_width
    scale_y = rendered_page.height / page_height

    image_paths = []
    for index, image in enumerate(image_boxes, start=1):
        left = round(float(image["x0"]) * scale_x)
        top = round(float(image["top"]) * scale_y)
        right = round(float(image["x1"]) * scale_x)
        bottom = round(float(image["bottom"]) * scale_y)

        tile = rendered_page.crop((left, top, right, bottom))
        file_path = IMAGES_DIR / f"{round_slug}_{index:02d}.jpg"
        tile.convert("RGB").save(file_path, quality=92)
        image_paths.append(file_path.as_posix())

    if not image_paths:
        raise ValueError(f"No image tiles found in {pdf_path}")

    return image_paths


def build_dataset(rounds: list[HandoutRound]) -> list[dict]:
    dataset = []
    next_id = 1

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        for handout_round in rounds:
            question_url = urljoin(BASE_URL, handout_round.question_pdf_path)
            answer_url = urljoin(BASE_URL, handout_round.answer_pdf_path)
            question_pdf = temp_path / f"{handout_round.slug}_questions.pdf"
            answer_pdf = temp_path / f"{handout_round.slug}_answers.pdf"

            download_pdf(question_url, question_pdf)
            download_pdf(answer_url, answer_pdf)

            prompt = extract_prompt(question_pdf)
            image_paths = extract_image_tiles(question_pdf, handout_round.slug)
            answers = extract_answers(answer_pdf)

            if len(image_paths) != len(answers):
                raise ValueError(
                    f"{handout_round.quiz_title}: extracted {len(image_paths)} images "
                    f"but {len(answers)} answers"
                )

            for item_number, (image_path, answer) in enumerate(
                zip(image_paths, answers),
                start=1,
            ):
                dataset.append(
                    {
                        "id": next_id,
                        "question": prompt,
                        "quiz_title": handout_round.quiz_title,
                        "media_url": image_path,
                        "answer": answer,
                        "source": {
                            "platform": "Ken's Quiz",
                            "url": question_url,
                            "answer_url": answer_url,
                            "quiz_title": handout_round.quiz_title,
                            "quiz_type": "Pub Quiz Handout Round",
                            "round_slug": handout_round.slug,
                            "item_number": item_number,
                        },
                    }
                )
                next_id += 1

    return dataset


def main() -> None:
    dataset = build_dataset(HANDOUT_ROUNDS)
    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(dataset, indent=4), encoding="utf-8")
    print(f"Saved {len(dataset)} questions to {OUTPUT_JSON}")


if __name__ == "__main__":
    main()
