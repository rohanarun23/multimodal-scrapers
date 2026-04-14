import json
import tempfile
from pathlib import Path

import requests
import pdfplumber
from pdf2image import convert_from_path

QUESTIONS_PDF_URL = "https://www.kensquiz.co.uk/wp-content/uploads/roadsignsq.pdf"
ANSWERS_PDF_URL = "https://www.kensquiz.co.uk/wp-content/uploads/roadsignsa.pdf"
OUTPUT_JSON = Path("dataset/kensquiz_dataset.json")
IMAGES_DIR = Path("dataset/images/kensquiz")
REQUEST_TIMEOUT_SECONDS = 60
GRID_COLUMNS = 4
GRID_ROWS = 6


def download_pdf(url: str, out_path: Path) -> None:
    print(f"Downloading PDF from {url}...")
    response = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()

    with out_path.open("wb") as file:
        file.write(response.content)

    print(f"Saved: {out_path}")


def extract_quiz_question(pdf_path: Path) -> str:
    print("Extracting quiz question (title) from questions PDF...")
    with pdfplumber.open(pdf_path) as pdf:
        first_page = pdf.pages[0]
        text = first_page.extract_text().split("\n")
        question_title = text[0].strip()

    print("Title extracted:", question_title)
    return question_title


def extract_images(pdf_path: Path) -> list[str]:
    print("Extracting images from questions PDF...")
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)

    pages = convert_from_path(pdf_path, dpi=200)
    image_paths = []

    page = pages[0]
    width, height = page.size

    cell_w = width // GRID_COLUMNS
    cell_h = height // GRID_ROWS

    count = 1

    for row_index in range(GRID_ROWS):
        for column_index in range(GRID_COLUMNS):
            left = column_index * cell_w
            top = row_index * cell_h
            right = left + cell_w
            bottom = top + cell_h

            crop = page.crop((left, top, right, bottom))
            filename = f"sign{count:02d}.png"
            filepath = IMAGES_DIR / filename
            crop.save(filepath)
            image_paths.append(filepath.as_posix())

            count += 1

    print("Image extraction complete.")
    return image_paths


def extract_answers(pdf_path: Path) -> list[str]:
    print("Extracting answers...")
    with pdfplumber.open(pdf_path) as pdf:
        text = ""
        for page in pdf.pages:
            text += page.extract_text() + "\n"

    lines = [line.strip() for line in text.split("\n") if line.strip()]

    start_index = None
    for i, line in enumerate(lines):
        if line.startswith("1."):
            start_index = i
            break

    if start_index is None:
        raise ValueError("Could not find start of numbered answers!")

    lines = lines[start_index:]

    answers = []
    for line in lines:
        if "." in line:
            _, ans = line.split(".", 1)
            answers.append(ans.strip())

    return answers[:24]


def build_dataset(image_paths: list[str], answers: list[str], question_title: str) -> None:
    dataset = []
    quiz_title = "Know Your Road Signs"
    quiz_description = question_title
    quiz_url = QUESTIONS_PDF_URL

    for i, (img, ans) in enumerate(zip(image_paths, answers), start=1):
        dataset.append(
            {
                "id": i,
                "question": quiz_description,
                "quiz_title": quiz_title,
                "media_url": img,
                "answer": ans,
                "source": {
                    "platform": "Ken's Quiz",
                    "url": quiz_url,
                    "quiz_title": quiz_title,
                },
            }
        )

    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_JSON.open("w") as file:
        json.dump(dataset, file, indent=4)

    print(f"Dataset saved to {OUTPUT_JSON}")


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        q_pdf = temp_path / "roadsigns_q.pdf"
        a_pdf = temp_path / "roadsigns_a.pdf"

        download_pdf(QUESTIONS_PDF_URL, q_pdf)
        download_pdf(ANSWERS_PDF_URL, a_pdf)

        question_title = extract_quiz_question(q_pdf)
        image_paths = extract_images(q_pdf)
        answers = extract_answers(a_pdf)
        build_dataset(image_paths, answers, question_title)
