import json
from dataclasses import dataclass
from pathlib import Path

import requests
from bs4 import BeautifulSoup


BASE_URL = "https://www.kensquiz.co.uk"
OUTPUT_JSON = Path("dataset/kensquiz_archive_questions.json")
REQUEST_TIMEOUT_SECONDS = 60
USER_AGENT = "Mozilla/5.0 (compatible; quiz-dataset-scraper/1.0)"

QUIZ_NUMBERS = [number for number in range(204, 256) if number not in {227, 229}]


@dataclass(frozen=True)
class ArchiveQuiz:
    number: int
    question_url: str
    answer_url: str
    quiz_title: str
    quiz_blurb: str | None
    published: str | None
    questions: list[str]
    answers: list[str]


SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})


def clean_text(value: str | None) -> str:
    return " ".join((value or "").split()).strip()


def make_request(url: str) -> requests.Response:
    response = SESSION.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response


def extract_text_items(soup: BeautifulSoup, selector: str) -> list[str]:
    return [
        clean_text(item.get_text(" ", strip=True))
        for item in soup.select(selector)
        if clean_text(item.get_text(" ", strip=True))
    ]


def scrape_archive_quiz(number: int) -> ArchiveQuiz:
    question_url = f"{BASE_URL}/quizzes/quiz-archive/quiz-number-{number}"
    answer_url = f"{question_url}/answers"

    print(f"Downloading Ken's Quiz archive page: {question_url}")
    question_soup = BeautifulSoup(make_request(question_url).text, "html.parser")
    answer_soup = BeautifulSoup(make_request(answer_url).text, "html.parser")

    questions = extract_text_items(question_soup, ".kensquizsite_questionlist_question")
    answers = extract_text_items(answer_soup, ".kensquizsite_questionlist_answer")

    if not questions:
        raise ValueError(f"No questions found for quiz number {number}")
    if len(questions) != len(answers):
        raise ValueError(
            f"Quiz number {number}: extracted {len(questions)} questions "
            f"but {len(answers)} answers"
        )

    title = clean_text(
        question_soup.select_one(".entry-title").get_text(" ", strip=True)
        if question_soup.select_one(".entry-title")
        else f"Quiz Number {number}"
    )
    quiz_blurb = clean_text(
        question_soup.select_one(".kensquizsite_quizblurb").get_text(" ", strip=True)
        if question_soup.select_one(".kensquizsite_quizblurb")
        else ""
    )
    published = clean_text(
        question_soup.select_one(".kensquizsite_publishedtext").get_text(" ", strip=True)
        if question_soup.select_one(".kensquizsite_publishedtext")
        else ""
    )

    return ArchiveQuiz(
        number=number,
        question_url=question_url,
        answer_url=answer_url,
        quiz_title=title,
        quiz_blurb=quiz_blurb or None,
        published=published or None,
        questions=questions,
        answers=answers,
    )


def build_dataset(quiz_numbers: list[int]) -> list[dict]:
    dataset = []
    next_id = 1

    for quiz_number in quiz_numbers:
        quiz = scrape_archive_quiz(quiz_number)
        for item_number, (question, answer) in enumerate(
            zip(quiz.questions, quiz.answers),
            start=1,
        ):
            dataset.append(
                {
                    "id": next_id,
                    "question": question,
                    "answer": answer,
                    "quiz_title": quiz.quiz_title,
                    "media_type": "text",
                    "source": {
                        "platform": "Ken's Quiz",
                        "url": quiz.question_url,
                        "answer_url": quiz.answer_url,
                        "quiz_title": quiz.quiz_title,
                        "quiz_type": "General Knowledge Archive",
                        "quiz_number": quiz.number,
                        "item_number": item_number,
                        "published": quiz.published,
                        "quiz_blurb": quiz.quiz_blurb,
                    },
                }
            )
            next_id += 1

    return dataset


def main() -> None:
    dataset = build_dataset(QUIZ_NUMBERS)
    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(dataset, indent=4), encoding="utf-8")
    print(f"Saved {len(dataset)} questions to {OUTPUT_JSON}")


if __name__ == "__main__":
    main()
