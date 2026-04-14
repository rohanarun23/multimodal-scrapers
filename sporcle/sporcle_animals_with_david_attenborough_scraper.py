from pathlib import Path

from sporcle_common import scrape_and_save_quiz


URL = "https://www.sporcle.com/games/nscox/animals-with-david-attenborough-slideshow"
OUTPUT_JSON = Path("dataset/sporcle_animals_with_david_attenborough.json")


def scrape_sporcle_animals_with_david_attenborough() -> None:
    scrape_and_save_quiz(URL, OUTPUT_JSON)


if __name__ == "__main__":
    scrape_sporcle_animals_with_david_attenborough()
