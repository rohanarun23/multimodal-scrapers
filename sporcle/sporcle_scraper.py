from pathlib import Path

from sporcle_common import scrape_and_save_quiz


URL = "https://www.sporcle.com/games/ghcgh/actors-in-three-forms-of-visual-media-ii"
OUTPUT_JSON = Path("dataset/sporcle_dataset.json")


def scrape_sporcle_images() -> None:
    scrape_and_save_quiz(URL, OUTPUT_JSON)


if __name__ == "__main__":
    scrape_sporcle_images()
