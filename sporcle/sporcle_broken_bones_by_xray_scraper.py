from pathlib import Path

from sporcle_common import scrape_and_save_quiz


URL = "https://www.sporcle.com/games/emilymarie07/broken-bone-xrays"
OUTPUT_JSON = Path("dataset/sporcle_broken_bones_by_xray.json")


def scrape_sporcle_broken_bones_by_xray() -> None:
    scrape_and_save_quiz(URL, OUTPUT_JSON)


if __name__ == "__main__":
    scrape_sporcle_broken_bones_by_xray()
