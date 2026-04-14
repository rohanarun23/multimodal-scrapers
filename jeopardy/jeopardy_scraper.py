from pathlib import Path

from jarchive_common import save_records, scrape_jarchive_game


URL = "https://www.j-archive.com/showgame.php?game_id=4972"
OUTPUT_JSON = Path("dataset/jeopardy_dataset.json")


def scrape_jeopardy_game() -> None:
    records = scrape_jarchive_game(URL, media_only=False)
    save_records(records, OUTPUT_JSON)


if __name__ == "__main__":
    scrape_jeopardy_game()
