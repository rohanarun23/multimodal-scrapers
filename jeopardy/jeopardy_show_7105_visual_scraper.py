from pathlib import Path

from jarchive_common import save_records, scrape_jarchive_game


URL = "https://www.j-archive.com/showgame.php?game_id=4951"
OUTPUT_JSON = Path("dataset/jeopardy_show_7105_visual_clues.json")


def scrape_jeopardy_show_7105_visual_clues() -> None:
    records = scrape_jarchive_game(URL, media_only=True)
    save_records(records, OUTPUT_JSON)


if __name__ == "__main__":
    scrape_jeopardy_show_7105_visual_clues()
