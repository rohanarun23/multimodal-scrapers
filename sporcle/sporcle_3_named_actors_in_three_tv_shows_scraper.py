from pathlib import Path

from sporcle_common import scrape_and_save_quiz


URL = "https://www.sporcle.com/games/ghcgh/3-named-actors-in-three-tv-shows"
OUTPUT_JSON = Path("dataset/sporcle_3_named_actors_in_three_tv_shows.json")


def scrape_sporcle_3_named_actors_in_three_tv_shows() -> None:
    scrape_and_save_quiz(URL, OUTPUT_JSON)


if __name__ == "__main__":
    scrape_sporcle_3_named_actors_in_three_tv_shows()
