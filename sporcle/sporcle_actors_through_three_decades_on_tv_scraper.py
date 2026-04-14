from pathlib import Path

from sporcle_common import scrape_and_save_quiz


URL = "https://www.sporcle.com/games/ghcgh/actors-through-three-decades-on-tv"
OUTPUT_JSON = Path("dataset/sporcle_actors_through_three_decades_on_tv.json")


def scrape_sporcle_actors_through_three_decades_on_tv() -> None:
    scrape_and_save_quiz(URL, OUTPUT_JSON)


if __name__ == "__main__":
    scrape_sporcle_actors_through_three_decades_on_tv()
