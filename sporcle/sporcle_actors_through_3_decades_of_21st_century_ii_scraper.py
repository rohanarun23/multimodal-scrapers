from pathlib import Path

from sporcle_common import scrape_and_save_quiz


URL = "https://www.sporcle.com/games/ghcgh/actors-through-3-decades-of-21st-century-1"
OUTPUT_JSON = Path("dataset/sporcle_actors_through_3_decades_of_21st_century_ii.json")


def scrape_sporcle_actors_through_3_decades_of_21st_century_ii() -> None:
    scrape_and_save_quiz(URL, OUTPUT_JSON)


if __name__ == "__main__":
    scrape_sporcle_actors_through_3_decades_of_21st_century_ii()
