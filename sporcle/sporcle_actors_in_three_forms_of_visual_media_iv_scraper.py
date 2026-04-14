from pathlib import Path

from sporcle_common import scrape_and_save_quiz


URL = "https://www.sporcle.com/games/ghcgh/actors-in-three-forms-of-visual-media-iv"
OUTPUT_JSON = Path("dataset/sporcle_actors_in_three_forms_of_visual_media_iv.json")


def scrape_sporcle_actors_in_three_forms_of_visual_media_iv() -> None:
    scrape_and_save_quiz(URL, OUTPUT_JSON)


if __name__ == "__main__":
    scrape_sporcle_actors_in_three_forms_of_visual_media_iv()
