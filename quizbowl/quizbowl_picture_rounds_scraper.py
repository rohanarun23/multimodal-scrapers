import json
import tempfile
from pathlib import Path

import requests
from pdf2image import convert_from_path


ROOT_DIR = Path(__file__).resolve().parents[1]
OUTPUT_JSON = ROOT_DIR / "dataset/quizbowl_picture_rounds.json"
IMAGES_DIR = ROOT_DIR / "dataset/images/quizbowl_visual_bonus"
REQUEST_TIMEOUT_SECONDS = 60
RENDER_DPI = 200

SOURCE_PLATFORM = "Quizbowl Packet Archive"

SOURCES = [
    {
        "slug": "visual_bonus_round",
        "set_title": "TTGTII VI: Lick My Love Pump",
        "packet_title": "The Visual Bonus Round",
        "pdf_url": "https://files.quizbowlpackets.com/1271/Visual%20Bonus%20Round.pdf",
        "entries": [
            {
                "bonus_number": 1,
                "source_page": 8,
                "prompt": "Identify the Nintendo Entertainment System games shown in the labeled screenshots.",
                "reasoning_focus": "video_game_screenshot_identification",
                "parts": [
                    {"label": "A", "question": "Which NES game is shown in screenshot A?", "answer": "Excitebike"},
                    {"label": "B", "question": "Which NES game is shown in screenshot B?", "answer": "Contra"},
                    {"label": "C", "question": "Which NES game is shown in screenshot C?", "answer": "Final Fantasy"},
                    {"label": "D", "question": "Which NES game is shown in screenshot D?", "answer": "R.C. Pro-Am"},
                    {"label": "E", "question": "Which NES game is shown in screenshot E?", "answer": "Super Mario Brothers 2"},
                    {"label": "F", "question": "Which NES game is shown in screenshot F?", "answer": "Tecmo Bowl"},
                ],
            },
            {
                "bonus_number": 2,
                "source_page": 9,
                "prompt": "Identify each Madonna painting from the labeled image, giving both title and artist.",
                "reasoning_focus": "fine_art_image_identification",
                "parts": [
                    {
                        "label": "A",
                        "question": "Which Madonna painting and artist are shown in image A?",
                        "answer": "Madonna with the Long Neck OR Long-necked Madonna by Parmigianino",
                    },
                    {
                        "label": "B",
                        "question": "Which Madonna painting and artist are shown in image B?",
                        "answer": "Madonna of the Rocks by Leonardo da Vinci",
                    },
                    {
                        "label": "C",
                        "question": "Which Madonna painting and artist are shown in image C?",
                        "answer": "Madonna of the Harpies by Andrea del Sarto",
                    },
                ],
            },
            {
                "bonus_number": 3,
                "source_page": 10,
                "prompt": "Given the molecular structure of a commonly known substance, name that substance.",
                "reasoning_focus": "molecular_structure_inference",
                "parts": [
                    {"label": "A", "question": "Identify the molecular structure labeled A.", "answer": "nicotine"},
                    {"label": "B", "question": "Identify the molecular structure labeled B.", "answer": "benzene"},
                    {"label": "C", "question": "Identify the molecular structure labeled C.", "answer": "caffeine"},
                    {"label": "D", "question": "Identify the molecular structure labeled D.", "answer": "aspirin OR acetaminophen"},
                ],
            },
            {
                "bonus_number": 4,
                "source_page": 11,
                "prompt": "Use the labeled anatomical diagram to match each named structure to its letter.",
                "reasoning_focus": "anatomical_diagram_labeling",
                "parts": [
                    {"label": "ejaculatory_duct", "question": "Which letter marks the ejaculatory duct on the diagram?", "answer": "G"},
                    {"label": "epididymis", "question": "Which letter marks the epididymis on the diagram?", "answer": "L"},
                    {"label": "prostate", "question": "Which letter marks the prostate on the diagram?", "answer": "F"},
                    {"label": "symphysis_pubis", "question": "Which letter marks the symphysis pubis on the diagram?", "answer": "B"},
                    {"label": "urethra", "question": "Which letter marks the urethra on the diagram?", "answer": "K"},
                    {"label": "vas_deferens", "question": "Which letter marks the vas deferens on the diagram?", "answer": "D"},
                ],
            },
            {
                "bonus_number": 5,
                "source_page": 12,
                "prompt": "Identify the European Union member country represented by each labeled flag.",
                "reasoning_focus": "flag_identification",
                "parts": [
                    {"label": "A", "question": "Which EU member country's flag is labeled A?", "answer": "Lithuania"},
                    {"label": "B", "question": "Which EU member country's flag is labeled B?", "answer": "Latvia"},
                    {"label": "C", "question": "Which EU member country's flag is labeled C?", "answer": "Denmark"},
                    {"label": "D", "question": "Which EU member country's flag is labeled D?", "answer": "Malta"},
                ],
            },
            {
                "bonus_number": 7,
                "source_page": 14,
                "prompt": "Identify the Capcom characters shown in the labeled Street Fighter-related images.",
                "reasoning_focus": "character_image_identification",
                "parts": [
                    {"label": "A", "question": "Which Capcom character is shown in image A?", "answer": "Akuma"},
                    {"label": "B", "question": "Which Capcom character is shown in image B?", "answer": "Dan"},
                    {"label": "C", "question": "Which Capcom character is shown in image C?", "answer": "Rolento"},
                    {"label": "D", "question": "Which Capcom character is shown in image D?", "answer": "Sakura"},
                    {"label": "E", "question": "Which Capcom character is shown in image E?", "answer": "Guy"},
                    {"label": "F", "question": "Which Capcom character is shown in image F?", "answer": "Rose"},
                ],
            },
            {
                "bonus_number": 8,
                "source_page": 15,
                "prompt": "Identify the Broadway play or musical from each labeled original playbill.",
                "reasoning_focus": "playbill_visual_identification",
                "parts": [
                    {"label": "A", "question": "Which Broadway play or musical has the playbill labeled A?", "answer": "Miss Saigon"},
                    {"label": "B", "question": "Which Broadway play or musical has the playbill labeled B?", "answer": "Les Miserables"},
                    {
                        "label": "C",
                        "question": "Which Broadway play or musical has the playbill labeled C?",
                        "answer": "You're a Good Man, Charlie Brown",
                    },
                    {"label": "D", "question": "Which Broadway play or musical has the playbill labeled D?", "answer": "Nunsense"},
                    {"label": "E", "question": "Which Broadway play or musical has the playbill labeled E?", "answer": "Phantom of the Opera"},
                    {
                        "label": "F",
                        "question": "Which Broadway play or musical has the playbill labeled F?",
                        "answer": "Kiss of the Spider Woman",
                    },
                ],
            },
            {
                "bonus_number": 9,
                "source_page": 16,
                "prompt": "Given the location and image of a presidential gravesite, identify the president.",
                "reasoning_focus": "location_plus_image_inference",
                "parts": [
                    {"label": "A", "question": "Which president is buried at site A?", "answer": "James Monroe"},
                    {"label": "B", "question": "Which president is buried at site B?", "answer": "Warren Harding"},
                    {"label": "C", "question": "Which president is buried at site C?", "answer": "Herbert Hoover"},
                ],
            },
            {
                "bonus_number": 10,
                "source_page": 17,
                "prompt": "Use the right triangle diagram to answer the requested quantitative subparts.",
                "reasoning_focus": "geometry_calculation",
                "parts": [
                    {"label": "A", "question": "For the figure shown, what is the value requested in part A?", "answer": "x equals 5"},
                    {"label": "B", "question": "For the figure shown, what is the value requested in part B?", "answer": "theta equals 30 degrees"},
                    {"label": "C_sin", "question": "For the figure shown, what is the sine value requested in part C?", "answer": ".60 OR 3/5"},
                    {"label": "C_cos", "question": "For the figure shown, what is the cosine value requested in part C?", "answer": ".80 OR 4/5"},
                    {"label": "C_tan", "question": "For the figure shown, what is the tangent value requested in part C?", "answer": ".75 OR 3/4"},
                ],
            },
            {
                "bonus_number": 11,
                "source_page": 18,
                "prompt": "Identify the constellation represented by each labeled picture.",
                "reasoning_focus": "constellation_visual_identification",
                "parts": [
                    {"label": "A", "question": "Which constellation is shown in picture A?", "answer": "Andromeda"},
                    {"label": "B", "question": "Which constellation is shown in picture B?", "answer": "Virgo"},
                    {"label": "C", "question": "Which constellation is shown in picture C?", "answer": "Aquila"},
                    {"label": "D", "question": "Which constellation is shown in picture D?", "answer": "Cygnus"},
                    {"label": "E", "question": "Which constellation is shown in picture E?", "answer": "Lepus"},
                    {"label": "F", "question": "Which constellation is shown in picture F?", "answer": "Orion"},
                ],
            },
            {
                "bonus_number": 12,
                "source_page": 19,
                "prompt": "Identify the U.S. Marine rank insignias shown on the handout.",
                "reasoning_focus": "ordered_visual_reasoning",
                "parts": [
                    {"label": "A", "question": "Which U.S. Marine rank insignia is labeled A?", "answer": "First Lieutenant"},
                    {"label": "B", "question": "Which U.S. Marine rank insignia is labeled B?", "answer": "Major General"},
                    {"label": "C", "question": "Which U.S. Marine rank insignia is labeled C?", "answer": "Lieutenant Colonel"},
                    {"label": "D", "question": "Which U.S. Marine rank insignia is labeled D?", "answer": "Private First Class"},
                    {"label": "E", "question": "Which U.S. Marine rank insignia is labeled E?", "answer": "Brigadier General"},
                    {"label": "F", "question": "Which U.S. Marine rank insignia is labeled F?", "answer": "Captain"},
                ],
            },
            {
                "bonus_number": 13,
                "source_page": 20,
                "prompt": "Identify the piano excerpts and the shared composer shown on the handout.",
                "reasoning_focus": "sheet_music_pattern_matching",
                "parts": [
                    {"label": "composer", "question": "Who is the common composer of the four works on this handout?", "answer": "Beethoven"},
                    {"label": "A", "question": "Which work is labeled A?", "answer": "5th Symphony"},
                    {"label": "B", "question": "Which work is labeled B?", "answer": "Fur Elise"},
                    {"label": "C", "question": "Which work is labeled C?", "answer": "Moonlight Sonata"},
                    {"label": "D", "question": "Which work is labeled D?", "answer": "Ode to Joy OR 9th Symphony"},
                ],
            },
            {
                "bonus_number": 14,
                "source_page": 21,
                "prompt": "Identify each noted 20th-century dictator from the labeled photograph.",
                "reasoning_focus": "historical_photo_identification",
                "parts": [
                    {"label": "A", "question": "Which 20th-century dictator is shown in photo A?", "answer": "Leonid Brezhnev"},
                    {"label": "B", "question": "Which 20th-century dictator is shown in photo B?", "answer": "Moammar el-Gadhafi"},
                    {"label": "C", "question": "Which 20th-century dictator is shown in photo C?", "answer": "Robert Mugabe"},
                    {"label": "D", "question": "Which 20th-century dictator is shown in photo D?", "answer": "Francisco Franco"},
                    {"label": "E", "question": "Which 20th-century dictator is shown in photo E?", "answer": "Kim Il-sung"},
                    {"label": "F", "question": "Which 20th-century dictator is shown in photo F?", "answer": "Francois OR Papa Doc Duvalier"},
                ],
            },
            {
                "bonus_number": 15,
                "source_page": 22,
                "prompt": "Identify the labeled parts on the flower diagram.",
                "reasoning_focus": "scientific_diagram_labeling",
                "parts": [
                    {"label": "A", "question": "Which flower part is labeled A?", "answer": "stigma"},
                    {"label": "B", "question": "Which flower part is labeled B?", "answer": "pistil"},
                    {"label": "C", "question": "Which flower part is labeled C?", "answer": "stamen"},
                    {"label": "D", "question": "Which flower part is labeled D?", "answer": "ovary"},
                    {"label": "E", "question": "Which flower part is labeled E?", "answer": "sepal"},
                ],
            },
            {
                "bonus_number": 16,
                "source_page": 23,
                "prompt": "Identify the American Sign Language letters shown on the handout.",
                "reasoning_focus": "symbol_to_letter_mapping",
                "parts": [
                    {"label": "1", "question": "Which ASL letter is shown in position 1?", "answer": "U"},
                    {"label": "2", "question": "Which ASL letter is shown in position 2?", "answer": "I"},
                    {"label": "3", "question": "Which ASL letter is shown in position 3?", "answer": "A"},
                    {"label": "4", "question": "Which ASL letter is shown in position 4?", "answer": "Q"},
                    {"label": "5", "question": "Which ASL letter is shown in position 5?", "answer": "C"},
                ],
            },
            {
                "bonus_number": 17,
                "source_page": 24,
                "prompt": "Identify each Salvador Dali work from the labeled image.",
                "reasoning_focus": "fine_art_image_identification",
                "parts": [
                    {"label": "A", "question": "Which Salvador Dali work is shown in image A?", "answer": "Sleep"},
                    {"label": "B", "question": "Which Salvador Dali work is shown in image B?", "answer": "The Persistence of Memory"},
                    {"label": "C", "question": "Which Salvador Dali work is shown in image C?", "answer": "The Three Sphinxes of Bikini"},
                    {"label": "D", "question": "Which Salvador Dali work is shown in image D?", "answer": "Metamorphosis of Narcissus"},
                ],
            },
            {
                "bonus_number": 18,
                "source_page": 25,
                "prompt": "Identify the film from each labeled horror or suspense image.",
                "reasoning_focus": "film_still_identification",
                "parts": [
                    {"label": "A", "question": "Which film is represented by image A?", "answer": "The Phantom of the Opera"},
                    {"label": "B", "question": "Which film is represented by image B?", "answer": "The Shining"},
                    {"label": "C", "question": "Which film is represented by image C?", "answer": "Pee-Wee's Big Adventure"},
                    {"label": "D", "question": "Which film is represented by image D?", "answer": "Un Chien Andalou OR An Andalusian Dog"},
                    {"label": "E", "question": "Which film is represented by image E?", "answer": "Poltergeist"},
                    {"label": "F", "question": "Which film is represented by image F?", "answer": "Psycho"},
                ],
            },
            {
                "bonus_number": 19,
                "source_page": 26,
                "prompt": "Identify the countries shown only as outlines on the handout.",
                "reasoning_focus": "geographic_outline_identification",
                "parts": [
                    {"label": "A", "question": "Which country outline is labeled A?", "answer": "China"},
                    {"label": "B", "question": "Which country outline is labeled B?", "answer": "Iceland"},
                    {"label": "C", "question": "Which country outline is labeled C?", "answer": "New Zealand"},
                    {"label": "D", "question": "Which country outline is labeled D?", "answer": "Columbia"},
                    {"label": "E", "question": "Which country outline is labeled E?", "answer": "Turkey"},
                ],
            },
            {
                "bonus_number": 20,
                "source_page": 27,
                "prompt": "Identify each movie from the labeled poster with key text removed.",
                "reasoning_focus": "movie_poster_identification",
                "parts": [
                    {"label": "A", "question": "Which movie poster is labeled A?", "answer": "Ali"},
                    {"label": "B", "question": "Which movie poster is labeled B?", "answer": "Candyman"},
                    {"label": "C", "question": "Which movie poster is labeled C?", "answer": "The Craft"},
                    {"label": "D", "question": "Which movie poster is labeled D?", "answer": "From Hell"},
                    {"label": "E", "question": "Which movie poster is labeled E?", "answer": "Remember the Titans"},
                    {"label": "F", "question": "Which movie poster is labeled F?", "answer": "We Were Soldiers"},
                ],
            },
        ],
    },
    {
        "slug": "veto_2016_kiras_team",
        "set_title": "VETO 2016",
        "packet_title": "Kira's Team",
        "pdf_url": "https://files.quizbowlpackets.com/1995/Kiras_Team.pdf",
        "entries": [
            {
                "bonus_number": 2,
                "prompt": "Identify the bridge shown and the body of water it crosses.",
                "reasoning_focus": "landmark_plus_water_inference",
                "parts": [
                    {
                        "label": "A",
                        "source_page": 13,
                        "question": "Identify the bridge shown on page A and the body of water it crosses.",
                        "answer": "Tower Bridge; River Thames",
                    },
                    {
                        "label": "B",
                        "source_page": 14,
                        "question": "Identify the bridge shown on page B and the body of water it crosses.",
                        "answer": "Confederation Bridge; Northumberland Strait",
                    },
                    {
                        "label": "C",
                        "source_page": 15,
                        "question": "Identify the bridge shown on page C and the body of water it crosses.",
                        "answer": "Ponte Vecchio; Arno River",
                    },
                ],
            },
        ],
    },
    {
        "slug": "sword_bowl_2005_utc",
        "set_title": "Sword Bowl 2005",
        "packet_title": "UTC",
        "pdf_url": "https://files.quizbowlpackets.com/1326/UTC.pdf",
        "entries": [
            {
                "bonus_number": 1,
                "source_page": 8,
                "prompt": "Identify the North American city skyline shown in each labeled picture.",
                "reasoning_focus": "skyline_visual_identification",
                "parts": [
                    {"label": "A", "question": "Which North American city skyline is labeled A?", "answer": "Chicago"},
                    {"label": "B", "question": "Which North American city skyline is labeled B?", "answer": "Toronto"},
                    {"label": "C", "question": "Which North American city skyline is labeled C?", "answer": "Detroit"},
                ],
            },
        ],
    },
    {
        "slug": "dennis_haskins_open_2005_round_03",
        "set_title": "Dennis Haskins Open March 2005",
        "packet_title": "Round Three",
        "pdf_url": "https://files.quizbowlpackets.com/1328/03.pdf",
        "entries": [
            {
                "bonus_number": 1,
                "source_page": 8,
                "prompt": "Identify the Civil War battle represented by each labeled map.",
                "reasoning_focus": "historical_battle_map_identification",
                "parts": [
                    {"label": "A", "question": "Which Civil War battle is shown on map A?", "answer": "Antietam"},
                    {"label": "B", "question": "Which Civil War battle is shown on map B?", "answer": "Gettysburg"},
                    {"label": "C", "question": "Which Civil War battle is shown on map C?", "answer": "Shiloh"},
                ],
            },
        ],
    },
    {
        "slug": "veto_2012_sfu",
        "set_title": "VETO 2012",
        "packet_title": "Team SFU Question Packet",
        "pdf_url": "https://files.quizbowlpackets.com/532/SFU.pdf",
        "entries": [
            {
                "bonus_number": 1,
                "source_page": 16,
                "prompt": "Use the labeled Austria map to identify the neighboring country marked by each letter.",
                "reasoning_focus": "map_label_matching",
                "parts": [
                    {"label": "A", "question": "Which neighboring country is labeled A on the Austria map?", "answer": "Germany"},
                    {"label": "B", "question": "Which neighboring country is labeled B on the Austria map?", "answer": "Czech Republic"},
                    {"label": "C", "question": "Which neighboring country is labeled C on the Austria map?", "answer": "Slovakia"},
                    {"label": "D", "question": "Which neighboring country is labeled D on the Austria map?", "answer": "Hungary"},
                    {"label": "E", "question": "Which neighboring country is labeled E on the Austria map?", "answer": "Slovenia"},
                    {"label": "F", "question": "Which neighboring country is labeled F on the Austria map?", "answer": "Italy"},
                    {"label": "G", "question": "Which neighboring country is labeled G on the Austria map?", "answer": "Switzerland"},
                    {"label": "H", "question": "Which neighboring country is labeled H on the Austria map?", "answer": "Lichtenstein"},
                ],
            },
            {
                "bonus_number": 2,
                "source_page": 17,
                "prompt": "Identify the mathematical function represented by each labeled graph.",
                "reasoning_focus": "graph_function_identification",
                "parts": [
                    {"label": "A", "question": "Which mathematical function is labeled A?", "answer": "Natural Logarithm"},
                    {"label": "B", "question": "Which mathematical function is labeled B?", "answer": "Exponential function OR e to the x"},
                    {"label": "C", "question": "Which mathematical function is labeled C?", "answer": "Sine function"},
                ],
            },
        ],
    },
    {
        "slug": "veto_2017_jbgp",
        "set_title": "VETO 2017",
        "packet_title": "BBGJP",
        "pdf_url": "https://files.quizbowlpackets.com/2172/JBGP.pdf",
        "entries": [
            {
                "bonus_number": 1,
                "source_page": 20,
                "prompt": "Identify the herb or plant segment shown in each labeled part of the handout.",
                "reasoning_focus": "botanical_visual_identification",
                "parts": [
                    {"label": "A", "question": "Which herb or plant segment is labeled A?", "answer": "Thyme OR Thymus vulgaris"},
                    {"label": "B", "question": "Which herb or plant segment is labeled B?", "answer": "Summer Savory OR Satureja hortensis"},
                    {"label": "C", "question": "Which herb or plant segment is labeled C?", "answer": "Shiso OR Perilla frutescens var. crispa"},
                    {"label": "D", "question": "Which herb or plant segment is labeled D?", "answer": "Lovage OR Levisticum officinale"},
                    {"label": "E", "question": "Which herb or plant segment is labeled E?", "answer": "Chocolate Mint"},
                    {"label": "F", "question": "Which herb or plant segment is labeled F?", "answer": "Peppermint OR Mentha x piperita"},
                ],
            },
            {
                "bonus_number": 2,
                "source_page": 21,
                "prompt": "From north to south, identify the principal labeled islands of Japan on the map.",
                "reasoning_focus": "map_based_geographic_reasoning",
                "parts": [
                    {"label": "north", "question": "Which principal island of Japan is the northernmost labeled island on the map?", "answer": "Hokkaido"},
                    {"label": "second", "question": "Which principal island of Japan is the second labeled island from north to south?", "answer": "Honshu"},
                    {"label": "third", "question": "Which principal island of Japan is the third labeled island from north to south?", "answer": "Shikoku"},
                    {"label": "south", "question": "Which principal island of Japan is the southernmost labeled island on the map?", "answer": "Kyushu"},
                ],
            },
        ],
    },
    {
        "slug": "st_louis_open_1999_round_15",
        "set_title": "ST. LOUIS OPEN - 1999",
        "packet_title": "Round 15",
        "pdf_url": "https://files.quizbowlpackets.com/1079/15.pdf",
        "entries": [
            {
                "bonus_number": 11,
                "prompt": "Identify the Constantin Brancusi sculpture shown on the handout page.",
                "reasoning_focus": "visual_fine_arts_identification",
                "parts": [
                    {"label": "A", "source_page": 12, "question": "Which Constantin Brancusi sculpture is shown on page A?", "answer": "Bird in Space"},
                    {"label": "B", "source_page": 13, "question": "Which Constantin Brancusi sculpture is shown on page B?", "answer": "The Kiss"},
                    {"label": "C", "source_page": 14, "question": "Which Constantin Brancusi sculpture is shown on page C?", "answer": "Sleeping Muse"},
                ],
            },
        ],
    },
    {
        "slug": "grim_up_north_2025_corn_exchanges",
        "set_title": "2025 GRIM UP NORTH",
        "packet_title": "Corn Exchanges Picture Bonus",
        "pdf_url": "https://files.quizbowlpackets.com/3436/pictures-bonus-cornex.pdf",
        "entries": [
            {
                "bonus_number": 6,
                "source_page": 1,
                "prompt": "Identify the city containing each pictured corn exchange.",
                "reasoning_focus": "architectural_landmark_location_identification",
                "parts": [
                    {"label": "10h", "question": "Which city contains the corn exchange labeled [10h]?", "answer": "Doncaster"},
                    {"label": "10e", "question": "Which city contains the corn exchange labeled [10e]?", "answer": "Leeds"},
                    {"label": "10m", "question": "Which city contains the corn exchange labeled [10m]?", "answer": "Manchester"},
                ],
            },
        ],
    },
    {
        "slug": "grim_up_north_2025_county_flags",
        "set_title": "2025 GRIM UP NORTH",
        "packet_title": "County Flags Picture Bonus",
        "pdf_url": "https://files.quizbowlpackets.com/3436/pictures-bonus-flags.pdf",
        "entries": [
            {
                "bonus_number": 19,
                "source_page": 1,
                "prompt": "Identify the county represented by each pictured flag.",
                "reasoning_focus": "flag_identification",
                "parts": [
                    {
                        "label": "top_left",
                        "question": "Which county's flag is shown in the top-left position?",
                        "answer": "Northumberland",
                    },
                    {
                        "label": "top_right",
                        "question": "Which county's flag is shown in the top-right position?",
                        "answer": "Yorkshire",
                    },
                    {
                        "label": "middle_left",
                        "question": "Which county's flag is shown in the middle-left position?",
                        "answer": "Cheshire",
                    },
                    {
                        "label": "middle_right",
                        "question": "Which county's flag is shown in the middle-right position?",
                        "answer": "County Durham",
                    },
                    {
                        "label": "bottom_left",
                        "question": "Which county's flag is shown in the bottom-left position?",
                        "answer": "Cleveland",
                    },
                    {
                        "label": "bottom_right",
                        "question": "Which county's flag is shown in the bottom-right position?",
                        "answer": "Merseyside",
                    },
                ],
            },
        ],
    },
]


def download_pdf(url: str, out_path: Path) -> None:
    print(f"Downloading {url}")
    response = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()

    content_type = response.headers.get("content-type", "")
    if "pdf" not in content_type.lower() and not response.content.startswith(b"%PDF"):
        raise ValueError(f"Expected a PDF from {url}, got {content_type!r}")

    out_path.write_bytes(response.content)


def sanitize_slug(value: str) -> str:
    return "".join(character if character.isalnum() else "_" for character in value.lower()).strip("_")


def render_page(pdf_path: Path, page_number: int, filename_base: str) -> str:
    rendered_pages = convert_from_path(
        pdf_path,
        dpi=RENDER_DPI,
        first_page=page_number,
        last_page=page_number,
        fmt="jpeg",
    )
    if not rendered_pages:
        raise ValueError(f"Failed to render page {page_number} from {pdf_path}")

    IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    output_path = IMAGES_DIR / f"{filename_base}.jpg"
    rendered_pages[0].save(output_path, "JPEG")
    try:
        return output_path.resolve().relative_to(ROOT_DIR).as_posix()
    except ValueError:
        return output_path.as_posix()


def build_dataset(temp_dir: Path) -> list[dict]:
    dataset = []
    next_id = 1

    for source in SOURCES:
        pdf_path = temp_dir / f"{source['slug']}.pdf"
        download_pdf(source["pdf_url"], pdf_path)

        rendered_pages: dict[int, str] = {}
        for entry in source["entries"]:
            for part in entry["parts"]:
                source_page = part.get("source_page") or entry.get("source_page")
                if source_page is None:
                    raise ValueError(
                        f"Missing source_page for {source['slug']} bonus {entry['bonus_number']} part {part['label']}"
                    )
                if source_page not in rendered_pages:
                    filename_base = f"{source['slug']}_page_{source_page:02d}"
                    rendered_pages[source_page] = render_page(pdf_path, source_page, filename_base)

                dataset.append(
                    {
                        "id": next_id,
                        "question": part["question"],
                        "quiz_title": source["packet_title"],
                        "media_url": rendered_pages[source_page],
                        "answer": part["answer"],
                        "source": {
                            "platform": SOURCE_PLATFORM,
                            "url": source["pdf_url"],
                            "set_title": source["set_title"],
                            "packet_title": source["packet_title"],
                            "quiz_type": "Visual Bonus",
                            "bonus_number": entry["bonus_number"],
                            "part_label": part["label"],
                            "source_page": source_page,
                            "reasoning_focus": entry["reasoning_focus"],
                            "shared_prompt": entry["prompt"],
                        },
                    }
                )
                next_id += 1

    return dataset


def main() -> None:
    with tempfile.TemporaryDirectory() as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        dataset = build_dataset(temp_dir)

    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(dataset, indent=4), encoding="utf-8")
    print(f"Saved {len(dataset)} questions to {OUTPUT_JSON}")


if __name__ == "__main__":
    main()
