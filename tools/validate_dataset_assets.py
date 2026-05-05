import json
from pathlib import Path
from urllib.parse import urlparse


ROOT_DIR = Path(__file__).resolve().parents[1]
DATASET_DIR = ROOT_DIR / "dataset"
MEDIA_FIELDS = ("image_url", "media_url")
TEXT_MEDIA_TYPES = {"text", "text_only"}


def is_remote_url(value: str) -> bool:
    return urlparse(value).scheme in {"http", "https"}


def source_label(record: dict) -> str:
    source = record.get("source")
    if isinstance(source, dict):
        return str(source.get("platform") or source.get("source") or source.get("url") or "unknown")
    return str(source or "unknown")


def validate_dataset(path: Path) -> tuple[list[str], list[str]]:
    records = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(records, list):
        return [], [f"{path.relative_to(ROOT_DIR)} is not a JSON list"]

    notes = []
    errors = []
    source_counts: dict[str, int] = {}
    media_counts: dict[str, int] = {}

    for index, record in enumerate(records, start=1):
        if not isinstance(record, dict):
            errors.append(f"{path.relative_to(ROOT_DIR)} record {index} is not an object")
            continue

        label = source_label(record)
        source_counts[label] = source_counts.get(label, 0) + 1
        media_type = str(record.get("media_type") or "").lower()
        present_media_fields = [field for field in MEDIA_FIELDS if record.get(field)]

        if not present_media_fields:
            if media_type not in TEXT_MEDIA_TYPES:
                errors.append(
                    f"{path.relative_to(ROOT_DIR)} record {index} ({label}) has no media field and is not marked text"
                )
            continue

        media_counts[label] = media_counts.get(label, 0) + len(present_media_fields)

        for field in present_media_fields:
            value = record[field]
            if not isinstance(value, str):
                errors.append(f"{path.relative_to(ROOT_DIR)} record {index} {field} is not a string")
                continue
            if is_remote_url(value):
                errors.append(f"{path.relative_to(ROOT_DIR)} record {index} {field} is remote: {value}")
                continue

            asset_path = Path(value)
            if not asset_path.is_absolute():
                asset_path = ROOT_DIR / asset_path
            if not asset_path.exists():
                errors.append(f"{path.relative_to(ROOT_DIR)} record {index} {field} is missing: {value}")

    text_only_sources = sorted(
        source for source, count in source_counts.items() if source not in media_counts and count > 0
    )
    if text_only_sources:
        notes.append(
            f"{path.relative_to(ROOT_DIR)} has text-only source(s): {', '.join(text_only_sources)}"
        )

    return notes, errors


def main() -> int:
    all_notes = []
    all_errors = []

    for path in sorted(DATASET_DIR.glob("*.json")):
        notes, errors = validate_dataset(path)
        all_notes.extend(notes)
        all_errors.extend(errors)

    for note in all_notes:
        print(f"NOTE: {note}")
    for error in all_errors:
        print(f"ERROR: {error}")

    print(
        f"Checked {len(list(DATASET_DIR.glob('*.json')))} dataset files: "
        f"{len(all_errors)} error(s), {len(all_notes)} note(s)."
    )
    return 1 if all_errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
