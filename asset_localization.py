import mimetypes
import re
from pathlib import Path
from urllib.parse import urlparse

import requests


REQUEST_TIMEOUT_SECONDS = 60
CHUNK_SIZE = 65536
DEFAULT_EXTENSION = ".bin"
CONTENT_TYPE_EXTENSIONS = {
    "audio/mpeg": ".mp3",
    "audio/wav": ".wav",
    "audio/x-wav": ".wav",
    "image/gif": ".gif",
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/svg+xml": ".svg",
    "image/webp": ".webp",
}

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": "ScraperAssetLocalizer/1.0",
        "Accept-Language": "en-US,en;q=0.9",
    }
)


def sanitize_filename(value: str) -> str:
    filename = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value or "")).strip("._")
    return filename or "asset"


def infer_extension(url: str, content_type: str | None) -> str:
    normalized_content_type = (content_type or "").split(";", 1)[0].strip().lower()
    if normalized_content_type in CONTENT_TYPE_EXTENSIONS:
        return CONTENT_TYPE_EXTENSIONS[normalized_content_type]

    guessed_extension = mimetypes.guess_extension(normalized_content_type, strict=False) if normalized_content_type else None
    if guessed_extension == ".jpe":
        return ".jpg"
    if guessed_extension:
        return guessed_extension

    suffix = Path(urlparse(url).path).suffix.lower()
    if suffix:
        return suffix

    return DEFAULT_EXTENSION


def download_asset(url: str, output_dir: Path, filename_base: str) -> str | None:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return None

    response = None
    try:
        response = SESSION.get(url, timeout=REQUEST_TIMEOUT_SECONDS, stream=True)
        response.raise_for_status()

        extension = infer_extension(url, response.headers.get("Content-Type"))
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"{sanitize_filename(filename_base)}{extension}"

        if output_path.exists():
            return output_path.as_posix()

        with output_path.open("wb") as file:
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:
                    file.write(chunk)

        return output_path.as_posix()
    except requests.RequestException as exc:
        print(f"Failed to download asset {url}: {exc}")
        return None
    finally:
        if response is not None:
            response.close()


def download_assets(urls: list[str], output_dir: Path, filename_base: str) -> list[str]:
    local_paths = []

    for index, url in enumerate(urls, start=1):
        suffix = "" if len(urls) == 1 else f"_{index:02d}"
        local_path = download_asset(url, output_dir, f"{filename_base}{suffix}")
        if local_path:
            local_paths.append(local_path)

    return local_paths
