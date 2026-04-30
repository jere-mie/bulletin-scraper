from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from . import scraping
from .config import AppPaths, RunConfig
from .models import BulletinDocument, BulletinFamily, InputArtifact, InputMode
from .pdf_to_images import convert_pdf_to_images, get_pdf_page_count
from .schemas import BulletinCacheEntry, BulletinCacheManifest


def load_data_bundle(paths: AppPaths) -> dict[str, Any]:
    return {
        "churches": _load_json(paths.churches_path, default=[]),
        "events": _load_json(paths.events_path, default=[]),
        "intentions": _load_json(paths.intentions_path, default=[]),
    }


def save_data_bundle(paths: AppPaths, bundle: dict[str, Any]) -> None:
    _save_json(paths.churches_path, bundle["churches"])
    _save_json(paths.events_path, bundle["events"])
    _save_json(paths.intentions_path, bundle["intentions"])


def build_families(churches: list[dict[str, Any]]) -> list[BulletinFamily]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for church in churches:
        website = church.get("bulletin_website")
        if not website or website == "N/A":
            continue
        grouped.setdefault(website, []).append(church)

    families: list[BulletinFamily] = []
    for website, grouped_churches in grouped.items():
        family_name = next(
            (church.get("familyOfParishes") for church in grouped_churches if church.get("familyOfParishes")),
            None,
        )
        if not family_name:
            family_name = " / ".join(church.get("name", "Unknown") for church in grouped_churches)
        family_id = slugify(family_name)
        families.append(
            BulletinFamily(
                family_id=family_id,
                name=family_name,
                bulletin_website=website,
                churches=sorted(grouped_churches, key=lambda church: church.get("name", "")),
            )
        )

    families.sort(key=lambda family: family.family_id)
    return families


def filter_families(families: list[BulletinFamily], config: RunConfig) -> list[BulletinFamily]:
    filtered = families
    if config.family_filter:
        needle = config.family_filter.casefold()
        filtered = [
            family
            for family in filtered
            if needle in family.family_id.casefold() or needle in family.name.casefold()
        ]
    if config.family_limit is not None:
        filtered = filtered[: config.family_limit]
    return filtered


def ensure_family_documents(
    families: list[BulletinFamily],
    paths: AppPaths,
    config: RunConfig,
    logger: logging.Logger,
) -> list[BulletinFamily]:
    paths.bulletins_dir.mkdir(parents=True, exist_ok=True)
    cache = _load_bulletin_cache(paths.bulletin_cache_path)
    now = datetime.now().isoformat(timespec="seconds")
    today = now[:10]

    ready: list[BulletinFamily] = []
    for family in families:
        pdf_path = paths.bulletins_dir / f"{family.family_id}.pdf"
        entry = cache.families.get(family.family_id)
        if entry is None:
            entry = BulletinCacheEntry(
                family_id=family.family_id,
                family_name=family.name,
                bulletin_website=family.bulletin_website,
                primary_website=_primary_website(family),
                pdf_path=pdf_path,
            )
            cache.families[family.family_id] = entry
        else:
            entry.family_name = family.name
            entry.bulletin_website = family.bulletin_website
            entry.primary_website = _primary_website(family)
            entry.pdf_path = pdf_path

        if pdf_path.exists() and not entry.last_downloaded_at:
            entry.last_downloaded_at = _timestamp_from_mtime(pdf_path)
        if pdf_path.exists() and not entry.last_scraped_at:
            entry.last_scraped_at = entry.last_downloaded_at

        if _should_reuse_cached_pdf(entry, pdf_path, config, today):
            bulletin_date = entry.bulletin_date or _resolve_bulletin_date(entry.pdf_url, pdf_path)
            entry.bulletin_date = bulletin_date
            family.document = BulletinDocument(
                website=family.bulletin_website,
                pdf_link=entry.pdf_url,
                pdf_path=pdf_path,
                bulletin_date=bulletin_date,
            )
            entry.status = "cached"
            entry.error = None
            entry.last_reused_at = now
            ready.append(family)
            continue

        entry.last_attempted_at = now
        pdf_link = scraping.scrape_bulletin_with_retry(family.name, family.bulletin_website)
        entry.last_scraped_at = now
        entry.pdf_url = pdf_link

        if not pdf_link:
            entry.status = "scrape_failed"
            entry.error = "No bulletin PDF found on the bulletin website."
            logger.warning("No bulletin PDF found for family %s", family.family_id)
            continue

        if scraping.download_pdf(pdf_link, str(pdf_path)):
            bulletin_date = _resolve_bulletin_date(pdf_link, pdf_path)
            family.document = BulletinDocument(
                website=family.bulletin_website,
                pdf_link=pdf_link,
                pdf_path=pdf_path,
                bulletin_date=bulletin_date,
            )
            entry.bulletin_date = bulletin_date
            entry.status = "downloaded"
            entry.error = None
            entry.last_downloaded_at = now
            ready.append(family)
        else:
            entry.status = "download_failed"
            entry.error = f"Failed to download bulletin from {pdf_link}."
            logger.warning("Failed to download bulletin for family %s", family.family_id)

    cache.updated_at = now
    _save_bulletin_cache(paths.bulletin_cache_path, cache)
    return ready


def build_input_artifact(
    family: BulletinFamily,
    input_mode: InputMode,
    artifacts_dir: Path,
    max_pages: int,
) -> InputArtifact:
    if not family.document:
        raise ValueError(f"Family {family.family_id} is missing a bulletin document.")

    pdf_path = family.document.pdf_path
    if input_mode is InputMode.PDF:
        return InputArtifact(
            mode=input_mode,
            payload=pdf_path,
            description=f"PDF upload from {pdf_path.name}",
            page_count=get_pdf_page_count(str(pdf_path)) or None,
        )

    if input_mode is InputMode.IMAGES:
        image_dir = artifacts_dir / family.family_id / "images"
        image_paths = convert_pdf_to_images(str(pdf_path), output_dir=str(image_dir), max_pages=max_pages)
        return InputArtifact(
            mode=input_mode,
            payload=[Path(path) for path in image_paths],
            description=f"{len(image_paths)} page image(s)",
            page_count=len(image_paths),
        )

    text, page_count = extract_text_from_pdf(pdf_path, max_pages=max_pages)
    preview = text[:1000] if text else None
    if input_mode is InputMode.TEXT_IMAGES:
        image_dir = artifacts_dir / family.family_id / "text-images"
        image_paths = convert_pdf_to_images(str(pdf_path), output_dir=str(image_dir), max_pages=max_pages)
        return InputArtifact(
            mode=input_mode,
            payload={
                "text": text,
                "images": [Path(path) for path in image_paths],
            },
            description=f"{len(image_paths)} page image(s) plus extracted text",
            page_count=max(page_count, len(image_paths)),
            text_preview=preview,
        )

    return InputArtifact(
        mode=input_mode,
        payload=text,
        description=f"OCR/text extraction from {pdf_path.name}",
        page_count=page_count,
        text_preview=preview,
    )


def extract_text_from_pdf(pdf_path: Path, max_pages: int | None = None) -> tuple[str, int]:
    try:
        import fitz
    except ImportError as exc:
        raise RuntimeError("PyMuPDF is required for text extraction mode.") from exc

    document = fitz.open(pdf_path)
    try:
        page_count = len(document)
        pages_to_read = min(page_count, max_pages) if max_pages else page_count
        page_texts: list[str] = []
        for page_number in range(pages_to_read):
            page_text = document[page_number].get_text("text")
            clean_text = re.sub(r"\n{3,}", "\n\n", page_text).strip()
            page_texts.append(f"--- Page {page_number + 1} ---\n{clean_text}")
        return "\n\n".join(page_texts), pages_to_read
    finally:
        document.close()


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.casefold()).strip("-")
    return slug or "family"


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=4, ensure_ascii=False)


def _load_bulletin_cache(path: Path) -> BulletinCacheManifest:
    if not path.exists():
        return BulletinCacheManifest()
    with path.open("r", encoding="utf-8") as handle:
        return BulletinCacheManifest.model_validate(json.load(handle))


def _save_bulletin_cache(path: Path, cache: BulletinCacheManifest) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(cache.model_dump(mode="json"), handle, indent=2, ensure_ascii=False)


def _should_reuse_cached_pdf(
    entry: BulletinCacheEntry,
    pdf_path: Path,
    config: RunConfig,
    today: str,
) -> bool:
    if not pdf_path.exists():
        return False
    if config.refresh_bulletins:
        return False
    if config.use_existing_bulletins:
        return True
    if entry.status not in {"cached", "downloaded", "missing"}:
        return False
    return any(
        timestamp is not None and timestamp.startswith(today)
        for timestamp in (entry.last_scraped_at, entry.last_downloaded_at)
    )


def _primary_website(family: BulletinFamily) -> str | None:
    for church in family.churches:
        website = church.get("website")
        if website and website != "N/A":
            return website
    return None


def _timestamp_from_mtime(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds")


def _resolve_bulletin_date(pdf_link: str | None, pdf_path: Path) -> str | None:
    for candidate in (_candidate_name_from_link(pdf_link), pdf_path.name):
        if not candidate:
            continue
        resolved = _extract_date_from_text(candidate)
        if resolved:
            return resolved
    return None


def _candidate_name_from_link(pdf_link: str | None) -> str | None:
    if not pdf_link:
        return None
    parsed = urlparse(pdf_link)
    return unquote(Path(parsed.path).name)


def _extract_date_from_text(text: str) -> str | None:
    compact_match = re.search(r"(?<!\d)(20\d{2})(\d{2})(\d{2})(?!\d)", text)
    if compact_match:
        year, month, day = compact_match.groups()
        return f"{year}-{month}-{day}"

    month_match = re.search(
        r"(?P<month>[A-Za-z]+)[ _-]+(?P<day>\d{1,2})(?:st|nd|rd|th)?(?:,)?[ _-]+(?P<year>20\d{2})",
        text,
    )
    if month_match:
        month = month_match.group("month")
        day = month_match.group("day")
        year = month_match.group("year")
        for fmt in ("%B %d %Y", "%b %d %Y"):
            try:
                return datetime.strptime(f"{month} {day} {year}", fmt).date().isoformat()
            except ValueError:
                continue
    return None
