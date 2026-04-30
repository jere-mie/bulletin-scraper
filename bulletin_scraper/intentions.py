from __future__ import annotations

import logging
from datetime import datetime
from typing import Any


logger = logging.getLogger(__name__)


def add_intention_metadata(intention: dict[str, Any], pdf_link: str | None) -> dict[str, Any]:
    intention["source_bulletin_link"] = pdf_link
    intention["extracted_at"] = datetime.now().isoformat()
    return intention


def replace_family_intentions(
    existing_intentions: list[dict[str, Any]],
    family_church_ids: list[str],
    new_intentions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    family_church_id_set = set(family_church_ids)
    preserved = [entry for entry in existing_intentions if entry.get("church_id") not in family_church_id_set]
    combined = preserved + list(new_intentions)
    combined.sort(
        key=lambda intention: (
            intention.get("date") or "",
            intention.get("time") or "",
            intention.get("church_id") or "",
        )
    )
    logger.info(
        "Replaced intentions for %s church(es): kept %s unrelated entries, added %s new entries",
        len(family_church_id_set),
        len(preserved),
        len(new_intentions),
    )
    return combined


def normalize_intentions(payload: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, Any, Any], dict[str, Any]] = {}
    for entry in payload:
        key = (entry.get("church_id"), entry.get("date"), entry.get("time"))
        normalized_lines = _merge_intention_lines(entry.get("intentions", []))
        if key not in grouped:
            grouped[key] = {**entry, "intentions": normalized_lines}
            continue
        grouped[key]["intentions"] = _merge_intention_lines(grouped[key].get("intentions", []) + normalized_lines)

    combined = list(grouped.values())
    combined.sort(
        key=lambda intention: (
            intention.get("date") or "",
            intention.get("time") or "",
            intention.get("church_id") or "",
        )
    )
    return combined


def intention_quality(payload: list[dict[str, Any]]) -> tuple[int, int, int, int]:
    masses = len(payload)
    total_lines = sum(len(entry.get("intentions", [])) for entry in payload)
    normalized_for_values = {
        _normalize_party_name(line.get("for"))
        for entry in payload
        for line in entry.get("intentions", [])
        if _normalize_party_name(line.get("for"))
    }
    with_by = sum(
        1
        for entry in payload
        for line in entry.get("intentions", [])
        if line.get("by")
    )
    suspicious_by = sum(
        1
        for entry in payload
        for line in entry.get("intentions", [])
        if _normalize_party_name(line.get("by")) in normalized_for_values
    )
    return masses, total_lines, with_by, suspicious_by


def _merge_intention_lines(lines: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[tuple[str, str | None], dict[str, Any]] = {}
    for line in lines:
        intention_for = str(line.get("for") or "").strip()
        intention_by = line.get("by")
        key = (intention_for.casefold(), str(intention_by or "").strip().casefold() or None)
        existing = merged.get(key)
        if existing is None:
            merged[key] = {"for": intention_for, "by": intention_by}
            continue
        if not existing.get("by") and intention_by:
            merged[key] = {"for": intention_for, "by": intention_by}
    return list(merged.values())


def _normalize_party_name(value: Any) -> str:
    return str(value or "").strip().casefold()


def merge_intentions(
    existing_intentions: list[dict[str, Any]],
    new_intentions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    existing_by_key: dict[tuple[Any, Any, Any], dict[str, Any]] = {}
    for intention in existing_intentions:
        key = (intention.get("church_id"), intention.get("date"), intention.get("time"))
        existing_by_key[key] = intention

    new_count = 0
    updated_count = 0
    for intention in new_intentions:
        key = (intention.get("church_id"), intention.get("date"), intention.get("time"))
        if key in existing_by_key:
            updated_count += 1
        else:
            new_count += 1
        existing_by_key[key] = intention

    logger.info("Merged intentions: %s new, %s updated", new_count, updated_count)
    merged_intentions = list(existing_by_key.values())
    merged_intentions.sort(
        key=lambda intention: (
            intention.get("date") or "",
            intention.get("time") or "",
            intention.get("church_id") or "",
        )
    )
    return merged_intentions