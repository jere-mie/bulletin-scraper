from __future__ import annotations

from copy import deepcopy
import logging
import random
import re
import string
from datetime import date, datetime, timedelta
from typing import Any


logger = logging.getLogger(__name__)


def generate_event_id() -> str:
    chars = string.digits + string.ascii_lowercase
    return "".join(random.choice(chars) for _ in range(8))


def filter_events_for_family(events: list[dict[str, Any]], family_of_parishes: str | None) -> list[dict[str, Any]]:
    if not family_of_parishes:
        return []
    filtered = [event for event in events if event.get("family_of_parishes") == family_of_parishes]
    logger.debug("Filtered %s events for family %s", len(filtered), family_of_parishes)
    return filtered


def merge_events(existing_events: list[dict[str, Any]], new_events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged_events = [deepcopy(event) for event in existing_events]
    index_by_id = {
        event.get("id"): index
        for index, event in enumerate(merged_events)
        if event.get("id")
    }
    new_count = 0
    updated_count = 0

    for event in new_events:
        event_copy = deepcopy(event)
        event_id = event_copy.get("id")
        if event_id and event_id in index_by_id:
            updated_count += 1
            merged_events[index_by_id[event_id]] = _merge_event_records(merged_events[index_by_id[event_id]], event_copy)
            continue

        matched_index = _find_matching_event_index(event_copy, merged_events)
        matched_id = merged_events[matched_index].get("id") if matched_index is not None else None
        if matched_id:
            updated_count += 1
            event_copy["id"] = matched_id
            merged_events[matched_index] = _merge_event_records(merged_events[matched_index], event_copy)
            continue

        if event_id is None:
            event_id = generate_event_id()
            event_copy["id"] = event_id
        new_count += 1
        index_by_id[event_id] = len(merged_events)
        merged_events.append(event_copy)

    logger.info("Merged events: %s new, %s updated", new_count, updated_count)
    return merged_events


def add_event_metadata(event: dict[str, Any], pdf_link: str | None, bulletin_date: str | None = None) -> dict[str, Any]:
    event["source_bulletin_link"] = pdf_link
    event["source_bulletin_date"] = bulletin_date or datetime.now().strftime("%Y-%m-%d")
    event["extracted_at"] = datetime.now().isoformat()
    return event


def dedupe_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    for event in events:
        matched_index = next(
            (index for index, existing in enumerate(merged) if _events_match(existing, event)),
            None,
        )
        if matched_index is None:
            merged.append(deepcopy(event))
            continue
        merged[matched_index] = _merge_event_records(merged[matched_index], event)
    return merged


def filter_event_candidates(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for event in events:
        if _is_non_event_notice(event):
            continue
        filtered.append(event)
    return filtered


def prune_stale_events(events: list[dict[str, Any]], bulletin_date: str | None) -> list[dict[str, Any]]:
    kept = [event for event in events if _event_is_recent_enough(event)]
    return kept


def sort_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        events,
        key=lambda event: (
            event.get("date") or "9999-12-31",
            event.get("start_time") or "9999",
            _normalize_text(event.get("title")),
        ),
    )


def _find_matching_event_index(event: dict[str, Any], existing_events: list[dict[str, Any]]) -> int | None:
    for index, existing in enumerate(existing_events):
        if _events_match(existing, event):
            return index
    return None


def _events_match(left: dict[str, Any], right: dict[str, Any]) -> bool:
    left_title = _normalize_text(left.get("title"))
    right_title = _normalize_text(right.get("title"))
    if not left_title or left_title != right_title:
        return False
    left_date = left.get("date")
    right_date = right.get("date")
    if left_date and right_date and left_date != right_date:
        return False
    left_time = left.get("start_time")
    right_time = right.get("start_time")
    if left_time and right_time and left_time != right_time:
        return False
    left_location = _normalize_text(left.get("location"))
    right_location = _normalize_text(right.get("location"))
    if left_location and right_location and left_location != right_location:
        if left_location not in right_location and right_location not in left_location:
            return False
    return True


def duplicate_event_count(events: list[dict[str, Any]]) -> int:
    seen: set[tuple[str, str | None]] = set()
    duplicates = 0
    for event in events:
        key = (_normalize_text(event.get("title")), event.get("date"))
        if not key[0]:
            continue
        if key in seen:
            duplicates += 1
            continue
        seen.add(key)
    return duplicates


def _merge_event_records(existing: dict[str, Any], new_event: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(existing)
    for field_name in ("title", "description", "date", "start_time", "end_time", "location"):
        merged[field_name] = _prefer_richer_text(merged.get(field_name), new_event.get(field_name))

    existing_church_id = merged.get("church_id")
    new_church_id = new_event.get("church_id")
    if not existing_church_id:
        merged["church_id"] = new_church_id
    elif new_church_id and new_church_id != existing_church_id:
        merged["church_id"] = None

    existing_church_name = merged.get("church_name")
    new_church_name = new_event.get("church_name")
    if not existing_church_name:
        merged["church_name"] = new_church_name
    elif new_church_name and new_church_name != existing_church_name:
        merged["church_name"] = None

    merged["tags"] = sorted(set(merged.get("tags", [])) | set(new_event.get("tags", [])))

    families = _merge_string_lists(
        merged.get("families"),
        [merged.get("family_of_parishes")],
        new_event.get("families"),
        [new_event.get("family_of_parishes")],
    )
    if families:
        merged["families"] = families
        merged["family_of_parishes"] = families[0] if len(families) == 1 else None

    bulletin_links = _merge_string_lists(
        merged.get("source_bulletin_links"),
        [merged.get("source_bulletin_link")],
        new_event.get("source_bulletin_links"),
        [new_event.get("source_bulletin_link")],
    )
    if bulletin_links:
        merged["source_bulletin_links"] = bulletin_links
        merged["source_bulletin_link"] = bulletin_links[-1]

    merged["source_bulletin_date"] = max(
        filter(None, [merged.get("source_bulletin_date"), new_event.get("source_bulletin_date")]),
        default=None,
    )
    merged["extracted_at"] = max(
        filter(None, [merged.get("extracted_at"), new_event.get("extracted_at")]),
        default=None,
    )
    merged["id"] = merged.get("id") or new_event.get("id") or generate_event_id()
    return merged


def _event_is_recent_enough(event: dict[str, Any]) -> bool:
    event_date = _parse_iso_date(event.get("date"))
    if event_date is None:
        return True
    reference_date = _parse_iso_date(event.get("source_bulletin_date"))
    if reference_date is None:
        return True
    cutoff = reference_date - timedelta(days=7)
    return event_date >= cutoff


def _is_non_event_notice(event: dict[str, Any]) -> bool:
    title = _normalize_text(event.get("title"))
    description = _normalize_text(event.get("description"))
    combined = f"{title} {description}".strip()
    tags = {str(tag).casefold() for tag in event.get("tags", [])}

    positive_keywords = {
        "fundraiser",
        "sale",
        "camp",
        "conference",
        "retreat",
        "picnic",
        "drive",
        "school",
        "dinner",
        "gala",
        "meeting",
        "session",
        "open house",
        "yard sale",
        "pilgrimage",
        "lunch",
        "ultreya",
        "bundle",
    }
    negative_keywords = {
        "mass",
        "holy hour",
        "adoration",
        "rosary",
        "novena",
        "confession",
        "benediction",
    }

    has_positive_keyword = any(keyword in combined for keyword in positive_keywords)
    has_negative_keyword = any(keyword in combined for keyword in negative_keywords)
    has_non_event_tags = tags.issubset({"liturgy", "seasonal", "sacramental", "community"}) and tags

    if has_negative_keyword and not has_positive_keyword:
        return True
    if has_non_event_tags and not has_positive_keyword:
        return True
    return False


def _merge_string_lists(*groups: list[str] | None) -> list[str]:
    merged: list[str] = []
    for group in groups:
        if not group:
            continue
        for item in group:
            if not item:
                continue
            if item not in merged:
                merged.append(item)
    return merged


def _prefer_richer_text(current: Any, candidate: Any) -> Any:
    if current in (None, "", []):
        return candidate
    if candidate in (None, "", []):
        return current
    if isinstance(current, str) and isinstance(candidate, str):
        return candidate if len(candidate.strip()) > len(current.strip()) else current
    return current or candidate


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"[^a-z0-9]+", " ", str(value).casefold()).strip()


def _parse_iso_date(value: Any) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None