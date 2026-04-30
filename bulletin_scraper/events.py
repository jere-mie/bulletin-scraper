from __future__ import annotations

import logging
import random
import string
from datetime import datetime
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
    events_by_id = {event["id"]: event for event in existing_events if event.get("id")}
    new_count = 0
    updated_count = 0

    for event in new_events:
        event_id = event.get("id")
        if event_id is None:
            event_id = generate_event_id()
            event["id"] = event_id
            new_count += 1
        elif event_id in events_by_id:
            updated_count += 1
        else:
            new_count += 1
        events_by_id[event_id] = event

    logger.info("Merged events: %s new, %s updated", new_count, updated_count)
    return list(events_by_id.values())


def add_event_metadata(event: dict[str, Any], pdf_link: str | None, bulletin_date: str | None = None) -> dict[str, Any]:
    event["source_bulletin_link"] = pdf_link
    event["source_bulletin_date"] = bulletin_date or datetime.now().strftime("%Y-%m-%d")
    event["extracted_at"] = datetime.now().isoformat()
    return event