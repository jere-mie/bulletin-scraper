from __future__ import annotations

import logging
from datetime import datetime
from typing import Any


logger = logging.getLogger(__name__)


def add_intention_metadata(intention: dict[str, Any], pdf_link: str | None) -> dict[str, Any]:
    intention["source_bulletin_link"] = pdf_link
    intention["extracted_at"] = datetime.now().isoformat()
    return intention


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