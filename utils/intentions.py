"""
Intentions utilities for extracting and managing Mass intentions from bulletins.
"""

import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def load_intentions_json(intentions_path):
    """
    Load existing intentions from JSON file.
    Returns empty list if file doesn't exist.
    """
    try:
        with open(intentions_path, 'r', encoding='utf-8') as f:
            intentions = json.load(f)
            logger.info(f"Loaded {len(intentions)} existing intentions from {intentions_path}")
            return intentions
    except FileNotFoundError:
        logger.info(f"No existing intentions file at {intentions_path}, starting fresh")
        return []
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse intentions JSON: {e}")
        return []


def save_intentions_json(intentions, intentions_path):
    """Save intentions to JSON file."""
    with open(intentions_path, 'w', encoding='utf-8') as f:
        json.dump(intentions, f, indent=4, ensure_ascii=False)
    logger.info(f"Saved {len(intentions)} Mass intentions to {intentions_path}")


def prepare_churches_context(churches_for_bulletin):
    """
    Prepare a simplified churches context for the LLM prompt.
    Only includes relevant fields for intention extraction.
    """
    return [
        {
            'id': c.get('id'),
            'name': c.get('name'),
            'masses': c.get('masses', []),
            'daily_masses': c.get('daily_masses', []),
        }
        for c in churches_for_bulletin
    ]


def add_intention_metadata(intention, pdf_link):
    """
    Add metadata fields to an extracted intention.
    """
    intention['source_bulletin_link'] = pdf_link
    intention['extracted_at'] = datetime.now().isoformat()
    return intention


def merge_intentions(existing_intentions, new_intentions):
    """
    Merge new intentions with existing intentions.
    Deduplicates by (church_id, date, time) composite key.
    If a Mass already exists in existing data, the new entry replaces it
    (bulletin may have updated intentions).
    Returns merged list.
    """
    # Build composite key for existing
    existing_by_key = {}
    for intention in existing_intentions:
        key = (intention.get('church_id'), intention.get('date'), intention.get('time'))
        existing_by_key[key] = intention

    new_count = 0
    updated_count = 0

    for intention in new_intentions:
        key = (intention.get('church_id'), intention.get('date'), intention.get('time'))
        if key in existing_by_key:
            updated_count += 1
        else:
            new_count += 1
        existing_by_key[key] = intention

    logger.info(f"Merged intentions: {new_count} new, {updated_count} updated")
    # Ensure deterministic ordering to avoid noisy diffs when new_intentions
    # come from parallel processing. Sort by date, then time, then church_id.
    merged_intentions = list(existing_by_key.values())
    merged_intentions.sort(
        key=lambda intention: (
            intention.get('date') or "",
            intention.get('time') or "",
            intention.get('church_id') or "",
        )
    )
    return merged_intentions
