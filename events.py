"""
Events utilities for extracting and managing parish events from bulletins.
"""

import json
import random
import string
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def generate_event_id():
    """
    Generate 8-character base36 random ID.
    Example: 'k7m2x9p1'
    """
    chars = string.digits + string.ascii_lowercase
    return ''.join(random.choice(chars) for _ in range(8))


def load_events_json(events_path):
    """
    Load existing events from JSON file.
    Returns empty list if file doesn't exist.
    """
    try:
        with open(events_path, 'r', encoding='utf-8') as f:
            events = json.load(f)
            logger.info(f"Loaded {len(events)} existing events from {events_path}")
            return events
    except FileNotFoundError:
        logger.info(f"No existing events file at {events_path}, starting fresh")
        return []
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse events JSON: {e}")
        return []


def save_events_json(events, events_path):
    """Save events to JSON file."""
    with open(events_path, 'w', encoding='utf-8') as f:
        json.dump(events, f, indent=4, ensure_ascii=False)
    logger.info(f"Saved {len(events)} events to {events_path}")


def filter_events_for_family(events, family_of_parishes):
    """
    Filter existing events to those belonging to a specific family of parishes.
    Used to provide context to LLM for deduplication.
    """
    if not family_of_parishes:
        return []
    
    filtered = [
        e for e in events 
        if e.get('family_of_parishes') == family_of_parishes
    ]
    logger.debug(f"Filtered {len(filtered)} events for family: {family_of_parishes}")
    return filtered


def get_family_of_parishes(churches_for_bulletin):
    """
    Get the family of parishes from a list of churches sharing a bulletin.
    Returns the first non-empty familyOfParishes found.
    """
    for church in churches_for_bulletin:
        fop = church.get('familyOfParishes')
        if fop:
            return fop
    return None


def prepare_churches_context(churches_for_bulletin):
    """
    Prepare a simplified churches context for the LLM prompt.
    Only includes relevant fields for event extraction.
    """
    return [
        {
            'id': c.get('id'),
            'name': c.get('name'),
            'familyOfParishes': c.get('familyOfParishes')
        }
        for c in churches_for_bulletin
    ]


def prepare_existing_events_context(existing_events):
    """
    Prepare existing events for LLM context.
    Only includes fields needed for deduplication matching.
    """
    return [
        {
            'id': e.get('id'),
            'title': e.get('title'),
            'date': e.get('date'),
            'church_id': e.get('church_id'),
            'church_name': e.get('church_name'),
            'start_time': e.get('start_time')
        }
        for e in existing_events
    ]


def merge_events(existing_events, new_events):
    """
    Merge new events with existing events.
    - Events with matching IDs are updated (new overwrites old)
    - Events with id=None get a new generated ID
    - Returns merged list
    """
    # Build a dict of existing events by ID
    events_by_id = {e['id']: e for e in existing_events}
    
    new_count = 0
    updated_count = 0
    
    for event in new_events:
        event_id = event.get('id')
        
        if event_id is None:
            # New event - generate ID
            event_id = generate_event_id()
            event['id'] = event_id
            new_count += 1
        elif event_id in events_by_id:
            # Existing event - will be updated
            updated_count += 1
        else:
            # ID provided but not in existing (shouldn't happen normally)
            new_count += 1
        
        events_by_id[event_id] = event
    
    logger.info(f"Merged events: {new_count} new, {updated_count} updated")
    return list(events_by_id.values())


def add_event_metadata(event, pdf_link, bulletin_date=None):
    """
    Add metadata fields to an extracted event.
    """
    event['source_bulletin_link'] = pdf_link
    event['source_bulletin_date'] = bulletin_date or datetime.now().strftime('%Y-%m-%d')
    event['extracted_at'] = datetime.now().isoformat()
    return event
