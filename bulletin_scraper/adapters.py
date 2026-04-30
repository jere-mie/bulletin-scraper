from __future__ import annotations

from abc import ABC, abstractmethod
from copy import deepcopy
from datetime import datetime
from typing import Any

from . import events as events_utils
from . import intentions as intentions_utils
from .json_utils import pretty_json
from .models import BulletinFamily, TargetKind
from .schemas import CombinedPayload, EventsPayload, IntentionsPayload, ScheduleExtractionPayload, SchedulePayload


class TargetAdapter(ABC):
    kind: TargetKind
    max_pages = 8

    @abstractmethod
    def get_scope(self, bundle: dict[str, Any], family: BulletinFamily) -> dict[str, Any]:
        ...

    @abstractmethod
    def coerce_final_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        ...

    @abstractmethod
    def build_direct_prompt(self, family: BulletinFamily, scope: dict[str, Any]) -> str:
        ...

    @abstractmethod
    def build_extract_prompt(self, family: BulletinFamily, scope: dict[str, Any]) -> str:
        ...

    @abstractmethod
    def build_merge_prompt(self, family: BulletinFamily, scope: dict[str, Any], extracted: dict[str, Any]) -> str:
        ...

    @abstractmethod
    def build_review_prompt(self, family: BulletinFamily, scope: dict[str, Any], proposal: dict[str, Any]) -> str:
        ...

    @abstractmethod
    def apply(self, bundle: dict[str, Any], family: BulletinFamily, payload: dict[str, Any]) -> dict[str, Any]:
        ...

    @abstractmethod
    def summarize(self, payload: dict[str, Any], apply_details: dict[str, Any]) -> str:
        ...

    def coerce_extracted_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.coerce_final_payload(payload)

    def postprocess_output(self, scope: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        return payload

    def payload_size(self, payload: dict[str, Any]) -> int:
        return 0


class ScheduleAdapter(TargetAdapter):
    kind = TargetKind.SCHEDULE
    max_pages = 6

    def get_scope(self, bundle: dict[str, Any], family: BulletinFamily) -> dict[str, Any]:
        return {
            "churches": [
                {
                    "id": church.get("id"),
                    "name": church.get("name"),
                    "masses": church.get("masses", []),
                    "daily_masses": church.get("daily_masses", []),
                    "confession": church.get("confession", []),
                    "adoration": church.get("adoration", []),
                }
                for church in family.churches
            ]
        }

    def coerce_final_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        return SchedulePayload.model_validate(payload).model_dump(mode="json", by_alias=True)

    def coerce_extracted_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        return ScheduleExtractionPayload.model_validate(payload).model_dump(mode="json", by_alias=True)

    def build_direct_prompt(self, family: BulletinFamily, scope: dict[str, Any]) -> str:
        return f"""You are reviewing the regular weekly schedule for a Catholic family of parishes.

FAMILY:
{family.name}

CURRENT DATABASE SLICE:
{pretty_json(scope)}

TASK:
- Compare the bulletin to the current database slice.
- Return only confirmed changes for regular schedules.
- Only update these fields: masses, daily_masses, confession, adoration.
- Omit any church that does not need a change.
- If a field is uncertain, omit that field entirely.
- Ignore memorial masses, seasonal services, special holiday schedules, and one-off events.

OUTPUT JSON:
{{
  "church_updates": [
    {{
      "church_id": "church-id",
      "masses": [{{"day": "Sunday", "time": "1100"}}],
      "daily_masses": [{{"day": "Tuesday", "time": "1800"}}],
      "confession": [{{"day": "Saturday", "start": "0900", "end": "0930"}}],
      "adoration": [{{"day": "Wednesday", "start": "0930", "end": "2130"}}],
      "reason": "brief rationale",
      "confidence": "high"
    }}
  ]
}}

RULES:
- Time format must be HHMM.
- Return an empty church_updates array if there are no confirmed changes.
- Return only JSON."""

    def build_extract_prompt(self, family: BulletinFamily, scope: dict[str, Any]) -> str:
        return f"""You are extracting the regular weekly schedule from a Catholic bulletin.

FAMILY:
{family.name}

CHURCHES:
{pretty_json(scope['churches'])}

TASK:
- Extract the current regular masses, daily masses, confession times, and adoration times for each church.
- Include only churches whose regular schedule is clearly stated in the bulletin.
- Ignore special liturgies, holiday schedules, memorial masses, and one-off events.

OUTPUT JSON:
{{
  "church_schedules": [
    {{
      "church_id": "church-id",
      "masses": [{{"day": "Sunday", "time": "1100"}}],
      "daily_masses": [{{"day": "Tuesday", "time": "1800"}}],
      "confession": [{{"day": "Saturday", "start": "0900", "end": "0930"}}],
      "adoration": [{{"day": "Wednesday", "start": "0930", "end": "2130"}}]
    }}
  ]
}}

Return only JSON."""

    def build_merge_prompt(self, family: BulletinFamily, scope: dict[str, Any], extracted: dict[str, Any]) -> str:
        return f"""You are merging extracted bulletin schedule data into the existing church database.

CURRENT DATABASE SLICE:
{pretty_json(scope)}

EXTRACTED BULLETIN SCHEDULE:
{pretty_json(extracted)}

TASK:
- Produce only the confirmed schedule changes needed to bring the database in sync.
- If the extracted data does not justify a change, omit it.
- Do not invent fields or churches.

OUTPUT JSON:
{{
  "church_updates": [
    {{
      "church_id": "church-id",
      "masses": [{{"day": "Sunday", "time": "1100"}}],
      "daily_masses": [{{"day": "Tuesday", "time": "1800"}}],
      "confession": [{{"day": "Saturday", "start": "0900", "end": "0930"}}],
      "adoration": [{{"day": "Wednesday", "start": "0930", "end": "2130"}}],
      "reason": "brief rationale",
      "confidence": "high"
    }}
  ]
}}

Return only JSON."""

    def build_review_prompt(self, family: BulletinFamily, scope: dict[str, Any], proposal: dict[str, Any]) -> str:
        return f"""You are the review agent for schedule updates.

CURRENT DATABASE SLICE:
{pretty_json(scope)}

PROPOSED CHANGES:
{pretty_json(proposal)}

TASK:
- Reject any proposed change that is not clearly supported by the bulletin.
- Keep unchanged proposals exactly as-is.
- Return the corrected church_updates object.
- Be conservative. False positives are worse than missed updates.

Return only JSON with the same schema as the proposal."""

    def apply(self, bundle: dict[str, Any], family: BulletinFamily, payload: dict[str, Any]) -> dict[str, Any]:
        churches_by_id = {church.get("id"): church for church in bundle["churches"]}
        touched_churches = 0
        changed_fields = 0
        for update in payload.get("church_updates", []):
            church_id = update.get("church_id")
            if church_id not in family.church_ids:
                continue
            church = churches_by_id.get(church_id)
            if not church:
                continue
            changed_for_church = False
            for field_name in ("masses", "daily_masses", "confession", "adoration"):
                if field_name in update and update[field_name] is not None and church.get(field_name) != update[field_name]:
                    church[field_name] = deepcopy(update[field_name])
                    changed_fields += 1
                    changed_for_church = True
            if changed_for_church:
                touched_churches += 1
        return {
            "churches_updated": touched_churches,
            "fields_changed": changed_fields,
        }

    def summarize(self, payload: dict[str, Any], apply_details: dict[str, Any]) -> str:
        return (
            f"schedule updates={len(payload.get('church_updates', []))} "
            f"applied_fields={apply_details.get('fields_changed', 0)}"
        )

    def postprocess_output(self, scope: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        current_by_id = {
            church.get("id"): church
            for church in scope.get("churches", [])
            if church.get("id")
        }
        filtered_updates: list[dict[str, Any]] = []
        for update in payload.get("church_updates", []):
            church_id = update.get("church_id")
            current = current_by_id.get(church_id)
            if not current:
                continue
            normalized = dict(update)
            changed = False
            for field_name in ("masses", "daily_masses", "confession", "adoration"):
                field_value = normalized.get(field_name)
                if field_value is None:
                    continue
                if current.get(field_name) == field_value:
                    normalized[field_name] = None
                    continue
                changed = True
            if changed:
                filtered_updates.append(normalized)
        return {"church_updates": filtered_updates}

    def payload_size(self, payload: dict[str, Any]) -> int:
        return len(payload.get("church_updates", []))


class EventsAdapter(TargetAdapter):
    kind = TargetKind.EVENTS
    max_pages = 8

    def get_scope(self, bundle: dict[str, Any], family: BulletinFamily) -> dict[str, Any]:
        family_events = events_utils.filter_events_for_family(bundle["events"], family.name)
        return {
            "churches": [
                {
                    "id": church.get("id"),
                    "name": church.get("name"),
                    "familyOfParishes": church.get("familyOfParishes"),
                }
                for church in family.churches
            ],
            "existing_events": family_events,
        }

    def coerce_final_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        return EventsPayload.model_validate(payload).model_dump(mode="json", by_alias=True)

    def build_direct_prompt(self, family: BulletinFamily, scope: dict[str, Any]) -> str:
        return f"""You are extracting upcoming special events from a Catholic bulletin.

CHURCHES:
{pretty_json(scope['churches'])}

EXISTING FAMILY EVENTS:
{pretty_json(scope['existing_events'])}

TASK:
- Extract only one-time or limited-time special events.
- Exclude regular weekly schedules for Mass, confession, and adoration.
- Reuse an existing event id when the event clearly matches an existing event.
- Return only JSON.

OUTPUT JSON:
{{
  "events": [
    {{
      "id": null,
      "title": "Event Name",
      "description": "Brief description",
      "church_id": null,
      "church_name": null,
      "family_of_parishes": "{family.name}",
      "date": "YYYY-MM-DD",
      "start_time": "HHMM",
      "end_time": null,
      "location": null,
      "tags": ["community"]
    }}
  ]
}}"""

    def build_extract_prompt(self, family: BulletinFamily, scope: dict[str, Any]) -> str:
        return self.build_direct_prompt(family, scope)

    def build_merge_prompt(self, family: BulletinFamily, scope: dict[str, Any], extracted: dict[str, Any]) -> str:
        return f"""You are reviewing extracted special events before they are merged.

EXISTING FAMILY EVENTS:
{pretty_json(scope['existing_events'])}

EXTRACTED EVENTS:
{pretty_json(extracted)}

TASK:
- Keep only valid upcoming special events.
- Reuse an existing id for matching events.
- Return the final events payload only.

Return only JSON with an events array."""

    def build_review_prompt(self, family: BulletinFamily, scope: dict[str, Any], proposal: dict[str, Any]) -> str:
        return f"""You are the review agent for special events.

PROPOSED EVENTS:
{pretty_json(proposal)}

TASK:
- Remove false positives.
- Fix church assignment, dates, or times only when clearly necessary.
- Preserve ids for matching existing events.

Return only JSON with an events array."""

    def apply(self, bundle: dict[str, Any], family: BulletinFamily, payload: dict[str, Any]) -> dict[str, Any]:
        stamped_events = []
        bulletin_link = family.document.pdf_link if family.document else None
        bulletin_date = datetime.now().strftime("%Y-%m-%d")
        for event in payload.get("events", []):
            event_copy = deepcopy(event)
            event_copy.setdefault("family_of_parishes", family.name)
            events_utils.add_event_metadata(event_copy, bulletin_link, bulletin_date)
            stamped_events.append(event_copy)
        merged = events_utils.merge_events(bundle["events"], stamped_events)
        bundle["events"] = merged
        return {
            "events_extracted": len(stamped_events),
            "total_events": len(merged),
        }

    def summarize(self, payload: dict[str, Any], apply_details: dict[str, Any]) -> str:
        return (
            f"events extracted={len(payload.get('events', []))} "
            f"total_events={apply_details.get('total_events', 0)}"
        )

    def payload_size(self, payload: dict[str, Any]) -> int:
        return len(payload.get("events", []))


class IntentionsAdapter(TargetAdapter):
    kind = TargetKind.INTENTIONS
    max_pages = 8

    def get_scope(self, bundle: dict[str, Any], family: BulletinFamily) -> dict[str, Any]:
        return {
            "churches": [
                {
                    "id": church.get("id"),
                    "name": church.get("name"),
                    "masses": church.get("masses", []),
                    "daily_masses": church.get("daily_masses", []),
                }
                for church in family.churches
            ],
            "existing_intentions": [
                entry for entry in bundle["intentions"] if entry.get("church_id") in family.church_ids
            ],
        }

    def coerce_final_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        return IntentionsPayload.model_validate(payload).model_dump(mode="json", by_alias=True)

    def build_direct_prompt(self, family: BulletinFamily, scope: dict[str, Any]) -> str:
        return f"""You are extracting Mass intentions from a Catholic bulletin.

CHURCHES:
{pretty_json(scope['churches'])}

TASK:
- Extract every Mass intention listed for the churches in this bulletin.
- Match each intention set to the correct church and Mass time.
- Return only JSON.

OUTPUT JSON:
{{
  "intentions": [
    {{
      "church_id": "church-id",
      "date": "YYYY-MM-DD",
      "time": "HHMM",
      "intentions": [
        {{
          "for": "Person or cause",
          "by": "Requester or null"
        }}
      ]
    }}
  ]
}}"""

    def build_extract_prompt(self, family: BulletinFamily, scope: dict[str, Any]) -> str:
        return self.build_direct_prompt(family, scope)

    def build_merge_prompt(self, family: BulletinFamily, scope: dict[str, Any], extracted: dict[str, Any]) -> str:
        return f"""You are reviewing extracted Mass intentions before they are merged.

EXISTING INTENTIONS:
{pretty_json(scope['existing_intentions'])}

EXTRACTED INTENTIONS:
{pretty_json(extracted)}

TASK:
- Keep valid intention entries.
- Correct church/date/time alignment when supported.
- Return only JSON with the final intentions array."""

    def build_review_prompt(self, family: BulletinFamily, scope: dict[str, Any], proposal: dict[str, Any]) -> str:
        return f"""You are the review agent for Mass intentions.

PROPOSED INTENTIONS:
{pretty_json(proposal)}

TASK:
- Remove false positives.
- Correct assignments only if the bulletin supports the correction.
- Return only JSON with the final intentions array."""

    def apply(self, bundle: dict[str, Any], family: BulletinFamily, payload: dict[str, Any]) -> dict[str, Any]:
        stamped = []
        bulletin_link = family.document.pdf_link if family.document else None
        for entry in payload.get("intentions", []):
            entry_copy = deepcopy(entry)
            intentions_utils.add_intention_metadata(entry_copy, bulletin_link)
            stamped.append(entry_copy)
        merged = intentions_utils.merge_intentions(bundle["intentions"], stamped)
        bundle["intentions"] = merged
        return {
            "intentions_extracted": len(stamped),
            "total_intentions": len(merged),
        }

    def summarize(self, payload: dict[str, Any], apply_details: dict[str, Any]) -> str:
        return (
            f"intentions extracted={len(payload.get('intentions', []))} "
            f"total_intentions={apply_details.get('total_intentions', 0)}"
        )

    def payload_size(self, payload: dict[str, Any]) -> int:
        return len(payload.get("intentions", []))


class CombinedAdapter(TargetAdapter):
    kind = TargetKind.COMBINED
    max_pages = 8

    def __init__(self) -> None:
        self.schedule = ScheduleAdapter()
        self.events = EventsAdapter()
        self.intentions = IntentionsAdapter()

    def get_scope(self, bundle: dict[str, Any], family: BulletinFamily) -> dict[str, Any]:
        return {
            "schedule": self.schedule.get_scope(bundle, family),
            "events": self.events.get_scope(bundle, family),
            "intentions": self.intentions.get_scope(bundle, family),
        }

    def coerce_final_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        return CombinedPayload.model_validate(payload).model_dump(mode="json", by_alias=True)

    def build_direct_prompt(self, family: BulletinFamily, scope: dict[str, Any]) -> str:
        return f"""You are updating three bulletin-driven datasets at once: schedule, events, and intentions.

CURRENT DATA SLICE:
{pretty_json(scope)}

TASK:
- Review the bulletin conservatively.
- Return only confirmed updates.
- Use the schedule/events/intentions schemas exactly.

OUTPUT JSON:
{{
  "schedule": {{"church_updates": []}},
  "events": {{"events": []}},
  "intentions": {{"intentions": []}}
}}"""

    def build_extract_prompt(self, family: BulletinFamily, scope: dict[str, Any]) -> str:
        return self.build_direct_prompt(family, scope)

    def build_merge_prompt(self, family: BulletinFamily, scope: dict[str, Any], extracted: dict[str, Any]) -> str:
        return f"""You are reviewing a combined extraction payload.

CURRENT DATA SLICE:
{pretty_json(scope)}

EXTRACTED PAYLOAD:
{pretty_json(extracted)}

Return only corrected JSON with schedule, events, and intentions."""

    def build_review_prompt(self, family: BulletinFamily, scope: dict[str, Any], proposal: dict[str, Any]) -> str:
        return f"""You are the review agent for a combined bulletin update.

PROPOSED PAYLOAD:
{pretty_json(proposal)}

Return only corrected JSON with schedule, events, and intentions."""

    def apply(self, bundle: dict[str, Any], family: BulletinFamily, payload: dict[str, Any]) -> dict[str, Any]:
        return {
            "schedule": self.schedule.apply(bundle, family, payload.get("schedule", {"church_updates": []})),
            "events": self.events.apply(bundle, family, payload.get("events", {"events": []})),
            "intentions": self.intentions.apply(bundle, family, payload.get("intentions", {"intentions": []})),
        }

    def summarize(self, payload: dict[str, Any], apply_details: dict[str, Any]) -> str:
        return (
            f"combined schedule={len(payload.get('schedule', {}).get('church_updates', []))} "
            f"events={len(payload.get('events', {}).get('events', []))} "
            f"intentions={len(payload.get('intentions', {}).get('intentions', []))}"
        )

    def postprocess_output(self, scope: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        return {
            "schedule": self.schedule.postprocess_output(scope.get("schedule", {}), payload.get("schedule", {"church_updates": []})),
            "events": self.events.postprocess_output(scope.get("events", {}), payload.get("events", {"events": []})),
            "intentions": self.intentions.postprocess_output(scope.get("intentions", {}), payload.get("intentions", {"intentions": []})),
        }

    def payload_size(self, payload: dict[str, Any]) -> int:
        return (
            self.schedule.payload_size(payload.get("schedule", {"church_updates": []}))
            + self.events.payload_size(payload.get("events", {"events": []}))
            + self.intentions.payload_size(payload.get("intentions", {"intentions": []}))
        )


def build_adapter(target: TargetKind) -> TargetAdapter:
    if target is TargetKind.SCHEDULE:
        return ScheduleAdapter()
    if target is TargetKind.EVENTS:
        return EventsAdapter()
    if target is TargetKind.INTENTIONS:
        return IntentionsAdapter()
    if target is TargetKind.COMBINED:
        return CombinedAdapter()
    raise ValueError(f"Unsupported target: {target}")
