from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


def _normalize_hhmm(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(":", "").replace(" ", "")
    if text.isdigit() and len(text) == 3:
        text = f"0{text}"
    if text.isdigit() and len(text) == 4:
        return text
    raise ValueError(f"Expected HHMM time value, got {value!r}.")


def _normalize_date(value: str | date | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    if not text:
        return None
    return date.fromisoformat(text).isoformat()


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True, populate_by_name=True)


class LlmPayloadModel(BaseModel):
    model_config = ConfigDict(extra="ignore", validate_assignment=True, populate_by_name=True)


class ScheduleMassTime(LlmPayloadModel):
    day: str
    time: str

    @field_validator("day")
    @classmethod
    def _validate_day(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("day must not be empty")
        return normalized

    @field_validator("time", mode="before")
    @classmethod
    def _validate_time(cls, value: str) -> str:
        normalized = _normalize_hhmm(value)
        if normalized is None:
            raise ValueError("time must not be empty")
        return normalized


class ScheduleTimeRange(LlmPayloadModel):
    day: str
    start: str
    end: str

    @field_validator("day")
    @classmethod
    def _validate_day(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("day must not be empty")
        return normalized

    @field_validator("start", "end", mode="before")
    @classmethod
    def _validate_time(cls, value: str) -> str:
        normalized = _normalize_hhmm(value)
        if normalized is None:
            raise ValueError("time range values must not be empty")
        return normalized


class ScheduleUpdate(LlmPayloadModel):
    church_id: str
    masses: list[ScheduleMassTime] | None = None
    daily_masses: list[ScheduleMassTime] | None = None
    confession: list[ScheduleTimeRange] | None = None
    adoration: list[ScheduleTimeRange] | None = None
    reason: str | None = None
    confidence: Literal["low", "medium", "high"] = "medium"

    @field_validator("church_id")
    @classmethod
    def _validate_church_id(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("church_id must not be empty")
        return normalized

    @field_validator("reason")
    @classmethod
    def _validate_reason(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None


class SchedulePayload(LlmPayloadModel):
    church_updates: list[ScheduleUpdate] = Field(default_factory=list)


class ExtractedChurchSchedule(LlmPayloadModel):
    church_id: str
    masses: list[ScheduleMassTime] = Field(default_factory=list)
    daily_masses: list[ScheduleMassTime] = Field(default_factory=list)
    confession: list[ScheduleTimeRange] = Field(default_factory=list)
    adoration: list[ScheduleTimeRange] = Field(default_factory=list)

    @field_validator("church_id")
    @classmethod
    def _validate_church_id(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("church_id must not be empty")
        return normalized


class ScheduleExtractionPayload(LlmPayloadModel):
    church_schedules: list[ExtractedChurchSchedule] = Field(default_factory=list)


class EventRecord(LlmPayloadModel):
    id: str | None = None
    title: str
    description: str | None = None
    church_id: str | None = None
    church_name: str | None = None
    family_of_parishes: str | None = None
    date: str | None = None
    start_time: str | None = None
    end_time: str | None = None
    location: str | None = None
    tags: list[str] = Field(default_factory=list)

    @field_validator("title")
    @classmethod
    def _validate_title(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("title must not be empty")
        return normalized

    @field_validator("date", mode="before")
    @classmethod
    def _validate_date(cls, value: str | date | None) -> str | None:
        return _normalize_date(value)

    @field_validator("start_time", "end_time", mode="before")
    @classmethod
    def _validate_time(cls, value: str | None) -> str | None:
        return _normalize_hhmm(value)


class EventsPayload(LlmPayloadModel):
    events: list[EventRecord] = Field(default_factory=list)


class IntentionLine(LlmPayloadModel):
    for_: str = Field(alias="for")
    by: str | None = None

    @field_validator("for_")
    @classmethod
    def _validate_for(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("intention 'for' must not be empty")
        return normalized


class IntentionMass(LlmPayloadModel):
    church_id: str
    date: str
    time: str
    intentions: list[IntentionLine] = Field(default_factory=list)

    @field_validator("church_id")
    @classmethod
    def _validate_church_id(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("church_id must not be empty")
        return normalized

    @field_validator("date", mode="before")
    @classmethod
    def _validate_date(cls, value: str | date) -> str:
        normalized = _normalize_date(value)
        if normalized is None:
            raise ValueError("date must not be empty")
        return normalized

    @field_validator("time", mode="before")
    @classmethod
    def _validate_time(cls, value: str) -> str:
        normalized = _normalize_hhmm(value)
        if normalized is None:
            raise ValueError("time must not be empty")
        return normalized


class IntentionsPayload(LlmPayloadModel):
    intentions: list[IntentionMass] = Field(default_factory=list)


class CombinedPayload(LlmPayloadModel):
    schedule: SchedulePayload = Field(default_factory=SchedulePayload)
    events: EventsPayload = Field(default_factory=EventsPayload)
    intentions: IntentionsPayload = Field(default_factory=IntentionsPayload)


class BulletinCacheEntry(StrictModel):
    family_id: str
    family_name: str
    bulletin_website: str
    primary_website: str | None = None
    pdf_url: str | None = None
    pdf_path: Path
    bulletin_date: str | None = None
    status: Literal["missing", "cached", "downloaded", "scrape_failed", "download_failed"] = "missing"
    error: str | None = None
    last_attempted_at: str | None = None
    last_scraped_at: str | None = None
    last_downloaded_at: str | None = None
    last_reused_at: str | None = None


class BulletinCacheManifest(StrictModel):
    updated_at: str | None = None
    families: dict[str, BulletinCacheEntry] = Field(default_factory=dict)