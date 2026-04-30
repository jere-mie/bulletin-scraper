from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Any

from pydantic import Field

from .schemas import StrictModel


class TargetKind(StrEnum):
    SCHEDULE = "schedule"
    EVENTS = "events"
    INTENTIONS = "intentions"
    COMBINED = "combined"


class StrategyKind(StrEnum):
    DIRECT = "direct"
    EXTRACT_MERGE = "extract-merge"
    REVIEWED = "reviewed"


class InputMode(StrEnum):
    IMAGES = "images"
    TEXT = "text"
    PDF = "pdf"


class BulletinDocument(StrictModel):
    website: str
    pdf_link: str | None
    pdf_path: Path


class BulletinFamily(StrictModel):
    family_id: str
    name: str
    bulletin_website: str
    churches: list[dict[str, Any]]
    document: BulletinDocument | None = None

    @property
    def church_ids(self) -> list[str]:
        return [church.get("id", "") for church in self.churches if church.get("id")]

    @property
    def church_names(self) -> list[str]:
        return [church.get("name", "Unknown") for church in self.churches]


class InputArtifact(StrictModel):
    mode: InputMode
    payload: Any
    description: str
    page_count: int | None = None
    text_preview: str | None = None


class WorkflowCase(StrictModel):
    target: TargetKind
    strategy: StrategyKind
    input_mode: InputMode

    @property
    def case_id(self) -> str:
        return f"{self.target.value}-{self.strategy.value}-{self.input_mode.value}"


class WorkflowResult(StrictModel):
    family_id: str
    case: WorkflowCase
    status: str
    output: dict[str, Any] | None = None
    raw_outputs: dict[str, str] = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)
    summary: str = ""
    score: int = 0
    selected: bool = False
    artifact_path: Path | None = None
    apply_details: dict[str, Any] = Field(default_factory=dict)
    error: str | None = None
