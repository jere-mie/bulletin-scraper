from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from .models import InputMode, StrategyKind, TargetKind
from .schemas import StrictModel


DEFAULT_MODEL = "google/gemini-3.1-flash-lite-preview"


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    default_targets: str = Field(default="schedule", validation_alias=AliasChoices("BULLETIN_SCRAPER_TARGETS"))
    default_strategies: str = Field(
        default="direct,extract-merge,reviewed",
        validation_alias=AliasChoices("BULLETIN_SCRAPER_STRATEGIES"),
    )
    default_input_modes: str = Field(
        default="images,text,pdf",
        validation_alias=AliasChoices("BULLETIN_SCRAPER_INPUT_MODES"),
    )
    workers: int = Field(default=4, ge=1, validation_alias=AliasChoices("BULLETIN_SCRAPER_WORKERS"))
    family_limit: int | None = Field(default=None, ge=1, validation_alias=AliasChoices("BULLETIN_SCRAPER_FAMILY_LIMIT"))
    family_filter: str | None = Field(default=None, validation_alias=AliasChoices("BULLETIN_SCRAPER_FAMILY_FILTER"))
    use_existing_bulletins: bool = Field(
        default=False,
        validation_alias=AliasChoices("BULLETIN_SCRAPER_USE_EXISTING_BULLETINS"),
    )
    refresh_bulletins: bool = Field(
        default=False,
        validation_alias=AliasChoices("BULLETIN_SCRAPER_REFRESH_BULLETINS"),
    )
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        default="INFO",
        validation_alias=AliasChoices("BULLETIN_SCRAPER_LOG_LEVEL"),
    )
    model: str = Field(
        default=DEFAULT_MODEL,
        validation_alias=AliasChoices("OPENROUTER_MODEL", "BULLETIN_SCRAPER_MODEL"),
    )
    churches_path: str = Field(default="data/churches.json", validation_alias=AliasChoices("BULLETIN_SCRAPER_CHURCHES_PATH"))
    events_path: str = Field(default="data/events.json", validation_alias=AliasChoices("BULLETIN_SCRAPER_EVENTS_PATH"))
    intentions_path: str = Field(
        default="data/intentions.json",
        validation_alias=AliasChoices("BULLETIN_SCRAPER_INTENTIONS_PATH"),
    )
    bulletins_dir: str = Field(default="bulletins", validation_alias=AliasChoices("BULLETIN_SCRAPER_BULLETINS_DIR"))
    bulletin_cache_path: str = Field(
        default="bulletins/cache_index.json",
        validation_alias=AliasChoices("BULLETIN_SCRAPER_BULLETIN_CACHE_PATH"),
    )
    runs_dir: str = Field(default="runs", validation_alias=AliasChoices("BULLETIN_SCRAPER_RUNS_DIR"))
    openrouter_api_key: str | None = Field(
        default=None,
        validation_alias=AliasChoices("OPENROUTER_API_KEY", "BULLETIN_SCRAPER_OPENROUTER_API_KEY"),
    )
    openrouter_base_url: str = Field(
        default="https://openrouter.ai/api/v1",
        validation_alias=AliasChoices("OPENROUTER_BASE_URL", "BULLETIN_SCRAPER_OPENROUTER_BASE_URL"),
    )
    openrouter_site_url: str = Field(
        default="https://github.com/jere-mie/massfinder-we",
        validation_alias=AliasChoices("OPENROUTER_SITE_URL", "BULLETIN_SCRAPER_OPENROUTER_SITE_URL"),
    )
    openrouter_app_name: str = Field(
        default="bulletin-scraper",
        validation_alias=AliasChoices("OPENROUTER_APP_NAME", "BULLETIN_SCRAPER_OPENROUTER_APP_NAME"),
    )


@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    return AppSettings()


class AppPaths(StrictModel):
    root: Path
    data_dir: Path
    churches_path: Path
    events_path: Path
    intentions_path: Path
    bulletins_dir: Path
    bulletin_cache_path: Path
    runs_dir: Path


class RunConfig(StrictModel):
    targets: list[TargetKind] = Field(default_factory=lambda: [TargetKind.SCHEDULE])
    strategies: list[StrategyKind] = Field(
        default_factory=lambda: [StrategyKind.DIRECT, StrategyKind.EXTRACT_MERGE, StrategyKind.REVIEWED]
    )
    input_modes: list[InputMode] = Field(default_factory=lambda: [InputMode.IMAGES, InputMode.TEXT, InputMode.PDF])
    apply_changes: bool = False
    workers: int = Field(default=4, ge=1)
    family_limit: int | None = Field(default=None, ge=1)
    family_filter: str | None = None
    use_existing_bulletins: bool = False
    refresh_bulletins: bool = False
    interactive: bool = False
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    model: str = DEFAULT_MODEL
