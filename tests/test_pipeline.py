from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path

from bulletin_scraper.adapters import EventsAdapter, ScheduleAdapter
from bulletin_scraper.cli import parse_args
from bulletin_scraper.config import AppPaths, RunConfig, get_settings
from bulletin_scraper.graphs import run_strategy_graph
from bulletin_scraper.models import BulletinDocument, BulletinFamily, InputArtifact, InputMode, StrategyKind, TargetKind, WorkflowCase, WorkflowResult
from bulletin_scraper.runner import _select_results
from bulletin_scraper.schemas import BulletinCacheEntry, BulletinCacheManifest
from bulletin_scraper.sources import build_families, ensure_family_documents


class StubClient:
    def __init__(self, responses):
        self._responses = list(responses)

    def invoke_json(self, prompt, artifact, stage_name):
        payload = self._responses.pop(0)
        return payload, json.dumps(payload)


def test_build_families_groups_shared_bulletin_website():
    churches = [
        {
            "id": "alpha",
            "name": "Alpha",
            "familyOfParishes": "Shared Family",
            "bulletin_website": "https://example.com/bulletin",
        },
        {
            "id": "beta",
            "name": "Beta",
            "familyOfParishes": "Shared Family",
            "bulletin_website": "https://example.com/bulletin",
        },
        {
            "id": "gamma",
            "name": "Gamma",
            "familyOfParishes": "Other Family",
            "bulletin_website": "https://example.com/other",
        },
    ]

    families = build_families(churches)

    assert len(families) == 2
    shared = next(family for family in families if family.family_id == "shared-family")
    assert shared.church_ids == ["alpha", "beta"]


def test_reviewed_schedule_strategy_updates_selected_fields():
    bundle = {
        "churches": [
            {
                "id": "alpha",
                "name": "Alpha",
                "bulletin_website": "https://example.com/bulletin",
                "masses": [{"day": "Sunday", "time": "1000"}],
                "daily_masses": [],
                "confession": [],
                "adoration": [],
            }
        ],
        "events": [],
        "intentions": [],
    }
    family = BulletinFamily(
        family_id="shared-family",
        name="Shared Family",
        bulletin_website="https://example.com/bulletin",
        churches=bundle["churches"],
        document=BulletinDocument(
            website="https://example.com/bulletin",
            pdf_link="https://example.com/file.pdf",
            pdf_path=Path("dummy.pdf"),
        ),
    )
    adapter = ScheduleAdapter()
    artifact = InputArtifact(mode=InputMode.TEXT, payload="Sunday 11:00 AM", description="text")
    client = StubClient(
        [
            {
                "church_updates": [
                    {
                        "church_id": "alpha",
                        "masses": [{"day": "Sunday", "time": "1100"}],
                        "reason": "Bulletin shows 11:00 AM.",
                        "confidence": "medium",
                    }
                ]
            },
            {
                "church_updates": [
                    {
                        "church_id": "alpha",
                        "masses": [{"day": "Sunday", "time": "1100"}],
                        "reason": "Confirmed by reviewer.",
                        "confidence": "high",
                    }
                ]
            },
        ]
    )

    output, raw_outputs = run_strategy_graph(
        StrategyKind.REVIEWED,
        adapter,
        client,
        family,
        artifact,
        adapter.get_scope(bundle, family),
    )
    apply_details = adapter.apply(bundle, family, output)

    assert raw_outputs["proposal"]
    assert raw_outputs["review"]
    assert bundle["churches"][0]["masses"] == [{"day": "Sunday", "time": "1100"}]
    assert apply_details == {"churches_updated": 1, "fields_changed": 1}


def test_extract_merge_events_strategy_produces_mergeable_payload():
    bundle = {
        "churches": [
            {
                "id": "alpha",
                "name": "Alpha",
                "familyOfParishes": "Shared Family",
                "bulletin_website": "https://example.com/bulletin",
                "masses": [],
                "daily_masses": [],
                "confession": [],
                "adoration": [],
            }
        ],
        "events": [],
        "intentions": [],
    }
    family = BulletinFamily(
        family_id="shared-family",
        name="Shared Family",
        bulletin_website="https://example.com/bulletin",
        churches=bundle["churches"],
        document=BulletinDocument(
            website="https://example.com/bulletin",
            pdf_link="https://example.com/file.pdf",
            pdf_path=Path("dummy.pdf"),
        ),
    )
    adapter = EventsAdapter()
    artifact = InputArtifact(mode=InputMode.TEXT, payload="Parish dinner on 2026-05-10 at 18:00", description="text")
    client = StubClient(
        [
            {
                "events": [
                    {
                        "id": None,
                        "title": "Parish Dinner",
                        "description": "Community dinner",
                        "church_id": "alpha",
                        "church_name": "Alpha",
                        "family_of_parishes": "Shared Family",
                        "date": "2026-05-10",
                        "start_time": "1800",
                        "end_time": None,
                        "location": "Hall",
                        "tags": ["social"],
                    }
                ]
            },
            {
                "events": [
                    {
                        "id": None,
                        "title": "Parish Dinner",
                        "description": "Community dinner",
                        "church_id": "alpha",
                        "church_name": "Alpha",
                        "family_of_parishes": "Shared Family",
                        "date": "2026-05-10",
                        "start_time": "1800",
                        "end_time": None,
                        "location": "Hall",
                        "tags": ["social"],
                    }
                ]
            },
        ]
    )

    output, _ = run_strategy_graph(
        StrategyKind.EXTRACT_MERGE,
        adapter,
        client,
        family,
        artifact,
        adapter.get_scope(bundle, family),
    )
    apply_details = adapter.apply(bundle, family, output)

    assert len(output["events"]) == 1
    assert apply_details["events_extracted"] == 1
    assert len(bundle["events"]) == 1
    assert bundle["events"][0]["title"] == "Parish Dinner"


def test_selection_prefers_reviewed_images_case():
    direct_pdf = WorkflowResult(
        family_id="shared-family",
        case=WorkflowCase(target=TargetKind.SCHEDULE, strategy=StrategyKind.DIRECT, input_mode=InputMode.PDF),
        status="ok",
        score=110,
    )
    reviewed_images = WorkflowResult(
        family_id="shared-family",
        case=WorkflowCase(target=TargetKind.SCHEDULE, strategy=StrategyKind.REVIEWED, input_mode=InputMode.IMAGES),
        status="ok",
        score=330,
        output={"church_updates": [{"church_id": "alpha", "adoration": [{"day": "Wednesday", "start": "0930", "end": "2130"}]}]},
    )

    selected = _select_results({"shared-family": [direct_pdf, reviewed_images]})

    assert selected[("shared-family", "schedule")] is reviewed_images
    assert reviewed_images.selected is True


def test_selection_prefers_substantive_schedule_output_over_empty_higher_ranked_case():
    direct_images = WorkflowResult(
        family_id="shared-family",
        case=WorkflowCase(target=TargetKind.SCHEDULE, strategy=StrategyKind.DIRECT, input_mode=InputMode.IMAGES),
        status="ok",
        score=130,
        output={"church_updates": [{"church_id": "alpha", "adoration": [{"day": "Saturday", "start": "0930", "end": "1100"}]}]},
    )
    reviewed_text = WorkflowResult(
        family_id="shared-family",
        case=WorkflowCase(target=TargetKind.SCHEDULE, strategy=StrategyKind.REVIEWED, input_mode=InputMode.TEXT),
        status="ok",
        score=320,
        output={"church_updates": []},
    )

    selected = _select_results({"shared-family": [direct_images, reviewed_text]})

    assert selected[("shared-family", "schedule")] is direct_images


def test_schedule_postprocess_removes_unchanged_updates():
    adapter = ScheduleAdapter()
    scope = {
        "churches": [
            {
                "id": "alpha",
                "name": "Alpha",
                "masses": [{"day": "Sunday", "time": "1100"}],
                "daily_masses": [],
                "confession": [],
                "adoration": [{"day": "Wednesday", "start": "0930", "end": "2130"}],
            }
        ]
    }
    payload = {
        "church_updates": [
            {
                "church_id": "alpha",
                "adoration": [{"day": "Wednesday", "start": "0930", "end": "2130"}],
                "reason": "Restates existing adoration.",
                "confidence": "high",
            }
        ]
    }

    assert adapter.postprocess_output(scope, payload) == {"church_updates": []}


def test_parse_args_returns_validated_enum_config():
    config, paths = parse_args(
        [
            "--targets",
            "schedule,events",
            "--strategies",
            "direct",
            "--input-modes",
            "text,pdf",
            "--workers",
            "2",
            "--refresh-bulletins",
        ]
    )

    assert config.targets == [TargetKind.SCHEDULE, TargetKind.EVENTS]
    assert config.strategies == [StrategyKind.DIRECT]
    assert config.input_modes == [InputMode.TEXT, InputMode.PDF]
    assert config.refresh_bulletins is True
    assert paths.bulletin_cache_path.name == "cache_index.json"


def test_parse_args_uses_env_backed_defaults_and_cli_can_override(monkeypatch):
    get_settings.cache_clear()
    monkeypatch.setenv("BULLETIN_SCRAPER_WORKERS", "7")
    monkeypatch.setenv("OPENROUTER_MODEL", "example/model")
    monkeypatch.setenv("BULLETIN_SCRAPER_USE_EXISTING_BULLETINS", "true")

    try:
        config, _ = parse_args(["--targets", "schedule", "--no-use-existing-bulletins"])
    finally:
        get_settings.cache_clear()

    assert config.workers == 7
    assert config.model == "example/model"
    assert config.use_existing_bulletins is False


def test_ensure_family_documents_reuses_same_day_cache_entry(tmp_path, monkeypatch):
    family = BulletinFamily(
        family_id="shared-family",
        name="Shared Family",
        bulletin_website="https://example.com/bulletin",
        churches=[
            {
                "id": "alpha",
                "name": "Alpha",
                "website": "https://example.com",
                "bulletin_website": "https://example.com/bulletin",
            }
        ],
    )
    bulletins_dir = tmp_path / "bulletins"
    bulletins_dir.mkdir()
    pdf_path = bulletins_dir / "shared-family.pdf"
    pdf_path.write_bytes(b"%PDF-1.4\n")
    cache_path = bulletins_dir / "cache_index.json"
    now = datetime.now().isoformat(timespec="seconds")
    cache_path.write_text(
        json.dumps(
            BulletinCacheManifest(
                updated_at=now,
                families={
                    family.family_id: BulletinCacheEntry(
                        family_id=family.family_id,
                        family_name=family.name,
                        bulletin_website=family.bulletin_website,
                        primary_website="https://example.com",
                        pdf_url="https://example.com/file.pdf",
                        pdf_path=pdf_path,
                        status="downloaded",
                        last_scraped_at=now,
                        last_downloaded_at=now,
                    )
                },
            ).model_dump(mode="json"),
            indent=2,
        ),
        encoding="utf-8",
    )

    def _unexpected_scrape(*args, **kwargs):
        raise AssertionError("scrape should not run for a same-day cached bulletin")

    def _unexpected_download(*args, **kwargs):
        raise AssertionError("download should not run for a same-day cached bulletin")

    monkeypatch.setattr("bulletin_scraper.sources.scraping.scrape_bulletin_with_retry", _unexpected_scrape)
    monkeypatch.setattr("bulletin_scraper.sources.scraping.download_pdf", _unexpected_download)

    paths = AppPaths(
        root=tmp_path,
        data_dir=tmp_path / "data",
        churches_path=tmp_path / "data" / "churches.json",
        events_path=tmp_path / "data" / "events.json",
        intentions_path=tmp_path / "data" / "intentions.json",
        bulletins_dir=bulletins_dir,
        bulletin_cache_path=cache_path,
        runs_dir=tmp_path / "runs",
    )

    ready = ensure_family_documents([family], paths, RunConfig(), logging.getLogger("test"))

    assert len(ready) == 1
    assert ready[0].document is not None
    assert ready[0].document.pdf_path == pdf_path
    assert ready[0].document.pdf_link == "https://example.com/file.pdf"