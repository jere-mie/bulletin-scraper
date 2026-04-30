from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path

from bulletin_scraper.adapters import EventsAdapter, IntentionsAdapter, ScheduleAdapter
from bulletin_scraper.cli import _collect_cli_options, _should_prompt_field, parse_args
from bulletin_scraper.config import AppPaths, RunConfig, get_settings
from bulletin_scraper.graphs import run_strategy_graph
from bulletin_scraper.llm_client import OpenRouterLlmClient
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


def test_events_apply_prunes_stale_items_and_merges_duplicates():
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
        "events": [
            {
                "id": "existing-1",
                "title": "Parish Dinner",
                "description": "Short description",
                "church_id": "alpha",
                "church_name": "Alpha",
                "family_of_parishes": "Shared Family",
                "date": "2026-05-10",
                "start_time": "1800",
                "end_time": None,
                "location": "Hall",
                "tags": ["social"],
                "source_bulletin_date": "2026-05-01",
            }
        ],
        "intentions": [],
    }
    family = BulletinFamily(
        family_id="shared-family",
        name="Shared Family",
        bulletin_website="https://example.com/bulletin",
        churches=bundle["churches"],
        document=BulletinDocument(
            website="https://example.com/bulletin",
            pdf_link="https://example.com/May-08-2026.pdf",
            pdf_path=Path("dummy.pdf"),
            bulletin_date="2026-05-08",
        ),
    )
    adapter = EventsAdapter()

    payload = {
        "events": [
            {
                "id": None,
                "title": "Parish Dinner",
                "description": "Longer community dinner description",
                "church_id": "alpha",
                "church_name": "Alpha",
                "family_of_parishes": "Shared Family",
                "date": "2026-05-10",
                "start_time": "1800",
                "end_time": None,
                "location": "Parish Hall",
                "tags": ["social", "community"],
            },
            {
                "id": None,
                "title": "Old Event",
                "description": "Should be removed",
                "church_id": "alpha",
                "church_name": "Alpha",
                "family_of_parishes": "Shared Family",
                "date": "2026-04-20",
                "start_time": "1800",
                "end_time": None,
                "location": "Hall",
                "tags": ["social"],
            },
        ]
    }

    apply_details = adapter.apply(bundle, family, payload)

    assert apply_details["events_extracted"] == 1
    assert len(bundle["events"]) == 1
    assert bundle["events"][0]["id"] == "existing-1"
    assert bundle["events"][0]["description"] == "Longer community dinner description"
    assert bundle["events"][0]["tags"] == ["community", "social"]


def test_events_apply_does_not_prune_unrelated_entries_without_source_bulletin_date():
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
        "events": [
            {
                "id": "legacy-1",
                "title": "Legacy Event",
                "description": "Historic entry without bulletin metadata",
                "church_id": None,
                "church_name": None,
                "family_of_parishes": "Other Family",
                "date": "2026-01-01",
                "start_time": None,
                "end_time": None,
                "location": None,
                "tags": ["community"],
            }
        ],
        "intentions": [],
    }
    family = BulletinFamily(
        family_id="shared-family",
        name="Shared Family",
        bulletin_website="https://example.com/bulletin",
        churches=bundle["churches"],
        document=BulletinDocument(
            website="https://example.com/bulletin",
            pdf_link="https://example.com/May-08-2026.pdf",
            pdf_path=Path("dummy.pdf"),
            bulletin_date="2026-05-08",
        ),
    )
    adapter = EventsAdapter()

    payload = {
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
    }

    adapter.apply(bundle, family, payload)

    assert len(bundle["events"]) == 2
    assert any(event["id"] == "legacy-1" for event in bundle["events"])
    assert any(event["title"] == "Parish Dinner" for event in bundle["events"])


def test_intentions_apply_replaces_existing_family_slice():
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
            },
            {
                "id": "beta",
                "name": "Beta",
                "familyOfParishes": "Other Family",
                "bulletin_website": "https://example.com/other",
                "masses": [],
                "daily_masses": [],
                "confession": [],
                "adoration": [],
            },
        ],
        "events": [],
        "intentions": [
            {"church_id": "alpha", "date": "2026-04-28", "time": "1800", "intentions": [{"for": "Old", "by": None}]},
            {"church_id": "beta", "date": "2026-04-28", "time": "1800", "intentions": [{"for": "Other", "by": None}]},
        ],
    }
    family = BulletinFamily(
        family_id="shared-family",
        name="Shared Family",
        bulletin_website="https://example.com/bulletin",
        churches=[bundle["churches"][0]],
        document=BulletinDocument(
            website="https://example.com/bulletin",
            pdf_link="https://example.com/May-08-2026.pdf",
            pdf_path=Path("dummy.pdf"),
            bulletin_date="2026-05-08",
        ),
    )
    adapter = IntentionsAdapter()

    payload = {
        "intentions": [
            {"church_id": "alpha", "date": "2026-05-08", "time": "1800", "intentions": [{"for": "New", "by": "Donor"}]}
        ]
    }

    apply_details = adapter.apply(bundle, family, payload)

    assert apply_details["intentions_extracted"] == 1
    assert len(bundle["intentions"]) == 2
    assert bundle["intentions"][0]["church_id"] == "beta"
    assert bundle["intentions"][1]["church_id"] == "alpha"
    assert bundle["intentions"][1]["date"] == "2026-05-08"


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
            "text,text-images,pdf",
            "--workers",
            "2",
            "--refresh-bulletins",
        ]
    )

    assert config.targets == [TargetKind.SCHEDULE, TargetKind.EVENTS]
    assert config.strategies == [StrategyKind.DIRECT]
    assert config.input_modes == [InputMode.TEXT, InputMode.TEXT_IMAGES, InputMode.PDF]
    assert config.refresh_bulletins is True
    assert paths.bulletin_cache_path.name == "cache_index.json"


def test_selection_ignores_pdf_when_non_pdf_candidate_exists():
    reviewed_pdf = WorkflowResult(
        family_id="shared-family",
        case=WorkflowCase(target=TargetKind.EVENTS, strategy=StrategyKind.REVIEWED, input_mode=InputMode.PDF),
        status="ok",
        score=310,
        output={"events": [{"title": "PDF event"}]},
    )
    direct_text = WorkflowResult(
        family_id="shared-family",
        case=WorkflowCase(target=TargetKind.EVENTS, strategy=StrategyKind.DIRECT, input_mode=InputMode.TEXT),
        status="ok",
        score=120,
        output={"events": [{"title": "Text event"}]},
    )

    selected = _select_results({"shared-family": [reviewed_pdf, direct_text]})

    assert selected[("shared-family", "events")] is direct_text


def test_selection_prefers_higher_precision_events_over_text_images_volume():
    reviewed_text = WorkflowResult(
        family_id="shared-family",
        case=WorkflowCase(target=TargetKind.EVENTS, strategy=StrategyKind.REVIEWED, input_mode=InputMode.TEXT),
        status="ok",
        score=320,
        output={
            "events": [
                {"title": "Parish Dinner", "date": "2026-05-10", "tags": ["social"]},
                {"title": "Fundraiser", "date": "2026-05-11", "tags": ["fundraiser"]},
            ]
        },
    )
    reviewed_text_images = WorkflowResult(
        family_id="shared-family",
        case=WorkflowCase(target=TargetKind.EVENTS, strategy=StrategyKind.REVIEWED, input_mode=InputMode.TEXT_IMAGES),
        status="ok",
        score=340,
        output={
            "events": [
                {"title": "Mother's Day Mass", "date": "2026-05-06", "tags": ["liturgy", "seasonal"]},
                {"title": "Healing Mass", "date": "2026-05-07", "tags": ["liturgy"]},
                {"title": "Parish Dinner", "date": "2026-05-10", "tags": ["social"]},
            ]
        },
    )

    selected = _select_results({"shared-family": [reviewed_text, reviewed_text_images]})

    assert selected[("shared-family", "events")] is reviewed_text


def test_selection_prefers_intentions_with_better_by_coverage():
    reviewed_images = WorkflowResult(
        family_id="shared-family",
        case=WorkflowCase(target=TargetKind.INTENTIONS, strategy=StrategyKind.REVIEWED, input_mode=InputMode.IMAGES),
        status="ok",
        score=330,
        output={
            "intentions": [
                {
                    "church_id": "alpha",
                    "date": "2026-05-01",
                    "time": "0900",
                    "intentions": [
                        {"for": "One", "by": "Donor A"},
                        {"for": "Two", "by": "Donor B"},
                    ],
                }
            ]
        },
    )
    reviewed_text_images = WorkflowResult(
        family_id="shared-family",
        case=WorkflowCase(target=TargetKind.INTENTIONS, strategy=StrategyKind.REVIEWED, input_mode=InputMode.TEXT_IMAGES),
        status="ok",
        score=340,
        output={
            "intentions": [
                {
                    "church_id": "alpha",
                    "date": "2026-05-01",
                    "time": "0900",
                    "intentions": [
                        {"for": "One", "by": None},
                        {"for": "Donor A", "by": None},
                        {"for": "Two", "by": None},
                        {"for": "Donor B", "by": None},
                    ],
                }
            ]
        },
    )

    selected = _select_results({"shared-family": [reviewed_images, reviewed_text_images]})

    assert selected[("shared-family", "intentions")] is reviewed_images


def test_selection_penalizes_shifted_intention_donors():
    direct_text_images = WorkflowResult(
        family_id="shared-family",
        case=WorkflowCase(target=TargetKind.INTENTIONS, strategy=StrategyKind.DIRECT, input_mode=InputMode.TEXT_IMAGES),
        status="ok",
        score=140,
        output={
            "intentions": [
                {
                    "church_id": "alpha",
                    "date": "2026-05-01",
                    "time": "0900",
                    "intentions": [
                        {"for": "Alice", "by": "Bob"},
                        {"for": "Bob", "by": "Carol"},
                        {"for": "Carol", "by": None},
                    ],
                }
            ]
        },
    )
    reviewed_text_images = WorkflowResult(
        family_id="shared-family",
        case=WorkflowCase(target=TargetKind.INTENTIONS, strategy=StrategyKind.REVIEWED, input_mode=InputMode.TEXT_IMAGES),
        status="ok",
        score=340,
        output={
            "intentions": [
                {
                    "church_id": "alpha",
                    "date": "2026-05-01",
                    "time": "0900",
                    "intentions": [
                        {"for": "Alice", "by": None},
                        {"for": "Bob", "by": "Family B"},
                        {"for": "Carol", "by": "Family C"},
                    ],
                }
            ]
        },
    )

    selected = _select_results({"shared-family": [direct_text_images, reviewed_text_images]})

    assert selected[("shared-family", "intentions")] is reviewed_text_images


def test_schedule_adapter_accepts_id_alias_for_church_id():
    adapter = ScheduleAdapter()

    payload = adapter.coerce_final_payload(
        {
            "church_updates": [
                {
                    "id": "alpha",
                    "masses": [{"day": "Sunday", "time": "1100"}],
                    "reason": "Bulletin shows 11:00 AM.",
                    "confidence": "high",
                }
            ]
        }
    )

    assert payload["church_updates"][0]["church_id"] == "alpha"


def test_events_postprocess_filters_liturgical_notices():
    adapter = EventsAdapter()
    scope = {"bulletin_date": "2026-05-03", "existing_events": [], "churches": []}
    payload = {
        "events": [
            {
                "title": "Mother's Day Mass",
                "description": "One-off Mass notice",
                "date": "2026-05-06",
                "tags": ["liturgy", "seasonal"],
            },
            {
                "title": "Parish Dinner",
                "description": "Community fundraiser dinner",
                "date": "2026-05-10",
                "tags": ["fundraiser", "social"],
            },
        ]
    }

    output = adapter.postprocess_output(scope, payload)

    assert [event["title"] for event in output["events"]] == ["Parish Dinner"]


def test_llm_client_adds_cache_control_to_large_text_blocks(monkeypatch):
    get_settings.cache_clear()
    monkeypatch.setenv("BULLETIN_SCRAPER_ENABLE_PROMPT_CACHING", "true")
    monkeypatch.setenv("BULLETIN_SCRAPER_PROMPT_CACHE_TTL", "1h")

    try:
        client = OpenRouterLlmClient(model="google/gemini-3.1-flash-lite-preview", api_key="test-key")
        artifact = InputArtifact(mode=InputMode.TEXT, payload=("x" * 2000), description="text")
        content = client._build_content("Dynamic prompt", artifact)
    finally:
        get_settings.cache_clear()

    cached_blocks = [block for block in content if isinstance(block, dict) and block.get("cache_control")]
    assert cached_blocks
    assert cached_blocks[0]["cache_control"] == {"type": "ephemeral", "ttl": "1h"}


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


def test_interactive_prompt_detection_skips_cli_and_env_values():
    cli_options = _collect_cli_options([
        "--targets",
        "schedule",
        "--workers",
        "2",
        "--no-use-existing-bulletins",
    ])
    env_fields = {"model", "default_strategies", "log_level"}

    assert _should_prompt_field("targets", cli_options, env_fields) is False
    assert _should_prompt_field("workers", cli_options, env_fields) is False
    assert _should_prompt_field("use_existing_bulletins", cli_options, env_fields) is False
    assert _should_prompt_field("strategies", cli_options, env_fields) is False
    assert _should_prompt_field("model", cli_options, env_fields) is False
    assert _should_prompt_field("family_filter", cli_options, env_fields) is True


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