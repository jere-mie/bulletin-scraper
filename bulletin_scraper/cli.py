from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pydantic import ValidationError

from .config import AppPaths, RunConfig, get_settings


def build_parser() -> argparse.ArgumentParser:
    settings = get_settings()
    parser = argparse.ArgumentParser(
        description="Run bulletin-family workflows using structured LangGraph pipelines."
    )
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Prompt for run settings in the terminal. If omitted, interactive mode is auto-enabled when no arguments are provided in a TTY.",
    )
    parser.add_argument(
        "--mode",
        default="mass",
        choices=["mass", "events", "intentions"],
        help="Legacy shorthand for selecting a single target.",
    )
    parser.add_argument(
        "--targets",
        default=settings.default_targets,
        help="Comma-separated targets: schedule,events,intentions,combined",
    )
    parser.add_argument(
        "--strategies",
        default=settings.default_strategies,
        help="Comma-separated strategies to evaluate",
    )
    parser.add_argument(
        "--input-modes",
        default=settings.default_input_modes,
        help="Comma-separated bulletin inputs: images,text,pdf",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Persist accepted results back into the JSON data files.",
    )
    parser.add_argument(
        "--modify-json",
        action="store_true",
        help="Legacy alias for --apply.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=settings.workers,
        help="Parallel worker count for family runs.",
    )
    parser.add_argument(
        "--family-limit",
        type=int,
        default=settings.family_limit,
        help="Limit execution to the first N bulletin families.",
    )
    parser.add_argument(
        "--family-filter",
        default=settings.family_filter,
        help="Only run families whose id or name contains this value.",
    )
    parser.add_argument(
        "--use-existing-bulletins",
        action=argparse.BooleanOptionalAction,
        default=settings.use_existing_bulletins,
        help="Reuse local bulletin PDFs whenever present, even if the cache index would normally refresh them.",
    )
    parser.add_argument(
        "--refresh-bulletins",
        action=argparse.BooleanOptionalAction,
        default=settings.refresh_bulletins,
        help="Ignore same-day cache entries and rescrape bulletin links for this run.",
    )
    parser.add_argument(
        "--no-images",
        action="store_true",
        help="Legacy shorthand that removes image mode from the default input set.",
    )
    parser.add_argument(
        "--model",
        default=settings.model,
        help="OpenRouter model name.",
    )
    parser.add_argument(
        "--churches-path",
        default=settings.churches_path,
        help="Path to churches.json.",
    )
    parser.add_argument(
        "--events-path",
        default=settings.events_path,
        help="Path to events.json.",
    )
    parser.add_argument(
        "--intentions-path",
        default=settings.intentions_path,
        help="Path to intentions.json.",
    )
    parser.add_argument(
        "--bulletins-dir",
        default=settings.bulletins_dir,
        help="Directory used for downloaded bulletin PDFs.",
    )
    parser.add_argument(
        "--bulletin-cache-path",
        default=settings.bulletin_cache_path,
        help="JSON index storing bulletin file mappings, scrape attempts, source URLs, and failure state.",
    )
    parser.add_argument(
        "--runs-dir",
        default=settings.runs_dir,
        help="Directory used for structured run artifacts.",
    )
    parser.add_argument(
        "--log-level",
        default=settings.log_level,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level.",
    )
    return parser


def parse_args(argv: list[str] | None = None) -> tuple[RunConfig, AppPaths]:
    parser = build_parser()
    args = parser.parse_args(argv)
    if _should_prompt_interactively(argv, args.interactive):
        args = _prompt_for_run_settings(args)

    root = Path(__file__).resolve().parent.parent
    targets_value = args.targets
    if targets_value == get_settings().default_targets and args.mode != "mass":
        targets_value = {
            "mass": "schedule",
            "events": "events",
            "intentions": "intentions",
        }[args.mode]

    input_modes_value = args.input_modes
    if args.no_images:
        resolved_input_modes = [mode for mode in _split_csv(input_modes_value) if mode != "images"]
        input_modes_value = ",".join(resolved_input_modes or ["text", "pdf"])

    churches_path = root / args.churches_path
    events_path = root / args.events_path
    intentions_path = root / args.intentions_path
    paths = AppPaths(
        root=root,
        data_dir=churches_path.parent,
        churches_path=churches_path,
        events_path=events_path,
        intentions_path=intentions_path,
        bulletins_dir=root / args.bulletins_dir,
        bulletin_cache_path=root / args.bulletin_cache_path,
        runs_dir=root / args.runs_dir,
    )
    config = RunConfig.model_validate({
        "targets": _split_csv(targets_value),
        "strategies": _split_csv(args.strategies),
        "input_modes": _split_csv(input_modes_value),
        "apply_changes": args.apply or args.modify_json,
        "workers": args.workers,
        "family_limit": args.family_limit,
        "family_filter": args.family_filter,
        "use_existing_bulletins": args.use_existing_bulletins,
        "refresh_bulletins": args.refresh_bulletins,
        "interactive": args.interactive,
        "log_level": args.log_level,
        "model": args.model,
    })
    return config, paths


def _split_csv(value: str) -> list[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


def _should_prompt_interactively(argv: list[str] | None, interactive_flag: bool) -> bool:
    if interactive_flag:
        return True
    return (not argv) and sys.stdin.isatty() and sys.stdout.isatty()


def _prompt_for_run_settings(args: argparse.Namespace) -> argparse.Namespace:
    args.interactive = True
    args.targets = _prompt_text(
        "Targets [schedule,events,intentions,combined]",
        args.targets,
    )
    args.strategies = _prompt_text(
        "Strategies [direct,extract-merge,reviewed]",
        args.strategies,
    )
    args.input_modes = _prompt_text(
        "Input modes [images,text,pdf]",
        args.input_modes,
    )
    args.family_filter = _prompt_text("Family filter (blank for all)", args.family_filter or "") or None
    args.family_limit = _prompt_optional_int("Family limit (blank for all)", args.family_limit)
    args.workers = _prompt_int("Workers", args.workers)
    args.apply = _prompt_bool("Apply selected results", args.apply or args.modify_json)
    args.modify_json = args.apply
    args.use_existing_bulletins = _prompt_bool("Force reuse of local bulletin PDFs", args.use_existing_bulletins)
    args.refresh_bulletins = _prompt_bool("Refresh bulletin cache this run", args.refresh_bulletins)
    args.model = _prompt_text("Model", args.model)
    args.log_level = _prompt_text("Log level [DEBUG,INFO,WARNING,ERROR]", args.log_level).upper()
    args.churches_path = _prompt_text("Churches JSON path", args.churches_path)
    args.events_path = _prompt_text("Events JSON path", args.events_path)
    args.intentions_path = _prompt_text("Intentions JSON path", args.intentions_path)
    args.bulletins_dir = _prompt_text("Bulletins directory", args.bulletins_dir)
    args.bulletin_cache_path = _prompt_text("Bulletin cache JSON path", args.bulletin_cache_path)
    args.runs_dir = _prompt_text("Runs directory", args.runs_dir)
    return args


def _prompt_text(label: str, default: str) -> str:
    response = input(f"{label} [{default}]: ").strip()
    return response or default


def _prompt_bool(label: str, default: bool) -> bool:
    default_label = "Y/n" if default else "y/N"
    while True:
        response = input(f"{label} [{default_label}]: ").strip().lower()
        if not response:
            return default
        if response in {"y", "yes"}:
            return True
        if response in {"n", "no"}:
            return False
        print("Enter y or n.")


def _prompt_int(label: str, default: int) -> int:
    while True:
        response = input(f"{label} [{default}]: ").strip()
        if not response:
            return default
        if response.isdigit() and int(response) > 0:
            return int(response)
        print("Enter a positive integer.")


def _prompt_optional_int(label: str, default: int | None) -> int | None:
    default_text = str(default) if default is not None else ""
    while True:
        response = input(f"{label} [{default_text}]: ").strip()
        if not response:
            return default
        if response.isdigit() and int(response) > 0:
            return int(response)
        print("Enter a positive integer or leave blank.")


def main(argv: list[str] | None = None) -> int:
    try:
        config, paths = parse_args(argv)
        from .runner import run_application

        return run_application(config, paths)
    except ValidationError as exc:
        print(exc, file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())