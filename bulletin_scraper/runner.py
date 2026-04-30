from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any

from .adapters import build_adapter
from .config import AppPaths, RunConfig
from .graphs import run_strategy_graph
from .json_utils import to_jsonable
from .llm_client import OpenRouterLlmClient
from .logging_config import setup_logging
from .models import BulletinFamily, InputMode, StrategyKind, TargetKind, WorkflowCase, WorkflowResult
from .sources import build_families, build_input_artifact, ensure_family_documents, filter_families, load_data_bundle, save_data_bundle


def run_application(config: RunConfig, paths: AppPaths) -> int:
    log_level = getattr(logging, config.log_level)
    logger = setup_logging(log_level)
    targets = config.targets
    strategies = config.strategies
    input_modes = config.input_modes

    logger.info(
        "Running targets=%s strategies=%s input_modes=%s",
        ",".join(target.value for target in targets),
        ",".join(strategy.value for strategy in strategies),
        ",".join(mode.value for mode in input_modes),
    )

    bundle = load_data_bundle(paths)
    families = filter_families(build_families(bundle["churches"]), config)
    if not families:
        logger.warning("No bulletin families matched the current filter.")
        return 0

    families = ensure_family_documents(families, paths, config, logger)
    if not families:
        logger.warning("No bulletin documents were available after scraping/downloading.")
        return 0

    client = OpenRouterLlmClient(model=config.model)
    run_id = datetime.now().strftime("%Y%m%dT%H%M%S")
    run_dir = paths.runs_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    results_by_family: dict[str, list[WorkflowResult]] = {}
    with ThreadPoolExecutor(max_workers=max(config.workers, 1)) as executor:
        futures = {
            executor.submit(
                _run_family_suite,
                family,
                bundle,
                targets,
                strategies,
                input_modes,
                client,
                run_dir,
            ): family
            for family in families
        }
        for future in as_completed(futures):
            family = futures[future]
            try:
                family_results = future.result()
            except Exception as exc:
                logger.exception("Family %s failed unexpectedly: %s", family.family_id, exc)
                results_by_family[family.family_id] = []
                continue
            results_by_family[family.family_id] = family_results
            logger.info(
                "Completed family %s with %s workflow case(s)",
                family.family_id,
                len(family_results),
            )

    selected_results = _select_results(results_by_family)
    if config.apply_changes:
        _apply_selected_results(selected_results, results_by_family, bundle, families)
        save_data_bundle(paths, bundle)
        logger.info("Persisted selected workflow results into data files.")

    _write_run_artifacts(run_dir, families, results_by_family, selected_results, config)
    logger.info("Wrote run artifacts to %s", run_dir)
    return 0


def _run_family_suite(
    family: BulletinFamily,
    bundle: dict[str, Any],
    targets: list[TargetKind],
    strategies: list[StrategyKind],
    input_modes: list[InputMode],
    client: OpenRouterLlmClient,
    run_dir: Path,
) -> list[WorkflowResult]:
    family_results: list[WorkflowResult] = []
    family_artifacts_dir = run_dir / "inputs"
    for target in targets:
        adapter = build_adapter(target)
        scope = adapter.get_scope(bundle, family)
        for strategy in strategies:
            for input_mode in input_modes:
                case = WorkflowCase(target=target, strategy=strategy, input_mode=input_mode)
                try:
                    artifact = build_input_artifact(family, input_mode, family_artifacts_dir, adapter.max_pages)
                    output, raw_outputs = run_strategy_graph(strategy, adapter, client, family, artifact, scope)
                    output = adapter.postprocess_output(scope, output)
                    result = WorkflowResult(
                        family_id=family.family_id,
                        case=case,
                        status="ok",
                        output=output,
                        raw_outputs=raw_outputs,
                        summary=adapter.summarize(output, {}),
                        score=_score_case(case),
                    )
                except Exception as exc:
                    result = WorkflowResult(
                        family_id=family.family_id,
                        case=case,
                        status="error",
                        error=str(exc),
                        warnings=[str(exc)],
                        score=-1,
                    )
                family_results.append(result)
    return family_results


def _select_results(results_by_family: dict[str, list[WorkflowResult]]) -> dict[tuple[str, str], WorkflowResult]:
    selected: dict[tuple[str, str], WorkflowResult] = {}
    for family_id, results in results_by_family.items():
        grouped: dict[str, list[WorkflowResult]] = {}
        for result in results:
            grouped.setdefault(result.case.target.value, []).append(result)
        for target_name, candidates in grouped.items():
            valid = [candidate for candidate in candidates if candidate.status == "ok"]
            if not valid:
                continue
            best = max(
                valid,
                key=lambda candidate: (
                    _payload_size_for_target(target_name, candidate.output or {}) > 0,
                    _payload_size_for_target(target_name, candidate.output or {}),
                    candidate.score,
                ),
            )
            best.selected = True
            selected[(family_id, target_name)] = best
    return selected


def _apply_selected_results(
    selected_results: dict[tuple[str, str], WorkflowResult],
    results_by_family: dict[str, list[WorkflowResult]],
    bundle: dict[str, Any],
    families: list[BulletinFamily],
) -> None:
    families_by_id = {family.family_id: family for family in families}
    for (family_id, target_name), result in selected_results.items():
        family = families_by_id[family_id]
        adapter = build_adapter(TargetKind(target_name))
        apply_details = adapter.apply(bundle, family, result.output or {})
        result.apply_details = apply_details
        result.summary = adapter.summarize(result.output or {}, apply_details)


def _write_run_artifacts(
    run_dir: Path,
    families: list[BulletinFamily],
    results_by_family: dict[str, list[WorkflowResult]],
    selected_results: dict[tuple[str, str], WorkflowResult],
    config: RunConfig,
) -> None:
    families_payload = []
    for family in families:
        family_dir = run_dir / "families" / family.family_id
        family_dir.mkdir(parents=True, exist_ok=True)
        family_results = results_by_family.get(family.family_id, [])
        for result in family_results:
            artifact_path = family_dir / f"{result.case.case_id}.json"
            payload = {
                "family": family,
                "case": result.case,
                "status": result.status,
                "selected": result.selected,
                "summary": result.summary,
                "warnings": result.warnings,
                "error": result.error,
                "score": result.score,
                "apply_details": result.apply_details,
                "output": result.output,
                "raw_outputs": result.raw_outputs,
            }
            artifact_path.write_text(
                json.dumps(to_jsonable(payload), indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            result.artifact_path = artifact_path

        families_payload.append(
            {
                "family": family,
                "results": family_results,
            }
        )

    manifest = {
        "run_config": config,
        "families": families_payload,
        "selected_results": {
            f"{family_id}:{target_name}": result for (family_id, target_name), result in selected_results.items()
        },
    }
    (run_dir / "manifest.json").write_text(
        json.dumps(to_jsonable(manifest), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    summary_lines = [
        "# Bulletin Workflow Summary",
        "",
        f"Applied changes: {'yes' if config.apply_changes else 'no'}",
        "",
    ]
    for family in families:
        summary_lines.append(f"## {family.name} ({family.family_id})")
        family_results = results_by_family.get(family.family_id, [])
        if not family_results:
            summary_lines.append("No workflow results.")
            summary_lines.append("")
            continue
        for result in family_results:
            marker = "[selected]" if result.selected else "[candidate]"
            summary_lines.append(
                f"- {marker} {result.case.case_id}: {result.status}"
                f" | score={result.score} | {result.summary or result.error or 'no summary'}"
            )
        summary_lines.append("")

    (run_dir / "summary.md").write_text("\n".join(summary_lines), encoding="utf-8")


def _score_case(case: WorkflowCase) -> int:
    strategy_score = {
        StrategyKind.REVIEWED: 300,
        StrategyKind.EXTRACT_MERGE: 200,
        StrategyKind.DIRECT: 100,
    }[case.strategy]
    input_score = {
        InputMode.IMAGES: 30,
        InputMode.TEXT: 20,
        InputMode.PDF: 10,
    }[case.input_mode]
    return strategy_score + input_score


def _payload_size_for_target(target_name: str, payload: dict[str, Any]) -> int:
    adapter = build_adapter(TargetKind(target_name))
    return adapter.payload_size(payload)