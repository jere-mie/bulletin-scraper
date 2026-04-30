from __future__ import annotations

from typing import Any, TypedDict

from .adapters import TargetAdapter
from .llm_client import LlmClient
from .models import BulletinFamily, InputArtifact, StrategyKind


class WorkflowState(TypedDict, total=False):
    family: BulletinFamily
    artifact: InputArtifact
    scope: dict[str, Any]
    extraction: dict[str, Any]
    proposal: dict[str, Any]
    final_output: dict[str, Any]
    raw_outputs: dict[str, str]


def run_strategy_graph(
    strategy: StrategyKind,
    adapter: TargetAdapter,
    client: LlmClient,
    family: BulletinFamily,
    artifact: InputArtifact,
    scope: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, str]]:
    from langgraph.graph import END, START, StateGraph

    def prepare(_: WorkflowState) -> WorkflowState:
        return {
            "family": family,
            "artifact": artifact,
            "scope": scope,
            "raw_outputs": {},
        }

    def propose(state: WorkflowState) -> WorkflowState:
        payload, raw_text = client.invoke_json(
            adapter.build_direct_prompt(state["family"], state["scope"]),
            state["artifact"],
            "proposal",
        )
        raw_outputs = dict(state.get("raw_outputs", {}))
        raw_outputs["proposal"] = raw_text
        return {
            "proposal": adapter.coerce_final_payload(payload),
            "raw_outputs": raw_outputs,
        }

    def extract(state: WorkflowState) -> WorkflowState:
        payload, raw_text = client.invoke_json(
            adapter.build_extract_prompt(state["family"], state["scope"]),
            state["artifact"],
            "extraction",
        )
        raw_outputs = dict(state.get("raw_outputs", {}))
        raw_outputs["extraction"] = raw_text
        return {
            "extraction": adapter.coerce_extracted_payload(payload),
            "raw_outputs": raw_outputs,
        }

    def merge(state: WorkflowState) -> WorkflowState:
        payload, raw_text = client.invoke_json(
            adapter.build_merge_prompt(state["family"], state["scope"], state["extraction"]),
            state["artifact"],
            "merge",
        )
        raw_outputs = dict(state.get("raw_outputs", {}))
        raw_outputs["merge"] = raw_text
        return {
            "final_output": adapter.coerce_final_payload(payload),
            "raw_outputs": raw_outputs,
        }

    def review(state: WorkflowState) -> WorkflowState:
        payload, raw_text = client.invoke_json(
            adapter.build_review_prompt(state["family"], state["scope"], state["proposal"]),
            state["artifact"],
            "review",
        )
        raw_outputs = dict(state.get("raw_outputs", {}))
        raw_outputs["review"] = raw_text
        return {
            "final_output": adapter.coerce_final_payload(payload),
            "raw_outputs": raw_outputs,
        }

    def finalize_from_proposal(state: WorkflowState) -> WorkflowState:
        return {"final_output": state["proposal"]}

    graph = StateGraph(WorkflowState)
    graph.add_node("prepare", prepare)
    graph.add_node("propose", propose)
    graph.add_node("extract", extract)
    graph.add_node("merge", merge)
    graph.add_node("review", review)
    graph.add_node("finalize_from_proposal", finalize_from_proposal)

    graph.add_edge(START, "prepare")
    if strategy is StrategyKind.DIRECT:
        graph.add_edge("prepare", "propose")
        graph.add_edge("propose", "finalize_from_proposal")
        graph.add_edge("finalize_from_proposal", END)
    elif strategy is StrategyKind.EXTRACT_MERGE:
        graph.add_edge("prepare", "extract")
        graph.add_edge("extract", "merge")
        graph.add_edge("merge", END)
    elif strategy is StrategyKind.REVIEWED:
        graph.add_edge("prepare", "propose")
        graph.add_edge("propose", "review")
        graph.add_edge("review", END)
    else:
        raise ValueError(f"Unsupported strategy: {strategy}")

    compiled = graph.compile()
    result = compiled.invoke({})
    return result["final_output"], result.get("raw_outputs", {})
