from __future__ import annotations

import base64
from copy import deepcopy
from pathlib import Path
from typing import Any, Protocol

from .config import get_settings
from .models import InputArtifact, InputMode


class LlmClient(Protocol):
    def invoke_json(self, prompt: str, artifact: InputArtifact, stage_name: str) -> tuple[dict[str, Any], str]:
        ...


class OpenRouterLlmClient:
    def __init__(self, model: str, api_key: str | None = None) -> None:
        settings = get_settings()
        self.model = model
        self.api_key = api_key or settings.openrouter_api_key
        self.enable_prompt_caching = settings.enable_prompt_caching
        self.prompt_cache_ttl = settings.prompt_cache_ttl
        if not self.api_key:
            raise RuntimeError(
                "OPENROUTER_API_KEY is not set. Configure it in the environment or .env before running workflows."
            )
        self._client = None
        self._artifact_content_cache: dict[str, list[dict[str, Any]]] = {}

    def invoke_json(self, prompt: str, artifact: InputArtifact, stage_name: str) -> tuple[dict[str, Any], str]:
        from langchain_core.messages import HumanMessage, SystemMessage
        from langchain_core.output_parsers import JsonOutputParser
        from langchain_core.runnables import RunnableLambda

        client = self._get_client()
        parser = JsonOutputParser()
        prompt_with_format = f"{prompt}\n\nFORMAT INSTRUCTIONS:\n{parser.get_format_instructions()}"
        chain = (
            RunnableLambda(
                lambda params: [
                    SystemMessage(
                        content=(
                            "You extract structured Catholic bulletin data. "
                            "Return only valid JSON that matches the requested schema."
                        )
                    ),
                    HumanMessage(content=self._build_content(params["prompt"], params["artifact"])),
                ]
            )
            | client
            | RunnableLambda(lambda response: _flatten_response_content(response.content))
        )
        content = chain.invoke({"prompt": prompt_with_format, "artifact": artifact})
        payload = parser.invoke(content)
        if not isinstance(payload, dict):
            raise ValueError(f"{stage_name} response must be a JSON object.")
        return payload, content

    def _get_client(self):
        if self._client is None:
            from langchain_openai import ChatOpenAI

            settings = get_settings()

            self._client = ChatOpenAI(
                model=self.model,
                api_key=self.api_key,
                base_url=settings.openrouter_base_url,
                temperature=0,
                max_retries=2,
                default_headers={
                    "HTTP-Referer": settings.openrouter_site_url,
                    "X-Title": settings.openrouter_app_name,
                },
            )
        return self._client

    def _build_content(self, prompt: str, artifact: InputArtifact):
        content = [{"type": "text", "text": "Use the bulletin source below as the primary reference."}]
        content.extend(self._get_artifact_content_blocks(artifact))
        content.append({"type": "text", "text": prompt})
        return content

    def _get_artifact_content_blocks(self, artifact: InputArtifact) -> list[dict[str, Any]]:
        cache_key = artifact.cache_key()
        cached = self._artifact_content_cache.get(cache_key)
        if cached is not None:
            return deepcopy(cached)

        blocks: list[dict[str, Any]] = []
        if artifact.mode is InputMode.TEXT:
            blocks = [
                {"type": "text", "text": "BULLETIN TEXT\n-------------"},
                self._maybe_add_cache_control({"type": "text", "text": artifact.payload}),
            ]
        elif artifact.mode is InputMode.TEXT_IMAGES:
            blocks = [
                {"type": "text", "text": "BULLETIN TEXT\n-------------"},
                self._maybe_add_cache_control({"type": "text", "text": artifact.payload.get("text", "")}),
                {"type": "text", "text": "BULLETIN PAGE IMAGES\n-------------------"},
            ]
            for image_path in artifact.payload.get("images", []):
                blocks.append(
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": _path_to_image_data_url(image_path),
                            "detail": "high",
                        },
                    }
                )
        elif artifact.mode is InputMode.IMAGES:
            blocks = [{"type": "text", "text": "BULLETIN PAGE IMAGES\n-------------------"}]
            for image_path in artifact.payload:
                blocks.append(
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": _path_to_image_data_url(image_path),
                            "detail": "high",
                        },
                    }
                )
        else:
            blocks = [
                {
                    "type": "file",
                    "file": {
                        "filename": Path(artifact.payload).name,
                        "file_data": _path_to_pdf_data_url(Path(artifact.payload)),
                    },
                }
            ]

        self._artifact_content_cache[cache_key] = deepcopy(blocks)
        return deepcopy(blocks)

    def _maybe_add_cache_control(self, block: dict[str, Any]) -> dict[str, Any]:
        if not self.enable_prompt_caching:
            return block
        if block.get("type") != "text":
            return block
        text = str(block.get("text") or "")
        if len(text) < 1500:
            return block
        cache_control: dict[str, str] = {"type": "ephemeral"}
        if self.prompt_cache_ttl == "1h":
            cache_control["ttl"] = "1h"
        enriched = dict(block)
        enriched["cache_control"] = cache_control
        return enriched


def _flatten_response_content(content) -> str:
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict) and item.get("type") == "text":
                parts.append(item.get("text", ""))
        return "\n".join(part.strip() for part in parts if part.strip())
    return str(content).strip()


def _path_to_pdf_data_url(path: Path) -> str:
    raw_bytes = path.read_bytes()
    encoded = base64.b64encode(raw_bytes).decode("utf-8")
    return f"data:application/pdf;base64,{encoded}"


def _path_to_image_data_url(path: Path) -> str:
    suffix = path.suffix.lower()
    mime = "image/png" if suffix == ".png" else "image/jpeg"
    raw_bytes = path.read_bytes()
    encoded = base64.b64encode(raw_bytes).decode("utf-8")
    return f"data:{mime};base64,{encoded}"
