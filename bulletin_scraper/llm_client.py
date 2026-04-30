from __future__ import annotations

import base64
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
        if not self.api_key:
            raise RuntimeError(
                "OPENROUTER_API_KEY is not set. Configure it in the environment or .env before running workflows."
            )
        self._client = None

    def invoke_json(self, prompt: str, artifact: InputArtifact, stage_name: str) -> tuple[dict[str, Any], str]:
        from langchain_core.messages import HumanMessage
        from langchain_core.output_parsers import JsonOutputParser
        from langchain_core.runnables import RunnableLambda

        client = self._get_client()
        parser = JsonOutputParser()
        prompt_with_format = f"{prompt}\n\nFORMAT INSTRUCTIONS:\n{parser.get_format_instructions()}"
        chain = (
            RunnableLambda(
                lambda params: [
                    HumanMessage(content=self._build_content(params["prompt"], params["artifact"]))
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
        if artifact.mode is InputMode.TEXT:
            return (
                f"{prompt}\n\nBULLETIN TEXT\n"
                f"-------------\n{artifact.payload}\n\n"
                "Return only JSON."
            )

        content = [{"type": "text", "text": prompt}]
        if artifact.mode is InputMode.IMAGES:
            for image_path in artifact.payload:
                content.append(
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": _path_to_image_data_url(image_path),
                            "detail": "high",
                        },
                    }
                )
            return content

        content.append(
            {
                "type": "file",
                "file": {
                    "filename": Path(artifact.payload).name,
                    "file_data": _path_to_pdf_data_url(Path(artifact.payload)),
                },
            }
        )
        return content


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
