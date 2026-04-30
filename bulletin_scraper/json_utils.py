from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import BaseModel


def extract_json_document(text: str) -> str:
    start = _find_json_start(text)
    if start == -1:
        raise ValueError("No JSON object or array found in model response.")

    opening = text[start]
    closing = "}" if opening == "{" else "]"
    depth = 0
    in_string = False
    escape = False

    for index in range(start, len(text)):
        char = text[index]
        if in_string:
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == '"':
                in_string = False
            continue

        if char == '"':
            in_string = True
            continue

        if char == opening:
            depth += 1
        elif char == closing:
            depth -= 1
            if depth == 0:
                return text[start : index + 1]

    raise ValueError("Unterminated JSON document in model response.")


def parse_json_document(text: str) -> Any:
    return json.loads(extract_json_document(text))


def to_jsonable(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return to_jsonable(value.model_dump(mode="json", by_alias=True))
    if is_dataclass(value):
        return {key: to_jsonable(item) for key, item in asdict(value).items()}
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): to_jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [to_jsonable(item) for item in value]
    return value


def pretty_json(value: Any) -> str:
    return json.dumps(to_jsonable(value), indent=2, ensure_ascii=False, sort_keys=True)


def _find_json_start(text: str) -> int:
    candidates = [index for index in (text.find("{"), text.find("[")) if index != -1]
    return min(candidates) if candidates else -1
