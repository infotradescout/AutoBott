from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any


SCHEMA_PATH = Path(__file__).resolve().parents[2] / "schemas" / "phase1_market_snapshot.schema.json"


class SnapshotValidationError(ValueError):
    def __init__(self, errors: list[str]) -> None:
        self.errors = errors
        super().__init__("Snapshot validation failed:\n" + "\n".join(f"- {error}" for error in errors))


def validate_market_snapshot(payload: dict[str, Any], schema_path: Path | None = None) -> None:
    schema = json.loads((schema_path or SCHEMA_PATH).read_text(encoding="utf-8"))
    errors = _validate_node(payload, schema, "$", schema)
    if errors:
        raise SnapshotValidationError(errors)


def _validate_node(value: Any, schema: dict[str, Any], path: str, root_schema: dict[str, Any]) -> list[str]:
    if "$ref" in schema:
        schema = _resolve_ref(schema["$ref"], root_schema)

    errors: list[str] = []
    expected_type = schema.get("type")
    if expected_type is not None and not _matches_type(value, expected_type):
        return [f"{path}: expected {_type_label(expected_type)}, got {type(value).__name__}"]

    if "enum" in schema and value not in schema["enum"]:
        errors.append(f"{path}: expected one of {schema['enum']}, got {value!r}")

    if isinstance(value, dict):
        required = schema.get("required", [])
        for field in required:
            if field not in value:
                errors.append(f"{path}.{field}: required field is missing")
        properties = schema.get("properties", {})
        for field, child_schema in properties.items():
            if field in value:
                errors.extend(_validate_node(value[field], child_schema, f"{path}.{field}", root_schema))

    if isinstance(value, list):
        min_items = schema.get("minItems")
        if min_items is not None and len(value) < min_items:
            errors.append(f"{path}: expected at least {min_items} items, got {len(value)}")
        child_schema = schema.get("items")
        if child_schema is not None:
            for index, item in enumerate(value):
                errors.extend(_validate_node(item, child_schema, f"{path}[{index}]", root_schema))

    if isinstance(value, str):
        min_length = schema.get("minLength")
        if min_length is not None and len(value) < min_length:
            errors.append(f"{path}: expected at least {min_length} characters")
        fmt = schema.get("format")
        if fmt == "date-time":
            errors.extend(_validate_datetime(value, path))
        elif fmt == "date":
            errors.extend(_validate_date(value, path))

    if _is_number(value):
        minimum = schema.get("minimum")
        if minimum is not None and value < minimum:
            errors.append(f"{path}: expected value >= {minimum}, got {value}")
        exclusive_minimum = schema.get("exclusiveMinimum")
        if exclusive_minimum is not None and value <= exclusive_minimum:
            errors.append(f"{path}: expected value > {exclusive_minimum}, got {value}")

    return errors


def _resolve_ref(ref: str, root_schema: dict[str, Any]) -> dict[str, Any]:
    if not ref.startswith("#/"):
        raise ValueError(f"Unsupported schema reference: {ref}")
    node: Any = root_schema
    for part in ref[2:].split("/"):
        node = node[part]
    return node


def _matches_type(value: Any, expected_type: str | list[str]) -> bool:
    types = expected_type if isinstance(expected_type, list) else [expected_type]
    return any(_matches_single_type(value, item) for item in types)


def _matches_single_type(value: Any, expected_type: str) -> bool:
    if expected_type == "object":
        return isinstance(value, dict)
    if expected_type == "array":
        return isinstance(value, list)
    if expected_type == "string":
        return isinstance(value, str)
    if expected_type == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected_type == "number":
        return _is_number(value)
    if expected_type == "boolean":
        return isinstance(value, bool)
    if expected_type == "null":
        return value is None
    return True


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _type_label(expected_type: str | list[str]) -> str:
    return " or ".join(expected_type) if isinstance(expected_type, list) else expected_type


def _validate_datetime(value: str, path: str) -> list[str]:
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return [f"{path}: expected ISO-8601 date-time"]
    return []


def _validate_date(value: str, path: str) -> list[str]:
    try:
        date.fromisoformat(value)
    except ValueError:
        return [f"{path}: expected ISO-8601 date"]
    return []
