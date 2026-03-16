from __future__ import annotations

import io
import json
import re
import traceback
from contextlib import redirect_stdout
from pathlib import Path
from typing import Any, Dict


ALLOWED_IMPORTS = {
    "collections",
    "csv",
    "datetime",
    "itertools",
    "json",
    "math",
    "pathlib",
    "re",
    "statistics",
}


def _restricted_import(name: str, globals_: Dict[str, Any] | None = None, locals_: Dict[str, Any] | None = None, fromlist=(), level: int = 0):
    root_name = name.split(".", 1)[0]
    if root_name not in ALLOWED_IMPORTS:
        raise ImportError(f"Import '{name}' is not allowed in the sandbox.")
    return __import__(name, globals_, locals_, fromlist, level)


SAFE_BUILTINS = {
    "abs": abs,
    "all": all,
    "any": any,
    "bool": bool,
    "dict": dict,
    "enumerate": enumerate,
    "Exception": Exception,
    "filter": filter,
    "float": float,
    "getattr": getattr,
    "hasattr": hasattr,
    "int": int,
    "isinstance": isinstance,
    "iter": iter,
    "KeyError": KeyError,
    "len": len,
    "list": list,
    "map": map,
    "max": max,
    "min": min,
    "next": next,
    "None": None,
    "print": print,
    "range": range,
    "repr": repr,
    "reversed": reversed,
    "round": round,
    "set": set,
    "sorted": sorted,
    "str": str,
    "sum": sum,
    "TypeError": TypeError,
    "type": type,
    "tuple": tuple,
    "ValueError": ValueError,
    "zip": zip,
    "__import__": _restricted_import,
}


_TIME_PART_RE = re.compile(r"\d+")


def _time_sort_key(value: Any) -> tuple[int, ...]:
    text = str(value or "").strip()
    parts = [int(part) for part in _TIME_PART_RE.findall(text)]
    return tuple(parts) if parts else (-1,)


def _ensure_row_mapping(row: Any) -> Dict[str, Any] | None:
    return row if isinstance(row, dict) else None


def _row_matches(row: Any, criteria: Dict[str, Any]) -> bool:
    typed_row = _ensure_row_mapping(row)
    if typed_row is None:
        return False
    for key, expected in criteria.items():
        if typed_row.get(key) != expected:
            return False
    return True


def _key_tuple_from_row(row: Dict[str, Any], key_fields: tuple[str, ...]) -> tuple[Any, ...]:
    return tuple(row.get(field) for field in key_fields)


def _coerce_number(value: Any, default: Any = None) -> Any:
    if value is None:
        return default
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip().replace(",", "")
        if not text:
            return default
        try:
            return float(text)
        except ValueError:
            return default
    return default


def _extract_numeric_from_row(row: Dict[str, Any], field: str, default: Any = None) -> Any:
    value = row.get(field)
    if isinstance(value, dict):
        for candidate in ("value", "raw", "numeric", "number"):
            numeric = _coerce_number(value.get(candidate), default=None)
            if numeric is not None:
                return numeric
        return default
    return _coerce_number(value, default=default)


def _safe_float(value: Any) -> float | None:
    coerced = _coerce_number(value, default=None)
    return float(coerced) if coerced is not None else None


def main() -> int:
    import sys

    if len(sys.argv) != 2:
        raise SystemExit("sandbox_runner.py requires exactly one payload path argument.")

    payload_path = Path(sys.argv[1])
    payload = json.loads(payload_path.read_text(encoding="utf-8"))

    artifact_map = payload.get("artifacts") or {}
    output_dir = Path(payload["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)
    soul_path = Path(__file__).resolve().parents[3] / "SOUL.md"

    created_artifacts = []

    def list_artifacts():
        return [
            {
                "artifact_id": artifact_id,
                "kind": str(meta.get("kind") or ""),
                "label": str(meta.get("label") or ""),
                "summary": str(meta.get("summary") or ""),
            }
            for artifact_id, meta in artifact_map.items()
        ]

    def load_artifact(artifact_id: str):
        if artifact_id not in artifact_map:
            raise KeyError(f"Unknown artifact_id: {artifact_id}")
        path = Path(str(artifact_map[artifact_id]["path"]))
        return json.loads(path.read_text(encoding="utf-8"))

    def inspect_artifact(artifact_id: str):
        artifact = load_artifact(artifact_id)
        meta = artifact_map.get(artifact_id) or {}
        if isinstance(artifact, dict):
            top_level_keys = sorted(artifact.keys())
            resolved_dataset = artifact.get("resolved_dataset")
            resolved_dataset_keys = (
                sorted(resolved_dataset.keys())
                if isinstance(resolved_dataset, dict)
                else []
            )
            result_value = artifact.get("result")
            result_keys = sorted(result_value.keys()) if isinstance(result_value, dict) else []
            return {
                "artifact_id": artifact_id,
                "kind": str(meta.get("kind") or ""),
                "label": str(meta.get("label") or ""),
                "summary": str(meta.get("summary") or ""),
                "top_level_keys": top_level_keys,
                "has_resolved_dataset": isinstance(resolved_dataset, dict),
                "resolved_dataset_keys": resolved_dataset_keys[:20],
                "result_keys": result_keys[:20],
            }
        return {
            "artifact_id": artifact_id,
            "kind": str(meta.get("kind") or ""),
            "label": str(meta.get("label") or ""),
            "summary": str(meta.get("summary") or ""),
            "artifact_type": type(artifact).__name__,
        }

    def save_json(name: str, data: Any):
        filename = f"{name}.json" if not str(name).endswith(".json") else str(name)
        path = output_dir / filename
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        created_artifacts.append({"name": filename, "path": str(path), "kind": "sandbox_json"})
        return str(path)

    def save_text(name: str, text: str):
        filename = f"{name}.txt" if not str(name).endswith(".txt") else str(name)
        path = output_dir / filename
        path.write_text(str(text), encoding="utf-8")
        created_artifacts.append({"name": filename, "path": str(path), "kind": "sandbox_text"})
        return str(path)

    def get_resolved_dataset(artifact_id: str):
        artifact = load_artifact(artifact_id)
        if isinstance(artifact, dict) and isinstance(artifact.get("resolved_dataset"), dict):
            return artifact["resolved_dataset"]
        if isinstance(artifact, dict):
            return artifact
        raise TypeError("Artifact does not contain a resolved dataset object.")

    def get_series_rows(artifact_id: str):
        dataset = get_resolved_dataset(artifact_id)
        rows = []
        for series in dataset.get("series") or []:
            if not isinstance(series, dict):
                continue
            series_dims = series.get("dimensions") or {}
            for observation in series.get("observations") or []:
                if not isinstance(observation, dict):
                    continue
                row = {}
                for key, value in series_dims.items():
                    if isinstance(value, dict):
                        row[f"{key}_code"] = value.get("code")
                        if value.get("label") is not None:
                            row[f"{key}_label"] = value.get("label")
                    else:
                        row[key] = value
                obs_dims = observation.get("dimensions") or {}
                for key, value in obs_dims.items():
                    if isinstance(value, dict):
                        row[f"{key}_code"] = value.get("code")
                        if value.get("label") is not None:
                            row[f"{key}_label"] = value.get("label")
                    else:
                        row[key] = value
                row["observationKey"] = observation.get("observationKey")
                row["value"] = observation.get("value")
                row["value_numeric"] = _extract_numeric_from_row(row, "value", default=None)
                rows.append(row)
        return rows

    def inspect_artifact_schema(artifact_id: str):
        rows = get_series_rows(artifact_id)
        sample_row = rows[0] if rows else {}
        keys = sorted(sample_row.keys()) if isinstance(sample_row, dict) else []
        time_keys = [key for key in keys if "TIME" in key.upper()]
        value_keys = [key for key in keys if key == "value" or key.endswith("_value")]
        label_keys = [key for key in keys if key.endswith("_label")]
        code_keys = [key for key in keys if key.endswith("_code")]
        return {
            "row_count": len(rows),
            "sample_keys": keys,
            "sample_row": sample_row,
            "time_keys": time_keys,
            "value_keys": value_keys,
            "label_keys": label_keys[:20],
            "code_keys": code_keys[:20],
        }

    def sort_rows_by_time(rows, descending: bool = False):
        typed_rows = [row for row in rows if isinstance(row, dict)]
        return sorted(
            typed_rows,
            key=lambda row: _time_sort_key(row.get("TIME_PERIOD_code") or row.get("TIME_PERIOD")),
            reverse=descending,
        )

    def latest_row(rows):
        ordered = sort_rows_by_time(rows, descending=True)
        return ordered[0] if ordered else None

    def earliest_row(rows):
        ordered = sort_rows_by_time(rows, descending=False)
        return ordered[0] if ordered else None

    def numeric_change(first_value: Any, last_value: Any):
        if first_value is None or last_value is None:
            raise ValueError("numeric_change requires two non-null numeric values")
        first_num = float(first_value)
        last_num = float(last_value)
        absolute_change = last_num - first_num
        percent_change = None
        if first_num != 0:
            percent_change = (absolute_change / first_num) * 100.0
        return {
            "first": first_num,
            "last": last_num,
            "absolute_change": absolute_change,
            "percent_change": percent_change,
        }

    def coerce_number(value: Any, default: Any = None):
        return _coerce_number(value, default=default)

    def safe_float(value: Any):
        return _safe_float(value)

    def get_numeric(row: Any, field: str = "value", default: Any = None):
        typed_row = _ensure_row_mapping(row)
        if typed_row is None:
            return default
        return _extract_numeric_from_row(typed_row, field, default=default)

    def get_value(row: Any):
        return get_numeric(row, "value", default=None)

    def numeric_fields(rows):
        typed_rows = [_ensure_row_mapping(row) for row in rows]
        sample_rows = [row for row in typed_rows if row][:50]
        if not sample_rows:
            return []
        candidate_keys = sorted({key for row in sample_rows for key in row.keys()})
        fields = []
        for key in candidate_keys:
            numeric_count = 0
            populated_count = 0
            for row in sample_rows:
                if key not in row or row.get(key) is None:
                    continue
                populated_count += 1
                if _extract_numeric_from_row(row, key, default=None) is not None:
                    numeric_count += 1
            if populated_count and numeric_count == populated_count:
                fields.append(key)
        return fields

    def distinct_values(rows, field: str, *, drop_none: bool = True):
        seen = []
        seen_keys = set()
        for row in rows:
            typed_row = _ensure_row_mapping(row)
            if typed_row is None:
                continue
            value = typed_row.get(field)
            if drop_none and value is None:
                continue
            marker = json.dumps(value, ensure_ascii=False, sort_keys=True) if isinstance(value, (dict, list)) else value
            if marker in seen_keys:
                continue
            seen_keys.add(marker)
            seen.append(value)
        return seen

    def filter_rows(rows, **criteria):
        return [row for row in rows if _row_matches(row, criteria)]

    def find_row(rows, **criteria):
        for row in rows:
            if _row_matches(row, criteria):
                return row
        return None

    def require_row(rows, **criteria):
        row = find_row(rows, **criteria)
        if row is None:
            criteria_text = ", ".join(f"{key}={value!r}" for key, value in criteria.items()) or "<no criteria>"
            raise ValueError(f"No row matched: {criteria_text}")
        return row

    def safe_get(obj: Any, key: str, default: Any = None):
        if not isinstance(obj, dict):
            return default
        return obj.get(key, default)

    def require_fields(obj: Any, *fields: str):
        if not isinstance(obj, dict):
            raise TypeError("require_fields expects a dict")
        missing = [field for field in fields if field not in obj or obj.get(field) is None]
        if missing:
            raise ValueError(f"Missing required fields: {', '.join(missing)}")
        return obj

    def index_rows(rows, *key_fields: str):
        if not key_fields:
            raise ValueError("index_rows requires at least one key field")
        indexed = {}
        for row in rows:
            typed_row = _ensure_row_mapping(row)
            if typed_row is None:
                continue
            indexed[_key_tuple_from_row(typed_row, tuple(key_fields))] = typed_row
        return indexed

    def group_rows(rows, *key_fields: str):
        if not key_fields:
            raise ValueError("group_rows requires at least one key field")
        grouped = {}
        for row in rows:
            typed_row = _ensure_row_mapping(row)
            if typed_row is None:
                continue
            key = _key_tuple_from_row(typed_row, tuple(key_fields))
            grouped.setdefault(key, []).append(typed_row)
        return grouped

    def sum_values(rows, value_field: str = "value"):
        total = 0.0
        count = 0
        for row in rows:
            typed_row = _ensure_row_mapping(row)
            if typed_row is None:
                continue
            raw_value = typed_row.get(value_field)
            if raw_value is None:
                continue
            total += float(raw_value)
            count += 1
        return {"total": total, "count": count}

    def sort_by_numeric(rows, field: str = "value", *, descending: bool = False, missing: str = "last"):
        if missing not in {"first", "last"}:
            raise ValueError("sort_by_numeric missing must be 'first' or 'last'")

        typed_rows = []
        for row in rows:
            typed_row = _ensure_row_mapping(row)
            if typed_row is not None:
                typed_rows.append(typed_row)

        present = []
        missing_rows = []
        for row in typed_rows:
            numeric = _extract_numeric_from_row(row, field, default=None)
            if numeric is None:
                missing_rows.append(row)
            else:
                present.append((numeric, row))

        present.sort(key=lambda item: item[0], reverse=descending)
        ordered_present = [row for _, row in present]
        if missing == "first":
            return missing_rows + ordered_present
        return ordered_present + missing_rows

    def top_n_by_numeric(rows, field: str = "value", n: int = 5, *, descending: bool = True):
        limit = int(n)
        if limit < 0:
            raise ValueError("top_n_by_numeric n must be >= 0")
        ordered = sort_by_numeric(rows, field, descending=descending, missing="last")
        selected = []
        for row in ordered:
            if _extract_numeric_from_row(row, field, default=None) is None:
                continue
            selected.append(row)
            if len(selected) >= limit:
                break
        return selected

    def safe_ratio(numerator: Any, denominator: Any, *, default: Any = None):
        num = _coerce_number(numerator, default=None)
        den = _coerce_number(denominator, default=None)
        if num is None or den in {None, 0.0}:
            return default
        return num / den

    def latest_common_period(*row_sets):
        normalized_sets = []
        for rows in row_sets:
            period_set = set()
            for row in rows or []:
                typed_row = _ensure_row_mapping(row)
                if typed_row is None:
                    continue
                period = typed_row.get("TIME_PERIOD_code") or typed_row.get("TIME_PERIOD")
                if period is not None:
                    period_set.add(period)
            if not period_set:
                return None
            normalized_sets.append(period_set)
        if not normalized_sets:
            return None
        common = set.intersection(*normalized_sets)
        if not common:
            return None
        return max(common, key=_time_sort_key)

    def join_rows(left_rows, right_rows, left_keys, right_keys=None, *, how: str = "inner", right_prefix: str = "right_"):
        if isinstance(left_keys, str):
            left_key_fields = (left_keys,)
        else:
            left_key_fields = tuple(left_keys)
        if not left_key_fields:
            raise ValueError("join_rows requires at least one left key")

        if right_keys is None:
            right_key_fields = left_key_fields
        elif isinstance(right_keys, str):
            right_key_fields = (right_keys,)
        else:
            right_key_fields = tuple(right_keys)

        if len(left_key_fields) != len(right_key_fields):
            raise ValueError("join_rows requires the same number of left_keys and right_keys")
        if how not in {"inner", "left"}:
            raise ValueError("join_rows how must be 'inner' or 'left'")

        right_index = {}
        for row in right_rows:
            typed_row = _ensure_row_mapping(row)
            if typed_row is None:
                continue
            key = _key_tuple_from_row(typed_row, right_key_fields)
            right_index.setdefault(key, []).append(typed_row)

        joined = []
        for row in left_rows:
            left_typed = _ensure_row_mapping(row)
            if left_typed is None:
                continue
            left_key = _key_tuple_from_row(left_typed, left_key_fields)
            matches = right_index.get(left_key) or []
            if not matches and how == "left":
                joined.append(dict(left_typed))
                continue
            for match in matches:
                merged = dict(left_typed)
                for key, value in match.items():
                    if key in merged:
                        merged[f"{right_prefix}{key}"] = value
                    else:
                        merged[key] = value
                joined.append(merged)
        return joined

    def load_soul_md():
        if not soul_path.exists():
            raise FileNotFoundError(f"SOUL.md not found at {soul_path}")
        return soul_path.read_text(encoding="utf-8")

    exec_env: Dict[str, Any] = {
        "__builtins__": SAFE_BUILTINS,
        "coerce_number": coerce_number,
        "distinct_values": distinct_values,
        "earliest_row": earliest_row,
        "filter_rows": filter_rows,
        "find_row": find_row,
        "get_numeric": get_numeric,
        "get_value": get_value,
        "get_resolved_dataset": get_resolved_dataset,
        "get_series_rows": get_series_rows,
        "group_rows": group_rows,
        "inspect_artifact": inspect_artifact,
        "inspect_artifact_schema": inspect_artifact_schema,
        "index_rows": index_rows,
        "join_rows": join_rows,
        "latest_common_period": latest_common_period,
        "latest_row": latest_row,
        "list_artifacts": list_artifacts,
        "load_artifact": load_artifact,
        "load_soul_md": load_soul_md,
        "numeric_change": numeric_change,
        "numeric_fields": numeric_fields,
        "require_fields": require_fields,
        "require_row": require_row,
        "safe_float": safe_float,
        "safe_ratio": safe_ratio,
        "save_json": save_json,
        "save_text": save_text,
        "safe_get": safe_get,
        "sort_by_numeric": sort_by_numeric,
        "sum_values": sum_values,
        "top_n_by_numeric": top_n_by_numeric,
        "result": None,
        "sort_rows_by_time": sort_rows_by_time,
    }

    stdout_buffer = io.StringIO()
    error = None

    try:
        with redirect_stdout(stdout_buffer):
            # Use a single execution namespace so helper functions defined by the
            # model can reference themselves and each other normally.
            exec(payload["code"], exec_env, exec_env)
    except Exception:
        error = traceback.format_exc()

    result_payload = {
        "stdout": stdout_buffer.getvalue(),
        "result": exec_env.get("result"),
        "created_artifacts": created_artifacts,
        "error": error,
    }
    print(json.dumps(result_payload, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
