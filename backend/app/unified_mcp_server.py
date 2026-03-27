from __future__ import annotations

import csv
import json
import logging
import os
import sqlite3
import sys
import time
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

from mcp.server.fastmcp import FastMCP
from openai import OpenAI

from .config import get_settings
from .domestic_data import get_domestic_service
from .macro_data import (
    MacroCatalogEntry,
    _build_comtrade_metadata_payload,
    _fetch_comtrade,
    _fetch_imf,
    _fetch_oecd,
    _fetch_world_bank,
)
from .unified_catalog import (
    ensure_unified_catalog_artifacts,
    get_unified_catalog_entry,
    get_unified_source_record,
    search_unified_catalog,
)


settings = get_settings()
logger = logging.getLogger("abs.backend.unified_mcp")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(name)s - %(message)s"))
    logger.addHandler(handler)
logger.setLevel(logging.INFO)
logger.propagate = False

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RUNTIME_DIR = Path(os.getenv("NISABA_RUNTIME_DIR") or PROJECT_ROOT / "runtime")
CONVERSATION_ID = str(os.getenv("NISABA_CONVERSATION_ID") or "standalone").strip() or "standalone"
CODE_CONTAINER_ID = str(os.getenv("NISABA_CODE_CONTAINER_ID") or "").strip()
OPENAI_API_KEY = str(os.getenv("OPENAI_API_KEY") or "").strip()
MAX_ANALYSIS_UPLOAD_BYTES = 50 * 1024 * 1024


def _cid_prefix() -> str:
    return f"cid={CONVERSATION_ID} " if CONVERSATION_ID else ""


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _artifact_path(artifact_id: str) -> Path:
    return RUNTIME_DIR / "conversations" / CONVERSATION_ID / "artifacts" / f"{artifact_id}.json"


def _artifact_kind(artifact_id: str, payload: Optional[Dict[str, Any]] = None) -> str:
    payload_kind = _clean_text((payload or {}).get("kind"))
    if payload_kind:
        return payload_kind
    if artifact_id.startswith("raw-domestic-"):
        return "domestic_retrieve"
    if artifact_id.startswith("narrowed-domestic-"):
        return "domestic_narrowed"
    if artifact_id.startswith("raw-macro-"):
        return "macro_retrieve"
    if artifact_id.startswith("narrowed-macro-"):
        return "macro_narrowed"
    return ""


def _latest_artifact_id() -> Optional[str]:
    artifact_dir = RUNTIME_DIR / "conversations" / CONVERSATION_ID / "artifacts"
    if not artifact_dir.exists():
        return None
    candidates = [
        path
        for path in artifact_dir.glob("*.json")
        if any(
            path.name.startswith(prefix)
            for prefix in ("raw-domestic-", "narrowed-domestic-", "raw-macro-", "narrowed-macro-", "artifact-")
        )
    ]
    if not candidates:
        return None
    latest = max(candidates, key=lambda item: item.stat().st_mtime)
    return latest.stem


def _load_artifact_payload(artifact_id: str) -> Dict[str, Any]:
    path = _artifact_path(artifact_id)
    if not path.exists():
        raise RuntimeError(f"Artifact file is not available for {artifact_id}.")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"Artifact {artifact_id} is not a JSON object artifact.")
    return payload


def _store_artifact(payload: Dict[str, Any], artifact_id: str) -> None:
    path = _artifact_path(artifact_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _artifact_file_size_bytes(artifact_id: str) -> int:
    return int(_artifact_path(artifact_id).stat().st_size or 0)


def _analysis_filename(artifact_id: str, label: str) -> str:
    stem = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in str(label or "analysis")).strip("._-")
    stem = stem[:48] or "analysis"
    return f"{artifact_id}_{stem}.csv"


def _upload_analysis_csv(artifact_id: str, label: str, headers: List[str], rows: List[List[Any]]) -> Dict[str, Any]:
    if not CODE_CONTAINER_ID or not OPENAI_API_KEY or not headers:
        return {}
    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(headers)
    writer.writerows(rows)
    client = OpenAI(api_key=OPENAI_API_KEY)
    uploaded = client.containers.files.create(
        CODE_CONTAINER_ID,
        file=(_analysis_filename(artifact_id, label), csv_buffer.getvalue().encode("utf-8"), "text/csv"),
    )
    return {
        "analysis_container_id": CODE_CONTAINER_ID,
        "analysis_file_id": str(getattr(uploaded, "id", "") or ""),
        "analysis_filename": str(getattr(uploaded, "filename", "") or _analysis_filename(artifact_id, label)),
        "analysis_file": {
            "filename": str(getattr(uploaded, "filename", "") or _analysis_filename(artifact_id, label)),
            "container_id": CODE_CONTAINER_ID,
            "artifact_id": artifact_id,
        },
    }


def _is_domestic_dataset(dataset_id: str) -> bool:
    clean = _clean_text(dataset_id)
    return clean.startswith("ABS,") or clean.startswith("CUSTOM_AUS,")


def _is_custom_domestic_dataset(dataset_id: str) -> bool:
    return _clean_text(dataset_id).startswith("CUSTOM_AUS,")


def _normalize_anchor_type(dimension_id: str, concept_id: str) -> str:
    text = " ".join(part.upper() for part in [_clean_text(dimension_id), _clean_text(concept_id)] if part)
    if "MEASURE" in text:
        return "MEASURE"
    if "DATA_ITEM" in text or text.endswith("ITEM") or " ITEM" in text:
        return "DATA_ITEM"
    if any(token in text for token in ("CAT", "CATEGORY", "SUPG", "SUPC", "PRODUCT", "COMMODITY", "INDUSTRY", "SECTOR", "FLOW")):
        return "CATEGORY"
    return ""


def _anchor_priority(anchor_type: str) -> int:
    priority_map = {"DATA_ITEM": 100, "MEASURE": 90, "CATEGORY": 80}
    return priority_map.get(_clean_text(anchor_type).upper(), 0)


def _raw_metadata_payload(dataset_id: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
    dimensions = metadata.get("dimensions") if isinstance(metadata.get("dimensions"), list) else []
    concepts = metadata.get("concepts") if isinstance(metadata.get("concepts"), list) else []
    codelists = metadata.get("codelists") if isinstance(metadata.get("codelists"), list) else []
    codelist_by_id = {_clean_text(item.get("id")): item for item in codelists if isinstance(item, dict) and _clean_text(item.get("id"))}
    concept_by_id = {_clean_text(item.get("id")): item for item in concepts if isinstance(item, dict) and _clean_text(item.get("id"))}
    ordered_dimensions = sorted(
        [item for item in dimensions if isinstance(item, dict)],
        key=lambda item: int(item.get("position") or 0),
    )
    dimension_order: List[str] = []
    anchor_rows: List[Dict[str, Any]] = []

    for dimension in ordered_dimensions:
        dimension_id = _clean_text(dimension.get("id"))
        if not dimension_id:
            continue
        concept_id = _clean_text(dimension.get("conceptId"))
        concept = concept_by_id.get(concept_id, {})
        codelist_ref = dimension.get("codelist") if isinstance(dimension.get("codelist"), dict) else {}
        codelist_id = _clean_text(codelist_ref.get("id"))
        codelist = codelist_by_id.get(codelist_id, {})
        anchor_type = _normalize_anchor_type(dimension_id, concept_id)
        dimension_order.append(dimension_id)
        if not anchor_type:
            continue
        concept_name = (
            _clean_text(concept.get("name"))
            or _clean_text(concept.get("description"))
            or concept_id
            or dimension_id
        )
        anchor_codes = []
        for code in codelist.get("codes") if isinstance(codelist.get("codes"), list) else []:
            if not isinstance(code, dict):
                continue
            code_id = _clean_text(code.get("id"))
            if not code_id:
                continue
            anchor_codes.append(
                {
                    "code": code_id,
                    "label": _clean_text(code.get("name")),
                    "description": _clean_text(code.get("description")),
                }
            )
        anchor_rows.append(
            {
                "anchor_type": anchor_type,
                "dimension_id": dimension_id,
                "anchor_description": concept_name,
                "position": int(dimension.get("position") or 0),
                "anchor_codes": anchor_codes,
            }
        )

    def wildcard_template_for(anchor_dimension_id: str) -> str:
        parts = [f"{{{dimension_id}}}" if dimension_id == anchor_dimension_id else "" for dimension_id in dimension_order]
        return ".".join(parts) if parts else "all"

    anchor_candidates_by_type: Dict[str, Dict[str, Any]] = {}
    for row in anchor_rows:
        anchor_type = _clean_text(row.get("anchor_type")).upper()
        dimension_id = _clean_text(row.get("dimension_id"))
        if not anchor_type or not dimension_id:
            continue
        candidate = {
            "anchor_type": anchor_type,
            "anchor_description": _clean_text(row.get("anchor_description")) or anchor_type,
            "dimension_id": dimension_id,
            "wildcard_data_key_template": wildcard_template_for(dimension_id),
            "anchor_codes": row.get("anchor_codes") if isinstance(row.get("anchor_codes"), list) else [],
        }
        existing = anchor_candidates_by_type.get(anchor_type)
        if existing is None:
            anchor_candidates_by_type[anchor_type] = candidate
            continue
        existing_rank = dimension_order.index(_clean_text(existing.get("dimension_id")))
        current_rank = dimension_order.index(dimension_id)
        if existing_rank == -1 or (current_rank != -1 and current_rank < existing_rank):
            anchor_candidates_by_type[anchor_type] = candidate

    anchor_candidates = sorted(
        anchor_candidates_by_type.values(),
        key=lambda item: _anchor_priority(item.get("anchor_type", "")),
        reverse=True,
    )
    return {
        "kind": "raw_metadata",
        "dataset_id": dataset_id,
        "anchor_candidates": anchor_candidates,
        "metadata": metadata,
    }


def _build_wildcard_data_key(metadata_payload: Dict[str, Any], anchor_type: str, anchor_code: str) -> str:
    candidates = metadata_payload.get("anchor_candidates") if isinstance(metadata_payload.get("anchor_candidates"), list) else []
    target = None
    for item in candidates:
        if not isinstance(item, dict):
            continue
        if _clean_text(item.get("anchor_type")).upper() == _clean_text(anchor_type).upper():
            target = item
            break
    if target is None:
        raise RuntimeError(f"No anchor candidate found for anchorType={anchor_type}. Inspect metadata again.")
    allowed_codes = target.get("anchor_codes") if isinstance(target.get("anchor_codes"), list) else []
    clean_anchor_code = _clean_text(anchor_code)
    if not any(_clean_text(item.get("code")) == clean_anchor_code for item in allowed_codes if isinstance(item, dict)):
        raise RuntimeError(
            f"Invalid ABS anchor code '{clean_anchor_code}' for anchorType={anchor_type}. Choose a code from the metadata anchor_candidates list."
        )
    template = _clean_text(target.get("wildcard_data_key_template"))
    if not template:
        raise RuntimeError(f"Anchor candidate for anchorType={anchor_type} does not include a wildcard template.")
    return template.replace("{" + _clean_text(target.get("dimension_id")) + "}", clean_anchor_code)


def _validate_anchor_wildcard_data_key(dataset_id: str, data_key: str) -> None:
    clean_data_key = _clean_text(data_key)
    if not clean_data_key or clean_data_key.lower() == "all":
        raise RuntimeError(
            f"Invalid raw ABS dataKey. ABS retrieval must follow metadata-derived anchor selection; broad 'all' retrieval is not allowed. Received datasetId={dataset_id}, dataKey={data_key}."
        )
    segments = clean_data_key.split(".")
    fixed_segments = [segment.strip() for segment in segments if segment.strip()]
    if len(fixed_segments) != 1:
        raise RuntimeError(
            f"Invalid raw ABS dataKey. raw_retrieve must use exactly one anchored segment and wildcard every other segment. Received datasetId={dataset_id}, dataKey={data_key}."
        )
    anchor_token = fixed_segments[0]
    if "+" in anchor_token:
        raise RuntimeError(
            f"Invalid raw ABS dataKey. raw_retrieve must use exactly one anchor code, not multiple codes in one segment. Received datasetId={dataset_id}, dataKey={data_key}."
        )


def _store_domestic_artifact(payload: Dict[str, Any], label: str) -> Dict[str, Any]:
    artifact_id = f"raw-domestic-{uuid4()}"
    _store_artifact(payload, artifact_id)
    return {
        "artifact_id": artifact_id,
        "kind": "domestic_retrieve",
        "label": label,
        "summary": f"Stored domestic retrieval artifact for {label}. Inspect it before analysis.",
        "source_references": payload.get("source_references") if isinstance(payload.get("source_references"), list) else [],
        "manifest": _summary(payload),
    }


def _store_macro_artifact(payload: Dict[str, Any], label: str) -> Dict[str, Any]:
    artifact_id = f"raw-macro-{uuid4()}"
    _store_artifact(payload, artifact_id)
    return {
        "artifact_id": artifact_id,
        "kind": "macro_retrieve",
        "label": label,
        "summary": f"Stored macro retrieval artifact for {label}. Inspect it before analysis.",
        "source_references": payload.get("source_references") if isinstance(payload.get("source_references"), list) else [],
        "manifest": _summary(payload),
    }


def _summary(payload: Any) -> Dict[str, Any]:
    if isinstance(payload, dict):
        summary: Dict[str, Any] = {"keys": sorted(str(key) for key in list(payload.keys())[:12])}
        if isinstance(payload.get("candidates"), list):
            summary["candidates"] = len(payload["candidates"])
        if isinstance(payload.get("series"), list):
            summary["series"] = len(payload["series"])
        if isinstance(payload.get("dataflows"), list):
            summary["dataflows"] = len(payload["dataflows"])
        dataset = payload.get("dataset")
        if isinstance(dataset, dict):
            summary["datasetId"] = _clean_text(dataset.get("id"))
            summary["datasetName"] = _clean_text(dataset.get("name"))
        selected = payload.get("selected_indicator")
        if isinstance(selected, dict):
            summary["indicator"] = _clean_text(selected.get("indicator_label"))
        provider = payload.get("provider") or payload.get("provider_key")
        if provider:
            summary["provider"] = _clean_text(provider)
        return summary
    return {"type": type(payload).__name__}


def _flatten_macro_payload(payload: Dict[str, Any]) -> tuple[List[str], List[List[Any]]]:
    series_items = payload.get("series") if isinstance(payload.get("series"), list) else []
    headers = ["provider", "country", "country_code", "indicator", "series_id", "frequency", "unit", "x", "y"]
    rows: List[List[Any]] = []
    for series in series_items:
        if not isinstance(series, dict):
            continue
        for point in series.get("points") if isinstance(series.get("points"), list) else []:
            if not isinstance(point, dict):
                continue
            rows.append(
                [
                    series.get("provider"),
                    series.get("country"),
                    series.get("country_code"),
                    series.get("indicator"),
                    series.get("series_id"),
                    series.get("frequency"),
                    series.get("unit"),
                    point.get("x"),
                    point.get("y"),
                ]
            )
    return headers, rows


def _macro_preview_rows(payload: Dict[str, Any], limit: int = 8) -> List[Dict[str, Any]]:
    headers, rows = _flatten_macro_payload(payload)
    return [{headers[index]: row[index] for index in range(min(len(headers), len(row)))} for row in rows[:limit]]


def _matches_time_range(value: str, start: str, end: str) -> bool:
    clean = _clean_text(value)
    if not clean:
        return False
    if start and clean < start:
        return False
    if end and clean > end:
        return False
    return True


def _macro_manifest(
    artifact_id: str,
    kind: str,
    label: str,
    summary_text: str,
    payload: Dict[str, Any],
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    series_items = payload.get("series") if isinstance(payload.get("series"), list) else []
    point_count = 0
    countries: List[str] = []
    frequencies: List[str] = []
    for series in series_items:
        if not isinstance(series, dict):
            continue
        point_count += len(series.get("points") if isinstance(series.get("points"), list) else [])
        country = _clean_text(series.get("country_code") or series.get("country"))
        if country and country not in countries and len(countries) < 12:
            countries.append(country)
        frequency = _clean_text(series.get("frequency"))
        if frequency and frequency not in frequencies:
            frequencies.append(frequency)
    manifest: Dict[str, Any] = {
        "artifact_id": artifact_id,
        "kind": kind,
        "label": label,
        "summary": summary_text,
        "provider": _clean_text(payload.get("provider") or payload.get("provider_key")),
        "series_count": len(series_items),
        "point_count": point_count,
        "countries": countries,
        "frequencies": frequencies,
        "preview_rows": _macro_preview_rows(payload),
        "source_references": payload.get("source_references") if isinstance(payload.get("source_references"), list) else [],
    }
    if extra:
        manifest.update(extra)
    return manifest


def _flatten_domestic_payload(payload: Dict[str, Any]) -> tuple[List[str], List[List[Any]]]:
    series_items = payload.get("series") if isinstance(payload.get("series"), list) else []
    dimension_keys: List[str] = []
    attribute_keys: List[str] = []
    for series in series_items:
        if not isinstance(series, dict):
            continue
        series_dims = series.get("dimensions") if isinstance(series.get("dimensions"), dict) else {}
        for key in series_dims:
            if key not in dimension_keys:
                dimension_keys.append(key)
        observations = series.get("observations") if isinstance(series.get("observations"), list) else []
        for observation in observations:
            if not isinstance(observation, dict):
                continue
            obs_dims = observation.get("dimensions") if isinstance(observation.get("dimensions"), dict) else {}
            obs_attrs = observation.get("attributes") if isinstance(observation.get("attributes"), dict) else {}
            for key in obs_dims:
                if key not in dimension_keys:
                    dimension_keys.append(key)
            for key in obs_attrs:
                if key not in attribute_keys:
                    attribute_keys.append(key)
        series_attrs = series.get("attributes") if isinstance(series.get("attributes"), dict) else {}
        for key in series_attrs:
            if key not in attribute_keys:
                attribute_keys.append(key)

    headers = ["seriesKey", *dimension_keys, "observationKey", "value", *attribute_keys]
    rows: List[List[Any]] = []

    def label_or_value(value: Any) -> Any:
        if isinstance(value, dict):
            if value.get("label") is not None:
                return value.get("label")
            if value.get("code") is not None:
                return value.get("code")
        return value

    for series in series_items:
        if not isinstance(series, dict):
            continue
        series_dims = series.get("dimensions") if isinstance(series.get("dimensions"), dict) else {}
        series_attrs = series.get("attributes") if isinstance(series.get("attributes"), dict) else {}
        observations = series.get("observations") if isinstance(series.get("observations"), list) else []
        for observation in observations:
            if not isinstance(observation, dict):
                continue
            obs_dims = observation.get("dimensions") if isinstance(observation.get("dimensions"), dict) else {}
            obs_attrs = observation.get("attributes") if isinstance(observation.get("attributes"), dict) else {}
            row: List[Any] = [series.get("seriesKey")]
            for key in dimension_keys:
                row.append(label_or_value(obs_dims.get(key, series_dims.get(key))))
            row.append(observation.get("observationKey"))
            row.append(observation.get("value"))
            for key in attribute_keys:
                row.append(label_or_value(obs_attrs.get(key, series_attrs.get(key))))
            rows.append(row)
    return headers, rows


def _domestic_preview_rows(payload: Dict[str, Any], limit: int = 8) -> List[Dict[str, Any]]:
    headers, rows = _flatten_domestic_payload(payload)
    return [{headers[index]: row[index] for index in range(min(len(headers), len(row)))} for row in rows[:limit]]


def _domestic_slice_hints(payload: Dict[str, Any], row_limit: int = 600, value_limit: int = 6) -> Dict[str, List[str]]:
    headers, rows = _flatten_domestic_payload(payload)
    if not headers or not rows:
        return {}
    interesting = {
        "FREQ",
        "TSEST",
        "REGION",
        "AGE",
        "SEX",
        "MEASURE",
        "DATA_ITEM",
        "STATE",
        "SEAS_ADJ",
    }
    hints: Dict[str, List[str]] = {}
    header_index = {str(header): idx for idx, header in enumerate(headers)}
    for key in interesting:
        idx = header_index.get(key)
        if idx is None:
            continue
        values: List[str] = []
        for row in rows[:row_limit]:
            if idx >= len(row):
                continue
            value = _clean_text(row[idx])
            if not value or value in values:
                continue
            values.append(value)
            if len(values) >= value_limit:
                break
        if len(values) > 1:
            hints[key] = values
    return hints


def _estimate_csv_bytes(headers: List[str], rows: List[List[Any]]) -> int:
    buffer = StringIO()
    writer = csv.writer(buffer)
    if headers:
        writer.writerow(headers)
    writer.writerows(rows)
    return len(buffer.getvalue().encode("utf-8"))


def _is_matrix_style_domestic_payload(payload: Dict[str, Any]) -> bool:
    dataset = payload.get("dataset") if isinstance(payload.get("dataset"), dict) else {}
    dataset_id = _clean_text(dataset.get("id")).upper()
    label = _clean_text(dataset.get("name") or payload.get("label")).upper()
    return (
        dataset_id.startswith("ABS_SU_TABLE_")
        or "SUPPLY USE" in label
        or "SUPPLY-USE" in label
        or "INPUT OUTPUT" in label
        or "INPUT-OUTPUT" in label
        or "MATRIX" in label
    )


def _domestic_manifest(
    artifact_id: str,
    kind: str,
    label: str,
    summary_text: str,
    payload: Dict[str, Any],
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    dataset = payload.get("dataset") if isinstance(payload.get("dataset"), dict) else {}
    series_items = payload.get("series") if isinstance(payload.get("series"), list) else []
    observation_count = 0
    dimensions: Dict[str, List[str]] = {}
    for series in series_items:
        if not isinstance(series, dict):
            continue
        observations = series.get("observations") if isinstance(series.get("observations"), list) else []
        observation_count += len(observations)
        series_dims = series.get("dimensions") if isinstance(series.get("dimensions"), dict) else {}
        for key, value in series_dims.items():
            label_value = _clean_text(value.get("label") if isinstance(value, dict) else value)
            if not label_value:
                continue
            dimensions.setdefault(str(key), [])
            if label_value not in dimensions[str(key)] and len(dimensions[str(key)]) < 6:
                dimensions[str(key)].append(label_value)
    manifest: Dict[str, Any] = {
        "artifact_id": artifact_id,
        "kind": kind,
        "label": label,
        "summary": summary_text,
        "dataset_id": _clean_text(dataset.get("id")),
        "series_count": len(series_items),
        "observation_count": observation_count,
        "dimensions": dimensions,
        "preview_rows": _domestic_preview_rows(payload),
        "source_references": payload.get("source_references") if isinstance(payload.get("source_references"), list) else [],
    }
    if extra:
        manifest.update(extra)
    return manifest


def _normalize_dimension_filters(value: Any) -> Dict[str, List[str]]:
    if isinstance(value, list):
        normalized: Dict[str, List[str]] = {}
        for item in value:
            if not isinstance(item, dict):
                continue
            clean_key = _clean_text(item.get("dimension") or item.get("key"))
            raw_values = item.get("values") if isinstance(item.get("values"), list) else []
            values = [_clean_text(entry) for entry in raw_values if _clean_text(entry)]
            if clean_key and values:
                normalized[clean_key] = values
        return normalized
    if not isinstance(value, dict):
        return {}
    normalized: Dict[str, List[str]] = {}
    for key, raw in value.items():
        clean_key = _clean_text(key)
        if not clean_key:
            continue
        if isinstance(raw, list):
            values = [_clean_text(item) for item in raw if _clean_text(item)]
        else:
            single = _clean_text(raw)
            values = [single] if single else []
        if values:
            normalized[clean_key] = values
    return normalized


def _route_entry(dataset_id: str) -> Dict[str, Any]:
    entry = get_unified_catalog_entry(dataset_id)
    if entry is None:
        raise RuntimeError(f"Unknown datasetId '{dataset_id}'. Search the unified catalog first.")
    return entry


def _route_source_record(dataset_id: str) -> Dict[str, Any]:
    record = get_unified_source_record(dataset_id)
    if record is None:
        raise RuntimeError(f"Unknown datasetId '{dataset_id}' in unified source registry.")
    return record


def _macro_entry_from_record(record: Dict[str, Any]) -> MacroCatalogEntry:
    return MacroCatalogEntry(
        entry_id=_clean_text(record.get("datasetId")),
        provider_key=_clean_text(record.get("providerKey")),
        provider_name=_clean_text(record.get("providerName") or record.get("provider")),
        concept_id=_clean_text(record.get("conceptId")),
        concept_label=_clean_text(record.get("conceptLabel")),
        indicator_label=_clean_text(record.get("indicatorLabel")),
        unit=_clean_text(record.get("unit")),
        description=_clean_text(record.get("description")),
        search_text="",
        provider_config=dict(record.get("providerConfig") or {}),
    )


def _macro_metadata_from_record(record: Dict[str, Any], query: str) -> Dict[str, Any]:
    entry = _macro_entry_from_record(record)
    if entry.provider_key != "comtrade":
        return {
            "kind": "metadata_not_required",
            "dataset_id": entry.entry_id,
            "provider": entry.provider_name,
            "summary": "This source does not require a separate metadata step before retrieval.",
        }
    clean_query = _clean_text(query) or entry.indicator_label or entry.concept_label or entry.entry_id
    return _build_comtrade_metadata_payload(clean_query, entry)


def _retrieve_macro_from_record(
    record: Dict[str, Any],
    query: str,
    *,
    countries: Optional[List[str]] = None,
    all_countries: bool = False,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
    reporter_codes: Optional[List[str]] = None,
    partner_codes: Optional[List[str]] = None,
    flow_code: Optional[str] = None,
    frequency_code: Optional[str] = None,
    hs_codes: Optional[List[str]] = None,
) -> Dict[str, Any]:
    entry = _macro_entry_from_record(record)
    provider_key = entry.provider_key
    provider_config = dict(entry.provider_config)
    clean_query = _clean_text(query) or entry.indicator_label or entry.concept_label or entry.entry_id
    if provider_key == "worldbank":
        result = _fetch_world_bank(clean_query, entry, provider_config, countries or [], start_year, end_year, all_countries=all_countries)
    elif provider_key == "imf":
        result = _fetch_imf(clean_query, entry, provider_config, countries or [], start_year, end_year, all_countries=all_countries)
    elif provider_key == "oecd":
        result = _fetch_oecd(clean_query, entry, provider_config, countries or [], start_year, end_year, all_countries=all_countries)
    elif provider_key == "comtrade":
        result = _fetch_comtrade(
            clean_query,
            entry,
            provider_config,
            reporter_codes=[_clean_text(code) for code in (reporter_codes or []) if _clean_text(code)],
            partner_codes=[_clean_text(code) for code in (partner_codes or []) if _clean_text(code)],
            flow_code=_clean_text(flow_code).upper() or "",
            frequency_code=_clean_text(frequency_code).upper() or "",
            hs_codes=[_clean_text(code) for code in (hs_codes or []) if _clean_text(code)],
            start_year=start_year,
            end_year=end_year,
        )
    else:
        raise RuntimeError(f"Unsupported macro provider '{provider_key}'.")
    result["query"] = clean_query
    result["provider_key"] = provider_key
    result["selected_indicator"] = {
        "entry_id": entry.entry_id,
        "provider_key": entry.provider_key,
        "provider": entry.provider_name,
        "concept_id": entry.concept_id,
        "concept_label": entry.concept_label,
        "indicator_label": entry.indicator_label,
    }
    result["countries"] = countries or []
    result["all_countries"] = bool(all_countries)
    result["start_year"] = start_year
    result["end_year"] = end_year
    return result


server = FastMCP(
    name="nisaba-mcp",
    instructions=(
        "Access Nisaba data through one unified MCP. Preferred workflow: use search_catalog to shortlist datasets "
        "across Australian domestic and global macro sources, use get_metadata only when the selected source needs it, "
        "use retrieve to fetch the selected dataset through the correct source-specific adapter, then inspect_artifact "
        "before deciding whether narrow_artifact is needed. Do not invent dataset ids, ABS anchors, provider ids, or "
        "Comtrade codes. Respect source-specific retrieval semantics."
    ),
)


@server.tool()
def search_catalog(query: str, limit: int = 40, forceRefresh: bool = False) -> Dict[str, Any]:
    """Search the unified Nisaba catalog across ABS, curated Australian sources, and global macro sources."""
    started_at = time.perf_counter()
    logger.info("%stool=search_catalog event=start query=%r limit=%s refresh=%s", _cid_prefix(), query[:160], limit, forceRefresh)
    ensure_unified_catalog_artifacts(bool(forceRefresh))
    payload = search_unified_catalog(query, limit=limit, force_refresh=False)
    logger.info(
        "%stool=search_catalog event=success duration_ms=%s summary=%s",
        _cid_prefix(),
        int((time.perf_counter() - started_at) * 1000),
        _summary(payload),
    )
    return payload


@server.tool()
def get_metadata(datasetId: str, query: str = "", forceRefresh: bool = False) -> Dict[str, Any]:
    """Get metadata for the selected dataset when the source requires it. ABS uses metadata-first anchored retrieval. Comtrade uses a provider-specific metadata step."""
    started_at = time.perf_counter()
    logger.info("%stool=get_metadata event=start datasetId=%r query=%r", _cid_prefix(), datasetId[:160], query[:160])
    entry = _route_entry(datasetId)
    source_record = _route_source_record(datasetId)
    if _is_domestic_dataset(datasetId):
        payload = get_domestic_service().get_data_structure_for_dataflow(datasetId, bool(forceRefresh))
        result = payload if _is_custom_domestic_dataset(datasetId) else _raw_metadata_payload(datasetId, payload)
    else:
        clean_query = _clean_text(query) or _clean_text(entry.get("title")) or datasetId
        result = _macro_metadata_from_record(source_record, clean_query)
    logger.info(
        "%stool=get_metadata event=success duration_ms=%s summary=%s",
        _cid_prefix(),
        int((time.perf_counter() - started_at) * 1000),
        _summary(result),
    )
    return result


@server.tool()
def retrieve(
    datasetId: str,
    query: str = "",
    dataKey: str = "",
    anchorType: str = "",
    anchorCode: str = "",
    startPeriod: str = "",
    endPeriod: str = "",
    detail: str = "",
    dimensionAtObservation: str = "",
    forceRefresh: bool = False,
    countries: Optional[List[str]] = None,
    allCountries: bool = False,
    startYear: Optional[int] = None,
    endYear: Optional[int] = None,
    reporterCodes: Optional[List[str]] = None,
    partnerCodes: Optional[List[str]] = None,
    flowCode: str = "",
    frequencyCode: str = "",
    hsCodes: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Retrieve a shortlisted dataset through the correct source-specific path and store the result as an artifact."""
    started_at = time.perf_counter()
    logger.info("%stool=retrieve event=start datasetId=%r query=%r", _cid_prefix(), datasetId[:160], query[:160])
    entry = _route_entry(datasetId)
    source_record = _route_source_record(datasetId)
    if _is_domestic_dataset(datasetId):
        clean_data_key = _clean_text(dataKey)
        if not _is_custom_domestic_dataset(datasetId):
            if clean_data_key:
                raise RuntimeError(
                    "ABS retrieve does not accept dataKey input. Use get_metadata first, choose one anchor from anchor_candidates, and call retrieve with anchorType + anchorCode."
                )
            clean_anchor_type = _clean_text(anchorType).upper()
            clean_anchor_code = _clean_text(anchorCode)
            if not clean_anchor_type or not clean_anchor_code:
                raise RuntimeError(
                    "ABS retrieve requires anchorType + anchorCode from get_metadata. ABS always uses metadata-derived anchor + wildcard retrieval."
                )
            metadata = get_domestic_service().get_data_structure_for_dataflow(datasetId, bool(forceRefresh))
            metadata_payload = _raw_metadata_payload(datasetId, metadata)
            clean_data_key = _build_wildcard_data_key(metadata_payload, clean_anchor_type, clean_anchor_code)
            _validate_anchor_wildcard_data_key(datasetId, clean_data_key)
        result = get_domestic_service().resolve_dataset(
            datasetId,
            data_key=clean_data_key or "",
            start_period=_clean_text(startPeriod),
            end_period=_clean_text(endPeriod),
            detail=_clean_text(detail),
            dimension_at_observation=_clean_text(dimensionAtObservation),
            force_refresh=bool(forceRefresh),
        )
        dataset = result.get("dataset") if isinstance(result.get("dataset"), dict) else {}
        label = _clean_text(dataset.get("name")) or _clean_text(dataset.get("id")) or datasetId
        manifest = _store_domestic_artifact(result, label)
    else:
        clean_query = _clean_text(query) or _clean_text(entry.get("title")) or datasetId
        result = _retrieve_macro_from_record(
            source_record,
            clean_query,
            countries=countries,
            all_countries=bool(allCountries),
            start_year=startYear,
            end_year=endYear,
            reporter_codes=reporterCodes,
            partner_codes=partnerCodes,
            flow_code=_clean_text(flowCode).upper() or None,
            frequency_code=_clean_text(frequencyCode).upper() or None,
            hs_codes=hsCodes,
        )
        label = _clean_text(
            (result.get("selected_indicator") if isinstance(result.get("selected_indicator"), dict) else {}).get("indicator_label")
            or result.get("concept_label")
            or result.get("provider")
        ) or datasetId
        manifest = _store_macro_artifact(result, label)
    logger.info(
        "%stool=retrieve event=success duration_ms=%s summary=%s",
        _cid_prefix(),
        int((time.perf_counter() - started_at) * 1000),
        _summary(manifest),
    )
    return manifest


@server.tool()
def inspect_artifact(artifactId: str = "") -> Dict[str, Any]:
    """Inspect the latest stored artifact and decide whether it is ready for analysis or still needs narrowing."""
    started_at = time.perf_counter()
    clean_artifact_id = _clean_text(artifactId) or (_latest_artifact_id() or "")
    logger.info("%stool=inspect_artifact event=start artifactId=%r", _cid_prefix(), clean_artifact_id[:160])
    if not clean_artifact_id:
        raise RuntimeError("No artifact is available to inspect yet.")
    payload = _load_artifact_payload(clean_artifact_id)
    kind = _artifact_kind(clean_artifact_id, payload)
    if kind.startswith("domestic"):
        dataset = payload.get("dataset") if isinstance(payload.get("dataset"), dict) else {}
        label = _clean_text(dataset.get("name")) or clean_artifact_id
        headers, rows = _flatten_domestic_payload(payload)
        estimated_bytes = _estimate_csv_bytes(headers, rows)
        observation_count = 0
        series_items = payload.get("series") if isinstance(payload.get("series"), list) else []
        for series in series_items:
            if isinstance(series, dict) and isinstance(series.get("observations"), list):
                observation_count += len(series.get("observations") or [])
        slice_hints = _domestic_slice_hints(payload)
        extra: Dict[str, Any] = {
            "artifact_size_bytes": _artifact_file_size_bytes(clean_artifact_id),
            "analysis_estimated_bytes": estimated_bytes,
            "analysis_estimated_mb": round(estimated_bytes / (1024 * 1024), 2),
            "slice_hints": slice_hints,
            "default_variant_preferences": {
                "frequency": ["Annual", "Quarterly", "Monthly"],
                "adjustment": ["Trend", "Seasonally Adjusted", "Original"],
            },
        }
        if not _is_matrix_style_domestic_payload(payload) and (
            observation_count > 400
            or estimated_bytes > 500_000
            or bool(slice_hints.get("FREQ"))
            or bool(slice_hints.get("TSEST"))
            or bool(slice_hints.get("AGE"))
            or bool(slice_hints.get("SEX"))
            or bool(slice_hints.get("REGION"))
        ):
            extra.update(
                {
                    "analysis_should_narrow": True,
                    "analysis_guidance": (
                        "This domestic artifact is still broad. Before python analysis, use narrow_artifact to isolate the minimum comparable slice needed. "
                        "For time-series questions, prefer Annual over Quarterly or Monthly when the user did not ask for high-frequency detail, "
                        "and prefer Trend over Seasonally Adjusted over Original unless the user clearly wants another variant."
                    ),
                }
            )
        if _is_matrix_style_domestic_payload(payload) and estimated_bytes <= MAX_ANALYSIS_UPLOAD_BYTES:
            extra.update(_upload_analysis_csv(clean_artifact_id, label, headers, rows))
        elif _is_matrix_style_domestic_payload(payload) and estimated_bytes > MAX_ANALYSIS_UPLOAD_BYTES:
            extra.update(
                {
                    "analysis_too_large_for_direct_python": True,
                    "analysis_limit_bytes": MAX_ANALYSIS_UPLOAD_BYTES,
                    "analysis_guidance": "This matrix-style artifact is too large to send directly to python. Narrow to one correct full matrix or one metric/anchor first, then analyze that full matrix.",
                }
            )
        manifest = _domestic_manifest(clean_artifact_id, kind, label, f"Inspected domestic artifact '{label}'.", payload, extra)
    elif kind.startswith("macro"):
        label = _clean_text(
            (payload.get("selected_indicator") if isinstance(payload.get("selected_indicator"), dict) else {}).get("indicator_label")
            or payload.get("concept_label")
            or payload.get("provider")
            or clean_artifact_id
        )
        manifest = _macro_manifest(clean_artifact_id, kind, label, f"Inspected macro artifact '{label}'.", payload)
    else:
        raise RuntimeError(f"Unsupported artifact kind for {clean_artifact_id}.")
    logger.info(
        "%stool=inspect_artifact event=success duration_ms=%s summary=%s",
        _cid_prefix(),
        int((time.perf_counter() - started_at) * 1000),
        _summary(manifest),
    )
    return manifest


@server.tool()
def narrow_artifact(
    artifactId: str = "",
    dimensionFilters: Optional[List[Dict[str, Any]]] = None,
    countryCodes: Optional[List[str]] = None,
    frequencies: Optional[List[str]] = None,
    start: str = "",
    end: str = "",
    seriesKeyContains: str = "",
    maxSeries: int = 12,
) -> Dict[str, Any]:
    """Narrow a stored artifact to the minimum slice needed before analysis. For broad domestic time-series artifacts, prefer explicit dimension filters and comparable variants before using python."""
    started_at = time.perf_counter()
    clean_artifact_id = _clean_text(artifactId) or (_latest_artifact_id() or "")
    logger.info("%stool=narrow_artifact event=start artifactId=%r", _cid_prefix(), clean_artifact_id[:160])
    if not clean_artifact_id:
        raise RuntimeError("No artifact is available to narrow yet.")
    payload = _load_artifact_payload(clean_artifact_id)
    kind = _artifact_kind(clean_artifact_id, payload)
    clean_start = _clean_text(start)
    clean_end = _clean_text(end)
    clean_series_key_contains = _clean_text(seriesKeyContains).lower()
    limited_max_series = max(1, min(int(maxSeries or 12), 40))

    if kind.startswith("macro"):
        label = _clean_text(
            (payload.get("selected_indicator") if isinstance(payload.get("selected_indicator"), dict) else {}).get("indicator_label")
            or payload.get("concept_label")
            or payload.get("provider")
            or clean_artifact_id
        )
        clean_countries = [_clean_text(item).upper() for item in (countryCodes or []) if _clean_text(item)]
        clean_frequencies = [_clean_text(item).upper() for item in (frequencies or []) if _clean_text(item)]
        no_explicit_filters = not clean_countries and not clean_frequencies and not clean_series_key_contains and not clean_start and not clean_end
        source_series = payload.get("series") if isinstance(payload.get("series"), list) else []
        if no_explicit_filters and kind == "macro_narrowed" and len(source_series) <= limited_max_series:
            headers, rows = _flatten_macro_payload(payload)
            analysis = _upload_analysis_csv(clean_artifact_id, label, headers, rows)
            manifest = _macro_manifest(clean_artifact_id, kind, label, f"Narrowed macro artifact '{label}'.", payload, analysis)
            return manifest
        narrowed_series: List[Dict[str, Any]] = []
        for series in source_series:
            if not isinstance(series, dict):
                continue
            country_code = _clean_text(series.get("country_code")).upper()
            frequency = _clean_text(series.get("frequency")).upper()
            series_id = _clean_text(series.get("series_id")).lower()
            indicator = _clean_text(series.get("indicator")).lower()
            if clean_countries and country_code not in clean_countries:
                continue
            if clean_frequencies and frequency not in clean_frequencies:
                continue
            if clean_series_key_contains and clean_series_key_contains not in f"{series_id} {indicator}":
                continue
            points = []
            for point in series.get("points") if isinstance(series.get("points"), list) else []:
                if not isinstance(point, dict):
                    continue
                x_value = _clean_text(point.get("x"))
                if (clean_start or clean_end) and not _matches_time_range(x_value, clean_start, clean_end):
                    continue
                points.append(point)
            if not points:
                continue
            narrowed = dict(series)
            narrowed["points"] = points
            narrowed_series.append(narrowed)
            if len(narrowed_series) >= limited_max_series:
                break
        narrowed_payload = dict(payload)
        narrowed_payload["kind"] = "macro_narrowed"
        narrowed_payload["series"] = narrowed_series
        artifact_id = f"narrowed-macro-{uuid4()}"
        _store_artifact(narrowed_payload, artifact_id)
        headers, rows = _flatten_macro_payload(narrowed_payload)
        analysis = _upload_analysis_csv(artifact_id, f"{label} narrowed", headers, rows)
        manifest = _macro_manifest(
            artifact_id,
            "macro_narrowed",
            f"{label} (narrowed)",
            f"Narrowed macro artifact '{label}'.",
            narrowed_payload,
            {"parent_artifact_id": clean_artifact_id, **analysis},
        )
        return manifest

    if kind.startswith("domestic"):
        label = _clean_text(
            (payload.get("dataset") if isinstance(payload.get("dataset"), dict) else {}).get("name")
            or clean_artifact_id
        )
        dimension_filters = _normalize_dimension_filters(dimensionFilters)
        no_explicit_filters = not dimension_filters and not clean_series_key_contains and not clean_start and not clean_end
        matrix_style = _is_matrix_style_domestic_payload(payload)
        if matrix_style and no_explicit_filters:
            raise RuntimeError(
                "For supply-use, input-output, or matrix-style domestic tables, narrow_artifact requires a specific metric/anchor filter so it can isolate one correct full matrix before python analysis."
            )
        source_series = payload.get("series") if isinstance(payload.get("series"), list) else []
        if no_explicit_filters and kind == "domestic_narrowed" and len(source_series) <= limited_max_series:
            headers, rows = _flatten_domestic_payload(payload)
            analysis = _upload_analysis_csv(clean_artifact_id, label, headers, rows)
            return _domestic_manifest(clean_artifact_id, kind, label, f"Narrowed domestic artifact '{label}'.", payload, analysis)
        narrowed_series: List[Dict[str, Any]] = []
        for series in source_series:
            if not isinstance(series, dict):
                continue
            series_key = _clean_text(series.get("seriesKey")).lower()
            if clean_series_key_contains and clean_series_key_contains not in series_key:
                continue
            series_dims = series.get("dimensions") if isinstance(series.get("dimensions"), dict) else {}
            skip_series = False
            for key, allowed in dimension_filters.items():
                value = series_dims.get(key)
                label_value = _clean_text(value.get("label") if isinstance(value, dict) else value) or _clean_text(value.get("code") if isinstance(value, dict) else "")
                if label_value and label_value not in allowed:
                    skip_series = True
                    break
            if skip_series:
                continue
            observations = series.get("observations") if isinstance(series.get("observations"), list) else []
            if matrix_style:
                narrowed_series.append({**series, "observations": observations})
                if len(narrowed_series) >= limited_max_series:
                    break
                continue
            narrowed_observations = []
            for observation in observations:
                if not isinstance(observation, dict):
                    continue
                obs_dims = observation.get("dimensions") if isinstance(observation.get("dimensions"), dict) else {}
                matches_dims = True
                for key, allowed in dimension_filters.items():
                    value = obs_dims.get(key, series_dims.get(key))
                    label_value = _clean_text(value.get("label") if isinstance(value, dict) else value) or _clean_text(value.get("code") if isinstance(value, dict) else "")
                    if label_value and label_value not in allowed:
                        matches_dims = False
                        break
                if not matches_dims:
                    continue
                time_value = _clean_text(
                    observation.get("observationKey")
                    or (obs_dims.get("TIME_PERIOD") if isinstance(obs_dims.get("TIME_PERIOD"), dict) else {}).get("label")
                    or (obs_dims.get("TIME_PERIOD") if isinstance(obs_dims.get("TIME_PERIOD"), dict) else {}).get("code")
                )
                if (clean_start or clean_end) and not _matches_time_range(time_value, clean_start, clean_end):
                    continue
                narrowed_observations.append(observation)
            if not narrowed_observations:
                continue
            narrowed_series.append({**series, "observations": narrowed_observations})
            if len(narrowed_series) >= limited_max_series:
                break
        narrowed_payload = dict(payload)
        narrowed_payload["kind"] = "domestic_narrowed"
        narrowed_payload["series"] = narrowed_series
        artifact_id = f"narrowed-domestic-{uuid4()}"
        _store_artifact(narrowed_payload, artifact_id)
        headers, rows = _flatten_domestic_payload(narrowed_payload)
        estimated_bytes = _estimate_csv_bytes(headers, rows)
        if estimated_bytes > MAX_ANALYSIS_UPLOAD_BYTES:
            raise RuntimeError(
                f"Narrowed artifact is still too large for python handoff ({estimated_bytes / (1024 * 1024):.2f}MB > 50MB). Narrow further to one correct full matrix or one metric/anchor."
            )
        analysis = _upload_analysis_csv(artifact_id, f"{label} narrowed", headers, rows)
        return _domestic_manifest(
            artifact_id,
            "domestic_narrowed",
            f"{label} (narrowed)",
            f"Narrowed domestic artifact '{label}'.",
            narrowed_payload,
            {
                "parent_artifact_id": clean_artifact_id,
                "analysis_estimated_bytes": estimated_bytes,
                "analysis_estimated_mb": round(estimated_bytes / (1024 * 1024), 2),
                **analysis,
            },
        )

    raise RuntimeError(f"Unsupported artifact kind for {clean_artifact_id}.")


if __name__ == "__main__":
    server.run()
