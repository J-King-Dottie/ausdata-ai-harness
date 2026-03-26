from __future__ import annotations

import logging
import os
import sys
import time
import json
import csv
from typing import Any, Dict, List, Optional
from pathlib import Path
from uuid import uuid4
from io import StringIO

from mcp.server.fastmcp import FastMCP
from openai import OpenAI

from .macro_data import (
    build_macro_shortlist,
    get_macro_candidate_metadata,
    retrieve_macro_candidate,
    run_macro_query,
)


logger = logging.getLogger("abs.backend.macro_mcp")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(
        logging.Formatter("[%(asctime)s] %(levelname)s %(name)s - %(message)s")
    )
    logger.addHandler(handler)
logger.setLevel(logging.INFO)
logger.propagate = False
RUNTIME_DIR = Path(os.getenv("NISABA_RUNTIME_DIR") or Path(__file__).resolve().parents[2] / "runtime")
CONVERSATION_ID = str(os.getenv("NISABA_CONVERSATION_ID") or "").strip()
CODE_CONTAINER_ID = str(os.getenv("NISABA_CODE_CONTAINER_ID") or "").strip()
OPENAI_API_KEY = str(os.getenv("OPENAI_API_KEY") or "").strip()


def _cid_prefix() -> str:
    return f"cid={CONVERSATION_ID} " if CONVERSATION_ID else ""


def _summary(payload: Any) -> Dict[str, Any]:
    if isinstance(payload, dict):
        summary: Dict[str, Any] = {"keys": sorted(str(key) for key in list(payload.keys())[:12])}
        if isinstance(payload.get("candidates"), list):
            summary["candidates"] = len(payload["candidates"])
        if isinstance(payload.get("series"), list):
            summary["series"] = len(payload["series"])
        selected = payload.get("selected_indicator")
        if isinstance(selected, dict):
            summary["indicator"] = str(selected.get("indicator_label") or "").strip()
        provider = payload.get("provider") or payload.get("provider_key")
        if provider:
            summary["provider"] = str(provider).strip()
        return summary
    return {"type": type(payload).__name__}


def _artifact_path(artifact_id: str) -> Path:
    return RUNTIME_DIR / "conversations" / CONVERSATION_ID / "artifacts" / f"{artifact_id}.json"


def _latest_macro_artifact_id() -> str | None:
    if not CONVERSATION_ID:
        return None
    artifact_dir = RUNTIME_DIR / "conversations" / CONVERSATION_ID / "artifacts"
    if not artifact_dir.exists():
        return None
    candidates = [
        path for path in artifact_dir.glob("*.json")
        if path.name.startswith("raw-macro-") or path.name.startswith("narrowed-macro-") or path.name.startswith("artifact-")
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


def _flatten_macro_payload(payload: Dict[str, Any]) -> tuple[List[str], List[List[Any]]]:
    series_items = payload.get("series")
    if not isinstance(series_items, list):
        return [], []
    headers = ["provider", "country", "country_code", "indicator", "series_id", "frequency", "unit", "x", "y"]
    rows: List[List[Any]] = []
    for series in series_items:
        if not isinstance(series, dict):
            continue
        for point in series.get("points") or []:
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
    preview: List[Dict[str, Any]] = []
    for row in rows[:limit]:
        preview.append({headers[index]: row[index] for index in range(min(len(headers), len(row)))})
    return preview


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
        country = str(series.get("country_code") or series.get("country") or "").strip()
        if country and country not in countries and len(countries) < 12:
            countries.append(country)
        frequency = str(series.get("frequency") or "").strip()
        if frequency and frequency not in frequencies:
            frequencies.append(frequency)
    manifest: Dict[str, Any] = {
        "artifact_id": artifact_id,
        "kind": kind,
        "label": label,
        "summary": summary_text,
        "provider": str(payload.get("provider") or payload.get("provider_key") or "").strip(),
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


def _matches_time_range(value: str, start: str, end: str) -> bool:
    clean = str(value or "").strip()
    if not clean:
        return False
    if start and clean < start:
        return False
    if end and clean > end:
        return False
    return True


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
    upload_name = _analysis_filename(artifact_id, label)
    client = OpenAI(api_key=OPENAI_API_KEY)
    uploaded = client.containers.files.create(
        CODE_CONTAINER_ID,
        file=(upload_name, csv_buffer.getvalue().encode("utf-8"), "text/csv"),
    )
    return {
        "analysis_container_id": CODE_CONTAINER_ID,
        "analysis_file_id": str(getattr(uploaded, "id", "") or ""),
        "analysis_filename": str(getattr(uploaded, "filename", "") or upload_name),
        "analysis_file": {
            "filename": str(getattr(uploaded, "filename", "") or upload_name),
            "container_id": CODE_CONTAINER_ID,
            "artifact_id": artifact_id,
        },
    }


def _store_macro_artifact(payload: Dict[str, Any], label: str) -> Dict[str, Any]:
    if not CONVERSATION_ID:
        raise RuntimeError("NISABA_CONVERSATION_ID is not set for macro MCP retrieval.")
    artifact_id = f"raw-macro-{uuid4()}"
    path = _artifact_path(artifact_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "artifact_id": artifact_id,
        "kind": "macro_retrieve",
        "label": label,
        "summary": f"Stored macro retrieval artifact for {label}. Inspect it before analysis.",
        "source_references": payload.get("source_references") if isinstance(payload.get("source_references"), list) else [],
        "manifest": _summary(payload),
    }


server = FastMCP(
    name="nisaba-macro-mcp",
    instructions=(
        "Access global macroeconomic and trade data for Nisaba. "
        "Use macro_search_catalog to shortlist candidates, macro_get_metadata to inspect a candidate, "
        "and macro_retrieve to fetch structured data from the selected provider."
    ),
)


@server.tool()
def macro_search_catalog(query: str, limit: int = 12) -> Dict[str, Any]:
    """Search the macro catalog and return a ranked shortlist of candidate indicators."""
    started_at = time.perf_counter()
    clean_limit = max(1, min(int(limit or 12), 40))
    logger.info(
        "%stool=macro_search_catalog event=start query=%r limit=%s",
        _cid_prefix(),
        str(query or "")[:160],
        clean_limit,
    )
    try:
        payload = build_macro_shortlist(query, limit=clean_limit)
    except Exception as exc:
        logger.exception(
            "%stool=macro_search_catalog event=failure duration_ms=%s error=%s",
            _cid_prefix(),
            int((time.perf_counter() - started_at) * 1000),
            exc,
        )
        raise
    logger.info(
        "%stool=macro_search_catalog event=success duration_ms=%s summary=%s",
        _cid_prefix(),
        int((time.perf_counter() - started_at) * 1000),
        _summary(payload),
    )
    return payload


@server.tool()
def macro_get_metadata(candidateId: str, query: str) -> Dict[str, Any]:
    """Fetch metadata for a shortlisted macro candidate using its candidateId and the original query."""
    started_at = time.perf_counter()
    logger.info(
        "%stool=macro_get_metadata event=start candidateId=%r query=%r",
        _cid_prefix(),
        str(candidateId or "")[:120],
        str(query or "")[:160],
    )
    try:
        payload = get_macro_candidate_metadata(candidateId, query)
    except Exception as exc:
        logger.exception(
            "%stool=macro_get_metadata event=failure duration_ms=%s candidateId=%r error=%s",
            _cid_prefix(),
            int((time.perf_counter() - started_at) * 1000),
            str(candidateId or "")[:120],
            exc,
        )
        raise
    logger.info(
        "%stool=macro_get_metadata event=success duration_ms=%s summary=%s",
        _cid_prefix(),
        int((time.perf_counter() - started_at) * 1000),
        _summary(payload),
    )
    return payload


@server.tool()
def macro_retrieve(
    query: str,
    candidateId: str = "",
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
    """Retrieve macro data either from a selected candidateId or by running the best direct match for the query."""
    started_at = time.perf_counter()
    clean_candidate_id = str(candidateId or "").strip()
    logger.info(
        "%stool=macro_retrieve event=start candidateId=%r query=%r countries=%s allCountries=%s years=%s-%s",
        _cid_prefix(),
        clean_candidate_id[:120],
        str(query or "")[:160],
        countries[:6] if isinstance(countries, list) else [],
        bool(allCountries),
        startYear,
        endYear,
    )
    try:
        if not clean_candidate_id:
            payload = run_macro_query(query)
            label = str(
                payload.get("concept_label")
                or payload.get("provider")
                or payload.get("provider_key")
                or "Macro dataset"
            ).strip()
            manifest = _store_macro_artifact(payload, label)
            logger.info(
                "%stool=macro_retrieve event=success duration_ms=%s summary=%s",
                _cid_prefix(),
                int((time.perf_counter() - started_at) * 1000),
                _summary(manifest),
            )
            return manifest

        payload = retrieve_macro_candidate(
            clean_candidate_id,
            query,
            countries=countries,
            all_countries=bool(allCountries),
            start_year=startYear,
            end_year=endYear,
            reporter_codes=reporterCodes,
            partner_codes=partnerCodes,
            flow_code=str(flowCode or "").strip().upper() or None,
            frequency_code=str(frequencyCode or "").strip().upper() or None,
            hs_codes=hsCodes,
        )
    except Exception as exc:
        logger.exception(
            "%stool=macro_retrieve event=failure duration_ms=%s candidateId=%r error=%s",
            _cid_prefix(),
            int((time.perf_counter() - started_at) * 1000),
            clean_candidate_id[:120],
            exc,
        )
        raise
    label = str(
        (
            payload.get("selected_indicator")
            if isinstance(payload.get("selected_indicator"), dict)
            else {}
        ).get("indicator_label")
        or payload.get("concept_label")
        or payload.get("provider")
        or "Macro dataset"
    ).strip()
    manifest = _store_macro_artifact(payload, label)
    logger.info(
        "%stool=macro_retrieve event=success duration_ms=%s summary=%s",
        _cid_prefix(),
        int((time.perf_counter() - started_at) * 1000),
        _summary(manifest),
    )
    return manifest


@server.tool()
def macro_inspect_artifact(artifactId: str = "") -> Dict[str, Any]:
    """Inspect a stored macro retrieval artifact and return a compact structural summary plus preview rows."""
    started_at = time.perf_counter()
    clean_artifact_id = str(artifactId or "").strip() or (_latest_macro_artifact_id() or "")
    logger.info(
        "%stool=macro_inspect_artifact event=start artifactId=%r",
        _cid_prefix(),
        clean_artifact_id[:120],
    )
    if not clean_artifact_id:
        raise RuntimeError("No macro artifact is available to inspect yet.")
    payload = _load_artifact_payload(clean_artifact_id)
    label = str(
        (
            payload.get("selected_indicator")
            if isinstance(payload.get("selected_indicator"), dict)
            else {}
        ).get("indicator_label")
        or payload.get("concept_label")
        or payload.get("provider")
        or clean_artifact_id
    ).strip()
    kind = str(payload.get("kind") or ("macro_narrowed" if clean_artifact_id.startswith("narrowed-macro-") else "macro_retrieve")).strip()
    manifest = _macro_manifest(
        clean_artifact_id,
        kind,
        label,
        f"Inspected macro artifact '{label}'.",
        payload,
    )
    logger.info(
        "%stool=macro_inspect_artifact event=success duration_ms=%s summary=%s",
        _cid_prefix(),
        int((time.perf_counter() - started_at) * 1000),
        _summary(manifest),
    )
    return manifest


@server.tool()
def macro_narrow_artifact(
    artifactId: str = "",
    countryCodes: Optional[List[str]] = None,
    frequencies: Optional[List[str]] = None,
    start: str = "",
    end: str = "",
    seriesKeyContains: str = "",
    maxSeries: int = 12,
) -> Dict[str, Any]:
    """Create a narrowed macro artifact by filtering the stored artifact down to the minimum slice needed."""
    started_at = time.perf_counter()
    clean_artifact_id = str(artifactId or "").strip() or (_latest_macro_artifact_id() or "")
    logger.info(
        "%stool=macro_narrow_artifact event=start artifactId=%r countries=%s frequencies=%s start=%r end=%r",
        _cid_prefix(),
        clean_artifact_id[:120],
        countryCodes[:6] if isinstance(countryCodes, list) else [],
        frequencies[:6] if isinstance(frequencies, list) else [],
        str(start or "")[:40],
        str(end or "")[:40],
    )
    if not clean_artifact_id:
        raise RuntimeError("No macro artifact is available to narrow yet.")
    payload = _load_artifact_payload(clean_artifact_id)
    label = str(
        (
            payload.get("selected_indicator")
            if isinstance(payload.get("selected_indicator"), dict)
            else {}
        ).get("indicator_label")
        or payload.get("concept_label")
        or payload.get("provider")
        or clean_artifact_id
    ).strip()
    kind = str(payload.get("kind") or ("macro_narrowed" if clean_artifact_id.startswith("narrowed-macro-") else "macro_retrieve")).strip()
    clean_countries = [str(item).strip().upper() for item in (countryCodes or []) if str(item).strip()]
    clean_frequencies = [str(item).strip().upper() for item in (frequencies or []) if str(item).strip()]
    clean_series_key_contains = str(seriesKeyContains or "").strip().lower()
    clean_start = str(start or "").strip()
    clean_end = str(end or "").strip()
    limited_max_series = max(1, min(int(maxSeries or 12), 40))
    no_explicit_filters = (
        not clean_countries and not clean_frequencies and not clean_series_key_contains and not clean_start and not clean_end
    )
    source_series = payload.get("series") if isinstance(payload.get("series"), list) else []
    if no_explicit_filters and kind == "macro_narrowed" and len(source_series) <= limited_max_series:
        headers, rows = _flatten_macro_payload(payload)
        analysis = _upload_analysis_csv(clean_artifact_id, label, headers, rows)
        manifest = _macro_manifest(
            clean_artifact_id,
            kind,
            label,
            f"Narrowed macro artifact '{label}'.",
            payload,
            analysis,
        )
        logger.info(
            "%stool=macro_narrow_artifact event=success duration_ms=%s summary=%s",
            _cid_prefix(),
            int((time.perf_counter() - started_at) * 1000),
            _summary(manifest),
        )
        return manifest

    narrowed_series: List[Dict[str, Any]] = []
    for series in source_series:
        if not isinstance(series, dict):
            continue
        country_code = str(series.get("country_code") or "").strip().upper()
        frequency = str(series.get("frequency") or "").strip().upper()
        series_id = str(series.get("series_id") or "").strip().lower()
        indicator = str(series.get("indicator") or "").strip().lower()
        if clean_countries and country_code not in clean_countries:
            continue
        if clean_frequencies and frequency not in clean_frequencies:
            continue
        if clean_series_key_contains and clean_series_key_contains not in f"{series_id} {indicator}":
            continue
        points: List[Dict[str, Any]] = []
        for point in series.get("points") or []:
            if not isinstance(point, dict):
                continue
            x = str(point.get("x") or "").strip()
            if (clean_start or clean_end) and not _matches_time_range(x, clean_start, clean_end):
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
    artifact_path = _artifact_path(artifact_id)
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text(json.dumps(narrowed_payload, ensure_ascii=False, indent=2), encoding="utf-8")
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
    logger.info(
        "%stool=macro_narrow_artifact event=success duration_ms=%s summary=%s",
        _cid_prefix(),
        int((time.perf_counter() - started_at) * 1000),
        _summary(manifest),
    )
    return manifest


def main() -> None:
    server.run(transport="stdio")


if __name__ == "__main__":
    main()
