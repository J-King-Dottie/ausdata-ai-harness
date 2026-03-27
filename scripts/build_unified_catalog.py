#!/usr/bin/env python3

from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import httpx

from build_macro_catalog import (
    build_comtrade_catalog,
    dedupe_entries,
    fetch_imf_catalog,
    fetch_oecd_catalog,
    fetch_world_bank_catalog,
    filter_stale_entries,
)


ROOT = Path(__file__).resolve().parents[1]
ABS_DATAFLOWS_PATH = ROOT / "ABS_DATAFLOWS_FULL.json"
MANUAL_SOURCE_DEFINITIONS_PATH = ROOT / "MANUAL_SOURCE_DEFINITIONS.json"
CATALOG_ENRICHMENTS_PATH = ROOT / "CATALOG_ENRICHMENTS.json"
OUTPUT_PATH = ROOT / "UNIFIED_CATALOG_FULL.json"
FTS_DB_PATH = ROOT / "UNIFIED_CATALOG_FTS.sqlite3"


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").replace("\u0000", " ")).strip()


def _join_search_text(parts: List[str]) -> str:
    deduped: List[str] = []
    for part in parts:
        clean = _clean_text(part)
        if clean and clean not in deduped:
            deduped.append(clean)
    return " ".join(deduped)


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_abs_flows() -> List[Dict[str, Any]]:
    payload = _load_json(ABS_DATAFLOWS_PATH)
    flows = payload.get("flows") if isinstance(payload, dict) else None
    if isinstance(flows, list):
        return [item for item in flows if isinstance(item, dict)]
    legacy = payload.get("dataflows") if isinstance(payload, dict) else None
    if isinstance(legacy, list):
        return [item for item in legacy if isinstance(item, dict)]
    raise RuntimeError(f"Unsupported ABS snapshot format in {ABS_DATAFLOWS_PATH}")


def _load_manual_flows() -> List[Dict[str, Any]]:
    payload = _load_json(MANUAL_SOURCE_DEFINITIONS_PATH)
    flows = payload.get("flows") if isinstance(payload, dict) else None
    if isinstance(flows, list):
        return [item for item in flows if isinstance(item, dict)]
    legacy = payload.get("dataflows") if isinstance(payload, dict) else None
    if isinstance(legacy, list):
        return [item for item in legacy if isinstance(item, dict)]
    raise RuntimeError(f"Unsupported manual source definition format in {MANUAL_SOURCE_DEFINITIONS_PATH}")


def _load_enrichments() -> Dict[str, str]:
    if not CATALOG_ENRICHMENTS_PATH.exists():
        return {}
    payload = _load_json(CATALOG_ENRICHMENTS_PATH)
    items = payload.get("enrichments") if isinstance(payload, dict) else None
    if not isinstance(items, list):
        return {}
    result: Dict[str, str] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        dataset_id = _clean_text(item.get("datasetId"))
        if not dataset_id:
            continue
        topics = item.get("topics") if isinstance(item.get("topics"), list) else []
        search_terms = item.get("searchTerms") if isinstance(item.get("searchTerms"), list) else []
        notes = _clean_text(item.get("notes"))
        result[dataset_id] = _join_search_text(
            [*(str(value or "") for value in topics), *(str(value or "") for value in search_terms), notes]
        )
    return result


def _abs_source_url(agency_id: str, flow_id: str, version: str) -> str:
    clean_agency = _clean_text(agency_id)
    clean_flow = _clean_text(flow_id)
    clean_version = _clean_text(version)
    if not clean_agency or not clean_flow or not clean_version:
        return ""
    return f"https://data.api.abs.gov.au/rest/dataflow/{clean_agency}/{clean_flow}/{clean_version}"


def _manual_search_text(flow: Dict[str, Any]) -> str:
    curation = flow.get("curation") if isinstance(flow.get("curation"), dict) else {}
    table_code = _clean_text(curation.get("tableCode"))
    sheet_groups = curation.get("sheetGroups") if isinstance(curation.get("sheetGroups"), list) else []
    sheet_group_text = []
    for item in sheet_groups:
        if not isinstance(item, dict):
            continue
        sheet_group_text.append(_clean_text(item.get("id")))
        sheet_group_text.append(_clean_text(item.get("description")))
        sheets = item.get("sheets") if isinstance(item.get("sheets"), list) else []
        sheet_group_text.extend(_clean_text(sheet) for sheet in sheets)
    return _join_search_text(
        [
            _clean_text(flow.get("id")),
            _clean_text(flow.get("agencyID")),
            _clean_text(flow.get("name")),
            _clean_text(flow.get("description")),
            _clean_text(flow.get("flowType")),
            _clean_text(flow.get("sourceType")),
            _clean_text(flow.get("sourceOrganization")),
            _clean_text(flow.get("sourcePageUrl")),
            _clean_text(flow.get("sourceUrl")),
            table_code,
            *sheet_group_text,
        ]
    )


def _build_abs_entries(enrichments: Dict[str, str]) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    for flow in _load_abs_flows():
        flow_id = _clean_text(flow.get("id"))
        agency_id = _clean_text(flow.get("agencyID")) or "ABS"
        version = _clean_text(flow.get("version"))
        if not flow_id or not version:
            continue
        dataset_id = f"{agency_id},{flow_id},{version}"
        enrichment = enrichments.get(dataset_id) or enrichments.get(flow_id) or ""
        entries.append(
            {
                "route": "domestic",
                "provider": "ABS",
                "datasetId": dataset_id,
                "title": _clean_text(flow.get("name")) or flow_id,
                "description": _clean_text(flow.get("description")),
                "searchText": _join_search_text(
                    [
                        flow_id,
                        dataset_id,
                        agency_id,
                        _clean_text(flow.get("name")),
                        _clean_text(flow.get("description")),
                        enrichment,
                    ]
                ),
                "sourceUrl": _abs_source_url(agency_id, flow_id, version),
                "requiresMetadataBeforeRetrieval": True,
                "providerKey": "",
                "providerName": "ABS",
                "conceptId": "",
                "conceptLabel": "",
                "indicatorLabel": "",
                "unit": "",
                "providerConfig": {},
            }
        )
    return entries


def _build_manual_entries() -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    for flow in _load_manual_flows():
        flow_id = _clean_text(flow.get("id"))
        agency_id = _clean_text(flow.get("agencyID")) or "CUSTOM_AUS"
        version = _clean_text(flow.get("version")) or "1.0"
        if not flow_id:
            continue
        dataset_id = f"{agency_id},{flow_id},{version}"
        entries.append(
            {
                "route": "domestic",
                "provider": _clean_text(flow.get("sourceOrganization")) or agency_id,
                "datasetId": dataset_id,
                "title": _clean_text(flow.get("name")) or flow_id,
                "description": _clean_text(flow.get("description")),
                "searchText": _manual_search_text(flow),
                "sourceUrl": _clean_text(flow.get("sourcePageUrl")) or _clean_text(flow.get("sourceUrl")),
                "requiresMetadataBeforeRetrieval": bool(flow.get("requiresMetadataBeforeRetrieval")),
                "providerKey": "",
                "providerName": _clean_text(flow.get("sourceOrganization")) or agency_id,
                "conceptId": "",
                "conceptLabel": "",
                "indicatorLabel": "",
                "unit": "",
                "providerConfig": {},
            }
        )
    return entries


def _build_macro_entries() -> List[Dict[str, Any]]:
    with httpx.Client(follow_redirects=True) as client:
        world_bank_entries = fetch_world_bank_catalog(client)
        imf_entries = fetch_imf_catalog(client)
        oecd_entries = fetch_oecd_catalog(client)
    raw_entries = filter_stale_entries(
        dedupe_entries(build_comtrade_catalog() + world_bank_entries + imf_entries + oecd_entries)
    )
    entries: List[Dict[str, Any]] = []
    for item in raw_entries:
        if not isinstance(item, dict):
            continue
        provider_config = item.get("provider_config") if isinstance(item.get("provider_config"), dict) else {}
        dataset_id = _clean_text(item.get("entry_id"))
        if not dataset_id:
            continue
        requires_metadata = bool(provider_config.get("requires_metadata_before_retrieval")) or (
            _clean_text(item.get("provider_key")).lower() == "comtrade"
        )
        entries.append(
            {
                "route": "macro",
                "provider": _clean_text(item.get("provider_name")) or _clean_text(item.get("provider_key")),
                "datasetId": dataset_id,
                "title": _clean_text(item.get("indicator_label")) or _clean_text(item.get("concept_label")) or dataset_id,
                "description": _clean_text(item.get("description")),
                "searchText": _join_search_text(
                    [
                        _clean_text(item.get("entry_id")),
                        _clean_text(item.get("concept_id")),
                        _clean_text(item.get("concept_label")),
                        _clean_text(item.get("indicator_label")),
                        _clean_text(item.get("description")),
                        _clean_text(item.get("search_text")),
                        _clean_text(item.get("provider_key")),
                        _clean_text(item.get("provider_name")),
                    ]
                ),
                "sourceUrl": _clean_text(provider_config.get("source_url_template")),
                "requiresMetadataBeforeRetrieval": requires_metadata,
                "providerKey": _clean_text(item.get("provider_key")),
                "providerName": _clean_text(item.get("provider_name")),
                "conceptId": _clean_text(item.get("concept_id")),
                "conceptLabel": _clean_text(item.get("concept_label")),
                "indicatorLabel": _clean_text(item.get("indicator_label")),
                "unit": _clean_text(item.get("unit")),
                "providerConfig": provider_config,
            }
        )
    return entries


def _dedupe_entries(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduped: Dict[str, Dict[str, Any]] = {}
    for entry in entries:
        dataset_id = _clean_text(entry.get("datasetId"))
        if not dataset_id:
            continue
        deduped[dataset_id] = {
            "route": _clean_text(entry.get("route")),
            "provider": _clean_text(entry.get("provider")),
            "datasetId": dataset_id,
            "title": _clean_text(entry.get("title")) or dataset_id,
            "description": _clean_text(entry.get("description")),
            "searchText": _clean_text(entry.get("searchText")),
            "sourceUrl": _clean_text(entry.get("sourceUrl")),
            "requiresMetadataBeforeRetrieval": bool(entry.get("requiresMetadataBeforeRetrieval")),
            "providerKey": _clean_text(entry.get("providerKey")),
            "providerName": _clean_text(entry.get("providerName")),
            "conceptId": _clean_text(entry.get("conceptId")),
            "conceptLabel": _clean_text(entry.get("conceptLabel")),
            "indicatorLabel": _clean_text(entry.get("indicatorLabel")),
            "unit": _clean_text(entry.get("unit")),
            "providerConfig": dict(entry.get("providerConfig") or {}),
        }
    return sorted(
        deduped.values(),
        key=lambda item: (
            _clean_text(item.get("provider")).lower(),
            _clean_text(item.get("title")).lower(),
            _clean_text(item.get("datasetId")).lower(),
        ),
    )


def _write_catalog(entries: List[Dict[str, Any]]) -> None:
    payload = {
        "lastUpdated": datetime.now(timezone.utc).isoformat(),
        "entries": entries,
    }
    OUTPUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

def _build_fts(entries: List[Dict[str, Any]]) -> None:
    if FTS_DB_PATH.exists():
        FTS_DB_PATH.unlink()
    conn = sqlite3.connect(FTS_DB_PATH)
    try:
        conn.executescript(
            """
            CREATE TABLE catalog (
                dataset_id TEXT PRIMARY KEY,
                provider TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                search_text TEXT NOT NULL,
                source_url TEXT NOT NULL,
                requires_metadata_before_retrieval INTEGER NOT NULL
            );

            CREATE VIRTUAL TABLE catalog_fts USING fts5(
                provider,
                dataset_id,
                title,
                description,
                search_text,
                content='catalog',
                content_rowid='rowid'
            );
            """
        )
        conn.executemany(
            """
            INSERT INTO catalog (
                dataset_id,
                provider,
                title,
                description,
                search_text,
                source_url,
                requires_metadata_before_retrieval
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    entry["datasetId"],
                    entry["provider"],
                    entry["title"],
                    entry["description"],
                    entry["searchText"],
                    entry["sourceUrl"],
                    1 if entry["requiresMetadataBeforeRetrieval"] else 0,
                )
                for entry in entries
            ],
        )
        conn.execute(
            """
            INSERT INTO catalog_fts(rowid, provider, dataset_id, title, description, search_text)
            SELECT rowid, provider, dataset_id, title, description, search_text
            FROM catalog
            """
        )
        conn.commit()
    finally:
        conn.close()


def main() -> None:
    enrichments = _load_enrichments()
    macro_entries = _build_macro_entries()
    entries = _dedupe_entries(_build_abs_entries(enrichments) + _build_manual_entries() + macro_entries)
    _write_catalog(entries)
    _build_fts(entries)
    print(f"Wrote {len(entries)} unified catalog entries to {OUTPUT_PATH}")
    print(f"Wrote unified FTS database to {FTS_DB_PATH}")


if __name__ == "__main__":
    main()
