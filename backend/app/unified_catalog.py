from __future__ import annotations

import json
import re
import sqlite3
import subprocess
import sys
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional


ROOT = Path(__file__).resolve().parents[2]
CATALOG_PATH = ROOT / "UNIFIED_CATALOG_FULL.json"
FTS_DB_PATH = ROOT / "UNIFIED_CATALOG_FTS.sqlite3"
BUILD_SCRIPT_PATH = ROOT / "scripts" / "build_unified_catalog.py"

STOPWORDS = {
    "the",
    "and",
    "for",
    "with",
    "from",
    "that",
    "this",
    "into",
    "over",
    "under",
    "using",
    "show",
    "data",
    "series",
    "table",
    "tables",
    "latest",
    "time",
    "timeseries",
    "trend",
    "what",
    "which",
    "where",
}


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _normalize_tokens(query: str) -> List[str]:
    raw_tokens = re.findall(r"[A-Za-z0-9]+", str(query or "").lower())
    return [token for token in raw_tokens if len(token) > 1 and token not in STOPWORDS]


def _build_match_query(tokens: List[str], operator: str) -> str:
    if not tokens:
        return ""
    return f" {operator} ".join(f'"{token}"*' for token in tokens)


def _strict_match_query(query: str) -> str:
    return _build_match_query(_normalize_tokens(query), "AND")


def _relaxed_match_query(query: str) -> str:
    return _build_match_query(_normalize_tokens(query), "OR")


def _invalidate_caches() -> None:
    _catalog_entries.cache_clear()
    _catalog_entries_by_id.cache_clear()


def ensure_unified_catalog_artifacts(force_refresh: bool = False) -> None:
    needs_build = force_refresh or not CATALOG_PATH.exists() or not FTS_DB_PATH.exists()
    if not needs_build:
        return
    subprocess.run([sys.executable, str(BUILD_SCRIPT_PATH)], cwd=str(ROOT), check=True)
    _invalidate_caches()


@lru_cache(maxsize=1)
def _catalog_entries() -> List[Dict[str, Any]]:
    ensure_unified_catalog_artifacts(False)
    payload = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
    entries = payload.get("entries") if isinstance(payload, dict) else None
    if not isinstance(entries, list):
        raise RuntimeError(f"Unified catalog file {CATALOG_PATH} must contain an 'entries' array.")
    normalized: List[Dict[str, Any]] = []
    for item in entries:
        if not isinstance(item, dict):
            continue
        dataset_id = _clean_text(item.get("datasetId"))
        if not dataset_id:
            continue
        record = dict(item)
        record["datasetId"] = dataset_id
        record["provider"] = _clean_text(item.get("provider"))
        record["title"] = _clean_text(item.get("title")) or dataset_id
        record["description"] = _clean_text(item.get("description"))
        record["searchText"] = _clean_text(item.get("searchText"))
        record["sourceUrl"] = _clean_text(item.get("sourceUrl"))
        record["requiresMetadataBeforeRetrieval"] = bool(item.get("requiresMetadataBeforeRetrieval"))
        normalized.append(record)
    return normalized


@lru_cache(maxsize=1)
def _catalog_entries_by_id() -> Dict[str, Dict[str, Any]]:
    return {entry["datasetId"]: entry for entry in _catalog_entries()}


def get_unified_catalog_entry(dataset_id: str) -> Optional[Dict[str, Any]]:
    return _catalog_entries_by_id().get(_clean_text(dataset_id))


def get_unified_source_record(dataset_id: str) -> Optional[Dict[str, Any]]:
    return get_unified_catalog_entry(dataset_id)


def _row_to_entry(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "provider": str(row["provider"] or "").strip(),
        "datasetId": str(row["dataset_id"] or "").strip(),
        "title": str(row["title"] or "").strip(),
        "description": str(row["description"] or "").strip(),
        "searchText": str(row["search_text"] or "").strip(),
        "sourceUrl": str(row["source_url"] or "").strip(),
        "requiresMetadataBeforeRetrieval": bool(int(row["requires_metadata_before_retrieval"] or 0)),
    }


def _execute_match_search(
    connection: sqlite3.Connection,
    match_query: str,
    limit: int,
    exclude_ids: set[str] | None = None,
) -> List[sqlite3.Row]:
    if not match_query:
        return []
    rows = connection.execute(
        """
        SELECT
            c.provider,
            c.dataset_id,
            c.title,
            c.description,
            c.search_text,
            c.source_url,
            c.requires_metadata_before_retrieval
        FROM catalog_fts f
        JOIN catalog c ON c.rowid = f.rowid
        WHERE catalog_fts MATCH ?
        ORDER BY bm25(catalog_fts, 3.0, 4.0, 5.0, 2.5, 1.8), c.provider, c.dataset_id
        LIMIT ?
        """,
        (match_query, max(1, limit)),
    ).fetchall()
    if not exclude_ids:
        return rows
    return [row for row in rows if str(row["dataset_id"] or "").strip() not in exclude_ids]


def search_unified_catalog(query: str, limit: int = 40, *, force_refresh: bool = False) -> Dict[str, Any]:
    ensure_unified_catalog_artifacts(force_refresh)
    clean_query = _clean_text(query)
    clean_limit = max(1, min(int(limit or 40), 40))
    connection = sqlite3.connect(FTS_DB_PATH)
    connection.row_factory = sqlite3.Row
    try:
        if not clean_query:
            rows = connection.execute(
                """
                SELECT provider, dataset_id, title, description, search_text, source_url, requires_metadata_before_retrieval
                FROM catalog
                ORDER BY provider, title, dataset_id
                LIMIT ?
                """,
                (clean_limit,),
            ).fetchall()
        else:
            strict_query = _strict_match_query(clean_query)
            rows = _execute_match_search(connection, strict_query, clean_limit)
            if len(rows) < clean_limit:
                relaxed_query = _relaxed_match_query(clean_query)
                if relaxed_query and relaxed_query != strict_query:
                    existing_ids = {str(row["dataset_id"] or "").strip() for row in rows}
                    rows.extend(
                        _execute_match_search(
                            connection,
                            relaxed_query,
                            clean_limit,
                            exclude_ids=existing_ids,
                        )[: max(0, clean_limit - len(rows))]
                    )
        entries = [_row_to_entry(row) for row in rows]
        return {
            "query": clean_query,
            "total": len(entries),
            "candidates": entries,
        }
    finally:
        connection.close()
