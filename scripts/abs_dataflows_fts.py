#!/usr/bin/env python3

import argparse
import json
import re
import sqlite3
from pathlib import Path


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ABS dataflow FTS search helper")
    parser.add_argument("--json-cache", required=True, help="Path to ABS_DATAFLOWS_FULL.json")
    parser.add_argument("--db", required=True, help="Path to local SQLite FTS database")
    parser.add_argument("--query", required=True, help="Search query")
    parser.add_argument("--limit", type=int, default=8, help="Maximum number of results")
    return parser.parse_args()


def load_flows(json_cache_path: Path) -> list[dict]:
    payload = json.loads(json_cache_path.read_text(encoding="utf-8"))
    flows = payload.get("flows")
    if isinstance(flows, list):
        return flows
    legacy = payload.get("dataflows")
    if isinstance(legacy, list):
        return legacy
    raise ValueError(f"Unsupported dataflow cache format in {json_cache_path}")


def normalize_tokens(query: str) -> list[str]:
    raw_tokens = re.findall(r"[A-Za-z0-9]+", query.lower())
    return [token for token in raw_tokens if len(token) > 1 and token not in STOPWORDS]


def build_match_query(tokens: list[str], operator: str) -> str:
    if not tokens:
        return ""
    joiner = f" {operator} "
    return joiner.join(f'"{token}"*' for token in tokens)


def strict_match_query(query: str) -> str:
    tokens = normalize_tokens(query)
    return build_match_query(tokens, "AND")


def relaxed_match_query(query: str) -> str:
    tokens = normalize_tokens(query)
    return build_match_query(tokens, "OR")


def execute_match_search(
    connection: sqlite3.Connection,
    match_query: str,
    limit: int,
    exclude_ids: set[str] | None = None,
) -> list[sqlite3.Row]:
    if not match_query:
        return []

    rows = connection.execute(
        """
        SELECT
            d.dataset_id,
            d.agency_id,
            d.version,
            d.name,
            d.description
        FROM dataflows_fts f
        JOIN dataflows d ON d.rowid = f.rowid
        WHERE dataflows_fts MATCH ?
        ORDER BY bm25(dataflows_fts, 5.0, 3.0, 1.5), d.dataset_id
        LIMIT ?
        """,
        (match_query, max(1, limit)),
    ).fetchall()
    if not exclude_ids:
        return rows
    return [row for row in rows if row[0] not in exclude_ids]


def ensure_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS dataflows (
            dataset_id TEXT PRIMARY KEY,
            agency_id TEXT,
            version TEXT,
            name TEXT,
            description TEXT
        );

        CREATE VIRTUAL TABLE IF NOT EXISTS dataflows_fts USING fts5(
            dataset_id,
            name,
            description,
            content='dataflows',
            content_rowid='rowid'
        );
        """
    )


def index_is_stale(connection: sqlite3.Connection, json_cache_path: Path) -> bool:
    row = connection.execute(
        "SELECT value FROM meta WHERE key = 'json_cache_signature'"
    ).fetchone()
    signature = f"{json_cache_path.stat().st_mtime_ns}:{json_cache_path.stat().st_size}"
    return row is None or row[0] != signature


def rebuild_index(connection: sqlite3.Connection, json_cache_path: Path) -> None:
    flows = load_flows(json_cache_path)
    signature = f"{json_cache_path.stat().st_mtime_ns}:{json_cache_path.stat().st_size}"

    with connection:
        connection.execute("DELETE FROM dataflows_fts")
        connection.execute("DELETE FROM dataflows")
        connection.executemany(
            """
            INSERT INTO dataflows (
                dataset_id,
                agency_id,
                version,
                name,
                description
            ) VALUES (?, ?, ?, ?, ?)
            """,
            [
                (
                    flow.get("id", ""),
                    flow.get("agencyID", ""),
                    flow.get("version", ""),
                    flow.get("name", ""),
                    flow.get("description", ""),
                )
                for flow in flows
            ],
        )
        connection.execute(
            """
            INSERT INTO dataflows_fts(rowid, dataset_id, name, description)
            SELECT rowid, dataset_id, name, description
            FROM dataflows
            """
        )
        connection.execute(
            """
            INSERT INTO meta(key, value)
            VALUES ('json_cache_signature', ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (signature,),
        )


def search(connection: sqlite3.Connection, query: str, limit: int) -> dict:
    strict_query = strict_match_query(query)
    if not strict_query:
        rows = connection.execute(
            """
            SELECT dataset_id, agency_id, version, name, description
            FROM dataflows
            ORDER BY dataset_id
            LIMIT ?
            """,
            (max(1, limit),),
        ).fetchall()
    else:
        rows = execute_match_search(connection, strict_query, limit)
        if len(rows) < max(1, limit):
            relaxed_query = relaxed_match_query(query)
            if relaxed_query and relaxed_query != strict_query:
                existing_ids = {row[0] for row in rows}
                relaxed_rows = execute_match_search(
                    connection,
                    relaxed_query,
                    limit,
                    exclude_ids=existing_ids,
                )
                rows.extend(relaxed_rows[: max(0, limit - len(rows))])

    return {
        "total": len(rows),
        "searchQuery": query,
        "dataflows": [
            {
                "id": row[0],
                "agencyID": row[1],
                "version": row[2],
                "name": row[3] or "",
                "description": row[4] or "",
            }
            for row in rows
        ],
    }


def main() -> None:
    args = parse_args()
    json_cache_path = Path(args.json_cache).resolve()
    db_path = Path(args.db).resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(db_path)
    try:
        ensure_schema(connection)
        if index_is_stale(connection, json_cache_path):
            rebuild_index(connection, json_cache_path)
        result = search(connection, args.query, args.limit)
    finally:
        connection.close()

    print(json.dumps(result))


if __name__ == "__main__":
    main()
