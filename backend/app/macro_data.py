from __future__ import annotations

import csv
import io
import json
import logging
import re
import sqlite3
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

from .config import get_settings


settings = get_settings()
MACRO_CATALOG_PATH = Path(__file__).resolve().parents[2] / "MACRO_CATALOG_FULL.json"
COMTRADE_METADATA_PATH = Path(__file__).resolve().parents[2] / "COMTRADE_METADATA.json"
logger = logging.getLogger("abs.backend.macro")


COUNTRY_ALIASES: Dict[str, str] = {
    "australia": "AUS",
    "australian": "AUS",
    "aus": "AUS",
    "japan": "JPN",
    "jpn": "JPN",
    "united states": "USA",
    "us": "USA",
    "usa": "USA",
    "america": "USA",
    "united kingdom": "GBR",
    "uk": "GBR",
    "britain": "GBR",
    "england": "GBR",
    "germany": "DEU",
    "deu": "DEU",
    "france": "FRA",
    "fra": "FRA",
    "canada": "CAN",
    "can": "CAN",
    "china": "CHN",
    "chn": "CHN",
    "india": "IND",
    "ind": "IND",
    "italy": "ITA",
    "ita": "ITA",
    "spain": "ESP",
    "esp": "ESP",
    "korea": "KOR",
    "south korea": "KOR",
    "kor": "KOR",
    "new zealand": "NZL",
    "nzl": "NZL",
    "brazil": "BRA",
    "bra": "BRA",
    "mexico": "MEX",
    "mex": "MEX",
    "indonesia": "IDN",
    "idn": "IDN",
    "singapore": "SGP",
    "sgp": "SGP",
    "euro area": "EA19",
    "eurozone": "EA19",
}

COUNTRY_GROUPS: Dict[str, List[str]] = {
    "g7": ["USA", "JPN", "DEU", "GBR", "FRA", "ITA", "CAN"],
    "g7 countries": ["USA", "JPN", "DEU", "GBR", "FRA", "ITA", "CAN"],
    "all oecd countries": [
        "AUS", "AUT", "BEL", "CAN", "CHL", "COL", "CRI", "CZE", "DNK", "EST",
        "FIN", "FRA", "DEU", "GRC", "HUN", "ISL", "IRL", "ISR", "ITA", "JPN",
        "KOR", "LVA", "LTU", "LUX", "MEX", "NLD", "NZL", "NOR", "POL", "PRT",
        "SVK", "SVN", "ESP", "SWE", "CHE", "TUR", "GBR", "USA",
    ],
    "oecd country": [
        "AUS", "AUT", "BEL", "CAN", "CHL", "COL", "CRI", "CZE", "DNK", "EST",
        "FIN", "FRA", "DEU", "GRC", "HUN", "ISL", "IRL", "ISR", "ITA", "JPN",
        "KOR", "LVA", "LTU", "LUX", "MEX", "NLD", "NZL", "NOR", "POL", "PRT",
        "SVK", "SVN", "ESP", "SWE", "CHE", "TUR", "GBR", "USA",
    ],
    "oecd countries": [
        "AUS", "AUT", "BEL", "CAN", "CHL", "COL", "CRI", "CZE", "DNK", "EST",
        "FIN", "FRA", "DEU", "GRC", "HUN", "ISL", "IRL", "ISR", "ITA", "JPN",
        "KOR", "LVA", "LTU", "LUX", "MEX", "NLD", "NZL", "NOR", "POL", "PRT",
        "SVK", "SVN", "ESP", "SWE", "CHE", "TUR", "GBR", "USA",
    ],
}

WORLD_BANK_PROVIDER = "World Bank"
IMF_PROVIDER = "IMF"
OECD_PROVIDER = "OECD"
COMTRADE_PROVIDER = "UN Comtrade"


def _truncate_log(text: Any, length: int = 400) -> str:
    value = str(text or "").replace("\n", " ").strip()
    return value if len(value) <= length else value[: length - 1] + "…"


def _request_url(url: str, params: Optional[Dict[str, Any]] = None) -> str:
    if not params:
        return url
    filtered = {key: value for key, value in params.items() if value is not None}
    return f"{url}?{httpx.QueryParams(filtered)}"

MACRO_CONCEPTS: List[Dict[str, Any]] = [
    {
        "concept_id": "gdp",
        "label": "GDP",
        "synonyms": [
            "gdp",
            "gross domestic product",
            "economic output",
            "economy size",
            "nominal gdp",
        ],
        "default_provider": "worldbank",
        "providers": {
            "worldbank": {
                "series_id": "NY.GDP.MKTP.CD",
                "label": "GDP (current US$)",
                "source_url_template": "https://data.worldbank.org/indicator/NY.GDP.MKTP.CD",
            },
            "oecd": {
                "dataset_label": "NAAG Chapter 1: GDP",
                "agency": "OECD.SDD.NAD",
                "dataflow": "DSD_NAAG@DF_NAAG_I",
                "version": "1.0",
                "row_filters": {
                    "MEASURE": "B1GQ_R",
                    "FREQ": "A",
                    "ADJUSTMENT": "N",
                    "TRANSFORMATION": "N",
                },
                "label": "Real gross domestic product",
                "source_url_template": "https://sdmx.oecd.org/public/rest/data/OECD.SDD.NAD,DSD_NAAG@DF_NAAG_I,1.0",
            },
        },
    },
    {
        "concept_id": "gdp_growth",
        "label": "GDP growth",
        "synonyms": [
            "gdp growth",
            "real gdp growth",
            "economic growth",
            "growth rate",
        ],
        "default_provider": "worldbank",
        "providers": {
            "worldbank": {
                "series_id": "NY.GDP.MKTP.KD.ZG",
                "label": "GDP growth (annual %)",
                "source_url_template": "https://data.worldbank.org/indicator/NY.GDP.MKTP.KD.ZG",
            },
            "imf": {
                "series_id": "NGDP_RPCH",
                "label": "Real GDP growth",
                "source_url_template": "https://www.imf.org/external/datamapper/NGDP_RPCH@WEO",
            },
        },
    },
    {
        "concept_id": "gdp_per_capita",
        "label": "GDP per capita",
        "synonyms": ["gdp per capita", "income per person", "output per person"],
        "default_provider": "worldbank",
        "providers": {
            "worldbank": {
                "series_id": "NY.GDP.PCAP.CD",
                "label": "GDP per capita (current US$)",
                "source_url_template": "https://data.worldbank.org/indicator/NY.GDP.PCAP.CD",
            }
        },
    },
    {
        "concept_id": "inflation",
        "label": "Inflation",
        "synonyms": ["inflation", "cpi", "consumer prices", "price growth"],
        "default_provider": "worldbank",
        "providers": {
            "worldbank": {
                "series_id": "FP.CPI.TOTL.ZG",
                "label": "Inflation, consumer prices (annual %)",
                "source_url_template": "https://data.worldbank.org/indicator/FP.CPI.TOTL.ZG",
            },
            "imf": {
                "series_id": "PCPIPCH",
                "label": "Inflation, average consumer prices",
                "source_url_template": "https://www.imf.org/external/datamapper/PCPIPCH@WEO",
            },
            "oecd": {
                "dataset_label": "Consumer price indices (CPIs, HICPs), COICOP 1999",
                "agency": "OECD.SDD.TPS",
                "dataflow": "DSD_PRICES@DF_PRICES_ALL",
                "version": "1.0",
                "row_filters": {
                    "MEASURE": "CPI",
                    "UNIT_MEASURE": "PC",
                    "EXPENDITURE": "CP00",
                    "FREQ": "M",
                },
                "preferred_transformations": ["GY", "G12", "G1"],
                "label": "Consumer price inflation",
                "source_url_template": "https://sdmx.oecd.org/public/rest/data/OECD.SDD.TPS,DSD_PRICES@DF_PRICES_ALL,1.0",
            },
        },
    },
    {
        "concept_id": "unemployment",
        "label": "Unemployment",
        "synonyms": ["unemployment", "jobless rate", "unemployment rate"],
        "default_provider": "worldbank",
        "providers": {
            "worldbank": {
                "series_id": "SL.UEM.TOTL.ZS",
                "label": "Unemployment, total (% of total labor force)",
                "source_url_template": "https://data.worldbank.org/indicator/SL.UEM.TOTL.ZS",
            },
            "imf": {
                "series_id": "LUR",
                "label": "Unemployment rate",
                "source_url_template": "https://www.imf.org/external/datamapper/LUR@WEO",
            },
            "oecd": {
                "dataset_label": "Monthly unemployment rates",
                "agency": "OECD.SDD.TPS",
                "dataflow": "DSD_LFS@DF_IALFS_UNE_M",
                "version": "1.0",
                "row_filters": {
                    "MEASURE": "UNE_LF_M",
                    "FREQ": "M",
                },
                "preferred_totals": {
                    "SEX": ["T", "TOT"],
                    "AGE": ["Y15T74", "Y15T64", "Y15T99", "Y15T24"],
                    "ADJUSTMENT": ["S", "Y", "N"],
                },
                "label": "Monthly unemployment rate",
                "source_url_template": "https://sdmx.oecd.org/public/rest/data/OECD.SDD.TPS,DSD_LFS@DF_IALFS_UNE_M,1.0",
            },
        },
    },
    {
        "concept_id": "population",
        "label": "Population",
        "synonyms": ["population", "people", "population size"],
        "default_provider": "worldbank",
        "providers": {
            "worldbank": {
                "series_id": "SP.POP.TOTL",
                "label": "Population, total",
                "source_url_template": "https://data.worldbank.org/indicator/SP.POP.TOTL",
            }
        },
    },
    {
        "concept_id": "government_debt",
        "label": "Government debt",
        "synonyms": ["government debt", "public debt", "debt to gdp", "sovereign debt", "national debt"],
        "default_provider": "imf",
        "providers": {
            "imf": {
                "series_id": "GGXWDG_NGDP",
                "label": "General government gross debt (% of GDP)",
                "source_url_template": "https://www.imf.org/external/datamapper/GGXWDG_NGDP@WEO",
            }
        },
    },
    {
        "concept_id": "fiscal_balance",
        "label": "Fiscal balance",
        "synonyms": ["fiscal balance", "budget deficit", "fiscal deficit", "government balance"],
        "default_provider": "imf",
        "providers": {
            "imf": {
                "series_id": "GGXCNL_NGDP",
                "label": "General government net lending/borrowing (% of GDP)",
                "source_url_template": "https://www.imf.org/external/datamapper/GGXCNL_NGDP@WEO",
            }
        },
    },
    {
        "concept_id": "current_account",
        "label": "Current account balance",
        "synonyms": ["current account", "balance of payments", "bop", "external balance"],
        "default_provider": "imf",
        "providers": {
            "imf": {
                "series_id": "BCA_NGDPD",
                "label": "Current account balance (% of GDP)",
                "source_url_template": "https://www.imf.org/external/datamapper/BCA_NGDPD@WEO",
            }
        },
    },
    {
        "concept_id": "productivity",
        "label": "Productivity",
        "synonyms": [
            "productivity",
            "labour productivity",
            "labor productivity",
            "productivity growth",
            "gdp per hour worked",
        ],
        "default_provider": "oecd",
        "providers": {
            "oecd": {
                "dataset_label": "Productivity growth rates",
                "agency": "OECD.SDD.TPS",
                "dataflow": "DSD_PDB@DF_PDB_GR",
                "version": "1.0",
                "row_filters": {
                    "MEASURE": "GDPHRS",
                    "FREQ": "A",
                },
                "preferred_transformations": ["GY", "_Z"],
                "label": "GDP per hour worked",
                "source_url_template": "https://sdmx.oecd.org/public/rest/data/OECD.SDD.TPS,DSD_PDB@DF_PDB_GR,1.0",
            }
        },
    },
]

PROVIDER_KEYWORDS: Dict[str, List[str]] = {
    "worldbank": ["world bank", "worldbank", "wb data"],
    "imf": ["imf", "international monetary fund"],
    "oecd": ["oecd", "organisation for economic co-operation and development"],
    "comtrade": ["comtrade", "un comtrade", "uncomtrade", "united nations comtrade"],
}

DISCRIMINATOR_TERMS: Dict[str, List[str]] = {
    "gdp": ["gdp", "gross domestic product", "economic output"],
    "growth": ["growth", "growing", "change"],
    "inflation": ["inflation", "cpi", "consumer prices"],
    "unemployment": ["unemployment", "jobless"],
    "employment": ["employment", "employed", "labour", "labor", "jobs"],
    "manufacturing": ["manufacturing", "industry", "economic activity"],
    "share": ["share", "percent", "percentage", "%", "ratio", "part of total"],
    "population": ["population", "people"],
    "debt": ["debt", "borrowing"],
    "productivity": ["productivity", "hour worked", "gdp per hour"],
    "trade": ["trade", "imports", "import", "exports", "export", "hs", "commodity"],
}

SPECIFICITY_TERMS: Dict[str, Dict[str, Any]] = {
    "debt": {"terms": ["debt", "borrowing"], "penalty": 25.0, "boost": 6.0},
    "growth": {"terms": ["growth", "annual growth", "change"], "penalty": 35.0, "boost": 6.0},
    "per_capita": {"terms": ["per capita", "per person", "per employed", "per worker"], "penalty": 35.0, "boost": 6.0},
    "female": {"terms": ["female", "women", "girls"], "penalty": 35.0, "boost": 6.0},
    "male": {"terms": ["male", "men", "boys"], "penalty": 35.0, "boost": 6.0},
    "public_sector": {"terms": ["public sector"], "penalty": 35.0, "boost": 6.0},
    "education": {"terms": ["education"], "penalty": 25.0, "boost": 5.0},
    "nonagricultural": {"terms": ["nonagricultural"], "penalty": 25.0, "boost": 5.0},
    "manufacturing": {"terms": ["manufacturing"], "penalty": 20.0, "boost": 8.0},
    "unemployment": {"terms": ["unemployment", "jobless"], "penalty": 35.0, "boost": 6.0},
}

CATALOG_STOPWORDS = {
    "the", "a", "an", "of", "for", "in", "to", "and", "or", "show", "get", "find",
    "latest", "available", "historical", "comparison", "including", "countries",
    "country", "annual", "quarterly", "monthly", "data", "series",
    "vs", "versus", "compare", "compared", "between", "across", "over", "year", "years",
}


@dataclass
class MacroCatalogEntry:
    entry_id: str
    provider_key: str
    provider_name: str
    concept_id: str
    concept_label: str
    indicator_label: str
    unit: str
    description: str
    search_text: str
    provider_config: Dict[str, Any]


_CATALOG_ENTRIES: List[MacroCatalogEntry] = []
_CATALOG_ENTRY_BY_ID: Dict[str, MacroCatalogEntry] = {}
_CATALOG_CONN: Optional[sqlite3.Connection] = None
_CATALOG_FILE_MTIME: Optional[float] = None


def _extra_macro_catalog_entries() -> List[MacroCatalogEntry]:
    return [
        MacroCatalogEntry(
            entry_id="comtrade::goods_trade",
            provider_key="comtrade",
            provider_name=COMTRADE_PROVIDER,
            concept_id="goods_trade",
            concept_label="Goods trade",
            indicator_label="UN Comtrade goods trade (imports and exports by partner and HS code)",
            unit="US Dollars",
            description=(
                "UN Comtrade goods trade retrieval for imports and exports, bilateral trade, world totals, "
                "and HS product codes down to 4-digit headings. Metadata exposes reporter countries, "
                "partner areas, annual or monthly frequency, and HS code descriptions."
            ),
            search_text=(
                "goods trade imports exports import export bilateral trade partner hs code hs4 hs 4 digit heading "
                "commodity merchandise trade un comtrade united nations comtrade comtrade"
            ),
            provider_config={
                "series_id": "UN_COMTRADE_GOODS_TRADE",
                "label": "UN Comtrade goods trade",
                "requires_metadata_before_retrieval": True,
                "metadata_source": "COMTRADE_METADATA.json",
                "source_url_template": "https://comtradeplus.un.org/TradeFlow",
            },
        )
    ]


def _build_macro_catalog_entries() -> List[MacroCatalogEntry]:
    entries: List[MacroCatalogEntry] = []
    for concept in MACRO_CONCEPTS:
        providers = concept.get("providers") if isinstance(concept.get("providers"), dict) else {}
        for provider_key, provider_config in providers.items():
            if not isinstance(provider_config, dict):
                continue
            indicator_label = str(provider_config.get("label") or concept.get("label") or "").strip()
            dataset_label = str(provider_config.get("dataset_label") or "").strip()
            description_parts = [
                str(concept.get("label") or "").strip(),
                indicator_label,
                dataset_label,
                str(provider_key).strip(),
            ]
            search_terms = [
                str(concept.get("label") or "").strip(),
                *(str(item or "").strip() for item in (concept.get("synonyms") or [])),
                indicator_label,
                dataset_label,
                *(str(item or "").strip() for item in (provider_config.get("search_terms") or [])),
                provider_key,
                PROVIDER_KEYWORDS.get(provider_key, [""])[0],
            ]
            description = ". ".join(part for part in description_parts if part)
            search_text = " ".join(part for part in search_terms if part)
            entry = MacroCatalogEntry(
                entry_id=f"{concept.get('concept_id')}::{provider_key}",
                provider_key=str(provider_key).strip(),
                provider_name={
                    "worldbank": WORLD_BANK_PROVIDER,
                    "imf": IMF_PROVIDER,
                    "oecd": OECD_PROVIDER,
                }.get(str(provider_key).strip(), str(provider_key).strip().upper()),
                concept_id=str(concept.get("concept_id") or "").strip(),
                concept_label=str(concept.get("label") or "").strip(),
                indicator_label=indicator_label,
                unit="",
                description=description,
                search_text=search_text,
                provider_config=dict(provider_config),
            )
            entries.append(entry)
    return entries


def _load_macro_catalog_entries_from_file() -> List[MacroCatalogEntry]:
    if not MACRO_CATALOG_PATH.exists():
        raise RuntimeError(f"Macro catalog file not found: {MACRO_CATALOG_PATH}")

    try:
        raw_entries = json.loads(MACRO_CATALOG_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(f"Failed to read macro catalog file {MACRO_CATALOG_PATH}: {exc}") from exc

    if not isinstance(raw_entries, list):
        raise RuntimeError(f"Macro catalog file {MACRO_CATALOG_PATH} must contain a top-level array.")

    entries: List[MacroCatalogEntry] = []
    for item in raw_entries:
        if not isinstance(item, dict):
            continue
        entry = MacroCatalogEntry(
            entry_id=str(item.get("entry_id") or "").strip(),
            provider_key=str(item.get("provider_key") or "").strip(),
            provider_name=str(item.get("provider_name") or "").strip(),
            concept_id=str(item.get("concept_id") or "").strip(),
            concept_label=str(item.get("concept_label") or "").strip(),
            indicator_label=str(item.get("indicator_label") or "").strip(),
            unit=str(item.get("unit") or "").strip(),
            description=str(item.get("description") or "").strip(),
            search_text=str(item.get("search_text") or "").strip(),
            provider_config=dict(item.get("provider_config") or {}),
        )
        if not entry.entry_id or not entry.provider_key or not entry.indicator_label:
            continue
        entries.append(entry)
    if not entries:
        raise RuntimeError(f"Macro catalog file {MACRO_CATALOG_PATH} contained no valid entries.")
    existing_ids = {entry.entry_id for entry in entries}
    for entry in _extra_macro_catalog_entries():
        if entry.entry_id not in existing_ids:
            entries.append(entry)
    return entries


def _get_macro_catalog_connection() -> sqlite3.Connection:
    global _CATALOG_CONN, _CATALOG_ENTRIES, _CATALOG_ENTRY_BY_ID, _CATALOG_FILE_MTIME

    current_mtime = MACRO_CATALOG_PATH.stat().st_mtime if MACRO_CATALOG_PATH.exists() else None
    if _CATALOG_CONN is not None and _CATALOG_FILE_MTIME == current_mtime:
        return _CATALOG_CONN

    if _CATALOG_CONN is not None:
        _CATALOG_CONN.close()
        _CATALOG_CONN = None

    _CATALOG_ENTRIES = _load_macro_catalog_entries_from_file()
    _CATALOG_ENTRY_BY_ID = {entry.entry_id: entry for entry in _CATALOG_ENTRIES}

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE macro_indicators (
            entry_id TEXT PRIMARY KEY,
            provider_key TEXT NOT NULL,
            provider_name TEXT NOT NULL,
            concept_id TEXT NOT NULL,
            concept_label TEXT NOT NULL,
            indicator_label TEXT NOT NULL,
            description TEXT NOT NULL,
            unit TEXT NOT NULL,
            search_text TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE VIRTUAL TABLE macro_indicators_fts USING fts5(
            entry_id UNINDEXED,
            provider_name,
            concept_label,
            indicator_label,
            description,
            search_text
        )
        """
    )
    for entry in _CATALOG_ENTRIES:
        conn.execute(
            """
            INSERT INTO macro_indicators (
                entry_id, provider_key, provider_name, concept_id, concept_label, indicator_label, description, unit, search_text
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                entry.entry_id,
                entry.provider_key,
                entry.provider_name,
                entry.concept_id,
                entry.concept_label,
                entry.indicator_label,
                entry.description,
                entry.unit,
                entry.search_text,
            ),
        )
        conn.execute(
            """
            INSERT INTO macro_indicators_fts (
                entry_id, provider_name, concept_label, indicator_label, description, search_text
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                entry.entry_id,
                entry.provider_name,
                entry.concept_label,
                entry.indicator_label,
                entry.description,
                entry.search_text,
            ),
        )
    _CATALOG_CONN = conn
    _CATALOG_FILE_MTIME = current_mtime
    return conn


def _normalize_catalog_query(query: str) -> str:
    normalized = _normalize_text(query)
    for alias in sorted({*COUNTRY_ALIASES.keys(), *COUNTRY_GROUPS.keys()}, key=len, reverse=True):
        clean_alias = _normalize_text(alias)
        if not clean_alias:
            continue
        normalized = re.sub(rf"(?<![a-z0-9]){re.escape(clean_alias)}(?![a-z0-9])", " ", normalized)
    normalized = re.sub(r"\b(?:19|20)\d{2}\b", " ", normalized)
    normalized = re.sub(r"\b\d{1,2}\b", " ", normalized)
    tokens = [token for token in normalized.split() if token and token not in CATALOG_STOPWORDS]
    expanded: List[str] = []
    for token in tokens:
        expanded.append(token)
        if token == "gdp":
            expanded.extend(["gross", "domestic", "product"])
        elif token in {"labour", "labor"}:
            expanded.append("employment")
        elif token == "jobs":
            expanded.append("employment")
        elif token in {"percent", "percentage"}:
            expanded.extend(["share", "ratio"])
        elif token == "rate":
            expanded.append("rates")
    deduped: List[str] = []
    for token in expanded:
        if token not in deduped:
            deduped.append(token)
    return " ".join(deduped)


def _search_macro_catalog(query: str, explicit_provider: Optional[str], limit: int = 8) -> List[MacroCatalogEntry]:
    normalized = _normalize_catalog_query(query)
    if not normalized:
        return []
    conn = _get_macro_catalog_connection()
    terms = [token for token in normalized.split() if token]
    if len(terms) > 1:
        fts_query = f"\"{normalized}\" OR " + " OR ".join(terms[:12])
    else:
        fts_query = " OR ".join(terms[:12]) if terms else normalized
    rows = conn.execute(
        """
        SELECT entry_id, bm25(macro_indicators_fts) AS rank_score
        FROM macro_indicators_fts
        WHERE macro_indicators_fts MATCH ?
        ORDER BY rank_score ASC
        LIMIT ?
        """,
        (fts_query, max(limit * 25, 200)),
    ).fetchall()
    candidates: List[MacroCatalogEntry] = []
    for row in rows:
        entry = _CATALOG_ENTRY_BY_ID.get(str(row["entry_id"]))
        if entry is None:
            continue
        if explicit_provider and entry.provider_key != explicit_provider:
            continue
        candidates.append(entry)

    return candidates[:limit]


def _catalog_preview_labels(limit: int = 8) -> str:
    _get_macro_catalog_connection()
    preview = []
    for entry in _CATALOG_ENTRIES:
        label = str(entry.indicator_label or entry.concept_label or "").strip()
        if label and label not in preview:
            preview.append(label)
        if len(preview) >= limit:
            break
    return ", ".join(preview)


def build_macro_shortlist(query: str, limit: int = 40) -> Dict[str, Any]:
    clean_query = str(query or "").strip()
    if not clean_query:
        raise RuntimeError("Macro shortlist query cannot be empty.")

    explicit_provider = detect_explicit_provider(clean_query)
    matches = _search_macro_catalog(clean_query, explicit_provider=explicit_provider, limit=limit)
    candidates = [
        {
            "candidate_id": entry.entry_id,
            "provider_key": entry.provider_key,
            "provider": entry.provider_name,
            "concept_id": entry.concept_id,
            "concept_label": entry.concept_label,
            "indicator_label": entry.indicator_label,
            "description": entry.description,
            "requires_metadata_before_retrieval": entry.provider_key == "comtrade",
        }
        for entry in matches
    ]
    return {
        "query": clean_query,
        "candidates": candidates,
    }


def _normalize_text(value: str) -> str:
    return re.sub(r"[^a-z0-9\s]+", " ", str(value or "").lower()).strip()


def _contains_token(text: str, token: str) -> bool:
    clean_text = f" {str(text or '').strip()} "
    clean_token = str(token or "").strip()
    if not clean_token:
        return False
    return f" {clean_token} " in clean_text


@lru_cache(maxsize=1)
def _load_comtrade_metadata() -> Dict[str, Any]:
    if not COMTRADE_METADATA_PATH.exists():
        raise RuntimeError(f"Comtrade metadata file not found: {COMTRADE_METADATA_PATH}")
    try:
        payload = json.loads(COMTRADE_METADATA_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(f"Failed to read Comtrade metadata file {COMTRADE_METADATA_PATH}: {exc}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"Comtrade metadata file {COMTRADE_METADATA_PATH} must contain a top-level object.")
    return payload


def _comtrade_dimension(name: str) -> List[Dict[str, Any]]:
    payload = _load_comtrade_metadata()
    values = payload.get(name)
    return [item for item in values if isinstance(item, dict)]


def _score_comtrade_option(query: str, option: Dict[str, Any]) -> int:
    normalized_query = f" {_normalize_text(query)} "
    score = 0
    code = str(option.get("code") or "").strip().lower()
    label = _normalize_text(str(option.get("label") or ""))

    if code and _contains_token(normalized_query, code):
        score += 40
    if label and label in normalized_query:
        score += 30

    for text in [label]:
        if not text:
            continue
        tokens = [token for token in text.split() if len(token) >= 3 and token not in CATALOG_STOPWORDS]
        token_hits = sum(1 for token in tokens if _contains_token(normalized_query, token))
        score += token_hits * 3

    return score


def _comtrade_matches(query: str, options: List[Dict[str, Any]], *, limit: int = 12) -> List[Dict[str, Any]]:
    ranked: List[tuple[int, Dict[str, Any]]] = []
    for option in options:
        score = _score_comtrade_option(query, option)
        if score <= 0:
            continue
        ranked.append((score, option))
    ranked.sort(key=lambda item: (-item[0], str(item[1].get("label") or ""), str(item[1].get("code") or "")))
    return [item for _, item in ranked[:limit]]


def _inject_comtrade_option(
    options: List[Dict[str, Any]],
    injected: Dict[str, Any],
) -> List[Dict[str, Any]]:
    injected_code = str(injected.get("code") or "").strip()
    if not injected_code:
        return list(options)
    deduped = [item for item in options if str(item.get("code") or "").strip() != injected_code]
    return [dict(injected), *deduped]


def _comtrade_default_flow(query: str) -> str:
    normalized_query = f" {_normalize_text(query)} "
    if " import " in normalized_query or " imports " in normalized_query:
        return "M"
    if " export " in normalized_query or " exports " in normalized_query:
        return "X"
    raise RuntimeError("Comtrade retrieval requires a trade flow. Choose either Import (M) or Export (X).")


def _comtrade_default_frequency(query: str) -> str:
    normalized_query = f" {_normalize_text(query)} "
    if " monthly " in normalized_query or " month " in normalized_query:
        return "M"
    return "A"


def _comtrade_default_period_range(
    *,
    frequency_code: str,
    start_year: Optional[int],
    end_year: Optional[int],
) -> tuple[int, int]:
    now = datetime.now(timezone.utc)
    default_end = now.year - 2 if frequency_code == "A" else now.year - 1
    resolved_end = end_year if isinstance(end_year, int) else default_end
    resolved_start = start_year if isinstance(start_year, int) else (resolved_end - 9 if frequency_code == "A" else resolved_end - 1)
    if resolved_start > resolved_end:
        resolved_start = resolved_end
    return resolved_start, resolved_end


def _comtrade_period_values(start_year: int, end_year: int, frequency_code: str) -> List[str]:
    if frequency_code == "M":
        values: List[str] = []
        for year in range(start_year, end_year + 1):
            for month in range(1, 13):
                values.append(f"{year}{month:02d}")
        return values
    return [str(year) for year in range(start_year, end_year + 1)]


def _chunk_period_values(period_values: List[str], max_periods: int = 12) -> List[str]:
    if not period_values:
        return []
    size = max(1, int(max_periods))
    return [
        ",".join(period_values[index : index + size])
        for index in range(0, len(period_values), size)
    ]


def _coerce_code_list(raw_value: Any) -> List[str]:
    values: List[str] = []
    if isinstance(raw_value, list):
        for item in raw_value:
            clean = str(item or "").strip()
            if clean and clean not in values:
                values.append(clean)
    elif raw_value is not None:
        clean = str(raw_value).strip()
        if clean:
            values.append(clean)
    return values


def _resolve_comtrade_area_codes(raw_values: List[str], options: List[Dict[str, Any]]) -> List[str]:
    by_key: Dict[str, str] = {}
    for option in options:
        code = str(option.get("code") or "").strip()
        if not code:
            continue
        by_key[code] = code
        label = _normalize_text(str(option.get("label") or ""))
        if label:
            by_key[label] = code

    resolved: List[str] = []
    for raw in raw_values:
        clean = str(raw or "").strip()
        if not clean:
            continue
        mapped = by_key.get(clean)
        if mapped is None:
            mapped = by_key.get(clean.upper())
        if mapped is None:
            mapped = by_key.get(_normalize_text(clean))
        if mapped and mapped not in resolved:
            resolved.append(mapped)
    return resolved


def _resolve_comtrade_hs_codes(raw_values: List[str], options: List[Dict[str, Any]]) -> List[str]:
    by_key: Dict[str, str] = {}
    for option in options:
        code = str(option.get("code") or "").strip()
        label = _normalize_text(str(option.get("label") or ""))
        if code:
            by_key[code] = code
        if label:
            by_key[label] = code

    resolved: List[str] = []
    for raw in raw_values:
        clean = str(raw or "").strip()
        if not clean:
            continue
        mapped = by_key.get(clean) or by_key.get(_normalize_text(clean))
        if mapped and mapped not in resolved:
            resolved.append(mapped)
    return resolved


def detect_explicit_provider(query: str) -> Optional[str]:
    normalized = _normalize_text(query)
    for provider, phrases in PROVIDER_KEYWORDS.items():
        for phrase in phrases:
            if phrase in normalized:
                return provider
    return None


def _score_concept(query: str, concept: Dict[str, Any]) -> int:
    normalized = _normalize_text(query)
    score = 0
    label = _normalize_text(str(concept.get("label") or ""))
    if label and label in normalized:
        score += 15
    for synonym in concept.get("synonyms") or []:
        clean = _normalize_text(str(synonym))
        if not clean:
            continue
        if clean in normalized:
            score += 12 if " " in clean else 6
            continue
        tokens = [token for token in clean.split() if token]
        token_hits = sum(1 for token in tokens if _contains_token(normalized, token))
        score += token_hits * 2
    return score


def search_concepts(query: str, limit: int = 5) -> List[Dict[str, Any]]:
    ranked = []
    for concept in MACRO_CONCEPTS:
        score = _score_concept(query, concept)
        if score <= 0:
            continue
        ranked.append((score, concept))
    ranked.sort(key=lambda item: item[0], reverse=True)
    return [item for _, item in ranked[:limit]]


def detect_countries(query: str) -> List[str]:
    normalized = f" {_normalize_text(query)} "
    found: List[str] = []

    for alias, countries in COUNTRY_GROUPS.items():
        if f" {alias} " in normalized:
            for code in countries:
                if code not in found:
                    found.append(code)

    for alias in sorted(COUNTRY_ALIASES.keys(), key=len, reverse=True):
        if f" {alias} " in normalized:
            code = COUNTRY_ALIASES[alias]
            if code not in found:
                found.append(code)
    return found


def wants_all_countries(query: str) -> bool:
    normalized = f" {_normalize_text(query)} "
    phrases = [
        " by country ",
        " across countries ",
        " all countries ",
        " country ranking ",
        " countries ranked ",
    ]
    return any(phrase in normalized for phrase in phrases)


def wants_latest_only(query: str) -> bool:
    normalized = f" {_normalize_text(query)} "
    phrases = [
        " latest ",
        " latest year ",
        " latest available ",
        " most recent ",
        " newest ",
        " current year ",
    ]
    return any(phrase in normalized for phrase in phrases)


def wants_country_ranking(query: str) -> bool:
    normalized = f" {_normalize_text(query)} "
    ranking_markers = [
        " top ",
        " biggest ",
        " largest ",
        " highest ",
        " lowest ",
        " rank ",
        " ranking ",
        " performers ",
        " compare cumulative ",
    ]
    return any(marker in normalized for marker in ranking_markers)


def _build_comtrade_metadata_payload(query: str, selected_entry: MacroCatalogEntry) -> Dict[str, Any]:
    flows = _comtrade_dimension("flows")
    countries = _comtrade_dimension("countries")
    hs_2digit = _comtrade_dimension("hs_2digit")
    hs_4digit = _comtrade_dimension("hs_4digit")

    partner_options = _inject_comtrade_option(
        countries,
        {"code": "0", "label": "All partners (World total)"},
    )
    matched_hs_2digit = _inject_comtrade_option(
        _comtrade_matches(query, hs_2digit, limit=25),
        {"code": "TOTAL", "label": "TOTAL - All products"},
    )
    matched_hs_4digit = _inject_comtrade_option(
        _comtrade_matches(query, hs_4digit, limit=100),
        {"code": "TOTAL", "label": "TOTAL - All products"},
    )

    return {
        "provider": COMTRADE_PROVIDER,
        "candidate_id": selected_entry.entry_id,
        "concept_id": selected_entry.concept_id,
        "concept_label": selected_entry.concept_label,
        "indicator_label": selected_entry.indicator_label,
        "requires_metadata_before_retrieval": True,
        "defaults": {
            "partnerCode": "0",
            "frequencyCode": "A",
        },
        "guidance": [
            "Pick one reporter, one partner, one flow, and one HS code before retrieval.",
            "Use partner code 0 for world total unless the user explicitly asks for bilateral trade with a named counterpart.",
            "Choose either a 2-digit HS chapter or a 4-digit HS heading. If you choose a 2-digit code, retrieval should use that 2-digit level directly.",
            "Annual world trade is the default shape. Use monthly only if the user explicitly asks for it.",
            "Do not attempt broad UN Comtrade retrievals across many countries, many HS codes, and long time ranges at the same time; expand only one axis at a time and rely on downstream filtering for the rest.",
        ],
        "dimensions": [
            {
                "id": "FLOW",
                "label": "Trade flow",
                "required": True,
                "options": flows,
            },
            {
                "id": "REPORTER",
                "label": "Reporter country",
                "required": True,
                "optionCount": len(countries),
                "options": countries,
            },
            {
                "id": "PARTNER",
                "label": "Partner country",
                "required": True,
                "optionCount": len(partner_options),
                "options": partner_options,
            },
            {
                "id": "HS_2DIGIT",
                "label": "HS 2-digit chapter",
                "required": True,
                "optionCount": len(hs_2digit),
                "matchedOptions": matched_hs_2digit,
            },
            {
                "id": "HS_4DIGIT",
                "label": "HS 4-digit heading",
                "required": True,
                "optionCount": len(hs_4digit),
                "matchedOptions": matched_hs_4digit,
            },
        ],
        "fullDimensionCounts": {
            "flows": len(flows),
            "reporters": len(countries),
            "partners": len(partner_options),
            "hs_2digit": len(hs_2digit),
            "hs_4digit": len(hs_4digit),
        },
    }


def get_macro_candidate_metadata(candidate_id: str, query: str) -> Dict[str, Any]:
    clean_candidate_id = str(candidate_id or "").strip()
    clean_query = str(query or "").strip()
    if not clean_candidate_id:
        raise RuntimeError("macro metadata requires candidateId.")
    if not clean_query:
        raise RuntimeError("macro metadata requires query.")

    _get_macro_catalog_connection()
    selected_entry = _CATALOG_ENTRY_BY_ID.get(clean_candidate_id)
    if selected_entry is None:
        raise RuntimeError(f"Unknown macro candidateId '{clean_candidate_id}'.")
    if selected_entry.provider_key != "comtrade":
        raise RuntimeError(f"Macro metadata is only supported for Comtrade candidates in this harness. Received provider '{selected_entry.provider_key}'.")
    return _build_comtrade_metadata_payload(clean_query, selected_entry)


def infer_macro_retrieval_shape(query: str, countries: List[str]) -> Dict[str, Any]:
    normalized = f" {_normalize_text(query)} "
    explicit_country_count = len(_sort_country_codes(countries))
    multi_country = wants_all_countries(query) or explicit_country_count > 1 or (
        wants_country_ranking(query) and explicit_country_count == 0
    )
    if multi_country and wants_latest_only(query):
        shape = "all_countries_latest_year"
    elif multi_country:
        shape = "all_countries_over_time"
    elif explicit_country_count == 1:
        shape = "one_country_over_time"
    else:
        shape = "single_series"
    min_country_count = 3 if multi_country and re.search(r"\btop\s*3\b|\bthree\b", normalized) else (2 if multi_country else 1)
    return {
        "shape": shape,
        "multi_country": multi_country,
        "latest_only": wants_latest_only(query),
        "ranking": wants_country_ranking(query),
        "min_country_count": min_country_count,
        "explicit_country_count": explicit_country_count,
    }


def normalize_macro_retrieval_inputs(
    query: str,
    *,
    countries: Optional[List[str]] = None,
    all_countries: bool = False,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
) -> Dict[str, Any]:
    normalized_query = str(query or "").strip()
    normalized_countries = _sort_country_codes(countries or [])
    inferred_countries = _sort_country_codes(detect_countries(normalized_query))
    if not normalized_countries:
        normalized_countries = inferred_countries

    inferred_start_year, inferred_end_year = detect_time_range(normalized_query)
    resolved_start_year = start_year if isinstance(start_year, int) else inferred_start_year
    resolved_end_year = end_year if isinstance(end_year, int) else inferred_end_year

    resolved_all_countries = bool(all_countries)
    if not normalized_countries and not resolved_all_countries:
        resolved_all_countries = wants_all_countries(normalized_query)

    return {
        "query": normalized_query,
        "countries": normalized_countries,
        "all_countries": resolved_all_countries,
        "start_year": resolved_start_year,
        "end_year": resolved_end_year,
    }


def detect_time_range(query: str) -> tuple[Optional[int], Optional[int]]:
    text = str(query or "")
    current_year = datetime.utcnow().year

    explicit = re.search(r"\b(19|20)\d{2}\s*(?:to|-|through|until)\s*(19|20)\d{2}\b", text, re.IGNORECASE)
    if explicit:
        years = re.findall(r"(19|20)\d{2}", explicit.group(0))
        full_years = [int(y) for y in re.findall(r"(?:19|20)\d{2}", explicit.group(0))]
        if len(full_years) >= 2:
            return full_years[0], full_years[1]

    all_years = [int(value) for value in re.findall(r"\b(?:19|20)\d{2}\b", text)]
    if len(all_years) >= 2:
        return min(all_years), max(all_years)
    if len(all_years) == 1:
        year = all_years[0]
        return year, year

    last_n_years = re.search(r"\blast\s+(\d{1,2})\s+years?\b", text, re.IGNORECASE)
    if last_n_years:
        count = max(1, int(last_n_years.group(1)))
        return current_year - count, current_year

    return current_year - 10, current_year


def _choose_provider(query: str, concept: Dict[str, Any], explicit_provider: Optional[str]) -> str:
    providers = concept.get("providers") if isinstance(concept.get("providers"), dict) else {}
    if explicit_provider and explicit_provider in providers:
        return explicit_provider
    return str(concept.get("default_provider") or "").strip() or next(iter(providers.keys()))


def _sort_country_codes(countries: List[str]) -> List[str]:
    preferred = []
    for code in countries:
        clean = str(code or "").strip().upper()
        if clean and clean not in preferred:
            preferred.append(clean)
    return preferred


def _get_country_name_from_iso3(code: str) -> str:
    for alias, iso3 in COUNTRY_ALIASES.items():
        if iso3 == code and len(alias) > 3:
            return alias.title()
    return code


def _source_reference(provider: str, *, indicator: str, series_id: str, country: str = "", source_url: str = "") -> Dict[str, Any]:
    ref: Dict[str, Any] = {
        "provider": provider,
        "indicator": indicator,
        "series_id": series_id,
    }
    if country:
        ref["country"] = country
    if source_url:
        ref["source_url"] = source_url
    return ref


def _parse_numeric(value: Any) -> Optional[float]:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _parse_world_bank_error(payload: Any) -> Optional[str]:
    if not isinstance(payload, list) or not payload:
        return None
    first = payload[0]
    if not isinstance(first, dict):
        return None
    messages = first.get("message")
    if not isinstance(messages, list):
        return None
    parts: List[str] = []
    for item in messages:
        if not isinstance(item, dict):
            continue
        value = str(item.get("value") or "").strip()
        key = str(item.get("key") or "").strip()
        text = value or key
        if text:
            parts.append(text)
    return "; ".join(parts) if parts else None


def _looks_like_html_error(text: str) -> bool:
    normalized = str(text or "").lstrip().lower()
    return normalized.startswith("<!doctype html") or normalized.startswith("<html") or normalized.startswith("<?xml")


def _fetch_world_bank_with_curl(request_url: str) -> Any:
    completed = subprocess.run(
        ["curl", "-L", "-sS", request_url],
        check=True,
        capture_output=True,
        text=True,
    )
    body = completed.stdout or ""
    if _looks_like_html_error(body):
        raise RuntimeError(f"World Bank curl fallback returned HTML error page for {request_url}")
    try:
        return json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"World Bank curl fallback returned non-JSON response for {request_url}: {_truncate_log(body, 300)}"
        ) from exc


def _fetch_world_bank(query: str, entry: MacroCatalogEntry, provider_config: Dict[str, Any], countries: List[str], start_year: Optional[int], end_year: Optional[int], *, all_countries: bool = False) -> Dict[str, Any]:
    if not countries and not all_countries:
        countries = ["AUS"]
    series_id = str(provider_config.get("series_id") or "").strip()
    label = str(provider_config.get("label") or entry.concept_label or series_id).strip()
    requested_countries = list(countries)
    country_path = "all"
    url = f"{settings.worldbank_base_url.rstrip('/')}/country/{country_path}/indicator/{series_id}"
    params: Dict[str, Any] = {"format": "json", "per_page": 20000}
    if start_year and end_year:
        params["date"] = f"{start_year}:{end_year}"
    request_url = _request_url(url, params)
    logger.info(
        'Macro retrieval request provider=worldbank indicator=%s countries="%s" all_countries=%s url="%s"',
        series_id,
        ",".join(countries),
        all_countries,
        _truncate_log(request_url, 700),
    )
    try:
        response = httpx.get(url, params=params, timeout=settings.macro_timeout_seconds)
        response.raise_for_status()
        if _looks_like_html_error(response.text):
            raise RuntimeError("World Bank returned an HTML error page.")
        payload = response.json()
    except Exception as exc:
        response = exc.response if isinstance(exc, httpx.HTTPStatusError) else None
        body_preview = _truncate_log(response.text, 500) if response is not None else ""
        logger.error(
            'Macro retrieval error provider=worldbank indicator=%s url="%s" status=%s error="%s" body="%s"',
            series_id,
            _truncate_log(request_url, 700),
            getattr(response, "status_code", ""),
            _truncate_log(exc, 500),
            body_preview,
        )
        logger.info(
            'Macro retrieval retry provider=worldbank indicator=%s method=curl url="%s"',
            series_id,
            _truncate_log(request_url, 700),
        )
        try:
            payload = _fetch_world_bank_with_curl(request_url)
        except Exception as curl_exc:
            logger.error(
                'Macro retrieval error provider=worldbank indicator=%s method=curl url="%s" error="%s"',
                series_id,
                _truncate_log(request_url, 700),
                _truncate_log(curl_exc, 500),
            )
            raise exc
    error_message = _parse_world_bank_error(payload)
    if error_message:
        logger.error(
            'Macro retrieval error provider=worldbank indicator=%s url="%s" api_error="%s"',
            series_id,
            _truncate_log(request_url, 700),
            _truncate_log(error_message, 500),
        )
        raise RuntimeError(f"World Bank error for {series_id}: {error_message}")
    if not isinstance(payload, list) or len(payload) < 2 or not isinstance(payload[1], list):
        raise RuntimeError("World Bank returned an unexpected response shape.")

    rows = payload[1]
    by_country: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        value = _parse_numeric(row.get("value"))
        year = str(row.get("date") or "").strip()
        iso3 = str(row.get("countryiso3code") or "").strip().upper()
        if value is None or not year or not iso3:
            continue
        by_country.setdefault(iso3, []).append({"x": year, "y": value})

    series: List[Dict[str, Any]] = []
    source_refs: List[Dict[str, Any]] = []
    country_codes = sorted(by_country.keys()) if all_countries else requested_countries
    for country_code in country_codes:
        points = sorted(by_country.get(country_code) or [], key=lambda item: item["x"])
        if not points:
            continue
        country_name = _get_country_name_from_iso3(country_code)
        source_url = f"{provider_config.get('source_url_template') or ''}?locations={country_code}"
        series.append(
            {
                "provider": WORLD_BANK_PROVIDER,
                "country": country_name,
                "country_code": country_code,
                "indicator": label,
                "series_id": series_id,
                "unit": entry.unit,
                "frequency": "annual",
                "points": points,
                "source_url": source_url,
            }
        )
        source_refs.append(
            _source_reference(
                WORLD_BANK_PROVIDER,
                indicator=label,
                series_id=series_id,
                country=country_name,
                source_url=source_url,
            )
        )

    if not series:
        logger.error(
            'Macro retrieval error provider=worldbank indicator=%s url="%s" error="%s"',
            series_id,
            _truncate_log(request_url, 700),
            "World Bank returned no usable data.",
        )
        raise RuntimeError(f"World Bank returned no usable data for {series_id}.")
    logger.info(
        "Macro retrieval success provider=worldbank indicator=%s series=%s url=\"%s\"",
        series_id,
        len(series),
        _truncate_log(str(response.request.url), 700),
    )

    return {
        "provider": WORLD_BANK_PROVIDER,
        "concept_id": entry.concept_id,
        "concept_label": entry.concept_label,
        "api_request_url": str(response.request.url),
        "series": series,
        "source_references": source_refs,
    }


def _fetch_imf(query: str, entry: MacroCatalogEntry, provider_config: Dict[str, Any], countries: List[str], start_year: Optional[int], end_year: Optional[int], *, all_countries: bool = False) -> Dict[str, Any]:
    series_id = str(provider_config.get("series_id") or "").strip()
    label = str(provider_config.get("label") or entry.concept_label or series_id).strip()
    url = f"{settings.imf_base_url.rstrip('/')}/{series_id}"
    logger.info(
        'Macro retrieval request provider=imf indicator=%s countries="%s" all_countries=%s url="%s"',
        series_id,
        ",".join(countries),
        all_countries,
        _truncate_log(url, 700),
    )
    try:
        response = httpx.get(url, timeout=settings.macro_timeout_seconds)
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        response = exc.response if isinstance(exc, httpx.HTTPStatusError) else None
        body_preview = _truncate_log(response.text, 500) if response is not None else ""
        logger.error(
            'Macro retrieval error provider=imf indicator=%s url="%s" status=%s error="%s" body="%s"',
            series_id,
            _truncate_log(url, 700),
            getattr(response, "status_code", ""),
            _truncate_log(exc, 500),
            body_preview,
        )
        raise
    values = payload.get("values") if isinstance(payload.get("values"), dict) else {}
    series_values = values.get(series_id) if isinstance(values.get(series_id), dict) else {}
    if not isinstance(series_values, dict):
        raise RuntimeError("IMF returned an unexpected response shape.")
    if not countries:
        if all_countries:
            countries = sorted(str(code or "").strip().upper() for code in series_values.keys() if str(code or "").strip())
        else:
            countries = ["AUS"]

    series: List[Dict[str, Any]] = []
    source_refs: List[Dict[str, Any]] = []
    for country_code in countries:
        country_values = series_values.get(country_code) if isinstance(series_values.get(country_code), dict) else {}
        points = []
        for year, raw_value in country_values.items():
            value = _parse_numeric(raw_value)
            year_text = str(year or "").strip()
            if value is None or not year_text:
                continue
            if start_year and year_text.isdigit() and int(year_text) < start_year:
                continue
            if end_year and year_text.isdigit() and int(year_text) > end_year:
                continue
            points.append({"x": year_text, "y": value})
        points.sort(key=lambda item: item["x"])
        if not points:
            continue
        country_name = _get_country_name_from_iso3(country_code)
        source_url = f"{provider_config.get('source_url_template') or ''}/{country_code}"
        series.append(
            {
                "provider": IMF_PROVIDER,
                "country": country_name,
                "country_code": country_code,
                "indicator": label,
                "series_id": series_id,
                "unit": entry.unit,
                "frequency": "annual",
                "points": points,
                "source_url": source_url,
            }
        )
        source_refs.append(
            _source_reference(
                IMF_PROVIDER,
                indicator=label,
                series_id=series_id,
                country=country_name,
                source_url=source_url,
            )
        )

    if not series:
        logger.error(
            'Macro retrieval error provider=imf indicator=%s url="%s" error="%s"',
            series_id,
            _truncate_log(str(response.request.url), 700),
            "IMF returned no usable data.",
        )
        raise RuntimeError(f"IMF returned no usable data for {series_id}.")
    logger.info(
        "Macro retrieval success provider=imf indicator=%s series=%s url=\"%s\"",
        series_id,
        len(series),
        _truncate_log(str(response.request.url), 700),
    )

    return {
        "provider": IMF_PROVIDER,
        "concept_id": entry.concept_id,
        "concept_label": entry.concept_label,
        "api_request_url": str(response.request.url),
        "series": series,
        "source_references": source_refs,
    }


def _choose_oecd_rows(rows: List[Dict[str, str]], provider_config: Dict[str, Any], countries: List[str]) -> List[Dict[str, str]]:
    base_filters = provider_config.get("row_filters") if isinstance(provider_config.get("row_filters"), dict) else {}
    preferred_transformations = [
        str(item or "").strip()
        for item in (provider_config.get("preferred_transformations") or [])
        if str(item or "").strip()
    ]
    preferred_totals = provider_config.get("preferred_totals") if isinstance(provider_config.get("preferred_totals"), dict) else {}

    filtered = []
    for row in rows:
        if str(row.get("REF_AREA") or "").strip().upper() not in countries:
            continue
        keep = True
        for key, expected in base_filters.items():
            if str(row.get(key) or "").strip() != str(expected):
                keep = False
                break
        if keep:
            filtered.append(row)

    if not filtered and preferred_transformations:
        relaxed = []
        for row in rows:
            if str(row.get("REF_AREA") or "").strip().upper() not in countries:
                continue
            keep = True
            for key, expected in base_filters.items():
                if key == "TRANSFORMATION":
                    continue
                if str(row.get(key) or "").strip() != str(expected):
                    keep = False
                    break
            if keep:
                relaxed.append(row)
        filtered = relaxed

    if preferred_transformations and filtered:
        best = []
        for candidate in preferred_transformations:
            subset = [row for row in filtered if str(row.get("TRANSFORMATION") or "").strip() == candidate]
            if subset:
                best = subset
                break
        if best:
            filtered = best

    if preferred_totals and filtered:
        for key, preferred_values in preferred_totals.items():
            subset = [
                row
                for row in filtered
                if str(row.get(key) or "").strip() in {str(item) for item in preferred_values}
            ]
            if subset:
                filtered = subset

    return filtered


def _fetch_oecd(query: str, entry: MacroCatalogEntry, provider_config: Dict[str, Any], countries: List[str], start_year: Optional[int], end_year: Optional[int], *, all_countries: bool = False) -> Dict[str, Any]:
    agency = str(provider_config.get("agency") or "").strip()
    dataflow = str(provider_config.get("dataflow") or "").strip()
    version = str(provider_config.get("version") or "1.0").strip()
    if not agency or not dataflow:
        raise RuntimeError("OECD provider configuration is incomplete.")

    url = f"{settings.oecd_base_url.rstrip('/')}/data/{agency},{dataflow},{version}"
    params: Dict[str, Any] = {
        "dimensionAtObservation": "AllDimensions",
        "format": "csvfilewithlabels",
    }
    if start_year:
        params["startPeriod"] = str(start_year)
    if end_year:
        params["endPeriod"] = str(end_year)

    request_url = _request_url(url, params)
    logger.info(
        'Macro retrieval request provider=oecd indicator=%s countries="%s" all_countries=%s url="%s"',
        dataflow,
        ",".join(countries),
        all_countries,
        _truncate_log(request_url, 700),
    )
    try:
        response = httpx.get(url, params=params, timeout=max(settings.macro_timeout_seconds, 60))
        response.raise_for_status()
        text = response.text
    except Exception as exc:
        response = exc.response if isinstance(exc, httpx.HTTPStatusError) else None
        body_preview = _truncate_log(response.text, 500) if response is not None else ""
        logger.error(
            'Macro retrieval error provider=oecd indicator=%s url="%s" status=%s error="%s" body="%s"',
            dataflow,
            _truncate_log(request_url, 700),
            getattr(response, "status_code", ""),
            _truncate_log(exc, 500),
            body_preview,
        )
        raise
    if "Could not find Dataflow" in text:
        raise RuntimeError(f"OECD dataflow {agency},{dataflow},{version} was not found.")

    rows = list(csv.DictReader(io.StringIO(text)))
    if not countries:
        if all_countries:
            countries = sorted(
                {
                    str(row.get("REF_AREA") or "").strip().upper()
                    for row in rows
                    if isinstance(row, dict) and str(row.get("REF_AREA") or "").strip()
                }
            )
        else:
            countries = ["AUS"]
    selected_rows = _choose_oecd_rows(rows, provider_config, countries)
    if not selected_rows:
        logger.error(
            'Macro retrieval error provider=oecd indicator=%s url="%s" error="%s"',
            dataflow,
            _truncate_log(str(response.request.url), 700),
            "OECD returned no usable rows.",
        )
        raise RuntimeError(f"OECD returned no usable rows for {dataflow}.")

    label = str(provider_config.get("label") or entry.concept_label or dataflow).strip()
    series_id = dataflow
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in selected_rows:
        country_code = str(row.get("REF_AREA") or "").strip().upper()
        period = str(row.get("TIME_PERIOD") or "").strip()
        value = _parse_numeric(row.get("OBS_VALUE"))
        if not country_code or not period or value is None:
            continue
        grouped.setdefault(country_code, []).append({"x": period, "y": value})

    series: List[Dict[str, Any]] = []
    source_refs: List[Dict[str, Any]] = []
    for country_code in countries:
        points = sorted(grouped.get(country_code) or [], key=lambda item: item["x"])
        if not points:
            continue
        country_name = _get_country_name_from_iso3(country_code)
        source_url = str(provider_config.get("source_url_template") or "").strip()
        series.append(
            {
                "provider": OECD_PROVIDER,
                "country": country_name,
                "country_code": country_code,
                "indicator": label,
                "series_id": series_id,
                "unit": entry.unit,
                "frequency": "annual" if all("-" not in item["x"] for item in points) else "mixed",
                "points": points,
                "source_url": source_url,
            }
        )
        source_refs.append(
            _source_reference(
                OECD_PROVIDER,
                indicator=label,
                series_id=series_id,
                country=country_name,
                source_url=source_url,
            )
        )

    if not series:
        logger.error(
            'Macro retrieval error provider=oecd indicator=%s url="%s" error="%s"',
            dataflow,
            _truncate_log(str(response.request.url), 700),
            "OECD returned no usable points.",
        )
        raise RuntimeError(f"OECD returned no usable points for {dataflow}.")
    logger.info(
        "Macro retrieval success provider=oecd indicator=%s series=%s url=\"%s\"",
        dataflow,
        len(series),
        _truncate_log(str(response.request.url), 700),
    )

    return {
        "provider": OECD_PROVIDER,
        "concept_id": entry.concept_id,
        "concept_label": entry.concept_label,
        "api_request_url": str(response.request.url),
        "series": series,
        "source_references": source_refs,
    }


def _fetch_comtrade(
    query: str,
    entry: MacroCatalogEntry,
    provider_config: Dict[str, Any],
    *,
    reporter_codes: List[str],
    partner_codes: List[str],
    flow_code: str,
    frequency_code: str,
    hs_codes: List[str],
    start_year: Optional[int],
    end_year: Optional[int],
) -> Dict[str, Any]:
    reporters_lookup = {str(item.get("code") or "").strip(): item for item in _comtrade_dimension("countries")}
    hs_lookup = {
        str(item.get("code") or "").strip(): item
        for item in (_comtrade_dimension("hs_2digit") + _comtrade_dimension("hs_4digit"))
    }
    flow_lookup = {str(item.get("code") or "").strip(): item for item in _comtrade_dimension("flows")}
    frequency_lookup = {
        "A": {"code": "A", "label": "Annual"},
        "M": {"code": "M", "label": "Monthly"},
    }

    clean_reporters = _resolve_comtrade_area_codes(reporter_codes, list(reporters_lookup.values()))
    clean_partners = [str(code).strip() for code in partner_codes if str(code).strip()]
    if any(str(item or "").strip() == "0" for item in partner_codes):
        clean_partners.insert(0, "0")
        clean_partners = list(dict.fromkeys(clean_partners))
    clean_hs_codes = _resolve_comtrade_hs_codes(hs_codes, list(hs_lookup.values()))
    if not clean_reporters:
        raise RuntimeError("Comtrade retrieval requires at least one valid reporterCode.")
    if not clean_partners:
        clean_partners = ["0"]
    if not clean_hs_codes:
        clean_hs_codes = ["TOTAL"]

    clean_flow_code = flow_code if flow_code in flow_lookup else _comtrade_default_flow(query)
    clean_frequency_code = frequency_code if frequency_code in frequency_lookup else _comtrade_default_frequency(query)
    resolved_start_year, resolved_end_year = _comtrade_default_period_range(
        frequency_code=clean_frequency_code,
        start_year=start_year,
        end_year=end_year,
    )

    period_values = _comtrade_period_values(resolved_start_year, resolved_end_year, clean_frequency_code)
    period_chunks = _chunk_period_values(period_values, max_periods=12)
    if not period_chunks:
        raise RuntimeError("Comtrade retrieval produced an empty period selection.")

    request_count = len(clean_reporters) * len(clean_partners) * len(clean_hs_codes) * len(period_chunks)
    point_budget = len(clean_reporters) * len(clean_partners) * len(clean_hs_codes) * len(period_values)
    if request_count > 36 or point_budget > 1200:
        raise RuntimeError(
            "Requested UN Comtrade retrieval is too broad. Narrow one of: countries, partners, HS codes, frequency, or time range."
        )

    if settings.comtrade_api_key:
        base_url = f"{settings.comtrade_base_url.rstrip('/')}/C/{clean_frequency_code}/HS"
    else:
        base_url = f"https://comtradeapi.un.org/public/v1/preview/C/{clean_frequency_code}/HS"
    source_url = "https://comtradeplus.un.org/TradeFlow"
    flow_label = str((flow_lookup.get(clean_flow_code) or {}).get("label") or clean_flow_code)
    frequency_label = str((frequency_lookup.get(clean_frequency_code) or {}).get("label") or clean_frequency_code)

    all_series: Dict[tuple[str, str, str, str], Dict[str, Any]] = {}
    source_refs: List[Dict[str, Any]] = []
    last_request_url = ""

    for reporter_code in clean_reporters:
        for partner_code in clean_partners:
            for hs_code in clean_hs_codes:
                for period_chunk in period_chunks:
                    params: Dict[str, Any] = {
                        "typeCode": "C",
                        "freqCode": clean_frequency_code,
                        "clCode": "HS",
                        "reporterCode": reporter_code,
                        "period": period_chunk,
                        "partnerCode": partner_code,
                        "cmdCode": hs_code,
                        "flowCode": clean_flow_code,
                        "format": "json",
                    }
                    if settings.comtrade_api_key:
                        params["subscription-key"] = settings.comtrade_api_key

                    request_url = _request_url(base_url, params)
                    last_request_url = request_url
                    logger.info(
                        'Macro retrieval request provider=comtrade reporters="%s" partners="%s" hs="%s" flow=%s frequency=%s url="%s"',
                        ",".join(clean_reporters),
                        ",".join(clean_partners),
                        ",".join(clean_hs_codes),
                        clean_flow_code,
                        clean_frequency_code,
                        _truncate_log(request_url, 700),
                    )
                    try:
                        response = httpx.get(base_url, params=params, timeout=max(settings.macro_timeout_seconds, 60))
                        response.raise_for_status()
                        payload = response.json()
                    except Exception as exc:
                        response = exc.response if isinstance(exc, httpx.HTTPStatusError) else None
                        body_preview = _truncate_log(response.text, 500) if response is not None else ""
                        logger.error(
                            'Macro retrieval error provider=comtrade url="%s" status=%s error="%s" body="%s"',
                            _truncate_log(request_url, 700),
                            getattr(response, "status_code", ""),
                            _truncate_log(exc, 500),
                            body_preview,
                        )
                        raise

                    rows = payload.get("data") if isinstance(payload, dict) else None
                    if not isinstance(rows, list):
                        continue

                    for row in rows:
                        if not isinstance(row, dict):
                            continue
                        period = str(row.get("period") or "").strip()
                        value = _parse_numeric(row.get("primaryValue"))
                        if not period or value is None:
                            continue
                        row_reporter_code = str(row.get("reporterCode") or reporter_code).strip()
                        row_partner_code = str(row.get("partnerCode") or partner_code).strip()
                        row_cmd_code = str(row.get("cmdCode") or hs_code).strip() or hs_code
                        key = (row_reporter_code, row_partner_code, row_cmd_code, clean_flow_code)
                        series_entry = all_series.setdefault(
                            key,
                            {
                                "provider": COMTRADE_PROVIDER,
                                "country": str(row.get("reporterDesc") or (reporters_lookup.get(row_reporter_code) or {}).get("label") or row_reporter_code),
                                "country_code": row_reporter_code,
                                "partner": str(row.get("partnerDesc") or ("World" if row_partner_code == "0" else row_partner_code)),
                                "partner_code": row_partner_code,
                                "indicator": f"{flow_label} - {str((hs_lookup.get(row_cmd_code) or {}).get('label') or row.get('cmdDesc') or row_cmd_code)}",
                                "series_id": provider_config.get("series_id") or entry.concept_id or "UN_COMTRADE_GOODS_TRADE",
                                "unit": entry.unit or "US Dollars",
                                "frequency": "monthly" if clean_frequency_code == "M" else "annual",
                                "flow_code": clean_flow_code,
                                "flow_label": flow_label,
                                "hs_code": row_cmd_code,
                                "hs_label": str((hs_lookup.get(row_cmd_code) or {}).get("label") or row.get("cmdDesc") or row_cmd_code),
                                "source_url": source_url,
                                "points": [],
                            },
                        )
                        x_value = f"{period[:4]}-{period[4:6]}" if clean_frequency_code == "M" and len(period) == 6 else period
                        series_entry["points"].append({"x": x_value, "y": value})

    series: List[Dict[str, Any]] = []
    seen_refs: set[str] = set()
    for item in all_series.values():
        points_by_x: Dict[str, float] = {}
        for point in item.get("points") or []:
            x_value = str(point.get("x") or "").strip()
            y_value = point.get("y")
            if not x_value or not isinstance(y_value, (int, float)):
                continue
            if x_value not in points_by_x or y_value > points_by_x[x_value]:
                points_by_x[x_value] = float(y_value)
        points = [{"x": key, "y": points_by_x[key]} for key in sorted(points_by_x.keys())]
        if not points:
            continue
        item["points"] = points
        series.append(item)
        ref_key = "|".join(
            [
                str(item.get("country") or ""),
                str(item.get("partner") or ""),
                str(item.get("hs_code") or ""),
                str(item.get("flow_code") or ""),
            ]
        )
        if ref_key in seen_refs:
            continue
        seen_refs.add(ref_key)
        source_refs.append(
            _source_reference(
                COMTRADE_PROVIDER,
                indicator=str(item.get("indicator") or ""),
                series_id=str(item.get("hs_code") or ""),
                country=str(item.get("country") or ""),
                source_url=source_url,
            )
        )

    if not series:
        raise RuntimeError("UN Comtrade returned no usable data for the selected request shape.")

    logger.info(
        'Macro retrieval success provider=comtrade series=%s request="%s"',
        len(series),
        _truncate_log(last_request_url, 700),
    )

    return {
        "provider": COMTRADE_PROVIDER,
        "concept_id": entry.concept_id,
        "concept_label": entry.concept_label,
        "api_request_url": last_request_url,
        "query_parameters": {
            "reporterCodes": clean_reporters,
            "partnerCodes": clean_partners,
            "flowCode": clean_flow_code,
            "frequencyCode": clean_frequency_code,
            "frequencyLabel": frequency_label,
            "hsCodes": clean_hs_codes,
            "startYear": resolved_start_year,
            "endYear": resolved_end_year,
        },
        "series": series,
        "source_references": source_refs,
    }


def run_macro_query(query: str) -> Dict[str, Any]:
    clean_query = str(query or "").strip()
    if not clean_query:
        raise RuntimeError("macro query cannot be empty.")

    explicit_provider = detect_explicit_provider(clean_query)
    catalog_matches = _search_macro_catalog(clean_query, explicit_provider=explicit_provider, limit=5)
    if not catalog_matches:
        supported = _catalog_preview_labels(limit=8)
        raise RuntimeError(
            f"No supported macro indicator was matched for '{clean_query}'. "
            f"Currently supported concepts include: {supported}."
        )
    selected_entry = catalog_matches[0]
    provider_key = selected_entry.provider_key
    provider_config = dict(selected_entry.provider_config)

    retrieval_inputs = normalize_macro_retrieval_inputs(clean_query)
    countries = retrieval_inputs["countries"]
    start_year = retrieval_inputs["start_year"]
    end_year = retrieval_inputs["end_year"]
    all_countries = retrieval_inputs["all_countries"]

    if provider_key == "worldbank":
        result = _fetch_world_bank(clean_query, selected_entry, provider_config, countries, start_year, end_year, all_countries=all_countries)
    elif provider_key == "imf":
        result = _fetch_imf(clean_query, selected_entry, provider_config, countries, start_year, end_year, all_countries=all_countries)
    elif provider_key == "oecd":
        result = _fetch_oecd(clean_query, selected_entry, provider_config, countries, start_year, end_year, all_countries=all_countries)
    elif provider_key == "comtrade":
        if not countries:
            raise RuntimeError("UN Comtrade retrieval requires an explicit reporter country. Inspect metadata first and choose exact reporter/partner/HS codes.")
        result = _fetch_comtrade(
            clean_query,
            selected_entry,
            provider_config,
            reporter_codes=[countries[0]],
            partner_codes=["0"],
            flow_code=_comtrade_default_flow(clean_query),
            frequency_code=_comtrade_default_frequency(clean_query),
            hs_codes=["TOTAL"],
            start_year=start_year,
            end_year=end_year,
        )
    else:
        raise RuntimeError(f"Unsupported macro provider '{provider_key}'.")

    result["query"] = clean_query
    result["provider_key"] = provider_key
    result["matched_indicators"] = [
        {
            "entry_id": item.entry_id,
            "provider_key": item.provider_key,
            "provider": item.provider_name,
            "concept_id": item.concept_id,
            "concept_label": item.concept_label,
            "indicator_label": item.indicator_label,
        }
        for item in catalog_matches[:3]
    ]
    result["matched_concepts"] = [
        {
            "concept_id": item.concept_id,
            "label": item.concept_label,
        }
        for item in catalog_matches[:3]
    ]
    result["selected_indicator"] = {
        "entry_id": selected_entry.entry_id,
        "provider_key": selected_entry.provider_key,
        "provider": selected_entry.provider_name,
        "concept_id": selected_entry.concept_id,
        "concept_label": selected_entry.concept_label,
        "indicator_label": selected_entry.indicator_label,
    }
    result["countries"] = countries
    result["all_countries"] = all_countries
    result["start_year"] = start_year
    result["end_year"] = end_year
    return result


def retrieve_macro_candidate(
    candidate_id: str,
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
    clean_candidate_id = str(candidate_id or "").strip()
    if not clean_candidate_id:
        raise RuntimeError("macro retrieval requires candidateId.")
    clean_query = str(query or "").strip()
    if not clean_query:
        raise RuntimeError("macro retrieval requires query.")

    _get_macro_catalog_connection()
    selected_entry = _CATALOG_ENTRY_BY_ID.get(clean_candidate_id)
    if selected_entry is None:
        available = ", ".join(entry.entry_id for entry in _CATALOG_ENTRIES[:12])
        raise RuntimeError(
            f"Unknown macro candidateId '{clean_candidate_id}'. Available examples: {available}"
        )

    retrieval_inputs = normalize_macro_retrieval_inputs(
        clean_query,
        countries=countries,
        all_countries=all_countries,
        start_year=start_year,
        end_year=end_year,
    )
    countries = retrieval_inputs["countries"]
    start_year = retrieval_inputs["start_year"]
    end_year = retrieval_inputs["end_year"]
    all_countries = retrieval_inputs["all_countries"]
    provider_key = selected_entry.provider_key
    provider_config = dict(selected_entry.provider_config)

    if provider_key == "worldbank":
        result = _fetch_world_bank(clean_query, selected_entry, provider_config, countries, start_year, end_year, all_countries=all_countries)
    elif provider_key == "imf":
        result = _fetch_imf(clean_query, selected_entry, provider_config, countries, start_year, end_year, all_countries=all_countries)
    elif provider_key == "oecd":
        result = _fetch_oecd(clean_query, selected_entry, provider_config, countries, start_year, end_year, all_countries=all_countries)
    elif provider_key == "comtrade":
        result = _fetch_comtrade(
            clean_query,
            selected_entry,
            provider_config,
            reporter_codes=[str(item).strip() for item in (reporter_codes or []) if str(item).strip()],
            partner_codes=[str(item).strip() for item in (partner_codes or []) if str(item).strip()],
            flow_code=str(flow_code or "").strip().upper(),
            frequency_code=str(frequency_code or "").strip().upper(),
            hs_codes=[str(item).strip() for item in (hs_codes or []) if str(item).strip()],
            start_year=start_year,
            end_year=end_year,
        )
    else:
        raise RuntimeError(f"Unsupported macro provider '{provider_key}'.")

    result["query"] = clean_query
    result["provider_key"] = provider_key
    result["selected_indicator"] = {
        "entry_id": selected_entry.entry_id,
        "provider_key": selected_entry.provider_key,
        "provider": selected_entry.provider_name,
        "concept_id": selected_entry.concept_id,
        "concept_label": selected_entry.concept_label,
        "indicator_label": selected_entry.indicator_label,
    }
    result["countries"] = countries
    result["all_countries"] = all_countries
    result["start_year"] = start_year
    result["end_year"] = end_year
    return result
