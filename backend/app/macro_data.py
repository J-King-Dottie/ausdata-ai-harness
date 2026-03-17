from __future__ import annotations

import csv
import io
import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

from .config import get_settings


settings = get_settings()
MACRO_CATALOG_PATH = Path(__file__).resolve().parents[2] / "MACRO_CATALOG_FULL.json"


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
    description: str
    search_text: str
    provider_config: Dict[str, Any]
    concept: Dict[str, Any]


_CATALOG_ENTRIES: List[MacroCatalogEntry] = []
_CATALOG_ENTRY_BY_ID: Dict[str, MacroCatalogEntry] = {}
_CATALOG_CONN: Optional[sqlite3.Connection] = None
_CATALOG_FILE_MTIME: Optional[float] = None


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
                description=description,
                search_text=search_text,
                provider_config=dict(provider_config),
                concept=dict(concept),
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
            description=str(item.get("description") or "").strip(),
            search_text=str(item.get("search_text") or "").strip(),
            provider_config=dict(item.get("provider_config") or {}),
            concept=dict(item.get("concept") or {}),
        )
        if not entry.entry_id or not entry.provider_key or not entry.indicator_label:
            continue
        entries.append(entry)
    if not entries:
        raise RuntimeError(f"Macro catalog file {MACRO_CATALOG_PATH} contained no valid entries.")
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
                entry_id, provider_key, provider_name, concept_id, concept_label, indicator_label, description, search_text
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                entry.entry_id,
                entry.provider_key,
                entry.provider_name,
                entry.concept_id,
                entry.concept_label,
                entry.indicator_label,
                entry.description,
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
        }
        for entry in matches
    ]
    return {
        "query": clean_query,
        "candidates": candidates,
    }


def _normalize_text(value: str) -> str:
    return re.sub(r"[^a-z0-9\s]+", " ", str(value or "").lower()).strip()




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


def evaluate_macro_result_shape(query: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    series = payload.get("series") if isinstance(payload.get("series"), list) else []
    countries = {
        str(item.get("country_code") or "").strip().upper()
        for item in series
        if isinstance(item, dict) and str(item.get("country_code") or "").strip()
    }
    query_countries = payload.get("countries") if isinstance(payload.get("countries"), list) else []
    shape = infer_macro_retrieval_shape(query, [str(item) for item in query_countries])
    country_count = len(countries)
    is_acceptable = country_count >= int(shape.get("min_country_count") or 1)
    reason = ""
    if not is_acceptable and bool(shape.get("multi_country")):
        reason = (
            f"Requested {shape.get('shape')} but only {country_count} country series were returned; "
            f"needed at least {shape.get('min_country_count')}."
        )
    return {
        "shape": shape,
        "country_count": country_count,
        "is_acceptable": is_acceptable,
        "reason": reason,
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


def _fetch_world_bank(query: str, concept: Dict[str, Any], provider_config: Dict[str, Any], countries: List[str], start_year: Optional[int], end_year: Optional[int], *, all_countries: bool = False) -> Dict[str, Any]:
    if not countries and not all_countries:
        countries = ["AUS"]
    series_id = str(provider_config.get("series_id") or "").strip()
    label = str(provider_config.get("label") or concept.get("label") or series_id).strip()
    country_path = "all" if all_countries else ";".join(countries)
    url = f"{settings.worldbank_base_url.rstrip('/')}/country/{country_path}/indicator/{series_id}"
    params: Dict[str, Any] = {"format": "json", "per_page": 20000}
    if start_year and end_year:
        params["date"] = f"{start_year}:{end_year}"
    response = httpx.get(url, params=params, timeout=settings.macro_timeout_seconds)
    response.raise_for_status()
    payload = response.json()
    error_message = _parse_world_bank_error(payload)
    if error_message:
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
    country_codes = sorted(by_country.keys()) if all_countries else countries
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
                "unit": "",
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
        raise RuntimeError(f"World Bank returned no usable data for {series_id}.")

    return {
        "provider": WORLD_BANK_PROVIDER,
        "concept_id": concept["concept_id"],
        "concept_label": concept["label"],
        "api_request_url": str(response.request.url),
        "series": series,
        "source_references": source_refs,
    }


def _fetch_imf(query: str, concept: Dict[str, Any], provider_config: Dict[str, Any], countries: List[str], start_year: Optional[int], end_year: Optional[int], *, all_countries: bool = False) -> Dict[str, Any]:
    series_id = str(provider_config.get("series_id") or "").strip()
    label = str(provider_config.get("label") or concept.get("label") or series_id).strip()
    url = f"{settings.imf_base_url.rstrip('/')}/{series_id}"
    response = httpx.get(url, timeout=settings.macro_timeout_seconds)
    response.raise_for_status()
    payload = response.json()
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
                "unit": "",
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
        raise RuntimeError(f"IMF returned no usable data for {series_id}.")

    return {
        "provider": IMF_PROVIDER,
        "concept_id": concept["concept_id"],
        "concept_label": concept["label"],
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


def _fetch_oecd(query: str, concept: Dict[str, Any], provider_config: Dict[str, Any], countries: List[str], start_year: Optional[int], end_year: Optional[int], *, all_countries: bool = False) -> Dict[str, Any]:
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

    response = httpx.get(url, params=params, timeout=max(settings.macro_timeout_seconds, 60))
    response.raise_for_status()
    text = response.text
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
        raise RuntimeError(f"OECD returned no usable rows for {dataflow}.")

    label = str(provider_config.get("label") or concept.get("label") or dataflow).strip()
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
                "unit": "",
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
        raise RuntimeError(f"OECD returned no usable points for {dataflow}.")

    return {
        "provider": OECD_PROVIDER,
        "concept_id": concept["concept_id"],
        "concept_label": concept["label"],
        "api_request_url": str(response.request.url),
        "series": series,
        "source_references": source_refs,
    }


def run_macro_query(query: str) -> Dict[str, Any]:
    clean_query = str(query or "").strip()
    if not clean_query:
        raise RuntimeError("macro_data_tool query cannot be empty.")

    explicit_provider = detect_explicit_provider(clean_query)
    catalog_matches = _search_macro_catalog(clean_query, explicit_provider=explicit_provider, limit=5)
    if not catalog_matches:
        supported = _catalog_preview_labels(limit=8)
        raise RuntimeError(
            f"No supported macro indicator was matched for '{clean_query}'. "
            f"Currently supported concepts include: {supported}."
        )
    selected_entry = catalog_matches[0]
    concept = dict(selected_entry.concept)
    provider_key = selected_entry.provider_key
    provider_config = dict(selected_entry.provider_config)

    retrieval_inputs = normalize_macro_retrieval_inputs(clean_query)
    countries = retrieval_inputs["countries"]
    start_year = retrieval_inputs["start_year"]
    end_year = retrieval_inputs["end_year"]
    all_countries = retrieval_inputs["all_countries"]

    if provider_key == "worldbank":
        result = _fetch_world_bank(clean_query, concept, provider_config, countries, start_year, end_year, all_countries=all_countries)
    elif provider_key == "imf":
        result = _fetch_imf(clean_query, concept, provider_config, countries, start_year, end_year, all_countries=all_countries)
    elif provider_key == "oecd":
        result = _fetch_oecd(clean_query, concept, provider_config, countries, start_year, end_year, all_countries=all_countries)
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
    result["retrieval_evaluation"] = evaluate_macro_result_shape(clean_query, result)
    return result


def retrieve_macro_candidate(
    candidate_id: str,
    query: str,
    *,
    countries: Optional[List[str]] = None,
    all_countries: bool = False,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
) -> Dict[str, Any]:
    clean_candidate_id = str(candidate_id or "").strip()
    if not clean_candidate_id:
        raise RuntimeError("macro_data_tool retrieve requires candidateId.")
    clean_query = str(query or "").strip()
    if not clean_query:
        raise RuntimeError("macro_data_tool retrieve requires query.")

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
    concept = dict(selected_entry.concept)
    provider_key = selected_entry.provider_key
    provider_config = dict(selected_entry.provider_config)

    if provider_key == "worldbank":
        result = _fetch_world_bank(clean_query, concept, provider_config, countries, start_year, end_year, all_countries=all_countries)
    elif provider_key == "imf":
        result = _fetch_imf(clean_query, concept, provider_config, countries, start_year, end_year, all_countries=all_countries)
    elif provider_key == "oecd":
        result = _fetch_oecd(clean_query, concept, provider_config, countries, start_year, end_year, all_countries=all_countries)
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
    result["retrieval_evaluation"] = evaluate_macro_result_shape(clean_query, result)
    return result
