from __future__ import annotations

import json
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, List

import httpx


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_PATH = ROOT / "MACRO_CATALOG_FULL.json"

WORLD_BANK_URL = "https://api.worldbank.org/v2/indicator"
IMF_URL = "https://www.imf.org/external/datamapper/api/v1/indicators"
OECD_DATAFLOW_URL = "https://sdmx.oecd.org/public/rest/dataflow/all/all/latest"

WORLD_BANK_PROVIDER = "World Bank"
IMF_PROVIDER = "IMF"
OECD_PROVIDER = "OECD"
COMTRADE_PROVIDER = "UN Comtrade"

SDMX_NS = {
    "message": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message",
    "structure": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
    "common": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common",
}


def _clean_text(value: Any) -> str:
    text = str(value or "")
    text = re.sub(r"\s+", " ", text.replace("\u0000", " ")).strip()
    return text


def _join_search_text(parts: List[str]) -> str:
    deduped: List[str] = []
    for part in parts:
        clean = _clean_text(part)
        if clean and clean not in deduped:
            deduped.append(clean)
    return " ".join(deduped)


def _entry_has_stale_signal(entry: Dict[str, Any]) -> bool:
    entry_id = _clean_text(entry.get("entry_id"))
    text = " ".join(
        _clean_text(entry.get(field))
        for field in ("entry_id", "indicator_label", "concept_label", "description", "search_text")
    ).lower()
    if not text and not entry_id:
        return False
    if re.search(r"worldbank::[0-9]+\.[0-9]+\.hcount\.", entry_id, re.I):
        return True
    stale_markers = (
        "wdi database archives",
        "database archives",
        " archived",
        "archive ",
    )
    return any(marker in text for marker in stale_markers)


def fetch_world_bank_catalog(client: httpx.Client) -> List[Dict[str, Any]]:
    first = client.get(
        WORLD_BANK_URL,
        params={"format": "json", "per_page": 20000, "page": 1},
        timeout=120,
    )
    first.raise_for_status()
    payload = first.json()
    meta = payload[0] if isinstance(payload, list) and payload else {}
    pages = int(meta.get("pages") or 1)
    rows = payload[1] if isinstance(payload, list) and len(payload) > 1 else []

    all_rows = list(rows) if isinstance(rows, list) else []
    for page in range(2, pages + 1):
        response = client.get(
            WORLD_BANK_URL,
            params={"format": "json", "per_page": 20000, "page": page},
            timeout=120,
        )
        response.raise_for_status()
        page_payload = response.json()
        page_rows = page_payload[1] if isinstance(page_payload, list) and len(page_payload) > 1 else []
        if isinstance(page_rows, list):
            all_rows.extend(page_rows)

    entries: List[Dict[str, Any]] = []
    for row in all_rows:
        if not isinstance(row, dict):
            continue
        indicator_id = _clean_text(row.get("id"))
        label = _clean_text(row.get("name"))
        if not indicator_id or not label:
            continue
        source = row.get("source") if isinstance(row.get("source"), dict) else {}
        source_label = _clean_text(source.get("value"))
        source_note = _clean_text(row.get("sourceNote"))
        source_org = _clean_text(row.get("sourceOrganization"))
        topics = []
        for topic in row.get("topics") or []:
            if isinstance(topic, dict):
                topic_label = _clean_text(topic.get("value"))
                if topic_label:
                    topics.append(topic_label)
        description = _clean_text(" ".join(part for part in [label, source_note, source_org] if part))
        search_text = _join_search_text(
            [
                indicator_id,
                label,
                source_label,
                source_note,
                source_org,
                *topics,
                "world bank",
                "worldbank",
            ]
        )
        source_url = f"https://data.worldbank.org/indicator/{indicator_id}"
        entries.append(
            {
                "entry_id": f"worldbank::{indicator_id}",
                "provider_key": "worldbank",
                "provider_name": WORLD_BANK_PROVIDER,
                "concept_id": indicator_id,
                "concept_label": label,
                "indicator_label": label,
                "unit": "",
                "description": description or label,
                "search_text": search_text,
                "provider_config": {
                    "series_id": indicator_id,
                    "label": label,
                    "source_url_template": source_url,
                },
            }
        )
    return entries


def fetch_imf_catalog(client: httpx.Client) -> List[Dict[str, Any]]:
    response = client.get(IMF_URL, timeout=120)
    response.raise_for_status()
    payload = response.json()
    indicators = payload.get("indicators") if isinstance(payload, dict) else {}
    if not isinstance(indicators, dict):
        return []

    entries: List[Dict[str, Any]] = []
    for series_id, item in indicators.items():
        if not isinstance(item, dict):
            continue
        clean_series_id = _clean_text(series_id)
        label = _clean_text(item.get("label"))
        if not clean_series_id or not label:
            continue
        description = _clean_text(item.get("description"))
        dataset = _clean_text(item.get("dataset"))
        source = _clean_text(item.get("source"))
        unit = _clean_text(item.get("unit"))
        source_url = (
            f"https://www.imf.org/external/datamapper/{clean_series_id}@{dataset}"
            if dataset
            else f"https://www.imf.org/external/datamapper/{clean_series_id}"
        )
        entries.append(
            {
                "entry_id": f"imf::{clean_series_id}",
                "provider_key": "imf",
                "provider_name": IMF_PROVIDER,
                "concept_id": clean_series_id,
                "concept_label": label,
                "indicator_label": label,
                "unit": unit,
                "description": _clean_text(" ".join(part for part in [label, description, source, unit] if part)) or label,
                "search_text": _join_search_text(
                    [
                        clean_series_id,
                        label,
                        description,
                        dataset,
                        source,
                        unit,
                        "imf",
                        "international monetary fund",
                    ]
                ),
                "provider_config": {
                    "series_id": clean_series_id,
                    "label": label,
                    "dataset": dataset,
                    "source_url_template": source_url,
                },
            }
        )
    return entries


def fetch_oecd_catalog(client: httpx.Client) -> List[Dict[str, Any]]:
    response = client.get(OECD_DATAFLOW_URL, timeout=180)
    response.raise_for_status()
    root = ET.fromstring(response.text)
    flows = root.findall(".//structure:Dataflow", SDMX_NS)

    entries: List[Dict[str, Any]] = []
    for flow in flows:
        agency_id = _clean_text(flow.attrib.get("agencyID"))
        if not agency_id.startswith("OECD"):
            continue
        dataflow_id = _clean_text(flow.attrib.get("id"))
        version = _clean_text(flow.attrib.get("version")) or "latest"
        if not dataflow_id:
            continue
        name_node = flow.find("common:Name", SDMX_NS)
        desc_node = flow.find("common:Description", SDMX_NS)
        structure_ref = flow.find("structure:Structure/common:Ref", SDMX_NS) or flow.find("structure:Structure/Ref", SDMX_NS)
        label = _clean_text(name_node.text if name_node is not None else dataflow_id)
        description = _clean_text(desc_node.text if desc_node is not None else label)
        dsd_id = _clean_text(structure_ref.attrib.get("id")) if structure_ref is not None else ""
        source_url = f"https://sdmx.oecd.org/public/rest/data/{agency_id},{dataflow_id},{version}"
        entries.append(
            {
                "entry_id": f"oecd::{agency_id}::{dataflow_id}::{version}",
                "provider_key": "oecd",
                "provider_name": OECD_PROVIDER,
                "concept_id": dataflow_id,
                "concept_label": label,
                "indicator_label": label,
                "unit": "",
                "description": description or label,
                "search_text": _join_search_text(
                    [
                        dataflow_id,
                        dsd_id,
                        label,
                        description,
                        agency_id,
                        "oecd",
                    ]
                ),
                "provider_config": {
                    "agency": agency_id,
                    "dataflow": dataflow_id,
                    "version": version,
                    "label": label,
                    "source_url_template": source_url,
                },
            }
        )
    return entries


def build_comtrade_catalog() -> List[Dict[str, Any]]:
    label = "UN Comtrade goods trade (imports and exports by partner and HS code)"
    description = (
        "UN Comtrade goods trade retrieval for imports and exports, bilateral trade, world totals, "
        "and HS product codes down to 4-digit headings. Metadata exposes reporter countries, "
        "partner areas, annual or monthly frequency, and HS code descriptions."
    )
    return [
        {
            "entry_id": "comtrade::goods_trade",
            "provider_key": "comtrade",
            "provider_name": COMTRADE_PROVIDER,
            "concept_id": "goods_trade",
            "concept_label": "Goods trade",
            "indicator_label": label,
            "unit": "US Dollars",
            "description": description,
            "search_text": _join_search_text(
                [
                    "goods trade",
                    "imports",
                    "exports",
                    "import",
                    "export",
                    "bilateral trade",
                    "partner",
                    "hs code",
                    "hs4",
                    "hs 4 digit heading",
                    "commodity",
                    "merchandise trade",
                    "un comtrade",
                    "united nations comtrade",
                    "comtrade",
                ]
            ),
            "provider_config": {
                "series_id": "UN_COMTRADE_GOODS_TRADE",
                "label": "UN Comtrade goods trade",
                "requires_metadata_before_retrieval": True,
                "metadata_source": "COMTRADE_METADATA.json",
                "source_url_template": "https://comtradeplus.un.org/TradeFlow",
            },
        }
    ]


def dedupe_entries(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduped: Dict[str, Dict[str, Any]] = {}
    for entry in entries:
        entry_id = _clean_text(entry.get("entry_id"))
        if not entry_id:
            continue
        existing = deduped.get(entry_id)
        if existing is None:
            deduped[entry_id] = entry
            continue
        existing["search_text"] = _join_search_text(
            [existing.get("search_text", ""), entry.get("search_text", "")]
        )
        if len(_clean_text(entry.get("description"))) > len(_clean_text(existing.get("description"))):
            existing["description"] = entry["description"]
        if not _clean_text(existing.get("unit")) and _clean_text(entry.get("unit")):
            existing["unit"] = entry["unit"]
    return list(deduped.values())


def filter_stale_entries(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [entry for entry in entries if not _entry_has_stale_signal(entry)]


def main() -> None:
    with httpx.Client(follow_redirects=True) as client:
        world_bank_entries = fetch_world_bank_catalog(client)
        imf_entries = fetch_imf_catalog(client)
        oecd_entries = fetch_oecd_catalog(client)
    comtrade_entries = build_comtrade_catalog()

    entries = sorted(
        filter_stale_entries(dedupe_entries(comtrade_entries + world_bank_entries + imf_entries + oecd_entries)),
        key=lambda item: (item["provider_key"], item["concept_label"].lower(), item["entry_id"]),
    )
    OUTPUT_PATH.write_text(json.dumps(entries, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {len(entries)} entries to {OUTPUT_PATH}")
    print(
        "Counts: "
        f"comtrade={len(comtrade_entries)} "
        f"worldbank={len(world_bank_entries)} "
        f"imf={len(imf_entries)} "
        f"oecd={len(oecd_entries)}"
    )


if __name__ == "__main__":
    main()
