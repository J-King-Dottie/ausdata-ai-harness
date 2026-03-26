from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List
from urllib.request import urlopen


PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_PATH = PROJECT_ROOT / "COMTRADE_METADATA.json"
REFERENCE_BASE = "https://comtradeapi.un.org/files/v1/app/reference"
SOURCE_URLS = {
    "reporters": f"{REFERENCE_BASE}/Reporters.json",
    "flows": f"{REFERENCE_BASE}/tradeRegimes.json",
    "hs": f"{REFERENCE_BASE}/HS.json",
}


def _fetch_json(url: str) -> Dict[str, Any]:
    with urlopen(url, timeout=120) as response:
        return json.loads(response.read().decode("utf-8-sig"))


def _clean_text(value: Any) -> str:
    return " ".join(str(value or "").replace("\xa0", " ").split()).strip()


def _normalize_area_codes(items: List[Dict[str, Any]], *, code_key: str, name_key: str) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        code = str(item.get(code_key) or item.get("id") or "").strip()
        label = _clean_text(item.get(name_key) or item.get("text") or "")
        if not code or not label:
            continue
        normalized.append({"code": code, "label": label})
    normalized.append({"code": "0", "label": "All partners (World total)"})
    deduped: Dict[str, Dict[str, Any]] = {}
    for item in normalized:
        deduped[str(item["code"])] = item
    normalized = list(deduped.values())
    normalized.sort(key=lambda item: (item["label"].lower(), item["code"]))
    return normalized


def _normalize_hs_codes(items: List[Dict[str, Any]], level: int) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        code = str(item.get("id") or "").strip()
        if not code:
            continue
        aggr_level = int(item.get("aggrLevel") or 0)
        if aggr_level != level:
            continue
        label = _clean_text(item.get("text") or "")
        if not label:
            continue
        entry = {"code": code, "label": label}
        if level == 4:
            entry["parent"] = str(item.get("parent") or "").strip()
        normalized.append(entry)
    normalized.append({"code": "TOTAL", "label": "TOTAL - All products"})
    deduped: Dict[str, Dict[str, Any]] = {}
    for item in normalized:
        deduped[str(item["code"])] = item
    normalized = list(deduped.values())
    normalized.sort(key=lambda item: item["code"])
    return normalized


def main() -> None:
    reporters = _fetch_json(SOURCE_URLS["reporters"])
    flows = _fetch_json(SOURCE_URLS["flows"])
    hs = _fetch_json(SOURCE_URLS["hs"])

    payload = {
        "flows": [
            {
                "code": str(item.get("id") or "").strip(),
                "label": _clean_text(item.get("text") or ""),
            }
            for item in (flows.get("results") if isinstance(flows.get("results"), list) else [])
            if str(item.get("id") or "").strip() in {"M", "X"}
        ],
        "countries": _normalize_area_codes(
            reporters.get("results") if isinstance(reporters.get("results"), list) else [],
            code_key="reporterCode",
            name_key="reporterDesc",
        ),
        "hs_2digit": _normalize_hs_codes(
            hs.get("results") if isinstance(hs.get("results"), list) else [],
            2,
        ),
        "hs_4digit": _normalize_hs_codes(
            hs.get("results") if isinstance(hs.get("results"), list) else [],
            4,
        ),
    }

    OUTPUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    counts = {key: len(value) for key, value in payload.items() if isinstance(value, list)}
    print(json.dumps({"output": str(OUTPUT_PATH), "counts": counts}, ensure_ascii=False))


if __name__ == "__main__":
    main()
