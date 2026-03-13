from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional


BASE_PATH = Path(__file__).resolve().parents[2]
CATALOG_PATH = BASE_PATH / "CURATED_ABS_CATALOG.txt"
STRUCTURES_PATH = BASE_PATH / "CURATED_ABS_STRUCTURES.txt"
CATALOG_AI_PATH = BASE_PATH / "CURATED_ABS_CATALOG_AI.txt"
STRUCTURES_AI_PATH = BASE_PATH / "CURATED_ABS_STRUCTURES_AI.txt"


def _load_json_array(path: Path, *, label: str) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise RuntimeError(f"{label} at {path} must be a JSON array.")

    return [
        item
        for item in raw
        if isinstance(item, dict) and str(item.get("dataset_id") or "").strip()
    ]


@lru_cache(maxsize=1)
def _load_catalog() -> List[Dict[str, Any]]:
    return _load_json_array(CATALOG_PATH, label="Curated ABS catalog")


@lru_cache(maxsize=1)
def _load_catalog_ai() -> List[Dict[str, Any]]:
    return _load_json_array(CATALOG_AI_PATH, label="AI curated ABS catalog")


@lru_cache(maxsize=1)
def _load_structures() -> List[Dict[str, Any]]:
    return _load_json_array(STRUCTURES_PATH, label="Curated ABS structures")


@lru_cache(maxsize=1)
def _load_structures_ai() -> List[Dict[str, Any]]:
    return _load_json_array(STRUCTURES_AI_PATH, label="AI curated ABS structures")


def _merge_named_list(
    base_items: Any,
    overlay_items: Any,
    *,
    key_field: str,
) -> Any:
    if not isinstance(base_items, list) or not isinstance(overlay_items, list):
        return overlay_items if overlay_items is not None else base_items

    merged: List[Any] = []
    index_by_key: Dict[str, int] = {}

    for item in base_items:
        copied = dict(item) if isinstance(item, dict) else item
        merged.append(copied)
        if isinstance(copied, dict):
            key = str(copied.get(key_field) or "").strip()
            if key:
                index_by_key[key] = len(merged) - 1

    for item in overlay_items:
        copied = dict(item) if isinstance(item, dict) else item
        if not isinstance(copied, dict):
            merged.append(copied)
            continue
        key = str(copied.get(key_field) or "").strip()
        if not key or key not in index_by_key:
            merged.append(copied)
            if key:
                index_by_key[key] = len(merged) - 1
            continue
        base_item = merged[index_by_key[key]]
        if isinstance(base_item, dict):
            merged[index_by_key[key]] = _merge_overlay_dict(base_item, copied)
        else:
            merged[index_by_key[key]] = copied
    return merged


def _merge_overlay_dict(base_entry: Dict[str, Any], overlay_entry: Dict[str, Any]) -> Dict[str, Any]:
    merged: Dict[str, Any] = dict(base_entry)
    for key, overlay_value in overlay_entry.items():
        if overlay_value is None:
            continue
        base_value = merged.get(key)
        if isinstance(base_value, dict) and isinstance(overlay_value, dict):
            merged[key] = _merge_overlay_dict(base_value, overlay_value)
            continue
        if key == "query_templates":
            merged[key] = _merge_named_list(base_value, overlay_value, key_field="template_id")
            continue
        if key == "measures":
            merged[key] = _merge_named_list(base_value, overlay_value, key_field="measure_id")
            continue
        if key == "data_items":
            merged[key] = _merge_named_list(base_value, overlay_value, key_field="data_item_id")
            continue
        merged[key] = overlay_value
    return merged


def _merge_by_dataset_id(
    base_entries: List[Dict[str, Any]],
    overlay_entries: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = [dict(item) for item in base_entries]
    seen_ids = {
        str(item.get("dataset_id") or "").strip()
        for item in merged
        if str(item.get("dataset_id") or "").strip()
    }
    for item in overlay_entries:
        dataset_id = str(item.get("dataset_id") or "").strip()
        if not dataset_id:
            continue
        if dataset_id in seen_ids:
            for index, merged_item in enumerate(merged):
                if str(merged_item.get("dataset_id") or "").strip() == dataset_id:
                    merged[index] = _merge_overlay_dict(merged_item, dict(item))
                    break
            continue
        merged.append(dict(item))
        seen_ids.add(dataset_id)
    return sorted(merged, key=lambda item: str(item.get("dataset_id") or ""))


def list_curated_datasets() -> List[Dict[str, Any]]:
    return _merge_by_dataset_id(_load_catalog(), _load_catalog_ai())


def list_ai_curated_datasets() -> List[Dict[str, Any]]:
    return [dict(item) for item in _load_catalog_ai()]


def get_curated_dataset(dataset_id: str) -> Optional[Dict[str, Any]]:
    target = str(dataset_id or "").strip()
    catalog_entry = None
    for item in list_curated_datasets():
        if str(item.get("dataset_id") or "").strip() == target:
            catalog_entry = dict(item)
            break

    for item in _merge_by_dataset_id(_load_structures(), _load_structures_ai()):
        if str(item.get("dataset_id") or "").strip() == target:
            merged = dict(item)
            if catalog_entry is not None:
                merged["title"] = str(catalog_entry.get("title") or merged.get("title") or "").strip()
                merged["description"] = str(catalog_entry.get("description") or "").strip()
                if catalog_entry.get("data_shape") is not None:
                    merged["data_shape"] = catalog_entry.get("data_shape")
                merged["curation_source"] = str(catalog_entry.get("curation_source") or merged.get("curation_source") or "").strip()
            return merged
    return None


def clear_curated_cache() -> None:
    _load_catalog.cache_clear()
    _load_catalog_ai.cache_clear()
    _load_structures.cache_clear()
    _load_structures_ai.cache_clear()


def write_curated_files(
    *,
    catalog_entries: List[Dict[str, Any]],
    structure_entries: List[Dict[str, Any]],
) -> None:
    CATALOG_PATH.write_text(
        json.dumps(catalog_entries, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    STRUCTURES_PATH.write_text(
        json.dumps(structure_entries, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    clear_curated_cache()


def write_ai_curated_files(
    *,
    catalog_entries: List[Dict[str, Any]],
    structure_entries: List[Dict[str, Any]],
) -> None:
    CATALOG_AI_PATH.write_text(
        json.dumps(catalog_entries, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    STRUCTURES_AI_PATH.write_text(
        json.dumps(structure_entries, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    clear_curated_cache()


def upsert_curated_dataset(structure_entry: Dict[str, Any]) -> Dict[str, Any]:
    dataset_id = str(structure_entry.get("dataset_id") or "").strip()
    if not dataset_id:
        raise RuntimeError("Curated dataset update requires dataset_id")

    existing_catalog_entry = None
    for entry in list_curated_datasets():
        if str(entry.get("dataset_id") or "").strip() == dataset_id:
            existing_catalog_entry = dict(entry)
            break

    catalog_entry = {
        "dataset_id": dataset_id,
        "title": str(
            structure_entry.get("title")
            or (existing_catalog_entry or {}).get("title")
            or ""
        ).strip(),
        "description": str(
            (existing_catalog_entry or {}).get("description")
            or structure_entry.get("description")
            or ""
        ).strip(),
    }

    catalog_entries = list_curated_datasets()
    structure_entries = _load_structures()

    catalog_replaced = False
    for index, entry in enumerate(catalog_entries):
        if str(entry.get("dataset_id") or "").strip() == dataset_id:
            catalog_entries[index] = catalog_entry
            catalog_replaced = True
            break
    if not catalog_replaced:
        catalog_entries.append(catalog_entry)

    structure_replaced = False
    for index, entry in enumerate(structure_entries):
        if str(entry.get("dataset_id") or "").strip() == dataset_id:
            structure_entries[index] = structure_entry
            structure_replaced = True
            break
    if not structure_replaced:
        structure_entries.append(structure_entry)

    catalog_entries = sorted(catalog_entries, key=lambda item: str(item.get("dataset_id") or ""))
    structure_entries = sorted(
        [dict(item) for item in structure_entries],
        key=lambda item: str(item.get("dataset_id") or ""),
    )
    write_curated_files(
        catalog_entries=[dict(item) for item in catalog_entries],
        structure_entries=structure_entries,
    )
    return structure_entry


def upsert_ai_curated_dataset(structure_entry: Dict[str, Any]) -> Dict[str, Any]:
    dataset_id = str(structure_entry.get("dataset_id") or "").strip()
    if not dataset_id:
        raise RuntimeError("AI curated dataset update requires dataset_id")

    base_catalog_entry = None
    for entry in _load_catalog():
        if str(entry.get("dataset_id") or "").strip() == dataset_id:
            base_catalog_entry = dict(entry)
            break

    existing_ai_catalog_entry = None
    for entry in _load_catalog_ai():
        if str(entry.get("dataset_id") or "").strip() == dataset_id:
            existing_ai_catalog_entry = dict(entry)
            break

    catalog_entry = {
        "dataset_id": dataset_id,
        "title": str(
            structure_entry.get("title")
            or (existing_ai_catalog_entry or {}).get("title")
            or (base_catalog_entry or {}).get("title")
            or ""
        ).strip(),
        "description": str(
            structure_entry.get("description")
            or (existing_ai_catalog_entry or {}).get("description")
            or (base_catalog_entry or {}).get("description")
            or ""
        ).strip(),
        "data_shape": (
            structure_entry.get("data_shape")
            or (existing_ai_catalog_entry or {}).get("data_shape")
            or (base_catalog_entry or {}).get("data_shape")
        ),
        "curation_source": "ai_overlay",
    }

    structure_entry = dict(structure_entry)
    structure_entry["curation_source"] = "ai_overlay"

    catalog_entries = list_ai_curated_datasets()
    structure_entries = _load_structures_ai()

    catalog_replaced = False
    for index, entry in enumerate(catalog_entries):
        if str(entry.get("dataset_id") or "").strip() == dataset_id:
            catalog_entries[index] = catalog_entry
            catalog_replaced = True
            break
    if not catalog_replaced:
        catalog_entries.append(catalog_entry)

    structure_replaced = False
    for index, entry in enumerate(structure_entries):
        if str(entry.get("dataset_id") or "").strip() == dataset_id:
            structure_entries[index] = structure_entry
            structure_replaced = True
            break
    if not structure_replaced:
        structure_entries.append(structure_entry)

    catalog_entries = sorted(catalog_entries, key=lambda item: str(item.get("dataset_id") or ""))
    structure_entries = sorted(
        [dict(item) for item in structure_entries],
        key=lambda item: str(item.get("dataset_id") or ""),
    )
    write_ai_curated_files(
        catalog_entries=[dict(item) for item in catalog_entries],
        structure_entries=structure_entries,
    )
    return structure_entry
