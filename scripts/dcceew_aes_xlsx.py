#!/usr/bin/env python3

import argparse
import json
import re
import zipfile
from collections import defaultdict
from pathlib import Path
import xml.etree.ElementTree as ET


NS = {
    "a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
}
STATE_CODES = {"NSW", "VIC", "QLD", "WA", "SA", "TAS", "NT", "AUS"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DCCEEW AES workbook parser")
    parser.add_argument("command", choices=["metadata", "resolve"])
    parser.add_argument("--xlsx", required=True)
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--agency-id", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--name", required=True)
    parser.add_argument("--description", required=True)
    parser.add_argument("--curation-json", required=True)
    parser.add_argument("--data-key")
    parser.add_argument("--detail", default="full")
    return parser.parse_args()


def column_letters(cell_ref: str) -> str:
    out = []
    for ch in cell_ref:
        if ch.isalpha():
            out.append(ch)
        else:
            break
    return "".join(out)


def column_number(column_ref: str) -> int:
    value = 0
    for ch in str(column_ref or "").upper():
        if not ("A" <= ch <= "Z"):
            break
        value = value * 26 + (ord(ch) - ord("A") + 1)
    return value


def parse_float(value: str):
    text = str(value or "").strip().replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def normalize_code(value: str) -> str:
    code = re.sub(r"[^A-Za-z0-9]+", "_", str(value or "").strip().upper()).strip("_")
    return code or "UNKNOWN"


def load_workbook(xlsx_path: Path):
    with zipfile.ZipFile(xlsx_path) as workbook:
        shared_strings = []
        if "xl/sharedStrings.xml" in workbook.namelist():
            root = ET.fromstring(workbook.read("xl/sharedStrings.xml"))
            for item in root:
                text = "".join(
                    node.text or ""
                    for node in item.iter("{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t")
                )
                shared_strings.append(text)

        rel_root = ET.fromstring(workbook.read("xl/_rels/workbook.xml.rels"))
        relationships = {
            item.attrib["Id"]: item.attrib["Target"]
            for item in rel_root
            if item.attrib.get("Id")
        }
        wb_root = ET.fromstring(workbook.read("xl/workbook.xml"))
        sheets = {}
        for sheet in wb_root.find("a:sheets", NS):
            name = sheet.attrib.get("name", "")
            rid = sheet.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
            target = relationships.get(rid, "")
            sheet_path = f"xl/{target}"
            sheet_root = ET.fromstring(workbook.read(sheet_path))
            rows = []
            for row in sheet_root.findall(".//a:sheetData/a:row", NS):
                row_values = {}
                for cell in row.findall("a:c", NS):
                    ref = cell.attrib.get("r", "")
                    col = column_letters(ref)
                    cell_type = cell.attrib.get("t")
                    value_node = cell.find("a:v", NS)
                    value = ""
                    if value_node is not None and value_node.text is not None:
                        if cell_type == "s":
                            value = shared_strings[int(value_node.text)]
                        else:
                            value = value_node.text
                    row_values[col] = value
                rows.append(row_values)
            sheets[name] = rows
    return sheets


def find_header_row(rows: list[dict]) -> int:
    for idx, row in enumerate(rows):
        b_value = str(row.get("B", "")).strip()
        data_cells = [
            str(value).strip()
            for col, value in row.items()
            if column_number(col) >= column_number("C") and str(value).strip()
        ]
        if not b_value and len(data_cells) >= 2:
            return idx
    raise ValueError("Unable to identify header row")


def detect_column_dimension(header_values: list[str]) -> str:
    cleaned = [value.strip() for value in header_values if value.strip()]
    if cleaned and all(re.search(r"\d{4}", value) for value in cleaned):
        return "TIME_PERIOD"
    if cleaned and all(value in STATE_CODES for value in cleaned):
        return "REGION"
    return "COLUMN_KEY"


def infer_sheet_region(sheet_name: str):
    prefix = str(sheet_name or "").split(" ")[0].strip().upper()
    if prefix in STATE_CODES:
        return "Australia" if prefix == "AUS" else prefix
    return None


def infer_period_basis(sheet_name: str):
    upper = str(sheet_name or "").upper()
    if upper.endswith("FY"):
        return "financial_year"
    if upper.endswith("CY"):
        return "calendar_year"
    return None


def sheet_title(rows: list[dict]) -> str:
    for index in (1, 0):
        if index < len(rows):
            value = str(rows[index].get("B", "")).strip()
            if value:
                return value
    return ""


def extract_sheet_records(sheet_name: str, rows: list[dict], sheet_group_id: str):
    header_idx = find_header_row(rows)
    header_row = rows[header_idx]
    unit_row = rows[header_idx + 1] if header_idx + 1 < len(rows) else {}
    columns = sorted(
        (col for col in header_row.keys() if column_number(col) >= column_number("C")),
        key=column_number,
    )
    header_values = [str(header_row.get(col, "")).strip() for col in columns]
    column_dimension = detect_column_dimension(header_values)
    section = None
    records = []

    for row in rows[header_idx + 2 :]:
        label = str(row.get("B", "")).strip()
        row_values = [str(row.get(col, "")).strip() for col in columns]

        if label.startswith("Notes:"):
            break
        if not label and not any(row_values):
            continue
        if label and not any(row_values):
            section = label
            continue
        if not label:
            continue

        for col, column_value in zip(columns, header_values):
            value = parse_float(row.get(col, ""))
            if value is None:
                continue
            record = {
                "sheet_group": sheet_group_id,
                "sheet": sheet_name,
                "sheet_title": sheet_title(rows),
                "section": section,
                "category": label,
                "column_dimension": column_dimension,
                "column_value": column_value,
                "unit": str(unit_row.get(col, "")).strip() or None,
                "value": value,
            }
            region = infer_sheet_region(sheet_name)
            if region and column_dimension != "REGION":
                record["region"] = region
            period_basis = infer_period_basis(sheet_name)
            if period_basis:
                record["period_basis"] = period_basis
            records.append(record)
    return records


def build_metadata(args: argparse.Namespace, curation: dict, workbook_sheets: dict) -> dict:
    sheet_groups = curation.get("sheetGroups") or []
    ignored_sheets = curation.get("ignoredSheets") or []
    group_codes = [
        {
            "id": item["id"],
            "name": item["id"],
            "description": item.get("description", ""),
        }
        for item in sheet_groups
    ]
    sheet_codes = []
    for item in sheet_groups:
        for sheet_name in item.get("sheets", []):
            if sheet_name in workbook_sheets:
                sheet_codes.append(
                    {
                        "id": normalize_code(sheet_name),
                        "name": sheet_name,
                        "description": sheet_title(workbook_sheets[sheet_name]),
                    }
                )
    concepts = [
        {
            "id": "DATA_KEY",
            "name": "Custom retrieval key",
            "description": "Use dataKey equal to one of the curated SHEET_GROUP ids to retrieve a grouped workbook slice.",
        },
        {
            "id": "SOURCE_URL",
            "name": "Source workbook URL",
            "description": args.description,
        },
    ]
    if ignored_sheets:
        concepts.append(
            {
                "id": "IGNORED_SHEETS",
                "name": "Ignored sheets",
                "description": ", ".join(ignored_sheets),
            }
        )

    return {
        "dataStructure": {
            "id": args.dataset_id,
            "agencyID": args.agency_id,
            "version": args.version,
            "name": args.name,
            "description": (
                f"{args.description} Retrieve using dataKey equal to one of the curated "
                "sheet group ids such as national_financial_year, state_financial_year, "
                "bioenergy_breakdown_financial_year, or national_calendar_year."
            ),
        },
        "dimensions": [
            {
                "id": "SHEET_GROUP",
                "position": 1,
                "conceptId": "SHEET_GROUP",
                "codelist": {"id": "SHEET_GROUPS"},
            },
            {
                "id": "SHEET",
                "position": 2,
                "conceptId": "SHEET",
                "codelist": {"id": "SHEETS"},
            },
        ],
        "attributes": [
            {
                "id": "UNIT",
                "attachmentLevel": "Observation",
                "conceptId": "UNIT",
            }
        ],
        "codelists": [
            {
                "id": "SHEET_GROUPS",
                "name": "Curated sheet groups",
                "codes": group_codes,
            },
            {
                "id": "SHEETS",
                "name": "Workbook sheets",
                "codes": sheet_codes,
            },
        ],
        "concepts": concepts,
    }


def select_group(data_key: str, curation: dict) -> tuple[str, list[str]]:
    sheet_groups = curation.get("sheetGroups") or []
    if not data_key or data_key == "all":
        first = sheet_groups[0]
        return first["id"], first.get("sheets", [])
    for item in sheet_groups:
        if item.get("id") == data_key:
            return item["id"], item.get("sheets", [])
        if data_key in item.get("sheets", []):
            return item["id"], [data_key]
    raise ValueError(f"Unknown custom dataKey '{data_key}'")


def build_resolved_dataset(args: argparse.Namespace, curation: dict, workbook_sheets: dict) -> dict:
    group_id, target_sheets = select_group(args.data_key or "all", curation)
    records = []
    for sheet_name in target_sheets:
        rows = workbook_sheets.get(sheet_name)
        if not rows:
            continue
        records.extend(extract_sheet_records(sheet_name, rows, group_id))
    if not records:
        raise ValueError(f"No records extracted for dataKey '{args.data_key or 'all'}'")

    dimensions_lookup = defaultdict(dict)
    series_map = {}

    for record in records:
        dimensions_lookup["SHEET_GROUP"][record["sheet_group"]] = record["sheet_group"]
        dimensions_lookup["SHEET"][record["sheet"]] = record["sheet"]
        if record.get("section"):
            dimensions_lookup["SECTION"][record["section"]] = record["section"]
        dimensions_lookup["CATEGORY"][record["category"]] = record["category"]
        if record.get("region"):
            dimensions_lookup["REGION"][record["region"]] = record["region"]
        if record.get("period_basis"):
            dimensions_lookup["PERIOD_BASIS"][record["period_basis"]] = record["period_basis"]

        series_dims = {
            "SHEET_GROUP": {"code": record["sheet_group"], "label": record["sheet_group"]},
            "SHEET": {"code": record["sheet"], "label": record["sheet"]},
            "CATEGORY": {"code": normalize_code(record["category"]), "label": record["category"]},
        }
        if record.get("section"):
            series_dims["SECTION"] = {"code": normalize_code(record["section"]), "label": record["section"]}
        if record.get("region"):
            series_dims["REGION"] = {"code": normalize_code(record["region"]), "label": record["region"]}
        if record.get("period_basis"):
            series_dims["PERIOD_BASIS"] = {
                "code": normalize_code(record["period_basis"]),
                "label": record["period_basis"],
            }

        series_key_parts = [
            record["sheet_group"],
            record["sheet"],
            record.get("region") or "",
            record.get("section") or "",
            record["category"],
        ]
        series_key = "|".join(series_key_parts)
        series = series_map.setdefault(
            series_key,
            {
                "seriesKey": series_key,
                "dimensions": series_dims,
                "observations": [],
            },
        )

        obs_dims = {
            record["column_dimension"]: {
                "code": normalize_code(record["column_value"]),
                "label": record["column_value"],
            }
        }
        dimensions_lookup[record["column_dimension"]][record["column_value"]] = record["column_value"]

        observation = {
            "observationKey": record["column_value"],
            "value": record["value"],
            "dimensions": obs_dims,
        }
        if record.get("unit"):
            observation["attributes"] = {"UNIT": record["unit"]}
            dimensions_lookup["UNIT"][record["unit"]] = record["unit"]
        series["observations"].append(observation)

    return {
        "dataset": {
            "id": args.dataset_id,
            "agencyID": args.agency_id,
            "version": args.version,
            "name": args.name,
            "description": args.description,
        },
        "query": {
            "dataKey": args.data_key or "all",
            "detail": args.detail,
        },
        "dimensions": dict(dimensions_lookup),
        "observationCount": sum(len(item["observations"]) for item in series_map.values()),
        "series": list(series_map.values()),
    }


def main() -> None:
    args = parse_args()
    curation = json.loads(args.curation_json)
    workbook_sheets = load_workbook(Path(args.xlsx))

    if args.command == "metadata":
        print(json.dumps(build_metadata(args, curation, workbook_sheets)))
        return

    print(json.dumps(build_resolved_dataset(args, curation, workbook_sheets)))


if __name__ == "__main__":
    main()
