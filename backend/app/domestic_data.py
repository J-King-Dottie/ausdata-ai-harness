from __future__ import annotations

import json
import logging
import re
import subprocess
import tempfile
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

from .config import get_settings


logger = logging.getLogger(__name__)
settings = get_settings()

ROOT = Path(__file__).resolve().parents[2]
ABS_CACHE_PATH = ROOT / "ABS_DATAFLOWS_FULL.json"
MANUAL_DEFINITIONS_PATH = ROOT / "MANUAL_SOURCE_DEFINITIONS.json"
DCCEEW_SCRIPT_PATH = ROOT / "scripts" / "dcceew_aes_xlsx.py"
RBA_SCRIPT_PATH = ROOT / "scripts" / "rba_tables_csv.py"


def _local_name(tag: str) -> str:
    return str(tag or "").split("}", 1)[-1]


def _direct_children(node: Optional[ET.Element], name: str) -> List[ET.Element]:
    if node is None:
        return []
    return [child for child in list(node) if _local_name(child.tag) == name]


def _first_child(node: Optional[ET.Element], name: str) -> Optional[ET.Element]:
    children = _direct_children(node, name)
    return children[0] if children else None


def _iter_descendants(node: Optional[ET.Element], name: str) -> List[ET.Element]:
    if node is None:
        return []
    return [child for child in node.iter() if _local_name(child.tag) == name]


def _localized_text(node: Optional[ET.Element], child_name: str) -> str:
    matches = _direct_children(node, child_name)
    if not matches:
        return ""
    for child in matches:
        lang = child.attrib.get("{http://www.w3.org/XML/1998/namespace}lang") or child.attrib.get("lang") or ""
        if str(lang).strip().lower() == "en":
            return " ".join((child.text or "").split())
    return " ".join((matches[0].text or "").split())


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


class ABSApiClient:
    def __init__(self) -> None:
        self._client = httpx.Client(
            base_url=settings.abs_api_base.rstrip("/"),
            timeout=120.0,
            headers={"Accept": "application/xml"},
            follow_redirects=True,
        )

    def get_dataflows_xml(self, agency_id: str = "ABS") -> str:
        response = self._client.get(
            f"/rest/dataflow/{agency_id}",
            headers={"Accept": "application/vnd.sdmx.structure+xml;version=2.1"},
        )
        response.raise_for_status()
        return response.text

    def get_data_structure_xml(
        self,
        agency_id: str,
        structure_id: str,
        version: str,
        references: str = "children",
        detail: str = "full",
    ) -> str:
        response = self._client.get(
            f"/rest/datastructure/{agency_id}/{structure_id}/{version}",
            params={"references": references, "detail": detail},
            headers={"Accept": "application/vnd.sdmx.structure+xml;version=2.1"},
        )
        response.raise_for_status()
        return response.text

    def get_data(
        self,
        dataflow_id: str,
        data_key: str = "all",
        *,
        start_period: str = "",
        end_period: str = "",
        detail: str = "",
        dimension_at_observation: str = "",
        format_name: str = "jsondata",
    ) -> Any:
        params: Dict[str, str] = {"format": format_name}
        if start_period:
            params["startPeriod"] = start_period
        if end_period:
            params["endPeriod"] = end_period
        if detail:
            params["detail"] = detail
        if dimension_at_observation:
            params["dimensionAtObservation"] = dimension_at_observation
        response = self._client.get(
            f"/rest/data/{dataflow_id}/{data_key}",
            params=params,
            headers={"Accept": "application/vnd.sdmx.data+json" if format_name == "jsondata" else "application/xml"},
        )
        response.raise_for_status()
        if format_name == "jsondata":
            return response.json()
        return response.text


class CustomDomesticService:
    def __init__(self, script_path: Path, flow_type: str, suffix: str) -> None:
        self.script_path = script_path
        self.flow_type = flow_type
        self.suffix = suffix

    def supports(self, flow: Dict[str, Any]) -> bool:
        return _clean_text(flow.get("flowType")) == self.flow_type

    def get_metadata(self, flow: Dict[str, Any]) -> Dict[str, Any]:
        source_path = self._download_to_temp(flow)
        try:
            return self._run_script("metadata", flow, source_path)
        finally:
            source_path.unlink(missing_ok=True)

    def resolve(self, flow: Dict[str, Any], *, data_key: str = "all", detail: str = "full") -> Dict[str, Any]:
        source_path = self._download_to_temp(flow)
        try:
            return self._run_script("resolve", flow, source_path, data_key=data_key, detail=detail)
        finally:
            source_path.unlink(missing_ok=True)

    def _download_to_temp(self, flow: Dict[str, Any]) -> Path:
        source_url = _clean_text(flow.get("sourceUrl"))
        if not source_url:
            raise RuntimeError(f"Custom flow {_clean_text(flow.get('id'))} is missing sourceUrl.")
        suffix = self.suffix
        handle = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        temp_path = Path(handle.name)
        handle.close()
        try:
            with httpx.Client(timeout=120.0, follow_redirects=True, headers={"User-Agent": "Mozilla/5.0"}) as client:
                with client.stream("GET", source_url) as response:
                    response.raise_for_status()
                    with temp_path.open("wb") as out:
                        for chunk in response.iter_bytes():
                            out.write(chunk)
        except Exception as exc:
            logger.info("Primary custom source download failed; retrying with curl", extra={"source_url": source_url, "error": str(exc)})
            temp_path.unlink(missing_ok=True)
            curl_result = subprocess.run(
                ["curl", "-sSL", source_url, "-o", str(temp_path)],
                cwd=str(ROOT),
                capture_output=True,
                text=True,
                check=False,
            )
            if curl_result.returncode != 0:
                temp_path.unlink(missing_ok=True)
                raise RuntimeError((curl_result.stderr or curl_result.stdout or str(exc)).strip() or f"Failed to download {source_url}")
        return temp_path

    def _run_script(
        self,
        command: str,
        flow: Dict[str, Any],
        source_path: Path,
        *,
        data_key: str = "all",
        detail: str = "full",
    ) -> Dict[str, Any]:
        path_flag = "--xlsx" if self.suffix == ".xlsx" else "--csv"
        args = [
            settings.python_binary,
            str(self.script_path),
            command,
            path_flag,
            str(source_path),
            "--dataset-id",
            _clean_text(flow.get("id")),
            "--agency-id",
            _clean_text(flow.get("agencyID")),
            "--version",
            _clean_text(flow.get("version")),
            "--name",
            _clean_text(flow.get("name")),
            "--description",
            _clean_text(flow.get("description")),
            "--curation-json",
            json.dumps(flow.get("curation") or {}),
        ]
        if command == "resolve":
            args.extend(["--data-key", data_key or "all", "--detail", detail or "full"])
        result = subprocess.run(
            args,
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            message = (result.stderr or result.stdout or "").strip()
            raise RuntimeError(message or f"Custom domestic script failed: {self.script_path.name}")
        parsed = json.loads(result.stdout)
        if not isinstance(parsed, dict):
            raise RuntimeError(f"Custom domestic script returned unexpected payload type: {self.script_path.name}")
        return parsed


class DomesticDataService:
    def __init__(self) -> None:
        self.api_client = ABSApiClient()
        self.dcceew_service = CustomDomesticService(DCCEEW_SCRIPT_PATH, "dcceew_aes_xlsx", ".xlsx")
        self.rba_service = CustomDomesticService(RBA_SCRIPT_PATH, "rba_tables_csv", ".csv")

    def get_data_flows(self, force_refresh: bool = False) -> List[Dict[str, Any]]:
        if force_refresh or not ABS_CACHE_PATH.exists():
            flows = self._fetch_abs_dataflows()
            payload = {
                "lastUpdated": datetime.now(timezone.utc).isoformat(),
                "flows": flows,
            }
            ABS_CACHE_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        payload = _load_json(ABS_CACHE_PATH)
        abs_flows = payload.get("flows") if isinstance(payload, dict) else None
        if not isinstance(abs_flows, list):
            legacy = payload.get("dataflows") if isinstance(payload, dict) else None
            abs_flows = legacy if isinstance(legacy, list) else []
        custom_flows = self._load_custom_flows()
        return [*custom_flows, *abs_flows]

    def resolve_flow(self, dataflow_identifier: str, force_refresh: bool = False) -> Dict[str, Any]:
        parsed = self.parse_dataflow_identifier(dataflow_identifier)
        flows = self.get_data_flows(force_refresh)
        candidates = [
            flow
            for flow in flows
            if _clean_text(flow.get("id")) == parsed["dataflowId"]
            and (not parsed["agencyId"] or _clean_text(flow.get("agencyID")) == parsed["agencyId"])
        ]
        if not candidates:
            raise RuntimeError(f"Unknown dataflow identifier: {dataflow_identifier}")
        if parsed.get("version"):
            for flow in candidates:
                if _clean_text(flow.get("version")) == parsed["version"]:
                    return flow
            raise RuntimeError(f"Dataflow {dataflow_identifier} not found for version {parsed['version']}")
        latest = self.select_latest_flow(candidates)
        if latest is None:
            raise RuntimeError(f"Unable to resolve latest version for dataflow {dataflow_identifier}")
        return latest

    def get_data_structure_for_dataflow(self, dataflow_identifier: str, force_refresh: bool = False) -> Dict[str, Any]:
        flow = self.resolve_flow(dataflow_identifier, force_refresh)
        if self.dcceew_service.supports(flow):
            return self.dcceew_service.get_metadata(flow)
        if self.rba_service.supports(flow):
            return self.rba_service.get_metadata(flow)
        structure = flow.get("structure") if isinstance(flow.get("structure"), dict) else {}
        structure_id = _clean_text(structure.get("id")) or _clean_text(flow.get("id"))
        agency_id = _clean_text(structure.get("agencyID")) or _clean_text(flow.get("agencyID")) or "ABS"
        version = _clean_text(structure.get("version")) or _clean_text(flow.get("version"))
        xml_text = self.api_client.get_data_structure_xml(agency_id, structure_id, version)
        metadata = self._extract_data_structure(xml_text)
        metadata["dataflow"] = flow
        return metadata

    def resolve_dataset(
        self,
        dataset_id: str,
        *,
        data_key: str = "",
        start_period: str = "",
        end_period: str = "",
        detail: str = "",
        dimension_at_observation: str = "",
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        flow = self.resolve_flow(dataset_id, force_refresh)
        clean_data_key = _clean_text(data_key) or "all"
        clean_detail = _clean_text(detail) or "full"
        if self.dcceew_service.supports(flow):
            return self.dcceew_service.resolve(flow, data_key=clean_data_key, detail=clean_detail)
        if self.rba_service.supports(flow):
            return self.rba_service.resolve(flow, data_key=clean_data_key, detail=clean_detail)
        full_dataset_id = self.format_dataflow_identifier(flow)
        clean_dimension = _clean_text(dimension_at_observation) or "TIME_PERIOD"
        payload = self.api_client.get_data(
            full_dataset_id,
            clean_data_key,
            start_period=_clean_text(start_period),
            end_period=_clean_text(end_period),
            detail=clean_detail,
            dimension_at_observation=clean_dimension,
            format_name="jsondata",
        )
        return self._transform_json_data(
            flow,
            {
                "dataKey": clean_data_key,
                "startPeriod": _clean_text(start_period),
                "endPeriod": _clean_text(end_period),
                "detail": clean_detail,
                "dimensionAtObservation": clean_dimension,
            },
            payload,
        )

    def _fetch_abs_dataflows(self) -> List[Dict[str, Any]]:
        root = ET.fromstring(self.api_client.get_dataflows_xml("ABS"))
        flows: List[Dict[str, Any]] = []
        for flow in _iter_descendants(root, "Dataflow"):
            flow_id = _clean_text(flow.attrib.get("id"))
            agency_id = _clean_text(flow.attrib.get("agencyID")) or "ABS"
            version = _clean_text(flow.attrib.get("version"))
            if not flow_id:
                continue
            item: Dict[str, Any] = {
                "id": flow_id,
                "agencyID": agency_id,
                "version": version,
                "name": _localized_text(flow, "Name"),
                "description": _localized_text(flow, "Description"),
            }
            structure_node = _first_child(flow, "Structure")
            structure_ref = _first_child(structure_node, "Ref")
            if structure_ref is not None:
                item["structure"] = {
                    "id": _clean_text(structure_ref.attrib.get("id")),
                    "version": _clean_text(structure_ref.attrib.get("version")),
                    "agencyID": _clean_text(structure_ref.attrib.get("agencyID")),
                }
            flows.append(item)
        return flows

    def _load_custom_flows(self) -> List[Dict[str, Any]]:
        if not MANUAL_DEFINITIONS_PATH.exists():
            return []
        payload = _load_json(MANUAL_DEFINITIONS_PATH)
        flows = payload.get("flows") if isinstance(payload, dict) else None
        if isinstance(flows, list):
            return flows
        legacy = payload.get("dataflows") if isinstance(payload, dict) else None
        return legacy if isinstance(legacy, list) else []

    def _extract_data_structure(self, xml_text: str) -> Dict[str, Any]:
        root = ET.fromstring(xml_text)
        data_structure_node = None
        for node in _iter_descendants(root, "DataStructure"):
            if _clean_text(node.attrib.get("id")):
                data_structure_node = node
                break
        if data_structure_node is None:
            raise RuntimeError("No data structure found in ABS response.")
        dimensions = self._extract_dimensions(data_structure_node)
        attributes = self._extract_attributes(data_structure_node)
        codelists = self._extract_codelists(root)
        concepts = self._extract_concepts(root)
        return {
            "dataStructure": {
                "id": _clean_text(data_structure_node.attrib.get("id")),
                "agencyID": _clean_text(data_structure_node.attrib.get("agencyID")),
                "version": _clean_text(data_structure_node.attrib.get("version")),
                "name": _localized_text(data_structure_node, "Name"),
                "description": _localized_text(data_structure_node, "Description"),
            },
            "dimensions": dimensions,
            "attributes": attributes,
            "codelists": codelists,
            "concepts": concepts,
        }

    def _extract_dimensions(self, data_structure_node: ET.Element) -> List[Dict[str, Any]]:
        components = _first_child(data_structure_node, "DataStructureComponents")
        dimension_list = _first_child(components, "DimensionList")
        result: List[Dict[str, Any]] = []
        for index, dimension in enumerate(_direct_children(dimension_list, "Dimension"), start=1):
            concept_identity = _first_child(dimension, "ConceptIdentity")
            concept_ref = _first_child(concept_identity, "Ref")
            local_representation = _first_child(dimension, "LocalRepresentation")
            representation = _first_child(dimension, "Representation")
            enumeration = _first_child(local_representation, "Enumeration") or _first_child(representation, "Enumeration")
            enum_ref = _first_child(enumeration, "Ref")
            codelist = None
            if enum_ref is not None:
                codelist = {
                    "id": _clean_text(enum_ref.attrib.get("id")),
                    "agencyID": _clean_text(enum_ref.attrib.get("agencyID")),
                    "version": _clean_text(enum_ref.attrib.get("version")),
                }
            result.append(
                {
                    "id": _clean_text(dimension.attrib.get("id")),
                    "position": int(dimension.attrib.get("position") or index),
                    "conceptId": _clean_text(concept_ref.attrib.get("id")) if concept_ref is not None else "",
                    "role": _clean_text((_first_child(_first_child(dimension, "Role"), "Ref") or ET.Element("")).attrib.get("id")),
                    "codelist": codelist,
                }
            )
        return result

    def _extract_attributes(self, data_structure_node: ET.Element) -> List[Dict[str, Any]]:
        components = _first_child(data_structure_node, "DataStructureComponents")
        attribute_list = _first_child(components, "AttributeList")
        result: List[Dict[str, Any]] = []
        for attribute in _direct_children(attribute_list, "Attribute"):
            concept_identity = _first_child(attribute, "ConceptIdentity")
            concept_ref = _first_child(concept_identity, "Ref")
            local_representation = _first_child(attribute, "LocalRepresentation")
            representation = _first_child(attribute, "Representation")
            enumeration = _first_child(local_representation, "Enumeration") or _first_child(representation, "Enumeration")
            enum_ref = _first_child(enumeration, "Ref")
            codelist = None
            if enum_ref is not None:
                codelist = {
                    "id": _clean_text(enum_ref.attrib.get("id")),
                    "agencyID": _clean_text(enum_ref.attrib.get("agencyID")),
                    "version": _clean_text(enum_ref.attrib.get("version")),
                }
            attachment_level = _clean_text(attribute.attrib.get("attachmentLevel") or attribute.attrib.get("AttachmentLevel"))
            result.append(
                {
                    "id": _clean_text(attribute.attrib.get("id")),
                    "assignmentStatus": _clean_text(attribute.attrib.get("assignmentStatus")),
                    "attachmentLevel": attachment_level,
                    "conceptId": _clean_text(concept_ref.attrib.get("id")) if concept_ref is not None else "",
                    "codelist": codelist,
                    "relatedTo": self._extract_attribute_relationship(_first_child(attribute, "AttributeRelationship")) or None,
                }
            )
        return result

    def _extract_attribute_relationship(self, relationship_node: Optional[ET.Element]) -> List[str]:
        if relationship_node is None:
            return []
        related: List[str] = []
        for dimension in _direct_children(relationship_node, "Dimension"):
            ref = _first_child(dimension, "Ref")
            identifier = _clean_text((ref or dimension).attrib.get("id"))
            if identifier and identifier not in related:
                related.append(identifier)
        for group in _direct_children(relationship_node, "Group"):
            ref = _first_child(group, "Ref")
            identifier = _clean_text((ref or group).attrib.get("id"))
            if identifier and identifier not in related:
                related.append(identifier)
        primary_measure = _first_child(_first_child(relationship_node, "PrimaryMeasure"), "Ref")
        measure_id = _clean_text(primary_measure.attrib.get("id")) if primary_measure is not None else ""
        if measure_id and measure_id not in related:
            related.append(measure_id)
        if _first_child(relationship_node, "Observation") is not None and "OBSERVATION" not in related:
            related.append("OBSERVATION")
        return related

    def _extract_codelists(self, root: ET.Element) -> List[Dict[str, Any]]:
        result: List[Dict[str, Any]] = []
        for codelist in _iter_descendants(root, "Codelist"):
            codes: List[Dict[str, Any]] = []
            for code in _direct_children(codelist, "Code"):
                parent = _first_child(code, "Parent")
                parent_ref = _first_child(parent, "Ref")
                codes.append(
                    {
                        "id": _clean_text(code.attrib.get("id")),
                        "name": _localized_text(code, "Name"),
                        "description": _localized_text(code, "Description"),
                        "parentID": _clean_text((parent_ref or ET.Element("")).attrib.get("id")),
                    }
                )
            result.append(
                {
                    "id": _clean_text(codelist.attrib.get("id")),
                    "agencyID": _clean_text(codelist.attrib.get("agencyID")),
                    "version": _clean_text(codelist.attrib.get("version")),
                    "name": _localized_text(codelist, "Name"),
                    "description": _localized_text(codelist, "Description"),
                    "codes": codes,
                }
            )
        return result

    def _extract_concepts(self, root: ET.Element) -> List[Dict[str, Any]]:
        result: List[Dict[str, Any]] = []
        for scheme in _iter_descendants(root, "ConceptScheme"):
            scheme_info = {
                "id": _clean_text(scheme.attrib.get("id")),
                "agencyID": _clean_text(scheme.attrib.get("agencyID")),
                "version": _clean_text(scheme.attrib.get("version")),
                "name": _localized_text(scheme, "Name"),
            }
            for concept in _direct_children(scheme, "Concept"):
                result.append(
                    {
                        "id": _clean_text(concept.attrib.get("id")),
                        "name": _localized_text(concept, "Name"),
                        "description": _localized_text(concept, "Description"),
                        "scheme": scheme_info,
                    }
                )
        return result

    def _transform_json_data(self, flow: Dict[str, Any], query: Dict[str, Any], payload: Any) -> Dict[str, Any]:
        errors = payload.get("errors") if isinstance(payload, dict) else None
        if isinstance(errors, list) and errors:
            raise RuntimeError(f"ABS API returned errors: {json.dumps(errors)}")
        data_envelope = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data_envelope, dict):
            raise RuntimeError("ABS response did not include a data section.")
        structures = data_envelope.get("structures")
        structure = structures[0] if isinstance(structures, list) and structures else None
        if not isinstance(structure, dict):
            raise RuntimeError("ABS response did not include structure metadata.")
        series_dimensions = self._to_list(((structure.get("dimensions") or {}).get("series") if isinstance(structure.get("dimensions"), dict) else None))
        observation_dimensions = self._to_list(((structure.get("dimensions") or {}).get("observation") if isinstance(structure.get("dimensions"), dict) else None))
        series_attribute_defs = self._to_list(((structure.get("attributes") or {}).get("series") if isinstance(structure.get("attributes"), dict) else None))
        observation_attribute_defs = self._to_list(((structure.get("attributes") or {}).get("observation") if isinstance(structure.get("attributes"), dict) else None))
        data_sets = self._to_list(data_envelope.get("dataSets"))
        dataset = data_sets[0] if data_sets else None
        if not isinstance(dataset, dict):
            raise RuntimeError("ABS response did not include dataset observations.")
        dimension_map = self._build_dimension_lookup([*series_dimensions, *observation_dimensions])
        series_groups: Dict[str, Dict[str, Any]] = {}
        observation_count = 0
        series_entries = dataset.get("series").items() if isinstance(dataset.get("series"), dict) else []
        for series_key, series_entry in series_entries:
            group = series_groups.setdefault(series_key, {"seriesKey": series_key, "observations": []})
            series_index_values = self._parse_key_indices(series_key, len(series_dimensions))
            series_coordinates = self._build_coordinate_record(series_dimensions, series_index_values)
            condensed_series_coordinates = self._to_condensed_coordinate_map(series_coordinates)
            if condensed_series_coordinates:
                group["dimensions"] = condensed_series_coordinates
            series_attributes = self._compact_attributes(
                self._map_attribute_values(
                    series_attribute_defs,
                    series_entry.get("attributes") if isinstance(series_entry, dict) else [],
                )
            )
            if series_attributes:
                group["attributes"] = series_attributes
            observations = series_entry.get("observations") if isinstance(series_entry, dict) else {}
            for observation_key, value_array in observations.items():
                observation_indices = self._parse_key_indices(observation_key, len(observation_dimensions))
                observation_coordinates = self._build_coordinate_record(observation_dimensions, observation_indices)
                condensed_observation_coordinates = self._to_condensed_coordinate_map(observation_coordinates)
                observation_attributes = self._compact_attributes(
                    self._map_attribute_values(
                        observation_attribute_defs,
                        value_array[1:] if isinstance(value_array, list) else [],
                    )
                )
                observation: Dict[str, Any] = {
                    "observationKey": observation_key,
                    "value": self._coerce_value(value_array[0] if isinstance(value_array, list) and value_array else value_array),
                }
                if condensed_observation_coordinates:
                    observation["dimensions"] = condensed_observation_coordinates
                if observation_attributes:
                    observation["attributes"] = observation_attributes
                group["observations"].append(observation)
                observation_count += 1
        if not series_groups:
            group = series_groups.setdefault("__all__", {"seriesKey": "__all__", "observations": []})
            for observation_key, value_array in (dataset.get("observations") or {}).items():
                observation_indices = self._parse_key_indices(observation_key, len(observation_dimensions))
                observation_coordinates = self._build_coordinate_record(observation_dimensions, observation_indices)
                condensed_observation_coordinates = self._to_condensed_coordinate_map(observation_coordinates)
                observation_attributes = self._compact_attributes(
                    self._map_attribute_values(
                        observation_attribute_defs,
                        value_array[1:] if isinstance(value_array, list) else [],
                    )
                )
                observation = {
                    "observationKey": observation_key,
                    "value": self._coerce_value(value_array[0] if isinstance(value_array, list) and value_array else value_array),
                }
                if condensed_observation_coordinates:
                    observation["dimensions"] = condensed_observation_coordinates
                if observation_attributes:
                    observation["attributes"] = observation_attributes
                group["observations"].append(observation)
                observation_count += 1
        series = sorted(series_groups.values(), key=lambda item: str(item.get("seriesKey") or ""))
        dimension_lookup = {key: value for key, value in dimension_map.items() if value}
        summary = {"dataKey": query.get("dataKey"), "detail": query.get("detail")}
        if query.get("startPeriod"):
            summary["startPeriod"] = query["startPeriod"]
        if query.get("endPeriod"):
            summary["endPeriod"] = query["endPeriod"]
        if query.get("dimensionAtObservation"):
            summary["dimensionAtObservation"] = query["dimensionAtObservation"]
        return {
            "dataset": {
                "id": _clean_text(flow.get("id")),
                "agencyID": _clean_text(flow.get("agencyID")),
                "version": _clean_text(flow.get("version")),
                "name": _clean_text(flow.get("name")),
                "description": _clean_text(flow.get("description")),
            },
            "query": summary,
            "dimensions": dimension_lookup,
            "observationCount": observation_count,
            "series": series,
        }

    def _build_dimension_lookup(self, dimensions: List[Any]) -> Dict[str, Dict[str, str]]:
        lookup: Dict[str, Dict[str, str]] = {}
        for dimension in dimensions:
            if not isinstance(dimension, dict):
                continue
            dimension_id = _clean_text(dimension.get("id"))
            values = self._to_list(dimension.get("values"))
            if not dimension_id or not values:
                continue
            registry = lookup.setdefault(dimension_id, {})
            for value in values:
                if not isinstance(value, dict):
                    continue
                code = _clean_text(value.get("id"))
                if not code or code in registry:
                    continue
                label = self._extract_name(value) or code
                registry[code] = label
        return lookup

    def _build_coordinate_record(self, dimensions: List[Any], indices: List[int]) -> Dict[str, Dict[str, Any]]:
        record: Dict[str, Dict[str, Any]] = {}
        for idx, dimension in enumerate(dimensions):
            if not isinstance(dimension, dict):
                continue
            index = indices[idx] if idx < len(indices) else 0
            dimension_id = _clean_text(dimension.get("id")) or f"DIM_{idx}"
            values = self._to_list(dimension.get("values"))
            value_meta = values[index] if index < len(values) else {}
            record[dimension_id] = {
                "code": _clean_text(value_meta.get("id")) or str(index),
                "label": self._extract_name(value_meta),
                "description": self._extract_description(value_meta),
            }
        return record

    def _to_condensed_coordinate_map(self, coordinates: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, str]]:
        result: Dict[str, Dict[str, str]] = {}
        for dimension_id, value in coordinates.items():
            code = _clean_text(value.get("code"))
            if not code:
                continue
            entry: Dict[str, str] = {"code": code}
            label = _clean_text(value.get("label"))
            if label and label != code:
                entry["label"] = label
            result[dimension_id] = entry
        return result

    def _map_attribute_values(self, definitions: List[Any], values: Any) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        raw_values = values if isinstance(values, list) else []
        for index, definition in enumerate(definitions):
            value = raw_values[index] if index < len(raw_values) else None
            if value in (None, ""):
                continue
            key = _clean_text(definition.get("id")) if isinstance(definition, dict) else f"ATTR_{index}"
            result[key] = self._lookup_value(definition.get("values") if isinstance(definition, dict) else [], value)
        return result

    def _lookup_value(self, options: Any, value: Any) -> Any:
        if not isinstance(options, list) or not options:
            return value
        value_as_string = str(value)
        for option in options:
            if isinstance(option, dict) and str(option.get("id")) == value_as_string:
                return self._extract_name(option) or value_as_string
        try:
            numeric_index = int(value)
        except Exception:
            numeric_index = -1
        if 0 <= numeric_index < len(options) and isinstance(options[numeric_index], dict):
            return self._extract_name(options[numeric_index]) or value
        return value

    def _coerce_value(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return value
        if isinstance(value, str):
            try:
                numeric = float(value)
                return int(numeric) if numeric.is_integer() else numeric
            except Exception:
                return value
        return value

    def _parse_key_indices(self, key: Any, expected_length: int) -> List[int]:
        if expected_length == 0:
            return []
        parts = str(key or "").split(":")
        indices: List[int] = []
        for part in parts:
            if part == "":
                continue
            try:
                indices.append(int(part))
            except Exception:
                indices.append(0)
        while len(indices) < expected_length:
            indices.append(0)
        return indices[:expected_length]

    def _compact_attributes(self, attributes: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        compact = {key: value for key, value in attributes.items() if value not in (None, "")}
        return compact or None

    def _extract_name(self, value: Any) -> str:
        if isinstance(value, str):
            return value
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, dict):
            for key in ("name", "label", "id"):
                candidate = value.get(key)
                if isinstance(candidate, str) and candidate:
                    return candidate
                if isinstance(candidate, dict):
                    english = candidate.get("en")
                    if isinstance(english, str) and english:
                        return english
                    for item in candidate.values():
                        if isinstance(item, str) and item:
                            return item
        return ""

    def _extract_description(self, value: Any) -> str:
        if isinstance(value, dict):
            description = value.get("description")
            if isinstance(description, str):
                return description
            descriptions = value.get("descriptions")
            if isinstance(descriptions, dict):
                english = descriptions.get("en")
                if isinstance(english, str) and english:
                    return english
                for item in descriptions.values():
                    if isinstance(item, str) and item:
                        return item
        return ""

    def _to_list(self, value: Any) -> List[Any]:
        if value is None:
            return []
        return value if isinstance(value, list) else [value]

    @staticmethod
    def format_dataflow_identifier(flow: Dict[str, Any]) -> str:
        return f"{_clean_text(flow.get('agencyID'))},{_clean_text(flow.get('id'))},{_clean_text(flow.get('version'))}"

    @staticmethod
    def parse_dataflow_identifier(identifier: str) -> Dict[str, str]:
        candidate = _clean_text(identifier)
        if candidate.startswith("{") and "datasetId" in candidate:
            try:
                parsed = json.loads(candidate)
                if isinstance(parsed, dict) and isinstance(parsed.get("datasetId"), str):
                    candidate = _clean_text(parsed["datasetId"])
            except Exception:
                pass
        parts = [part.strip() for part in candidate.split(",") if part.strip()]
        if not parts:
            raise RuntimeError("Empty dataflow identifier provided.")
        if len(parts) == 1:
            return {"agencyId": "", "dataflowId": parts[0]}
        if len(parts) == 2:
            return {"agencyId": parts[0] or "ABS", "dataflowId": parts[1]}
        return {"agencyId": parts[0] or "ABS", "dataflowId": parts[1], "version": parts[2]}

    @staticmethod
    def select_latest_flow(flows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not flows:
            return None
        return max(flows, key=lambda flow: DomesticDataService._version_key(_clean_text(flow.get("version"))))

    @staticmethod
    def _version_key(version: str) -> tuple[int, ...]:
        parts = []
        for item in str(version or "").split("."):
            try:
                parts.append(int(item))
            except Exception:
                parts.append(0)
        return tuple(parts)


_DOMESTIC_SERVICE: Optional[DomesticDataService] = None


def get_domestic_service() -> DomesticDataService:
    global _DOMESTIC_SERVICE
    if _DOMESTIC_SERVICE is None:
        _DOMESTIC_SERVICE = DomesticDataService()
    return _DOMESTIC_SERVICE
