#!/usr/bin/env python3
"""Generate repository CycloneDX and CSV software bills of materials."""

from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

from cyclonedx.schema import OutputFormat, SchemaVersion
from cyclonedx.validation import make_schemabased_validator


REPOSITORY_DIR = Path(__file__).resolve().parents[1]
CONFIG_PATH = REPOSITORY_DIR / "sbom.config.json"
JSON_OUTPUT_PATH = REPOSITORY_DIR / "SBOM.cdx.json"
CSV_OUTPUT_PATH = REPOSITORY_DIR / "SBOM.csv"
DEPENDENCY_BOM_PATH = REPOSITORY_DIR / ".SBOM.dependencies.tmp.json"
JSON_TEMPORARY_PATH = REPOSITORY_DIR / ".SBOM.cdx.tmp.json"
CSV_TEMPORARY_PATH = REPOSITORY_DIR / ".SBOM.tmp.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate CycloneDX 1.6 JSON and review-friendly CSV SBOMs."
    )
    parser.add_argument(
        "--python",
        type=Path,
        default=REPOSITORY_DIR / ".airflow-env" / "bin" / "python",
        help="Python interpreter whose installed package metadata should be used",
    )
    return parser.parse_args()


def run(command: list[str]) -> str:
    return subprocess.run(
        command,
        cwd=REPOSITORY_DIR,
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    ).stdout


def normalized_python_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def get_property(component: dict[str, Any], name: str) -> str | None:
    for entry in component.get("properties", []):
        if entry.get("name") == name:
            return entry.get("value")
    return None


def set_property(component: dict[str, Any], name: str, value: str) -> None:
    properties = component.setdefault("properties", [])
    for entry in properties:
        if entry.get("name") == name:
            entry["value"] = value
            return
    properties.append({"name": name, "value": value})


def require_string(value: Any, location: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{location} must be a non-empty string")
    return value


def dependency_closure(
    start_refs: list[str], dependency_map: dict[str, list[str]]
) -> set[str]:
    visited: set[str] = set()
    pending = list(start_refs)
    while pending:
        current = pending.pop()
        if current in visited:
            continue
        visited.add(current)
        pending.extend(dependency_map.get(current, []))
    return visited


def generate_dependency_bom(python_executable: Path) -> dict[str, Any]:
    executable = python_executable.absolute()
    if not executable.is_file():
        raise FileNotFoundError(
            f"Python environment not found at {executable}. Run ./init.sh first or "
            "pass --python."
        )

    run(
        [
            sys.executable,
            "-m",
            "cyclonedx_py",
            "environment",
            "--spec-version",
            "1.6",
            "--output-format",
            "JSON",
            "--output-file",
            str(DEPENDENCY_BOM_PATH),
            str(executable),
        ]
    )
    return json.loads(DEPENDENCY_BOM_PATH.read_text(encoding="utf-8"))


def add_repository_metadata(bom: dict[str, Any], config: dict[str, Any]) -> None:
    root = config.get("rootComponent")
    if not isinstance(root, dict):
        raise ValueError("sbom.config.json must contain a rootComponent object")
    for field in ("type", "bom-ref", "name", "version"):
        require_string(root.get(field), f"rootComponent.{field}")

    direct_config = config.get("directPythonDependencies")
    if not isinstance(direct_config, list):
        raise ValueError(
            "sbom.config.json must contain a directPythonDependencies array"
        )

    direct_sources: dict[str, str] = {}
    for index, dependency in enumerate(direct_config):
        location = f"directPythonDependencies[{index}]"
        if not isinstance(dependency, dict):
            raise ValueError(f"{location} must be an object")
        name = require_string(dependency.get("name"), f"{location}.name")
        source = require_string(
            dependency.get("metadataSource"), f"{location}.metadataSource"
        )
        direct_sources[normalized_python_name(name)] = source

    components = bom.setdefault("components", [])
    components_by_name = {
        normalized_python_name(component["name"]): component
        for component in components
        if isinstance(component.get("name"), str)
    }
    missing = [
        dependency["name"]
        for dependency in direct_config
        if normalized_python_name(dependency["name"]) not in components_by_name
    ]
    if missing:
        raise ValueError(
            "Configured direct Python dependencies are not installed in the scanned "
            f"environment: {', '.join(missing)}"
        )

    dependency_map = {
        entry["ref"]: entry.get("dependsOn", [])
        for entry in bom.setdefault("dependencies", [])
    }
    direct_refs = [
        components_by_name[name]["bom-ref"] for name in direct_sources
    ]
    runtime_refs = dependency_closure(direct_refs, dependency_map)
    repository = root["name"]

    # cyclonedx-py inventories every installed distribution in the selected
    # environment. Keep only the dependency graph reachable from packages used
    # by the DAGs so unrelated Airflow development-environment packages do not
    # leak into this repository-scoped SBOM.
    bom["components"] = [
        component
        for component in components
        if component["bom-ref"] in runtime_refs
    ]
    bom["dependencies"] = [
        {
            **entry,
            "dependsOn": [
                dependency
                for dependency in entry.get("dependsOn", [])
                if dependency in runtime_refs
            ],
        }
        for entry in bom["dependencies"]
        if entry["ref"] in runtime_refs
    ]
    components = bom["components"]

    set_property(root, "sbom:repository", repository)
    set_property(root, "sbom:ecosystem", "first-party")
    set_property(root, "sbom:relationship", "root component")
    set_property(root, "sbom:metadata-source", f"{repository}/sbom.config.json")
    set_property(root, "sbom:notes", "First-party repository component.")
    try:
        set_property(root, "vcs:commit", run(["git", "rev-parse", "HEAD"]).strip())
        status = run(["git", "status", "--porcelain"]).strip()
        set_property(
            root,
            "sbom:source-state",
            "clean" if not status else "modified working tree",
        )
    except (OSError, subprocess.CalledProcessError):
        set_property(root, "sbom:source-state", "Git state unavailable")

    for component in components:
        normalized_name = normalized_python_name(component["name"])
        component_ref = component["bom-ref"]
        if normalized_name in direct_sources:
            relationship = "direct runtime dependency"
            metadata_source = f"{repository}/{direct_sources[normalized_name]}"
        elif component_ref in runtime_refs:
            relationship = "transitive runtime dependency"
            metadata_source = f"{repository}/.airflow-env installed package metadata"
        else:
            raise ValueError(
                f"Component {component['name']} is outside the DAG dependency closure"
            )

        set_property(component, "sbom:repository", repository)
        set_property(component, "sbom:ecosystem", "PyPI")
        set_property(component, "sbom:relationship", relationship)
        set_property(component, "sbom:metadata-source", metadata_source)
        set_property(
            component,
            "sbom:notes",
            "Generated from installed distribution metadata by cyclonedx-bom.",
        )

    bom.setdefault("metadata", {})["component"] = root
    root_ref = root["bom-ref"]
    root_dependencies = next(
        (entry for entry in bom["dependencies"] if entry["ref"] == root_ref),
        None,
    )
    if root_dependencies is None:
        root_dependencies = {"ref": root_ref, "dependsOn": []}
        bom["dependencies"].insert(0, root_dependencies)
    root_dependencies["dependsOn"] = sorted(set(direct_refs))


def add_configured_components(bom: dict[str, Any], config: dict[str, Any]) -> None:
    configured_components = config.get("additionalComponents")
    if not isinstance(configured_components, list):
        raise ValueError("sbom.config.json must contain an additionalComponents array")

    repository = bom["metadata"]["component"]["name"]
    components: list[dict[str, Any]] = []
    for index, configured in enumerate(configured_components):
        location = f"additionalComponents[{index}]"
        if not isinstance(configured, dict):
            raise ValueError(f"{location} must be an object")
        component = dict(configured)
        ecosystem = require_string(component.pop("ecosystem", None), f"{location}.ecosystem")
        relationship = require_string(
            component.pop("relationship", None), f"{location}.relationship"
        )
        metadata_source = require_string(
            component.pop("metadataSource", None), f"{location}.metadataSource"
        )
        notes = require_string(component.pop("notes", None), f"{location}.notes")
        for field in ("type", "bom-ref", "name", "version"):
            require_string(component.get(field), f"{location}.{field}")

        set_property(component, "sbom:repository", repository)
        set_property(component, "sbom:ecosystem", ecosystem)
        set_property(component, "sbom:relationship", relationship)
        set_property(
            component,
            "sbom:metadata-source",
            f"{repository}/{metadata_source}",
        )
        set_property(component, "sbom:notes", notes)
        components.append(component)

    bom["components"].extend(components)
    root_ref = bom["metadata"]["component"]["bom-ref"]
    root_dependencies = next(
        entry for entry in bom["dependencies"] if entry["ref"] == root_ref
    )
    root_dependencies["dependsOn"] = sorted(
        set(root_dependencies["dependsOn"])
        | {component["bom-ref"] for component in components}
    )


def add_generation_metadata(bom: dict[str, Any]) -> None:
    tools = bom.setdefault("metadata", {}).setdefault("tools", {})
    components = tools.setdefault("components", [])
    components.append(
        {
            "type": "application",
            "name": "repository SBOM enrichment script",
            "version": "1",
        }
    )
    bom["metadata"]["properties"] = [
        {
            "name": "sbom:scope",
            "value": (
                "This repository's two Airflow DAGs, their reachable Python package "
                "dependencies, and the container images executed by those DAGs"
            ),
        },
        {
            "name": "sbom:container-detail",
            "value": (
                "Container components are inventory references; packages inside the "
                "images are not expanded"
            ),
        },
        {
            "name": "sbom:license-method",
            "value": (
                "Installed Python distribution metadata plus repository declarations "
                "for first-party and referenced software"
            ),
        },
        {
            "name": "sbom:unresolved-policy",
            "value": (
                "Missing licenses, NOASSERTION, and not-pinned markers are retained "
                "for manual review; no license is guessed"
            ),
        },
    ]


def validate_bom(bom: dict[str, Any]) -> str:
    if bom.get("bomFormat") != "CycloneDX" or bom.get("specVersion") != "1.6":
        raise ValueError("Generator did not produce a CycloneDX 1.6 document")

    all_components = [bom["metadata"]["component"], *bom["components"]]
    refs = [component.get("bom-ref") for component in all_components]
    if any(not isinstance(ref, str) or not ref for ref in refs):
        raise ValueError("SBOM contains a component without a bom-ref")
    if len(set(refs)) != len(refs):
        raise ValueError("SBOM contains duplicate component bom-ref values")

    known_refs = set(refs)
    dependency_refs = {
        ref
        for entry in bom["dependencies"]
        for ref in [entry.get("ref"), *entry.get("dependsOn", [])]
    }
    dangling_refs = sorted(ref for ref in dependency_refs if ref not in known_refs)
    if dangling_refs:
        raise ValueError(
            f"SBOM contains dangling dependency references: {', '.join(dangling_refs)}"
        )

    output = json.dumps(bom, indent=2) + "\n"
    validation_error = make_schemabased_validator(
        OutputFormat.JSON, SchemaVersion.V1_6
    ).validate_str(output)
    if validation_error:
        raise ValueError(f"SBOM is invalid against CycloneDX 1.6: {validation_error}")
    return output


def license_value(component: dict[str, Any]) -> str:
    values: list[str] = []
    for entry in component.get("licenses", []):
        if "expression" in entry:
            values.append(entry["expression"])
        else:
            license_data = entry.get("license", {})
            values.append(
                license_data.get("id")
                or license_data.get("name")
                or "NOASSERTION"
            )
    return " AND ".join(values) if values else "NOASSERTION"


def create_csv(bom: dict[str, Any]) -> None:
    repository = bom["metadata"]["component"]["name"]
    components = [bom["metadata"]["component"], *bom["components"]]

    def rank(component: dict[str, Any]) -> tuple[int, str]:
        if component is bom["metadata"]["component"]:
            component_rank = 0
        elif component.get("type") == "container":
            component_rank = 1
        elif get_property(component, "sbom:ecosystem") != "PyPI":
            component_rank = 2
        else:
            component_rank = 3
        return component_rank, component.get("name", "").lower()

    with CSV_TEMPORARY_PATH.open("w", encoding="utf-8", newline="") as output:
        # Preserve the checked-in CSV's RFC 4180 line endings.
        writer = csv.writer(output, lineterminator="\r\n")
        writer.writerow(
            [
                "Repository",
                "Component Type",
                "Ecosystem",
                "Name",
                "Version",
                "Dependency Scope",
                "Relationship",
                "License",
                "PURL or Reference",
                "Metadata Source",
                "Notes",
            ]
        )
        for component in sorted(components, key=rank):
            relationship = get_property(component, "sbom:relationship") or ""
            writer.writerow(
                [
                    repository,
                    component.get("type", ""),
                    get_property(component, "sbom:ecosystem") or "",
                    component.get("name", ""),
                    component.get("version", ""),
                    "development" if "development" in relationship else "runtime/build",
                    relationship,
                    license_value(component),
                    component.get("purl") or component.get("bom-ref", ""),
                    get_property(component, "sbom:metadata-source") or "",
                    get_property(component, "sbom:notes") or "",
                ]
            )


def main() -> None:
    args = parse_args()
    config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    try:
        bom = generate_dependency_bom(args.python)
        add_repository_metadata(bom, config)
        add_configured_components(bom, config)
        add_generation_metadata(bom)
        output = validate_bom(bom)
        JSON_TEMPORARY_PATH.write_text(output, encoding="utf-8")
        create_csv(bom)
        JSON_TEMPORARY_PATH.replace(JSON_OUTPUT_PATH)
        CSV_TEMPORARY_PATH.replace(CSV_OUTPUT_PATH)
    finally:
        DEPENDENCY_BOM_PATH.unlink(missing_ok=True)
        JSON_TEMPORARY_PATH.unlink(missing_ok=True)
        CSV_TEMPORARY_PATH.unlink(missing_ok=True)

    print(
        f"Generated {JSON_OUTPUT_PATH.name} and {CSV_OUTPUT_PATH.name} "
        f"with {len(bom['components'])} components."
    )


if __name__ == "__main__":
    main()
