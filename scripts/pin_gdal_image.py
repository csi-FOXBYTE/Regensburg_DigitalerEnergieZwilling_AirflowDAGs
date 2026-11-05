#!/usr/bin/env python3
"""Pin a freshly built GDAL image in the terrain DAG and the SBOM configuration.

Called by the workflow "Build GDAL image" after the image passed its checks.
Replaces the GDAL_IMAGE reference in dags/pipeline/config.py and the matching
container entry in sbom.config.json. Changes nothing if any check fails.
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import re
from pathlib import Path


CONFIG_PATH = "dags/pipeline/config.py"
SBOM_CONFIG_PATH = "sbom.config.json"
IMAGE = re.compile(r"^[a-z0-9.-]+(/[a-z0-9._-]+)+$")
TAG = re.compile(r"^(?P<version>\d+\.\d+\.\d+)-build-\d+-\d+$")
DIGEST = re.compile(r"^sha256:[a-f0-9]{64}$")
PINNED_IMAGE = re.compile(r"^[a-z0-9.-]+(/[a-z0-9._-]+)+:[A-Za-z0-9._-]+@sha256:[a-f0-9]{64}$")
# The assignment as it is written in config.py: image:tag and digest on two lines.
ASSIGNMENT = re.compile(
    r'^GDAL_IMAGE = \(\n    "(?P<image>[^"\n]+)"\n    "(?P<digest>@sha256:[a-f0-9]{64})"\n\)$',
    re.MULTILINE,
)


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def gdal_image(config_source: str) -> str:
    """The GDAL_IMAGE value config.py assigns."""
    for node in ast.parse(config_source).body:
        if (isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name) and node.targets[0].id == "GDAL_IMAGE"):
            return ast.literal_eval(node.value)
    raise ValueError(f"{CONFIG_PATH} assigns no GDAL_IMAGE")


def _write(path: Path, content: str) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(content, encoding="utf-8")
    os.replace(temporary, path)


def pin(destination: Path, *, image: str, tag: str, digest: str, base_image: str) -> str:
    require(IMAGE.fullmatch(image) is not None, "Invalid image repository")
    match = TAG.fullmatch(tag)
    require(match is not None, "Tag must be <GDAL version>-build-<run id>-<attempt>")
    require(DIGEST.fullmatch(digest) is not None, "Invalid image digest")
    require(PINNED_IMAGE.fullmatch(base_image) is not None, "The base image must be pinned by tag and digest")
    version = match.group("version")
    require(f"-{version}@sha256:" in base_image, f"The base image is not GDAL {version}")
    reference = f"{image}:{tag}@{digest}"

    config_path = destination / CONFIG_PATH
    config_source = config_path.read_text(encoding="utf-8")
    assignments = list(ASSIGNMENT.finditer(config_source))
    require(len(assignments) == 1, f"{CONFIG_PATH} must assign GDAL_IMAGE exactly once, as image:tag and digest")
    current = gdal_image(config_source)
    require(current == assignments[0]["image"] + assignments[0]["digest"],
            f"Unexpected GDAL_IMAGE assignment in {CONFIG_PATH}")
    start, end = assignments[0].span()
    new_config = (config_source[:start]
                  + f'GDAL_IMAGE = (\n    "{image}:{tag}"\n    "@{digest}"\n)'
                  + config_source[end:])
    require(gdal_image(new_config) == reference, "The new GDAL_IMAGE does not parse back")

    # The SBOM entry must describe the image config.py used until now; this
    # keeps the two files in step.
    sbom_path = destination / SBOM_CONFIG_PATH
    sbom_config = json.loads(sbom_path.read_text(encoding="utf-8"))
    entries = [component for component in sbom_config.get("additionalComponents", [])
               if component.get("bom-ref") == f"oci:{current}"]
    require(len(entries) == 1, f"{SBOM_CONFIG_PATH} must list the current GDAL image {current} exactly once")
    entries[0].update({
        "bom-ref": f"oci:{reference}",
        "name": image,
        "version": f"{tag}@{digest}",
        "notes": (f"GDAL {version} terrain-processing image built by this repository (images/gdal): "
                  f"{base_image} with Alpine security updates applied. Packages inside the image are "
                  "not expanded here; the image carries a BuildKit SBOM attestation."),
    })

    _write(config_path, new_config)
    _write(sbom_path, json.dumps(sbom_config, indent=2, ensure_ascii=False) + "\n")
    return reference


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    for name in ("image", "tag", "digest", "base-image"):
        parser.add_argument(f"--{name}", required=True)
    arguments = parser.parse_args()
    pinned = pin(Path(__file__).resolve().parents[1], image=arguments.image, tag=arguments.tag,
                 digest=arguments.digest, base_image=arguments.base_image)
    print(f"Pinned {pinned}")
