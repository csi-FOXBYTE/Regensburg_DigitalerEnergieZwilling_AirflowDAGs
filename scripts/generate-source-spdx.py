"""Write a fast SPDX 2.3 inventory of the tracked source tree.

This records file hashes and the repository's declared license. It does not
infer per-file licenses, audit dependencies, or approve redistribution.
"""

import hashlib
import json
from datetime import datetime, timezone
import os
from pathlib import Path
import re
import subprocess


ROOT = Path(os.environ.get("SPDX_SOURCE_ROOT", Path(__file__).resolve().parent.parent)).resolve()
OUTPUT = ROOT / "SPDX_source.spdx.json"
SOURCE_NAME = os.environ.get("SPDX_SOURCE_NAME", ROOT.name)


def sha1(data):
    return hashlib.sha1(data).hexdigest()


def source_files():
    paths = subprocess.check_output(["git", "ls-files", "-z"], cwd=ROOT).split(b"\0")
    for raw in paths:
        if not raw:
            continue
        path = Path(raw.decode())
        if path.as_posix() == OUTPUT.name or not (ROOT / path).is_file():
            continue
        yield path


package_json = ROOT / "package.json"
package = json.loads(package_json.read_text()) if package_json.exists() else {}
notice = (ROOT / "NOTICE").read_text()
match = re.search(r"^SPDX license identifier:\s*(\S+)", notice, re.MULTILINE)
license_declared = package.get("license") or (match.group(1) if match else "NOASSERTION")
files = []
relationships = [{"spdxElementId": "SPDXRef-DOCUMENT", "relationshipType": "DESCRIBES", "relatedSpdxElement": "SPDXRef-SOURCE"}]
for path in source_files():
    data = (ROOT / path).read_bytes()
    identifier = "SPDXRef-File-" + hashlib.sha256(str(path).encode()).hexdigest()[:24]
    files.append({
        "SPDXID": identifier,
        "fileName": "./" + path.as_posix(),
        "checksums": [{"algorithm": "SHA1", "checksumValue": sha1(data)}],
        "licenseConcluded": "NOASSERTION",
        "licenseInfoInFiles": ["NOASSERTION"],
        "copyrightText": "NOASSERTION",
    })
    relationships.append({"spdxElementId": "SPDXRef-SOURCE", "relationshipType": "CONTAINS", "relatedSpdxElement": identifier})
files.sort(key=lambda item: item["fileName"])
verification = sha1("".join(sorted(item["checksums"][0]["checksumValue"] for item in files)).encode())
identity = hashlib.sha256("".join(item["fileName"] + item["checksums"][0]["checksumValue"] for item in files).encode()).hexdigest()
namespace = "https://spdx.org/spdxdocs/" + SOURCE_NAME + "-" + identity
created = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
if OUTPUT.exists():
    previous = json.loads(OUTPUT.read_text())
    if previous.get("documentNamespace") == namespace:
        created = previous["creationInfo"]["created"]
source = {
    "SPDXID": "SPDXRef-SOURCE",
    "name": SOURCE_NAME,
    "downloadLocation": "NOASSERTION",
    "filesAnalyzed": True,
    "packageVerificationCode": {"packageVerificationCodeValue": verification,
                                "packageVerificationCodeExcludedFiles": ["./" + OUTPUT.name]},
    "primaryPackagePurpose": "SOURCE",
    "licenseDeclared": license_declared,
    "licenseConcluded": "NOASSERTION",
    "licenseInfoFromFiles": ["NOASSERTION"],
    "copyrightText": "NOASSERTION",
}
if package.get("version"):
    source["versionInfo"] = package["version"]
document = {
    "spdxVersion": "SPDX-2.3",
    "dataLicense": "CC0-1.0",
    "SPDXID": "SPDXRef-DOCUMENT",
    "name": SOURCE_NAME + " source",
    "documentNamespace": namespace,
    "creationInfo": {"created": created, "creators": ["Tool: generate-source-spdx.py"]},
    "comment": "Inventory of tracked source files only. License and copyright conclusions require review; built artifacts need separate inventories.",
    "documentDescribes": ["SPDXRef-SOURCE"],
    "packages": [source],
    "files": files,
    "relationships": relationships,
}
OUTPUT.write_text(json.dumps(document, indent=2, ensure_ascii=False) + "\n")
print(f"Wrote {OUTPUT.name} with {len(files)} tracked files")
