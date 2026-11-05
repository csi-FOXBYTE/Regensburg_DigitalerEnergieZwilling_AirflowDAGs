"""Offline checks for the GDAL image this repository builds and pins itself."""

import json
import re
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "dags"))

from pipeline import config  # noqa: E402
from scripts.pin_gdal_image import CONFIG_PATH, SBOM_CONFIG_PATH, gdal_image, pin  # noqa: E402


WORKFLOWS = ROOT / ".github/workflows"
DOCKERFILE = (ROOT / "images/gdal/Dockerfile").read_text()
WORKFLOW = yaml.safe_load((WORKFLOWS / "gdal-image.yml").read_text())
ENV = WORKFLOW["env"]
DIGEST = "sha256:" + "e" * 64


def triggers(workflow: dict) -> dict:
    # PyYAML reads the bare key "on" as True.
    return workflow.get("on", workflow.get(True))


def steps(job: str) -> dict:
    return {step.get("name"): step for step in WORKFLOW["jobs"][job]["steps"]}


class GdalImageTests(unittest.TestCase):
    def test_dockerfile_only_refreshes_os_packages_and_labels_the_base(self):
        instructions = [line.split()[0] for line in DOCKERFILE.splitlines()
                        if line and not line.startswith((" ", "#"))]
        # No USER, ENTRYPOINT, CMD, ENV, WORKDIR or COPY: the image behaves like
        # the OSGeo image the DAG used before.
        self.assertEqual(set(instructions), {"ARG", "FROM", "RUN", "LABEL"})
        self.assertEqual(instructions.count("RUN"), 1)
        self.assertIn('RUN echo "OS package refresh: ${REFRESH_EPOCH}" && apk upgrade --no-cache\n',
                      DOCKERFILE)
        self.assertIn("FROM ${BASE_IMAGE}\n", DOCKERFILE)
        for label in ("source", "revision", "version", "base.name"):
            self.assertIn(f"org.opencontainers.image.{label}=", DOCKERFILE)

    def test_workflow_builds_the_dockerfile_defaults(self):
        self.assertIn(f"ARG BASE_IMAGE={ENV['BASE_IMAGE']}\n", DOCKERFILE)
        self.assertIn(f"ARG GDAL_VERSION={ENV['GDAL_VERSION']}\n", DOCKERFILE)
        self.assertRegex(ENV["BASE_IMAGE"],
                         rf"^ghcr\.io/osgeo/gdal:alpine-small-{re.escape(ENV['GDAL_VERSION'])}"
                         r"@sha256:[0-9a-f]{64}$")
        self.assertRegex(ENV["REFRESH_EPOCH"], r"^\d+$")
        build = steps("build")["Build and push"]["with"]
        self.assertEqual(build["context"], "images/gdal")
        self.assertEqual(build["platforms"], "${{ env.PLATFORM }}")
        self.assertEqual(ENV["PLATFORM"], "linux/amd64")
        self.assertEqual(build["tags"], "${{ env.IMAGE }}:${{ steps.prepare.outputs.tag }}")
        for argument in ("BASE_IMAGE=${{ env.BASE_IMAGE }}", "REFRESH_EPOCH=${{ env.REFRESH_EPOCH }}",
                         "GDAL_VERSION=${{ env.GDAL_VERSION }}", "SOURCE_COMMIT=${{ github.sha }}"):
            self.assertIn(argument, build["build-args"])
        self.assertIn('echo "tag=$GDAL_VERSION-build-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT"',
                      steps("build")["Prepare the build"]["run"])

    def test_workflow_scans_the_pushed_image_before_pinning(self):
        self.assertRegex(ENV["TRIVY_IMAGE"], r"^aquasec/trivy:[\d.]+@sha256:[0-9a-f]{64}$")
        check = steps("build")["Check the pushed image"]["run"]
        self.assertIn('docker run --rm --network none --user 50000:0 "$IMAGE@$DIGEST" gdalinfo --version',
                      check)
        scan = " ".join(steps("build")["Scan the pushed image"]["run"].split())
        for option in ("--input /scan/image.tar", "--scanners vuln,secret", "--severity HIGH,CRITICAL",
                       "--ignore-unfixed", "--exit-code 1"):
            self.assertIn(option, scan)
        # The pin job runs only after the whole build job, scan included, passed.
        self.assertEqual(WORKFLOW["jobs"]["pin"]["needs"], "build")
        self.assertIn("scripts/pin_gdal_image.py", steps("pin")["Pin the image"]["run"])
        self.assertIn("git add -- dags/pipeline/config.py sbom.config.json\n",
                      steps("pin")["Commit the pin on main"]["run"])

    def test_workflow_is_manual_main_only_and_least_privilege(self):
        self.assertEqual(set(triggers(WORKFLOW)), {"workflow_dispatch"})
        self.assertEqual(WORKFLOW["permissions"], {})
        jobs = WORKFLOW["jobs"]
        for job in jobs.values():
            self.assertEqual(job["if"], "github.ref == 'refs/heads/main'")
        self.assertEqual(jobs["build"]["permissions"], {"contents": "read", "packages": "write"})
        self.assertEqual(jobs["pin"]["permissions"], {"contents": "write"})
        for name, step in {**steps("build"), **steps("pin")}.items():
            if "uses" in step:
                self.assertRegex(step["uses"], r"^[\w.-]+/[\w.-]+@[0-9a-f]{40}$", name)
            if step.get("uses", "").startswith("actions/checkout@"):
                self.assertIs(step["with"]["persist-credentials"], False)

    def test_commits_to_main_never_race_and_never_publish(self):
        groups = set()
        for path in WORKFLOWS.glob("*.yml"):
            workflow = yaml.safe_load(path.read_text())
            if any(job.get("permissions", {}).get("contents") == "write"
                   for job in workflow["jobs"].values()):
                groups.add(workflow["concurrency"]["group"])
                self.assertIs(workflow["concurrency"]["cancel-in-progress"], False)
        self.assertEqual(groups, {WORKFLOW["concurrency"]["group"]})
        # A pin commit cannot start the publication: it is manual only.
        publish = yaml.safe_load((WORKFLOWS / "publish-dags.yml").read_text())
        self.assertEqual(set(triggers(publish)), {"workflow_dispatch"})

    def test_dag_uses_the_base_image_or_a_pinned_build(self):
        build = re.compile(rf"^{re.escape(ENV['IMAGE'])}:{re.escape(ENV['GDAL_VERSION'])}"
                           r"-build-\d+-\d+@sha256:[0-9a-f]{64}$")
        self.assertTrue(config.GDAL_IMAGE == ENV["BASE_IMAGE"] or build.match(config.GDAL_IMAGE),
                        config.GDAL_IMAGE)

    def test_sbom_config_lists_every_image_the_dags_run(self):
        sbom = json.loads((ROOT / "sbom.config.json").read_text())
        listed = {component["bom-ref"] for component in sbom["additionalComponents"]
                  if component["type"] == "container"}
        images = {f"oci:{value}" for name, value in vars(config).items() if name.endswith("_IMAGE")}
        self.assertEqual(listed, images)

    def _copy(self, directory: Path) -> Path:
        for path in (CONFIG_PATH, SBOM_CONFIG_PATH):
            (directory / path).parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / path, directory / path)
        return directory

    def test_pins_a_new_build_in_config_and_sbom(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = self._copy(Path(tmp))
            before = (directory / CONFIG_PATH).read_text()
            arguments = dict(image=ENV["IMAGE"], tag=f"{ENV['GDAL_VERSION']}-build-123-1", digest=DIGEST,
                             base_image=ENV["BASE_IMAGE"])
            reference = pin(directory, **arguments)
            self.assertEqual(reference, f"{ENV['IMAGE']}:{ENV['GDAL_VERSION']}-build-123-1@{DIGEST}")
            after = (directory / CONFIG_PATH).read_text()
            self.assertEqual(gdal_image(after), reference)
            # Only the GDAL_IMAGE assignment changed.
            image, digest = gdal_image(before).split("@", 1)
            self.assertEqual(after, before.replace(f'"{image}"\n    "@{digest}"',
                                                   f'"{ENV["IMAGE"]}:{arguments["tag"]}"\n    "@{DIGEST}"'))
            sbom = json.loads((directory / SBOM_CONFIG_PATH).read_text())
            entry = next(c for c in sbom["additionalComponents"] if c["bom-ref"] == f"oci:{reference}")
            self.assertEqual(entry["name"], ENV["IMAGE"])
            self.assertEqual(entry["version"], f"{arguments['tag']}@{DIGEST}")
            self.assertIn(ENV["BASE_IMAGE"], entry["notes"])
            self.assertEqual(len(sbom["additionalComponents"]),
                             len(json.loads((ROOT / SBOM_CONFIG_PATH).read_text())["additionalComponents"]))
            # A rerun for the same build changes nothing; the next build replaces it.
            snapshot = [(directory / path).read_bytes() for path in (CONFIG_PATH, SBOM_CONFIG_PATH)]
            pin(directory, **arguments)
            self.assertEqual([(directory / path).read_bytes() for path in (CONFIG_PATH, SBOM_CONFIG_PATH)],
                             snapshot)
            pin(directory, **{**arguments, "tag": f"{ENV['GDAL_VERSION']}-build-124-1",
                              "digest": "sha256:" + "f" * 64})
            self.assertTrue(gdal_image((directory / CONFIG_PATH).read_text()).endswith("f" * 64))

    def test_refuses_invalid_builds_without_writing_anything(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = self._copy(Path(tmp))
            snapshot = [(directory / path).read_bytes() for path in (CONFIG_PATH, SBOM_CONFIG_PATH)]
            base = dict(image=ENV["IMAGE"], tag=f"{ENV['GDAL_VERSION']}-build-123-1", digest=DIGEST,
                        base_image=ENV["BASE_IMAGE"])
            for change, message in (({"tag": "latest"}, "Tag must be"),
                                    ({"digest": "sha256:1234"}, "Invalid image digest"),
                                    ({"image": "GHCR.io/x"}, "Invalid image repository"),
                                    ({"base_image": "ghcr.io/osgeo/gdal:alpine-small-3.13.2"}, "pinned"),
                                    ({"tag": "3.12.0-build-123-1"}, "not GDAL 3.12.0")):
                with self.subTest(change=change), self.assertRaisesRegex(ValueError, message):
                    pin(directory, **{**base, **change})
            # sbom.config.json out of step with config.py.
            sbom = json.loads(snapshot[1])
            for component in sbom["additionalComponents"]:
                component["bom-ref"] = component["bom-ref"].replace("@sha256:", "@sha256:0")
            (directory / SBOM_CONFIG_PATH).write_text(json.dumps(sbom))
            with self.assertRaisesRegex(ValueError, "exactly once"):
                pin(directory, **base)
            self.assertEqual((directory / CONFIG_PATH).read_bytes(), snapshot[0])


if __name__ == "__main__":
    unittest.main()
