import json
import os
import shutil
import sys
import tempfile
import unittest
from unittest import mock


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))

from pipeline import config, manifest
from pipeline.tasks import cleanup


class ArtifactCleanupTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.work_dir = self.temp_dir.name
        self.run_id = "manual__2026-07-14T10:00:00+00:00"

        self.work_dir_patcher = mock.patch.object(config, "WORK_DIR", self.work_dir)
        self.manifest_work_dir_patcher = mock.patch.object(manifest, "WORK_DIR", self.work_dir)
        self.work_dir_patcher.start()
        self.manifest_work_dir_patcher.start()
        self.addCleanup(self.work_dir_patcher.stop)
        self.addCleanup(self.manifest_work_dir_patcher.stop)

        self.job_dir = config.get_job_dir(self.run_id)
        os.makedirs(self.job_dir)

    def _create_manifest(self, steps=None):
        manifest.create_manifest(
            job_dir=self.job_dir,
            job_id=config.sanitize_job_id(self.run_id),
            run_id=self.run_id,
            dag_id="digital_twin_pipeline",
            params={},
            step_names=steps or ["processing", "cleanup", "finalize_manifest"],
        )

    def _read_manifest(self):
        with open(os.path.join(self.job_dir, "manifest.json")) as file:
            return json.load(file)

    def _write_artifact(self, directory, filename="artifact.dat"):
        path = os.path.join(self.job_dir, directory)
        os.makedirs(path, exist_ok=True)
        with open(os.path.join(path, filename), "w") as file:
            file.write("data")

    def test_manifest_omits_empty_directories_and_discovers_files(self):
        os.makedirs(os.path.join(self.job_dir, "json"))
        self._create_manifest()
        self.assertEqual(self._read_manifest()["artifacts"], {})

        self._write_artifact("gml_in")
        manifest.update_step(self.job_dir, "processing", "success")

        self.assertEqual(
            self._read_manifest()["artifacts"],
            {"gml_in": f"jobs/{config.sanitize_job_id(self.run_id)}/gml_in/"},
        )

    def test_successful_cleanup_removes_artifacts_and_keeps_manifest(self):
        for directory in manifest.ARTIFACT_DIRS:
            self._write_artifact(directory)
        self._create_manifest()

        cleanup._cleanup_callable(manifest.ARTIFACT_DIRS, self.run_id, "cleanup")
        manifest.finalize_manifest(self.job_dir)

        result = self._read_manifest()
        self.assertTrue(os.path.isfile(os.path.join(self.job_dir, "manifest.json")))
        self.assertTrue(all(not os.path.exists(os.path.join(self.job_dir, d)) for d in manifest.ARTIFACT_DIRS))
        self.assertEqual(result["artifacts"], {})
        self.assertEqual(result["steps"]["cleanup"]["status"], "success")
        self.assertEqual(result["steps"]["finalize_manifest"]["status"], "success")
        self.assertEqual(result["status"], "success")

    def test_failed_pipeline_retains_non_empty_artifacts(self):
        self._write_artifact("json")
        self._create_manifest()

        manifest.update_step(self.job_dir, "processing", "failed", error="processing failed")
        manifest.finalize_manifest(self.job_dir)

        result = self._read_manifest()
        self.assertTrue(os.path.isfile(os.path.join(self.job_dir, "json", "artifact.dat")))
        self.assertIn("json", result["artifacts"])
        self.assertEqual(result["steps"]["cleanup"]["status"], "skipped")
        self.assertEqual(result["status"], "failed")

    def test_cleanup_failure_records_error_and_remaining_artifacts(self):
        self._write_artifact("json")
        self._write_artifact("gml_in")
        self._create_manifest()
        real_rmtree = shutil.rmtree

        def remove_then_fail(path):
            if path.endswith("gml_in"):
                raise OSError("cannot remove gml_in")
            real_rmtree(path)

        with mock.patch.object(cleanup.shutil, "rmtree", side_effect=remove_then_fail):
            with self.assertRaisesRegex(OSError, "cannot remove gml_in"):
                cleanup._cleanup_callable(["json", "gml_in"], self.run_id, "cleanup")

        result = self._read_manifest()
        self.assertFalse(os.path.exists(os.path.join(self.job_dir, "json")))
        self.assertTrue(os.path.isfile(os.path.join(self.job_dir, "gml_in", "artifact.dat")))
        self.assertEqual(set(result["artifacts"]), {"gml_in"})
        self.assertEqual(result["steps"]["cleanup"]["status"], "failed")
        self.assertEqual(result["errors"][-1]["error"], "cannot remove gml_in")


if __name__ == "__main__":
    unittest.main()
