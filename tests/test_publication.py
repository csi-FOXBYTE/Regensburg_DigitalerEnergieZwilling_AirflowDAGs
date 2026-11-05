import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))
sys.path.insert(0, os.path.dirname(__file__))

from s3_fake import FakeS3Client
from pipeline import config, manifest, publication
from pipeline.tasks import upload


TILES = "det-rg-main"
GML = "det-rg-gml"
INPUT = "det-rg-upload"
RUN_ID = "manual__2026-11-04T10:00:00+00:00"
JOB_ID = config.sanitize_job_id(RUN_ID)

PREVIOUS_TILES = {
    "tileset.json": b"old-root",
    "tiles/0.b3dm": b"old-0",
    "tiles/old-only.b3dm": b"old-x",
    "det-rg-addresses.sqlite": b"old-db",
}
PREVIOUS_GML = {"a.gml": b"old-a", "old-only.gml": b"old-b"}


def _write(root: Path, files: dict[str, bytes]) -> None:
    for key, body in files.items():
        path = root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)


class PublicationTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        _write(self.root / "3d_tiles", {
            "tileset.json": b"new-root",
            "tiles/0.b3dm": b"new-0",
            "tiles/1.b3dm": b"new-1",
        })
        _write(self.root / "address_db", {"det-rg-addresses.sqlite": b"new-db"})
        _write(self.root / "gml_out", {"a.gml": b"new-a", "c.gml": b"new-c"})
        self.client = FakeS3Client({TILES: PREVIOUS_TILES, GML: PREVIOUS_GML})

    def _plans(self):
        return [
            publication.build_plan(GML, JOB_ID, [self.root / "gml_out"], label="gml"),
            publication.build_plan(
                TILES,
                JOB_ID,
                [self.root / "3d_tiles", self.root / "address_db"],
                required_keys=["det-rg-addresses.sqlite"],
                entry_keys=["tileset.json"],
                label="tiles",
            ),
        ]

    def _status(self, bucket):
        return json.loads(self.client.objects[bucket][publication.STATUS_KEY]["body"])

    def _assert_previous_content_live(self):
        self.assertEqual(self.client.live(TILES), PREVIOUS_TILES)
        self.assertEqual(self.client.live(GML), PREVIOUS_GML)

    def _assert_no_staging(self):
        for bucket in (TILES, GML):
            self.assertFalse(
                [k for k in self.client.keys(bucket) if k.startswith(publication.STAGING_PREFIX)]
            )

    def test_publish_replaces_content_prunes_stale_objects_and_writes_status(self):
        publication.publish(self.client, self._plans(), run_id=RUN_ID, workers=3)

        self.assertEqual(self.client.live(TILES), {
            "tileset.json": b"new-root",
            "tiles/0.b3dm": b"new-0",
            "tiles/1.b3dm": b"new-1",
            "det-rg-addresses.sqlite": b"new-db",
        })
        self.assertEqual(self.client.live(GML), {"a.gml": b"new-a", "c.gml": b"new-c"})
        self._assert_no_staging()
        status = self._status(TILES)
        self.assertEqual(status["state"], "published")
        self.assertEqual(status["live_content"], "complete")
        self.assertEqual(status["pruned_objects"], 1)
        self.assertEqual(status["last_published"]["objects"], 4)
        self.assertEqual(status["last_published"]["run_id"], RUN_ID)
        self.assertEqual(self._status(GML)["pruned_objects"], 1)

    def test_nothing_is_live_or_deleted_before_every_output_is_staged_and_verified(self):
        publication.publish(self.client, self._plans(), run_id=RUN_ID, workers=1)

        kinds = [kind for kind, _bucket, _key in self.client.events]
        last_upload = max(i for i, kind in enumerate(kinds) if kind == "upload")
        first_copy = kinds.index("copy")
        first_delete = kinds.index("delete")
        last_copy = max(i for i, kind in enumerate(kinds) if kind == "copy")
        self.assertLess(last_upload, first_copy)
        self.assertLess(last_copy, first_delete)
        # The tileset root is switched last, after all content it references.
        copies = [e for e in self.client.events if e[0] == "copy"]
        self.assertEqual(copies[-1], ("copy", TILES, "tileset.json"))
        # Uploads go to the run's staging prefix, never to live keys.
        for kind, _bucket, key in self.client.events:
            if kind == "upload":
                self.assertTrue(key.startswith(f".det-publication/staging/{JOB_ID}/"))

    def test_failure_mid_upload_leaves_previous_content_intact(self):
        # GML staging succeeds, the tiles upload aborts halfway.
        self.client.fail_upload_at = 4
        self.client.fail_upload_in_bucket = TILES

        with self.assertRaisesRegex(ConnectionError, "connection reset"):
            publication.publish(self.client, self._plans(), run_id=RUN_ID, workers=1)

        self._assert_previous_content_live()
        self._assert_no_staging()
        for bucket in (TILES, GML):
            status = self._status(bucket)
            self.assertEqual(status["state"], "failed")
            self.assertEqual(status["failed_phase"], "staging")
            self.assertEqual(status["live_content"], "unchanged")
            self.assertEqual(status["error_type"], "ConnectionError")
            self.assertNotIn("error", status)

    def test_incomplete_staging_is_not_promoted(self):
        self.client.drop_upload_suffix = "tiles/1.b3dm"

        with self.assertRaisesRegex(publication.PublicationError, "1 missing"):
            publication.publish(self.client, self._plans(), run_id=RUN_ID, workers=2)

        self._assert_previous_content_live()
        self._assert_no_staging()
        self.assertEqual(self._status(TILES)["failed_phase"], "verifying")

    def test_promotion_failure_never_empties_a_bucket(self):
        self.client.fail_copy_in_bucket = TILES

        with self.assertRaisesRegex(ConnectionError, "copy failed"):
            publication.publish(self.client, self._plans(), run_id=RUN_ID, workers=1)

        # Tiles keep the previous publication; nothing was pruned anywhere.
        self.assertEqual(self.client.live(TILES), PREVIOUS_TILES)
        self.assertIn("old-only.gml", self.client.live(GML))
        self._assert_no_staging()
        self.assertEqual(self._status(GML)["live_content"], "replaced")
        self.assertEqual(self._status(TILES)["live_content"], "partially_replaced")

    def test_failed_status_keeps_last_successful_publication(self):
        publication.publish(self.client, self._plans(), run_id="first", workers=2)
        self.client.fail_upload_at = self.client.uploads + 1

        with self.assertRaises(ConnectionError):
            publication.publish(self.client, self._plans(), run_id="second", workers=2)

        status = self._status(TILES)
        self.assertEqual(status["state"], "failed")
        self.assertEqual(status["run_id"], "second")
        self.assertEqual(status["last_published"]["run_id"], "first")

    def test_incomplete_local_output_is_rejected_before_s3_access(self):
        (self.root / "3d_tiles" / "tileset.json").unlink()

        with self.assertRaisesRegex(publication.PublicationError, "tileset.json"):
            self._plans()
        with self.assertRaisesRegex(publication.PublicationError, "does not exist"):
            publication.build_plan(GML, JOB_ID, [self.root / "missing_dir"])
        (self.root / "empty").mkdir()
        with self.assertRaisesRegex(publication.PublicationError, "No output files"):
            publication.build_plan(GML, JOB_ID, [self.root / "empty"])

    def test_stale_staging_of_aborted_runs_is_pruned(self):
        self.client.objects[TILES][".det-publication/staging/crashed/x.b3dm"] = {
            "body": b"x", "args": {},
        }

        publication.publish(self.client, self._plans(), run_id=RUN_ID, workers=2)

        self._assert_no_staging()
        self.assertEqual(self._status(TILES)["pruned_objects"], 1)

    def test_outputs_must_use_distinct_buckets(self):
        plans = self._plans()
        plans[0].bucket = TILES

        with self.assertRaisesRegex(publication.PublicationError, "own bucket"):
            publication.publish(self.client, plans, run_id=RUN_ID)
        self.assertEqual(self.client.events, [])


class PublishOutputsTaskTest(unittest.TestCase):
    OUTPUTS = [
        {"bucket_param": "gml_output_bucket", "directories": ["gml_out"]},
        {
            "bucket_param": "tiles_output_bucket",
            "directories": ["3d_tiles"],
            "entry_keys": ["tileset.json"],
        },
    ]

    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        for target in (config, manifest):
            patcher = mock.patch.object(target, "WORK_DIR", temp.name)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.job_dir = Path(config.get_job_dir(RUN_ID))
        _write(self.job_dir / "3d_tiles", {"tileset.json": b"root"})
        _write(self.job_dir / "gml_out", {"a.gml": b"a"})
        manifest.create_manifest(
            str(self.job_dir), JOB_ID, RUN_ID, "digital_twin_pipeline", {}, ["publish_outputs"]
        )

    def _run(self, params):
        client = FakeS3Client({TILES: {}, GML: {}})
        hook = mock.Mock()
        hook.get_conn.return_value = client
        with mock.patch.object(upload, "get_s3_hook", return_value=hook) as get_s3_hook:
            self.get_s3_hook = get_s3_hook
            upload._publish_outputs_callable(
                params,
                RUN_ID,
                "publish_outputs",
                self.OUTPUTS,
                "det_rg_s3",
                exclusive_bucket_params=("bucket",),
            )
        return client, get_s3_hook

    def _step(self):
        data = json.loads((self.job_dir / "manifest.json").read_text())
        return data["steps"]["publish_outputs"]

    def test_publishes_all_outputs(self):
        client, get_s3_hook = self._run({
            "bucket": INPUT,
            "tiles_output_bucket": TILES,
            "gml_output_bucket": GML,
        })

        get_s3_hook.assert_called_once_with("det_rg_s3")
        self.assertEqual(client.live(TILES), {"tileset.json": b"root"})
        self.assertEqual(client.live(GML), {"a.gml": b"a"})
        self.assertEqual(self._step()["status"], "success")

    def test_rejects_non_allowlisted_bucket_before_s3_access(self):
        with self.assertRaisesRegex(ValueError, "not in the allowlist"):
            self._run({
                "bucket": INPUT,
                "tiles_output_bucket": "someone-elses-bucket",
                "gml_output_bucket": GML,
            })
        self.get_s3_hook.assert_not_called()
        self.assertEqual(self._step()["status"], "failed")

    def test_rejects_output_bucket_shared_with_input(self):
        with self.assertRaisesRegex(ValueError, "dedicated buckets"):
            self._run({
                "bucket": INPUT,
                "tiles_output_bucket": TILES,
                "gml_output_bucket": INPUT,
            })
        self.get_s3_hook.assert_not_called()


if __name__ == "__main__":
    unittest.main()
