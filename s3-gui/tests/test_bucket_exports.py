import io
from pathlib import Path
import sys
import threading
import time
import unittest
from unittest import mock
import zipfile

import boto3
from botocore.exceptions import ReadTimeoutError
from botocore.response import StreamingBody
from botocore.stub import Stubber


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import app as gui
import bucket_exports


class FakeS3:
    def __init__(self, pages):
        self.pages = pages
        self.started = threading.Event()
        self.release = threading.Event()
        self.release.set()
        self.error = None
        self.closed = False

    def get_paginator(self, name):
        if name != "list_objects_v2":
            raise AssertionError(name)
        return self

    def paginate(self, **kwargs):
        for page in self.pages:
            yield {"Contents": [{"Key": key} for key in page]}

    def list_objects_v2(self, **kwargs):
        return {}

    def head_object(self, *, Bucket, Key):
        data = next(page[Key] for page in self.pages if Key in page)
        return {"ContentLength": len(data), "ETag": '"test"'}

    def get_object(self, *, Bucket, Key, Range, IfMatch):
        self.started.set()
        if not self.release.wait(5):
            raise RuntimeError("Test download was never released")
        if self.error:
            raise self.error
        data = next(page[Key] for page in self.pages if Key in page)
        start, end = map(int, Range.removeprefix("bytes=").split("-"))
        part = data[start:end + 1]
        return {
            "Body": StreamingBody(io.BytesIO(part), len(part)),
            "ContentLength": len(part),
            "ContentRange": f"bytes {start}-{end}/{len(data)}",
        }

    def close(self):
        self.closed = True


class BucketExportTest(unittest.TestCase):
    def setUp(self):
        self.s3 = FakeS3([{"folder/hello.txt": b"hello"}, {"terrain/0/0.terrain": b"mesh", "empty": b""}])
        self.exports = bucket_exports.BucketExports(lambda: self.s3)
        for patcher in (
            mock.patch.object(gui, "exports", self.exports),
            mock.patch.object(gui, "S3_BUCKET_NAMES", ["bucket", "other"]),
            mock.patch.object(gui, "_build_s3_client", return_value=self.s3),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)
        self.client = gui.app.test_client()

    def tearDown(self):
        self.s3.release.set()
        for job_id in list(self.exports._jobs):
            self.wait_for_export(job_id)
        if self.exports._directory:
            self.exports._directory.cleanup()

    def start_export(self):
        response = self.client.post("/exports", json={"bucket": "bucket"})
        self.assertEqual(response.status_code, 202)
        return response.json

    def wait_for_export(self, job_id):
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            job = self.exports.get(job_id)
            if job["status"] != "preparing":
                return job
            time.sleep(0.01)
        self.fail("Export did not finish")

    def test_download_includes_all_pages_and_preserves_paths_and_contents(self):
        with mock.patch.object(bucket_exports, "DOWNLOAD_RANGE_SIZE", 3):
            job = self.start_export()
            result = self.wait_for_export(job["id"])
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["objects"], 3)
        self.assertEqual(result["bytes"], 9)
        with self.client.get(job["download_url"]) as response:
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.mimetype, "application/zip")
            self.assertIn("bucket.zip", response.headers["Content-Disposition"])
            with zipfile.ZipFile(io.BytesIO(response.data)) as archive:
                self.assertEqual(
                    {name: archive.read(name) for name in archive.namelist()},
                    {key: value for page in self.s3.pages for key, value in page.items()},
                )
        self.assertTrue(self.s3.closed)

    def test_preparation_returns_before_s3_finishes_and_reconnects_after_refresh(self):
        self.s3.release.clear()
        job = self.start_export()
        self.assertTrue(self.s3.started.wait(2))
        self.assertEqual(self.client.get(job["status_url"]).json["status"], "preparing")
        self.assertEqual(self.client.get(job["download_url"]).status_code, 409)
        self.assertEqual(self.start_export()["id"], job["id"])
        self.assertEqual(self.client.post("/exports", json={"bucket": "other"}).status_code, 409)
        page = self.client.get("/?bucket=bucket").get_data(as_text=True)
        self.assertIn(job["id"], page)
        self.assertIn("Prepare bucket ZIP", page)
        self.s3.release.set()
        self.assertEqual(self.wait_for_export(job["id"])["status"], "ready")

    def test_completed_download_supports_resume_and_renews_retention(self):
        job = self.start_export()
        result = self.wait_for_export(job["id"])
        with self.client.get(job["download_url"]) as response:
            whole = response.data
            etag = response.headers["ETag"]
        with mock.patch.object(bucket_exports.time, "time", return_value=result["expires_at"] - 10):
            with self.client.get(job["download_url"], headers={"Range": "bytes=10-", "If-Range": etag}) as response:
                self.assertEqual(response.status_code, 206)
                self.assertEqual(response.data, whole[10:])
                self.assertEqual(response.headers["Content-Range"], f"bytes 10-{len(whole) - 1}/{len(whole)}")
        self.assertGreater(self.exports.get(job["id"])["expires_at"], result["expires_at"])

    def test_empty_bucket_produces_a_valid_empty_zip(self):
        self.s3.pages = [{}]
        job = self.start_export()
        self.assertEqual(self.wait_for_export(job["id"])["status"], "ready")
        with self.client.get(job["download_url"]) as response:
            with zipfile.ZipFile(io.BytesIO(response.data)) as archive:
                self.assertEqual(archive.namelist(), [])

    def test_failure_is_reported_and_partial_archive_is_removed(self):
        self.s3.error = RuntimeError("S3 connection lost")
        with self.assertLogs("bucket_exports", level="ERROR"):
            job = self.start_export()
            result = self.wait_for_export(job["id"])
        self.assertEqual(result["status"], "failed")
        self.assertIn("S3 connection lost", self.client.get(job["status_url"]).json["error"])
        self.assertFalse(self.exports.path(job["id"]).exists())
        self.assertEqual(self.client.get(job["download_url"]).status_code, 409)

    def test_expired_exports_are_removed_and_unavailable(self):
        job = self.start_export()
        result = self.wait_for_export(job["id"])
        path = self.exports.path(job["id"])
        with mock.patch.object(bucket_exports.time, "time", return_value=result["expires_at"] + 1):
            self.assertEqual(self.client.get(job["status_url"]).status_code, 404)
            self.assertEqual(self.client.get(job["download_url"]).status_code, 404)
        self.assertFalse(path.exists())

    def test_only_configured_buckets_can_be_exported(self):
        for data in ({"bucket": "unknown"}, {}, ["bucket"]):
            with self.subTest(data=data):
                self.assertEqual(self.client.post("/exports", json=data).status_code, 400)
        self.assertIsNone(self.exports._directory)

    def test_unsafe_archive_paths_are_rejected_without_renaming(self):
        for key in ("../outside", "/absolute", "a/../b", "a\\b", "C:/file", "a\x00b"):
            with self.subTest(key=key):
                with self.assertRaises(ValueError):
                    bucket_exports._validate_archive_key(key)

    def test_s3_read_timeout_is_retried_without_corrupting_zip(self):
        # Retry an interrupted response after reading part of a range, then
        # check that the ZIP contains exactly one copy of the object bytes.
        class InterruptedBody(io.BytesIO):
            def read(self, size=-1):
                if self.tell() >= 256 * 1024:
                    raise ReadTimeoutError(endpoint_url="https://s3.example.test")
                return super().read(size)

        client = boto3.client("s3", region_name="eu-central-1", aws_access_key_id="test", aws_secret_access_key="test")
        self.exports.client_factory = lambda: client
        data = b"sample terrain payload" * 30000
        real_sleep = time.sleep
        with Stubber(client) as stubber, mock.patch.object(
            bucket_exports.time, "sleep", side_effect=lambda seconds: real_sleep(min(seconds, 0.001))
        ):
            stubber.add_response("list_objects_v2", {"Contents": [{"Key": "terrain.tile", "Size": len(data)}]}, {"Bucket": "bucket"})
            stubber.add_response("head_object", {"ContentLength": len(data), "ETag": '"test"'}, {"Bucket": "bucket", "Key": "terrain.tile"})
            for body in (InterruptedBody(data), io.BytesIO(data)):
                stubber.add_response("get_object", {
                    "Body": StreamingBody(body, len(data)),
                    "ContentLength": len(data),
                    "ContentRange": f"bytes 0-{len(data) - 1}/{len(data)}",
                }, {"Bucket": "bucket", "Key": "terrain.tile", "Range": f"bytes=0-{len(data) - 1}", "IfMatch": '"test"'})
            job = self.start_export()
            self.assertEqual(self.wait_for_export(job["id"])["status"], "ready")
            self.assertEqual(self.exports.get(job["id"])["bytes"], len(data))
            stubber.assert_no_pending_responses()
        with zipfile.ZipFile(self.exports.path(job["id"])) as archive:
            self.assertEqual(archive.read("terrain.tile"), data)


if __name__ == "__main__":
    unittest.main()
