import io
from pathlib import Path
import re
import sys
import unittest
from unittest import mock
import zipfile


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import app as gui


class FakeS3:
    def __init__(self):
        self.uploads = []
        self.deleted = []

    def list_objects_v2(self, **kwargs):
        return {"Contents": []}

    def upload_fileobj(self, fileobj, bucket, key):
        self.uploads.append((bucket, key, fileobj.read()))

    def delete_object(self, Bucket, Key):
        self.deleted.append((Bucket, Key))

    def get_paginator(self, name):
        return self

    def paginate(self, **kwargs):
        yield {"Contents": [{"Key": "a"}]}

    def delete_objects(self, Bucket, Delete):
        self.deleted.extend((Bucket, obj["Key"]) for obj in Delete["Objects"])


def _zip_bytes():
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("inner/file.txt", b"zip")
    return buffer.getvalue()


class CsrfProtectionTest(unittest.TestCase):
    def setUp(self):
        self.s3 = FakeS3()
        self.exports = mock.Mock()
        self.exports.latest.return_value = None
        self.exports.start.return_value = {"id": "job", "bucket": "bucket", "status": "preparing"}
        for patcher in (
            mock.patch.object(gui, "S3_BUCKET_NAMES", ["bucket"]),
            mock.patch.object(gui, "_build_s3_client", return_value=self.s3),
            mock.patch.object(gui, "exports", self.exports),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)
        self.client = gui.app.test_client()

    def page_token(self):
        page = self.client.get("/?bucket=bucket").get_data(as_text=True)
        tokens = set(re.findall(r'name="csrf_token" value="([^"]+)"', page))
        self.assertEqual(len(tokens), 1, "every form must carry the same session token")
        token = tokens.pop()
        self.assertIn(f"'X-CSRF-Token': \"{token}\"", page)
        return token

    def requests(self, token=None):
        extra = {} if token is None else {"csrf_token": token}
        return {
            "/upload": dict(
                data={"bucket": "bucket", "file": (io.BytesIO(b"x"), "f.txt"), **extra},
                content_type="multipart/form-data",
            ),
            "/upload-zip": dict(
                data={"bucket": "bucket", "file": (io.BytesIO(_zip_bytes()), "f.zip"), **extra},
                content_type="multipart/form-data",
            ),
            "/delete": dict(data={"bucket": "bucket", "key": "a", **extra}),
            "/clear": dict(data={"bucket": "bucket", **extra}),
        }

    def assert_untouched(self):
        self.assertEqual(self.s3.uploads, [])
        self.assertEqual(self.s3.deleted, [])
        self.exports.start.assert_not_called()

    def test_state_changing_routes_reject_missing_token(self):
        self.page_token()
        for path, kwargs in self.requests().items():
            with self.subTest(path=path):
                self.assertEqual(self.client.post(path, **kwargs).status_code, 400)
        self.assertEqual(self.client.post("/exports", json={"bucket": "bucket"}).status_code, 400)
        self.assert_untouched()

    def test_state_changing_routes_reject_wrong_token(self):
        self.page_token()
        for path, kwargs in self.requests("forged").items():
            with self.subTest(path=path):
                self.assertEqual(self.client.post(path, **kwargs).status_code, 400)
        response = self.client.post(
            "/exports", json={"bucket": "bucket"}, headers={"X-CSRF-Token": "forged"}
        )
        self.assertEqual(response.status_code, 400)
        self.assert_untouched()

    def test_token_from_another_session_is_rejected(self):
        token = self.page_token()
        other = gui.app.test_client()
        other.get("/")
        response = other.post("/delete", data={"bucket": "bucket", "key": "a", "csrf_token": token})
        self.assertEqual(response.status_code, 400)
        self.assert_untouched()

    def test_state_changing_routes_accept_session_token(self):
        token = self.page_token()
        for path, kwargs in self.requests(token).items():
            with self.subTest(path=path):
                self.assertEqual(self.client.post(path, **kwargs).status_code, 303)
        self.assertEqual(
            [key for _, key, _ in self.s3.uploads], ["f.txt", "inner/file.txt"]
        )
        self.assertEqual(self.s3.deleted, [("bucket", "a"), ("bucket", "a")])
        response = self.client.post(
            "/exports", json={"bucket": "bucket"}, headers={"X-CSRF-Token": token}
        )
        self.assertEqual(response.status_code, 202)

    def test_session_cookie_is_strict_and_http_only(self):
        response = self.client.get("/")
        cookie = response.headers["Set-Cookie"]
        self.assertTrue(cookie.startswith("s3_gui_session="))
        self.assertIn("HttpOnly", cookie)
        self.assertIn("SameSite=Strict", cookie)


class BindAddressTest(unittest.TestCase):
    def test_defaults_to_loopback(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            self.assertEqual(gui._server_address(), ("127.0.0.1", 3000))

    def test_container_override(self):
        env = {"S3_GUI_HOST": "0.0.0.0", "S3_GUI_INTERNAL_PORT": "3100"}
        with mock.patch.dict("os.environ", env, clear=True):
            self.assertEqual(gui._server_address(), ("0.0.0.0", 3100))

    def test_dockerfile_listens_in_container_and_drops_root(self):
        dockerfile = (Path(__file__).resolve().parents[1] / "Dockerfile").read_text()
        self.assertRegex(dockerfile, r"S3_GUI_HOST=0\.0\.0\.0")
        users = re.findall(r"^USER\s+(\S+)", dockerfile, re.MULTILINE)
        self.assertTrue(users)
        self.assertNotIn(users[-1].split(":")[0], {"root", "0"})


if __name__ == "__main__":
    unittest.main()
