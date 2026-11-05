import hashlib
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))

from pipeline import config, manifest
from pipeline.tasks import terrain_sources
from pipeline.tasks.terrain_sources import (
    EXPECTED_SOURCE_COUNT,
    attach_reference_hashes,
    parse_metalink,
    parse_reference_metalink,
)


PROJECT_METALINK = Path(__file__).resolve().parents[1] / "dags" / "dgm1.meta4"
REFERENCE_URL = "https://geodaten.bayern.de/odd/a/dgm/dgm1/meta/metalink/09362000.meta4"
TILE = b"II*\x00 tiny stand-in for a DGM1 GeoTIFF"
TILE_SHA256 = hashlib.sha256(TILE).hexdigest()


def _reference_metalink(entries: dict[str, tuple[str, int]]) -> bytes:
    """Provider (BVV-MetaLinker) format: size, sha-256 hash and two mirrors."""
    files = "".join(
        f'  <file name="{name}">\n'
        f"    <size>{size}</size>\n"
        f'    <hash type="sha-256">{digest}</hash>\n'
        f"    <url>https://download1.bayernwolke.de/a/dgm/dgm1/{name}</url>\n"
        f"    <url>https://download2.bayernwolke.de/a/dgm/dgm1/{name}</url>\n"
        "  </file>\n"
        for name, (digest, size) in entries.items()
    )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<metalink xmlns="urn:ietf:params:xml:ns:metalink">\n'
        "  <generator>BVV-MetaLinker</generator>\n"
        f"{files}</metalink>\n"
    ).encode()


def _source(name="720_5432.tif", sha256=TILE_SHA256, size=len(TILE)):
    return {
        "name": name,
        "urls": [f"https://download1.bayernwolke.de/a/dgm/dgm1/{name}"],
        "sha256": sha256,
        "size": size,
    }


class ReferenceHashTest(unittest.TestCase):
    def test_provider_metalink_hashes_are_attached_to_every_source(self):
        sources = parse_metalink(PROJECT_METALINK)
        reference = _reference_metalink(
            {item["name"]: (f"{index:064x}", 1000 + index) for index, item in enumerate(sources)}
        )
        with mock.patch.object(
            terrain_sources, "fetch_reference_metalink", return_value=reference
        ) as fetch:
            result = attach_reference_hashes(sources, [REFERENCE_URL])

        fetch.assert_called_once_with(REFERENCE_URL)
        self.assertEqual(len(result), EXPECTED_SOURCE_COUNT)
        self.assertTrue(all(len(item["sha256"]) == 64 for item in result))
        self.assertEqual(result[0]["size"], 1000)

    def test_source_without_published_hash_fails_closed(self):
        sources = [_source("720_5432.tif"), _source("721_5432.tif")]
        for source in sources:
            source.pop("sha256")
            source.pop("size")
        reference = _reference_metalink({"720_5432.tif": (TILE_SHA256, len(TILE))})
        with mock.patch.object(
            terrain_sources, "fetch_reference_metalink", return_value=reference
        ):
            with self.assertRaisesRegex(ValueError, r"No published SHA-256.*721_5432\.tif"):
                attach_reference_hashes(sources, [REFERENCE_URL])

    def test_no_reference_metalinks_and_no_pinned_hash_fails_closed(self):
        source = _source()
        source.pop("sha256")
        with self.assertRaisesRegex(ValueError, "No published SHA-256"):
            attach_reference_hashes([source], [])

    def test_hash_pinned_in_project_metalink_needs_no_provider_metalink(self):
        self.assertEqual(attach_reference_hashes([_source()], [])[0]["sha256"], TILE_SHA256)

    def test_conflicting_references_fail(self):
        source = _source()
        reference = _reference_metalink({source["name"]: ("b" * 64, len(TILE))})
        with mock.patch.object(
            terrain_sources, "fetch_reference_metalink", return_value=reference
        ):
            with self.assertRaisesRegex(ValueError, "conflicts"):
                attach_reference_hashes([source], [REFERENCE_URL])

    def test_reference_metalink_rejects_dtd_and_invalid_hashes(self):
        with self.assertRaisesRegex(ValueError, "DTD"):
            parse_reference_metalink(
                b'<!DOCTYPE m [<!ENTITY x "y">]><metalink/>', REFERENCE_URL
            )
        with self.assertRaisesRegex(ValueError, "Invalid SHA-256"):
            parse_reference_metalink(
                _reference_metalink({"720_5432.tif": ("not-a-hash", 1)}), REFERENCE_URL
            )

    def test_reference_metalink_requires_https(self):
        with self.assertRaisesRegex(ValueError, "HTTPS"):
            terrain_sources.fetch_reference_metalink(
                "http://geodaten.bayern.de/odd/a/dgm/dgm1/meta/metalink/09362000.meta4"
            )

    def test_reference_metalinks_default_to_the_provider_over_https(self):
        self.assertTrue(config.DEFAULT_DGM1_REFERENCE_METALINKS)
        for url in config.DEFAULT_DGM1_REFERENCE_METALINKS:
            self.assertTrue(url.startswith("https://geodaten.bayern.de/"), url)


class DownloadVerificationTest(unittest.TestCase):
    def setUp(self):
        self._directory = tempfile.TemporaryDirectory()
        self.destination = Path(self._directory.name)
        sleep = mock.patch.object(terrain_sources.time, "sleep")
        sleep.start()
        self.addCleanup(sleep.stop)
        self.addCleanup(self._directory.cleanup)

    def _serve(self, payload: bytes):
        def download(_url, partial_path):
            partial_path.write_bytes(payload)

        return mock.patch.object(terrain_sources, "_download_url", side_effect=download)

    def test_matching_download_is_accepted(self):
        with self._serve(TILE):
            result = terrain_sources._download_source(_source(), self.destination)

        self.assertEqual(result["status"], "downloaded")
        self.assertEqual(result["sha256"], TILE_SHA256)
        self.assertEqual((self.destination / "720_5432.tif").read_bytes(), TILE)

    def test_hash_mismatch_fails_and_keeps_nothing(self):
        tampered = TILE.replace(b"tiny", b"evil")
        self.assertEqual(len(tampered), len(TILE))
        with self._serve(tampered) as download:
            with self.assertRaises(RuntimeError) as raised:
                terrain_sources._download_source(_source(), self.destination)

        self.assertIn("SHA-256 mismatch", str(raised.exception.__cause__))
        self.assertEqual(download.call_count, terrain_sources.DOWNLOAD_ATTEMPTS)
        self.assertEqual(list(self.destination.iterdir()), [])

    def test_size_mismatch_fails(self):
        with self._serve(TILE + b"x"):
            with self.assertRaises(RuntimeError) as raised:
                terrain_sources._download_source(_source(), self.destination)

        self.assertIn("Size mismatch", str(raised.exception.__cause__))
        self.assertFalse((self.destination / "720_5432.tif").exists())

    def test_existing_file_is_reused_only_when_it_matches(self):
        (self.destination / "720_5432.tif").write_bytes(TILE)
        with self._serve(TILE) as download:
            result = terrain_sources._download_source(_source(), self.destination)
        self.assertEqual(result["status"], "reused")
        download.assert_not_called()

        (self.destination / "720_5432.tif").write_bytes(b"stale")
        with self._serve(TILE) as download:
            result = terrain_sources._download_source(_source(), self.destination)
        self.assertEqual(result["status"], "downloaded")
        self.assertEqual((self.destination / "720_5432.tif").read_bytes(), TILE)


class SourceReportCrossCheckTest(unittest.TestCase):
    def test_inspected_hash_must_match_the_reference(self):
        with tempfile.TemporaryDirectory() as directory:
            job_dir = Path(directory)
            metadata_dir = job_dir / "dgm1_metadata"
            metadata_dir.mkdir()
            (metadata_dir / "sources.json").write_text(
                json.dumps({"sources": [_source()]}), encoding="utf-8"
            )
            (metadata_dir / "sha256.txt").write_text(
                f"{'c' * 64}  /work/dgm1_sources/720_5432.tif\n", encoding="utf-8"
            )
            manifest.create_manifest(
                str(job_dir), "smoke", "smoke", "dgm1_terrain_pipeline", {},
                ["validate_source_reports"],
            )
            with mock.patch.object(
                terrain_sources, "get_job_dir", return_value=str(job_dir)
            ):
                with self.assertRaisesRegex(ValueError, "differs from the published reference"):
                    terrain_sources._validate_source_reports_callable(
                        "smoke", "validate_source_reports"
                    )
            state = json.loads((job_dir / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(state["steps"]["validate_source_reports"]["status"], "failed")


if __name__ == "__main__":
    unittest.main()
