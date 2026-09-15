import gzip
import json
import os
import struct
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))

from pipeline import config, manifest
from pipeline.tasks import cleanup, terrain_processing, terrain_publish
from pipeline.tasks.terrain_processing import CTB_COMMAND
from pipeline.tasks.terrain_sources import EXPECTED_SOURCE_COUNT, parse_metalink
from pipeline.terrain_validation import (
    ATTRIBUTION,
    inspect_quantized_mesh,
    is_gzip_encoded,
    validate_tileset,
)
from dgm1_terrain_pipeline import dag as terrain_dag


class TerrainPipelineTest(unittest.TestCase):
    def test_publish_uses_the_terrain_dag_connection(self):
        task = terrain_dag.get_task("publish_terrain")

        self.assertEqual(task.op_kwargs["aws_conn_id"], "det_rg_s3")

    def test_project_metalink_deduplicates_repeated_urls(self):
        path = Path(__file__).resolve().parents[1] / "dags" / "dgm1.meta4"
        sources = parse_metalink(path)

        self.assertEqual(len(sources), EXPECTED_SOURCE_COUNT)
        self.assertEqual(sum(len(item["urls"]) for item in sources), EXPECTED_SOURCE_COUNT)
        self.assertEqual(len({item["urls"][0] for item in sources}), EXPECTED_SOURCE_COUNT)

    def test_images_are_digest_pinned_and_ctb_flags_are_explicit(self):
        self.assertRegex(config.GDAL_IMAGE, r"@sha256:[0-9a-f]{64}$")
        self.assertRegex(config.CTB_IMAGE, r"@sha256:[0-9a-f]{64}$")
        for option in ("-f Mesh", "-C", "-N", "-s 18", "-e 0", "-l"):
            self.assertIn(option, CTB_COMMAND)

    def test_validation_checks_all_zooms_normals_and_http_loading(self):
        with tempfile.TemporaryDirectory() as directory:
            terrain_dir = Path(directory)
            available = []
            for zoom in range(19):
                tile_path = terrain_dir / str(zoom) / "0" / "0.terrain"
                tile_path.parent.mkdir(parents=True)
                prefix = b"\x1f\x8b\x95" if zoom == 18 else b""
                tile_path.write_bytes(_quantized_mesh_tile(prefix=prefix))
                available.append(
                    [{"startX": 0, "startY": 0, "endX": 0, "endY": 0}]
                )
            layer = {
                "tilejson": "2.1.0",
                "format": "quantized-mesh-1.0",
                "scheme": "tms",
                "projection": "EPSG:4326",
                "extensions": ["octvertexnormals"],
                "tiles": ["{z}/{x}/{y}.terrain?v={version}"],
                "minzoom": 0,
                "maxzoom": 18,
                "available": available,
                "attribution": ATTRIBUTION,
            }
            (terrain_dir / "layer.json").write_text(json.dumps(layer), encoding="utf-8")

            with mock.patch(
                "pipeline.terrain_validation._cesium_http_smoke",
                return_value={
                    "layer_loaded": True,
                    "tile": "0/0/0.terrain",
                    "tile_bytes": len(_quantized_mesh_tile()),
                },
            ):
                report = validate_tileset(terrain_dir)

        self.assertEqual(report["tile_count"], 19)
        self.assertEqual(set(report["representative_tiles"]), {"0", "9", "18"})
        self.assertTrue(report["cesium_http_smoke"]["layer_loaded"])

    def test_gzip_detection_requires_the_deflate_compression_method(self):
        self.assertTrue(is_gzip_encoded(b"\x1f\x8b\x08\x00"))
        self.assertFalse(is_gzip_encoded(b"\x1f\x8b\x95\xcd"))

        with tempfile.TemporaryDirectory() as directory:
            tile_path = Path(directory) / "collision.terrain"
            tile_path.write_bytes(_quantized_mesh_tile(prefix=b"\x1f\x8b\x95"))
            details = inspect_quantized_mesh(tile_path)

        self.assertEqual(details["vertex_count"], 3)
        self.assertEqual(details["triangle_count"], 1)
        self.assertEqual(details["extension_ids"], [1])

    def test_publish_clears_bucket_and_uploads_layer_last(self):
        with tempfile.TemporaryDirectory() as directory:
            work_dir = Path(directory)
            run_id = "manual__terrain"
            job_dir = Path(config.get_job_dir(run_id))
            with (
                mock.patch.object(config, "WORK_DIR", str(work_dir)),
                mock.patch.object(manifest, "WORK_DIR", str(work_dir)),
            ):
                job_dir = Path(config.get_job_dir(run_id))
                (job_dir / "terrain" / "0" / "0").mkdir(parents=True)
                (job_dir / "dgm1_metadata").mkdir()
                (job_dir / "terrain" / "0" / "0" / "0.terrain").write_bytes(
                    _quantized_mesh_tile()
                )
                (job_dir / "terrain" / "layer.json").write_text(
                    '{"format":"quantized-mesh-1.0"}',
                    encoding="utf-8",
                )
                manifest.create_manifest(
                    str(job_dir),
                    "manual--terrain",
                    run_id,
                    "dgm1_terrain_pipeline",
                    {"terrain_output_bucket": "terrain"},
                    ["publish_terrain"],
                )
                client = _FakeS3Client()
                hook = mock.Mock()
                hook.get_conn.return_value = client
                with (
                    mock.patch.object(
                        terrain_publish,
                        "get_s3_hook",
                        return_value=hook,
                    ) as get_s3_hook,
                    mock.patch.object(terrain_publish, "DGM1_UPLOAD_WORKERS", 1),
                ):
                    terrain_publish._publish_terrain_callable(
                        {"terrain_output_bucket": "terrain"},
                        run_id,
                        "publish_terrain",
                        "det_rg_s3",
                    )

            self.assertEqual(client.deleted, ["obsolete"])
            self.assertEqual(client.uploads[-1][0], "layer.json")
            self.assertEqual(client.uploads[0][0], "0/0/0.terrain")
            self.assertNotIn("ContentEncoding", client.uploads[0][1])
            get_s3_hook.assert_called_once_with("det_rg_s3")

    def test_normalization_streams_gzip_and_fixes_ctb_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            job_dir = Path(directory)
            metadata_dir = job_dir / "dgm1_metadata"
            terrain_dir = job_dir / "terrain"
            metadata_dir.mkdir()
            terrain_dir.mkdir()
            with gzip.open(terrain_dir / "tile.terrain", "wb") as file:
                file.write(_quantized_mesh_tile())
            collision_path = terrain_dir / "collision.terrain"
            collision_tile = _quantized_mesh_tile(prefix=b"\x1f\x8b\x95")
            collision_path.write_bytes(collision_tile)
            (terrain_dir / "layer.json").write_text(
                json.dumps(
                    {
                        "format": "quantized-mesh-1.0",
                        "schema": "tms",
                        "bounds": [0.0, -90.0, 180.0, 90.0],
                        "available": [[] for _zoom in range(19)],
                    }
                ),
                encoding="utf-8",
            )
            (metadata_dir / "sha256.txt").write_text(
                f"{'a' * 64}  /work/dgm1_sources/source.tif\n",
                encoding="utf-8",
            )
            (metadata_dir / "source_validation.json").write_text(
                '{"bounds_wgs84":[12.0,48.9,12.2,49.1]}',
                encoding="utf-8",
            )
            manifest.create_manifest(
                str(job_dir),
                "smoke",
                "smoke",
                "dgm1_terrain_pipeline",
                {},
                ["normalize_and_prepare_layer"],
            )
            with mock.patch.object(
                terrain_processing,
                "get_job_dir",
                return_value=str(job_dir),
            ):
                terrain_processing._normalize_and_prepare_layer_callable(
                    "smoke",
                    "normalize_and_prepare_layer",
                )

            self.assertEqual(
                (terrain_dir / "tile.terrain").read_bytes(),
                _quantized_mesh_tile(),
            )
            self.assertEqual(collision_path.read_bytes(), collision_tile)
            normalization = json.loads(
                (metadata_dir / "terrain_normalization.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(normalization["terrain_file_count"], 2)
            self.assertEqual(normalization["gzip_files_normalized"], 1)
            layer = json.loads((terrain_dir / "layer.json").read_text(encoding="utf-8"))
            self.assertNotIn("schema", layer)
            self.assertEqual(layer["scheme"], "tms")
            self.assertEqual(layer["bounds"], [-180.0, -90.0, 180.0, 90.0])
            self.assertEqual(layer["bbox"], [12.0, 48.9, 12.2, 49.1])
            self.assertEqual(layer["attribution"], ATTRIBUTION)

    def test_skip_cleanup_retains_debug_artifacts(self):
        with tempfile.TemporaryDirectory() as directory:
            run_id = "manual__terrain"
            with (
                mock.patch.object(config, "WORK_DIR", directory),
                mock.patch.object(manifest, "WORK_DIR", directory),
            ):
                job_dir = Path(config.get_job_dir(run_id))
                artifact = job_dir / "terrain" / "layer.json"
                artifact.parent.mkdir(parents=True)
                artifact.write_text("{}", encoding="utf-8")
                manifest.create_manifest(
                    str(job_dir),
                    "manual--terrain",
                    run_id,
                    "dgm1_terrain_pipeline",
                    {"skip_cleanup": True},
                    ["cleanup"],
                )

                cleanup._cleanup_callable(
                    ["terrain"],
                    run_id,
                    "cleanup",
                    params={"skip_cleanup": True},
                    honor_skip_cleanup=True,
                )

                with (job_dir / "manifest.json").open(encoding="utf-8") as file:
                    result = json.load(file)
                self.assertTrue(artifact.is_file())
                self.assertEqual(result["steps"]["cleanup"]["status"], "skipped")


class _FakeS3Client:
    def __init__(self):
        self.deleted = []
        self.uploads = []
        self.list_calls = 0

    def list_objects_v2(self, **_kwargs):
        self.list_calls += 1
        if self.list_calls == 1:
            return {"Contents": [{"Key": "obsolete"}]}
        return {}

    def delete_objects(self, Bucket, Delete):
        self.deleted.extend(item["Key"] for item in Delete["Objects"])
        return {}

    def upload_file(self, filename, bucket, key, ExtraArgs):
        self.uploads.append((key, ExtraArgs))


def _quantized_mesh_tile(prefix: bytes = b"") -> bytes:
    data = bytearray(88)
    data[: len(prefix)] = prefix
    data.extend(struct.pack("<I", 3))
    data.extend(b"\x00" * 18)
    data.extend(struct.pack("<I", 1))
    data.extend(struct.pack("<HHH", 0, 0, 0))
    for _edge in range(4):
        data.extend(struct.pack("<I", 1))
        data.extend(struct.pack("<H", 0))
    data.extend(struct.pack("<BI", 1, 6))
    data.extend(b"\x00" * 6)
    return bytes(data)


if __name__ == "__main__":
    unittest.main()
