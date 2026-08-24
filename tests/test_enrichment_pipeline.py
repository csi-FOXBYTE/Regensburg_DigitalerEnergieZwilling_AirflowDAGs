import os
import sys
import unittest
from unittest import mock


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))

from pipeline import config, s3_connection
from pipeline.tasks import download
from pipeline.tasks.enrich_cityjson import make_enrich_cityjson_task
from digital_twin_pipeline import dag as digital_twin_dag


class EnrichmentPipelineTest(unittest.TestCase):
    def test_3d_tiles_converter_is_pinned_to_0_0_24(self):
        self.assertRegex(
            config.JSON_TO_3D_TILES_IMAGE,
            r":0\.0\.24@sha256:[0-9a-f]{64}$",
        )

    def test_digital_twin_cleanup_can_be_skipped(self):
        self.assertFalse(digital_twin_dag.params.get("skip_cleanup"))
        cleanup_task = digital_twin_dag.get_task("cleanup")
        self.assertTrue(cleanup_task.op_kwargs["honor_skip_cleanup"])

    def test_gml_bucket_is_cleared_before_upload(self):
        clear_task = digital_twin_dag.get_task("clear_gml_bucket")

        self.assertEqual(clear_task.op_kwargs["bucket_param"], "gml_output_bucket")
        self.assertEqual(
            clear_task.upstream_task_ids,
            {"convert_cityjson_to_citygml"},
        )
        self.assertEqual(clear_task.downstream_task_ids, {"upload_gml"})

    def test_digital_twin_s3_tasks_use_the_dag_connection(self):
        s3_task_ids = {
            "download_file_task",
            "download_gpkg_task",
            "clear_tiles_bucket",
            "clear_gml_bucket",
            "upload_tiles",
            "upload_address_db",
            "upload_gml",
        }

        for task_id in s3_task_ids:
            task = digital_twin_dag.get_task(task_id)
            self.assertEqual(task.op_kwargs["aws_conn_id"], "det_rg_s3")

    def test_s3_connection_lookup_is_explicit_and_fail_closed(self):
        connection = mock.Mock(conn_type="aws")
        hook = mock.Mock()
        with (
            mock.patch.object(
                s3_connection.BaseHook,
                "get_connection",
                return_value=connection,
            ) as get_connection,
            mock.patch.object(
                s3_connection,
                "S3Hook",
                return_value=hook,
            ) as s3_hook,
        ):
            result = s3_connection.get_s3_hook("det_rg_s3")

        self.assertIs(result, hook)
        get_connection.assert_called_once_with("det_rg_s3")
        s3_hook.assert_called_once_with(aws_conn_id="det_rg_s3")

        with mock.patch.object(
            s3_connection.BaseHook,
            "get_connection",
            side_effect=RuntimeError("connection missing"),
        ):
            with self.assertRaisesRegex(RuntimeError, "connection missing"):
                s3_connection.get_s3_hook("det_rg_s3")

    def test_enrichment_uses_version_0_7_0_and_pipeline_parameters(self):
        task = make_enrich_cityjson_task(
            "json",
            "enriched_json",
            "address_db",
            with_age_zones=True,
            with_geothermal=True,
        )

        self.assertRegex(
            config.ENRICH_IMAGE,
            r":0\.7\.0@sha256:[0-9a-f]{64}$",
        )
        self.assertEqual(digital_twin_dag.params.get("municipality_key"), "09362000")
        self.assertEqual(
            task.environment["MUNICIPALITY_KEY"],
            "{{ params.municipality_key }}",
        )
        self.assertEqual(
            task.environment["AGE_ZONES_FILE"],
            "{{ '/work/gpkg/age_zones.gpkg' if params.get('age_zones_key') else '' }}",
        )
        self.assertEqual(
            task.environment["GEOTHERMAL_FILE"],
            "{{ '/work/gpkg/geothermal.gpkg' if params.get('geothermal_key') else '' }}",
        )

    def test_download_gpkg_fetches_age_zones_and_geothermal_data(self):
        params = {
            "bucket": "input",
            "age_zones_key": "reference/Baualtersklassen.gpkg",
            "geothermal_key": "reference/Geothermie.gpkg",
        }

        with (
            mock.patch.object(download, "get_job_dir", return_value="/work/job"),
            mock.patch.object(download, "download_from_s3") as download_from_s3,
            mock.patch.object(download.mf, "update_step") as update_step,
        ):
            download._download_gpkg_callable(
                params,
                "manual__test",
                "download_gpkg_task",
                "det_rg_s3",
            )

        self.assertEqual(
            download_from_s3.call_args_list,
            [
                mock.call(
                    "input",
                    "reference/Baualtersklassen.gpkg",
                    "/work/job/gpkg/age_zones.gpkg",
                    "det_rg_s3",
                ),
                mock.call(
                    "input",
                    "reference/Geothermie.gpkg",
                    "/work/job/gpkg/geothermal.gpkg",
                    "det_rg_s3",
                ),
            ],
        )
        self.assertEqual(
            update_step.call_args_list,
            [
                mock.call("/work/job", "download_gpkg_task", "running"),
                mock.call("/work/job", "download_gpkg_task", "success"),
            ],
        )

    def test_download_gpkg_is_skipped_without_optional_keys(self):
        with (
            mock.patch.object(download, "get_job_dir", return_value="/work/job"),
            mock.patch.object(download, "download_from_s3") as download_from_s3,
            mock.patch.object(download.mf, "update_step") as update_step,
        ):
            download._download_gpkg_callable(
                {"bucket": "input"},
                "manual__test",
                "download_gpkg_task",
                "det_rg_s3",
            )

        download_from_s3.assert_not_called()
        update_step.assert_called_once_with(
            "/work/job",
            "download_gpkg_task",
            "skipped",
        )


if __name__ == "__main__":
    unittest.main()
