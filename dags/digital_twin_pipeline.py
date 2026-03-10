from airflow.sdk import DAG, Param
from airflow.sdk.definitions.param import ParamsDict
from datetime import datetime
from pipeline.tasks.download import download
from pipeline.tasks.extract_zip import extract_zip
from pipeline.tasks.convert_citygml_to_cityjson import makeConvertCityGMLToCityJSONTask
from pipeline.tasks.enrich_cityjson import makeEnrichCityJSONTask
from pipeline.tasks.convert_cityjson_to_3dtiles import makeConvertCityJSONTo3DTilesTask
from pipeline.tasks.convert_cityjson_to_citygml import makeConvertCityJSONToCityGMLTask
from pipeline.tasks.upload import upload

DAG_ID = "digital_twin_pipeline"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["S3", "Host", "Download"],
    params=ParamsDict({
        "bucket": Param(
            default="",
            type="string",
            description="Name of the S3 bucket containing the object",
        ),
        "key": Param(
            default="",
            type="string",
            description="Key (path) of the object in the bucket",
        ),
        "dest_dir": Param(
            default="",
            type="string",
            description="Host directory to save the downloaded file (defaults to storage dir)",
        ),
        "target_filename": Param(
            default="downloaded_file.zip",
            type="string",
            description="Filename to use for the downloaded object",
        ),
    }),
) as dag:
  download_task = download()
  extract_task = extract_zip()
  gml_to_json_task = makeConvertCityGMLToCityJSONTask("gml_in", "json")
  enrich_task = makeEnrichCityJSONTask("json")
  json_to_3d_task = makeConvertCityJSONTo3DTilesTask("json", "3d_tiles")
  json_to_gml_task = makeConvertCityJSONToCityGMLTask("json", "gml_out")
  upload_task = upload()

  _ = download_task >> extract_task >> gml_to_json_task >> enrich_task >> [json_to_3d_task, json_to_gml_task] >> upload_task