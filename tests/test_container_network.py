import os
import sys
import unittest


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))

from airflow.providers.docker.operators.docker import DockerOperator

from digital_twin_pipeline import dag as digital_twin_dag
from dgm1_terrain_pipeline import dag as terrain_dag
from pipeline.operators.job_docker_operator import JobDockerOperator


CONVERTER_TASKS = {
    digital_twin_dag.dag_id: {
        "convert_citygml_to_cityjson",
        "enrich_cityjson",
        "generate_tiles",
        "convert_cityjson_to_citygml",
    },
    terrain_dag.dag_id: {"inspect_sources", "build_vrt", "generate_terrain"},
}


class ContainerNetworkTest(unittest.TestCase):
    def test_every_converter_container_runs_without_network(self):
        for dag in (digital_twin_dag, terrain_dag):
            docker_tasks = {
                task.task_id: task
                for task in dag.tasks
                if isinstance(task, DockerOperator)
            }
            with self.subTest(dag=dag.dag_id):
                self.assertEqual(set(docker_tasks), CONVERTER_TASKS[dag.dag_id])
            for task_id, task in docker_tasks.items():
                with self.subTest(dag=dag.dag_id, task=task_id):
                    self.assertEqual(task.network_mode, "none")

    def test_job_operator_defaults_to_no_network(self):
        task = JobDockerOperator(task_id="probe", image="example")
        self.assertEqual(task.network_mode, "none")


if __name__ == "__main__":
    unittest.main()
