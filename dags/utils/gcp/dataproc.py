from constants.constants import GCP_PROJECT_ID, GCP_CLUSTER_NAME, GCS_BUCKET_NAME
from typing import Dict, Any, List, Union

ADDITIONAL_PYTHON_FILES = ["gs://property-dashboard/spark-job/property_spark.zip"]


def get_cluster_config(
    project_id: str = GCP_PROJECT_ID,
    zone: str = "us-central1-a",
    gcs_bucket_name: str = GCS_BUCKET_NAME
):
    from airflow.providers.google.cloud.operators.dataproc import ClusterGenerator
    
    return ClusterGenerator(
    project_id=project_id,
    zone=zone,
    master_machine_type="n1-standard-2",
    worker_machine_type="n1-standard-2",
    num_workers=2,
    worker_disk_size=300,
    master_disk_size=300,
    storage_bucket=gcs_bucket_name,
    metadata={"PIP_PACKAGES": "delta-spark requests pandas openpyxl"},

    ).make()


def get_spark_submit_job_driver(
    main_file: str,
    entry_point_arguments: Union[str, List[str]],
    additional_files: Union[str, List[str]] = ADDITIONAL_PYTHON_FILES,
    cluster_name: str = GCP_CLUSTER_NAME,
    project_id: str = GCP_PROJECT_ID
):

    return {
    "reference": {"project_id": project_id},
    "placement": {"cluster_name": cluster_name},
    "pyspark_job": {"main_python_file_uri": main_file,
                    "args": entry_point_arguments,
                    "properties": { 
                        "spark.jars.packages": "io.delta:delta-core_2.12:1.0.1",
                        },
                    "python_file_uris": additional_files},
}
