from airflow.decorators import dag, task
from airflow.operators.python import get_current_context, ShortCircuitOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

from pipelines.steamspy.extract import call_steamspy_api
from pipelines.common.spark.config import get_s3a_conf, get_spark_resource_conf, get_iceberg_catalog_conf
from pipelines.common.storage.minio_client import create_minio_client

bucket_name = 'landing'
_LANDING_PREFIX = "steamspy/raw/request=all/"


@dag(
    dag_id="steamspy",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "force_refresh": False,  # Default: skip extraction, reuse existing landing data
    },
)
def steamspy():

    def check_should_extract(**context) -> bool:

        is_run_full_refresh = context["params"].get("force_refresh", False)
        print(f"force_refresh parameter: {is_run_full_refresh}")

        if is_run_full_refresh:
            print("Extraction will run (force_refresh=True)")
        else:
            print("Skipping extraction (force_refresh=False). Will resolve latest landing partition.")

        return is_run_full_refresh

    should_extract = ShortCircuitOperator(
        task_id="should_extract",
        python_callable=check_should_extract,
        ignore_downstream_trigger_rules=False,
    )

    @task
    def extract():
        """Extract data from SteamSpy API and upload to landing zone."""
        ctx = get_current_context()
        ds = ctx["ds"]
        run_id = ctx["run_id"]

        pages_uploaded = call_steamspy_api(bucket=bucket_name, ds=ds, run_id=run_id)

        return {"ds": ds, "run_id": run_id, "pages_uploaded": pages_uploaded}


    @task(trigger_rule="none_failed")
    def resolve_partition(**context) -> str:
        """Determine which landing-zone partition bronze/silver should process.

        When force_refresh=True the DAG just ran extraction and wrote data under
        today's logical date (ds), so that value is returned directly.

        When force_refresh=False extraction was skipped; the task lists the
        MinIO landing bucket to find the latest available dt= partition so the
        pipeline can re-process existing data without the caller having to
        remember which date was originally extracted.
        """
        is_run_full_refresh = context["params"].get("force_refresh", False)
        date_string = context["ds"]

        if is_run_full_refresh:
            print(f"force_refresh=True — using current execution date: {date_string}")
            return date_string

        minio_client = create_minio_client()
        # list_objects() lists every object in a given file path
        raw_payload_minio_objects = minio_client.list_objects(bucket_name, prefix=_LANDING_PREFIX, recursive=False)

        dates_source_data_was_extraced = []

        for object in raw_payload_minio_objects:
            # object_name looks like "steamspy/raw/request=all/dt=2025-01-15/"
            object_datetime = object.object_name.rstrip("/").split("/")[-1]

            if object_datetime.startswith("dt="):
                dates_source_data_was_extraced.append(object_datetime[3:])

        if not dates_source_data_was_extraced:
            raise ValueError(
                "No partitions found under landing/steamspy/raw/request=all/. "
                "Run with force_refresh=True to extract data first."
            )

        latest_date_source_data_was_extracted = max(dates_source_data_was_extraced)
        print(f"force_refresh=False — resolved latest landing partition: {latest_date_source_data_was_extracted}")
        return latest_date_source_data_was_extracted

    extract_task = extract()
    resolve_partition_task = resolve_partition()


    bronze = SparkSubmitOperator(
        task_id="bronze",
        application="/opt/spark/jobs/bronze/bronze_steamspy_games.py",
        conn_id="spark_default",
        conf={
            **get_s3a_conf(),
            **get_spark_resource_conf(),
            **get_iceberg_catalog_conf(),
            "spark.steamspy.ds": "{{ task_instance.xcom_pull(task_ids='resolve_partition') }}",
            "spark.steamspy.run_id": "{{ run_id }}",
        },
        trigger_rule="none_failed",
    )

    silver_stg = SparkSubmitOperator(
        task_id="silver_stg",
        application="/opt/spark/jobs/staging/stg/silver_stg_steamspy_games.py",
        conn_id="spark_default",
        conf={
            **get_s3a_conf(),
            **get_spark_resource_conf(),
            **get_iceberg_catalog_conf(),
            "spark.steamspy.ds": "{{ task_instance.xcom_pull(task_ids='resolve_partition') }}",
            "spark.steamspy.run_id": "{{ run_id }}",
        },
        trigger_rule="none_failed_min_one_success",
    )

    should_extract >> extract_task >> resolve_partition_task >> bronze >> silver_stg


dag = steamspy()
