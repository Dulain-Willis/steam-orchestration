"""One project-wide DAG rendering the steam_analytics dbt project via Cosmos.

Manual trigger only (schedule=None) per steam-analytics issue #25. No retries:
a failed task should surface immediately by email rather than mask the
failure behind a retry.
"""

import os
from pathlib import Path

from airflow.providers.smtp.notifications.smtp import SmtpNotifier
from cosmos import DbtDag, ExecutionConfig, ExecutionMode, ProfileConfig, ProjectConfig

DBT_PROJECT_DIR = Path(os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt/steam_analytics"))

profile_config = ProfileConfig(
    profile_name="steam_analytics",
    target_name=os.environ.get("DBT_TARGET", "prod"),
    profiles_yml_filepath=DBT_PROJECT_DIR / "profiles.yml",
)

on_failure_notifier = SmtpNotifier(
    smtp_conn_id="smtp_default",
    to=os.environ["PIPELINE_ALERT_EMAIL"],
    from_email=os.environ.get("PIPELINE_ALERT_FROM_EMAIL", "airflow@steam-analytics.local"),
    subject="[steam-analytics] {{ ti.dag_id }} failed: {{ ti.task_id }}",
    html_content="Task <code>{{ ti.task_id }}</code> in DAG <code>{{ ti.dag_id }}</code> failed.<br>"
    "Log: {{ ti.log_url }}",
)

steam_dbt_dag = DbtDag(
    dag_id="steam_dbt_dag",
    schedule=None,
    default_args={
        "retries": 0,
        "on_failure_callback": on_failure_notifier,
    },
    project_config=ProjectConfig(DBT_PROJECT_DIR),
    profile_config=profile_config,
    execution_config=ExecutionConfig(execution_mode=ExecutionMode.LOCAL),
)
