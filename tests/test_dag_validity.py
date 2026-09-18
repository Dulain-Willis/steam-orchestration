"""DAG-integrity tests for steam_dbt_dag.

Renders the DAG against a tiny fixture dbt project (no network/warehouse
access) to check schedule, retry, and alerting config without needing a real
Snowflake connection or the real steam-analytics project.
"""

import importlib.util
from pathlib import Path

import pytest
from airflow.providers.smtp.notifications.smtp import SmtpNotifier

REPO_ROOT = Path(__file__).parent.parent
DAG_FILE = REPO_ROOT / "dags" / "steam_dbt_dag.py"
FIXTURE_PROJECT_DIR = Path(__file__).parent / "fixtures" / "minimal_dbt_project"


def _load_dag_module():
    spec = importlib.util.spec_from_file_location("steam_dbt_dag_under_test", DAG_FILE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def base_env(monkeypatch):
    monkeypatch.setenv("DBT_PROJECT_DIR", str(FIXTURE_PROJECT_DIR))
    monkeypatch.setenv("PIPELINE_ALERT_EMAIL", "pipeline-alerts@example.com")
    monkeypatch.setenv("DBT_TARGET", "test")


def test_dag_has_no_schedule(base_env):
    dag = _load_dag_module().steam_dbt_dag
    assert dag.schedule is None


def test_dag_has_no_retries_and_notifies_on_failure(base_env):
    dag = _load_dag_module().steam_dbt_dag
    assert dag.default_args["retries"] == 0
    notifier = dag.default_args["on_failure_callback"]
    assert isinstance(notifier, SmtpNotifier)
    assert notifier.to == "pipeline-alerts@example.com"


def test_dag_renders_tasks_from_fixture_project(base_env):
    dag = _load_dag_module().steam_dbt_dag
    assert len(dag.task_ids) >= 1


def test_missing_alert_email_fails_fast(monkeypatch):
    monkeypatch.setenv("DBT_PROJECT_DIR", str(FIXTURE_PROJECT_DIR))
    monkeypatch.delenv("PIPELINE_ALERT_EMAIL", raising=False)
    with pytest.raises(KeyError):
        _load_dag_module()
