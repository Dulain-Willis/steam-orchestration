# DAG Deploy Guide

## Safe to deploy anytime

- Bug fixes or logic changes inside a task (same inputs/outputs)
- Changing what a Spark job computes (same output schema)
- Schedule changes (cron expression)
- Retries, timeouts, alerts
- New DAGs
- Adding a task at the end of an existing DAG

## Requires caution — pause the DAG first

Pause the DAG, wait for any active runs to finish, then deploy.

- Renaming a task ID
- Changing task dependencies (reordering, adding mid-graph)
- Changing an XCom key that a downstream task reads
- Changing an output path or schema that a later task in the same run consumes
- Removing a task

## How to pause

```bash
airflow dags pause <dag_id>
# wait for active runs: airflow dags list-runs -d <dag_id> --state running
airflow dags unpause <dag_id>
```

DAG versioning (`my_dag_v2`) is only needed if the pipeline runs so frequently or is so SLA-critical that you cannot afford any pause window. For this platform, pause + deploy is sufficient.
