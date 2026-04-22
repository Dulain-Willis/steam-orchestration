# SteamSpy Pipeline Commands

## Overview

The SteamSpy pipeline has been optimized for rapid development iteration. The key improvement is **decoupling extraction from transformation**, allowing you to re-run Spark jobs without waiting 1+ hour for API extraction.

---

## Quick Reference

### Run Pipeline Skipping Extraction (Default Behavior)

```bash
# Skips extraction, reuses existing bronze data
docker exec airflow-scheduler airflow dags trigger steamspy
```

**What happens**:
- `should_extract` task returns `False` → `extract` task is skipped
- Rest of the pipeline runs against existing data

### Run Pipeline Including Extraction (Full Refresh)

```bash
# Forces full extraction from SteamSpy API
docker exec airflow-scheduler airflow dags trigger steamspy --conf '{"force_refresh": true}'
```

**What happens**:
- All tasks run, including extraction
- New bronze data is created with current `ds` (execution date)

---

## Development Workflows

### Workflow 1: Iterate on Silver Transformation

**Scenario**: You're fixing a bug or adding a feature to `spark_jobs/steamspy/silver.py`

```bash
# 1. Make changes to silver.py
vim spark_jobs/steamspy/silver.py

# 2. Rebuild Spark containers
docker compose build spark-master spark-worker
docker compose restart spark-master spark-worker

# 3. Re-run pipeline (skips extraction)
docker exec airflow-scheduler airflow dags trigger steamspy

# 4. Repeat steps 1-3 as needed
```

**Time per iteration**: ~5 minutes (vs. 1 hour if extraction ran every time)

### Workflow 2: Use Historical Bronze Data

**Scenario**: You want to test against a specific past extraction without triggering a new one

```bash
# List available bronze partitions
docker exec minio ls /data/bronze/steamspy/normalized/

# Example output:
# dt=2026-02-15/
# dt=2026-02-10/
# dt=2026-02-01/

# Trigger DAG with specific execution date
docker exec airflow-scheduler airflow dags trigger steamspy \
  --exec-date 2026-02-15T12:00:00+00:00

# This will use bronze data from dt=2026-02-15
```

### Workflow 3: Run Specific Tasks Manually

**Scenario**: Only the `silver` task failed, and you want to re-run just that task

```bash
# Get the DAG run ID from logs or web UI
# Format: manual__YYYY-MM-DDTHH:MM:SS+00:00

# Clear the failed task (marks it as eligible to re-run)
docker exec airflow-scheduler airflow tasks clear steamspy \
  -t silver \
  -d manual__2026-02-15T18:47:26+00:00

# The task will automatically re-run
```

**Alternative - Run task directly without Airflow**:

```bash
# SSH into Spark master container
docker exec -it spark-master bash

# Set execution date manually
export SPARK_CONF_spark_steamspy_ds=2026-02-15
export SPARK_CONF_spark_steamspy_run_id=test_run_001

# Run the silver job directly
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.iceberg.type=rest \
  --conf spark.sql.catalog.iceberg.uri=http://iceberg-rest:8181 \
  --conf spark.sql.catalog.iceberg.warehouse=s3a://silver/iceberg/ \
  --conf spark.steamspy.ds=2026-02-15 \
  --conf spark.steamspy.run_id=test_run_001 \
  /opt/spark/jobs/steamspy/silver.py
```

---

## How the Guard Logic Works

### Task: `should_extract`

Located in `dags/steamspy.py`:

```python
should_extract = ShortCircuitOperator(
    task_id="should_extract",
    python_callable=check_should_extract,
)
```

**Behavior**:
- Returns `True` if `force_refresh=True` → extraction runs
- Returns `False` if `force_refresh=False` (default) → extraction is skipped via `ShortCircuitOperator`

**ShortCircuitOperator**: If the callable returns `False`, all downstream tasks in the same branch are skipped, but tasks in parallel branches continue.

**Dependency Chain**:
```
should_extract >> extract >> bronze >> silver >> load_clickhouse
```

When `should_extract` returns `False`:
- `extract` is **skipped**
- `bronze`, `silver`, `load_clickhouse` **continue** (they run against existing data)

---

## Apache Iceberg Benefits

### What Changed in the Silver Layer

**Before (Plain Parquet)**:
```python
output_path = f"s3a://silver/steamspy/dt={ds}/"
df_final.write.mode("overwrite").parquet(output_path)
```

**After (Iceberg Table)**:
```python
table_name = "iceberg.steamspy.games"
df_final.writeTo(table_name).overwritePartitions()
```

### Why This Matters

1. **Atomic Commits**: If the silver job crashes mid-write, readers still see the previous complete snapshot. No partial/corrupted data.

2. **Snapshot Isolation**: ClickHouse can read from the table while Spark is writing new data. No table locking.

3. **Metadata Tracking**: Iceberg REST catalog tracks schema versions, snapshots, and partition manifests. You can query table history:
   ```sql
   SELECT * FROM iceberg.steamspy.games.snapshots;
   ```

4. **Future Evolution Path**: Easy to switch from `overwritePartitions()` to `MERGE` (upsert) without table migration:
   ```python
   target_table.merge(df_final, "target.appid = source.appid")
       .whenMatchedUpdateAll()
       .whenNotMatchedInsertAll()
       .execute()
   ```

---

## Verification Commands

### Check Bronze Data Exists

```bash
docker exec minio ls /data/bronze/steamspy/normalized/dt=2026-02-15/
```

### Check Iceberg Table Was Created

```bash
# Verify table exists in REST catalog
curl http://localhost:8181/v1/namespaces/steamspy/tables

# Expected output: ["games"]
```

### Query Iceberg Table via Spark SQL

```bash
docker exec -it spark-master spark-shell

# In Spark shell:
spark.sql("SHOW NAMESPACES IN iceberg").show()
spark.sql("SHOW TABLES IN iceberg.steamspy").show()
spark.sql("SELECT COUNT(*) FROM iceberg.steamspy.games").show()
spark.sql("SELECT * FROM iceberg.steamspy.games WHERE dt='2026-02-15' LIMIT 10").show()
```

### Check Iceberg Data Files in MinIO

```bash
docker exec minio ls /data/silver/iceberg/steamspy/games/data/dt=2026-02-15/
```

### Verify ClickHouse Loaded Data

```bash
docker exec clickhouse clickhouse-client

# In ClickHouse:
SELECT COUNT(*) FROM analytics.steamspy_silver WHERE dt = '2026-02-15';
SELECT * FROM analytics.steamspy_silver WHERE dt = '2026-02-15' LIMIT 10;
```

---

## Common Issues & Solutions

### Issue: "Table already exists" Error in Silver Job

**Cause**: Iceberg table was created in a previous run, but the job tries to `create()` again.

**Solution**: Already handled in code - `spark.catalog.tableExists()` check prevents this.

### Issue: "Connection refused to iceberg-rest:8181"

**Cause**: Iceberg REST catalog container is not running or not accessible from Spark containers.

**Solution**:
```bash
# Verify container is running
docker ps | grep iceberg-rest

# Check logs
docker logs iceberg-rest

# Restart if needed
docker compose restart iceberg-rest
```

### Issue: Silver Job Fails with S3 Access Denied

**Cause**: Iceberg catalog config is missing S3 credentials.

**Solution**: Verify `.env` file has correct MinIO credentials and rebuild containers:
```bash
cat .env | grep MINIO
docker compose build spark-master spark-worker
docker compose restart spark-master spark-worker
```

### Issue: DAG Stays in "Queued" State

**Cause**: DAG is paused.

**Solution**:
```bash
docker exec airflow-scheduler airflow dags unpause steamspy
```

---

## Performance Tips

### Reduce Memory Usage During Development

Set page limit in extract task (edit `src/pipelines/steamspy/extract.py`):

```python
max_pages = int(os.getenv("STEAMSPY_MAX_PAGES", "5"))  # Default: 5 pages for dev
```

Rebuild Airflow containers:
```bash
docker compose build airflow-scheduler airflow-webserver
docker compose restart airflow-scheduler airflow-webserver
```

This reduces extraction from 1 hour to ~5 minutes for testing.

### Reduce Shuffle Partitions

For small datasets, reduce shuffle partitions in Spark config (already set to 8 in `get_spark_resource_conf()`):

```python
"spark.sql.shuffle.partitions": "4"  # Reduce from 8 to 4
```

---

## Next Steps: MERGE/Upsert Evolution

Once the pipeline is stable, you can evolve from full partition overwrites to incremental updates using Iceberg's `MERGE` operation:

**Current (SCD Type 1 - Full Overwrite)**:
```python
df_final.writeTo(table_name).overwritePartitions()
```

**Future (Incremental Upsert)**:
```python
from pyspark.sql import DataFrame

target_table = spark.table(table_name)
(target_table.alias("target").merge(
    df_final.alias("source"),
    "target.appid = source.appid AND target.dt = source.dt")
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute())
```

**Benefits**:
- Only updates changed rows (more efficient for large tables)
- Preserves historical data in other partitions
- Single atomic snapshot for all updates + inserts

**No Migration Needed**: Same Iceberg table works for both approaches. Schema evolution is automatic.

---

## Additional Resources

- **Airflow Best Practices**: https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html
- **Iceberg Spark Writes**: https://iceberg.apache.org/docs/1.5.2/spark-writes/
- **Iceberg Table Evolution**: https://iceberg.apache.org/docs/1.5.2/evolution/
- **ShortCircuitOperator Docs**: https://airflow.apache.org/docs/apache-airflow/stable/operators.html#shortcircuitoperator

---

## Summary

**Default Workflow (Most Common)**:
```bash
# Make code changes
vim spark_jobs/steamspy/silver.py

# Rebuild containers
docker compose build spark-master spark-worker
docker compose restart spark-master spark-worker

# Run transformations only (5 minutes)
docker exec airflow-scheduler airflow dags trigger steamspy
```

**When You Need Fresh Data**:
```bash
# Extract new data (1 hour)
docker exec airflow-scheduler airflow dags trigger steamspy --conf '{"force_refresh": true}'
```

This pattern enables rapid iteration during development while maintaining the option for fresh data extraction when needed.