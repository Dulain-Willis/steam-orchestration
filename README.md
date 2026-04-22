# steam-orchestration

Airflow DAGs for the Steam Data Platform.

## DAGs

### `steamspy`

Extract → bronze → silver pipeline. Triggered manually.

- `force_refresh` (bool, default `false`): skip extraction and reuse the latest landing partition
- Task order: `should_extract` → `extract` → `resolve_partition` → `bronze` → `silver_stg`

### `replication`

Replicates Iceberg silver tables into ClickHouse. Triggered manually after `steamspy` completes.

- `full_load` (bool, default `false`): ignore snapshot state, reload everything
- `from_snapshot` (int, optional): override start snapshot ID for backfills

## Airflow UI

```
http://localhost:8080   (admin / admin)
```

## Docker Image

CI builds and pushes `ghcr.io/Dulain-Willis/steam-orchestration:latest` on every merge to `main`.

## Architecture Decisions

ADRs live in [`steam-data-platform/docs/decisions/`](../steam-data-platform/docs/decisions/).
