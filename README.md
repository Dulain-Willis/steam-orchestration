# steam-orchestration

Airflow DAG code for orchestrating the [`steam-analytics`](https://github.com/Dulain-Willis/steam-analytics)
dbt project via [Cosmos](https://astronomer.github.io/astronomer-cosmos/), deployed onto
`steam-infra`'s EKS cluster (Airflow via the official Helm chart). Tracks
`steam-analytics` issue #25.

## Scope of this repo

- `dags/steam_dbt_dag.py` — one `DbtDag`, `schedule=None` (manual trigger only), no retries, an
  `SmtpNotifier` fires an email on any task failure.
- `Dockerfile` — bakes the `steam-analytics` dbt project into the Airflow image at build time.
- `.github/workflows/build-and-push.yml` — builds and pushes the image to GHCR, triggered
  automatically whenever `steam-analytics` merges to `main` (see below).

**Deliberately out of scope here** (see the issue #25 handoff comment for the full picture):
- The Gmail App Password / `smtp_default` connection's underlying k8s Secret — manual, lives in
  `steam-infra`.
- `fct_pipeline_runs` — the dbt model that lands prod run rows — lives in `steam-analytics`.
- `tofu destroy` teardown verification — `steam-infra`.
- Actually rolling a new image out to the live EKS Airflow deployment once it's pushed to GHCR
  (image tag bump + rollout) — `steam-infra`.

## Continuous deployment of dbt changes

`steam-analytics`'s CI (`update-dbt-artifacts-and-docs.yml`), after a merge to `main`, fires a
`repository_dispatch` event (`dbt-project-updated`, carrying the new commit sha) at this repo.
That triggers `build-and-push.yml` here, which rebuilds the image with
`STEAM_ANALYTICS_REF` set to that exact commit and pushes
`ghcr.io/dulain-willis/steam-orchestration:latest` (and a sha-tagged copy) to GHCR. No manual
ref-bumping — every `steam-analytics` merge produces a new image automatically.

The dispatch call needs a PAT (scoped to this repo, stored as `ORCHESTRATION_DISPATCH_TOKEN` in
`steam-analytics`'s secrets) since GitHub's own `GITHUB_TOKEN` can't trigger workflows in a
different repo. `build-and-push.yml` can also be run manually (`workflow_dispatch`) with an
explicit `steam_analytics_ref` input, for a one-off rebuild against an arbitrary ref.

## Why the dbt project is baked into the image, not fetched at pod-start

This project has a single dbt-project consumer and no need for independent dbt/Airflow deploy
cadences, so a runtime delivery mechanism (init container + shared volume, mirroring what
`astro dbt deploy` does under the hood) would be more moving parts than it buys back. A dbt
change becomes: pick a `steam-analytics` commit sha or tag → bump `STEAM_ANALYTICS_REF` here →
rebuild → `helm upgrade` the image tag. One release mechanism, not two.

The dbt project is fetched straight from `steam-analytics` at that pinned ref via GitHub's public
archive-by-ref tarball endpoint (`https://github.com/Dulain-Willis/steam-analytics/archive/<ref>.tar.gz`)
— no token, no GitHub Actions API or artifact involved, since `steam-analytics` is a public repo.
A full commit sha gives a fully reproducible build; a branch or tag name tracks whatever that ref
currently points at.

## Prerequisites for this DAG to actually run

- An Airflow **Connection** named `smtp_default` pointing at `smtp.gmail.com:587` with a Gmail
  App Password (steam-infra AC4 — manual, 2FA-gated, not scriptable).
- Env vars on the Airflow deployment: `PIPELINE_ALERT_EMAIL` (required — DAG import fails fast
  without it) and optionally `PIPELINE_ALERT_FROM_EMAIL`.
- `DBT_TARGET` (defaults to `prod`) and the Snowflake key-pair env vars documented in
  `steam-analytics/profiles.yml` (`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_DATABASE`,
  `SNOWFLAKE_PRIVATE_KEY_PATH`, ...).

## Building the image locally

```
docker build \
  --build-arg STEAM_ANALYTICS_REF=<a steam-analytics commit sha, tag, or branch> \
  -t steam-orchestration:local .
```

No credentials needed — `steam-analytics` is a public repo, so the archive tarball fetch is a
plain unauthenticated `curl`.

## Running tests

```
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -r requirements-dev.txt
source .venv/bin/activate
pytest tests/
```

Tests render the DAG against a tiny fixture dbt project (`tests/fixtures/minimal_dbt_project`) —
no Snowflake connection or the real `steam-analytics` project needed. `tests/conftest.py`
auto-provisions an isolated Airflow metastore for this.
