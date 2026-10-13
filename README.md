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
  automatically whenever `steam-analytics` **or this repo** merges to `main` (see below).

**Deliberately out of scope here** (see the issue #25 handoff comment for the full picture):
- The Gmail App Password / `smtp_default` connection's underlying k8s Secret — manual, lives in
  `steam-infra`.
- `fct_pipeline_runs` — the dbt model that lands prod run rows — lives in `steam-analytics`.
- `tofu destroy` teardown verification — `steam-infra`.
- Actually rolling a new image out to the live EKS Airflow deployment once it's pushed to GHCR
  (image tag bump + rollout) — `steam-infra`.

## Continuous deployment

One image is built from two inputs: the DAG code in this repo, and a `steam-analytics` ref the
`Dockerfile` bakes in. A merge to `main` in **either** repo rebuilds it.

| Trigger | When | What gets baked in |
|---|---|---|
| `repository_dispatch` (`dbt-project-updated`) | `steam-analytics` merges to `main` and dispatches its new sha | that exact sha |
| `push` to `main` | this repo merges to `main` | `steam-analytics`' current `main`, resolved at build time |
| `workflow_dispatch` | manual, one-off | the `steam_analytics_ref` input (branch, tag or sha), defaulting to `steam-analytics`' `main` |

Whichever the trigger, the ref is resolved to a full commit sha before the build, so the image
is reproducible even when a branch name was given.

The image tag is **`<orchestration sha>-<analytics sha>-<run id>`**, all three parts
load-bearing: the first two say what is in the image, and the run id makes the tag unique to one
build. That matters because promotion (below) is by tag — if two builds could share a tag,
"prod runs the image that ran in staging" would not be a true statement, and a rollback to an
older tag would not be a rollback to a known artefact. `:latest` is also pushed, as a
convenience; nothing deployed ever references it.

The dispatch call needs a PAT (scoped to this repo, stored as `ORCHESTRATION_DISPATCH_TOKEN` in
`steam-analytics`'s secrets) since GitHub's own `GITHUB_TOKEN` can't trigger workflows in a
different repo.

### Where the new image goes

The workflow commits the new tag into `steam-infra`'s **staging** image values
(`k8s/airflow/values-image-staging.yaml`), and ArgoCD rolls staging onto it. It never writes
prod's image values: prod is bumped only by `steam-infra`'s promote-to-prod workflow, which
opens a PR copying staging's current tag across. Merging that PR is the approval gate, and this
bot having no write path to prod's file is what makes the gate real rather than a convention.

So the path to prod is: merge → staging → trigger `steam_dbt_dag` in staging and see it green →
run promote-to-prod → review and merge its PR. The image is never rebuilt in between.

## Why the dbt project is baked into the image, not fetched at pod-start

This project has a single dbt-project consumer and no need for independent dbt/Airflow deploy
cadences, so a runtime delivery mechanism (init container + shared volume, mirroring what
`astro dbt deploy` does under the hood) would be more moving parts than it buys back. A dbt
change rides the same path as a DAG change — one image tag moving from staging to prod — rather
than needing a release mechanism of its own.

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

## Local development (the dev environment)

Local Airflow in Docker, running the **same GHCR image as staging and prod**, writing to the
**same developer schemas** your own `dbt build` in `steam-analytics` writes to. One sandbox,
whichever way you enter it.

Once per machine:

```
cp .env.example .env     # then fill it in — see the comments in it
```

`.env` holds your Snowflake account, developer user, role, database, **developer schema**, the
host path to your `.p8` key, and the path to your `steam-analytics` checkout. It is gitignored:
one per machine, not one per branch. Use the *same* `SNOWFLAKE_SCHEMA` as `steam-analytics`'s own
`.env`, or the DAG will build into a different sandbox than your direct dbt runs.

Also run `dbt deps` once in your `steam-analytics` checkout: the mount shadows the image's
`dbt_packages/`, and Cosmos parses the mounted project.

Then:

```
make up        # bring it up, wait for the DAG to parse, report import errors honestly
make trigger   # kick off a steam_dbt_dag run
make logs      # follow all containers
make down      # stop it
make test      # the DAG validity tests (no Docker, no Snowflake)
```

The UI is at <http://localhost:8080> with no login (`simple_auth_manager_all_admins`).
`docker compose down -v` additionally throws away the local Airflow metadata database.

What local Airflow connects to:

- **Snowflake**, as *you*: `DBT_TARGET=dev`, your `STEAM_DEV_<NAME>` user, the `DEVELOPER` role,
  and `STEAM_DEV` — a role with no access at all to `STEAM_STAGING` or `STEAM_PROD`. Models land
  in `<SNOWFLAKE_SCHEMA>_stg`, `_int`, `_marts_*`, ... exactly as a direct `dbt build` does.
  Sources read `STEAM_RAW`.
- **Nothing else.** Postgres is a throwaway container for Airflow's own metadata. Failure email
  is inert: `PIPELINE_ALERT_EMAIL` only has to be non-empty (the DAG reads it at import), and
  `smtp_default` points at a dead local URI, so no mail leaves your machine.

`make up` mounts `./dags` and your `steam-analytics` checkout over the image's baked-in copies,
so DAG edits and dbt edits (models, macros, `profiles.yml`) take effect on the next DAG parse —
no image rebuild. The dag-processor re-parses every ~30s; a dbt change costs an extra `dbt ls`
(up to a minute) before the new task graph shows up.

The image is built for `linux/amd64` only, so on Apple Silicon it runs emulated — correct, but
noticeably slower than a native `dbt build`.

## Building the image locally

```
docker build \
  --build-arg STEAM_ANALYTICS_REF=<a steam-analytics commit sha, tag, or branch> \
  -t steam-orchestration:local .
```

No credentials needed — `steam-analytics` is a public repo, so the archive tarball fetch is a
plain unauthenticated `curl`.

## Running tests

`make test` does all of this for you (it creates `.venv` on first run). By hand:

```
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -r requirements-dev.txt
source .venv/bin/activate
pytest tests/
```

Tests render the DAG against a tiny fixture dbt project (`tests/fixtures/minimal_dbt_project`) —
no Snowflake connection or the real `steam-analytics` project needed. `tests/conftest.py`
auto-provisions an isolated Airflow metastore for this.
