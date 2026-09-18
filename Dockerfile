# syntax=docker/dockerfile:1
#
# Bakes the steam-analytics dbt project into the Airflow image at build time.
#
# Why baked in rather than fetched at pod-start: this project has one
# dbt-project consumer and no need for independent dbt/Airflow deploy
# cadences, so a second runtime delivery path (init container + volume) would
# just be more moving parts than an image tag bump buys back. See
# steam-analytics issue #25 handoff discussion.
#
# STEAM_ANALYTICS_REF pins the exact steam-analytics commit this image bakes
# in — bump it deliberately (like any dependency version) to pick up new dbt
# code. Any git ref works (branch, tag, or commit sha); use a full commit sha
# for a reproducible build. Fetched via GitHub's archive-by-ref tarball
# endpoint (https://github.com/<owner>/<repo>/archive/<ref>.tar.gz) — no
# token needed since steam-analytics is a public repo, and no GitHub Actions
# API/artifact involved.
#
# Build with: docker build --build-arg STEAM_ANALYTICS_REF=<sha-or-tag> .

ARG AIRFLOW_VERSION=3.2.2
ARG STEAM_ANALYTICS_REF

FROM alpine:3.20 AS dbt-repo-fetcher
SHELL ["/bin/sh", "-o", "pipefail", "-c"]
RUN apk add --no-cache curl
ARG STEAM_ANALYTICS_REF
WORKDIR /bundle
RUN test -n "${STEAM_ANALYTICS_REF}" || (echo "STEAM_ANALYTICS_REF build arg is required" >&2 && exit 1)
RUN curl \
      --fail \
      --silent \
      --show-error \
      --location \
      "https://github.com/Dulain-Willis/steam-analytics/archive/${STEAM_ANALYTICS_REF}.tar.gz" \
    | tar -xz --strip-components=1

FROM apache/airflow:${AIRFLOW_VERSION}-python3.12

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY dags/ /opt/airflow/dags/
COPY --from=dbt-repo-fetcher /bundle /opt/airflow/dbt/steam_analytics

RUN cd /opt/airflow/dbt/steam_analytics && dbt deps
