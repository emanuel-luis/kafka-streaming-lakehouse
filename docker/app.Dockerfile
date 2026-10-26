# syntax=docker/dockerfile:1
# Phase 1 has one runtime target, `app` (no JVM). Phase 3 adds `spark` (JRE, pyspark, baked jars).

FROM ghcr.io/astral-sh/uv:0.13.5 AS uv

FROM python:3.12.15-slim-bookworm AS build-base
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never
WORKDIR /app

# Dependencies first: this layer only changes when pyproject.toml or uv.lock do.
FROM build-base AS build-app
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    uv sync --locked --no-dev --no-editable --no-install-project
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable

# Same base as the builder, so the venv's interpreter path and ABI match.
FROM python:3.12.15-slim-bookworm AS runtime-base
RUN groupadd --system --gid 10001 app \
    && useradd --system --uid 10001 --gid app --create-home app \
    && mkdir /runs \
    && chown app:app /runs
ENV PATH="/opt/venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1
WORKDIR /home/app

FROM runtime-base AS app
COPY --from=build-app /opt/venv /opt/venv
USER app
# No image-level HEALTHCHECK: most app containers are one-shot CLI runs, and the long-running
# ones get a probe in compose.yaml.
ENTRYPOINT ["lakestream"]
