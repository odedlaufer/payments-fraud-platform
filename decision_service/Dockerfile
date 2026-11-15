# Image for the online feature sink and (Phase 3.4) the decision service.
# Build context is the repository root, so the uv workspace and its lock resolve.
FROM ghcr.io/astral-sh/uv:0.11.25 AS uv

FROM python:3.12.12-slim
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never \
    PATH="/app/.venv/bin:${PATH}"
WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
COPY simulator/pyproject.toml simulator/pyproject.toml
COPY features features
COPY rules rules
COPY decision_service decision_service
RUN uv sync --frozen --no-default-groups --package payments-decision \
 && useradd --system --no-create-home app

USER app
