FROM ghcr.io/astral-sh/uv:0.8.4 AS uv

FROM python:3.12.11-slim-bookworm

COPY --from=uv /uv /uvx /bin/

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    NEMO_DATA_DIR=/app/data \
    NEMO_ASSET_DIR=/app/assets \
    PATH=/app/.venv/bin:$PATH

WORKDIR /app

COPY refactored_nemo_app/pyproject.toml refactored_nemo_app/uv.lock refactored_nemo_app/README.md ./
RUN uv sync --frozen --no-dev --no-install-project

COPY refactored_nemo_app/src ./src
RUN uv sync --frozen --no-dev

COPY Columbia_logo.png nemo_logo.jpeg Account-project-user-adding-Nemo.xlsx ./assets/

RUN useradd --create-home --uid 10001 nemo \
    && mkdir -p /app/data \
    && chown -R nemo:nemo /app

USER nemo

EXPOSE 8000

CMD ["gunicorn", "--bind=0.0.0.0:8000", "--workers=2", "--threads=4", "--timeout=60", "nemo_app.web.app:create_app()"]
