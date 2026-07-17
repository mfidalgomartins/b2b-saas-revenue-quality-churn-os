FROM python:3.12.13-slim-bookworm@sha256:8a7e7cc04fd3e2bd787f7f24e22d5d119aa590d429b50c95dfe12b3abe52f48b

LABEL org.opencontainers.image.title="B2B SaaS Revenue Quality OS" \
      org.opencontainers.image.description="Reproducible revenue-quality analytics release job" \
      org.opencontainers.image.source="https://github.com/mfidalgomartins/b2b-saas-revenue-quality-churn-os"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    MPLCONFIGDIR=/tmp/matplotlib \
    XDG_CACHE_HOME=/tmp/.cache

WORKDIR /app

RUN groupadd --gid 10001 revenue_os \
    && useradd --uid 10001 --gid revenue_os --create-home --shell /usr/sbin/nologin revenue_os

COPY requirements-dev.lock pyproject.toml README.md ./
RUN python -m pip install --no-cache-dir --require-hashes -r requirements-dev.lock

COPY --chown=revenue_os:revenue_os . .
RUN python -m pip install --no-deps --no-build-isolation -e .

USER revenue_os

CMD ["python", "-m", "src.pipeline.run_project_pipeline", "--base-dir", ".", "--seed", "42"]
