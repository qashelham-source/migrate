FROM python:3.11-slim AS builder

ENV PIP_NO_CACHE_DIR=1 \
    VIRTUAL_ENV=/opt/venv

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/* \
    && python -m venv "$VIRTUAL_ENV"

ENV PATH="$VIRTUAL_ENV/bin:$PATH"
WORKDIR /build
COPY requirements.txt ./
RUN python -m pip install --upgrade pip \
    && pip install --requirement requirements.txt \
    && pip uninstall --yes setuptools wheel jaraco.context \
    && python -m pip uninstall --yes pip


FROM python:3.11-slim AS runtime

ARG APP_UID=10001
ARG APP_GID=10001

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH"

# Packaging tools are unnecessary at runtime. Removing both the global copies
# inherited from the base image and the virtualenv copies reduces attack surface.
RUN python -m pip uninstall --yes setuptools wheel jaraco.context \
    && python -m pip uninstall --yes pip \
    && groupadd --gid "$APP_GID" migration \
    && useradd --uid "$APP_UID" --gid "$APP_GID" --create-home --shell /usr/sbin/nologin migration

WORKDIR /app
COPY --from=builder /opt/venv /opt/venv
COPY --chown=migration:migration . .

RUN mkdir -p /app/data /app/sessions /app/downloads/active /app/downloads/failed /app/downloads/completed \
    && touch /app/config.yaml \
    && chown -R migration:migration /app

USER migration:migration

CMD ["python", "main.py", "run"]
