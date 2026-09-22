# Imagen de la aplicación: corre el simulador y lanza el pipeline.
# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    HOME=/app

# Java hace falta acá también: el cliente que arma el pipeline levanta el expansion
# service de KafkaIO antes de enviarlo al job server.
RUN apt-get update \
    && apt-get install --yes --no-install-recommends openjdk-17-jre-headless curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

COPY simulador ./simulador
COPY pipeline ./pipeline
COPY config ./config
RUN uv sync --frozen --no-dev

# Se cachea el JAR del expansion service durante el build. Sin esto, el primer pipeline
# que arranque se queda esperando una descarga de Maven, y en una demo eso parece que el
# sistema está colgado.
RUN /app/.venv/bin/python -c "\
from apache_beam.io.kafka import default_io_expansion_service; \
s = default_io_expansion_service(); a = s.__enter__(); print('expansion service:', a); \
s.__exit__(None, None, None)"

RUN useradd --create-home --uid 10001 medicion \
    && mkdir -p /app/datos \
    && chown -R medicion:medicion /app
USER medicion

CMD ["/app/.venv/bin/python", "-m", "simulador", "--help"]
