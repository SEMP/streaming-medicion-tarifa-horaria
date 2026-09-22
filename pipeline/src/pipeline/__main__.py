"""Arranca el pipeline en modo streaming contra el job server de Flink."""

from __future__ import annotations

import logging

import apache_beam as beam

from .config import Ajustes
from .esqueleto import construir, opciones


def main() -> int:
    logging.getLogger().setLevel(logging.INFO)
    ajustes = Ajustes.desde_entorno()
    logging.info("job server: %s · lecturas: %s", ajustes.job_endpoint, ajustes.topico_lecturas)

    with beam.Pipeline(options=opciones(ajustes, nombre="consumo-por-franja")) as pipeline:
        construir(pipeline, ajustes, grupo="g-consumo-franja")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
