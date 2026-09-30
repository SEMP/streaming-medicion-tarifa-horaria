"""Arranca el pipeline en modo streaming contra el job server de Flink."""

from __future__ import annotations

import logging

import apache_beam as beam

from .cadena import cadena
from .config import Ajustes
from .esqueleto import construir, opciones
from .franjas import CalendarioInvalido, cargar_calendario


def main() -> int:
    logging.getLogger().setLevel(logging.INFO)
    ajustes = Ajustes.desde_entorno()

    # El calendario se carga y valida **antes** de levantar el pipeline. Uno mal formado
    # tiene que romper el arranque con un mensaje claro, no aparecer como un resultado raro
    # tres horas después.
    try:
        calendario = cargar_calendario(ajustes.config_franjas)
    except (CalendarioInvalido, OSError) as error:
        logging.error("no se pudo cargar %s: %s", ajustes.config_franjas, error)
        return 2

    logging.info(
        "job server: %s · lecturas: %s → %s · franjas: %s",
        ajustes.job_endpoint,
        ajustes.topico_lecturas,
        ajustes.topico_consumo,
        ", ".join(calendario.nombres),   # es una property, no un método
    )

    with beam.Pipeline(options=opciones(ajustes, nombre="consumo-por-franja")) as pipeline:
        construir(
            pipeline,
            ajustes,
            grupo="g-consumo-franja",
            transformaciones=cadena(ajustes, calendario),
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
