"""Configuración del pipeline, tomada del entorno.

Vive en un solo lugar para que el simulador, el pipeline y las herramientas de consola no
puedan discrepar sobre a qué tópico escribe cada uno.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Ajustes:
    servidores_kafka: str
    topico_lecturas: str
    topico_consumo: str
    topico_cuarentena: str
    job_endpoint: str
    paralelismo: int
    config_franjas: str
    zona_horaria: str

    @classmethod
    def desde_entorno(cls) -> Ajustes:
        return cls(
            servidores_kafka=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
            topico_lecturas=os.environ.get("TOPICO_LECTURAS", "medicion.lecturas.v1"),
            topico_consumo=os.environ.get("TOPICO_CONSUMO", "medicion.consumo-franja.v1"),
            topico_cuarentena=os.environ.get("TOPICO_CUARENTENA", "medicion.cuarentena.v1"),
            job_endpoint=os.environ.get("BEAM_JOB_ENDPOINT", "localhost:8099"),
            paralelismo=int(os.environ.get("BEAM_PARALLELISM", "2")),
            config_franjas=os.environ.get("CONFIG_FRANJAS", "config/franjas.example.toml"),
            zona_horaria=os.environ.get("ZONA_HORARIA", "America/Asuncion"),
        )
