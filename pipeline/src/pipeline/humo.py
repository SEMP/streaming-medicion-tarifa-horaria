"""Prueba de humo del recorrido completo: simulador → Kafka → Beam → Kafka.

No verifica lógica de dominio —todavía no hay— sino que **el cableado funciona**: que
KafkaIO levanta, que Flink acepta el trabajo, que los bytes entran y salen.

Es la prueba que hay que correr primero cuando algo se rompe, porque separa "el pipeline
está mal" de "la infraestructura está mal", que son dos problemas muy distintos.

    docker compose -f infra/docker-compose.yml --profile humo up --build \
        --abort-on-container-exit --exit-code-from humo
"""

from __future__ import annotations

import logging
import sys
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import apache_beam as beam

from .config import Ajustes
from .esqueleto import construir, opciones

LECTURAS_DE_PRUEBA = 40
ESPERA_MAXIMA_SEGUNDOS = 240

log = logging.getLogger("humo")


def sembrar(ajustes: Ajustes) -> int:
    """Publica unas pocas lecturas sintéticas al tópico de entrada."""
    from simulador.agenda import ConfigAgenda, generar
    from simulador.fallas import ConfigFallas
    from simulador.parque import generar_parque
    from simulador.publicador import publicar

    tz = ZoneInfo(ajustes.zona_horaria)
    inicio = datetime.now(tz).replace(hour=0, minute=0, second=0, microsecond=0)
    parque = generar_parque(cabinas=2, inicio=inicio, dias=1, semilla=2026)
    cfg = ConfigAgenda(inicio=inicio, fin=inicio + timedelta(hours=2))

    lecturas = list(generar(parque, cfg, ConfigFallas(), semilla=2026))[:LECTURAS_DE_PRUEBA]
    enviadas = publicar(
        lecturas, servidores=ajustes.servidores_kafka, topico=ajustes.topico_lecturas
    )
    log.info("sembradas %d lecturas en %s", enviadas, ajustes.topico_lecturas)
    return enviadas


def correr_pipeline(ajustes: Ajustes, cuantas: int) -> None:
    """Corre el pipeline **acotado**: sin `max_num_records` no terminaría nunca."""
    log.info("lanzando el pipeline contra %s", ajustes.job_endpoint)
    with beam.Pipeline(
        options=opciones(ajustes, nombre="humo", streaming=False)
    ) as pipeline:
        construir(pipeline, ajustes, grupo="g-humo", max_registros=cuantas)


def verificar_salida(ajustes: Ajustes, esperadas: int) -> int:
    """Consume el tópico de salida y comprueba que llegaron los mensajes."""
    from confluent_kafka import Consumer

    consumidor = Consumer(
        {
            "bootstrap.servers": ajustes.servidores_kafka,
            "group.id": "g-humo-verificador",
            "auto.offset.reset": "earliest",
        }
    )
    consumidor.subscribe([ajustes.topico_consumo])

    vistos = 0
    limite = time.monotonic() + ESPERA_MAXIMA_SEGUNDOS
    try:
        while vistos < esperadas and time.monotonic() < limite:
            mensaje = consumidor.poll(2.0)
            if mensaje is None or mensaje.error():
                continue
            vistos += 1
            if vistos == 1:
                log.info("primer mensaje de salida: %s", mensaje.value()[:120])
    finally:
        consumidor.close()
    return vistos


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", stream=sys.stdout)
    ajustes = Ajustes.desde_entorno()

    enviadas = sembrar(ajustes)
    correr_pipeline(ajustes, enviadas)
    recibidas = verificar_salida(ajustes, enviadas)

    log.info("entraron %d · salieron %d", enviadas, recibidas)
    if recibidas < enviadas:
        log.error(
            "el recorrido no se completó: faltan %d mensajes en %s",
            enviadas - recibidas,
            ajustes.topico_consumo,
        )
        return 1

    log.info("✅ el recorrido simulador → Kafka → Beam → Kafka funciona")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
