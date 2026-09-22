"""Publicación de lecturas a Kafka.

Está aparte del resto del simulador a propósito: `agenda.generar()` produce lecturas sin
saber a dónde van, así que el simulador **sigue funcionando sin Kafka** —escribiendo
JSONL— y nadie queda bloqueado esperando la infraestructura.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable

from .evento import Lectura

log = logging.getLogger(__name__)


def publicar(
    lecturas: Iterable[Lectura],
    *,
    servidores: str,
    topico: str,
    cada_cuantos_avisa: int = 1000,
) -> int:
    """Publica las lecturas y devuelve cuántas salieron.

    La **clave es el medidor**, no el evento: manda todas las lecturas de un mismo equipo a
    la misma partición, con lo que se preserva su orden relativo. Esa garantía es la que
    permite que la etapa de diferenciación reste lecturas consecutivas con sentido.
    """
    from confluent_kafka import Producer

    productor = Producer({"bootstrap.servers": servidores, "linger.ms": 50})
    enviadas = 0

    def informar(error, _registro):
        if error is not None:
            log.warning("no se pudo publicar: %s", error)

    for lectura in lecturas:
        # El timestamp del record de Kafka se fija al instante de PUBLICACIÓN, no al de
        # lectura: es el tiempo de llegada. El tiempo de evento viaja en el payload y lo
        # asigna el pipeline al parsear — no se confunden.
        productor.produce(
            topico,
            key=lectura.clave.encode(),
            value=lectura.a_json().encode(),
            timestamp=int(lectura.publicado_at.timestamp() * 1000),
            headers=lectura.headers(),
            on_delivery=informar,
        )
        enviadas += 1
        if enviadas % cada_cuantos_avisa == 0:
            productor.poll(0)
            log.info("%d lecturas publicadas", enviadas)

    productor.flush(30)
    return enviadas
