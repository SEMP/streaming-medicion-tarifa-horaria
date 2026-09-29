"""La demostración, pero sobre el stack real: Kafka → Flink → Beam → Kafka.

`pipeline.demostracion` corre con `DirectRunner` y prueba **la lógica**. `pipeline.humo`
levanta el recorrido con un *passthrough* y prueba **el cableado**. Falta la pregunta que
ninguna de las dos responde: ¿la lógica da lo mismo cuando la ejecuta Flink?

No es una pregunta retórica. El runner portable serializa las funciones y el estado hacia
procesos que no comparten memoria con el que arma el pipeline, y hay cosas que andan en local
y no allá — un objeto que no viaja, un estado que el runner implementa distinto.

Esto siembra **las mismas cinco lecturas** de la demostración y exige **el mismo resultado**.

    docker compose -f infra/docker-compose.yml --profile e2e run --rm -T extremo-a-extremo

Con `--replay` no siembra nada: **relee el tópico de entrada desde el offset 0**, con un
`group.id` nuevo y por lo tanto sin estado previo, y escribe a un tópico de salida propio.
Es la idempotencia sobre el stack real: el mismo log, procesado de nuevo desde cero, tiene
que converger a las mismas dos celdas. Se corre después del recorrido normal:

    docker compose -f infra/docker-compose.yml --profile e2e run --rm -T repeticion
"""

from __future__ import annotations

import json
import logging
import sys
import time
from datetime import datetime

import apache_beam as beam

from .cadena import cadena
from .config import Ajustes
from .esqueleto import construir, opciones
from .franjas import cargar_calendario

ESPERA_MAXIMA_SEGUNDOS = 180

MEDIDOR = "MED-0042"
CABINA = "CAB-07"
DIA = "2026-09-25"

LECTURAS = (
    ("17:40", 100.0),
    ("17:55", 101.5),
    ("18:20", 105.5),
    ("17:55", 101.5),   # duplicado: reintento de publicación
    ("18:00", 102.4),   # tardía, justo sobre el borde de franja
)

ESPERADO = {
    f"{MEDIDOR}|{DIA}|resto": 2.4,
    f"{MEDIDOR}|{DIA}|punta": 3.1,
}

log = logging.getLogger("e2e")


def sembrar(ajustes: Ajustes) -> int:
    """Publica las cinco lecturas, en el orden en que las cuenta la demostración."""
    from simulador.evento import Lectura, Registro
    from simulador.publicador import publicar

    lecturas = []
    for secuencia, (hora, valor) in enumerate(LECTURAS):
        instante = datetime.fromisoformat(f"{DIA}T{hora}:00-03:00")
        lecturas.append(
            Lectura(
                medidor_id=MEDIDOR,
                cabina_id=CABINA,
                lote_id="LOTE-E2E",
                secuencia=secuencia,
                instante_lectura=instante,
                registros=(
                    Registro(obis="15.8.0", naturaleza="acumulado", valor=valor, unidad="kWh"),
                ),
                calidad="ok",
                publicado_at=instante,
            )
        )

    enviadas = publicar(
        lecturas, servidores=ajustes.servidores_kafka, topico=ajustes.topico_lecturas
    )
    log.info("sembradas %d lecturas en %s", enviadas, ajustes.topico_lecturas)
    return enviadas


def contar_entrada(ajustes: Ajustes) -> int:
    """Cuántos records hay en el tópico de entrada, sumando todas sus particiones.

    El replay no sabe cuántas lecturas sembró el recorrido anterior —puede haber corrido más
    de una vez—, y `max_registros` tiene que coincidir con lo que hay, o el pipeline no
    termina.
    """
    from confluent_kafka import Consumer, TopicPartition

    consumidor = Consumer(
        {"bootstrap.servers": ajustes.servidores_kafka, "group.id": "g-e2e-contador"}
    )
    try:
        particiones = consumidor.list_topics(ajustes.topico_lecturas, timeout=10).topics[
            ajustes.topico_lecturas
        ].partitions
        total = 0
        for numero in particiones:
            bajo, alto = consumidor.get_watermark_offsets(
                TopicPartition(ajustes.topico_lecturas, numero), timeout=10
            )
            total += alto - bajo
    finally:
        consumidor.close()
    return total


def correr_pipeline(ajustes: Ajustes, cuantas: int, *, grupo: str = "g-e2e") -> None:
    calendario = cargar_calendario(ajustes.config_franjas)
    with beam.Pipeline(options=opciones(ajustes, nombre="e2e", streaming=False)) as pipeline:
        construir(
            pipeline,
            ajustes,
            grupo=grupo,
            transformaciones=cadena(ajustes, calendario),
            max_registros=cuantas,
        )


def leer_celdas(ajustes: Ajustes) -> dict[str, dict]:
    """Consume la salida aplicando **upsert por clave**, igual que haría el tablero.

    Cada pane trae el valor absoluto de la celda, así que una clave repetida es una revisión
    y vale la última. Consumir sumando sería un error del consumidor, no del pipeline.
    """
    from confluent_kafka import Consumer

    consumidor = Consumer(
        {
            "bootstrap.servers": ajustes.servidores_kafka,
            # Grupo nuevo en cada corrida: hay que releer el tópico entero para poder
            # aplicar el upsert desde cero. Con un grupo fijo, la segunda corrida arrancaría
            # después de lo que ya consumió y vería una tabla incompleta.
            "group.id": f"g-e2e-verificador-{int(time.time())}",
            "auto.offset.reset": "earliest",
        }
    )
    consumidor.subscribe([ajustes.topico_consumo])

    tabla: dict[str, dict] = {}
    silencio_desde = time.monotonic()
    limite = time.monotonic() + ESPERA_MAXIMA_SEGUNDOS
    try:
        while time.monotonic() < limite:
            mensaje = consumidor.poll(2.0)
            if mensaje is None or mensaje.error():
                # Tres segundos sin nada nuevo: el pipeline ya terminó de emitir.
                if tabla and time.monotonic() - silencio_desde > 3:
                    break
                continue
            tabla[mensaje.key().decode()] = json.loads(mensaje.value())
            silencio_desde = time.monotonic()
    finally:
        consumidor.close()
    return tabla


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", stream=sys.stdout)
    ajustes = Ajustes.desde_entorno()
    replay = "--replay" in (sys.argv[1:] if argv is None else argv)

    if replay:
        # Grupo nuevo: sin offsets confirmados, `auto.offset.reset=earliest` lo manda al
        # offset 0. Y un trabajo nuevo de Flink arranca sin estado de deduplicación.
        cuantas = contar_entrada(ajustes)
        grupo = f"g-e2e-replay-{int(time.time())}"
        log.info("replay: %d records de %s, grupo %s", cuantas, ajustes.topico_lecturas, grupo)
        if cuantas == 0:
            log.error("MAL el tópico de entrada está vacío: correr antes extremo-a-extremo")
            return 1
        correr_pipeline(ajustes, cuantas, grupo=grupo)
    else:
        enviadas = sembrar(ajustes)
        correr_pipeline(ajustes, enviadas)
    tabla = leer_celdas(ajustes)

    print(f"\n  {'celda':<34} {'kWh':>8}  {'esperado':>9}  origen")
    problemas = []
    for clave, esperado in sorted(ESPERADO.items()):
        celda = tabla.get(clave)
        if celda is None:
            print(f"  {clave:<34} {'—':>8}  {esperado:>9.3f}  MAL no llegó")
            problemas.append(f"falta {clave}")
            continue
        obtenido = round(celda["energia_kwh"], 3)
        origen = "interpolado" if celda["interpolada"] else "medido"
        marca = "OK" if obtenido == esperado else "MAL"
        print(f"  {clave:<34} {obtenido:>8.3f}  {esperado:>9.3f}  {origen} {marca}")
        if obtenido != esperado:
            problemas.append(f"{clave}: {obtenido} ≠ {esperado}")

    sobrantes = set(tabla) - set(ESPERADO)
    if sobrantes:
        problemas.append(f"celdas inesperadas: {sorted(sobrantes)}")

    total = round(sum(c["energia_kwh"] for c in tabla.values()), 3)
    print(f"  {'TOTAL':<34} {total:>8.3f}  {sum(ESPERADO.values()):>9.3f}")

    if problemas:
        for p in problemas:
            log.error("MAL %s", p)
        return 1

    if replay:
        print(
            "\n  BIEN: releer el log desde el offset 0, con estado nuevo, converge a las mismas\n"
            "     celdas. Reprocesar es inocuo: la salida es idempotente sobre el stack real.\n"
        )
        return 0
    print(
        "\n  BIEN: Flink y Kafka dan el mismo resultado que la demostración con DirectRunner.\n"
        "     El duplicado no sumó, la tardía corrigió el reparto, y el total se conserva.\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
