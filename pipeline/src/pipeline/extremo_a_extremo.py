"""La demostración, pero sobre el stack real: Kafka → Flink → Beam → Kafka.

`pipeline.demostracion` corre con `DirectRunner` y prueba **la lógica**. `pipeline.humo`
levanta el recorrido con un *passthrough* y prueba **el cableado**. Falta la pregunta que
ninguna de las dos responde: ¿la lógica da lo mismo cuando la ejecuta Flink?

No es una pregunta retórica. El runner portable serializa las funciones y el estado hacia
procesos que no comparten memoria con el que arma el pipeline, y hay cosas que andan en local
y no allá — un objeto que no viaja, un estado que el runner implementa distinto.

Esto siembra **las mismas cinco lecturas** de la demostración y exige **el mismo resultado**.

    docker compose -f infra/docker-compose.yml --profile e2e run --rm -T extremo-a-extremo
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


def correr_pipeline(ajustes: Ajustes, cuantas: int) -> None:
    calendario = cargar_calendario(ajustes.config_franjas)
    with beam.Pipeline(options=opciones(ajustes, nombre="e2e", streaming=False)) as pipeline:
        construir(
            pipeline,
            ajustes,
            grupo="g-e2e",
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


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", stream=sys.stdout)
    ajustes = Ajustes.desde_entorno()

    enviadas = sembrar(ajustes)
    correr_pipeline(ajustes, enviadas)
    tabla = leer_celdas(ajustes)

    print(f"\n  {'celda':<34} {'kWh':>8}  {'esperado':>9}  origen")
    problemas = []
    for clave, esperado in sorted(ESPERADO.items()):
        celda = tabla.get(clave)
        if celda is None:
            print(f"  {clave:<34} {'—':>8}  {esperado:>9.3f}  ✘ no llegó")
            problemas.append(f"falta {clave}")
            continue
        obtenido = round(celda["energia_kwh"], 3)
        origen = "interpolado" if celda["interpolada"] else "medido"
        marca = "✔" if obtenido == esperado else "✘"
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
            log.error("✘ %s", p)
        return 1

    print(
        "\n  ✅ Flink y Kafka dan el mismo resultado que la demostración con DirectRunner.\n"
        "     El duplicado no sumó, la tardía corrigió el reparto, y el total se conserva.\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
