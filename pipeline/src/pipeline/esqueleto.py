"""Esqueleto del pipeline: entra por Kafka, sale por Kafka.

**Lo que hay acá es el cableado, no la lógica.** Las transformaciones del dominio viven en
[`cadena`], que es lo que se mete en el hueco que deja `construir`.

La separación se mantiene aunque el hueco ya esté lleno: permite correr el recorrido con un
*passthrough* para verificar que la infraestructura funciona antes de sospechar de la lógica,
que es lo que hace `pipeline.humo`.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka, default_io_expansion_service
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.typehints import KV

from .config import Ajustes


def servicio_expansion_kafka():
    """Levanta el *expansion service* de KafkaIO.

    **Atención — KafkaIO no es una librería Python.** Es una transformación *cross-language*: las
    etapas de lectura y escritura las ejecuta el SDK **Java**, y este servicio es el puente.

    `PROCESS` hace que ese SDK corra como un proceso **dentro del TaskManager**, usando el
    binario que la imagen de Flink ya trae. La alternativa sería que Beam levantara un
    contenedor por entorno, lo que obligaría a darle al TaskManager acceso al demonio de
    Docker.
    """
    return default_io_expansion_service(
        append_args=[
            "--defaultEnvironmentType=PROCESS",
            '--defaultEnvironmentConfig={"command":"/opt/apache/beam/boot"}',
        ]
    )


def opciones(ajustes: Ajustes, *, nombre: str, streaming: bool = True) -> PipelineOptions:
    """Opciones para correr sobre Flink a través del job server."""
    args = [
        "--runner=PortableRunner",
        f"--job_endpoint={ajustes.job_endpoint}",
        "--environment_type=PROCESS",
        '--environment_config={"command":"/opt/medicion/python-sdk/boot"}',
        f"--parallelism={ajustes.paralelismo}",
        f"--job_name={nombre}",
        "--experiments=use_sdf_read",
    ]
    if streaming:
        args.append("--streaming")
    return PipelineOptions(args)


def leer_lecturas(
    pipeline: beam.Pipeline,
    ajustes: Ajustes,
    *,
    grupo: str,
    max_registros: int | None = None,
) -> beam.PCollection:
    """Lee el tópico de lecturas crudas.

    `timestamp_policy=create_time` hace que Beam use el timestamp **del record de Kafka**.
    Ojo con eso: no es el `instante_lectura` del payload. El tiempo de evento del dominio
    hay que asignarlo explícitamente después de parsear — es justamente la distinción que
    todo el proyecto trata de respetar.

    `max_registros` acota la lectura para pruebas: sin él, el pipeline no termina nunca,
    que es lo correcto en streaming y poco práctico en una prueba de humo.
    """
    extra = {"max_num_records": max_registros} if max_registros is not None else {}
    return pipeline | "LeerLecturas" >> ReadFromKafka(
        consumer_config={
            "bootstrap.servers": ajustes.servidores_kafka,
            "group.id": grupo,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "true",
        },
        topics=[ajustes.topico_lecturas],
        timestamp_policy=ReadFromKafka.create_time_policy,
        expansion_service=servicio_expansion_kafka(),
        **extra,
    )


def escribir(coleccion: beam.PCollection, ajustes: Ajustes, topico: str, *, etiqueta: str):
    """Escribe una colección de pares (clave, valor) en bytes a un tópico.

    La **clave importa**: es lo que manda todas las revisiones de una misma celda a la misma
    partición, de modo que se lean en orden y la última gane. Sin eso, el *upsert* del
    consumidor no sería determinista.

    **Las garantías se declaran, no se heredan.** `acks=all` y la idempotencia van escritas
    aunque el cliente Java de Kafka las traiga por defecto desde la 3.0: lo que la tabla de
    garantías del documento afirma sobre este tramo tiene que poder auditarse en el código, y
    un default puede cambiar con una actualización del cliente. Es la misma razón por la que
    el publicador del simulador las declara — allá además hacen falta, porque librdkafka trae
    la idempotencia apagada.

    Lo que aportan acá: con `acks=1` un broker que confirma y se cae antes de replicar pierde
    esa escritura. Perder una revisión intermedia de una celda es inocuo —la siguiente la
    reemplaza—, pero perder **la última** deja al consumidor con un valor viejo y sin forma de
    saberlo.
    """
    return coleccion | f"Escribir{etiqueta}" >> WriteToKafka(
        producer_config={
            "bootstrap.servers": ajustes.servidores_kafka,
            "acks": "all",
            "enable.idempotence": "true",
        },
        topic=topico,
        expansion_service=servicio_expansion_kafka(),
    )


def construir(
    pipeline: beam.Pipeline,
    ajustes: Ajustes,
    *,
    grupo: str,
    transformaciones: Callable[[beam.PCollection], Any] | None = None,
    max_registros: int | None = None,
):
    """Arma el recorrido completo: Kafka → transformaciones → Kafka.

    `transformaciones` recibe la PCollection de records crudos y devuelve o bien una sola
    PCollection de pares (clave, valor) en bytes, o bien un objeto con los atributos `consumo`
    y `cuarentena` — que es lo que devuelve [`cadena.cadena`]. Cuando trae cuarentena, se
    escribe a su propio tópico.

    Si no se pasa nada, el pipeline hace *passthrough*. Sirve para verificar que el cableado
    funciona sin que la lógica del dominio intervenga: separa «el pipeline está mal» de «la
    infraestructura está mal», que son dos problemas distintos.
    """
    crudas = leer_lecturas(pipeline, ajustes, grupo=grupo, max_registros=max_registros)

    if transformaciones is None:
        salida = crudas | "Passthrough" >> beam.Map(lambda kv: kv).with_output_types(
            KV[bytes, bytes]
        )
    else:
        salida = transformaciones(crudas)

    consumo = getattr(salida, "consumo", salida)
    escribir(consumo, ajustes, ajustes.topico_consumo, etiqueta="Consumo")

    cuarentena = getattr(salida, "cuarentena", None)
    if cuarentena is not None:
        escribir(cuarentena, ajustes, ajustes.topico_cuarentena, etiqueta="Cuarentena")

    return salida
