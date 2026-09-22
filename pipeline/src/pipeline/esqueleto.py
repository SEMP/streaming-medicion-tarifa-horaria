"""Esqueleto del pipeline: entra por Kafka, sale por Kafka.

**Lo que hay acá es el cableado, no la lógica.** Las transformaciones del dominio
—validación, diferenciación, atribución de franja, agregación— van en el hueco marcado más
abajo y son responsabilidad de Clara (ver `docs/planes/`).

La idea es que ese hueco se pueda llenar **sin pelear con la infraestructura**: leer de
Kafka desde Python no es un `pip install`, y ese problema ya está resuelto acá.
"""

from __future__ import annotations

from collections.abc import Callable

import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka, default_io_expansion_service
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.typehints import KV

from .config import Ajustes


def servicio_expansion_kafka():
    """Levanta el *expansion service* de KafkaIO.

    ⚠️ **KafkaIO no es una librería Python.** Es una transformación *cross-language*: las
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

    La **clave importa**: es lo que manda todos los panes de una misma celda a la misma
    partición, de modo que se lean en orden y el último gane. Sin eso, el *upsert* del
    consumidor no sería determinista.
    """
    return coleccion | f"Escribir{etiqueta}" >> WriteToKafka(
        producer_config={"bootstrap.servers": ajustes.servidores_kafka},
        topic=topico,
        expansion_service=servicio_expansion_kafka(),
    )


def construir(
    pipeline: beam.Pipeline,
    ajustes: Ajustes,
    *,
    grupo: str,
    transformaciones: Callable[[beam.PCollection], beam.PCollection] | None = None,
    max_registros: int | None = None,
) -> beam.PCollection:
    """Arma el recorrido completo: Kafka → transformaciones → Kafka.

    `transformaciones` es **el hueco de Clara**: recibe la PCollection de records crudos y
    devuelve la de resultados, ya en pares (clave, valor) de bytes. Si no se pasa nada, el
    pipeline hace *passthrough* — sirve para verificar que el cableado funciona antes de
    que exista una sola transformación del dominio.
    """
    crudas = leer_lecturas(pipeline, ajustes, grupo=grupo, max_registros=max_registros)

    if transformaciones is None:
        salida = crudas | "Passthrough" >> beam.Map(lambda kv: kv).with_output_types(
            KV[bytes, bytes]
        )
    else:
        salida = transformaciones(crudas)

    escribir(salida, ajustes, ajustes.topico_consumo, etiqueta="Consumo")
    return salida
