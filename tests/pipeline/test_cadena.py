"""La cadena completa, alimentada con bytes como los que trae Kafka.

Corre con `DirectRunner` y sin broker: lo que se prueba acá es la **lógica de la cadena**
—parseo, ruteo a cuarentena, ventana, agregación y serialización—, no que KafkaIO levante.
Eso último lo prueba `pipeline.humo`, que necesita el stack completo.
"""

from __future__ import annotations

import json
from pathlib import Path

import apache_beam as beam
import pytest
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from pipeline.cadena import cadena, desplazamiento_del_dia_local
from pipeline.config import Ajustes
from pipeline.franjas import cargar_calendario

EJEMPLO = Path(__file__).parents[2] / "config" / "franjas.example.toml"


@pytest.fixture
def cal():
    return cargar_calendario(EJEMPLO)


@pytest.fixture
def ajustes():
    return Ajustes.desde_entorno()


def registro(medidor: str, instante: str, valor: float, *, cabina: str = "CAB-07") -> tuple:
    """Un record de Kafka: par de bytes, como sale de `ReadFromKafka`."""
    payload = {
        "schema_version": 1,
        "medidor_id": medidor,
        "cabina_id": cabina,
        "instante_lectura": instante,
        "registros": [
            {"obis": "15.8.0", "naturaleza": "acumulado", "valor": valor, "unidad": "kWh"}
        ],
        "calidad": "ok",
    }
    return medidor.encode(), json.dumps(payload).encode()


def opciones():
    o = PipelineOptions()
    o.view_as(StandardOptions).streaming = True
    return o


def test_el_desplazamiento_alinea_la_ventana_con_la_medianoche_local(cal):
    """**Sin esto, el día corta a las 21:00 locales, en pleno horario de `punta`.**

    Beam ventanea sobre el instante absoluto. Asunción está en UTC−3, así que la medianoche
    local son las 03:00 UTC y la ventana hay que correrla tres horas.
    """
    assert desplazamiento_del_dia_local(cal) == 3 * 3600


def test_la_cadena_produce_celdas_por_franja(ajustes, cal):
    """El recorrido feliz: dos lecturas → un intervalo → las celdas que toca."""
    with TestPipeline(options=opciones()) as p:
        salidas = cadena(ajustes, cal)(
            p | beam.Create([
                registro("MED-1", "2026-09-25T17:40:00-03:00", 100.0),
                registro("MED-1", "2026-09-25T17:55:00-03:00", 101.5),
            ])
        )
        assert_that(
            salidas.consumo | beam.Map(lambda kv: (kv[0].decode(), json.loads(kv[1]))),
            equal_to([("MED-1|2026-09-25|resto", {
                "energia_kwh": 1.5,
                "minutos_cubiertos": 15.0,
                "interpolada": False,
                "separacion_maxima_minutos": 15.0,
                "cabina_id": "CAB-07",
                "intervalos_usados": 1,
            })]),
        )


def test_un_intervalo_que_cruza_el_borde_cae_en_dos_celdas(ajustes, cal):
    """Y las dos quedan marcadas como interpoladas, porque ninguna se midió entera."""
    with TestPipeline(options=opciones()) as p:
        salidas = cadena(ajustes, cal)(
            p | beam.Create([
                registro("MED-1", "2026-09-25T17:55:00-03:00", 101.5),
                registro("MED-1", "2026-09-25T18:20:00-03:00", 105.5),
            ])
        )
        assert_that(
            salidas.consumo
            | beam.Map(lambda kv: (kv[0].decode(), round(json.loads(kv[1])["energia_kwh"], 3),
                                   json.loads(kv[1])["interpolada"])),
            equal_to([
                ("MED-1|2026-09-25|resto", 0.8, True),
                ("MED-1|2026-09-25|punta", 3.2, True),
            ]),
        )


@pytest.mark.parametrize(
    ("entrada", "motivo"),
    [
        ((b"MED-1", b"esto no es json"), "json_invalido"),
        ((b"", json.dumps({"instante_lectura": "2026-09-25T10:00:00-03:00"}).encode()),
         "sin_medidor_id"),
        ((b"MED-1", json.dumps({"medidor_id": "MED-1", "instante_lectura": "ayer"}).encode()),
         "instante_invalido"),
        ((b"MED-1",
          json.dumps({"medidor_id": "MED-1", "instante_lectura": "2026-09-25T10:00:00"}).encode()),
         "instante_sin_huso"),
    ],
)
def test_lo_que_no_se_puede_procesar_va_a_cuarentena(ajustes, cal, entrada, motivo):
    """**Nada se descarta en silencio.** Cada caso llega con su motivo, y el volumen del
    tópico es una señal operativa: si sube, el problema está aguas arriba."""
    with TestPipeline(options=opciones()) as p:
        salidas = cadena(ajustes, cal)(p | beam.Create([entrada]))
        assert_that(
            salidas.cuarentena | beam.Map(lambda kv: json.loads(kv[1])["motivo"]),
            equal_to([motivo]),
            label="motivo",
        )
        assert_that(salidas.consumo, equal_to([]), label="y nada al agregado")


def test_una_lectura_sin_huso_no_se_interpreta_en_la_zona_del_proceso(ajustes, cal):
    """Va a cuarentena en lugar de adivinar: si se asumiera la zona local del worker, la
    franja atribuida dependería de en qué máquina corre el pipeline."""
    with TestPipeline(options=opciones()) as p:
        salidas = cadena(ajustes, cal)(
            p | beam.Create([registro("MED-1", "2026-09-25T17:40:00", 100.0)])
        )
        assert_that(
            salidas.cuarentena | beam.Map(lambda kv: kv[0].decode()),
            equal_to(["MED-1"]),
            label="clavado por medidor",
        )


def test_el_duplicado_no_llega_a_la_salida(ajustes, cal):
    """El mismo reintento de publicación que prueba `test_transformaciones`, ahora sobre la
    cadena entera y entrando como bytes."""
    with TestPipeline(options=opciones()) as p:
        salidas = cadena(ajustes, cal)(
            p | beam.Create([
                registro("MED-1", "2026-09-25T17:40:00-03:00", 100.0),
                registro("MED-1", "2026-09-25T17:55:00-03:00", 101.5),
                registro("MED-1", "2026-09-25T17:55:00-03:00", 101.5),
            ])
        )
        assert_that(
            salidas.consumo | beam.Map(lambda kv: round(json.loads(kv[1])["energia_kwh"], 3)),
            equal_to([1.5]),
        )
