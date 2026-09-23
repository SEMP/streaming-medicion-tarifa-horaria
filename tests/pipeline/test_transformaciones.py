"""Pruebas de las etapas con estado, con control explícito del tiempo.

`TestStream` permite decidir **cuándo llega cada evento y hasta dónde avanzó el watermark**,
que es la única forma de probar comportamiento tardío de manera determinista: con un reloj
real habría que esperar, y el resultado dependería de la máquina.

Corren con `DirectRunner`, sin Kafka ni Flink.
"""

from __future__ import annotations

from datetime import datetime

import apache_beam as beam
import pytest
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms import trigger
from pipeline.transformaciones import (
    CUARENTENA,
    DeduplicarLecturas,
    DiferenciarContador,
)

TZ = "-03:00"
DIA = "2026-09-22"


def instante(hora: str) -> str:
    return f"{DIA}T{hora}{TZ}"


def hhmm(iso: str) -> str:
    """Extrae HH:MM de un ISO-8601. Con función y no rebanando: el string puede o no traer
    segundos, y un recorte fijo se rompe en silencio."""
    return datetime.fromisoformat(iso).strftime("%H:%M")


def marca(hora: str) -> float:
    """Timestamp en segundos, que es lo que TestStream usa para ubicar el evento."""
    return datetime.fromisoformat(instante(hora)).timestamp()


def lectura(medidor: str, hora: str, valor: float, *, cabina: str = "CAB-0") -> dict:
    return {
        "schema_version": 1,
        "medidor_id": medidor,
        "cabina_id": cabina,
        "instante_lectura": instante(hora),
        "registros": [
            {"obis": "15.8.0", "naturaleza": "acumulado", "valor": valor, "unidad": "kWh"}
        ],
        "calidad": "ok",
    }


def opciones_streaming() -> PipelineOptions:
    opciones = PipelineOptions()
    opciones.view_as(StandardOptions).streaming = True
    return opciones


@pytest.fixture
def ventana_diaria():
    """Ventana de un día con la política temporal de `contratos.md` §2.4."""
    return beam.WindowInto(
        beam.window.FixedWindows(24 * 3600),
        trigger=trigger.AfterWatermark(late=trigger.AfterCount(1)),
        allowed_lateness=36 * 3600,
        accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
    )


# --------------------------------------------------------------------- deduplicación


def test_un_duplicado_exacto_no_pasa_dos_veces(ventana_diaria):
    """El reintento de publicación: mismo medidor, mismo instante. Es el único duplicado
    real — un reintento de comunicación produce una lectura nueva, no un duplicado."""
    flujo = (
        TestStream()
        .advance_watermark_to(marca("00:00"))
        .add_elements([("M1", lectura("M1", "08:00", 100.0))], event_timestamp=marca("08:00"))
        .add_elements([("M1", lectura("M1", "08:00", 100.0))], event_timestamp=marca("08:00"))
        .advance_watermark_to_infinity()
    )

    with TestPipeline(options=opciones_streaming()) as p:
        salida = (
            p
            | flujo
            | ventana_diaria
            | beam.ParDo(DeduplicarLecturas()).with_outputs(CUARENTENA, main="ok")
        )
        assert_that(
            salida.ok | beam.Map(lambda kv: kv[1]["instante_lectura"]),
            equal_to([instante("08:00")]),
            label="una sola vez",
        )


def test_dos_medidores_pueden_repetir_el_mismo_instante(ventana_diaria):
    """El estado es por clave: dos medidores no se interfieren."""
    flujo = (
        TestStream()
        .advance_watermark_to(marca("00:00"))
        .add_elements(
            [("M1", lectura("M1", "08:00", 100.0)), ("M2", lectura("M2", "08:00", 200.0))],
            event_timestamp=marca("08:00"),
        )
        .advance_watermark_to_infinity()
    )

    with TestPipeline(options=opciones_streaming()) as p:
        salida = (
            p | flujo | ventana_diaria
            | beam.ParDo(DeduplicarLecturas()).with_outputs(CUARENTENA, main="ok")
        )
        assert_that(salida.ok | beam.Map(lambda kv: kv[0]), equal_to(["M1", "M2"]))


def test_una_lectura_sin_instante_va_a_cuarentena(ventana_diaria):
    """Nada se descarta en silencio: lo que no se puede procesar queda contado."""
    rota = lectura("M1", "08:00", 100.0)
    del rota["instante_lectura"]

    flujo = (
        TestStream()
        .advance_watermark_to(marca("00:00"))
        .add_elements([("M1", rota)], event_timestamp=marca("08:00"))
        .advance_watermark_to_infinity()
    )

    with TestPipeline(options=opciones_streaming()) as p:
        salida = (
            p | flujo | ventana_diaria
            | beam.ParDo(DeduplicarLecturas()).with_outputs(CUARENTENA, main="ok")
        )
        assert_that(
            getattr(salida, CUARENTENA) | beam.Map(lambda x: x["motivo"]),
            equal_to(["instante_invalido"]),
        )


# --------------------------------------------------------------------- diferenciación


def test_el_consumo_sale_de_restar_dos_lecturas(ventana_diaria):
    """Un contador acumulado no trae el consumo: vive **entre** dos lecturas."""
    flujo = (
        TestStream()
        .advance_watermark_to(marca("00:00"))
        .add_elements([("M1", lectura("M1", "08:00", 100.0))], event_timestamp=marca("08:00"))
        .add_elements([("M1", lectura("M1", "08:30", 102.5))], event_timestamp=marca("08:30"))
        .advance_watermark_to_infinity()
    )

    with TestPipeline(options=opciones_streaming()) as p:
        salida = (
            p | flujo | ventana_diaria
            | beam.ParDo(DiferenciarContador()).with_outputs(CUARENTENA, main="ok")
        )
        assert_that(
            salida.ok | beam.Map(lambda c: (c.desde, c.hasta, c.energia_kwh)),
            equal_to([(instante("08:00"), instante("08:30"), 2.5)]),
        )


def test_una_lectura_fuera_de_orden_parte_el_intervalo_que_la_contenia(ventana_diaria):
    """**El caso que justifica guardar las lecturas en lugar de restar contra la última.**

    Llegan 08:00 y 09:00, que forman un intervalo de una hora. Después llega, tardía, la de
    08:30 — que cae en el medio. Restar contra «la última vista» daría un valor negativo o
    absurdo; acá parte el intervalo en dos y emite las dos mitades, que con salida por
    *upsert* reemplazan al valor grosero anterior.
    """
    flujo = (
        TestStream()
        .advance_watermark_to(marca("00:00"))
        .add_elements([("M1", lectura("M1", "08:00", 100.0))], event_timestamp=marca("08:00"))
        .add_elements([("M1", lectura("M1", "09:00", 106.0))], event_timestamp=marca("09:00"))
        .add_elements([("M1", lectura("M1", "08:30", 102.0))], event_timestamp=marca("08:30"))
        .advance_watermark_to_infinity()
    )

    with TestPipeline(options=opciones_streaming()) as p:
        salida = (
            p | flujo | ventana_diaria
            | beam.ParDo(DiferenciarContador()).with_outputs(CUARENTENA, main="ok")
        )
        assert_that(
            salida.ok | beam.Map(lambda c: (hhmm(c.desde), hhmm(c.hasta), c.energia_kwh)),
            equal_to(
                [
                    ("08:00", "09:00", 6.0),   # el intervalo grosero, emitido primero
                    ("08:00", "08:30", 2.0),   # y sus dos mitades, al llegar la tardía
                    ("08:30", "09:00", 4.0),
                ]
            ),
        )


def test_un_contador_que_retrocede_va_a_cuarentena(ventana_diaria):
    """Sin compra de energía al usuario, el contador solo puede subir: un retroceso es un
    reseteo del equipo o una trama truncada, nunca una medición válida."""
    flujo = (
        TestStream()
        .advance_watermark_to(marca("00:00"))
        .add_elements([("M1", lectura("M1", "08:00", 5000.0))], event_timestamp=marca("08:00"))
        .add_elements([("M1", lectura("M1", "08:30", 12.0))], event_timestamp=marca("08:30"))
        .advance_watermark_to_infinity()
    )

    with TestPipeline(options=opciones_streaming()) as p:
        salida = (
            p | flujo | ventana_diaria
            | beam.ParDo(DiferenciarContador()).with_outputs(CUARENTENA, main="ok")
        )
        assert_that(
            getattr(salida, CUARENTENA) | beam.Map(lambda x: x["motivo"]),
            equal_to(["contador_retrocede"]),
            label="a cuarentena",
        )
        assert_that(salida.ok, equal_to([]), label="y nada al agregado")


def test_el_intervalo_declara_su_separacion(ventana_diaria):
    """La separación es la cota del error de atribución: un intervalo de 40 minutos puede
    cruzar un borde de franja, y parte de su energía se atribuye al lado equivocado."""
    flujo = (
        TestStream()
        .advance_watermark_to(marca("00:00"))
        .add_elements([("M1", lectura("M1", "17:40", 100.0))], event_timestamp=marca("17:40"))
        .add_elements([("M1", lectura("M1", "18:20", 104.0))], event_timestamp=marca("18:20"))
        .advance_watermark_to_infinity()
    )

    with TestPipeline(options=opciones_streaming()) as p:
        salida = (
            p | flujo | ventana_diaria
            | beam.ParDo(DiferenciarContador()).with_outputs(CUARENTENA, main="ok")
        )
        assert_that(
            salida.ok | beam.Map(lambda c: c.separacion_minutos), equal_to([40.0])
        )


# --------------------------------------------------------------------- las dos juntas


def test_el_duplicado_debe_deduplicarse_antes_de_diferenciar(ventana_diaria):
    """**El orden de las dos etapas no es intercambiable.**

    Si el duplicado llegara a la diferenciación, se restaría una lectura contra sí misma y
    produciría un consumo de 0 que, con salida por *upsert*, pisaría el valor correcto.
    Esta prueba fija el orden correcto para que nadie lo invierta sin darse cuenta.
    """
    flujo = (
        TestStream()
        .advance_watermark_to(marca("00:00"))
        .add_elements([("M1", lectura("M1", "08:00", 100.0))], event_timestamp=marca("08:00"))
        .add_elements([("M1", lectura("M1", "08:30", 103.0))], event_timestamp=marca("08:30"))
        .add_elements([("M1", lectura("M1", "08:30", 103.0))], event_timestamp=marca("08:30"))
        .advance_watermark_to_infinity()
    )

    with TestPipeline(options=opciones_streaming()) as p:
        salida = (
            p
            | flujo
            | ventana_diaria
            | "Deduplicar" >> beam.ParDo(DeduplicarLecturas()).with_outputs(CUARENTENA, main="ok")
        )
        consumos = (
            salida.ok
            | "Diferenciar" >> beam.ParDo(DiferenciarContador()).with_outputs(CUARENTENA, main="ok")
        )
        assert_that(
            consumos.ok | beam.Map(lambda c: c.energia_kwh),
            equal_to([3.0]),
            label="un solo intervalo, sin el 0 que lo pisaría",
        )
