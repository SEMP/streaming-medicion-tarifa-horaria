"""Demostración narrada de los tres escenarios: normal, duplicado y tardío.

El enunciado es explícito: *«Una ejecución exitosa con datos ideales no es evidencia
suficiente»*. Esto es lo contrario de una corrida feliz — es una historia de cinco lecturas
sobre **un solo medidor**, elegida para que cada paso se pueda leer a simple vista y
verificar con una resta mental.

Corre con `DirectRunner` y `TestStream`, **sin Kafka ni Docker**. Es a propósito: el
comportamiento tardío depende de dónde está el watermark, y con un reloj real habría que
esperar y el resultado dependería de la máquina. Acá el tiempo se controla, así que la salida
es idéntica en cualquier computadora — que es lo que la vuelve evidencia y no anécdota.

El recorrido completo sobre Kafka y Flink lo prueba `pipeline.humo`; son complementarias.

    uv run python -m pipeline.demostracion
"""

from __future__ import annotations

import json
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms import trigger

from .franjas import CalendarioTarifario, cargar_calendario, repartir_por_franja
from .transformaciones import (
    CUARENTENA,
    DeduplicarLecturas,
    DiferenciarContador,
    IntervalosVigentes,
)

DIA = "2026-09-25"
TZ = "-03:00"
MEDIDOR = "MED-0042"
CABINA = "CAB-07"

CALENDARIO = Path(__file__).parents[3] / "config" / "franjas.example.toml"

ANCHO = 78


@dataclass(frozen=True)
class Acto:
    titulo: str
    explicacion: str
    llega: tuple[str, float] | None
    """La lectura que se suma en este acto: `(hora, valor del contador)`."""


ACTOS = (
    Acto(
        titulo="Acto 1 — lecturas normales",
        explicacion=(
            "Tres lecturas en orden. El contador es acumulado, así que el consumo sale de\n"
            "restar. El segundo intervalo cruza las 18:00, que es donde empieza `punta`."
        ),
        llega=None,
    ),
    Acto(
        titulo="Acto 2 — un duplicado",
        explicacion=(
            "El concentrador reintenta publicar la lectura de las 17:55. Mismo instante,\n"
            "mismo valor: es un duplicado real, no una relectura. La tabla no debe moverse."
        ),
        llega=("17:55", 101.5),
    ),
    Acto(
        titulo="Acto 3 — una lectura tardía",
        explicacion=(
            "Llega, con retraso, la lectura de las 18:00 — justo sobre el borde de franja.\n"
            "Parte el intervalo que la contenía y **corrige** el reparto que se había\n"
            "estimado por interpolación."
        ),
        llega=("18:00", 102.4),
    ),
)

BASE = (("17:40", 100.0), ("17:55", 101.5), ("18:20", 105.5))


# --------------------------------------------------------------------------- utilidades


def instante(hora: str) -> str:
    return f"{DIA}T{hora}:00{TZ}"


def marca(hora: str) -> float:
    return datetime.fromisoformat(instante(hora)).timestamp()


def hhmm(iso: str) -> str:
    return datetime.fromisoformat(iso).strftime("%H:%M")


def lectura(hora: str, valor: float) -> dict:
    return {
        "schema_version": 1,
        "medidor_id": MEDIDOR,
        "cabina_id": CABINA,
        "instante_lectura": instante(hora),
        "registros": [
            {"obis": "15.8.0", "naturaleza": "acumulado", "valor": valor, "unidad": "kWh"}
        ],
        "calidad": "ok",
    }


def titulo(texto: str) -> None:
    print(f"\n{'═' * ANCHO}\n {texto}\n{'═' * ANCHO}")


# ----------------------------------------------------------------------------- pipeline


def a_celdas(consumos, cal: CalendarioTarifario, *, etiqueta: str):
    """Intervalos → celdas `medidor|fecha|franja`, que es la clave de salida del contrato."""

    def repartir(c):
        for parte in repartir_por_franja(
            datetime.fromisoformat(c.desde), datetime.fromisoformat(c.hasta),
            c.energia_kwh, cal,
        ):
            yield (
                f"{c.medidor_id}|{parte.fecha_local}|{parte.franja}",
                (parte.energia_kwh, parte.interpolada),
            )

    def combinar(valores):
        valores = list(valores)
        return (
            sum(e for e, _ in valores),
            any(i for _, i in valores),
        )

    return (
        consumos
        | f"Repartir{etiqueta}" >> beam.FlatMap(repartir)
        | f"Sumar{etiqueta}" >> beam.CombinePerKey(combinar)
    )


def correr(lecturas: tuple[tuple[str, float], ...], cal: CalendarioTarifario) -> dict:
    """Reproduce las lecturas desde cero y devuelve qué emitió el pipeline.

    **Cada acto es un replay completo**, con estado nuevo. No es una comodidad de la demo:
    es la propiedad que el proyecto declara —reprocesar la misma entrada produce el mismo
    resultado— puesta a prueba tres veces seguidas.
    """
    opciones = PipelineOptions()
    opciones.view_as(StandardOptions).streaming = True

    flujo = TestStream().advance_watermark_to(marca("00:00"))
    for hora, valor in lecturas:
        flujo = flujo.add_elements(
            [(MEDIDOR, lectura(hora, valor))], event_timestamp=marca(hora)
        )
    flujo = flujo.advance_watermark_to_infinity()

    with tempfile.TemporaryDirectory() as tmp:
        salidas = {
            n: f"{tmp}/{n}" for n in ("emitidos", "celdas", "celdas_sin_vigencia")
        }
        with beam.Pipeline(options=opciones) as p:
            deduplicadas = (
                p
                | flujo
                | beam.WindowInto(
                    beam.window.FixedWindows(24 * 3600),
                    trigger=trigger.AfterWatermark(late=trigger.AfterCount(1)),
                    allowed_lateness=36 * 3600,
                    accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
                )
                | "Deduplicar" >> beam.ParDo(DeduplicarLecturas()).with_outputs(
                    CUARENTENA, main="ok"
                )
            )
            consumos = deduplicadas.ok | "Diferenciar" >> beam.ParDo(
                DiferenciarContador()
            ).with_outputs(CUARENTENA, main="ok")

            (
                consumos.ok
                | "Describir" >> beam.Map(
                    lambda c: json.dumps(
                        [hhmm(c.desde), hhmm(c.hasta), round(c.energia_kwh, 3)]
                    )
                )
                | "GuardarEmitidos" >> beam.io.WriteToText(salidas["emitidos"])
            )
            (
                a_celdas(consumos.ok | IntervalosVigentes(), cal, etiqueta="Vigentes")
                | "GuardarCeldas" >> beam.Map(json.dumps)
                | "EscribirCeldas" >> beam.io.WriteToText(salidas["celdas"])
            )
            # La misma agregación **sin** el filtro de vigencia, para mostrar qué pasaría.
            (
                a_celdas(consumos.ok, cal, etiqueta="SinVigencia")
                | "GuardarSinVigencia" >> beam.Map(json.dumps)
                | "EscribirSinVigencia" >> beam.io.WriteToText(
                    salidas["celdas_sin_vigencia"]
                )
            )

        return {n: leer(Path(tmp), Path(ruta).name) for n, ruta in salidas.items()}


def leer(carpeta: Path, prefijo: str) -> list:
    lineas: list = []
    for archivo in sorted(carpeta.glob(f"{prefijo}-*")):
        lineas += [json.loads(x) for x in archivo.read_text().splitlines() if x]
    return lineas


# ------------------------------------------------------------------------------ informe


def mostrar_intervalos(emitidos: list) -> None:
    print("\n  Intervalos que emitió la diferenciación")
    print(f"  {'desde':>7}  {'hasta':>7}  {'kWh':>7}")
    for desde, hasta, kwh in sorted(emitidos):
        print(f"  {desde:>7}  {hasta:>7}  {kwh:>7.3f}")


def mostrar_celdas(celdas: list) -> dict[str, float]:
    print("\n  Celdas de salida — clave `medidor|fecha|franja`")
    print(f"  {'clave':<34} {'kWh':>8}  origen")
    tabla = {}
    for clave, (kwh, interpolada) in sorted(celdas):
        origen = "interpolado" if interpolada else "medido"
        print(f"  {clave:<34} {kwh:>8.3f}  {origen}")
        tabla[clave] = kwh
    print(f"  {'TOTAL':<34} {sum(tabla.values()):>8.3f}")
    return tabla


def main() -> int:
    cal = cargar_calendario(CALENDARIO)

    titulo("Consumo por franja horaria — demostración de los tres escenarios")
    print(
        f"\n  Medidor {MEDIDOR}, cabina {CABINA}, {DIA} (America/Asuncion).\n"
        "  Franjas:  valle 00:00–07:00 · resto 07:00–18:00 y 22:00–24:00 · punta 18:00–22:00\n"
        "\n  Cada acto reproduce la historia **desde cero**. Que el resultado no dependa de\n"
        "  cuántas veces se reprocese es justamente lo que hay que demostrar."
    )

    lecturas = BASE
    tablas: list[dict[str, float]] = []
    problemas: list[str] = []

    for acto in ACTOS:
        if acto.llega:
            lecturas = (*lecturas, acto.llega)
        titulo(acto.titulo)
        print(f"\n{acto.explicacion}")
        print("\n  Lecturas del contador, en orden de llegada")
        for posicion, (hora, valor) in enumerate(lecturas):
            # Por posición y no por valor: el duplicado del acto 2 es idéntico a la lectura
            # que repite, así que comparar por valor marcaría a las dos.
            recien = acto.llega is not None and posicion == len(lecturas) - 1
            print(f"    {hora}   {valor:>9.1f} kWh{' ← llega ahora' if recien else ''}")

        salida = correr(lecturas, cal)
        mostrar_intervalos(salida["emitidos"])
        tablas.append(mostrar_celdas(salida["celdas"]))

        if acto is ACTOS[-1]:
            sin = sum(kwh for _, (kwh, _) in salida["celdas_sin_vigencia"])
            print(
                f"\n  Sin el filtro de intervalos vigentes serían {sin:.3f} kWh: el intervalo\n"
                "  grosero seguiría sumando junto con las dos mitades que lo reemplazan."
            )

    # --------------------------------------------------------------- lo que hay que leer
    titulo("Qué demostró cada acto")

    if tablas[0] != tablas[1]:
        problemas.append("el duplicado movió la tabla")
    print(
        "\n  1 → 2   El duplicado **no cambió nada**. La tabla del acto 2 es idéntica a la\n"
        "          del acto 1, y no apareció ningún intervalo de 0 kWh que pisara un valor\n"
        "          bueno — que es lo que pasaría si se dedujera después de diferenciar."
    )

    total_antes, total_despues = sum(tablas[1].values()), sum(tablas[2].values())
    if abs(total_antes - total_despues) > 1e-6:
        problemas.append(
            f"la tardía cambió el consumo total: {total_antes:.3f} → {total_despues:.3f}"
        )
    print(
        "\n  2 → 3   La tardía **corrigió el reparto sin cambiar el total**. El consumo del\n"
        f"          medidor es el mismo, {total_despues:.3f} kWh: medirlo con más detalle no\n"
        "          crea ni destruye energía. Lo que cambia es a qué franja se le atribuye."
    )

    print("\n          Franja por franja, lo que la interpolación se había equivocado:\n")
    print(f"          {'celda':<34} {'estimado':>9} {'corregido':>10} {'error':>8}")
    for clave in sorted(set(tablas[1]) | set(tablas[2])):
        antes, despues = tablas[1].get(clave, 0.0), tablas[2].get(clave, 0.0)
        print(f"          {clave:<34} {antes:>9.3f} {despues:>10.3f} {despues - antes:>+8.3f}")

    print(
        "\n  Ese error es el que el proyecto existe para medir. No viene de un defecto del\n"
        "  pipeline: viene de que el intervalo cruzaba el borde de franja y hubo que suponer\n"
        "  potencia constante. La lectura tardía es la que revela cuánto se erró — y por eso\n"
        "  la salida declara qué celdas son interpoladas y cuáles medidas."
    )

    titulo("Resultado")
    if problemas:
        for p in problemas:
            print(f"  ✘ {p}")
        return 1
    print("\n  ✅ Los tres escenarios se comportaron como está documentado.\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
