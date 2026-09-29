"""La cadena de transformación completa: de bytes de Kafka a celdas de salida.

Es lo que va en el hueco que deja [`esqueleto.construir`]. El orden **no es arbitrario** y
cada paso tiene su razón, documentada en el docstring de cada función:

    parsear → marcar tiempo de evento → ventanear → deduplicar → diferenciar
            → celdas vigentes → serializar

Las mismas piezas que prueba `pipeline.demostracion` con `TestStream`, pero alimentadas por
Kafka en lugar de por un flujo sintético. Que sean **las mismas** es el punto: lo que la
demostración demuestra es lo que corre en producción, no una maqueta parecida.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime

import apache_beam as beam
from apache_beam.typehints import KV

from .config import Ajustes
from .franjas import CalendarioTarifario
from .transformaciones import (
    CUARENTENA,
    CeldasVigentes,
    DeduplicarLecturas,
    DiferenciarContador,
    a_cuarentena,
)

log = logging.getLogger(__name__)

LATENCIA_PERMITIDA_SEGUNDOS = 36 * 3600
"""`contratos.md` §2.4."""

DIA_SEGUNDOS = 24 * 3600


@dataclass
class Salidas:
    """Lo que la cadena entrega: el resultado y lo que no se pudo procesar.

    Son dos tópicos distintos y no uno con un campo de error, porque tienen consumidores,
    retenciones y volúmenes distintos. Y porque el volumen de la cuarentena es una señal
    operativa: si sube, el problema está en la red o en el pipeline.
    """

    consumo: beam.PCollection
    cuarentena: beam.PCollection


def desplazamiento_del_dia_local(cal: CalendarioTarifario) -> int:
    """Cuántos segundos hay que correr la ventana diaria para que empiece a medianoche local.

    Beam ventanea sobre el instante absoluto, así que una `FixedWindows(1 día)` sin desplazar
    cortaría a medianoche UTC — las 21:00 del día anterior en Asunción, en pleno horario de
    `punta`. El corte quedaría en el medio de la franja más cara.

    Se calcula del calendario en lugar de escribirlo a mano: si alguien cambia la zona en la
    configuración, el desplazamiento la sigue.
    """
    ahora = datetime.now(cal.zona)
    desplazamiento = -ahora.utcoffset().total_seconds()  # type: ignore[union-attr]
    return int(desplazamiento) % DIA_SEGUNDOS


def parsear(registro: tuple[bytes, bytes]):
    """Bytes de Kafka → diccionario, con el medidor como clave.

    Un mensaje que no sea JSON válido no puede ni siquiera ir a cuarentena con su
    `medidor_id`, porque no se le puede leer. Va igual, con los bytes crudos: **nada se
    descarta en silencio**, y un mensaje ilegible en el tópico es exactamente el tipo de
    problema que hay que poder ver.
    """
    clave, valor = registro
    try:
        payload = json.loads(valor.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        yield beam.pvalue.TaggedOutput(
            CUARENTENA,
            {"motivo": "json_invalido", "detalle": str(error), "bytes": repr(valor[:200])},
        )
        return

    medidor = payload.get("medidor_id") or (clave.decode("utf-8", "replace") if clave else "")
    if not medidor:
        yield beam.pvalue.TaggedOutput(CUARENTENA, a_cuarentena(payload, "sin_medidor_id"))
        return

    yield (medidor, payload)


def marcar_tiempo_de_evento(elemento: tuple[str, dict]):
    """Pone el timestamp del dominio, que **no** es el del record de Kafka.

    El record trae cuándo se publicó; `instante_lectura` trae cuándo se midió. Entre los dos
    puede haber horas —es justamente el retraso que justifica la lateness— y ventanear por el
    de publicación metería consumo en el día equivocado.

    El instante lo pone el concentrador, no el medidor (decisión 3): muchos equipos tienen el
    reloj mal o no reportan hora. Acá ya viene saneado desde la fuente.
    """
    medidor, payload = elemento
    crudo = payload.get("instante_lectura")
    try:
        momento = datetime.fromisoformat(crudo)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        yield beam.pvalue.TaggedOutput(CUARENTENA, a_cuarentena(payload, "instante_invalido"))
        return

    if momento.tzinfo is None:
        # Sin offset, el instante se interpretaría en la zona del proceso y la franja
        # atribuida dependería de en qué máquina corre el pipeline. Ver decisión 4.
        yield beam.pvalue.TaggedOutput(CUARENTENA, a_cuarentena(payload, "instante_sin_huso"))
        return

    yield beam.window.TimestampedValue((medidor, payload), momento.timestamp())


def a_bytes(par: tuple[str, dict]) -> tuple[bytes, bytes]:
    clave, valor = par
    return clave.encode("utf-8"), json.dumps(valor, ensure_ascii=False).encode("utf-8")


def cuarentena_a_bytes(registro: dict) -> tuple[bytes, bytes]:
    """La cuarentena se clavetea por medidor cuando se lo conoce.

    Cuando no —un mensaje ilegible—, va sin clave: Kafka la reparte por turno y no hay orden
    que preservar, porque no hay estado que dependa de ella.
    """
    medidor = (registro.get("lectura") or {}).get("medidor_id", "")
    return (
        medidor.encode("utf-8") if medidor else b"",
        json.dumps(registro, ensure_ascii=False).encode("utf-8"),
    )


def cadena(
    ajustes: Ajustes, cal: CalendarioTarifario
) -> Callable[[beam.PCollection], Salidas]:
    """Devuelve la transformación que `esqueleto.construir` mete entre las dos puntas.

    El calendario se pasa **ya cargado** en lugar de leerlo dentro de las etapas: así se
    valida una sola vez, al arrancar, y un calendario mal formado rompe el arranque en lugar
    de aparecer como un resultado raro tres horas después. Es un objeto chico y serializable,
    así que viaja a los workers sin problema.
    """
    desplazamiento = desplazamiento_del_dia_local(cal)
    log.info(
        "ventana diaria desplazada %d s para alinear con la medianoche de %s",
        desplazamiento, cal.zona,
    )

    def aplicar(crudas: beam.PCollection) -> Salidas:
        parseadas = crudas | "Parsear" >> beam.FlatMap(parsear).with_outputs(
            CUARENTENA, main="ok"
        )
        marcadas = parseadas.ok | "MarcarTiempo" >> beam.FlatMap(
            marcar_tiempo_de_evento
        ).with_outputs(CUARENTENA, main="ok")

        # **Sin trigger, y no es un olvido.** Los triggers disparan en un `GroupByKey` o un
        # `Combine`, y acá no hay ninguno: la agregación por celda la hace `CeldasVigentes`,
        # que es un `ParDo` con estado. Ver la decisión 12 — se llegó a eso porque encadenar
        # dos agregaciones bajo `ACCUMULATING` contaba doble.
        #
        # Configurar un trigger igual sería peor que no hacerlo: sugiere un comportamiento
        # que no ocurre. Lo que la ventana sí aporta es el `window.end` contra el que se
        # programan los timers, y la `allowed_lateness`, que fija cuándo expira el estado.
        #
        # El efecto práctico es que la salida se emite **por lectura que cambia una celda**,
        # no cada N segundos: más reactivo que el pane temprano que reemplaza.
        ventaneadas = marcadas.ok | "VentanaDiaria" >> beam.WindowInto(
            beam.window.FixedWindows(DIA_SEGUNDOS, offset=desplazamiento),
            allowed_lateness=LATENCIA_PERMITIDA_SEGUNDOS,
        )

        deduplicadas = ventaneadas | "Deduplicar" >> beam.ParDo(
            DeduplicarLecturas()
        ).with_outputs(CUARENTENA, main="ok")
        consumos = deduplicadas.ok | "Diferenciar" >> beam.ParDo(
            DiferenciarContador()
        ).with_outputs(CUARENTENA, main="ok")

        celdas = (
            consumos.ok
            | "ClavearPorMedidor" >> beam.Map(lambda c: (c.medidor_id, c))
            | "Celdas" >> beam.ParDo(CeldasVigentes(cal))
            | "ABytes" >> beam.Map(a_bytes).with_output_types(KV[bytes, bytes])
        )

        # Las dos primeras cuarentenas salen **antes** de ventanear, así que están en la
        # ventana global; las dos con estado salen después, en la ventana diaria. `Flatten`
        # no junta colecciones con ventanas distintas, y a la cuarentena la ventana no le
        # aporta nada —es un registro, no una agregación—, así que se unifica en la global.
        con_estado = (
            [getattr(deduplicadas, CUARENTENA), getattr(consumos, CUARENTENA)]
            | "JuntarCuarentenaConEstado" >> beam.Flatten()
            | "CuarentenaAVentanaGlobal" >> beam.WindowInto(beam.window.GlobalWindows())
        )
        cuarentena = (
            [getattr(parseadas, CUARENTENA), getattr(marcadas, CUARENTENA), con_estado]
            | "JuntarCuarentena" >> beam.Flatten()
            | "CuarentenaABytes" >> beam.Map(cuarentena_a_bytes).with_output_types(
                KV[bytes, bytes]
            )
        )

        return Salidas(consumo=celdas, cuarentena=cuarentena)

    return aplicar
