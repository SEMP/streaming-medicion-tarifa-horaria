"""Deduplicación y diferenciación: las dos etapas con estado por medidor.

El orden entre ellas **no es intercambiable**. La deduplicación va primero, porque restar
una lectura contra sí misma produce un consumo de 0 que, con salida por *upsert*, pisa el
valor correcto:

    1ª vez:  consumo = R₂ − R₁       ✔
    2ª vez:  consumo = R₂ − R₂ = 0   ✘  y el 0 reemplaza al bueno

Ver `docs/contratos.md` §1.9.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

import apache_beam as beam
from apache_beam.coders import StrUtf8Coder
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.userstate import (
    BagStateSpec,
    SetStateSpec,
    TimerSpec,
    on_timer,
)

LATENCIA_PERMITIDA_SEGUNDOS = 36 * 3600
"""36 horas, la lateness de `contratos.md` §2.4. El estado tiene que vivir al menos eso: si
expirara antes, un tardío legítimo volvería a parecer nuevo y se contaría dos veces."""

CUARENTENA = "cuarentena"
"""Etiqueta de la salida lateral. Nada se descarta en silencio."""


@dataclass(frozen=True)
class Consumo:
    """El consumo de un intervalo, que es lo que el contador NO trae.

    Un contador da el valor acumulado en un instante; el consumo vive **entre** dos
    lecturas. Por eso este registro tiene dos instantes y no uno.
    """

    medidor_id: str
    cabina_id: str
    desde: str
    hasta: str
    energia_kwh: float
    separacion_minutos: float
    """Cuánto tiempo abarca el intervalo. Es la cota del error de atribución de este dato:
    un intervalo de 40 minutos puede cruzar un borde de franja, y entonces parte de su
    energía se atribuye al lado equivocado."""

    def a_dict(self) -> dict:
        return {
            "medidor_id": self.medidor_id,
            "cabina_id": self.cabina_id,
            "desde": self.desde,
            "hasta": self.hasta,
            "energia_kwh": round(self.energia_kwh, 3),
            "separacion_minutos": round(self.separacion_minutos, 2),
        }


def a_cuarentena(payload: dict, motivo: str) -> dict:
    return {"motivo": motivo, "lectura": payload}


class DeduplicarLecturas(beam.DoFn):
    """Deja pasar una sola vez cada `(medidor_id, instante_lectura)`.

    El estado es **por clave**, así que Beam mantiene un conjunto separado por medidor y dos
    medidores distintos no se interfieren aunque compartieran un identificador.

    El duplicado que interesa es el del **reintento de publicación**: mismo instante, mismo
    valor. Un reintento de *comunicación* produce una lectura nueva, en un instante
    posterior, y no es un duplicado — distinguirlos es lo que evita descartar datos buenos.
    """

    VISTOS = SetStateSpec("vistos", StrUtf8Coder())
    EXPIRA = TimerSpec("expira", TimeDomain.WATERMARK)

    def __init__(self, lateness_segundos: int = LATENCIA_PERMITIDA_SEGUNDOS):
        self.lateness_segundos = lateness_segundos

    def process(
        self,
        elemento: tuple[str, dict],
        vistos=beam.DoFn.StateParam(VISTOS),
        expira=beam.DoFn.TimerParam(EXPIRA),
        ventana=beam.DoFn.WindowParam,
    ):
        _, payload = elemento
        instante = payload.get("instante_lectura")
        if not instante:
            yield beam.pvalue.TaggedOutput(
                CUARENTENA, a_cuarentena(payload, "instante_invalido")
            )
            return

        if instante in vistos.read():
            # Ya lo vimos: no se emite. No es un error, es el caso que la deduplicación
            # existe para atender, así que tampoco va a cuarentena.
            return

        vistos.add(instante)
        # El timer se programa contra el fin de la ventana MÁS la lateness. La entrada de un
        # pipeline de streaming es no acotada: sin expiración, el conjunto crece sin límite.
        expira.set(ventana.end + self.lateness_segundos)
        yield elemento

    @on_timer(EXPIRA)
    def expirar(self, vistos=beam.DoFn.StateParam(VISTOS)):
        """Se dispara una vez por clave y ventana, cuando ya no puede llegar nada para ella."""
        vistos.clear()


class DiferenciarContador(beam.DoFn):
    """Convierte lecturas de un contador acumulado en consumos por intervalo.

    **El problema que resuelve bien:** las lecturas llegan desordenadas, así que restar
    contra «la última que vi» da resultados equivocados. En lugar de eso guarda las lecturas
    de la ventana y, cuando llega una nueva, emite solo **los intervalos que esa lectura
    forma con su vecino anterior y con el posterior**.

    Es a lo sumo dos emisiones por llegada, no una recomputación completa, y es correcto sin
    importar en qué orden lleguen: una lectura tardía que cae en el medio parte el intervalo
    que la contenía y emite las dos mitades, que con salida por *upsert* reemplazan al valor
    grosero anterior.

    **Un consumo negativo nunca es válido**: en el mercado modelado no hay compra de energía
    al usuario, así que el contador solo puede subir. Un retroceso es un reseteo del equipo o
    una trama truncada, y va a cuarentena.
    """

    LECTURAS = BagStateSpec("lecturas", StrUtf8Coder())
    EXPIRA = TimerSpec("expira_dif", TimeDomain.WATERMARK)

    def __init__(self, lateness_segundos: int = LATENCIA_PERMITIDA_SEGUNDOS):
        self.lateness_segundos = lateness_segundos

    def process(
        self,
        elemento: tuple[str, dict],
        lecturas=beam.DoFn.StateParam(LECTURAS),
        expira=beam.DoFn.TimerParam(EXPIRA),
        ventana=beam.DoFn.WindowParam,
    ):
        from datetime import datetime

        medidor_id, payload = elemento
        registro = next(
            (r for r in payload.get("registros", []) if r.get("obis") == "15.8.0"), None
        )
        if registro is None:
            yield beam.pvalue.TaggedOutput(
                CUARENTENA, a_cuarentena(payload, "sin_registro_util")
            )
            return

        instante = payload["instante_lectura"]
        valor = float(registro["valor"])
        cabina_id = payload.get("cabina_id", "")

        previas = [json.loads(x) for x in lecturas.read()]
        lecturas.add(json.dumps([instante, valor, cabina_id]))
        expira.set(ventana.end + self.lateness_segundos)

        ordenadas = sorted([*previas, [instante, valor, cabina_id]], key=lambda x: x[0])
        posicion = next(i for i, x in enumerate(ordenadas) if x[0] == instante)

        # Solo los intervalos que esta lectura forma: con el anterior y con el siguiente.
        for izq, der in (
            (posicion - 1, posicion),
            (posicion, posicion + 1),
        ):
            if izq < 0 or der >= len(ordenadas):
                continue
            (i_desde, v_desde, _), (i_hasta, v_hasta, cab) = ordenadas[izq], ordenadas[der]
            delta = v_hasta - v_desde

            if delta < 0:
                yield beam.pvalue.TaggedOutput(
                    CUARENTENA,
                    a_cuarentena(
                        {"medidor_id": medidor_id, "desde": i_desde, "hasta": i_hasta,
                         "valor_desde": v_desde, "valor_hasta": v_hasta},
                        "contador_retrocede",
                    ),
                )
                continue

            minutos = (
                datetime.fromisoformat(i_hasta) - datetime.fromisoformat(i_desde)
            ).total_seconds() / 60
            yield Consumo(
                medidor_id=medidor_id,
                cabina_id=cab,
                desde=i_desde,
                hasta=i_hasta,
                energia_kwh=delta,
                separacion_minutos=minutos,
            )

    @on_timer(EXPIRA)
    def expirar(self, lecturas=beam.DoFn.StateParam(LECTURAS)):
        lecturas.clear()
