"""Deduplicación y diferenciación: las dos etapas con estado por medidor.

El orden entre ellas **no es intercambiable**. La deduplicación va primero, porque restar
una lectura contra sí misma produce un consumo de 0 que, con salida por *upsert*, pisa el
valor correcto:

    1ª vez:  consumo = R₂ − R₁       OK
    2ª vez:  consumo = R₂ − R₂ = 0   MAL  y el 0 reemplaza al bueno

Ver `docs/contratos.md` sección 1.9.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime

import apache_beam as beam
from apache_beam.coders import StrUtf8Coder
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.userstate import (
    BagStateSpec,
    ReadModifyWriteStateSpec,
    SetStateSpec,
    TimerSpec,
    on_timer,
)

LATENCIA_PERMITIDA_SEGUNDOS = 36 * 3600
"""36 horas, la lateness de `contratos.md` sección 2.4. El estado tiene que vivir al menos eso: si
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
    que la contenía y emite las dos mitades.

    **Atención — El intervalo grosero no se retira**: ya fue emitido, y Beam Python no tiene
    retractaciones. Quien decide cuál sigue vigente es `CeldasVigentes`, aguas abajo. Sumar
    todo lo que emite esta etapa cuenta el consumo dos veces.

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


class CeldasVigentes(beam.DoFn):
    """Mantiene los intervalos vigentes de un medidor y emite el valor **absoluto** de cada
    celda que cambia.

    Reemplaza a lo que antes eran dos pasos —descartar intervalos superados y después sumar
    por celda—, y el motivo por el que son uno solo es la parte importante.

    **Atención — Encadenar dos agregaciones bajo un trigger `ACCUMULATING` cuenta doble.** Cada pane
    de la primera llega a la segunda como un elemento nuevo, y la segunda, que también
    acumula, lo suma otra vez. Con una ventana que dispara dos veces —lo normal en cuanto
    llega un tardío— el resultado se duplica:

        pane 1:   6 kWh   OK
        pane 2:  12 kWh   MAL   y es el que el consumidor se queda, porque el último gana

    No se ve con `TestStream` avanzando el watermark a infinito, porque dispara un solo pane.
    Se ve en producción, que es donde importa.

    Una etapa con estado no tiene ese problema: `process` corre **una vez por elemento**, no
    una vez por pane. Y al emitir el valor absoluto de la celda —no un incremento— el destino
    es un *upsert* puro, que es exactamente lo que el contrato pide (`contratos.md` sección 2.1).

    **Lo que no se reparte.** Un intervalo que cruza un borde y dura más que
    `separacion_maxima_minutos` no se interpola: su energía no entra en `energia_kwh`, la celda
    queda con `indeterminada = True` y `minutos_indeterminados` dice cuánto de la franja quedó
    sin cubrir. El resto de la celda **se conserva** — descartarla entera tiraría los intervalos
    buenos, y distinguir «consumió poco» de «falta un pedazo» es justamente para lo que están
    esos campos.

    **La regla de vigencia.** Los intervalos de un medidor parten la línea de tiempo, y cada
    uno queda identificado por su borde izquierdo. Partir uno exige una lectura interior, que
    acerca el borde derecho: un intervalo solo puede **acortarse**, nunca estirarse. Entre
    varios que empiezan en el mismo instante, vale el más corto. Es función pura del dato y no
    del orden de llegada, así que un *replay* converge al mismo resultado.
    """

    INTERVALOS = ReadModifyWriteStateSpec("intervalos", StrUtf8Coder())
    CELDAS = ReadModifyWriteStateSpec("celdas", StrUtf8Coder())
    EXPIRA = TimerSpec("expira_celdas", TimeDomain.WATERMARK)

    def __init__(self, calendario, lateness_segundos: int = LATENCIA_PERMITIDA_SEGUNDOS):
        self.calendario = calendario
        self.lateness_segundos = lateness_segundos

    def process(
        self,
        elemento: tuple[str, Consumo],
        intervalos=beam.DoFn.StateParam(INTERVALOS),
        celdas=beam.DoFn.StateParam(CELDAS),
        expira=beam.DoFn.TimerParam(EXPIRA),
        ventana=beam.DoFn.WindowParam,
    ):
        medidor, consumo = elemento

        vigentes = json.loads(intervalos.read() or "{}")
        previo = vigentes.get(consumo.desde)
        if previo is not None and previo[0] <= consumo.hasta:
            # Ya hay uno igual o más corto para ese borde izquierdo: este quedó superado.
            return

        vigentes[consumo.desde] = [consumo.hasta, consumo.energia_kwh, consumo.cabina_id]
        intervalos.write(json.dumps(vigentes))
        expira.set(ventana.end + self.lateness_segundos)

        tabla = self._recalcular(medidor, vigentes)
        anteriores = json.loads(celdas.read() or "{}")
        celdas.write(json.dumps(tabla))

        # Solo lo que cambió. Reemitir una celda idéntica no rompe nada —el upsert es
        # idempotente— pero gasta ancho de banda del tópico y ruido en el tablero.
        for clave, valor in tabla.items():
            if anteriores.get(clave) != valor:
                yield (clave, valor)

    def _recalcular(self, medidor: str, vigentes: dict) -> dict:
        """Recalcula **todas** las celdas del medidor desde los intervalos vigentes.

        Recalcular todo en lugar de solo lo que tocó la lectura nueva es más caro, y es a
        propósito: el resultado no depende de qué llegó antes, así que no hay forma de que un
        orden de llegada raro deje una celda desactualizada. El costo está acotado —los
        intervalos de un medidor en un día son del orden de cien— y la alternativa es un
        cálculo incremental cuya corrección habría que demostrar.
        """
        from .franjas import repartir_por_franja

        tolerado = self.calendario.separacion_maxima_minutos
        tabla: dict[str, dict] = {}
        for desde, (hasta, energia, cabina) in vigentes.items():
            separacion = (
                datetime.fromisoformat(hasta) - datetime.fromisoformat(desde)
            ).total_seconds() / 60

            for parte in repartir_por_franja(
                datetime.fromisoformat(desde), datetime.fromisoformat(hasta),
                energia, self.calendario,
            ):
                clave = f"{medidor}|{parte.fecha_local}|{parte.franja}"
                celda = tabla.setdefault(
                    clave,
                    {
                        "energia_kwh": 0.0,
                        "minutos_cubiertos": 0.0,
                        "minutos_indeterminados": 0.0,
                        "interpolada": False,
                        "indeterminada": False,
                        "separacion_maxima_minutos": 0.0,
                        "cabina_id": "",
                        "intervalos_usados": 0,
                    },
                )
                celda["cabina_id"] = cabina or celda["cabina_id"]
                celda["separacion_maxima_minutos"] = max(
                    celda["separacion_maxima_minutos"], separacion
                )

                # El umbral solo aplica al reparto. Un intervalo largo que entra ENTERO en una
                # franja no tiene error de atribución: sus dos extremos se midieron y toda su
                # energía pertenece a esa franja. Lo que no se puede sostener es repartir por
                # interpolación a lo largo de un hueco de horas (decisión 13).
                if parte.interpolada and separacion > tolerado:
                    celda["indeterminada"] = True
                    celda["minutos_indeterminados"] += parte.minutos
                    continue

                celda["energia_kwh"] = round(celda["energia_kwh"] + parte.energia_kwh, 6)
                celda["minutos_cubiertos"] += parte.minutos
                celda["interpolada"] = celda["interpolada"] or parte.interpolada
                celda["intervalos_usados"] += 1
        return tabla

    @on_timer(EXPIRA)
    def expirar(
        self,
        intervalos=beam.DoFn.StateParam(INTERVALOS),
        celdas=beam.DoFn.StateParam(CELDAS),
    ):
        intervalos.clear()
        celdas.clear()
