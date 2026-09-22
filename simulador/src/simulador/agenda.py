"""La agenda de pedidos: rondas sobre un bus compartido.

Este es el corazón del simulador y la razón de que el proyecto exista. Los medidores de una
cabina comparten un bus RS-485, así que se consultan **en secuencia**: el medidor que ocupa
la posición k de N se lee cuando la ronda llega hasta él, no cuando uno quisiera.

De ahí sale el problema central: **no se puede leer a todos los medidores en el borde de una
franja**, y el consumo de la franja hay que obtenerlo interpolando entre las dos lecturas que
la rodean.
"""

from __future__ import annotations

import random
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta

from .evento import OBIS_ENERGIA_ABSOLUTA, Lectura, Registro
from .fallas import ConfigFallas, truncar_valor
from .parque import (
    DURACION_EXITO_CON_REINTENTOS,
    DURACION_EXITO_PRIMER_INTENTO,
    LATENCIA_ENLACE_P50,
    PAUSA_ENTRE_MEDIDORES,
    Cabina,
    Medidor,
    Parque,
)


@dataclass(frozen=True)
class ConfigAgenda:
    inicio: datetime
    fin: datetime
    timeout_segundos: float = 120.0
    """Tope por medidor, configurable como en el sistema real. Dentro de ese presupuesto se
    hacen reintentos; agotado, se pasa al siguiente.

    **Medido:** el máximo observado en capturas reales es 119,61 s, o sea que este tope se
    alcanza tal cual."""
    pausa_entre_medidores: float = PAUSA_ENTRE_MEDIDORES
    """Pausa fija entre un medidor y el siguiente. **Es un parámetro del concentrador
    observado, no una constante del protocolo** — otro concentrador daría otra ronda."""
    cadencia_minutos: float | None = None
    """Cada cuánto **arranca** una ronda nueva.

    `None` = rondas continuas, encadenadas una tras otra. Es lo razonable cuando el bus es
    el cuello de botella: no hay nada que ganar esperando.

    Con un valor, las rondas arrancan a intervalo fijo — que es como operaría un dispositivo
    dedicado, que puede consultar cuando quiere y no tiene sentido que lo haga sin parar. Si
    la ronda tarda más que la cadencia, se encadenan igual: no se puede ir más rápido que el
    bus.
    """


def _tiempo_de_pedido(
    medidor: Medidor,
    cabina: Cabina,
    rng: random.Random,
    timeout: float,
    *,
    equipos_perfectos: bool = False,
) -> tuple[float, bool, str]:
    """Simula un pedido completo, con sus reintentos, sobre el bus.

    Devuelve `(segundos consumidos, hubo respuesta, marca de calidad)`.

    La duración es **trimodal**, que es lo que muestran las mediciones:

    | Resultado | Duración | Frecuencia medida |
    |---|---|---|
    | Éxito al primer intento | 5–10 s | 85,5% |
    | Éxito tras reintentar | 30–45 s | 3,8% |
    | Muro del timeout | 120 s | 9,5% |

    Los reintentos ocurren **dentro del mismo pedido** y ocupan el bus: un medidor
    problemático no solo falla, además retrasa a todos los que vienen detrás. De ahí que la
    duración de la ronda la fije la tasa de fallas y no la velocidad media.
    """
    modelo = medidor.modelo
    desvio_enlace = cabina.latencia_enlace - LATENCIA_ENLACE_P50

    def marca(*, permitir_truncamiento: bool = True) -> str:
        """La calidad que el concentrador le pone a la lectura."""
        if permitir_truncamiento and rng.random() < modelo.prob_trama_incompleta:
            return "truncada"  # corrupcion real: el valor no sirve
        if modelo.marca_checksum:
            # El equipo calcula el checksum distinto de lo que el concentrador espera.
            # El dato esta completo y correcto: NO es corrupcion.
            return "checksum_no_verificado"
        return "ok"

    exito_directo = rng.uniform(*DURACION_EXITO_PRIMER_INTENTO) + desvio_enlace

    if equipos_perfectos:
        # La marca de checksum SOBREVIVE al modo sin fallas, y eso es deliberado: no es una
        # falla. Es una caracteristica permanente de ese fabricante, y el pipeline la va a
        # ver siempre. Suprimirla acá escondería justamente la trampa que hay que probar.
        return min(exito_directo, timeout), True, marca(permitir_truncamiento=False)

    # Un enlace caido no entrega casi nada, y es persistente: no es un fallo momentaneo.
    prob_fallo = 0.831 if cabina.enlace_muerto else modelo.prob_fallo_enlace_sano

    if rng.random() >= prob_fallo:
        return min(exito_directo, timeout), True, marca()

    # El pedido no salio al primer intento. Reintenta DENTRO del mismo pedido, y si el
    # reintento funciona la lectura aterriza en la segunda moda.
    if rng.random() < modelo.prob_exito_reintento:
        recuperado = rng.uniform(*DURACION_EXITO_CON_REINTENTOS) + desvio_enlace
        return min(recuperado, timeout), True, marca()

    # Reintentar no sirvio: se consume el presupuesto completo. Esta es la tercera moda, y
    # es la que ocupa el bus sin entregar nada.
    return timeout, False, "sin_respuesta"


def _recorrer_cabina(
    cabina: Cabina,
    inicio_ronda: datetime,
    cfg: ConfigAgenda,
    fallas: ConfigFallas,
    rng: random.Random,
    lote_id: str,
) -> tuple[list[Lectura], datetime]:
    """Una ronda completa sobre una cabina. Devuelve las lecturas y cuándo terminó."""
    momento = inicio_ronda
    lecturas: list[Lectura] = []

    # El enlace de la cabina es compartido: si cae, se pierden TODOS sus medidores a la vez.
    # No es un fallo disperso, es correlacionado, y hay que demostrarlo.
    if not fallas.equipos_perfectos and rng.random() < cabina.prob_caida:
        # Una cabina caída igual consume tiempo: el concentrador espera el timeout de
        # cada medidor antes de darla por perdida.
        duracion = len(cabina) * (cfg.timeout_segundos + cfg.pausa_entre_medidores)
        return [], inicio_ronda + timedelta(seconds=duracion)

    for medidor in cabina.medidores:
        segundos, respondio, calidad = _tiempo_de_pedido(
            medidor, cabina, rng, cfg.timeout_segundos,
            equipos_perfectos=fallas.equipos_perfectos,
        )
        # La pausa entre medidores es del concentrador, no del protocolo, pero pesa: sumada
        # a la media por lectura da unos 30 s por medidor.
        momento = momento + timedelta(seconds=segundos + cfg.pausa_entre_medidores)
        if momento > cfg.fin:
            break
        if not respondio:
            continue  # sin lectura en este punto: puede dejar una franja sin cerrar

        valor = medidor.contador.leer(momento)

        if rng.random() < fallas.prob_reseteo_contador:
            valor = round(valor * rng.uniform(0.0, 0.02), 3)  # el equipo se reprogramó
        if calidad == "truncada":
            valor = truncar_valor(valor, rng)

        lecturas.append(
            Lectura(
                medidor_id=medidor.medidor_id,
                cabina_id=cabina.cabina_id,
                lote_id=lote_id,
                secuencia=medidor.posicion_en_bus,
                instante_lectura=momento.replace(microsecond=0),
                registros=(Registro(OBIS_ENERGIA_ABSOLUTA, valor, "kWh"),),
                calidad=calidad,
                publicado_at=momento,  # se corrige después, al publicar
            )
        )

    return lecturas, momento


def generar(
    parque: Parque,
    cfg: ConfigAgenda,
    fallas: ConfigFallas,
    semilla: int,
) -> Iterator[Lectura]:
    """Produce el flujo de lecturas tal como lo vería el tópico de entrada.

    Las cabinas avanzan **en paralelo** —cada una con su propia ronda— y las lecturas salen
    ordenadas por **instante de publicación**, no de lectura. Ese desfase, más las ráfagas
    tardías, es lo que genera el desorden que el pipeline tiene que tolerar.
    """
    pendientes: list[tuple[datetime, Lectura]] = []

    for cabina in parque.cabinas:
        rng = random.Random((semilla, cabina.cabina_id).__hash__() & 0xFFFFFFFF)
        momento = cfg.inicio
        ronda = 0

        while momento < cfg.fin:
            lote_id = f"LOTE-{cabina.cabina_id}-{ronda:04d}"
            lecturas, fin_ronda = _recorrer_cabina(cabina, momento, cfg, fallas, rng, lote_id)

            # ¿El concentrador pudo publicar al momento, o juntó y publicó más tarde?
            if rng.random() < fallas.prob_rafaga_tardia:
                atraso = timedelta(minutes=rng.randint(*fallas.minutos_rafaga))
            else:
                atraso = timedelta(seconds=rng.uniform(0.2, 3.0))

            for lectura in lecturas:
                publicado = lectura.instante_lectura + atraso
                pendientes.append((publicado, _con_publicacion(lectura, publicado)))

                # Reintento de publicación: la MISMA lectura entra dos veces.
                if rng.random() < fallas.prob_duplicado_publicacion:
                    repetido = publicado + timedelta(seconds=rng.uniform(1, 90))
                    pendientes.append((repetido, _con_publicacion(lectura, repetido)))

            if cfg.cadencia_minutos is None:
                momento = fin_ronda
            else:
                proxima = momento + timedelta(minutes=cfg.cadencia_minutos)
                momento = max(proxima, fin_ronda)
            ronda += 1

    pendientes.sort(key=lambda par: par[0])
    for _, lectura in pendientes:
        yield lectura


def _con_publicacion(lectura: Lectura, publicado: datetime) -> Lectura:
    from dataclasses import replace

    return replace(lectura, publicado_at=publicado)
