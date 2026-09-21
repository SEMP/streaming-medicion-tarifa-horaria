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
    ESCALERA_HUECO_FIJO,
    ESCALERA_HUECO_LARGO,
    ESCALERA_MAX_INTENTOS,
    ESCALERA_MIN_INTENTO_CON_EXITO,
    LATENCIA_ENLACE_TIPICA,
    LATENCIA_PRIMERA_RESPUESTA,
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
) -> tuple[float, bool, bool]:
    """Simula un pedido completo, con su escalera de reintentos, sobre el bus.

    Devuelve `(segundos consumidos, hubo respuesta, la trama llegó truncada)`.

    El modelo es **bimodal**, que es lo que muestran las capturas reales: o la respuesta
    sale al primer intento en unos 4 segundos, o el pedido cae en la escalera y consume
    decenas de segundos hasta el tope. No hay un medio.

    La escalera es **determinista**: un hueco largo alternando con uno fijo de 10 s, con
    tope de intentos, y cada reintento reenvía el pedido completo. Sin backoff ni jitter.

    Los reintentos **ocupan el bus**: un medidor problemático no solo falla, además retrasa
    a todos los que vienen detrás en la misma cabina. De ahí que la duración de la ronda la
    fije la tasa de fallas y no la velocidad media.
    """
    # El rango medido ya incluye la latencia del enlace típica, así que no se suma:
    # se desplaza según cuánto se aparta el enlace de esta cabina de ese valor típico.
    # Una cabina con enlace bueno queda por debajo del rango y una con enlace malo, arriba.
    primera = rng.uniform(*LATENCIA_PRIMERA_RESPUESTA) + (
        cabina.latencia_enlace - LATENCIA_ENLACE_TIPICA
    )

    if equipos_perfectos:
        return min(primera, timeout), True, False

    prob = min(1.0, medidor.perfil.prob_escalera * cabina.factor_fallas)
    if rng.random() >= prob:
        return primera, True, rng.random() < medidor.perfil.prob_trama_incompleta

    # Escalera: el primer intento ya se gastó y no respondió.
    gastado = primera
    for intento in range(2, ESCALERA_MAX_INTENTOS + 1):
        # Los huecos alternan: largo, fijo, largo, fijo…
        hueco = (
            rng.uniform(*ESCALERA_HUECO_LARGO) if intento % 2 == 0 else ESCALERA_HUECO_FIJO
        )
        if gastado + hueco >= timeout:
            return timeout, False, False
        gastado += hueco

        # Cada reintento reenvía el pedido completo, así que una respuesta exitosa cuesta
        # además su propio tiempo de transmisión.
        if intento >= ESCALERA_MIN_INTENTO_CON_EXITO and rng.random() >= prob:
            respuesta = min(gastado + primera, timeout)
            return respuesta, True, rng.random() < medidor.perfil.prob_trama_incompleta

    return min(gastado, timeout), False, False


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
        duracion = len(cabina) * cfg.timeout_segundos
        return [], inicio_ronda + timedelta(seconds=duracion)

    for medidor in cabina.medidores:
        segundos, respondio, truncada = _tiempo_de_pedido(
            medidor, cabina, rng, cfg.timeout_segundos,
            equipos_perfectos=fallas.equipos_perfectos,
        )
        momento = momento + timedelta(seconds=segundos)
        if momento > cfg.fin:
            break
        if not respondio:
            continue  # sin lectura en este punto: puede dejar una franja sin cerrar

        valor = medidor.contador.leer(momento)

        if rng.random() < fallas.prob_reseteo_contador:
            valor = round(valor * rng.uniform(0.0, 0.02), 3)  # el equipo se reprogramó
        if truncada:
            valor = truncar_valor(valor, rng)

        lecturas.append(
            Lectura(
                medidor_id=medidor.medidor_id,
                cabina_id=cabina.cabina_id,
                lote_id=lote_id,
                secuencia=medidor.posicion_en_bus,
                instante_lectura=momento.replace(microsecond=0),
                registros=(Registro(OBIS_ENERGIA_ABSOLUTA, valor, "kWh"),),
                calidad="estimado" if truncada else "ok",
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

    for indice, cabina in enumerate(parque.cabinas):
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
