"""El parque de medidores: cabinas, buses y perfiles de equipo.

Los medidores no cuelgan de la red uno por uno: están agrupados en **cabinas** y dentro de
cada cabina se comunican por un bus RS-485, que es compartido. Eso obliga a leerlos en
secuencia y es el origen de todo el problema temporal del proyecto.

Este módulo construye un parque sintético con esa estructura, de forma determinista.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import datetime

from .consumo import ContadorMedidor


# --------------------------------------------------------------------------- tiempos
#
# Los números de esta sección salen de 13 capturas reales de comunicación (bytes con
# estampa de tiempo, dos fabricantes). Lo que se midió es la **forma** de cada caso; las
# **frecuencias** no, porque las capturas están elegidas como ejemplo de cada condición.
# Está marcado abajo qué es medido y qué es nuestro.

TIMEOUT_SEGUNDOS = 120.0
"""Tope por medidor. **Medido:** el máximo observado es 119,61 s, o sea que el tope
configurado se alcanza tal cual."""

LATENCIA_PRIMERA_RESPUESTA = (3.5, 4.7)
"""Cuánto tarda una respuesta que sale al primer intento. **Medido**, y llamativamente
angosto.

De esos segundos, unos 2,2 aparecen de forma consistente en los dos fabricantes, lo que
indica que son **del enlace y no del medidor** — ver `Cabina.latencia_enlace`."""

LATENCIA_ENLACE_TIPICA = 2.2
"""Porción de la primera respuesta que corresponde al **enlace y no al medidor**.

**Medido:** aparece consistente en los dos fabricantes, lo que indica que es del enlace. Se
usa como punto de referencia: el rango de `LATENCIA_PRIMERA_RESPUESTA` ya lo incluye, así
que una cabina con enlace peor desplaza ese rango hacia arriba en lugar de sumarse encima."""

ESCALERA_HUECO_LARGO = (14.0, 26.0)
ESCALERA_HUECO_FIJO = 10.00
ESCALERA_MAX_INTENTOS = 10
ESCALERA_MIN_INTENTO_CON_EXITO = 3
"""A partir de qué intento puede salir una respuesta dentro de la escalera.

⚠️ **Inferido, no medido.** Las capturas muestran que la distribución es bimodal y que
**no hay nada entre 4,7 s y 30 s**. Si un reintento pudiera salir en el segundo intento, el
total caería alrededor de los 20 s y esa zona no estaría vacía. Con el éxito recién posible
en el tercero —dos huecos más la retransmisión— el mínimo de la escalera queda por encima de
los 30 s, que es lo observado.

Es una inferencia sobre el mecanismo a partir de la forma de la distribución. A confirmar
con quien tenga las capturas: alcanza con saber cuál fue la **duración mínima** de un pedido
que sí respondió por escalera."""
"""La escalera de reintentos. **Medido:** no es un tiempo sorteado sino un patrón
determinista — un hueco largo alternando con uno fijo de 10,00 s con precisión de
centésimas—, con tope de 10 intentos, y cada reintento reenvía el pedido completo desde
cero. Sin backoff ni jitter."""

# Consecuencia aritmética de lo anterior: un par de huecos consume ~30 s, así que la
# escalera llega a los 120 s alrededor del octavo intento. El tope de intentos y el de
# tiempo se alcanzan casi juntos, lo que concuerda con el máximo medido de 119,61 s.


@dataclass(frozen=True)
class PerfilMedidor:
    """Cómo se comporta un modelo de medidor al ser consultado.

    ⚠️ **La distribución de tiempos es bimodal, no un continuo.** En las capturas no hay
    nada entre 4,7 s y 30 s: o el medidor contesta al primer intento en unos 4 segundos, o
    cae en la escalera de reintentos y consume entre 30 y 120 s. No existe el "medidor
    lento" que responde en 20 s.

    Eso cambia cuál es la variable que importa: **la duración de una ronda la fija la tasa
    de fallas, no la velocidad media.** Un parque de medidores veloces con mal enlace tarda
    mucho más que uno de medidores mediocres con buen enlace.
    """

    nombre: str
    prob_escalera: float
    """Probabilidad de que el pedido no salga al primer intento y caiga en la escalera.

    ⚠️ **Este número es nuestro, no medido.** Las capturas muestran la forma de cada caso
    pero no permiten estimar con qué frecuencia ocurre cada uno. Es el parámetro más
    influyente del simulador y el primero que habría que calibrar con datos de operación.
    """
    prob_trama_incompleta: float
    """Probabilidad de que la respuesta llegue truncada. También nuestro."""


PERFILES = {
    "confiable": PerfilMedidor("confiable", prob_escalera=0.03, prob_trama_incompleta=0.005),
    "intermitente": PerfilMedidor("intermitente", prob_escalera=0.20, prob_trama_incompleta=0.02),
    "problematico": PerfilMedidor("problematico", prob_escalera=0.55, prob_trama_incompleta=0.08),
}
"""Tres perfiles que se distinguen por su **tasa de fallas**, no por su velocidad — que es
lo que la medición mostró que importa."""

MEZCLA_PERFILES = {"confiable": 0.55, "intermitente": 0.35, "problematico": 0.10}

TRAMOS_CABINA = [
    ((1, 9), 0.08),
    ((10, 19), 0.27),
    ((20, 29), 0.19),
    ((30, 49), 0.24),
    ((50, 99), 0.20),
    ((100, 199), 0.02),
]
"""Distribución de tamaños de cabina: (rango de medidores, proporción de cabinas).

Reproduce la forma de un parque real, donde la mayoría de los **medidores** vive en cabinas
de 30 a 100 aunque la mayoría de las **cabinas** sea más chica. Esa asimetría es la que hace
que la ronda dure decenas de minutos para el grueso del parque.
"""


@dataclass(frozen=True)
class Medidor:
    medidor_id: str
    cabina_id: str
    posicion_en_bus: int
    """Orden en que lo alcanza la ronda. Determina su desvío sistemático respecto del borde
    de franja: el medidor k de N se lee siempre alrededor de la misma fracción de la ronda."""
    perfil: PerfilMedidor
    contador: ContadorMedidor


@dataclass(frozen=True)
class Cabina:
    """Un grupo de medidores sobre un bus RS-485 compartido.

    Es la unidad de paralelismo —cabinas distintas se consultan a la vez— y también la
    unidad de falla: si el enlace de la cabina cae, **se caen todos sus medidores juntos**.
    """

    cabina_id: str
    medidores: tuple[Medidor, ...]
    prob_caida: float
    """Probabilidad de que la cabina entera quede inalcanzable durante una ronda."""
    latencia_enlace: float
    """Segundos de latencia que **comparten todos los medidores de la cabina**.

    Los ~2,2 s hasta la primera respuesta aparecen igual en los dos fabricantes medidos, lo
    que indica que son del enlace y no del equipo. Modelarlo por cabina y no por medidor
    importa: significa que las latencias **están correlacionadas** y no se promedian. Una
    cabina con mal enlace es lenta entera, no "algunos medidores lentos"."""
    factor_fallas: float
    """Multiplicador sobre `prob_escalera` de cada medidor, por la calidad del enlace de
    esta cabina. Por el mismo motivo: si el enlace es malo, falla todo lo que cuelga de él."""

    def __len__(self) -> int:
        return len(self.medidores)


@dataclass(frozen=True)
class Parque:
    cabinas: tuple[Cabina, ...]

    @property
    def medidores(self) -> tuple[Medidor, ...]:
        return tuple(m for c in self.cabinas for m in c.medidores)

    def resumen(self) -> str:
        tam = sorted(len(c) for c in self.cabinas)
        n_med = len(self.medidores)
        grandes = sum(len(c) for c in self.cabinas if len(c) >= 30)
        return (
            f"{len(self.cabinas)} cabinas · {n_med} medidores · "
            f"tamaño min/mediana/max = {tam[0]}/{tam[len(tam)//2]}/{tam[-1]} · "
            f"{grandes / n_med:.0%} de los medidores en cabinas de 30+"
        )


def _elegir(rng: random.Random, opciones: dict[str, float] | list[tuple[object, float]]):
    items = list(opciones.items()) if isinstance(opciones, dict) else opciones
    total = sum(p for _, p in items)
    umbral = rng.random() * total
    acumulado = 0.0
    for valor, peso in items:
        acumulado += peso
        if umbral <= acumulado:
            return valor
    return items[-1][0]


def generar_parque(
    *,
    cabinas: int,
    inicio: datetime,
    dias: int,
    semilla: int,
    prob_caida_cabina: float = 0.03,
    tamano_fijo: int | None = None,
) -> Parque:
    """Construye un parque determinista.

    `tamano_fijo` fuerza el tamaño de todas las cabinas. Con `tamano_fijo=1` se obtiene el
    **escenario B**: un dispositivo de lectura por medidor, sin bus compartido y por lo tanto
    sin contención — el caso que el proyecto usa como prueba de concepto.
    """
    rng = random.Random(semilla)
    resultado: list[Cabina] = []

    for i in range(cabinas):
        cabina_id = f"CAB-{i:04d}"
        if tamano_fijo is not None:
            n = tamano_fijo
        else:
            lo, hi = _elegir(rng, TRAMOS_CABINA)
            n = rng.randint(lo, hi)

        medidores: list[Medidor] = []
        for k in range(n):
            medidor_id = f"MED-{i:04d}-{k:03d}"
            # Semilla derivada del identificador: agregar un medidor no perturba a los demás.
            semilla_medidor = hash((semilla, medidor_id)) & 0xFFFFFFFF
            rng_medidor = random.Random(semilla_medidor)
            medidores.append(
                Medidor(
                    medidor_id=medidor_id,
                    cabina_id=cabina_id,
                    posicion_en_bus=k,
                    perfil=PERFILES[_elegir(rng_medidor, MEZCLA_PERFILES)],
                    contador=ContadorMedidor.crear(
                        medidor_id,
                        inicio,
                        dias,
                        valor_inicial_kwh=round(rng_medidor.uniform(1_000, 40_000), 3),
                        escala=round(rng_medidor.uniform(0.4, 2.5), 3),
                        variacion_diaria=0.15,
                    ),
                )
            )
        # La calidad del enlace es propiedad de la cabina y afecta a todos sus medidores.
        resultado.append(
            Cabina(
                cabina_id=cabina_id,
                medidores=tuple(medidores),
                prob_caida=prob_caida_cabina,
                latencia_enlace=round(rng.uniform(1.8, 2.6), 2),
                factor_fallas=round(rng.choice([0.5, 1.0, 1.0, 1.0, 2.5]), 2),
            )
        )

    return Parque(cabinas=tuple(resultado))
