"""El parque de medidores: cabinas, buses y perfiles de equipo.

Los medidores no cuelgan de la red uno por uno: están agrupados en **cabinas** y dentro de
cada cabina se comunican por un bus RS-485, que es compartido. Eso obliga a leerlos en
secuencia y es el origen de todo el problema temporal del proyecto.

Este módulo construye un parque sintético con esa estructura, de forma determinista.
"""

from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass
from datetime import datetime

from .consumo import ContadorMedidor

# --------------------------------------------------------------------------- tiempos
#
# Los números de esta sección salen de mediciones sobre un despliegue real: ~410.000
# pedidos en 7 días sobre ~29.000 medidores, cruzados con un monitor de red independiente
# (ping horario, 146.000 muestras). Está marcado qué es medido, qué es decisión de
# implementación del concentrador observado, y qué es nuestro.

TIMEOUT_SEGUNDOS = 120.0
"""Tope por medidor. **Medido:** el 9,5% de los pedidos termina exactamente acá — es un
muro, no una cola."""

PAUSA_ENTRE_MEDIDORES = 10.0
"""Pausa fija que el concentrador deja entre un medidor y el siguiente.

⚠️ **Es una decisión de diseño del concentrador observado, no una propiedad del protocolo
ni de los medidores.** Otro concentrador daría otra ronda. Pesa mucho: sumada a los ~19,8 s
de media por lectura da unos **30 s por medidor**, que es lo que fija la duración de la
ronda."""

DURACION_EXITO_PRIMER_INTENTO = (5.0, 10.0)
"""**Medido:** el grueso de las lecturas exitosas cae acá, y es el 85,5% del total.

Una estimación previa de ~3,5 s resultó ser **la mitad de lo real**: provenía de unas pocas
capturas sesgadas al caso limpio."""

DURACION_EXITO_CON_REINTENTOS = (30.0, 45.0)
"""**Medido:** las lecturas que salen recién después de reintentar aterrizan acá (3,8%).

La distribución de duraciones es **trimodal** —éxito rápido, éxito con reintentos, y muro
del timeout— con valles reales entre las modas (1,2% combinado)."""

LATENCIA_ENLACE_P50 = 1.665
"""**Medido:** RTT mediano del enlace. p90 2,29 s, p99 3,66 s, 92,9% alcanzable.

El enlace explica ~74% de la latencia hasta la primera respuesta (p50 2,26 s); el equipo
agrega unos 600 ms.

⚠️ **Es estacionario a lo largo del día.** Medido en las 24 horas sobre 30 días: p50 entre
1,59 y 1,70 s (±3%). **No hay degradación en hora punta**, así que el simulador no necesita
término diurno y el error de atribución no empeora justo en la franja que más cuesta."""

INTENTOS_MEDIA = 1.51
INTENTOS_MAX_OBSERVADO = 11
"""**Medido:** la media ponderada de intentos por pedido es 1,51 — la gran mayoría acierta
al primero. Varía por modelo entre 1,07 y 3,97, y eso es costo de bus directo: el peor
modelo ocupa ~4× lo que el mejor, por lectura.

⚠️ **El presupuesto es TIEMPO, no un contador de intentos.** Lo que se configura es el
límite por pedido (`TIMEOUT_SEGUNDOS`), y dentro de él se reintenta las veces que entren.
Que el máximo observado sea 11 es simplemente cuántos ciclos caben en el presupuesto: **no
es una constante ni un tope configurado**, y modelarlo como contador se alejaría de la
realidad.

Los reintentos ocurren **dentro del mismo pedido**: uno que falló ya agotó su presupuesto,
no hay una segunda oportunidad programada después."""


@dataclass(frozen=True)
class ModeloMedidor:
    """Un modelo de equipo. La correlación por modelo es el eje más interesante del parque.

    **Medido:** con el mismo enlace y la misma configuración, las tasas de entrega por
    modelo van de **65% a 94%**. Y como los modelos están repartidos por todo el parque,
    esa correlación es **espacialmente dispersa**: ninguna partición por ubicación la aísla.
    Para un pipeline particionado por cabina, ese patrón es invisible.

    ⚠️ Se desconoce *por qué* unos modelos fallan más (se descartaron red, configuración y
    truncamiento de buffer). Para un simulador da igual: se modela como tasa por modelo,
    que es lo observado.
    """

    nombre: str
    prob_fallo_enlace_sano: float
    """Probabilidad de que el pedido no salga, **sobre un enlace que funciona**.

    Este es el "fondo disperso" que un modelo basado solo en caídas de cabina no tiene:
    **el 52% de las lecturas fallidas ocurre sobre enlaces que pinguean perfecto.**"""
    prob_exito_reintento: float
    """Probabilidad de que un reintento rescate la lectura.

    ⚠️ **Reintento y éxito están desacoplados.** No vale "más reintentos = peor modelo": hay
    un modelo que hace 3,97 intentos y entrega 89,5%, y otro que hace 3,12 y entrega 65,5%.
    Para unos modelos reintentar funciona y para otros es tiempo tirado — asumir que el
    reintento siempre rescata la lectura es falso para una parte del parque."""
    marca_checksum: bool
    """Si el equipo marca sus lecturas con checksum incorrecto **sin que haya corrupción**.

    ⚠️ **Es la trampa más peligrosa del dominio.** Un fabricante que es el 55% del parque
    calcula el checksum distinto de lo que el concentrador espera, así que prácticamente
    **el 100% de sus lecturas sale marcada** — y el dato se extrae completo y correcto.

    En el reparto crudo de resultados eso aparece como "~50% de lecturas con checksum
    incorrecto", y leerlo como corrupción se equivoca **por un factor de cinco**. Un
    pipeline que descarte por bandera de calidad **tiraría la mitad de las lecturas
    buenas**."""
    prob_trama_incompleta: float
    """Truncamiento real, que sí es corrupción. Nuestro, no medido."""


MODELOS = {
    # Este es el fabricante mayoritario que marca checksum sin corromper nada.
    "modelo-a": ModeloMedidor("modelo-a", 0.035, 0.60, True, 0.004),
    "modelo-b": ModeloMedidor("modelo-b", 0.069, 0.55, False, 0.006),
    "modelo-c": ModeloMedidor("modelo-c", 0.138, 0.20, False, 0.010),
    # El peor entregador: reintenta mucho y le sirve poco. Es el caso que muestra que
    # reintento y exito estan desacoplados.
    "modelo-d": ModeloMedidor("modelo-d", 0.400, 0.10, False, 0.020),
}
"""Tasas calibradas para que la tasa global de fallas del parque quede cerca del **9,2%
medido**, con el ~52% de esas fallas cayendo sobre enlaces sanos.

⚠️ El reparto **entre** modelos es nuestro: lo medido es que las tasas de entrega por modelo
van de 65% a 94%, no cuánto pesa cada modelo en el parque."""

MEZCLA_MODELOS = {"modelo-a": 0.55, "modelo-b": 0.25, "modelo-c": 0.13, "modelo-d": 0.07}
"""Reparto de modelos en el parque. **Disperso a propósito**: se sortea por medidor y no por
cabina, porque la correlación por modelo no respeta la ubicación."""

PROPORCION_CABINAS_MUERTAS = 0.05
"""**Medido y validado:** 41 equipos muertos arrastran 1.203 medidores, ~29 cada uno — que es
el tamaño de cabina típico. La caída de cabina es real y **persistente**, no momentánea, y
explica el 38% de las fallas.

⚠️ Pero es **menos de la mitad** del problema: el 52% de las fallas ocurre sobre enlaces
sanos. Hacen falta los dos componentes."""

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
    modelo: ModeloMedidor
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

    El enlace explica ~74% de la latencia hasta la primera respuesta. Modelarlo por cabina y
    no por medidor importa: las latencias **están correlacionadas** y no se promedian."""
    enlace_muerto: bool
    """Si el enlace de esta cabina está caído.

    ⚠️ **Es binario a propósito, no una degradación gradual.** Medido: la disponibilidad por
    equipo es **bimodal** —el 90% está ≥90% alcanzable, el 4,7% está muerto, y el medio está
    casi vacío—, y el RTT **no** correlaciona con la disponibilidad. **No existe la población
    "enlace lento degradado".** Un enlace está arriba y rápido, o está caído."""

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


def semilla_derivada(semilla: int, etiqueta: str) -> int:
    """Semilla estable para un identificador, derivada de la semilla global.

    ⚠️ **No usar `hash()` acá.** Python aleatoriza el hash de las cadenas en cada proceso
    (PYTHONHASHSEED), así que `hash((semilla, medidor_id))` da un valor distinto en cada
    ejecución — y con él, un parque distinto. El determinismo es la promesa central del
    simulador: sin él, una prueba que falla no se puede repetir y dos personas que corren la
    misma orden obtienen resultados distintos.

    SHA-256 es estable entre procesos, entre versiones de Python y entre máquinas.

    Derivarla del identificador y no de un contador tiene además una propiedad útil: agregar
    o quitar un medidor **no perturba a los demás**.
    """
    material = f"{semilla}|{etiqueta}".encode()
    return int.from_bytes(hashlib.sha256(material).digest()[:4], "big")


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
            semilla_medidor = semilla_derivada(semilla, medidor_id)
            rng_medidor = random.Random(semilla_medidor)
            medidores.append(
                Medidor(
                    medidor_id=medidor_id,
                    cabina_id=cabina_id,
                    posicion_en_bus=k,
                    modelo=MODELOS[_elegir(rng_medidor, MEZCLA_MODELOS)],
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
                latencia_enlace=round(rng.uniform(1.4, 2.3), 2),
                enlace_muerto=rng.random() < PROPORCION_CABINAS_MUERTAS,
            )
        )

    return Parque(cabinas=tuple(resultado))
