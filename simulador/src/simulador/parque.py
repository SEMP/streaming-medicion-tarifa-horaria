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


@dataclass(frozen=True)
class PerfilMedidor:
    """Cómo se comporta un modelo de medidor al ser consultado.

    El parque no es homogéneo: hay equipos que responden unos pocos registros en segundos y
    otros que devuelven muchísimos datos sobre un enlace malo, tardando minutos y
    necesitando reintentos.
    """

    nombre: str
    segundos_respuesta: tuple[float, float]
    """Rango (mínimo, máximo) de lo que tarda una respuesta exitosa."""
    prob_reintento: float
    """Probabilidad de que un intento falle y haya que repetirlo dentro del timeout."""
    prob_trama_incompleta: float
    """Probabilidad de que la respuesta llegue truncada."""
    prob_fallo_total: float
    """Probabilidad de agotar el timeout sin obtener respuesta."""


PERFILES = {
    "rapido": PerfilMedidor("rapido", (1.5, 4.0), 0.02, 0.005, 0.005),
    "lento": PerfilMedidor("lento", (15.0, 45.0), 0.10, 0.02, 0.02),
    "inestable": PerfilMedidor("inestable", (20.0, 110.0), 0.35, 0.08, 0.12),
}
"""Tres perfiles representativos. `inestable` modela el equipo que devuelve muchos datos
sobre un enlace con segundos de latencia: tarda, reintenta y a veces no llega."""

MEZCLA_PERFILES = {"rapido": 0.55, "lento": 0.35, "inestable": 0.10}

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
        resultado.append(
            Cabina(cabina_id=cabina_id, medidores=tuple(medidores), prob_caida=prob_caida_cabina)
        )

    return Parque(cabinas=tuple(resultado))
