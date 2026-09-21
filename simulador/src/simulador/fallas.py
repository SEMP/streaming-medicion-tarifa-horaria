"""Inyección de fallas, configurable y determinista.

El simulador existe para esto. Un simulador que se porta bien no sirve para demostrar que
el pipeline tolera duplicados, desorden y pérdidas.
"""

from __future__ import annotations

import random
from dataclasses import dataclass


@dataclass(frozen=True)
class ConfigFallas:
    """Probabilidades de cada falla. En cero, el simulador produce un mundo perfecto —
    útil como línea de base contra la cual medir el efecto de cada falla por separado."""

    prob_rafaga_tardia: float = 0.05
    """Que el concentrador pierda enlace y publique más tarde lo que fue juntando."""
    minutos_rafaga: tuple[int, int] = (30, 240)
    """Cuánto se demora esa publicación."""
    prob_duplicado_publicacion: float = 0.02
    """Reintento de publicación: la misma lectura entra dos veces. Es el único duplicado
    real — un reintento de *comunicación* produce una lectura nueva, no un duplicado."""
    prob_reseteo_contador: float = 0.001
    """Cambio o reprogramación del medidor: el contador vuelve a un valor bajo y la resta
    contra la lectura anterior da negativo."""
    equipos_perfectos: bool = False
    """Neutraliza también las fallas que vienen del **perfil del medidor** —reintentos,
    tramas incompletas, fallos de respuesta— y las caídas de cabina.

    Existe porque esas probabilidades viven en el parque y no acá, así que sin este
    interruptor una configuración con todo en cero **no daría una línea de base limpia**: el
    equipo seguiría truncando tramas por su cuenta. Y sin línea de base limpia no se puede
    medir el efecto de cada falla por separado.

    ⚠️ **No suprime la marca de checksum**, y es a propósito: esa marca **no es una falla**.
    Es una característica permanente del fabricante mayoritario, cuyo dato llega completo y
    correcto. El pipeline la va a ver siempre, incluso en un mundo sin fallas, y esconderla
    acá ocultaría la trampa más peligrosa del dominio — que un pipeline que descarte por
    bandera de calidad tiraría la mitad de las lecturas buenas."""


def truncar_valor(valor: float, rng: random.Random) -> float:
    """Simula una trama que se corta **en medio de un número**.

    Es la falla más peligrosa del conjunto: `14380.81` truncado a `14380.8` es un valor
    perfectamente plausible, y ninguna validación de formato lo detecta. Solo se descubre
    comparando contra la lectura anterior del mismo medidor, o con el checksum de la trama.
    """
    texto = f"{valor:.3f}"
    if len(texto) <= 4:
        return valor
    corte = rng.randint(2, len(texto) - 2)
    truncado = texto[:corte].rstrip(".")
    try:
        return float(truncado)
    except ValueError:
        return valor
