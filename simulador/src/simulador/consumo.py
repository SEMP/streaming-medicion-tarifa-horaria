"""Modelo de consumo eléctrico y contador acumulado.

El medidor expone el registro OBIS 15.8.0, que es un **contador acumulado**: solo sube.
Este módulo genera una curva de demanda diaria realista y la integra, de modo que
`ContadorMedidor.leer(instante)` devuelva el valor del contador en cualquier momento.

La curva importa: si el consumo fuera uniforme, separar por franja horaria no tendría
sentido y la demostración no mostraría nada. La punta existe justamente porque la demanda
no es plana.
"""

from __future__ import annotations

import math
from bisect import bisect_right
from dataclasses import dataclass
from datetime import date, datetime, timedelta

PASO_MINUTOS = 1
"""Resolución con la que se integra la curva. Un minuto es suficiente: la grilla de
pedidos más fina que vamos a simular es de minutos."""

MINUTOS_DIA = 24 * 60


def _campana(minuto: int, centro_hora: float, ancho_horas: float, altura: float) -> float:
    """Aporte gaussiano a la demanda, en kW."""
    delta_horas = (minuto / 60) - centro_hora
    return altura * math.exp(-0.5 * (delta_horas / ancho_horas) ** 2)


def perfil_demanda_kw(minuto_del_dia: int, escala: float = 1.0) -> float:
    """Demanda instantánea en kW para un minuto del día, en hora local.

    Forma típica de un usuario residencial: una base permanente (heladera, equipos en
    espera), un repunte de mañana y una **punta pronunciada al anochecer**, que es la que
    justifica que exista una franja de punta.
    """
    base = 0.18
    manana = _campana(minuto_del_dia, centro_hora=7.5, ancho_horas=1.3, altura=0.55)
    tarde = _campana(minuto_del_dia, centro_hora=13.0, ancho_horas=1.8, altura=0.30)
    noche = _campana(minuto_del_dia, centro_hora=20.0, ancho_horas=1.9, altura=1.45)
    return (base + manana + tarde + noche) * escala


@dataclass(frozen=True)
class ContadorMedidor:
    """El contador acumulado de un medidor, consultable en cualquier instante.

    Precalcula la integral de la curva de demanda para todo el período simulado, así una
    lectura es una interpolación y no una integración.
    """

    medidor_id: str
    inicio: datetime
    """Primer instante simulado. Debe ser timezone-aware."""
    valor_inicial_kwh: float
    """Dónde arranca el contador. Un medidor instalado hace años marca miles de kWh."""
    escala: float
    """Factor de consumo del medidor: distingue una casa chica de una grande."""
    _acumulado: tuple[float, ...]
    """kWh acumulados desde `inicio`, uno por cada paso de PASO_MINUTOS."""

    @classmethod
    def crear(
        cls,
        medidor_id: str,
        inicio: datetime,
        dias: int,
        *,
        valor_inicial_kwh: float,
        escala: float,
        variacion_diaria: float = 0.0,
    ) -> ContadorMedidor:
        """Construye el contador integrando la curva de demanda.

        `variacion_diaria` desplaza la escala día a día de forma determinista, para que dos
        días del mismo medidor no salgan idénticos.
        """
        if inicio.tzinfo is None:
            raise ValueError("`inicio` debe ser timezone-aware: la franja depende de la hora local")

        pasos = (dias * MINUTOS_DIA) // PASO_MINUTOS
        acumulado: list[float] = [0.0]
        total = 0.0
        for paso in range(pasos):
            momento = inicio + timedelta(minutes=paso * PASO_MINUTOS)
            minuto_local = momento.hour * 60 + momento.minute
            factor_dia = 1.0 + variacion_diaria * math.sin(paso / MINUTOS_DIA)
            kw = perfil_demanda_kw(minuto_local, escala * factor_dia)
            total += kw * (PASO_MINUTOS / 60)  # kW × h = kWh
            acumulado.append(total)

        return cls(
            medidor_id=medidor_id,
            inicio=inicio,
            valor_inicial_kwh=valor_inicial_kwh,
            escala=escala,
            _acumulado=tuple(acumulado),
        )

    def leer(self, instante: datetime) -> float:
        """Valor del contador en `instante`, en kWh.

        Interpola linealmente entre los pasos precalculados. El resultado **nunca
        decrece**, que es la propiedad que define a un contador acumulado.
        """
        if instante < self.inicio:
            raise ValueError(f"{instante} es anterior al inicio de la simulación ({self.inicio})")

        minutos = (instante - self.inicio).total_seconds() / 60
        paso = minutos / PASO_MINUTOS
        i = int(paso)
        if i >= len(self._acumulado) - 1:
            return round(self.valor_inicial_kwh + self._acumulado[-1], 3)

        fraccion = paso - i
        interpolado = self._acumulado[i] + fraccion * (self._acumulado[i + 1] - self._acumulado[i])
        return round(self.valor_inicial_kwh + interpolado, 3)

    def consumo_entre(self, desde: datetime, hasta: datetime) -> float:
        """Consumo real en kWh entre dos instantes. Es la **verdad de referencia**.

        El pipeline no tiene acceso a esto: solo ve lecturas del contador. Sirve para
        medir cuánto se equivoca la atribución por franja, que es el resultado que el
        proyecto busca cuantificar.
        """
        return round(self.leer(hasta) - self.leer(desde), 3)
