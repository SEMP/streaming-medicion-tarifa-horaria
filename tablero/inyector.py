"""Inyector de irregularidades: pedir a mano lo que el simulador tira por probabilidad.

El simulador sabe producir las cinco fallas del dominio, pero las decide con un sorteo fijado
al arrancar. `pipeline.demostracion` sí las produce de forma determinista, pero guionadas de
antemano y con `TestStream`, sin Kafka de por medio. Falta lo del medio: **pedir una
irregularidad concreta, en vivo, y ver el efecto sobre el stack real**.

Todo lo de acá son funciones puras salvo [`enviar`], y no hay un solo `import marimo`: se
prueba con `pytest` sin levantar un notebook.

**El detalle que hace que esto funcione sin falsificar nada:** `Lectura.a_dict` calcula el
`event_id` como `sha256("<medidor_id>|<instante_lectura>")[:16]`. Republicar el mismo par
produce **el mismo identificador por construcción**, que es exactamente lo que el deduplicador
reconoce. El botón del duplicado no simula un duplicado: produce uno.
"""

from __future__ import annotations

import random
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta

from simulador.evento import Lectura, Registro
from simulador.fallas import truncar_valor
from simulador.publicador import publicar

MEDIDOR = "MED-DEMO-001"
CABINA = "CAB-DEMO"
LOTE = "LOTE-TABLERO"
"""Un medidor propio, que el simulador no genera: así lo inyectado no se confunde con las
21.000 lecturas de una corrida del perfil `demo`."""

HORA_ANCLA = (17, 40)
"""La primera lectura arranca a las 17:40 locales, como en `pipeline.demostracion`.

Con pasos de 15 minutos, el tercer intervalo va de 17:55 a 18:10 y **cruza
las 18:00**, que es donde empieza `punta` en el calendario de ejemplo. Sin ese cruce la
lectura tardía sobre el borde no tendría nada que corregir."""

PASO_MINUTOS = 15
CONSUMO_POR_MINUTO = 0.06
"""0,9 kWh cada 15 minutos: números chicos y redondos, fáciles de verificar a ojo."""

CONTADOR_INICIAL = 100.0
DESVIO_EN_EL_BORDE = 0.20
"""Cuánto se aparta la lectura del borde de lo que la interpolación había estimado.

**Sin esto el botón 3 no demostraría nada.** El consumo simulado es lineal, así que interpolar
un intervalo que cruza las 18:00 acierta exacto y la lectura tardía no corrige nada: cambiaría
solo la etiqueta, de `interpolada` a `medida`.

Apartarla 0,2 kWh modela lo que pasa de verdad, y es la razón de ser del proyecto: la
interpolación **supone potencia constante**, y en el borde de `punta` justamente no lo es. Con
el desvío, la tardía revela que se consumió menos antes de las 18:00 y más después — el total
no se mueve, pero 0,2 kWh se mudan de `resto` a `punta`."""
UMBRAL_INDETERMINADO_MINUTOS = 90
"""El de la decisión 13. El hueco largo tiene que superarlo para que la celda salga
`indeterminada`; se usa 100 minutos para no quedar al filo."""


def _registro(valor: float) -> tuple[Registro, ...]:
    return (Registro(obis="15.8.0", naturaleza="acumulado", valor=round(valor, 3), unidad="kWh"),)


def construir(
    medidor_id: str,
    cabina_id: str,
    instante: datetime,
    valor: float,
    *,
    secuencia: int = 0,
    calidad: str = "ok",
) -> Lectura:
    """Arma una lectura con la misma forma que publica el concentrador.

    `publicado_at` se fija igual al instante de lectura: el inyector publica al toque, y así
    el retraso de publicación queda en cero y no contamina la lectura del tablero.
    """
    return Lectura(
        medidor_id=medidor_id,
        cabina_id=cabina_id,
        lote_id=LOTE,
        secuencia=secuencia,
        instante_lectura=instante,
        registros=_registro(valor),
        calidad=calidad,
        publicado_at=instante,
    )


def _valor(lectura: Lectura) -> float:
    return lectura.registros[0].valor


def _hoy_a(hora: int, minuto: int, ahora: datetime) -> datetime:
    return ahora.replace(hour=hora, minute=minuto, second=0, microsecond=0)


# --------------------------------------------------------------------------- las seis


def normal(anterior: Lectura | None, *, ahora: datetime) -> Lectura:
    """Contador avanzado, un paso después de la última. Es la línea de base."""
    if anterior is None:
        return construir(MEDIDOR, CABINA, _hoy_a(*HORA_ANCLA, ahora), CONTADOR_INICIAL)
    instante = anterior.instante_lectura + timedelta(minutes=PASO_MINUTOS)
    return construir(
        MEDIDOR,
        CABINA,
        instante,
        _valor(anterior) + PASO_MINUTOS * CONSUMO_POR_MINUTO,
        secuencia=anterior.secuencia + 1,
    )


def duplicado(anterior: Lectura | None, *, ahora: datetime) -> Lectura:
    """La última lectura, tal cual.

    No hay nada que inventar: mismo medidor y mismo instante dan el mismo `event_id`, así que
    esto **es** un reintento de publicación, no una imitación. Lo tiene que descartar el
    deduplicador y el total no se debe mover.
    """
    if anterior is None:
        return normal(None, ahora=ahora)
    return anterior


def tardia_en_el_borde(anterior: Lectura | None, *, ahora: datetime) -> Lectura:
    """Una lectura con instante **anterior** al último, justo sobre el borde de `punta`.

    Parte en dos el intervalo que cruzaba las 18:00 y reemplaza una estimación por una
    medición. El total no cambia —medir mejor no crea energía—; lo que cambia es **a qué
    franja se atribuye**: unos 0,2 kWh se mudan de `resto` a `punta`, y la celda pasa de
    `interpolada` a `medida`. Ver `DESVIO_EN_EL_BORDE` para por qué ese desvío tiene que
    existir.
    """
    borde = _hoy_a(18, 0, ahora)
    if anterior is None:
        return construir(MEDIDOR, CABINA, borde, CONTADOR_INICIAL)
    ancla = _hoy_a(*HORA_ANCLA, ahora)
    minutos = (borde - ancla).total_seconds() / 60
    estimado = CONTADOR_INICIAL + minutos * CONSUMO_POR_MINUTO
    return construir(
        MEDIDOR,
        CABINA,
        borde,
        estimado - DESVIO_EN_EL_BORDE,
        secuencia=anterior.secuencia + 1,
    )


def reseteo(anterior: Lectura | None, *, ahora: datetime) -> Lectura:
    """Contador que retrocede: el equipo se cambió o se reprogramó.

    Como en el mercado modelado no hay compra de energía al usuario, el contador solo puede
    subir — un retroceso nunca es una medición válida. Tiene que ir a cuarentena con motivo
    `contador_retrocede`.
    """
    base = normal(anterior, ahora=ahora)
    return construir(MEDIDOR, CABINA, base.instante_lectura, 1.0, secuencia=base.secuencia)


def truncada(anterior: Lectura | None, *, ahora: datetime, semilla: int = 7) -> Lectura:
    """Trama cortada en medio de un número.

    El caso fino: `014380.81` cortado en `01438` **sigue siendo un número válido**, así que no
    lo agarra ninguna validación de formato. Lo agarra la comparación contra la lectura
    anterior, aguas abajo.
    """
    base = normal(anterior, ahora=ahora)
    cortado = truncar_valor(_valor(base), random.Random(semilla))
    return construir(
        MEDIDOR, CABINA, base.instante_lectura, cortado,
        secuencia=base.secuencia, calidad="truncada",
    )


def hueco_largo(anterior: Lectura | None, *, ahora: datetime) -> Lectura:
    """Una lectura a más de 90 minutos de la anterior.

    Es la única de las seis que muestra al sistema **diciendo «no sé»**: por encima del umbral
    de la decisión 13 el cruce no se reparte, la celda sale `indeterminada` y
    `minutos_indeterminados` dice cuánto quedó sin cubrir. Declarar la incertidumbre en lugar
    de inventar un número es el argumento central del proyecto.
    """
    salto = UMBRAL_INDETERMINADO_MINUTOS + 10
    if anterior is None:
        return construir(MEDIDOR, CABINA, _hoy_a(*HORA_ANCLA, ahora), CONTADOR_INICIAL)
    return construir(
        MEDIDOR,
        CABINA,
        anterior.instante_lectura + timedelta(minutes=salto),
        _valor(anterior) + salto * CONSUMO_POR_MINUTO,
        secuencia=anterior.secuencia + 1,
    )


@dataclass(frozen=True)
class Accion:
    clave: str
    titulo: str
    espera: str
    """Qué tiene que pasar. Es lo que se lee en voz alta en la demostración."""
    construir: Callable[..., Lectura]


ACCIONES: tuple[Accion, ...] = (
    Accion("normal", "Lectura normal",
           "Aparece o crece la celda de la franja que corresponde.", normal),
    Accion("duplicado", "Duplicado de publicación",
           "Nada cambia: mismo event_id, lo descarta el deduplicador.", duplicado),
    Accion("tardia", "Tardía sobre el borde",
           "Corrige el reparto sin mover el total; la celda pasa a `medida`.",
           tardia_en_el_borde),
    Accion("reseteo", "Reseteo de contador",
           "A cuarentena, con motivo `contador_retrocede`.", reseteo),
    Accion("truncada", "Trama truncada",
           "A cuarentena, o un valor absurdo según dónde caiga el corte.", truncada),
    Accion("hueco", "Hueco largo",
           "Celda `indeterminada`, con minutos_indeterminados > 0.", hueco_largo),
)

POR_CLAVE = {a.clave: a for a in ACCIONES}


def enviar(lectura: Lectura, *, servidores: str, topico: str) -> int:
    """Publica una lectura al tópico crudo. Lo único de este módulo que toca la red."""
    return publicar([lectura], servidores=servidores, topico=topico)
