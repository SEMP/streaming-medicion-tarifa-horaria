"""Calendario tarifario: carga, validación y atribución de franja.

Todo lo de acá son **funciones puras**: no dependen de Kafka, de Beam ni de la
infraestructura, y se prueban con `pytest` común. Es deliberado — permite trabajar en la
lógica de franjas sin esperar a que nada esté levantado.

Las interfaces que consume el pipeline son [`fecha_y_franja`] y [`repartir_por_franja`].
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

MINUTOS_DIA = 24 * 60


class CalendarioInvalido(ValueError):
    """La configuración no describe un calendario utilizable.

    Se levanta al cargar, no al usar: un calendario mal formado tiene que romper el arranque
    del pipeline y no aparecer como un resultado raro tres horas después.
    """


@dataclass(frozen=True)
class Rango:
    """Un tramo contiguo del día, en minutos desde medianoche local, `[desde, hasta)`."""

    desde: int
    hasta: int

    def contiene(self, minuto: int) -> bool:
        return self.desde <= minuto < self.hasta


@dataclass(frozen=True)
class CalendarioTarifario:
    zona: ZoneInfo
    separacion_maxima_minutos: int
    """Por encima de esta separación entre las dos lecturas que rodean un borde, el consumo
    se marca indeterminado en lugar de interpolarse. ⚠️ Pendiente P1: el valor todavía no
    está decidido."""
    cadencia_objetivo_minutos: int
    _por_minuto: tuple[str, ...]
    """Tabla de 1440 posiciones: minuto del día → nombre de franja.

    Se precomputa al cargar. La alternativa —recorrer los rangos en cada evento— es una
    búsqueda lineal que corre **una vez por lectura**, y con millones de lecturas por día esa
    diferencia se nota. Además, construir la tabla es lo que valida la cobertura: si algún
    minuto queda sin asignar, hay un hueco.
    """

    @property
    def nombres(self) -> tuple[str, ...]:
        vistos: list[str] = []
        for nombre in self._por_minuto:
            if nombre not in vistos:
                vistos.append(nombre)
        return tuple(vistos)

    def franja_de_minuto(self, minuto_local: int) -> str:
        return self._por_minuto[minuto_local]

    def duracion_minutos(self, nombre: str) -> int:
        """Cuántos minutos del día cubre una franja. Es el denominador del error de
        atribución que cada resultado declara."""
        return sum(1 for f in self._por_minuto if f == nombre)


def _minutos(texto: str, *, contexto: str) -> int:
    """Convierte "HH:MM" a minutos desde medianoche. Acepta 24:00 como fin del día."""
    try:
        horas, minutos = texto.split(":")
        valor = int(horas) * 60 + int(minutos)
    except (ValueError, AttributeError) as error:
        raise CalendarioInvalido(
            f"{contexto}: '{texto}' no tiene formato HH:MM"
        ) from error
    if not 0 <= valor <= MINUTOS_DIA:
        raise CalendarioInvalido(f"{contexto}: '{texto}' está fuera del día")
    return valor


def cargar_calendario(ruta: str | Path) -> CalendarioTarifario:
    """Carga y valida el calendario tarifario.

    Los mensajes de error dicen **qué** está mal y no solo que algo está mal: una
    configuración tarifaria la edita gente que no escribió este código, y "calendario
    inválido" a secas obliga a leer el fuente para entender qué corregir.
    """
    ruta = Path(ruta)
    try:
        crudo = tomllib.loads(ruta.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise CalendarioInvalido(f"no existe el archivo de calendario: {ruta}") from error
    except tomllib.TOMLDecodeError as error:
        raise CalendarioInvalido(f"{ruta} no es TOML válido: {error}") from error

    # Se comprueba la presencia por separado: `ZoneInfoNotFoundError` HEREDA de `KeyError`,
    # así que un solo `except KeyError` confundiría "falta la clave" con "la zona no existe"
    # y daría el mensaje equivocado justo en el error más común de editar esta configuración.
    if "zona_horaria" not in crudo:
        raise CalendarioInvalido(
            "falta `zona_horaria`. Debe ser un identificador IANA, "
            "por ejemplo 'America/Asuncion'"
        )
    try:
        zona = ZoneInfo(crudo["zona_horaria"])
    except Exception as error:
        raise CalendarioInvalido(
            f"`zona_horaria` desconocida: {crudo['zona_horaria']!r}. "
            "Debe ser un identificador IANA, por ejemplo 'America/Asuncion'"
        ) from error

    franjas = crudo.get("franjas")
    if not franjas:
        raise CalendarioInvalido("no hay franjas definidas")

    # Regla 2: nombres únicos. Si una franja aparece dos veces, la clave de salida
    # `medidor|fecha|franja` dejaría de identificar una celda sin ambigüedad.
    nombres = [f.get("nombre") for f in franjas]
    repetidos = {n for n in nombres if nombres.count(n) > 1}
    if repetidos:
        raise CalendarioInvalido(
            f"nombres de franja repetidos: {sorted(repetidos)}. "
            "Cada franja va una sola vez, con todos sus rangos juntos"
        )

    # La tabla arranca vacía: construirla ES la validación de cobertura.
    tabla: list[str | None] = [None] * MINUTOS_DIA

    for franja in franjas:
        nombre = franja.get("nombre")
        if not nombre:
            raise CalendarioInvalido("hay una franja sin `nombre`")
        for rango in franja.get("rangos", []):
            contexto = f"franja '{nombre}'"
            desde = _minutos(rango.get("desde", ""), contexto=contexto)
            hasta = _minutos(rango.get("hasta", ""), contexto=contexto)
            if desde >= hasta:
                raise CalendarioInvalido(
                    f"{contexto}: el rango {rango['desde']}–{rango['hasta']} no avanza"
                )
            for minuto in range(desde, hasta):
                if tabla[minuto] is not None:
                    raise CalendarioInvalido(
                        f"{contexto} se superpone con '{tabla[minuto]}' "
                        f"en {minuto // 60:02d}:{minuto % 60:02d}. "
                        "Una superposición facturaría esa energía dos veces"
                    )
                tabla[minuto] = nombre

    # Regla 1: cobertura exacta. Un hueco dejaría energía sin franja a la que atribuirse.
    if None in tabla:
        primero = tabla.index(None)
        ultimo = primero
        while ultimo + 1 < MINUTOS_DIA and tabla[ultimo + 1] is None:
            ultimo += 1
        raise CalendarioInvalido(
            f"el día no queda cubierto: falta desde {primero // 60:02d}:{primero % 60:02d} "
            f"hasta {(ultimo + 1) // 60:02d}:{(ultimo + 1) % 60:02d}. "
            "Todo minuto tiene que pertenecer a alguna franja"
        )

    return CalendarioTarifario(
        zona=zona,
        separacion_maxima_minutos=int(crudo.get("separacion_maxima_minutos", 45)),
        cadencia_objetivo_minutos=int(crudo.get("cadencia_objetivo_minutos", 15)),
        _por_minuto=tuple(tabla),  # type: ignore[arg-type]
    )


def fecha_y_franja(instante: datetime, cal: CalendarioTarifario) -> tuple[date, str]:
    """Devuelve `(fecha_local, nombre_de_franja)` para un instante.

    **Las dos cosas juntas, a propósito.** La clave de agregación necesita la fecha local y
    la franja, y las dos salen de la misma conversión de huso. Separarlas en dos funciones
    abre la puerta a convertir dos veces con reglas distintas — que es exactamente el error
    que hace que los consumos cercanos a medianoche caigan en el día equivocado.

    `instante` debe ser timezone-aware. Un naive se interpretaría en la zona del proceso, y
    entonces la franja atribuida dependería de en qué máquina corre el pipeline. Los datos
    con hora inválida ya fueron filtrados aguas arriba, así que acá es un error de
    programación y corresponde levantar excepción, no adivinar.
    """
    if instante.tzinfo is None:
        raise ValueError(
            "el instante debe ser timezone-aware: sin huso no hay franja que valga"
        )

    local = instante.astimezone(cal.zona)
    minuto = local.hour * 60 + local.minute
    return local.date(), cal.franja_de_minuto(minuto)


def borde_siguiente(momento: time, cal: CalendarioTarifario) -> int:
    """Minuto del día en que termina la franja que contiene a `momento`.

    Lo necesita la agregación para saber contra qué borde medir la separación de las
    lecturas, que es lo que determina el error de atribución declarado.
    """
    minuto = momento.hour * 60 + momento.minute
    actual = cal.franja_de_minuto(minuto)
    for siguiente in range(minuto + 1, MINUTOS_DIA):
        if cal.franja_de_minuto(siguiente) != actual:
            return siguiente
    return MINUTOS_DIA


@dataclass(frozen=True)
class Parte:
    """El trozo de un intervalo que cae dentro de una sola franja."""

    fecha_local: date
    franja: str
    energia_kwh: float
    minutos: float
    interpolada: bool
    """`True` si este trozo salió de repartir un intervalo que cruzaba un borde, y no de una
    lectura que caiga justo ahí. Es la diferencia entre un dato medido y uno estimado, y el
    consumidor tiene derecho a distinguirlos."""


def repartir_por_franja(
    desde: datetime, hasta: datetime, energia_kwh: float, cal: CalendarioTarifario
) -> list[Parte]:
    """Reparte la energía de un intervalo entre las franjas que toca.

    **Por qué esto es obligatorio y no una optimización** (decisión 5): este parque solo
    expone modo *readout*. No hay grilla fija de medición, hay rondas continuas sobre un bus
    compartido, así que los intervalos cruzan bordes de franja **siempre** y no hay
    configuración del calendario que lo evite.

    **El supuesto que hace:** potencia constante durante el intervalo, así que la energía se
    reparte en proporción al tiempo. Es la única hipótesis defendible sin más información —el
    contador no dice qué pasó *dentro* del intervalo— y es también la fuente del error de
    atribución que cada resultado declara. Cuanto más largo el intervalo, más se puede
    equivocar.

    Los trozos salen marcados con `interpolada`, que es `False` solo cuando el intervalo entra
    entero en una franja. Un intervalo que no cruza ningún borde no se estima: se mide.
    """
    # El huso se verifica primero: comparar un naive con un aware no da False, levanta
    # TypeError, y el mensaje de error resultante no diría cuál es el problema real.
    if desde.tzinfo is None or hasta.tzinfo is None:
        raise ValueError("los bordes del intervalo deben ser timezone-aware")
    if hasta <= desde:
        raise ValueError(f"intervalo vacío o invertido: {desde} → {hasta}")

    total_segundos = (hasta - desde).total_seconds()
    partes: list[Parte] = []
    cursor = desde

    while cursor < hasta:
        local = cursor.astimezone(cal.zona)
        fecha, franja = fecha_y_franja(cursor, cal)
        medianoche = local.replace(hour=0, minute=0, second=0, microsecond=0)
        fin_de_franja = medianoche + timedelta(minutes=borde_siguiente(local.time(), cal))
        fin = min(fin_de_franja, hasta)

        segundos = (fin - cursor).total_seconds()
        partes.append(
            Parte(
                fecha_local=fecha,
                franja=franja,
                energia_kwh=energia_kwh * segundos / total_segundos,
                minutos=segundos / 60,
                interpolada=segundos < total_segundos,
            )
        )
        cursor = fin

    return partes
