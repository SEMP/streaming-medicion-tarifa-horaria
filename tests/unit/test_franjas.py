"""Pruebas del calendario tarifario.

Todo acá es puro: no hace falta Kafka, ni Beam, ni Docker. Es lo que permite trabajar en la
atribución de franja sin esperar a que nada esté levantado.
"""

from __future__ import annotations

import textwrap
from datetime import UTC, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from pipeline.franjas import (
    CalendarioInvalido,
    borde_siguiente,
    cargar_calendario,
    fecha_y_franja,
    repartir_por_franja,
)

EJEMPLO = Path(__file__).parents[2] / "config" / "franjas.example.toml"


def escribir(tmp_path: Path, contenido: str) -> Path:
    ruta = tmp_path / "franjas.toml"
    ruta.write_text(textwrap.dedent(contenido), encoding="utf-8")
    return ruta


BASE = """
    zona_horaria = "America/Asuncion"
    separacion_maxima_minutos = 45

    [[franjas]]
    nombre = "valle"
    rangos = [{ desde = "00:00", hasta = "07:00" }]

    [[franjas]]
    nombre = "resto"
    rangos = [
      { desde = "07:00", hasta = "18:00" },
      { desde = "22:00", hasta = "24:00" },
    ]

    [[franjas]]
    nombre = "punta"
    rangos = [{ desde = "18:00", hasta = "22:00" }]
"""


# --------------------------------------------------------------------------- carga


def test_el_ejemplo_versionado_carga():
    """Si el ejemplo del repositorio no carga, cualquiera que lo copie arranca roto."""
    cal = cargar_calendario(EJEMPLO)
    assert set(cal.nombres) == {"valle", "resto", "punta"}
    assert str(cal.zona) == "America/Asuncion"


def test_las_franjas_cubren_el_dia_entero():
    cal = cargar_calendario(EJEMPLO)
    total = sum(cal.duracion_minutos(n) for n in cal.nombres)
    assert total == 24 * 60


def test_la_duracion_de_punta_es_la_esperada():
    """Es el denominador del error de atribución que declara cada resultado."""
    assert cargar_calendario(EJEMPLO).duracion_minutos("punta") == 4 * 60


# --------------------------------------------------------------------------- validación


def test_un_hueco_se_rechaza_diciendo_cual(tmp_path):
    """Un hueco dejaría energía sin franja a la que atribuirse."""
    ruta = escribir(tmp_path, BASE.replace('{ desde = "07:00", hasta = "18:00" }',
                                           '{ desde = "08:00", hasta = "18:00" }'))
    with pytest.raises(CalendarioInvalido) as error:
        cargar_calendario(ruta)
    assert "07:00" in str(error.value) and "08:00" in str(error.value)


def test_una_superposicion_se_rechaza_diciendo_donde(tmp_path):
    """Una superposición facturaría la misma energía dos veces."""
    ruta = escribir(tmp_path, BASE.replace('{ desde = "18:00", hasta = "22:00" }',
                                           '{ desde = "17:00", hasta = "22:00" }'))
    with pytest.raises(CalendarioInvalido) as error:
        cargar_calendario(ruta)
    assert "17:00" in str(error.value)


def test_nombres_repetidos_se_rechazan(tmp_path):
    """Con un nombre repetido, la clave `medidor|fecha|franja` dejaría de identificar una
    celda sin ambigüedad."""
    ruta = escribir(tmp_path, BASE + """
    [[franjas]]
    nombre = "punta"
    rangos = []
    """)
    with pytest.raises(CalendarioInvalido, match="repetidos"):
        cargar_calendario(ruta)


def test_un_rango_que_no_avanza_se_rechaza(tmp_path):
    ruta = escribir(tmp_path, BASE.replace('hasta = "07:00"', 'hasta = "00:00"'))
    with pytest.raises(CalendarioInvalido, match="no avanza"):
        cargar_calendario(ruta)


def test_una_hora_mal_formada_se_rechaza(tmp_path):
    ruta = escribir(tmp_path, BASE.replace('desde = "18:00"', 'desde = "6pm"'))
    with pytest.raises(CalendarioInvalido, match="HH:MM"):
        cargar_calendario(ruta)


def test_una_zona_horaria_inventada_se_rechaza(tmp_path):
    ruta = escribir(tmp_path, BASE.replace("America/Asuncion", "America/Inventada"))
    with pytest.raises(CalendarioInvalido, match="IANA"):
        cargar_calendario(ruta)


def test_un_archivo_que_no_existe_se_rechaza_con_claridad(tmp_path):
    with pytest.raises(CalendarioInvalido, match="no existe"):
        cargar_calendario(tmp_path / "no-esta.toml")


# --------------------------------------------------------------------------- atribución


@pytest.mark.parametrize(
    ("hora_local", "esperada"),
    [
        ("03:00", "valle"),
        ("06:59", "valle"),
        ("07:00", "resto"),   # el borde pertenece a la franja que empieza
        ("17:59", "resto"),
        ("18:00", "punta"),
        ("21:59", "punta"),
        ("22:00", "resto"),
        ("23:59", "resto"),
        ("00:00", "valle"),
    ],
)
def test_atribucion_de_franja_en_los_bordes(hora_local, esperada):
    """Los bordes son `[desde, hasta)`: el minuto del borde pertenece a la franja que
    empieza, no a la que termina. Si no, habría un minuto en dos franjas."""
    cal = cargar_calendario(EJEMPLO)
    h, m = map(int, hora_local.split(":"))
    instante = datetime(2026, 9, 22, h, m, tzinfo=cal.zona)
    _, franja = fecha_y_franja(instante, cal)
    assert franja == esperada


def test_la_fecha_local_no_es_la_utc_cerca_de_medianoche():
    """El caso que revela si la conversión de huso está bien hecha.

    A las 02:00 UTC del día 23 en Asunción todavía son las 23:00 del día 22. Si el pipeline
    usara la fecha UTC, ese consumo se facturaría en el día equivocado — y encima en la
    franja equivocada, porque a esa hora rige `resto` y no `valle`.
    """
    cal = cargar_calendario(EJEMPLO)
    instante = datetime(2026, 9, 23, 2, 0, tzinfo=UTC)

    fecha, franja = fecha_y_franja(instante, cal)

    assert instante.date().day == 23, "el instante es del 23 en UTC"
    assert fecha.day == 22, "pero del 22 en hora local, que es lo que se factura"
    assert franja == "resto"


def test_un_instante_naive_es_un_error_y_no_una_suposicion():
    """Sin huso, la franja atribuida dependería de en qué máquina corre el pipeline."""
    cal = cargar_calendario(EJEMPLO)
    with pytest.raises(ValueError, match="timezone-aware"):
        fecha_y_franja(datetime(2026, 9, 22, 18, 0), cal)


def test_el_mismo_instante_da_lo_mismo_venga_de_donde_venga():
    """Dos representaciones del mismo instante tienen que atribuirse igual: lo que importa
    es el instante absoluto, no cómo está escrito."""
    cal = cargar_calendario(EJEMPLO)
    en_utc = datetime(2026, 9, 22, 21, 0, tzinfo=UTC)
    en_local = en_utc.astimezone(cal.zona)
    assert fecha_y_franja(en_utc, cal) == fecha_y_franja(en_local, cal)


# --------------------------------------------------------------------------- bordes


def test_borde_siguiente_encuentra_el_fin_de_la_franja():
    """Lo usa la agregación para saber contra qué borde medir la separación de las lecturas,
    que es lo que determina el error de atribución declarado."""
    cal = cargar_calendario(EJEMPLO)
    assert borde_siguiente(time(19, 30), cal) == 22 * 60   # punta termina a las 22:00
    assert borde_siguiente(time(3, 0), cal) == 7 * 60      # valle termina a las 07:00
    assert borde_siguiente(time(23, 0), cal) == 24 * 60    # el último tramo cierra el día


# ------------------------------------------------- reparto de un intervalo entre franjas

ASU = ZoneInfo("America/Asuncion")


def local(dia: str, hora: str) -> datetime:
    return datetime.fromisoformat(f"{dia}T{hora}").replace(tzinfo=ASU)


@pytest.fixture
def cal():
    return cargar_calendario(EJEMPLO)


def test_un_intervalo_que_no_cruza_ningun_borde_no_se_interpola(cal):
    """Si entra entero en una franja, el dato es **medido**, no estimado."""
    partes = repartir_por_franja(
        local("2026-09-25", "17:40"), local("2026-09-25", "17:55"), 1.5, cal
    )
    assert len(partes) == 1
    assert (partes[0].franja, partes[0].energia_kwh) == ("resto", 1.5)
    assert partes[0].interpolada is False


def test_un_intervalo_que_cruza_el_borde_de_punta_se_reparte_en_proporcion(cal):
    """**El caso que vuelve obligatoria la interpolación** (decisión 5).

    Con readout no hay grilla de medición, así que los intervalos cruzan bordes siempre. Las
    17:55 → 18:20 son 25 minutos, de los cuales 5 caen en `resto` y 20 en `punta`. Con
    potencia constante, la energía sigue esa misma proporción.
    """
    partes = repartir_por_franja(
        local("2026-09-25", "17:55"), local("2026-09-25", "18:20"), 4.0, cal
    )
    assert [(p.franja, p.minutos, round(p.energia_kwh, 3)) for p in partes] == [
        ("resto", 5.0, 0.8),
        ("punta", 20.0, 3.2),
    ]
    assert all(p.interpolada for p in partes)


def test_repartir_conserva_la_energia(cal):
    """**La propiedad que no se puede romper.** Repartir no crea ni destruye energía: es la
    misma medición vista por partes. Si esto falla, se está facturando de más o de menos."""
    for desde, hasta, energia in [
        ("00:00", "23:59", 48.0),      # casi el día entero, cruza los cuatro tramos
        ("17:55", "18:20", 4.0),       # el borde de punta
        ("21:50", "22:10", 2.0),       # el borde de vuelta a resto
        ("06:58", "07:03", 0.4),       # el borde de valle
    ]:
        partes = repartir_por_franja(
            local("2026-09-25", desde), local("2026-09-25", hasta), energia, cal
        )
        assert sum(p.energia_kwh for p in partes) == pytest.approx(energia)
        assert sum(p.minutos for p in partes) == pytest.approx(
            (local("2026-09-25", hasta) - local("2026-09-25", desde)).total_seconds() / 60
        )


def test_un_intervalo_que_cruza_la_medianoche_cae_en_dos_fechas(cal):
    """La fecha local va en la clave de salida: un intervalo a caballo de medianoche aporta a
    **dos celdas distintas**, y confundirlas mete consumo en el día equivocado."""
    partes = repartir_por_franja(
        local("2026-09-25", "23:50"), local("2026-09-26", "00:10"), 2.0, cal
    )
    assert [(str(p.fecha_local), p.franja, p.energia_kwh) for p in partes] == [
        ("2026-09-25", "resto", 1.0),
        ("2026-09-26", "valle", 1.0),
    ]


def test_un_intervalo_invertido_o_vacio_es_un_error(cal):
    """No se devuelve una lista vacía: sería un consumo que desaparece en silencio."""
    with pytest.raises(ValueError, match="invertido"):
        repartir_por_franja(
            local("2026-09-25", "18:00"), local("2026-09-25", "17:00"), 1.0, cal
        )


def test_un_intervalo_sin_huso_es_un_error(cal):
    """Mismo criterio que `fecha_y_franja`: sin huso, la franja dependería de la máquina."""
    with pytest.raises(ValueError, match="timezone-aware"):
        repartir_por_franja(
            datetime(2026, 9, 25, 17, 55), local("2026-09-25", "18:20"), 4.0, cal
        )
