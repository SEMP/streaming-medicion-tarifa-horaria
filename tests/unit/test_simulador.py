"""Pruebas del simulador.

Lo que se verifica acá no es que "ande", sino las **propiedades de las que depende todo lo
demás**: que sea determinista (sin eso ninguna prueba del pipeline es reproducible), que el
contador se comporte como un contador, y que las fallas aparezcan solo cuando se piden.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from simulador.agenda import ConfigAgenda, generar
from simulador.consumo import ContadorMedidor, perfil_demanda_kw
from simulador.evento import calcular_event_id
from simulador.fallas import ConfigFallas, truncar_valor
from simulador.parque import generar_parque

TZ = ZoneInfo("America/Asuncion")
INICIO = datetime(2026, 9, 21, 0, 0, tzinfo=TZ)
SIN_FALLAS = ConfigFallas(0.0, (0, 0), 0.0, 0.0, equipos_perfectos=True)
"""Línea de base limpia: sin fallas de publicación **ni** del equipo."""


def _correr(*, cabinas=3, semilla=1, fallas=SIN_FALLAS, horas=24, tamano_fijo=None):
    fin = INICIO + timedelta(hours=horas)
    parque = generar_parque(
        cabinas=cabinas, inicio=INICIO, dias=1, semilla=semilla, tamano_fijo=tamano_fijo
    )
    cfg = ConfigAgenda(inicio=INICIO, fin=fin)
    return list(generar(parque, cfg, fallas, semilla=semilla))


# --------------------------------------------------------------------------- contador


def test_el_contador_nunca_decrece():
    """Es la propiedad que define a un contador acumulado, y de la que depende la regla de
    que un consumo negativo solo puede ser un reseteo."""
    c = ContadorMedidor.crear("M", INICIO, dias=1, valor_inicial_kwh=1000.0, escala=1.0)
    valores = [c.leer(INICIO + timedelta(minutes=m)) for m in range(0, 1440, 7)]
    assert all(b >= a for a, b in zip(valores, valores[1:]))


def test_el_contador_arranca_en_su_valor_inicial():
    c = ContadorMedidor.crear("M", INICIO, dias=1, valor_inicial_kwh=12_345.678, escala=1.0)
    assert c.leer(INICIO) == pytest.approx(12_345.678)


def test_leer_antes_del_inicio_es_un_error():
    c = ContadorMedidor.crear("M", INICIO, dias=1, valor_inicial_kwh=0.0, escala=1.0)
    with pytest.raises(ValueError):
        c.leer(INICIO - timedelta(minutes=1))


def test_el_inicio_debe_ser_timezone_aware():
    """Sin huso no se puede derivar la franja, y un naive se interpretaría en la zona del
    proceso — que es justo el error que vuelve irreproducible una simulación."""
    with pytest.raises(ValueError, match="timezone-aware"):
        ContadorMedidor.crear("M", datetime(2026, 9, 21), dias=1, valor_inicial_kwh=0.0, escala=1.0)


def test_la_demanda_en_punta_supera_holgadamente_a_la_de_madrugada():
    """Si la curva fuera plana, separar por franja no tendría sentido y la demostración no
    mostraría nada. Este test protege el valor didáctico del simulador."""
    punta = perfil_demanda_kw(20 * 60)
    madrugada = perfil_demanda_kw(3 * 60)
    assert punta > 3 * madrugada


def test_el_consumo_por_franja_es_marcadamente_distinto():
    c = ContadorMedidor.crear("M", INICIO, dias=1, valor_inicial_kwh=0.0, escala=1.0)
    valle = c.consumo_entre(INICIO, INICIO + timedelta(hours=7))
    punta = c.consumo_entre(INICIO + timedelta(hours=18), INICIO + timedelta(hours=22))
    assert punta / 4 > 3 * (valle / 7)  # kWh por hora


# --------------------------------------------------------------------------- identidad


def test_event_id_es_determinista():
    a = calcular_event_id("MED-0001-000", "2026-09-21T18:00:00-03:00")
    b = calcular_event_id("MED-0001-000", "2026-09-21T18:00:00-03:00")
    assert a == b and len(a) == 16


def test_event_id_distingue_medidor_e_instante():
    base = calcular_event_id("MED-0001-000", "2026-09-21T18:00:00-03:00")
    assert base != calcular_event_id("MED-0001-001", "2026-09-21T18:00:00-03:00")
    assert base != calcular_event_id("MED-0001-000", "2026-09-21T18:15:00-03:00")


# --------------------------------------------------------------------------- determinismo


def test_la_misma_semilla_produce_exactamente_lo_mismo():
    """Sin esto, una prueba del pipeline que falla no se puede repetir."""
    a = [e.a_json() for e in _correr(semilla=99)]
    b = [e.a_json() for e in _correr(semilla=99)]
    assert a == b and len(a) > 0


def test_semillas_distintas_producen_resultados_distintos():
    a = [e.a_json() for e in _correr(semilla=1)]
    b = [e.a_json() for e in _correr(semilla=2)]
    assert a != b


# --------------------------------------------------------------------------- fallas


def test_sin_fallas_no_hay_duplicados():
    ev = _correr(fallas=SIN_FALLAS)
    ids = [e.a_dict()["event_id"] for e in ev]
    assert len(ids) == len(set(ids))


def test_sin_fallas_los_contadores_no_retroceden():
    por_medidor: dict[str, list[tuple[datetime, float]]] = {}
    for e in _correr(fallas=SIN_FALLAS):
        por_medidor.setdefault(e.medidor_id, []).append(
            (e.instante_lectura, e.registros[0].valor)
        )
    for lecturas in por_medidor.values():
        lecturas.sort()
        assert all(b[1] >= a[1] for a, b in zip(lecturas, lecturas[1:]))


def test_con_fallas_aparecen_duplicados_y_retrocesos():
    """La contracara: el simulador tiene que ser capaz de romper las cosas a pedido."""
    ev = _correr(fallas=ConfigFallas(0.2, (30, 120), 0.15, 0.05), cabinas=4)
    ids = [e.a_dict()["event_id"] for e in ev]
    assert len(ids) > len(set(ids)), "se esperaban duplicados"

    retrocesos = 0
    por_medidor: dict[str, list[tuple[datetime, float]]] = {}
    for e in ev:
        por_medidor.setdefault(e.medidor_id, []).append(
            (e.instante_lectura, e.registros[0].valor)
        )
    for lecturas in por_medidor.values():
        lecturas.sort()
        retrocesos += sum(1 for a, b in zip(lecturas, lecturas[1:]) if b[1] < a[1])
    assert retrocesos > 0, "se esperaban reseteos o truncamientos"


def test_las_rafagas_tardias_publican_mucho_despues_de_la_lectura():
    ev = _correr(fallas=ConfigFallas(0.5, (60, 180), 0.0, 0.0), cabinas=4)
    atrasos = [(e.publicado_at - e.instante_lectura).total_seconds() / 60 for e in ev]
    assert max(atrasos) > 30


def test_truncar_valor_produce_un_numero_plausible_pero_distinto():
    """La falla más peligrosa: el valor truncado parece válido."""
    import random

    rng = random.Random(0)
    distintos = 0
    for _ in range(50):
        original = 14380.812
        truncado = truncar_valor(original, rng)
        assert isinstance(truncado, float)
        if truncado != original:
            distintos += 1
    assert distintos > 0


# --------------------------------------------------------------------------- estructura


def test_el_evento_serializa_a_json_con_los_campos_del_contrato():
    ev = _correr(cabinas=1, horas=2)
    assert ev, "la simulación no produjo lecturas"
    d = json.loads(ev[0].a_json())
    assert set(d) == {
        "schema_version", "event_id", "medidor_id", "cabina_id", "lote_id",
        "secuencia", "instante_lectura", "registros", "calidad", "publicado_at",
    }
    assert d["registros"][0]["obis"] == "15.8.0"
    assert d["registros"][0]["unidad"] == "kWh"
    assert "-03:00" in d["instante_lectura"], "el instante debe llevar offset explícito"


def test_los_codigos_obis_no_se_repiten_dentro_de_una_lectura():
    """Regla que la lista de registros no garantiza por estructura, al revés que un mapa."""
    for e in _correr(cabinas=2, horas=3):
        codigos = [r.obis for r in e.registros]
        assert len(codigos) == len(set(codigos))


def test_las_lecturas_salen_ordenadas_por_publicacion():
    ev = _correr(cabinas=4)
    publicaciones = [e.publicado_at for e in ev]
    assert publicaciones == sorted(publicaciones)


def test_el_orden_de_publicacion_no_es_el_de_lectura():
    """El desorden que el pipeline tiene que tolerar: las cabinas avanzan en paralelo."""
    ev = _correr(cabinas=4)
    lecturas = [e.instante_lectura for e in ev]
    assert lecturas != sorted(lecturas)


# --------------------------------------------------------------------------- escenario B


def test_el_escenario_b_no_tiene_contencion_de_bus():
    """Un dispositivo por medidor: cabinas de tamaño uno."""
    parque = generar_parque(cabinas=5, inicio=INICIO, dias=1, semilla=3, tamano_fijo=1)
    assert all(len(c) == 1 for c in parque.cabinas)


def test_el_escenario_b_lee_mucho_mas_seguido_al_mismo_medidor():
    """Sin bus compartido, el mismo medidor se consulta con mucha más frecuencia — que es
    justamente lo que reduce el error de atribución por franja."""
    a = _correr(cabinas=2, horas=6)
    b = _correr(cabinas=2, horas=6, tamano_fijo=1)

    def lecturas_por_medidor(ev):
        cuenta: dict[str, int] = {}
        for e in ev:
            cuenta[e.medidor_id] = cuenta.get(e.medidor_id, 0) + 1
        return max(cuenta.values())

    assert lecturas_por_medidor(b) > lecturas_por_medidor(a)
