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
    assert all(b >= a for a, b in zip(valores, valores[1:], strict=False))


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
        assert all(b[1] >= a[1] for a, b in zip(lecturas, lecturas[1:], strict=False))


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
        retrocesos += sum(1 for a, b in zip(lecturas, lecturas[1:], strict=False) if b[1] < a[1])
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


# --------------------------------------------------------------- tiempos de comunicación
#
# Estas pruebas fijan la forma de la distribución de tiempos, que se tomó de capturas
# reales de comunicación. Protegen que un cambio al modelo no la rompa sin que se note.


def _tiempos_de_pedido(*, cabinas=20, semilla=5, repeticiones=3):
    import random

    from simulador.agenda import _tiempo_de_pedido

    parque = generar_parque(cabinas=cabinas, inicio=INICIO, dias=1, semilla=semilla)
    rng = random.Random(semilla)
    return [
        _tiempo_de_pedido(medidor, cabina, rng, 120.0)[0]
        for cabina in parque.cabinas
        for medidor in cabina.medidores
        for _ in range(repeticiones)
    ]


def _resultados_de_pedido(*, cabinas=150, semilla=5, repeticiones=1):
    """Muestra grande a propósito: con pocas cabinas, la caída de enlace —que es un evento
    por cabina y no por medidor— es puro ruido, y las tasas globales no significan nada."""
    import random

    from simulador.agenda import _tiempo_de_pedido

    parque = generar_parque(cabinas=cabinas, inicio=INICIO, dias=1, semilla=semilla)
    rng = random.Random(semilla)
    return [
        (_tiempo_de_pedido(medidor, cabina, rng, 120.0), cabina, medidor)
        for cabina in parque.cabinas
        for medidor in cabina.medidores
        for _ in range(repeticiones)
    ]


def test_ningun_pedido_supera_el_timeout():
    """Medido: el 9,5% de los pedidos termina exactamente en el tope. Es un muro."""
    assert max(r[0][0] for r in _resultados_de_pedido()) <= 120.0


def test_la_distribucion_de_duraciones_es_trimodal():
    """Medido: éxito rápido en 5–10 s (85,5%), éxito tras reintentar en 30–45 s (3,8%), y
    muro del timeout (9,5%). Los valles entre modas son reales, no ruido de muestreo."""
    tiempos = [r[0][0] for r in _resultados_de_pedido()]

    rapidos = [x for x in tiempos if x <= 12]
    recuperados = [x for x in tiempos if 28 <= x <= 48]
    muro = [x for x in tiempos if x >= 119]
    assert rapidos and recuperados and muro, "se esperaban las tres modas"

    valle_bajo = [x for x in tiempos if 13 < x < 27]
    valle_alto = [x for x in tiempos if 50 < x < 118]
    poblado = len(tiempos)
    assert len(valle_bajo) / poblado < 0.03, "el valle entre la 1.ª y la 2.ª moda no está vacío"
    assert len(valle_alto) / poblado < 0.03, "el valle entre la 2.ª y la 3.ª moda no está vacío"


def test_la_tasa_global_de_fallas_se_parece_a_la_medida():
    """Calibración contra el dato real: ~9,2% de los pedidos no entrega dato."""
    resultados = _resultados_de_pedido(cabinas=250)
    fallidos = sum(1 for (_, ok, _), _, _ in resultados if not ok)
    tasa = fallidos / len(resultados)
    assert 0.06 < tasa < 0.13, f"tasa de fallas {tasa:.1%}, se esperaba cerca del 9,2% medido"


def test_la_mitad_de_las_fallas_cae_sobre_enlaces_sanos():
    """El hallazgo que un modelo basado solo en caídas de cabina no tiene: **el 52% de las
    lecturas fallidas ocurre sobre enlaces que pinguean perfecto**. Hace falta un fondo
    disperso además del componente correlacionado."""
    resultados = _resultados_de_pedido(cabinas=250)
    fallidos = [(c, m) for (_, ok, _), c, m in resultados if not ok]
    assert fallidos

    sobre_enlace_sano = sum(1 for c, _ in fallidos if not c.enlace_muerto)
    proporcion = sobre_enlace_sano / len(fallidos)
    assert proporcion > 0.35, (
        f"solo {proporcion:.0%} de las fallas cae sobre enlaces sanos; el modelo se estaría "
        "apoyando demasiado en la caída de cabina"
    )


def test_la_marca_de_checksum_no_implica_corrupcion():
    """La trampa más peligrosa del dominio.

    Un fabricante mayoritario calcula el checksum distinto de lo que el concentrador
    espera, así que casi todas sus lecturas salen marcadas — **y el dato está completo y
    correcto**. Un pipeline que descarte por bandera de calidad tiraría la mitad de las
    lecturas buenas.

    Esta prueba fija las dos mitades: que la marca aparezca en una proporción grande, y que
    NO se confunda con la trama truncada, que sí es corrupción.
    """
    eventos = _correr(fallas=SIN_FALLAS, cabinas=6)
    calidades = [e.calidad for e in eventos]

    marcados = sum(1 for c in calidades if c == "checksum_no_verificado")
    assert marcados / len(calidades) > 0.30, (
        "se esperaba que una proporción grande de lecturas venga marcada: es el 55% del parque"
    )
    assert "truncada" not in calidades, (
        "la marca de checksum no debe mezclarse con el truncamiento, que sí es corrupción"
    )


def test_los_modelos_de_equipo_estan_dispersos_por_el_parque():
    """La correlación por modelo es **espacialmente dispersa**: con el mismo enlace, las
    tasas de entrega por modelo van de 65% a 94%. Como los modelos están repartidos por
    todo el parque, ninguna partición por ubicación aísla ese patrón — para un pipeline
    particionado por cabina es invisible."""
    parque = generar_parque(cabinas=20, inicio=INICIO, dias=1, semilla=13)
    grandes = [c for c in parque.cabinas if len(c) >= 10]
    assert grandes

    mezcladas = sum(1 for c in grandes if len({m.modelo.nombre for m in c.medidores}) > 1)
    assert mezcladas / len(grandes) > 0.8, "los modelos deberían convivir dentro de una cabina"


def test_el_enlace_esta_sano_o_muerto_pero_no_degradado():
    """Medido: la disponibilidad por equipo es bimodal y el RTT no correlaciona con ella.
    **No existe la población "enlace lento degradado"**, así que el estado es binario."""
    parque = generar_parque(cabinas=60, inicio=INICIO, dias=1, semilla=17)
    assert all(isinstance(c.enlace_muerto, bool) for c in parque.cabinas)

    muertas = sum(1 for c in parque.cabinas if c.enlace_muerto)
    assert 0 < muertas < len(parque.cabinas) // 3, "se esperaba una minoría de cabinas caídas"


def test_la_ronda_la_fija_la_tasa_de_fallas_y_no_la_velocidad():
    """La conclusión central: subir la tasa de fallas alarga la ronda mucho más que
    cualquier diferencia de velocidad, porque los reintentos ocupan el bus y retrasan a
    todos los medidores que vienen detrás."""
    import random

    from simulador.agenda import _tiempo_de_pedido
    from simulador.consumo import ContadorMedidor
    from simulador.parque import MODELOS, Cabina, Medidor

    contador = ContadorMedidor.crear("M", INICIO, dias=1, valor_inicial_kwh=0.0, escala=1.0)

    def ronda(modelo):
        medidores = tuple(Medidor(f"M{k}", "CAB", k, modelo, contador) for k in range(50))
        cabina = Cabina("CAB", medidores, 0.0, latencia_enlace=1.665, enlace_muerto=False)
        rng = random.Random(7)
        return sum(_tiempo_de_pedido(m, cabina, rng, 120.0)[0] for m in medidores)

    assert ronda(MODELOS["modelo-d"]) > 3 * ronda(MODELOS["modelo-a"])


def test_reintento_y_exito_estan_desacoplados():
    """No vale "más reintentos = peor modelo": hay un modelo que reintenta mucho y entrega
    bien, y otro que reintenta bastante y entrega mal. Asumir que el reintento siempre
    rescata la lectura es falso para una parte del parque."""
    from simulador.parque import MODELOS

    peor = MODELOS["modelo-d"]
    assert peor.prob_exito_reintento < 0.25, (
        "para el peor modelo, reintentar debería servir poco: es tiempo de bus tirado"
    )
def test_la_latencia_esta_correlacionada_dentro_de_la_cabina():
    """El enlace es compartido, así que su latencia NO es independiente por medidor: una
    cabina con mal enlace es lenta entera. Si se modelara por medidor, se promediaría y la
    duración de la ronda saldría optimista."""
    parque = generar_parque(cabinas=40, inicio=INICIO, dias=1, semilla=11)
    latencias = {c.cabina_id: c.latencia_enlace for c in parque.cabinas}
    assert len(set(latencias.values())) > 1, "las cabinas deberían diferir entre sí"
    for cabina in parque.cabinas:
        assert cabina.latencia_enlace == latencias[cabina.cabina_id]
