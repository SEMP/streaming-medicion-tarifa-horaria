"""Pruebas del inyector del tablero. Sin Kafka: todo lo que se prueba acá es puro.

Lo que justifica estas pruebas es el botón del duplicado. Su premisa —que republicar la misma
`(medidor_id, instante_lectura)` produce el mismo `event_id`— no se ve en la interfaz: si se
rompiera, el botón publicaría un evento nuevo, el deduplicador lo dejaría pasar y el total se
movería. Eso es exactamente lo contrario de lo que la demostración afirma.
"""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from inyector import (
    ACCIONES,
    CONTADOR_INICIAL,
    POR_CLAVE,
    UMBRAL_INDETERMINADO_MINUTOS,
    construir,
    duplicado,
    hueco_largo,
    normal,
    reseteo,
    tardia_en_el_borde,
    truncada,
)

AHORA = datetime(2026, 9, 30, 21, 0, tzinfo=ZoneInfo("America/Asuncion"))


def _valor(lectura):
    return lectura.registros[0].valor


def _event_id(lectura):
    return lectura.a_dict()["event_id"]


# ---------------------------------------------------------------- la premisa del duplicado


def test_el_event_id_es_determinista():
    """Dos lecturas con el mismo medidor e instante dan el mismo id, aunque sean objetos
    distintos. Es lo que hace reconocible al duplicado."""
    instante = datetime(2026, 9, 30, 17, 40, tzinfo=ZoneInfo("America/Asuncion"))
    una = construir("MED-X", "CAB-X", instante, 100.0)
    otra = construir("MED-X", "CAB-X", instante, 100.0, secuencia=99)
    assert _event_id(una) == _event_id(otra)


def test_el_event_id_cambia_con_el_instante():
    instante = datetime(2026, 9, 30, 17, 40, tzinfo=ZoneInfo("America/Asuncion"))
    una = construir("MED-X", "CAB-X", instante, 100.0)
    otra = construir("MED-X", "CAB-X", instante + timedelta(minutes=1), 100.0)
    assert _event_id(una) != _event_id(otra)


def test_el_duplicado_conserva_el_event_id():
    """El botón 2: lo que publica tiene que ser indistinguible de lo anterior."""
    primera = normal(None, ahora=AHORA)
    repetida = duplicado(primera, ahora=AHORA)
    assert _event_id(repetida) == _event_id(primera)
    assert repetida.a_dict()["registros"] == primera.a_dict()["registros"]


# ------------------------------------------------------------------------ las otras cinco


def test_la_normal_avanza_el_contador_y_el_reloj():
    primera = normal(None, ahora=AHORA)
    segunda = normal(primera, ahora=AHORA)
    assert segunda.instante_lectura > primera.instante_lectura
    assert _valor(segunda) > _valor(primera)


def test_la_secuencia_de_normales_cruza_el_borde_de_punta():
    """Sin ese cruce, la tardía sobre el borde no tendría nada que corregir."""
    lecturas = []
    ultima = None
    for _ in range(3):
        ultima = normal(ultima, ahora=AHORA)
        lecturas.append(ultima)
    borde = AHORA.replace(hour=18, minute=0, second=0, microsecond=0)
    assert lecturas[1].instante_lectura < borde < lecturas[2].instante_lectura


def test_la_tardia_cae_sobre_el_borde_y_es_anterior_a_la_ultima():
    ultima = None
    for _ in range(3):
        ultima = normal(ultima, ahora=AHORA)
    atrasada = tardia_en_el_borde(ultima, ahora=AHORA)
    assert atrasada.instante_lectura.hour == 18
    assert atrasada.instante_lectura.minute == 0
    assert atrasada.instante_lectura < ultima.instante_lectura


def test_el_reseteo_retrocede_el_contador():
    previa = normal(None, ahora=AHORA)
    rota = reseteo(previa, ahora=AHORA)
    assert _valor(rota) < _valor(previa)


def test_la_truncada_se_marca_y_no_crece():
    previa = normal(None, ahora=AHORA)
    rota = truncada(previa, ahora=AHORA)
    assert rota.calidad == "truncada"
    assert _valor(rota) != _valor(normal(previa, ahora=AHORA))


def test_el_hueco_supera_el_umbral_de_indeterminado():
    previa = normal(None, ahora=AHORA)
    lejana = hueco_largo(previa, ahora=AHORA)
    minutos = (lejana.instante_lectura - previa.instante_lectura).total_seconds() / 60
    assert minutos > UMBRAL_INDETERMINADO_MINUTOS


# ------------------------------------------------------------------------------ el conjunto


def test_hay_seis_acciones_con_clave_unica():
    assert len(ACCIONES) == 6
    assert len(POR_CLAVE) == 6


@pytest.mark.parametrize("accion", ACCIONES, ids=lambda a: a.clave)
def test_toda_accion_funciona_sin_lectura_previa(accion):
    """El tablero se abre en cualquier orden: ningún botón puede romperse si es el primero."""
    lectura = accion.construir(None, ahora=AHORA)
    assert lectura.medidor_id
    assert lectura.registros
    assert lectura.a_dict()["event_id"]


@pytest.mark.parametrize("accion", ACCIONES, ids=lambda a: a.clave)
def test_toda_accion_produce_un_evento_del_contrato(accion):
    previa = normal(None, ahora=AHORA)
    d = accion.construir(previa, ahora=AHORA).a_dict()
    for campo in ("schema_version", "event_id", "medidor_id", "cabina_id",
                  "instante_lectura", "registros", "calidad", "publicado_at"):
        assert campo in d, campo
    assert d["registros"][0]["obis"] == "15.8.0"


def test_el_contador_inicial_es_el_declarado():
    assert _valor(normal(None, ahora=AHORA)) == CONTADOR_INICIAL


def test_la_tardia_no_cae_sobre_la_recta_de_la_interpolacion():
    """Si cayera, el botón 3 no demostraría nada.

    El consumo simulado es lineal, así que interpolar el intervalo que cruza las 18:00
    acertaría exacto y la lectura tardía no corregiría ningún reparto. El desvío es lo que
    hace visible que la interpolación **supone potencia constante** y que en el borde de
    `punta` eso no se cumple.
    """
    previas = []
    ultima = None
    for _ in range(3):
        ultima = normal(ultima, ahora=AHORA)
        previas.append(ultima)

    antes, despues = previas[1], previas[2]
    atrasada = tardia_en_el_borde(ultima, ahora=AHORA)

    total = (atrasada.instante_lectura - antes.instante_lectura).total_seconds()
    tramo = (despues.instante_lectura - antes.instante_lectura).total_seconds()
    interpolado = _valor(antes) + (_valor(despues) - _valor(antes)) * total / tramo

    assert _valor(atrasada) < interpolado, "la medición tiene que corregir hacia abajo"
    assert abs(interpolado - _valor(atrasada)) == pytest.approx(0.2, abs=0.01)
