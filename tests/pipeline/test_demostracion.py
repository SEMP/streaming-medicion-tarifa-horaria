"""La demostración es un entregable, no un script suelto: se prueba como tal.

Si se rompe, el video que la muestra deja de ser reproducible — y el enunciado pide
justamente que el recorrido se pueda repetir.
"""

from __future__ import annotations

from pipeline.demostracion import BASE, CALENDARIO, cargar_calendario, correr, main


def test_la_demostracion_termina_bien(capsys):
    """Sus propias verificaciones tienen que pasar: devuelve 0 o falla."""
    assert main() == 0
    salida = capsys.readouterr().out
    assert "Los tres escenarios se comportaron como está documentado" in salida


def test_el_duplicado_no_mueve_las_celdas():
    """El acto 2, aislado: agregar el duplicado deja la tabla idéntica."""
    cal = cargar_calendario(CALENDARIO)
    sin = dict(tuple(x) for x in _celdas(correr(BASE, cal)))
    con = dict(tuple(x) for x in _celdas(correr((*BASE, ("17:55", 101.5)), cal)))
    assert sin == con


def test_la_tardia_corrige_sin_cambiar_el_total():
    """El acto 3: cambia el reparto entre franjas, no la energía total."""
    cal = cargar_calendario(CALENDARIO)
    antes = dict(tuple(x) for x in _celdas(correr(BASE, cal)))
    despues = dict(
        tuple(x) for x in _celdas(correr((*BASE, ("18:00", 102.4)), cal))
    )

    assert sum(antes.values()) == sum(despues.values()) == 5.5
    assert antes != despues
    assert round(despues["MED-0042|2026-09-25|punta"], 3) == 3.1
    assert round(despues["MED-0042|2026-09-25|resto"], 3) == 2.4


def _celdas(salida: dict) -> list:
    return [(clave, round(kwh, 3)) for clave, (kwh, _) in salida["celdas"]]
