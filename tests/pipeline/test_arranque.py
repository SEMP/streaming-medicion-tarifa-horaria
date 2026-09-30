"""El arranque del pipeline, que es el punto de entrada del perfil `demo`.

**Existe por un defecto que se escapó a las 89 pruebas.** `__main__.main()` llamaba
`calendario.nombres()` con paréntesis sobre una `@property`, y reventaba con
`TypeError: 'tuple' object is not callable` **antes** de construir el pipeline. El resultado
era que el perfil `demo` nunca escribía nada al tópico de salida.

No lo detectó nadie porque ninguna prueba ejecutaba este módulo: el recorrido de punta a punta
entra por `extremo_a_extremo`, que es otro punto de entrada. Lo encontró una persona externa
al equipo siguiendo el README, que es exactamente para lo que el enunciado pide esa prueba.
"""

from __future__ import annotations

import pytest
from pipeline.__main__ import main


def test_el_arranque_registra_las_franjas_y_construye_el_pipeline(monkeypatch, caplog):
    """Corre `main()` de verdad, con Beam anulado para no necesitar el stack.

    Lo que importa es que la función **llegue entera** hasta construir el pipeline: el defecto
    original moría en la línea de log, tres líneas antes.
    """
    construidos = []

    class PipelineFalso:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

    monkeypatch.setattr("pipeline.__main__.beam.Pipeline", lambda **_: PipelineFalso())
    monkeypatch.setattr(
        "pipeline.__main__.construir", lambda *a, **k: construidos.append(True)
    )
    monkeypatch.setenv("CONFIG_FRANJAS", "config/franjas.example.toml")

    with caplog.at_level("INFO"):
        assert main() == 0

    assert construidos, "main() no llegó a construir el pipeline"
    registrado = " ".join(r.getMessage() for r in caplog.records)
    for franja in ("valle", "resto", "punta"):
        assert franja in registrado


def test_un_calendario_ilegible_no_levanta_el_pipeline(monkeypatch):
    """Un calendario mal formado tiene que romper el arranque con código 2, no aparecer como
    un resultado raro tres horas después."""
    monkeypatch.setenv("CONFIG_FRANJAS", "no-existe.toml")
    assert main() == 2


def test_nombres_es_una_property_y_no_un_metodo():
    """Fija la forma que el defecto confundió, para que el error no pueda volver."""
    from pipeline.franjas import cargar_calendario

    cal = cargar_calendario("config/franjas.example.toml")
    assert isinstance(cal.nombres, tuple)
    assert isinstance(type(cal).nombres, property)
    with pytest.raises(TypeError):
        cal.nombres()
