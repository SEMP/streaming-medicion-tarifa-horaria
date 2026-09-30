"""Dibuja el pipeline: el mismo grafo que ejecuta Flink, como imagen.

    uv run python -m pipeline.grafico

Deja `docs/diagramas/pipeline-dag.svg`. No procesa ni un dato: arma el pipeline igual que
[`__main__`] y lo entrega al `RenderRunner` de Beam, que lo recorre y lo dibuja. Que salga
del mismo `construir` + `cadena` que corre en producción es lo que hace que el dibujo no
pueda quedar desactualizado respecto del código — a diferencia de
[`docs/diagramas/arquitectura.svg`], que se mantiene a mano y muestra otra cosa: los
componentes y los tópicos, no los pasos de adentro.

Necesita Graphviz (`dot`) en el PATH. En Ubuntu: `sudo apt install graphviz`.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from .cadena import cadena
from .config import Ajustes
from .esqueleto import construir
from .franjas import cargar_calendario

SALIDA = Path("docs/diagramas/pipeline-dag.svg")

log = logging.getLogger("grafico")


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    destino = Path(argv[0]) if argv else SALIDA
    destino.parent.mkdir(parents=True, exist_ok=True)

    ajustes = Ajustes.desde_entorno()
    calendario = cargar_calendario(ajustes.config_franjas)

    # El `RenderRunner` no ejecuta: recorre el proto del pipeline y lo dibuja. Igual hay que
    # resolver `KafkaIO`, que es una transformación cross-language, así que esto levanta el
    # servicio de expansión de Java un momento — por eso tarda más de lo que un dibujo
    # sugeriría.
    opciones = PipelineOptions(
        [
            "--runner=apache_beam.runners.render.RenderRunner",
            f"--render_output={destino}",
            "--streaming",
            # `KafkaIO` es una transformación cross-language y por dentro trae una docena
            # de pasos del SDK de Java que no dicen nada sobre este pipeline. Se dibujan
            # como una caja: lo que importa es qué entra y qué sale de Kafka, no cómo lo
            # hace KafkaIO por dentro.
            "--render_leaf_composite_nodes=LeerLecturas,Escribir.*",
        ]
    )
    with beam.Pipeline(options=opciones) as pipeline:
        construir(
            pipeline,
            ajustes,
            grupo="g-grafico",
            transformaciones=cadena(ajustes, calendario),
        )

    if not destino.exists():
        log.error("el runner no dejó %s", destino)
        return 1

    if destino.suffix == ".svg":
        # Beam etiqueta cada arista con el tag de la salida, y la salida principal no tiene
        # tag: escribe "None". Las de cuarentena sí llevan su nombre y valen la pena, así
        # que se borra solo el "None", que no informa nada y ensucia el dibujo.
        texto = destino.read_text(encoding="utf-8")
        destino.write_text(texto.replace(">None</text>", "></text>"), encoding="utf-8")
    log.info("grafo en %s · %d KB", destino, destino.stat().st_size // 1024)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
