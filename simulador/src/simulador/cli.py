"""Punto de entrada del simulador.

    uv run python -m simulador --help
    uv run python -m simulador --cabinas 20 --dias 1 --salida datos/lecturas.jsonl
    uv run python -m simulador --escenario B --salida datos/lecturas-dispositivo.jsonl

Publica a un archivo JSONL. La escritura a Kafka se agrega cuando exista la
infraestructura; que funcione sin ella es deliberado, para no bloquear a nadie.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from .agenda import ConfigAgenda, generar
from .fallas import ConfigFallas
from .parque import generar_parque

ZONA = "America/Asuncion"


def construir_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="simulador",
        description="Simula la lectura remota de medidores agrupados en cabinas "
                    "sobre un bus RS-485.",
    )
    p.add_argument("--escenario", choices=["A", "B"], default="A",
                   help="A: cabinas con bus compartido (situación actual). "
                        "B: un dispositivo por medidor, sin contención.")
    p.add_argument("--cabinas", type=int, default=20)
    p.add_argument("--dias", type=int, default=1)
    p.add_argument("--desde", default=None, help="Fecha de inicio AAAA-MM-DD (por defecto, hoy).")
    p.add_argument("--semilla", type=int, default=2026,
                   help="Misma semilla, mismos fallos: hace reproducibles las pruebas.")
    p.add_argument("--timeout", type=float, default=120.0, help="Tope por medidor, en segundos.")
    p.add_argument("--cada", type=float, default=None, metavar="MIN",
                   help="Minutos entre el arranque de una ronda y la siguiente. Por "
                        "defecto, rondas continuas en A y cada 15 min en B, que es como "
                        "operaría un dispositivo dedicado.")
    p.add_argument("--sin-fallas", action="store_true",
                   help="Mundo perfecto: línea de base para medir el efecto de cada falla.")
    p.add_argument("--salida", default="-", help="Archivo JSONL de salida, o '-' para stdout.")
    p.add_argument("--a-kafka", action="store_true",
                   help="Publica al tópico de lecturas en lugar de escribir JSONL. "
                        "Requiere la infraestructura levantada (ver infra/).")
    p.add_argument("--servidores", default=None,
                   help="Servidores de Kafka. Por defecto, KAFKA_BOOTSTRAP_SERVERS "
                        "o localhost:29092.")
    p.add_argument("--topico", default=None,
                   help="Tópico de destino. Por defecto, TOPICO_LECTURAS.")
    p.add_argument("--zona", default=ZONA)
    return p


def main(argv: list[str] | None = None) -> int:
    args = construir_parser().parse_args(argv)
    tz = ZoneInfo(args.zona)

    hoy = datetime.now(tz).date()
    dia = datetime.strptime(args.desde, "%Y-%m-%d").date() if args.desde else hoy
    inicio = datetime(dia.year, dia.month, dia.day, tzinfo=tz)
    fin = inicio + timedelta(days=args.dias)

    parque = generar_parque(
        cabinas=args.cabinas,
        inicio=inicio,
        dias=args.dias,
        semilla=args.semilla,
        tamano_fijo=1 if args.escenario == "B" else None,
    )
    fallas = (
        ConfigFallas(0.0, (0, 0), 0.0, 0.0, equipos_perfectos=True)
        if args.sin_fallas
        else ConfigFallas()
    )
    cadencia = args.cada if args.cada is not None else (15.0 if args.escenario == "B" else None)
    cfg = ConfigAgenda(
        inicio=inicio, fin=fin, timeout_segundos=args.timeout, cadencia_minutos=cadencia
    )

    print(f"escenario {args.escenario} · {parque.resumen()}", file=sys.stderr)
    ritmo = "rondas continuas" if cadencia is None else f"una ronda cada {cadencia:g} min"
    print(
        f"período {inicio:%Y-%m-%d %H:%M} → {fin:%Y-%m-%d %H:%M} · "
        f"semilla {args.semilla} · {ritmo}"
        f"{' · SIN FALLAS' if args.sin_fallas else ''}",
        file=sys.stderr,
    )

    lecturas = generar(parque, cfg, fallas, semilla=args.semilla)

    if args.a_kafka:
        import logging
        import os

        from .publicador import publicar

        logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stderr)
        servidores = args.servidores or os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"
        )
        topico = args.topico or os.environ.get("TOPICO_LECTURAS", "medicion.lecturas.v1")
        print(f"publicando a {servidores} · tópico {topico}", file=sys.stderr)
        n = publicar(lecturas, servidores=servidores, topico=topico)
        print(f"{n} lecturas publicadas en Kafka", file=sys.stderr)
        return 0

    destino = sys.stdout if args.salida == "-" else open(args.salida, "w", encoding="utf-8")
    n = 0
    try:
        for lectura in lecturas:
            destino.write(lectura.a_json() + "\n")
            n += 1
    finally:
        if destino is not sys.stdout:
            destino.close()

    print(f"{n} lecturas escritas" + ("" if args.salida == "-" else f" en {args.salida}"),
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
