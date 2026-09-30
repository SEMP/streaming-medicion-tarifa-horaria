#!/usr/bin/env python3
"""Dibuja una lámina SVG por cada etapa del pipeline: entrada, proceso y salida.

    uv run python docs/diagramas/generar-etapas.py

Deja `docs/diagramas/etapas/NN-<nombre>.svg`, una por etapa, más un índice en
`etapas/LEEME.md`. Sirven para explicar el pipeline paso a paso —en el documento, en la
defensa o en el video— sin tener que leer el código.

Son **distintas de los otros dos diagramas** y las tres hacen falta:

- `arquitectura.svg` muestra los componentes y los tópicos. Se mantiene a mano.
- `pipeline-dag.svg` lo dibuja Beam desde el código (`pipeline.grafico`): la topología real,
  sin explicación.
- Estas láminas explican **qué hace cada etapa por dentro**, con su estado, sus temporizadores
  y los motivos por los que manda algo a cuarentena.

El contenido de acá se escribe a mano y por eso **puede desactualizarse**: cada etapa declara
en `fuente` el archivo y las líneas de donde sale, para poder verificarlo.
"""

from __future__ import annotations

import html
from dataclasses import dataclass, field
from pathlib import Path

SALIDA = Path(__file__).parent / "etapas"

TIPO = "'DejaVu Sans','Helvetica Neue',Arial,sans-serif"
MONO = "'DejaVu Sans Mono','Menlo',monospace"

TINTA = "#1a202c"
SUAVE = "#4a5568"
TENUE = "#718096"

# Una familia de color por rol, la misma que usa arquitectura.svg.
ENTRADA = ("#ebf4ff", "#2b6cb0", "#2c5282")
PROCESO = ("#fffbeb", "#b7791f", "#744210")
SALIDA_C = ("#f0fff4", "#276749", "#22543d")
ESTADO = ("#faf5ff", "#6b46c1", "#553c9a")
ALERTA = ("#fff5f5", "#c53030", "#c53030")


@dataclass
class Caja:
    titulo: str
    lineas: list[str] = field(default_factory=list)
    color: tuple[str, str, str] = PROCESO
    nota: str = ""


@dataclass
class Etapa:
    numero: str
    nombre: str
    etiqueta: str
    resumen: str
    fuente: str
    entrada: Caja
    proceso: Caja
    salidas: list[Caja]
    estado: Caja | None = None


ETAPAS = [
    Etapa(
        numero="01",
        nombre="leer-lecturas",
        etiqueta="Leer lecturas del tópico crudo",
        resumen=(
            "KafkaIO no es una librería Python: es una transformación cross-language que "
            "ejecuta el SDK de Java, en modo PROCESS dentro del TaskManager."
        ),
        fuente="pipeline/src/pipeline/esqueleto.py:59 (leer_lecturas)",
        entrada=Caja(
            "Tópico medicion.lecturas.v1",
            [
                "clave   = medidor_id (bytes)",
                "valor   = JSON de la lectura (bytes)",
                "4 particiones",
                "grupo   = g-consumo-franja",
            ],
            ENTRADA,
            "La clave es el medidor y no la cabina: de 1 a 199 medidores por cabina daría un "
            "reparto desbalanceado en un factor de 200.",
        ),
        proceso=Caja(
            "ReadFromKafka",
            [
                "auto.offset.reset = earliest",
                "enable.auto.commit = true",
                "sin metadatos de Kafka",
            ],
            PROCESO,
            "El commit periódico no espera al procesamiento. Mientras Flink restaure desde "
            "checkpoint no importa: los offsets que valen son los del checkpoint.",
        ),
        salidas=[
            Caja(
                "PCollection de records",
                ["tuple[bytes, bytes]", "sin ventanear todavía"],
                SALIDA_C,
            )
        ],
    ),
    Etapa(
        numero="02",
        nombre="parsear",
        etiqueta="Parsear: de bytes a diccionario",
        resumen=(
            "Primera etapa que puede rechazar. Un mensaje ilegible va igual a cuarentena, con "
            "sus bytes crudos: nada se descarta en silencio."
        ),
        fuente="pipeline/src/pipeline/cadena.py:89 (parsear)",
        entrada=Caja("Records de Kafka", ["tuple[bytes, bytes]"], ENTRADA),
        proceso=Caja(
            "FlatMap(parsear)",
            [
                "1. json.loads del valor",
                "2. medidor_id del cuerpo,",
                "   o de la clave si falta",
                "3. emite (medidor, payload)",
            ],
            PROCESO,
            "Se lee el medidor del cuerpo y no de la clave porque la clave puede venir vacía; "
            "la clave sirve de respaldo.",
        ),
        salidas=[
            Caja("Salida principal", ["tuple[str, dict]", "clave = medidor_id"], SALIDA_C),
            Caja(
                "Cuarentena",
                ["json_invalido", "sin_medidor_id"],
                ALERTA,
                "json_invalido lleva los primeros 200 bytes crudos, porque no se le puede "
                "leer ni el medidor.",
            ),
        ],
    ),
    Etapa(
        numero="03",
        nombre="marcar-tiempo",
        etiqueta="Marcar el tiempo de evento",
        resumen=(
            "El record de Kafka trae cuándo se publicó; instante_lectura trae cuándo se midió. "
            "Entre los dos puede haber horas, y ventanear por el de publicación metería el "
            "consumo en el día equivocado."
        ),
        fuente="pipeline/src/pipeline/cadena.py:115 (marcar_tiempo_de_evento)",
        entrada=Caja("Lecturas parseadas", ["tuple[str, dict]"], ENTRADA),
        proceso=Caja(
            "FlatMap(marcar_tiempo_de_evento)",
            [
                "1. lee instante_lectura",
                "2. datetime.fromisoformat",
                "3. exige offset de huso",
                "4. TimestampedValue",
            ],
            PROCESO,
            "El instante lo pone el concentrador, no el medidor: muchos equipos tienen el "
            "reloj mal o no reportan hora (decisión 3).",
        ),
        salidas=[
            Caja(
                "Con tiempo de evento",
                ["TimestampedValue", "(medidor, payload)"],
                SALIDA_C,
            ),
            Caja(
                "Cuarentena",
                ["instante_invalido", "instante_sin_huso"],
                ALERTA,
                "Sin offset, la franja atribuida dependería de en qué máquina corre el "
                "pipeline (decisión 4).",
            ),
        ],
    ),
    Etapa(
        numero="04",
        nombre="ventana-diaria",
        etiqueta="Ventana diaria y tipado de la clave",
        resumen=(
            "La ventana no agrupa: acá no hay GroupByKey. Aporta el window.end contra el que "
            "se programan los temporizadores, y la lateness que fija cuándo expira el estado."
        ),
        fuente="pipeline/src/pipeline/cadena.py:189 (TiparClave, VentanaDiaria)",
        entrada=Caja("Lecturas con timestamp", ["TimestampedValue"], ENTRADA),
        proceso=Caja(
            "TiparClave + WindowInto",
            [
                "with_output_types(KV[str, Any])",
                "FixedWindows(24 h)",
                "offset = -3 h (America/Asuncion)",
                "allowed_lateness = 36 h",
                "sin trigger",
            ],
            PROCESO,
            "Sin trigger y no es un olvido: los triggers disparan en un GroupByKey o un "
            "Combine, y no hay ninguno. Configurarlo sugeriría algo que no ocurre.",
        ),
        salidas=[
            Caja(
                "Ventaneadas",
                ["KV[str, Any]", "ventana = día local"],
                SALIDA_C,
            )
        ],
    ),
    Etapa(
        numero="05",
        nombre="deduplicar",
        etiqueta="Deduplicar por instante de lectura",
        resumen=(
            "Va antes de diferenciar. Si fuera después, un duplicado se restaría contra sí "
            "mismo y daría un consumo de cero que, con salida por upsert, pisaría el valor "
            "correcto."
        ),
        fuente="pipeline/src/pipeline/transformaciones.py:72 (DeduplicarLecturas)",
        entrada=Caja("Lecturas ventaneadas", ["KV[str, Any]"], ENTRADA),
        proceso=Caja(
            "ParDo(DeduplicarLecturas)",
            [
                "1. exige instante_lectura",
                "2. ¿ya está en VISTOS?",
                "   sí  -> se cuenta y no se emite",
                "   no  -> se agrega y se emite",
                "3. programa el temporizador",
            ],
            PROCESO,
            "Un duplicado no es un error, es el caso que esta etapa existe para atender: no "
            "va a cuarentena, pero sí se cuenta.",
        ),
        estado=Caja(
            "Estado por clave y ventana",
            [
                "VISTOS : SetState(str)",
                "EXPIRA : Timer(WATERMARK)",
                "        window.end + 36 h",
                "métrica: duplicados_descartados",
            ],
            ESTADO,
            "Sin expiración el conjunto crecería sin límite: la entrada de un pipeline de "
            "streaming no está acotada.",
        ),
        salidas=[
            Caja("Lecturas únicas", ["KV[str, Any]"], SALIDA_C),
            Caja("Cuarentena", ["instante_invalido"], ALERTA),
        ],
    ),
    Etapa(
        numero="06",
        nombre="diferenciar",
        etiqueta="Diferenciar el contador acumulado",
        resumen=(
            "El medidor informa un acumulado; el consumo es la resta entre lecturas "
            "consecutivas. Cada lectura forma como mucho dos intervalos: con la anterior y "
            "con la siguiente."
        ),
        fuente="pipeline/src/pipeline/transformaciones.py:124 (DiferenciarContador)",
        entrada=Caja("Lecturas únicas", ["KV[str, Any]", "registro OBIS 15.8.0"], ENTRADA),
        proceso=Caja(
            "ParDo(DiferenciarContador)",
            [
                "1. busca el registro 15.8.0",
                "2. guarda (instante, valor, cabina)",
                "3. ordena todas las del estado",
                "4. arma los dos intervalos",
                "   vecinos de esta lectura",
                "5. delta = v_hasta - v_desde",
            ],
            PROCESO,
            "Recalcular contra el estado ordenado es lo que permite que una lectura tardía "
            "parta un intervalo ya emitido.",
        ),
        estado=Caja(
            "Estado por clave y ventana",
            [
                "LECTURAS : BagState(str)",
                "EXPIRA   : Timer(WATERMARK)",
                "          window.end + 36 h",
                "distribución: separacion_minutos",
            ],
            ESTADO,
            "La separación entre lecturas es la cota del error de atribución. Se consulta "
            "por la API de métricas de Beam: hoy no se ve en la interfaz de Flink.",
        ),
        salidas=[
            Caja(
                "Consumo por intervalo",
                [
                    "Consumo(medidor, cabina,",
                    "  desde, hasta,",
                    "  energia_kwh,",
                    "  separacion_minutos)",
                ],
                SALIDA_C,
            ),
            Caja(
                "Cuarentena",
                ["sin_registro_util", "contador_retrocede"],
                ALERTA,
                "Un delta negativo es un reseteo del contador: sin inyección remunerada, el "
                "acumulado no baja.",
            ),
        ],
    ),
    Etapa(
        numero="07",
        nombre="celdas-vigentes",
        etiqueta="Celdas vigentes: atribución por franja",
        resumen=(
            "Es una sola etapa con estado y no dos agregaciones, y ese es el punto: encadenar "
            "dos agregaciones bajo modo acumulativo contaba doble (decisión 12)."
        ),
        fuente="pipeline/src/pipeline/transformaciones.py:227 (CeldasVigentes)",
        entrada=Caja("Consumos por intervalo", ["KV[str, Consumo]", "clave = medidor_id"], ENTRADA),
        proceso=Caja(
            "ParDo(CeldasVigentes)",
            [
                "1. ¿el borde izquierdo ya tiene",
                "   un intervalo igual o más corto?",
                "   sí -> este quedó superado, sale",
                "2. lo agrega a los vigentes",
                "3. recalcula TODAS las celdas",
                "   del medidor desde cero",
                "4. reparte por franja; si el cruce",
                "   supera el umbral, indeterminada",
                "5. emite solo lo que cambió",
            ],
            PROCESO,
            "Recalcular todo es más caro y es a propósito: el resultado no depende del orden "
            "de llegada, así que ningún orden raro deja una celda vieja.",
        ),
        estado=Caja(
            "Estado por clave y ventana",
            [
                "INTERVALOS : ReadModifyWrite",
                "CELDAS     : ReadModifyWrite",
                "EXPIRA     : Timer(WATERMARK)",
                "métricas: celdas_emitidas,",
                "          celdas_indeterminadas",
            ],
            ESTADO,
            "INTERVALOS es lo que evita el doble conteo: sin él, el intervalo grosero seguía "
            "sumando junto con las dos mitades que lo reemplazan.",
        ),
        salidas=[
            Caja(
                "Celda, valor absoluto",
                [
                    "clave = medidor|fecha|franja",
                    "energia_kwh, interpolada,",
                    "indeterminada, minutos_cubiertos,",
                    "minutos_indeterminados,",
                    "separacion_maxima_minutos,",
                    "cabina_id, intervalos_usados",
                ],
                SALIDA_C,
                "Valor absoluto, no incremento: por eso el consumidor hace upsert y "
                "reprocesar es inocuo.",
            )
        ],
    ),
    Etapa(
        numero="08",
        nombre="escribir-consumo",
        etiqueta="Escribir al tópico derivado",
        resumen=(
            "La clave del mensaje es la celda, de modo que recalcular reemplace en lugar de "
            "duplicar. Es lo que hace idempotente a la salida."
        ),
        fuente="pipeline/src/pipeline/cadena.py:229 (ABytes) y esqueleto.py:91 (escribir)",
        entrada=Caja("Celdas que cambiaron", ["tuple[str, dict]"], ENTRADA),
        proceso=Caja(
            "ABytes + WriteToKafka",
            [
                "clave -> utf-8",
                "valor -> json.dumps utf-8",
                "KafkaIO Write (SDK de Java)",
            ],
            PROCESO,
            "El cliente Java usa acks=all e idempotencia desde Kafka 3.0. El productor del "
            "simulador, que es librdkafka, tiene que pedirlas explícitamente.",
        ),
        salidas=[
            Caja(
                "Tópico medicion.consumo-franja.v1",
                [
                    "clave = medidor|fecha|franja",
                    "compactable por clave",
                    "lo lee el tablero y facturación",
                ],
                SALIDA_C,
            )
        ],
    ),
    Etapa(
        numero="09",
        nombre="cuarentena",
        etiqueta="Cuarentena: la salida lateral",
        resumen=(
            "Cuatro etapas mandan acá. Su volumen es una señal operativa: si sube en una zona, "
            "el problema es la cobertura de red y no el pipeline."
        ),
        fuente="pipeline/src/pipeline/cadena.py:243 (JuntarCuarentena)",
        entrada=Caja(
            "Cuatro salidas etiquetadas",
            [
                "Parsear      -> 2 motivos",
                "MarcarTiempo -> 2 motivos",
                "Deduplicar   -> 1 motivo",
                "Diferenciar  -> 2 motivos",
            ],
            ENTRADA,
            "Las dos primeras salen antes de ventanear, así que están en la ventana global; "
            "las dos con estado, en la diaria.",
        ),
        proceso=Caja(
            "Re-ventanear + Flatten",
            [
                "1. las ventaneadas vuelven",
                "   a GlobalWindows",
                "2. Flatten de las cuatro",
                "3. clave = medidor, o vacía",
                "   si no se lo pudo leer",
            ],
            PROCESO,
            "Flatten exige el mismo ventaneo en todas las entradas: sin el paso 1 el pipeline "
            "no se construye.",
        ),
        salidas=[
            Caja(
                "Tópico medicion.cuarentena.v1",
                ['{"motivo": ..., "lectura": ...}', "un contador de Beam por motivo"],
                ALERTA,
            )
        ],
    ),
]


# ── dibujo ──────────────────────────────────────────────────────────────────────────────

ANCHO = 1180
COL_X = (40, 430, 830)
COL_W = (350, 360, 310)
ALTO_LINEA = 19
PAD = 14


def _texto(x, y, texto, *, tam=13, color=SUAVE, familia=TIPO, peso="normal", ancla="start"):
    # SVG colapsa los espacios de la izquierda, y en estas láminas la sangría es la que
    # muestra qué línea continúa a la anterior. Se preservan con espacios duros.
    sangria = len(texto) - len(texto.lstrip(" "))
    cuerpo = "\u00a0" * sangria + html.escape(texto.lstrip(" "))
    return (
        f'<text x="{x}" y="{y}" font-family="{familia}" font-size="{tam}" '
        f'fill="{color}" font-weight="{peso}" text-anchor="{ancla}">{cuerpo}</text>'
    )


def _envolver(texto: str, ancho_car: int) -> list[str]:
    lineas, actual = [], ""
    for palabra in texto.split():
        if len(actual) + len(palabra) + 1 > ancho_car:
            lineas.append(actual)
            actual = palabra
        else:
            actual = f"{actual} {palabra}".strip()
    if actual:
        lineas.append(actual)
    return lineas


def _alto_caja(caja: Caja, ancho: int) -> int:
    alto = PAD + 20 + len(caja.lineas) * ALTO_LINEA + PAD
    if caja.nota:
        alto += 8 + len(_envolver(caja.nota, ancho // 7)) * 16
    return alto


def _dibujar_caja(caja: Caja, x: int, y: int, ancho: int) -> tuple[str, int]:
    relleno, borde, titulo = caja.color
    alto = _alto_caja(caja, ancho)
    piezas = [
        f'<rect x="{x}" y="{y}" width="{ancho}" height="{alto}" rx="7" '
        f'fill="{relleno}" stroke="{borde}" stroke-width="1.6"/>',
        _texto(x + PAD, y + PAD + 12, caja.titulo, tam=14, color=titulo, peso="600"),
    ]
    cursor = y + PAD + 20
    for linea in caja.lineas:
        cursor += ALTO_LINEA
        piezas.append(_texto(x + PAD, cursor, linea, tam=12.5, familia=MONO, color=TINTA))
    if caja.nota:
        cursor += 8
        for linea in _envolver(caja.nota, ancho // 7):
            cursor += 16
            piezas.append(_texto(x + PAD, cursor, linea, tam=11.5, color=TENUE))
    return "\n  ".join(piezas), alto


def _flecha(x1, y, x2) -> str:
    return (
        f'<line x1="{x1}" y1="{y}" x2="{x2 - 9}" y2="{y}" stroke="#2d3748" '
        f'stroke-width="1.6" marker-end="url(#fl)"/>'
    )


def dibujar(etapa: Etapa) -> str:
    resumen = _envolver(etapa.resumen, 132)
    # La cabecera crece con el resumen: con una altura fija, un resumen de tres líneas se
    # metía debajo de los rótulos de columna.
    cabecera = 100 + (len(resumen) - 1) * 17
    cuerpo: list[str] = []

    entrada_svg, alto_e = _dibujar_caja(etapa.entrada, COL_X[0], cabecera, COL_W[0])
    cuerpo.append(entrada_svg)

    proceso_svg, alto_p = _dibujar_caja(etapa.proceso, COL_X[1], cabecera, COL_W[1])
    cuerpo.append(proceso_svg)
    fin_proceso = cabecera + alto_p
    if etapa.estado:
        estado_svg, alto_s = _dibujar_caja(etapa.estado, COL_X[1], fin_proceso + 16, COL_W[1])
        cuerpo.append(estado_svg)
        cuerpo.append(
            f'<line x1="{COL_X[1] + COL_W[1] // 2}" y1="{fin_proceso}" '
            f'x2="{COL_X[1] + COL_W[1] // 2}" y2="{fin_proceso + 16}" '
            f'stroke="#6b46c1" stroke-width="1.4" stroke-dasharray="4 3"/>'
        )
        fin_proceso += 16 + alto_s

    cursor = cabecera
    for salida in etapa.salidas:
        salida_svg, alto_x = _dibujar_caja(salida, COL_X[2], cursor, COL_W[2])
        cuerpo.append(salida_svg)
        cuerpo.append(_flecha(COL_X[1] + COL_W[1], cursor + 26, COL_X[2]))
        cursor += alto_x + 16
    fin_salidas = cursor - 16

    cuerpo.append(_flecha(COL_X[0] + COL_W[0], cabecera + 26, COL_X[1]))

    alto = max(cabecera + alto_e, fin_proceso, fin_salidas) + 58

    encabezado = [
        f'<rect width="{ANCHO}" height="{alto}" fill="#ffffff"/>',
        _texto(40, 42, f"{etapa.numero}. {etapa.etiqueta}", tam=21, color=TINTA, peso="600"),
    ]
    for i, linea in enumerate(resumen):
        encabezado.append(_texto(40, 66 + i * 17, linea, tam=13, color=SUAVE))

    rotulos = [
        _texto(COL_X[0], cabecera - 10, "ENTRADA", tam=11, color=ENTRADA[2], peso="700"),
        _texto(COL_X[1], cabecera - 10, "PROCESO", tam=11, color=PROCESO[2], peso="700"),
        _texto(COL_X[2], cabecera - 10, "SALIDA", tam=11, color=SALIDA_C[2], peso="700"),
    ]
    pie = _texto(40, alto - 22, etapa.fuente, tam=11.5, familia=MONO, color=TENUE)

    return f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {ANCHO} {alto}" \
width="{ANCHO}" height="{alto}" font-family={TIPO!r}>
  <defs>
    <marker id="fl" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" \
orient="auto-start-reverse">
      <path d="M 0 0 L 10 5 L 0 10 z" fill="#2d3748"/>
    </marker>
  </defs>
  {chr(10).join("  " + p for p in encabezado)}
  {chr(10).join("  " + p for p in rotulos)}
  {chr(10).join("  " + p for p in cuerpo)}
  {pie}
</svg>
"""


def main() -> int:
    SALIDA.mkdir(parents=True, exist_ok=True)
    filas = []
    for etapa in ETAPAS:
        destino = SALIDA / f"{etapa.numero}-{etapa.nombre}.svg"
        destino.write_text(dibujar(etapa), encoding="utf-8")
        filas.append(
            f"| {etapa.numero} | [`{destino.name}`]({destino.name}) | {etapa.etiqueta} |"
        )
        print(f"  {destino.relative_to(SALIDA.parent.parent.parent)}")

    (SALIDA / "LEEME.md").write_text(
        "# Las etapas del pipeline, una por lámina\n\n"
        "Cada lámina muestra **entrada, proceso y salida** de una etapa, con su estado, sus "
        "temporizadores y los motivos por los que manda algo a cuarentena. Se regeneran con:\n\n"
        "```bash\n"
        "uv run python docs/diagramas/generar-etapas.py\n"
        "```\n\n"
        "| # | Lámina | Etapa |\n|---|---|---|\n" + "\n".join(filas) + "\n\n"
        "## Los otros dos diagramas, que muestran otra cosa\n\n"
        "- [`../arquitectura.svg`](../arquitectura.svg): los componentes y los tópicos. "
        "Se mantiene a mano.\n"
        "- [`../pipeline-dag.svg`](../pipeline-dag.svg): la topología real, dibujada por Beam "
        "desde el mismo código que corre en producción (`uv run python -m pipeline.grafico`).\n\n"
        "**Estas láminas se escriben a mano** y por lo tanto pueden desactualizarse. Cada una "
        "declara al pie el archivo y la línea de donde sale, para poder verificarla.\n",
        encoding="utf-8",
    )
    print(f"  índice en {SALIDA.name}/LEEME.md · {len(ETAPAS)} etapas")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
