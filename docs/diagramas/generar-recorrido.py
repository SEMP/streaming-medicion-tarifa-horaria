#!/usr/bin/env python3
"""Dibuja el recorrido de un dato: del medidor al consumidor, con el dato real en cada punto.

    uv run python docs/diagramas/generar-recorrido.py

Deja `docs/diagramas/recorrido/`: una lámina con el recorrido completo y una de detalle por
estación, más un índice en `LEEME.md`.

**Es la vista que las otras tres no dan.** `arquitectura.svg` muestra los componentes,
`pipeline-dag.svg` la topología y `etapas/` el mecanismo de cada etapa. Acá se sigue **una
lectura concreta** y se muestra en qué se convierte en cada paso: qué entra, qué sale y por
qué cambia de forma.

La lectura que se sigue es la de las **17:55 del 25/09/2026 del medidor MED-0042**, con el
contador en 101,5 kWh: la misma de `pipeline.demostracion` y de `pipeline.extremo_a_extremo`,
así que los números coinciden con los del documento técnico y con la evidencia.

**Los payloads no están escritos a mano.** Salen de correr el código: el evento de entrada de
`Lectura.a_dict()`, y el `Consumo` y las celdas de pasar las tres lecturas por
`DeduplicarLecturas`, `DiferenciarContador` y `CeldasVigentes` con `DirectRunner`. Cada lámina
declara al pie de dónde sale.
"""

from __future__ import annotations

import html
from dataclasses import dataclass, field
from pathlib import Path

SALIDA = Path(__file__).parent / "recorrido"
PRESENTACION = SALIDA / "presentacion"

TIPO = "'DejaVu Sans','Helvetica Neue',Arial,sans-serif"
MONO = "'DejaVu Sans Mono','Menlo',monospace"

TINTA, SUAVE, TENUE = "#1a202c", "#4a5568", "#718096"
FUENTE_C = ("#faf5ff", "#6b46c1", "#553c9a")   # simulador
KAFKA_C = ("#fffbeb", "#b7791f", "#744210")    # tópicos
PIPE_C = ("#ebf4ff", "#2b6cb0", "#2c5282")     # pipeline
CONSU_C = ("#f0fff4", "#276749", "#22543d")    # consumidores
ALERTA = ("#fff5f5", "#c53030", "#c53030")     # cuarentena


@dataclass
class Estacion:
    numero: str
    nombre: str
    corto: str
    breve: str
    color: tuple[str, str, str]
    titular: str
    hace: list[str]
    sale: list[str]
    porque: str
    fuente: str
    forma: str                       # una línea, para la lámina del recorrido completo
    extra: list[str] = field(default_factory=list)
    """Contexto que acompaña a la salida pero **no viaja** a la estación siguiente."""
    cuarentena: list[str] = field(default_factory=list)
    entra: list[str] = field(default_factory=list)
    """Solo la estación 01 lo declara. Para el resto se deriva de la salida de la anterior,
    que es lo que hace que el recorrido se pueda seguir sin cortes y que las dos no puedan
    desincronizarse."""


def encadenar(estaciones: list[Estacion]) -> None:
    """La entrada de cada estación **es** la salida de la anterior, literalmente."""
    for previa, actual in zip(estaciones[:-1], estaciones[1:], strict=True):
        actual.entra = list(previa.sale)


ESTACIONES = [
    Estacion(
        "01", "simulador", "Simulador", "Simulador", FUENTE_C,
        "El dato nace porque alguien lo pidió",
        ["1. El concentrador recorre la cabina en orden",
         "2. Pregunta el registro OBIS 15.8.0",
         "3. El medidor responde su CONTADOR ACUMULADO,",
         "   no el consumo del intervalo",
         "4. Se arma la Lectura y se sella el event_id"],
        ['{',
         '  "schema_version": 1,',
         '  "event_id": "d4612d921b39e34a",',
         '  "medidor_id": "MED-0042",',
         '  "cabina_id": "CAB-07",',
         '  "instante_lectura": "2026-09-25T17:55:00-03:00",',
         '  "registros": [{"obis": "15.8.0",',
         '    "naturaleza": "acumulado",',
         '    "valor": 101.5, "unidad": "kWh"}],',
         '  "calidad": "ok"',
         '}'],
        "El valor 101,5 no es lo que se consumió: es el total desde que el medidor existe. El "
        "consumo hay que sacarlo restando, y por eso hace falta recordar la lectura anterior. "
        "Y el event_id es sha256(medidor|instante): un reintento de publicación produce el "
        "mismo id, que es lo que lo hace reconocible como duplicado.",
        "simulador/src/simulador/evento.py:58 (Lectura) · valor real de Lectura.a_dict()",
        "Lectura con contador acumulado",
        entra=["Nada. El medidor no transmite por su cuenta: el concentrador pregunta,",
               "medidor por medidor, sobre un bus RS-485 compartido."],
    ),
    Estacion(
        "02", "topico-crudo", "medicion.lecturas.v1", "Tópico crudo", KAFKA_C,
        "El log crudo: lo que llegó, tal como llegó",
        ["1. clave  = medidor_id, en bytes",
         "2. valor  = ese mismo JSON, serializado en bytes",
         "3. Kafka elige partición por hash de la clave",
         "4. El productor va con acks=all e idempotencia"],
        ["clave : b'MED-0042'",
         "valor : b'{\"schema_version\": 1, \"event_id\":",
         "         \"d4612d921b39e34a\", \"medidor_id\": ...}'"],
        "La clave es el medidor y no la cabina. Dos razones: de 1 a 199 medidores por cabina "
        "desbalancearía el reparto en un factor de 200, y la clave es lo que garantiza que las "
        "lecturas de un mismo medidor lleguen en orden — sin ese orden, restar dos lecturas "
        "consecutivas no tendría sentido.",
        "docs/contratos.md sección 1 · simulador/src/simulador/publicador.py:18",
        "clave=medidor · JSON",
        extra=["4 particiones · retención 7 días · cleanup.policy = delete"],
    ),
    Estacion(
        "03", "entrada-al-pipeline", "Parsear y marcar el tiempo",
        "Parsear y marcar el tiempo", PIPE_C,
        "De bytes a un par con clave, y con el tiempo del dominio",
        ["1. json.loads del valor -> ('MED-0042', {...})",
         "   el medidor sale del cuerpo; la clave es respaldo",
         "2. Se lee instante_lectura y se exige su huso",
         "3. Se sella como tiempo de EVENTO, no de llegada"],
        ["TimestampedValue(",
         "  ('MED-0042', {...}),",
         "  timestamp = 1790369700",
         ")"],
        "El record de Kafka trae cuándo se publicó; instante_lectura trae cuándo se midió. "
        "Entre los dos puede haber horas —una cabina que vuelve de una caída publica de golpe "
        "lo que fue juntando— y ventanear por el de publicación metería ese consumo en el día "
        "equivocado, que es dinero mal facturado.",
        "pipeline/src/pipeline/cadena.py:89 y :115",
        "('MED-0042', dict) con tiempo de evento",
        extra=["1790369700 = 2026-09-25 17:55:00-03:00",
               "{...} es el JSON de la estación 01, ya parseado a dict"],
        cuarentena=["json_invalido", "sin_medidor_id", "instante_invalido", "instante_sin_huso"],
    ),
    Estacion(
        "04", "ventana-y-dedup", "Ventana diaria y deduplicación",
        "Ventana y deduplicación", PIPE_C,
        "El duplicado muere acá, antes de poder hacer daño",
        ["1. Cae en la ventana del día local",
         "   FixedWindows(24 h), desplazada -3 h",
         "2. ¿'2026-09-25T17:55:00-03:00' en VISTOS?",
         "   sí -> se cuenta y NO se emite",
         "   no -> se agrega y sigue",
         "3. Timer para expirar a window.end + 36 h"],
        ["TimestampedValue(",
         "  ('MED-0042', {...}),",
         "  timestamp = 1790369700",
         ")   en la ventana del 2026-09-25 local"],
        "Va antes de diferenciar, y el orden no es intercambiable: si fuera después, el "
        "duplicado se restaría contra sí mismo y daría un consumo de cero que, con salida por "
        "upsert, pisaría el valor bueno. Un duplicado no es un error —es lo que esta etapa "
        "existe para atender— así que no va a cuarentena, pero sí se cuenta.",
        "pipeline/src/pipeline/transformaciones.py:72 (DeduplicarLecturas)",
        "lectura única, ventaneada",
        extra=["estado VISTOS del medidor, después de esta lectura:",
               "  {'2026-09-25T17:40:00-03:00',",
               "   '2026-09-25T17:55:00-03:00'}"],
        cuarentena=["instante_invalido"],
    ),
    Estacion(
        "05", "diferenciar", "Diferenciar el contador", "Diferenciar el contador", PIPE_C,
        "Recién acá aparece el consumo, que no venía en el dato",
        ["1. Busca el registro OBIS 15.8.0 -> 101.5",
         "2. Guarda (instante, valor, cabina) en el estado",
         "3. Ordena TODAS las del medidor en la ventana",
         "4. Arma los intervalos vecinos de esta lectura",
         "5. energia = v_hasta - v_desde = 101.5 - 100.0"],
        ["Consumo(",
         "  medidor_id='MED-0042',",
         "  cabina_id='CAB-07',",
         "  desde='2026-09-25T17:40:00-03:00',",
         "  hasta='2026-09-25T17:55:00-03:00',",
         "  energia_kwh=1.5,",
         "  separacion_minutos=15.0",
         ")"],
        "101,5 menos 100,0 da 1,5 kWh en quince minutos. Ordenar contra el estado en vez de "
        "recordar solo la última es lo que permite que una lectura tardía parta en dos un "
        "intervalo ya emitido. Y separacion_minutos es la cota del error de "
        "atribución, el número que el proyecto existe para medir.",
        "pipeline/src/pipeline/transformaciones.py:124 (DiferenciarContador)",
        "Consumo(desde, hasta, 1.5 kWh)",
        extra=["la lectura de las 17:40, con el contador en 100.0,",
               "estaba guardada en el estado desde su propio paso por acá"],
        cuarentena=["sin_registro_util", "contador_retrocede"],
    ),
    Estacion(
        "06", "celdas-vigentes", "Atribuir la franja y agregar",
        "Atribuir franja y agregar", PIPE_C,
        "Donde el intervalo se convierte en plata, y donde puede cruzar un borde",
        ["1. ¿Hay ya un intervalo igual o más corto para",
         "   ese borde izquierdo? Entonces este quedó",
         "   superado y no se suma",
         "2. Recalcula TODAS las celdas del medidor",
         "3. Reparte por franja; si el intervalo cruza",
         "   un borde, interpola suponiendo potencia",
         "   constante, y lo declara",
         "4. Emite solo lo que cambió"],
        ["('MED-0042|2026-09-25|resto',",
         ' {"energia_kwh": 1.5, "interpolada": false,',
         '  "indeterminada": false, "minutos_cubiertos": 15.0,',
         '  "separacion_maxima_minutos": 15.0,',
         '  "cabina_id": "CAB-07", "intervalos_usados": 1})'],
        "Este intervalo cae entero en resto, así que la celda sale medida. Cuando llegue el "
        "siguiente —17:55 a 18:20, 4,0 kWh— sí cruza el borde de punta: sus minutos se reparten "
        "5 de un lado y 20 del otro, o sea 0,8 kWh a resto y 3,2 a punta, y las dos celdas "
        "pasan a interpoladas. Recalcular todo en vez de solo lo que tocó la lectura nueva es "
        "más caro a propósito: así el resultado no depende del orden de llegada.",
        "pipeline/src/pipeline/transformaciones.py:227 (CeldasVigentes)",
        "celda medidor|fecha|franja, valor absoluto",
        extra=["y cuando llega el intervalo que cruza las 18:00,",
               "esta misma celda se reemite y aparece la otra:",
               "  MED-0042|2026-09-25|resto  2.3  interpolada",
               "  MED-0042|2026-09-25|punta  3.2  interpolada"],
    ),
    Estacion(
        "07", "topico-derivado", "medicion.consumo-franja.v1", "Tópico derivado", KAFKA_C,
        "El log derivado: la clave es la celda, no el evento",
        ["1. clave  = medidor|fecha|franja, en bytes",
         "2. valor  = el JSON de la celda, en bytes",
         "3. KafkaIO Write, con el SDK de Java"],
        ["clave : b'MED-0042|2026-09-25|resto'",
         "valor : b'{\"energia_kwh\": 1.5, \"interpolada\":",
         "         false, ...}'"],
        "Que la clave sea la celda y el valor sea absoluto es lo que hace idempotente a todo "
        "el sistema: cada mensaje reemplaza al anterior en vez de sumarse. Reprocesar el log "
        "entero desde el offset 0 converge a las mismas celdas, y eso está verificado sobre "
        "Kafka y Flink, no solo afirmado.",
        "pipeline/src/pipeline/cadena.py:229 · esqueleto.py:91",
        "clave=celda · absoluto",
        extra=["cleanup.policy = compact,delete · 90 días",
               "la compactación por clave deja viva la última revisión de cada celda"],
    ),
    Estacion(
        "08", "consumidores", "Tablero y facturación", "Tablero y facturación", CONSU_C,
        "Dos lectores del mismo log, con necesidades opuestas",
        ["El tablero relee desde el offset 0 y hace upsert",
         "por clave: el último gana, nunca suma.",
         "",
         "La facturación lee una sola vez, pasado",
         "ventana_fin + 36 h, cuando ya convergió."],
        ["MED-0042|2026-09-25|punta   3.100 kWh",
         "MED-0042|2026-09-25|resto   2.400 kWh",
         "TOTAL                       5.500 kWh"],
        "Las cinco revisiones que publicó el pipeline colapsan en dos celdas. Si el tablero "
        "sumara en vez de reemplazar, el total daría más del doble. El error de atribución lo "
        "calcula el consumidor y no el pipeline, porque el denominador es la duración de la "
        "franja, que vive en el calendario tarifario y no en el evento: ponerlo en el mensaje "
        "obligaría a que los dos coincidieran sobre qué calendario rige en cada fecha.",
        "tablero/tablero.py · docs/contratos.md sección 2.3",
        "5 mensajes -> 2 celdas · total 5.500",
        extra=["error de atribución = separacion_maxima_minutos",
               "                    / duración de la franja"],
    ),
]
encadenar(ESTACIONES)


# ── utilidades de dibujo ────────────────────────────────────────────────────────────────

def _texto(x, y, t, *, tam=13, color=SUAVE, familia=TIPO, peso="normal", ancla="start"):
    sangria = len(t) - len(t.lstrip(" "))
    cuerpo = " " * sangria + html.escape(t.lstrip(" "))
    return (f'<text x="{x}" y="{y}" font-family="{familia}" font-size="{tam}" fill="{color}" '
            f'font-weight="{peso}" text-anchor="{ancla}">{cuerpo}</text>')


def _envolver(t: str, ancho: int) -> list[str]:
    lineas, act = [], ""
    for p in t.split():
        if len(act) + len(p) + 1 > ancho:
            lineas.append(act)
            act = p
        else:
            act = f"{act} {p}".strip()
    if act:
        lineas.append(act)
    return lineas


def _marcador() -> str:
    return ('<defs><marker id="fl" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" '
            'markerHeight="7" orient="auto-start-reverse">'
            '<path d="M 0 0 L 10 5 L 0 10 z" fill="#2d3748"/></marker>'
            '<marker id="flr" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" '
            'markerHeight="7" orient="auto-start-reverse">'
            '<path d="M 0 0 L 10 5 L 0 10 z" fill="#c53030"/></marker></defs>')


# ── lámina de detalle ───────────────────────────────────────────────────────────────────

# Ancho medio de un carácter, en unidades de la fuente. En monoespaciada es exacto; en la
# proporcional es un promedio, suficiente para envolver sin desbordar.
_CAR_MONO = 0.6
_CAR_TIPO = 0.52


def _ancho_detalle(e: Estacion, margen: int = 40) -> int:
    """El ancho de la lámina sale de su línea más larga, no de un valor fijo.

    Con un ancho fijo las cajas quedaban a lo ancho de toda la lámina y el texto ocupaba la
    mitad izquierda: el resto era espacio vacío dentro del recuadro.
    """
    mono_entra = int(e.numero) > 1
    anchos = [len(ln) * 12.5 * (_CAR_MONO if mono_entra else _CAR_TIPO) for ln in e.entra]
    anchos += [len(ln) * 12.5 * _CAR_MONO for ln in (*e.hace, *e.sale)]
    anchos += [len(ln) * 11.5 * _CAR_MONO for ln in e.extra]
    if e.cuarentena:
        anchos.append(len(" · ".join(e.cuarentena)) * 12.5 * _CAR_MONO)
    anchos.append(len(e.fuente) * 11.5 * _CAR_MONO - 32)
    # Piso: que el rótulo más largo y el título entren holgados.
    caja = max(max(anchos) + 44, 560)
    return int(caja) + 2 * margen


def detalle(e: Estacion, presentacion: bool = False) -> str:
    """Una estación: lo que llega, lo que le pasa y lo que sale.

    Con `presentacion`, la lámina lleva **solo el diagrama**: sin título, sin el «por qué» y
    sin la línea de fuente, que en una diapositiva van como texto de la diapositiva y en sus
    notas. Así el texto explicativo no queda dos veces en pantalla, ni en una imagen que no se
    puede editar ni leer con la tipografía del resto.
    """
    relleno, borde, titulo = e.color
    m = 16 if presentacion else 40          # margen exterior
    t = m + 16                              # sangría del texto dentro de las cajas
    ANCHO_D = _ancho_detalle(e, m)
    piezas = []

    if presentacion:
        y = m
    else:
        cab = _envolver(e.titular, int((ANCHO_D - 80) / (14 * 0.56)))
        piezas.append(_texto(40, 44, f"{e.numero}. {e.corto}", tam=21, color=TINTA, peso="600"))
        for i, ln in enumerate(cab):
            piezas.append(_texto(40, 70 + i * 18, ln, tam=14, color=titulo, peso="600"))
        y = 70 + len(cab) * 18 + 22

    def bloque(rotulo, lineas, col, mono=True, extra=()):
        nonlocal y
        r, b, t_ = col
        alto = 34 + len(lineas) * 18 + 14 + (len(extra) * 17 + 10 if extra else 0)
        piezas.append(f'<rect x="{m}" y="{y}" width="{ANCHO_D - 2 * m}" height="{alto}" rx="7" '
                      f'fill="{r}" stroke="{b}" stroke-width="1.6"/>')
        piezas.append(_texto(t, y + 22, rotulo, tam=11, color=t_, peso="700"))
        for i, ln in enumerate(lineas):
            piezas.append(_texto(t, y + 42 + i * 18, ln, tam=12.5,
                                 familia=MONO if mono else TIPO, color=TINTA))
        # El contexto no viaja a la estación siguiente, así que va en gris y más chico:
        # lo que se copia hacia abajo es solo el bloque de arriba.
        base = y + 42 + len(lineas) * 18 + 4
        for i, ln in enumerate(extra):
            piezas.append(_texto(t, base + i * 17, ln, tam=11.5, familia=MONO, color=TENUE))
        y += alto
        return alto

    def flecha():
        nonlocal y
        piezas.append(f'<line x1="{ANCHO_D // 2}" y1="{y + 2}" x2="{ANCHO_D // 2}" y2="{y + 20}" '
                      f'stroke="#2d3748" stroke-width="1.8" marker-end="url(#fl)"/>')
        y += 26

    anterior = int(e.numero) - 1
    rot_entra = ("LO QUE LLEGA" if anterior < 1
                 else f"LO QUE LLEGA — es la salida de la {anterior:02d}")
    bloque(rot_entra, e.entra, ("#f7fafc", "#a0aec0", TENUE), mono=anterior >= 1)
    flecha()
    bloque("QUÉ LE PASA", e.hace, (relleno, borde, titulo))
    flecha()
    siguiente = int(e.numero) + 1
    rot_sale = ("LO QUE SALE" if siguiente > 8
                else f"LO QUE SALE — es lo que llega a la {siguiente:02d}")
    bloque(rot_sale, e.sale, CONSU_C, extra=e.extra)

    if e.cuarentena:
        y += 10
        r, b, t_ = ALERTA
        alto = 34 + 18 + 14
        piezas.append(f'<rect x="{m}" y="{y}" width="{ANCHO_D - 2 * m}" height="{alto}" rx="7" '
                      f'fill="{r}" stroke="{b}" stroke-width="1.6" stroke-dasharray="5 3"/>')
        piezas.append(_texto(t, y + 22, "Y SI NO SE PUEDE, VA A CUARENTENA",
                             tam=11, color=t_, peso="700"))
        piezas.append(_texto(t, y + 42, " · ".join(e.cuarentena),
                             tam=12.5, familia=MONO, color=TINTA))
        y += alto

    if presentacion:
        alto_total = y + m
    else:
        y += 26
        piezas.append(_texto(40, y, "POR QUÉ", tam=11, color=TINTA, peso="700"))
        y += 8
        for ln in _envolver(e.porque, int((ANCHO_D - 80) / (13 * _CAR_TIPO))):
            y += 18
            piezas.append(_texto(40, y, ln, tam=13, color=SUAVE))

        y += 34
        piezas.append(_texto(40, y, e.fuente, tam=11.5, familia=MONO, color=TENUE))
        alto_total = y + 28

    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {ANCHO_D} {alto_total}" '
            f'width="{ANCHO_D}" height="{alto_total}" font-family={TIPO!r}>\n  {_marcador()}\n'
            f'  <rect width="{ANCHO_D}" height="{alto_total}" fill="#ffffff"/>\n  '
            + "\n  ".join(piezas) + "\n</svg>\n")


# ── lámina del recorrido completo ───────────────────────────────────────────────────────

def recorrido(presentacion: bool = False) -> str:
    """Las ocho estaciones en fila. Con `presentacion`, sin el título, la introducción ni las
    tres transformaciones de abajo, que en la diapositiva van como texto."""
    cw, gap, ch = 152, 14, 150
    x0, y0 = (16, 16) if presentacion else (40, 150)
    # El lienzo termina donde termina la última estación, más el mismo margen de la izquierda.
    ancho, alto = x0 + len(ESTACIONES) * (cw + gap) - gap + x0, 560
    piezas = [f'<rect width="{ancho}" height="{alto}" fill="#ffffff"/>']
    if not presentacion:
        piezas.append(_texto(40, 44, "El recorrido de un dato: del medidor al consumidor",
                             tam=22, color=TINTA, peso="600"))
    for i, ln in enumerate([] if presentacion else _envolver(
            "Se sigue una sola lectura —MED-0042, 25/09/2026 17:55, contador en 101,5 kWh— y se "
            "muestra en qué se convierte en cada paso. Es la misma de la demostración y del "
            "recorrido sobre Flink, así que los números coinciden con el documento y con la "
            "evidencia.", 128)):
        piezas.append(_texto(40, 70 + i * 18, ln, tam=13, color=SUAVE))

    for i, e in enumerate(ESTACIONES):
        x = x0 + i * (cw + gap)
        r, b, t_ = e.color
        piezas.append(f'<rect x="{x}" y="{y0}" width="{cw}" height="{ch}" rx="7" fill="{r}" '
                      f'stroke="{b}" stroke-width="1.6"/>')
        piezas.append(_texto(x + 12, y0 + 22, e.numero, tam=11, color=t_, peso="700"))
        for j, ln in enumerate(_envolver(e.breve, 19)):
            piezas.append(_texto(x + 12, y0 + 42 + j * 15, ln, tam=12, color=TINTA, peso="600"))
        for j, ln in enumerate(_envolver(e.forma, 21)):
            piezas.append(_texto(x + 12, y0 + 92 + j * 14, ln, tam=10.5, familia=MONO, color=SUAVE))
        if i < len(ESTACIONES) - 1:
            piezas.append(f'<line x1="{x + cw}" y1="{y0 + ch // 2}" x2="{x + cw + gap - 3}" '
                          f'y2="{y0 + ch // 2}" stroke="#2d3748" stroke-width="1.6" '
                          f'marker-end="url(#fl)"/>')
        if e.cuarentena:
            piezas.append(f'<line x1="{x + cw // 2}" y1="{y0 + ch}" x2="{x + cw // 2}" '
                          f'y2="{y0 + ch + 54}" stroke="#c53030" stroke-width="1.4" '
                          f'stroke-dasharray="4 3" marker-end="url(#flr)"/>')

    yq = y0 + ch + 54
    piezas.append(f'<rect x="{x0}" y="{yq}" width="{6 * (cw + gap) - gap}" height="52" rx="7" '
                  f'fill="{ALERTA[0]}" stroke="{ALERTA[1]}" stroke-width="1.6"'
                  ' stroke-dasharray="5 3"/>')
    piezas.append(_texto(x0 + 14, yq + 22, "medicion.cuarentena.v1 — nada se descarta en silencio",
                         tam=12, color="#742a2a", peso="700"))
    piezas.append(_texto(x0 + 14, yq + 40,
                         "json_invalido · sin_medidor_id · instante_invalido · instante_sin_huso · "
                         "sin_registro_util · contador_retrocede",
                         tam=11, familia=MONO, color=SUAVE))

    if presentacion:
        alto = yq + 52 + y0
        piezas[0] = f'<rect width="{ancho}" height="{alto}" fill="#ffffff"/>'
        return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {ancho} {alto}" '
                f'width="{ancho}" height="{alto}" font-family={TIPO!r}>\n  {_marcador()}\n  '
                + "\n  ".join(piezas) + "\n</svg>\n")

    yb = yq + 82
    piezas.append(_texto(40, yb,
                         "Las tres transformaciones del dato, que es de lo que trata esto",
                         tam=14, color=TINTA, peso="600"))
    for i, (a, b_) in enumerate([
        ("Contador acumulado  ->  consumo",
         "101,5 kWh no es lo consumido: es el total histórico. El "
         "consumo aparece al restar dos lecturas (estación 05)."),
        ("Intervalo  ->  celda de franja",
         "Un intervalo que cruza las 18:00 se reparte entre resto y "
         "punta. Si no hubo lectura sobre el borde, se interpola y se declara (06)."),
        ("Evento  ->  estado con clave", "La clave pasa de medidor_id a medidor|fecha|franja. Esa "
         "mudanza es la que hace idempotente la salida (07)."),
    ]):
        yy = yb + 24 + i * 34
        piezas.append(_texto(40, yy, a, tam=12.5, familia=MONO, color=PIPE_C[2], peso="700"))
        for j, ln in enumerate(_envolver(b_, 92)):
            piezas.append(_texto(400, yy + j * 15, ln, tam=12, color=SUAVE))

    alto = yb + 24 + 3 * 34 + 24
    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {ancho} {alto}" '
            f'width="{ancho}" height="{alto}" font-family={TIPO!r}>\n  {_marcador()}\n  '
            + "\n  ".join(piezas) + "\n</svg>\n")


def main() -> int:
    SALIDA.mkdir(parents=True, exist_ok=True)
    (SALIDA / "00-recorrido-completo.svg").write_text(recorrido(), encoding="utf-8")
    print("  00-recorrido-completo.svg")
    filas = ["| 00 | [`00-recorrido-completo.svg`](00-recorrido-completo.svg)"
             " | El recorrido entero en una lámina |"]
    for e in ESTACIONES:
        nombre = f"{e.numero}-{e.nombre}.svg"
        (SALIDA / nombre).write_text(detalle(e), encoding="utf-8")
        filas.append(f"| {e.numero} | [`{nombre}`]({nombre}) | {e.corto} — {e.titular} |")
        print(f"  {nombre}")

    # Las mismas láminas sin el texto explicativo, para usarlas como imagen en una
    # diapositiva: el título y el «por qué» van en la diapositiva, no dentro del dibujo.
    PRESENTACION.mkdir(exist_ok=True)
    (PRESENTACION / "00-recorrido-completo.svg").write_text(recorrido(presentacion=True),
                                                           encoding="utf-8")
    for e in ESTACIONES:
        (PRESENTACION / f"{e.numero}-{e.nombre}.svg").write_text(
            detalle(e, presentacion=True), encoding="utf-8")
    print(f"  presentacion/: las mismas {len(ESTACIONES) + 1}, solo el diagrama")

    (SALIDA / "LEEME.md").write_text(
        "# El recorrido de un dato, paso a paso\n\n"
        "Se sigue **una sola lectura** —`MED-0042`, 25/09/2026 a las 17:55, con el contador en "
        "101,5 kWh— desde que el concentrador la pide hasta que el consumidor la lee. Cada "
        "lámina muestra **qué entra, qué le pasa, qué sale y por qué**.\n\n"
        "Es la misma lectura de `pipeline.demostracion` y de `pipeline.extremo_a_extremo`, así "
        "que los números coinciden con el documento técnico y con la evidencia.\n\n"
        "```bash\nuv run python docs/diagramas/generar-recorrido.py\n```\n\n"
        "| # | Lámina | Qué muestra |\n|---|---|---|\n" + "\n".join(filas) + "\n\n"
        "## La salida de una lámina es la entrada de la siguiente\n\n"
        "No está escrito dos veces: el generador **deriva** la entrada de cada estación de la "
        "salida de\nla anterior, así que no pueden desincronizarse. Por eso el rótulo dice de "
        "dónde viene —«es la\nsalida de la 01»— y a dónde va. Lo que aparece en gris debajo de "
        "la salida es contexto que\n**no viaja**: la política de retención de un tópico, el "
        "estado que queda guardado, la celda\nque se reemitirá después.\n\n"
        "## Los payloads salen del código, no de la memoria\n\n"
        "El evento de entrada es la salida real de `Lectura.a_dict()`. El `Consumo` y las celdas "
        "son la salida real de pasar las tres lecturas por `DeduplicarLecturas`, "
        "`DiferenciarContador` y `CeldasVigentes` con `DirectRunner`. Cada lámina declara al pie "
        "de qué archivo y línea sale.\n\n"
        "## Para una presentación: `presentacion/`\n\n"
        "Las mismas nueve láminas **solo con el diagrama**: sin título, sin el «por qué» y sin "
        "la línea de fuente. En una diapositiva ese texto va en la diapositiva misma y en sus "
        "notas, donde se puede editar y se lee con la tipografía del resto; dentro de la imagen "
        "quedaría dos veces. Las genera el mismo script, así que no se desincronizan.\n\n"
        "## Y las otras tres vistas, que muestran otra cosa\n\n"
        "- [`../arquitectura.svg`](../arquitectura.svg): los componentes y los tópicos.\n"
        "- [`../pipeline-dag.svg`](../pipeline-dag.svg): la topología real, dibujada por Beam "
        "desde el código.\n"
        "- [`../etapas/`](../etapas/LEEME.md): el mecanismo de cada etapa, con su estado y sus "
        "temporizadores.\n",
        encoding="utf-8")
    print(f"  índice en {SALIDA.name}/LEEME.md · {len(ESTACIONES)} estaciones")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
