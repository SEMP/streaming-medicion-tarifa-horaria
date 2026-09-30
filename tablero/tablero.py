"""Tablero del consumo por franja: el último eslabón del ciclo del dato.

El contrato de salida (`docs/contratos.md` sección 2) describe **dos lectores** del tópico
derivado con necesidades opuestas: este tablero, que mira todas las revisiones y muestra un
valor que cambia, y la facturación, que lee una sola vez pasado el horizonte de convergencia.
Acá está el primero.

Lo que demuestra, y por eso existe más allá de ser bonito:

1. **El consumidor hace *upsert*, nunca suma.** La clave `medidor|fecha|franja` identifica una
   celda; cada mensaje nuevo con esa clave reemplaza al anterior. Sumar daría el doble.
2. **Releer desde el principio converge a la misma vista.** Cada actualización vuelve a leer el
   tópico entero desde el offset 0 con una asignación nueva. Es caro a propósito: es la
   propiedad que el proyecto declara, ejecutándose a la vista.
3. **El error de atribución lo calcula el consumidor.** El mensaje trae el numerador
   —`separacion_maxima_minutos`—; el denominador es la duración de la franja, que vive en el
   calendario tarifario y no en el evento (`contratos.md` sección 2.3).

    uv run --with marimo marimo run tablero/tablero.py      # para verlo
    uv run --with marimo marimo edit tablero/tablero.py     # para tocarlo
"""

import marimo

__generated_with = "0.25.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _(mo):
    mo.md("""
    # Consumo por franja tarifaria

    Lee el tópico derivado y reconstruye la vista actual haciendo *upsert* por clave.
    **No suma**: cada mensaje reemplaza a la celda que identifica.
    """)
    return


@app.cell
def _():
    import json
    import os
    import uuid
    from collections import Counter
    from datetime import datetime

    from confluent_kafka import Consumer, KafkaException, TopicPartition
    from inyector import ACCIONES, enviar
    from pipeline.franjas import cargar_calendario

    SERVIDORES = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    TOPICO_CONSUMO = os.environ.get("TOPICO_CONSUMO", "medicion.consumo-franja.v1")
    TOPICO_CUARENTENA = os.environ.get("TOPICO_CUARENTENA", "medicion.cuarentena.v1")
    # El tópico **crudo**: es a donde el inyector publica, no al derivado que el tablero lee.
    TOPICO_LECTURAS = os.environ.get("TOPICO_LECTURAS", "medicion.lecturas.v1")
    CONFIG_FRANJAS = os.environ.get("CONFIG_FRANJAS", "config/franjas.example.toml")

    calendario = cargar_calendario(CONFIG_FRANJAS)
    return (
        ACCIONES,
        Consumer,
        Counter,
        KafkaException,
        SERVIDORES,
        TOPICO_CONSUMO,
        TOPICO_CUARENTENA,
        TOPICO_LECTURAS,
        TopicPartition,
        calendario,
        datetime,
        enviar,
        json,
        uuid,
    )


@app.cell
def _(Consumer, KafkaException, SERVIDORES, TopicPartition, uuid):
    def leer_desde_el_principio(topico: str, espera_s: float = 2.0) -> list[tuple[str, str]]:
        """Devuelve todos los mensajes del tópico, en orden, releyendo desde el offset 0.

        Se asignan las particiones a mano en lugar de suscribirse, y con un `group.id` nuevo
        cada vez: así no hay offsets confirmados que recordar y **cada lectura arranca del
        principio**. Es exactamente el reproceso que el proyecto afirma que converge, y que
        acá se ejecuta en cada actualización del tablero.

        `espera_s` es cuánto se tolera sin recibir nada antes de dar por leído el tópico.

        Si el broker no responde devuelve lista vacía, **no** una excepción: con el stack
        apagado el tablero tiene que mostrar el mensaje de «tópico vacío» con las
        instrucciones para levantarlo, que es justo el orden en que alguien lo abre por
        primera vez. Un traceback ahí no informa nada que se pueda usar.
        """
        consumidor = Consumer(
            {
                "bootstrap.servers": SERVIDORES,
                "group.id": f"tablero-{uuid.uuid4()}",
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )
        try:
            try:
                metadatos = consumidor.list_topics(topico, timeout=10)
            except KafkaException:
                return []  # el broker no está: el tópico, para el caso, está vacío
            if topico not in metadatos.topics or metadatos.topics[topico].error:
                return []
            particiones = [
                TopicPartition(topico, p, 0) for p in metadatos.topics[topico].partitions
            ]
            consumidor.assign(particiones)

            mensajes: list[tuple[str, str]] = []
            vacios = 0
            while vacios < espera_s / 0.2:
                mensaje = consumidor.poll(0.2)
                if mensaje is None:
                    vacios += 1
                    continue
                if mensaje.error():
                    continue
                vacios = 0
                clave = (mensaje.key() or b"").decode("utf-8", "replace")
                valor = (mensaje.value() or b"").decode("utf-8", "replace")
                mensajes.append((clave, valor))
            return mensajes
        finally:
            consumidor.close()

    return (leer_desde_el_principio,)


@app.cell
def _(mo):
    actualizar = mo.ui.refresh(
        options=["5s", "15s", "30s", "1m"],
        default_interval="15s",
        label="Releer el tópico cada",
    )
    actualizar
    return (actualizar,)


@app.cell
def _(mo):
    # El estado guarda la última lectura inyectada, y es lo que permite encadenar: el
    # duplicado repite la anterior, la tardía se ubica antes que ella, el hueco salta desde
    # ella. Sin esto cada botón sería un hecho suelto.
    obtener_ultima, fijar_ultima = mo.state(None)
    return fijar_ultima, obtener_ultima


@app.cell
def _(
    ACCIONES,
    SERVIDORES,
    TOPICO_LECTURAS,
    calendario,
    datetime,
    enviar,
    fijar_ultima,
    mo,
    obtener_ultima,
):
    def _accionar(accion):
        def _publicar(_):
            lectura = accion.construir(
                obtener_ultima(), ahora=datetime.now(calendario.zona)
            )
            enviar(lectura, servidores=SERVIDORES, topico=TOPICO_LECTURAS)
            fijar_ultima(lectura)

        return _publicar

    botones = [
        mo.ui.button(label=a.titulo, on_click=_accionar(a), tooltip=a.espera)
        for a in ACCIONES
    ]

    mo.vstack(
        [
            mo.md("""
## Pedir una irregularidad

Publica al tópico **crudo** una lectura del medidor `MED-DEMO-001`, y el efecto aparece abajo.
Requiere el **pipeline corriendo y el simulador apagado**: si el simulador está publicando,
las 21.000 lecturas de una corrida tapan lo inyectado.
"""),
            mo.hstack(botones, justify="start", gap=0.5, wrap=True),
        ]
    )
    return (botones,)


@app.cell
def _(
    Counter,
    TOPICO_CONSUMO,
    TOPICO_CUARENTENA,
    actualizar,
    json,
    leer_desde_el_principio,
    obtener_ultima,
):
    actualizar  # la dependencia es lo que hace que esta celda se recalcule sola
    obtener_ultima()  # y esta hace que se rehaga al instante cuando se inyecta algo

    crudos = leer_desde_el_principio(TOPICO_CONSUMO)

    # **Acá está el upsert.** Un dict indexado por la clave del mensaje: el último gana.
    # Si esto fuera una suma, cada revisión de una celda inflaría el total — que es
    # justamente el error que el contrato evita al emitir el valor absoluto de la celda.
    #
    # Y se valida el contrato de salida antes de aceptar nada. No es paranoia: la prueba de
    # humo hace *passthrough* y escribe lecturas crudas —claveadas por medidor, sin fecha ni
    # franja— en este mismo tópico. Mezclarlas con las celdas daría filas vacías y totales sin
    # sentido. Nada se descarta en silencio: lo ajeno se cuenta y se muestra aparte.
    celdas: dict[str, dict] = {}
    ajenos = 0
    for clave, valor in crudos:
        try:
            cuerpo = json.loads(valor)
        except json.JSONDecodeError:
            ajenos += 1
            continue
        if len(clave.split("|")) != 3 or "energia_kwh" not in cuerpo:
            ajenos += 1
            continue
        celdas[clave] = cuerpo

    motivos = Counter()
    for _clave, valor in leer_desde_el_principio(TOPICO_CUARENTENA):
        try:
            motivos[json.loads(valor).get("motivo", "sin motivo")] += 1
        except json.JSONDecodeError:
            motivos["json ilegible"] += 1

    revisiones = len(crudos)
    return ajenos, celdas, motivos, revisiones


@app.cell
def _(ajenos, calendario, celdas: dict[str, dict], mo, revisiones):
    def _filas():
        filas = []
        for clave in sorted(celdas):
            medidor, fecha, franja = (clave.split("|") + ["", "", ""])[:3]
            c = celdas[clave]
            separacion = c.get("separacion_maxima_minutos", 0.0)
            # `duracion_minutos` cuenta minutos sobre una tabla: para un nombre desconocido
            # devuelve 0 y no levanta. El `if duracion` de abajo ya cubre ese caso.
            duracion = calendario.duracion_minutos(franja)
            # El denominador sale del calendario, no del mensaje: sección 2.3 del contrato.
            error = (separacion / duracion * 100) if duracion else float("nan")

            if c.get("indeterminada"):
                calidad = "indeterminada"
            elif c.get("interpolada"):
                calidad = "interpolada"
            else:
                calidad = "medida"

            filas.append(
                {
                    "medidor": medidor,
                    "fecha": fecha,
                    "franja": franja,
                    "kWh": round(c.get("energia_kwh", 0.0), 3),
                    "calidad": calidad,
                    # Es la **cota**, no el error: la acota la separación entre las dos
                    # lecturas que rodean el borde (`contratos.md` sección 2.3). El error
                    # real solo se conoce cuando llega una lectura sobre el borde — en la
                    # demostración resultó ser 0,100 kWh.
                    "error máx. atribución": f"{error:.1f} %" if duracion else "—",
                    "separación máx (min)": separacion,
                    "min cubiertos": c.get("minutos_cubiertos", 0.0),
                    "min indeterminados": c.get("minutos_indeterminados", 0.0),
                    "intervalos": c.get("intervalos_usados", 0),
                    "cabina": c.get("cabina_id", ""),
                }
            )
        return filas

    filas = _filas()

    _aviso = (
        ""
        if not ajenos
        else (
            f"\n\n> **{ajenos} mensajes no siguen el contrato de salida** y quedaron fuera. "
            "Suele ser la prueba de humo, que escribe lecturas crudas en este tópico por "
            "*passthrough*. Para una demostración limpia conviene arrancar con el tópico "
            "vacío: `docker compose -f infra/docker-compose.yml down -v`."
        )
    )

    mo.md(
        f"**{len(celdas)} celdas** reconstruidas a partir de **{revisiones} mensajes**. "
        "La diferencia son las revisiones sucesivas: cada lectura tardía que corrige una celda "
        "publica un mensaje nuevo con la misma clave, y el *upsert* se queda con el último."
        + _aviso
    )
    return (filas,)


@app.cell
def _(filas, mo):
    mo.ui.table(filas, selection=None) if filas else mo.md(
        """
        > **El tópico está vacío.**
        >
        > Levantá el stack y producí datos antes de mirar acá:
        >
        > ```bash
        > docker compose -f infra/docker-compose.yml --profile e2e run --rm -T extremo-a-extremo
        > ```
        """
    )
    return


@app.cell
def _(filas, mo):
    def _resumen():
        if not filas:
            return mo.md("")
        total = sum(f["kWh"] for f in filas)
        indeterminadas = sum(1 for f in filas if f["calidad"] == "indeterminada")
        interpoladas = sum(1 for f in filas if f["calidad"] == "interpolada")
        medidas = sum(1 for f in filas if f["calidad"] == "medida")
        peor = max((f["separación máx (min)"] for f in filas), default=0)

        por_franja: dict[str, float] = {}
        for f in filas:
            por_franja[f["franja"]] = por_franja.get(f["franja"], 0.0) + f["kWh"]
        tope = max(por_franja.values(), default=1) or 1
        barras = "\n".join(
            f"| {nombre} | {energia:8.3f} | {'█' * max(1, round(energia / tope * 28))} |"
            for nombre, energia in sorted(por_franja.items(), key=lambda kv: -kv[1])
        )

        return mo.md(
            f"""
            ## Energía por franja

            | franja | kWh | |
            |---|---:|---|
            {barras}
            | **total** | **{total:.3f}** | |

            ## Cuánto se puede confiar

            **{medidas}** medidas · **{interpoladas}** interpoladas · **{indeterminadas}**
            indeterminadas. Separación máxima observada: **{peor:.1f} min**.

            Una celda *medida* tuvo lecturas sobre los bordes de su franja. Una *interpolada*
            no, y su energía se repartió suponiendo potencia constante — de ahí el error de
            atribución. Una *indeterminada* tuvo un cruce que superó el umbral tolerado y esa
            parte **no se repartió**: el dato se declara incompleto en lugar de inventarse.
            """
        )

    _resumen()
    return


@app.cell
def _(mo, motivos):
    def _cuarentena():
        if not motivos:
            return mo.md(
                "## Cuarentena\n\n**Vacía.** Nada se perdió por el camino."
            )
        filas_q = "\n".join(
            f"| `{motivo}` | {cuantos} |" for motivo, cuantos in motivos.most_common()
        )
        return mo.md(
            f"""
            ## Cuarentena

            | motivo | mensajes |
            |---|---:|
            {filas_q}

            Nada se descarta en silencio: lo que el pipeline no puede procesar sale por su
            propio tópico, con el motivo. Su volumen es una señal operativa — si sube, el
            problema está en la red o en los equipos, no en el cálculo.
            """
        )

    _cuarentena()
    return


if __name__ == "__main__":
    app.run()
