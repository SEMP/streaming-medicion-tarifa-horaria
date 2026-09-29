# Pendientes y bitácora

**Dónde mirar qué falta decidir, quién lo decide y a quién bloquea** — y qué se decidió desde
la última vez que hiciste `git pull`.

> ## La regla que hace que esto no se pudra
>
> **Este archivo lleva punteros, dueño y estado. Nunca el razonamiento.**
>
> El porqué de cada decisión vive en [`decisiones-de-diseno.md`](decisiones-de-diseno.md) y en
> [`contratos.md`](contratos.md), y en ningún otro lado. Si acá se empieza a explicar *por qué*
> algo se decidió, en dos días los dos textos dicen cosas distintas y nadie sabe cuál manda —
> que es exactamente lo que nos pasó con el borrador de interfaces.
>
> Una fila de este archivo no debería pasar de tres líneas.

---

## 1. Decisiones abiertas

| # | Qué hay que decidir | Decide | Bloquea a | Detalle |
|---|---|---|---|---|
| ~~**P3**~~ | ~~Confirmar 4 particiones contra el `docker-compose`~~ · Listo **coinciden** | Sergio | — | [infra](../infra/README.md) |
| ~~**P4**~~ | ~~La «regla 1 — alineación a la grilla» quedó sin efecto~~ · Listo reescrita: con readout no hay grilla, los intervalos **siempre** cruzan bordes | Sergio | — | [config](../config/franjas.example.toml) |
| ~~**P5**~~ | ~~Cómo se representa `CalendarioTarifario`~~ · Listo tabla de 1440 posiciones precomputada al cargar: construirla **es** la validación de cobertura | Sergio | — | `pipeline/franjas.py` |
| ~~**P6**~~ | ~~Si `fecha_y_franja` valida el timestamp~~ · Listo **no**: un naive es error de programación y levanta excepción. La validación de datos va aguas arriba | Sergio | — | `pipeline/franjas.py` |
| **P8** | Con qué se lista Daniel en la sección 8. Su plan original quedó superado: el área se implementó entre el 22 y el 27/09 → [puesta al día](planes/daniel-puesta-al-dia.md) | Los tres | La sección 8 | [planes/daniel-puesta-al-dia](planes/daniel-puesta-al-dia.md) |

**P1 y P2 son las que tienen consecuencia económica**: las dos deciden a qué franja se atribuye
energía que se factura a precio distinto. Las demás son de coordinación.

## 2. Acciones pendientes — no son decisiones, son cosas por hacer

| Quién | Qué | Por qué importa |
|---|---|---|
| ~~Sergio~~ | ~~`naturaleza` en `Registro.a_dict()`~~ · Listo hecho, más `instante` opcional y los headers de Kafka | — |
| ~~Sergio~~ | ~~`infra/` vacío~~ · Listo **stack levantado y verificado end-to-end** | — |
| ~~Sergio~~ | ~~Reescribir la regla 1 de `config/franjas.example.toml`~~ · Listo ya estaba hecha: la regla 1 es «cobertura exacta del día» y el archivo explica qué dejó de exigir. La fila contradecía a P4 en la sección 1 | — |
| ~~Daniel~~ | ~~Puede empezar las pruebas~~ · Listo 23 de franjas y **8 con `TestStream`**: duplicado, desorden, reseteo y el orden dedup→diferenciación | — |
| ~~Clara~~ | ~~Esqueleto del pipeline con fuente conmutable~~ · Listo **superada**: el pipeline está construido y corre end-to-end sobre Flink, leyendo de Kafka | — |
| Los tres | **Documento técnico**: el esqueleto está en [`tecnico/`](tecnico/documento.md) con dueño por sección y `Atención: PENDIENTE` donde falta. Se arma con `./docs/tecnico/armar-pdf.sh` | Queda **1 de 9**: la sección 8, contribuciones |
| ~~Sergio~~ | ~~Diagrama de arquitectura~~ · Listo `docs/diagramas/arquitectura.svg`, incrustado en el documento en página apaisada | — |
| ~~Clara~~ | ~~Revisar las secciones 3 y 4 del documento técnico~~ · Listo revisadas el 29/09, marcas de borrador retiradas | — |
| ~~Clara~~ | ~~Aviso de colisión con `pipeline.demostracion`~~ · Listo **no hubo colisión**: no llegué a escribir la cadena, la implementó Sergio entera | — |
| Los tres | **Sección 8**: contribuciones de cada uno | `git shortlog -sn --no-merges` lo respalda |
| Los tres | **Video** — guion escrito en [`guion-video.md`](guion-video.md): 7 escenas con sus comandos y quién habla. Falta grabarlo. Atención: la cátedra **no fija duración**: pide «video breve» y nada más | Lo único que no se puede dejar para el último día |

## 3. Bitácora — qué se decidió y cuándo

Lo más reciente arriba. Una línea por cambio, con el commit para ir al detalle.

| Fecha | Commit | Qué cambió |
|---|---|---|
| 29/09 | — | **P9 cerrado: el equipo es de tres.** No se suma un cuarto integrante |
| 29/09 | `33fbe76` | **PDF del documento técnico regenerado**, que había quedado tres commits atrás del `.md`. Atención: requiere **pandoc y las fuentes Liberation**: sin ellas Typst cae a otra tipografía en los títulos y el mismo fuente da un PDF distinto según quién compile. Verificar con `pdffonts` |
| 29/09 | `4290862` | **Secciones 3 y 4 del documento técnico revisadas** y marcas de borrador retiradas. Tres correcciones: sección 3.6 y sección 4.4 se contradecían sobre los panes, sección 3.6 contaba el borrador anterior, y sección 4.6 presentaba `error_atribucion_pct` como campo del mensaje cuando lo calcula el consumidor |
| 29/09 | `68ff16f` | **El contrato de salida ahora describe la salida.** La sección 2 declaraba 20 campos y el pipeline emite 8; está el JSON real, y se dice que la identidad viaja en la clave del mensaje. Cae `es_provisional`, que contradecía a sección 2.4. sección 2.3 resuelve la divergencia del `null` **a favor de la implementación**: conservar los intervalos sanos. Salen las notas de bitácora de la sección 1.5, sección 2.1, sección 2.2 y sección 2.3 |
| 29/09 | `bea30b8` | **`contratos.md` sección 2.4 reescrita**: el pipeline no tiene triggers y no puede tenerlos, porque no hay agregación en el grafo. Corrige además el argumento del *upsert*: la idempotencia sale de emitir el valor absoluto de la celda, no del modo de acumulación |
| 29/09 | — | **Para Clara:** tu hallazgo del `RUBRICA.md` eran **tres** afirmaciones sobre triggers, no una — también decía en dos lugares que el proyecto hace «triggers avanzados» como extensión opcional. Corregidas y pusheadas al repo de la materia, que es privado. Es justo el patrón que estás persiguiendo: un cambio en el código dejó tres documentos diciendo lo de antes |
| 29/09 | — | **Daniel validó el recorrido en otra máquina** (WSL2 + Docker Desktop): mismos números. Encontró que `uv run pytest` da *segmentation fault* en su host → nuevo perfil `pruebas` que corre la suite dentro del contenedor |
| 28/09 | — | **Decisión 13 aplicada**: umbral en 90 min, celda `indeterminada` con `minutos_indeterminados`. Verificado sobre Flink. 89 pruebas. Listo La divergencia con `contratos.md` sección 2.3 quedó resuelta el 29/09 **a favor de la implementación** |
| 27/09 | — | **Guion del video** en [`guion-video.md`](guion-video.md) |
| 27/09 | — | **P7 cerrado de hecho**: el recorrido corre sobre Flink y las pruebas con `DirectRunner`, las dos cosas verificadas |
| 27/09 | — | **P1 y P2 cerrados con datos**: umbral de 90 min, elegido donde la distribución se parte en dos → [decisión 13](decisiones-de-diseno.md). **Atención — Falta que el equipo lo ratifique y aplicarlo en el código** |
| 26/09 | — | **Secciones 3 y 4 del documento** escritas a partir de `contratos.md`, marcadas como borrador para Clara. Quedan 2 pendientes de 9 |
| 26/09 | — | **Corregido el 48 % de `contratos.md` sección 1.5**: contradecía el 66 % de la tabla resumen. Medido, es 63,2 % |
| 26/09 | — | **La cadena corre sobre Flink y da el mismo resultado que con `DirectRunner`**: `pipeline.extremo_a_extremo`, dos corridas idénticas, cuarentena vacía |
| 26/09 | — | **Cadena completa conectada a Kafka** (`cadena.py`): parseo, cuarentena por motivo, ventana alineada al día local, y la agregación por celda con estado. 86 pruebas |
| 26/09 | — | **Segunda trampa del mismo error**: encadenar dos agregaciones bajo `ACCUMULATING` cuenta doble. Ver [decisión 12](decisiones-de-diseno.md) |
| 25/09 | — | **Demostración narrada** de los tres escenarios (`pipeline.demostracion`), **reparto de un intervalo entre franjas** (`repartir_por_franja`, era de Daniel) y **sección 6** del documento técnico. 76 pruebas |
| 25/09 | — | **Una lectura tardía duplicaba el consumo de su intervalo.** El *upsert* no retiraba el intervalo superado. Corregido con `CeldasVigentes` → [decisión 12](decisiones-de-diseno.md). Listo Reflejado en `contratos.md` sección 2.1 el 29/09 |
| 24/09 | — | **Diagrama de arquitectura** en SVG a mano, y **secciones 1, 2, 5 y 7 del documento técnico**. Quedan 5 pendientes de 9 |
| 23/09 | — | **Deduplicación y diferenciación con estado**, y sus 8 pruebas con `TestStream`. La diferenciación guarda las lecturas en lugar de restar contra la última: una tardía que cae en el medio parte el intervalo y emite las dos mitades |
| 22/09 | — | **Esqueleto del documento técnico** en `tecnico/`, con la cadena pandoc → Typst ya funcionando y las 8 secciones con que organizamos los cinco contenidos que pide el enunciado, cada una con dueño |
| 22/09 | — | **Franjas implementadas** (era de Daniel): `cargar_calendario` con validación de cobertura, y `fecha_y_franja`. Cierra P4, P5 y P6 |
| 22/09 | `e31d58e` | **El simulador no era determinista entre procesos**: las semillas se derivaban con `hash()` de cadenas, que Python aleatoriza por ejecución. Corregido con SHA-256 y dos pruebas. Las cifras de `calidad` de [contratos](contratos.md) se remidieron: `checksum_no_verificado` es **66 %**, no 48 % |
| 22/09 | `e31d58e` | El simulador emite `naturaleza` y `instante` por registro, y el publicador manda los tres headers de Kafka. Cierra los huecos entre el contrato y lo que se producía |
| 22/09 | `40065a2` | **Infraestructura lista y verificada**: Kafka + Flink + job server, con `KafkaIO` andando. La prueba de humo recorre simulador → Kafka → Beam → Kafka. Ver [infra](../infra/README.md) |
| 22/09 | — | **P10 cerrado: el diagrama es SVG escrito a mano**, con la convención de la Tarea 1 de Sergio. El detalle está en [README de docs](README.md) sección 4 |
| 22/09 | `f060d8b` | **Los dos contratos quedaron cerrados.** Se resolvieron los 10 ítems que estaban abiertos: clave y particiones, nombres de tópicos y regla de versionado, cuarentena en tópico propio, qué gana ante un duplicado divergente, obligatoriedad de registros, `instante` opcional y `naturaleza` obligatoria; y del lado de la salida, la política temporal completa, el tablero leyendo el tópico directo y `compact,delete` |
| 22/09 | `f060d8b` | **Tres correcciones al borrador de interfaces**, las tres por arrastre del modelo de perfil de carga: los valores de `calidad` son los del readout (`ok`, `checksum_no_verificado`, `truncada`); `intervalos_esperados` se reemplaza por `cobertura_pct` porque sin grilla no existe un número de intervalos esperados; y la justificación por alineación a la grilla ya no rige |
| 21/09 | `9df7c0e` `a81513e` | El modelo de tiempos del simulador se recalibró contra mediciones de comunicación real y contra un despliegue de 410.000 pedidos en 7 días |
| 21/09 | `1b41c98` | El presupuesto de reintentos es tiempo, no un contador |
| 20/09 | `c03a4a2` | Simulador: modelo de consumo, parque, agenda de pedidos y fallas |
| 20/09 | `2972443` | **Rondas continuas e interpolación.** Revierte «pedir en el borde»: el bus RS-485 está ocupado con los demás medidores de la cabina, así que se lee en rondas y el consumo de cada franja se interpola. La interpolación pasa a ser obligatoria, no una optimización |
| 20/09 | `690de5a` `f2cea0c` | El simulador genera también el escenario con dispositivo dedicado, que convierte el error de atribución de estimación en medición |
| 20/09 | `81859a6` | Licencia **MIT** (se había elegido Apache-2.0 y se cambió por simplicidad) |
| 20/09 | `e328d5a` | **Decisión 10 cerrada:** el medidor entrega contador acumulado (`15.8.0`); el consumo sale de restar |
| 20/09 | `89e17c7` | **El modelo pasa a ser solo readout**, sin perfil de carga. Cae la defensa contra relojes de medidor: el instante lo pone el concentrador |

## 4. Cómo se usa

**Al empezar una sesión:** `git pull --ff-only` y mirar sección 3. Si hay filas nuevas desde la última
vez, algo que dabas por sentado puede haber cambiado.

**Cuando encontrás algo sin definir** —un umbral, un formato, una regla—: agregá una fila en la sección 1
con un id `P<n>` nuevo, decí quién lo decide y a quién bloquea, y seguí trabajando con un
supuesto explícito. No lo decidas en silencio.

**Cuando cerrás una decisión:**

1. Escribí el **razonamiento** en `decisiones-de-diseno.md` (o en `contratos.md` si es de
   contrato), con lo que se resigna. Ese es el texto que después se copia al documento técnico.
2. Sacá la fila de la sección 1 y poné **una línea** en la sección 3, con el hash del commit.
3. Si la decisión invalida algo escrito antes, **corregilo en el mismo commit**. Las tres
   correcciones del 22/09 existieron porque esto no se hizo en su momento.

**Citá el id en el commit** (`P3: fijar 4 particiones en el compose`). Así el historial dice
qué pendiente cerró cada cambio.
