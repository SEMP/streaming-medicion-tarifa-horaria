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
| **P1** | Umbral de desvío tolerado respecto del borde de franja, y su gemelo: el límite de separación por encima del cual no se interpola | Daniel, con Clara | Marcar `indeterminado`; **facturar** | [decisiones §3](decisiones-de-diseno.md) y [§5](decisiones-de-diseno.md) |
| **P2** | Qué se hace con los dos intervalos que quedan indeterminados cuando falta una lectura: marcarlos, o imputar el consumo combinado al bloque | Clara y Sergio | La agregación | [decisiones §10](decisiones-de-diseno.md) |
| ~~**P3**~~ | ~~Confirmar 4 particiones contra el `docker-compose`~~ · ✅ **coinciden** | Sergio | — | [infra](../infra/README.md) |
| ~~**P4**~~ | ~~La «regla 1 — alineación a la grilla» quedó sin efecto~~ · ✅ reescrita: con readout no hay grilla, los intervalos **siempre** cruzan bordes | Sergio | — | [config](../config/franjas.example.toml) |
| ~~**P5**~~ | ~~Cómo se representa `CalendarioTarifario`~~ · ✅ tabla de 1440 posiciones precomputada al cargar: construirla **es** la validación de cobertura | Sergio | — | `pipeline/franjas.py` |
| ~~**P6**~~ | ~~Si `fecha_y_franja` valida el timestamp~~ · ✅ **no**: un naive es error de programación y levanta excepción. La validación de datos va aguas arriba | Sergio | — | `pipeline/franjas.py` |
| **P7** | **Runner**: la propuesta es Flink con el stack de la clase 7 para la demo end-to-end, y `DirectRunner` con `TestStream` para las pruebas | Los tres | Clara: cómo escribe el pipeline | — |
| **P8** | Confirmación de Daniel sobre su parte del reparto: no estuvo en la reunión del 20/09 | Daniel | Planificación | [planes/README](planes/README.md) |
| **P9** | Cuarto integrante, si se suma alguien | Los tres | Nada | — |

**P1 y P2 son las que tienen consecuencia económica**: las dos deciden a qué franja se atribuye
energía que se factura a precio distinto. Las demás son de coordinación.

## 2. Acciones pendientes — no son decisiones, son cosas por hacer

| Quién | Qué | Por qué importa |
|---|---|---|
| ~~Sergio~~ | ~~`naturaleza` en `Registro.a_dict()`~~ · ✅ hecho, más `instante` opcional y los headers de Kafka | — |
| ~~Sergio~~ | ~~`infra/` vacío~~ · ✅ **stack levantado y verificado end-to-end** | — |
| Sergio | Reescribir la regla 1 de `config/franjas.example.toml` (era P4, de Daniel) | Si se reparten el trabajo de Daniel, alguien tiene que tomarla |
| ~~Daniel~~ | ~~Puede empezar las pruebas~~ · ✅ 23 pruebas de franjas, puras, sin infraestructura | — |
| Clara | Esqueleto del pipeline con fuente conmutable (`jsonl` \| `kafka`) | No depende de `infra/`: se construye contra `datos/*.jsonl` |
| Los tres | **Documento técnico**: el esqueleto está en [`tecnico/`](tecnico/documento.md) con dueño por sección y `⚠️ PENDIENTE` donde falta. Se arma con `./docs/tecnico/armar-pdf.sh` | 9 secciones sin escribir. Varias son casi transcripción de lo ya documentado |
| Sergio | Diagrama de arquitectura, SVG a mano, en `docs/diagramas/` | Lo necesita la sección 2 del documento |
| Los tres | **Video** | Nadie empezó. Conviene grabarlo antes del último día |

## 3. Bitácora — qué se decidió y cuándo

Lo más reciente arriba. Una línea por cambio, con el commit para ir al detalle.

| Fecha | Commit | Qué cambió |
|---|---|---|
| 22/09 | — | **Esqueleto del documento técnico** en `tecnico/`, con la cadena pandoc → Typst ya funcionando y las 8 secciones que pide el enunciado, cada una con dueño |
| 22/09 | — | **Franjas implementadas** (era de Daniel): `cargar_calendario` con validación de cobertura, y `fecha_y_franja`. Cierra P4, P5 y P6 |
| 22/09 | `e31d58e` | **El simulador no era determinista entre procesos**: las semillas se derivaban con `hash()` de cadenas, que Python aleatoriza por ejecución. Corregido con SHA-256 y dos pruebas. Las cifras de `calidad` de [contratos](contratos.md) se remidieron: `checksum_no_verificado` es **66 %**, no 48 % |
| 22/09 | `e31d58e` | El simulador emite `naturaleza` y `instante` por registro, y el publicador manda los tres headers de Kafka. Cierra los huecos entre el contrato y lo que se producía |
| 22/09 | `40065a2` | **Infraestructura lista y verificada**: Kafka + Flink + job server, con `KafkaIO` andando. La prueba de humo recorre simulador → Kafka → Beam → Kafka. Ver [infra](../infra/README.md) |
| 22/09 | — | **P10 cerrado: el diagrama es SVG escrito a mano**, con la convención de la Tarea 1 de Sergio. El detalle está en [README de docs](README.md) §4 |
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

**Al empezar una sesión:** `git pull --ff-only` y mirar §3. Si hay filas nuevas desde la última
vez, algo que dabas por sentado puede haber cambiado.

**Cuando encontrás algo sin definir** —un umbral, un formato, una regla—: agregá una fila en §1
con un id `P<n>` nuevo, decí quién lo decide y a quién bloquea, y seguí trabajando con un
supuesto explícito. No lo decidas en silencio.

**Cuando cerrás una decisión:**

1. Escribí el **razonamiento** en `decisiones-de-diseno.md` (o en `contratos.md` si es de
   contrato), con lo que se resigna. Ese es el texto que después se copia al documento técnico.
2. Sacá la fila de §1 y poné **una línea** en §3, con el hash del commit.
3. Si la decisión invalida algo escrito antes, **corregilo en el mismo commit**. Las tres
   correcciones del 22/09 existieron porque esto no se hizo en su momento.

**Citá el id en el commit** (`P3: fijar 4 particiones en el compose`). Así el historial dice
qué pendiente cerró cada cambio.
