# Propuesta: las tres interfaces

> ⚠️ **Es una propuesta, no una decisión.** Está escrita para que sea más rápido corregirla
> que redactarla de cero entre varios. Cada interfaz tiene al final **lo que queda por
> decidir**, que le corresponde a su dueño.
>
> Cuando se acuerden, esto se mueve a `docs/contratos.md` y deja de ser propuesta.

---

## 1. Contrato de evento de entrada — dueña: Clara

Tópico `medicion.lecturas.v1`, clave de particionamiento `medidor_id`.

> ⚠️ **Corregido el 20/09** tras cerrar la decisión 10. La versión anterior daba por hecho
> que el evento traía el consumo de un bloque; con contador acumulado eso no es así, y
> obliga a distinguir dos registros en dos etapas. Ver la nota al final de esta sección.

```json
{
  "schema_version": 1,
  "event_id": "9f2c41a70b8d3e56",
  "medidor_id": "MED-000457",
  "lote_id": "LOTE-2026-09-20-000123",
  "secuencia": 42,
  "instante_lectura": "2026-09-20T18:00:00-03:00",
  "registros": { "15.8.0": 12843.271 },
  "calidad": "ok",
  "reportado_at": "2026-09-21T02:14:07.220-03:00"
}
```

Headers de Kafka: `schema_version`, `content_type`, `trace_id` — permiten a un consumidor
enrutar o rechazar sin deserializar el cuerpo.

### Qué hace cada campo, y por qué está

| Campo | Para qué |
|---|---|
| `event_id` | Identidad estable. `sha256("<medidor_id>\|<instante_lectura>")[:16]` — **determinista, no aleatorio**: un reintento del productor produce el mismo id, y por eso el duplicado es reconocible |
| `medidor_id` | Clave de particionamiento: manda todas las lecturas de un medidor a la misma partición y preserva su orden |
| `lote_id` | La descarga en la que vino. Es lo que permite demostrar el duplicado por reintento y rastrear un lote tardío completo |
| `secuencia` | Índice del registro dentro de la curva de carga del medidor. Da identidad cuando el timestamp falta |
| `instante_lectura` | **El tiempo de evento**: el momento en que se capturó el valor del contador. ISO-8601 con offset (decisión 4). No es un período — ver la nota de abajo |
| `registros` | Mapa de código OBIS → valor, con **todos** los registros que trajo esa lectura. Hoy solo `15.8.0`, cuyo valor es el del contador en kWh: acumulado, solo sube. Ver la nota de abajo |
| `calidad` | `ok` \| `estimado` \| `sin_sincronizar`. Los medidores reales marcan sus lecturas; permite decidir sin adivinar |
| `reportado_at` | Cuándo lo emitió el medidor. Contra `inicio_intervalo` da el desfase, que es el insumo de la regla de Daniel |

### Los casos inválidos son inválidos *respecto de este contrato*

El simulador los produce a propósito; la validación los manda a cuarentena.

| Caso | Cómo se ve |
|---|---|
| Sin timestamp | `instante_lectura` ausente o `null` |
| Sin offset | `"2026-09-20T18:00:00"` — hay hora, no se sabe de qué huso |
| Reloj desfasado | formato válido, valor corrido minutos u horas |
| Reloj absurdo | `"1970-01-01T00:00:00-03:00"` o una fecha futura |

### Un mensaje por lectura, no uno por registro OBIS

Un medidor suele devolver **varios registros OBIS en respuesta a un mismo pedido**. Hay dos
formas de modelarlo y la propuesta elige la segunda:

| | Un mensaje por registro OBIS | **Un mensaje por lectura, con un mapa de registros** |
|---|---|---|
| Esquema | Plano y uniforme | Un nivel de anidamiento |
| Volumen | × cantidad de registros | Uno por lectura |
| Atomicidad | Se pierde: registros leídos juntos viajan como hechos independientes y pueden llegar parcialmente | Se preserva: un pedido, una respuesta, un instante |
| Clave de dedup | Necesita incluir el código OBIS | `(medidor_id, instante_lectura)` alcanza |
| Agregar un registro nuevo | Sin cambios | Sin cambios: entra en el mapa |

**Por qué el mapa aunque hoy tenga una sola entrada.** El día que el medidor traiga también
`1.8.0` o `2.8.0`, no hay que cambiar el esquema ni tocar a los consumidores que solo miran
`15.8.0`. Eso **es** una estrategia de evolución de esquema —agregar registros deja de ser un
cambio de versión— y el enunciado pide una explícitamente.

**El costo, dicho honestamente:** la validación se complica un poco, porque hay que declarar
qué registros son obligatorios, y el tipo es más laxo que un campo plano. Con un solo registro
en uso es un costo chico, pero es real.

### ⚠️ Dos registros, dos etapas — no confundirlos

Con contador acumulado hay **dos formas de registro** y solo la primera viaja por el tópico de
entrada:

| | Qué es | Campo de tiempo | Dónde vive |
|---|---|---|---|
| **Lectura cruda** | El valor del contador en un instante | `instante_lectura` | Tópico de entrada, es lo que emite el simulador |
| **Consumo de intervalo** | Cuánto se consumió entre dos lecturas consecutivas | `inicio_intervalo` + `duracion_minutos` | Interno al pipeline, lo produce la etapa de diferenciación |

```
lectura(18:00) = 12843.271 kWh   ─┐
                                  ├─→  consumo[18:00–18:15) = 0.742 kWh  →  franja punta
lectura(18:15) = 12844.013 kWh   ─┘
```

**La asignación de franja se aplica al consumo, no a la lectura.** Una lectura suelta no
pertenece a ninguna franja: es un instante, y las franjas dividen períodos. El que tiene
franja es el intervalo que queda *entre* dos lecturas, y se lo atribuye por su inicio.

Y por eso la regla de alineación de la decisión 5 es lo que hace que esto funcione: si los
bordes de franja están alineados a la grilla de medición, **ninguna lectura cae en medio de un
intervalo que cruce dos franjas**, y atribuir por el inicio es exacto en lugar de aproximado.

### Lo que falta decidir — Clara

- **Número de particiones**, y con qué criterio. Ojo: el throughput de escritura de este
  caudal no es el criterio.
- **Nombres de tópicos** y convención de versionado.
- **Adónde va lo rechazado**: ¿tópico propio `medicion.cuarentena.v1`, o un campo de motivo en
  el mismo tópico?
- **Si llegan dos eventos con el mismo `event_id` y distintos `registros`** —el medidor
  corrigió una lectura—: ¿gana el primero o el último? Con dedup estricto gana el primero y la
  corrección se pierde.
- **Qué registros son obligatorios** en el mapa, y qué hacer con una lectura que no trae
  `15.8.0`: ¿es inválida, o es válida pero no aporta al cálculo?
- **Mapa contra campo plano**, si el costo de validación te parece que no compensa. Es tu
  contrato.
- ✅ ~~Contador acumulado o consumo del intervalo~~ — **resuelto: contador acumulado**
  (OBIS `15.8.0`), y el contrato de arriba ya está corregido en consecuencia.

---

## 2. Asignación de franja — dueño: Daniel

Funciones puras, sin dependencias de Beam ni de Kafka.

```python
def cargar_calendario(ruta: Path) -> CalendarioTarifario:
    """Carga y valida config/franjas.toml.

    Levanta CalendarioInvalido si los bordes no están alineados a la grilla, si los
    rangos no cubren el día exacto, o si el intervalo no divide a 1440.
    El mensaje debe decir QUÉ está mal, no solo que está mal.
    """

def fecha_y_franja(instante: datetime, cal: CalendarioTarifario) -> tuple[date, str]:
    """Devuelve (fecha_local, nombre_de_franja) para un instante.

    `instante` debe ser timezone-aware; si es naive, es un error de programación y
    corresponde levantar excepción, no adivinar. Los datos con hora inválida ya
    fueron filtrados aguas arriba.
    """
```

**Por qué una sola función devuelve las dos cosas** y no dos funciones separadas: la clave de
agregación necesita `fecha_local` **y** `franja`, y las dos salen de la misma conversión de
huso. Separarlas abre la puerta a convertir dos veces con reglas distintas — justo el error
que hace que los intervalos cercanos a medianoche caigan en el día equivocado.

### Lo que falta decidir — Daniel

- ⚠️ **El umbral de corrección de reloj** y contra qué referencia se corrige. Derivarlo de lo
  que cuesta equivocar la franja: una lectura cuya hora real es 17:58 pero reporta 18:02 cruza
  a punta y se factura mal, así que no hay un umbral "chico y seguro".
- Cómo se representa `CalendarioTarifario` en memoria, y si conviene precomputar una tabla
  por la grilla.
- Si `fecha_y_franja` es también la que valida el timestamp, o si eso es una función aparte
  que corre antes.

---

## 3. Contrato de salida — dueña: Clara

Tópico `medicion.consumo-franja.v1`, clave **`medidor_id|fecha_local|franja`**.

```json
{
  "schema_version": 1,
  "medidor_id": "MED-000457",
  "fecha_local": "2026-09-20",
  "franja": "punta",
  "zona_horaria": "America/Asuncion",
  "energia_kwh": 3.184,
  "intervalos_contados": 16,
  "intervalos_esperados": 16,
  "es_provisional": true,
  "pane_index": 2,
  "pane_timing": "LATE",
  "ventana_inicio": "2026-09-20T00:00:00-03:00",
  "ventana_fin": "2026-09-21T00:00:00-03:00",
  "watermark_at_emission": "2026-09-20T22:30:00-03:00",
  "emitted_at": "2026-09-21T02:15:11.804-03:00"
}
```

**La clave hace el contrato.** Es estable a través de todos los panes de la misma celda, así
que todos van a la misma partición, se leen en orden y **el último gana**. La semántica del
consumidor es *upsert*, nunca *insert*. Eso es lo que hace que recalcular una ventana
reemplace su valor en lugar de sumar otro.

**`intervalos_contados` contra `intervalos_esperados`** es el campo más útil de la salida: con
una franja de 4 horas e intervalos de 15 minutos se esperan 16 registros. Si llegaron 12, el
total está incompleto y **el tablero puede decirlo** en lugar de mostrar un número bajo como
si fuera real. Es la diferencia entre "consumió poco" y "todavía no llegó todo".

**`es_provisional`** implementa la decisión 8: mientras no pase la lateness, el valor puede
cambiar. La finalidad no la anuncia un pane —después del último tardío no se emite nada—: la
deduce el consumidor cuando su reloj pasa `ventana_fin + lateness`.

### Un mensaje por lectura, no uno por registro OBIS

Un medidor suele devolver **varios registros OBIS en respuesta a un mismo pedido**. Hay dos
formas de modelarlo y la propuesta elige la segunda:

| | Un mensaje por registro OBIS | **Un mensaje por lectura, con un mapa de registros** |
|---|---|---|
| Esquema | Plano y uniforme | Un nivel de anidamiento |
| Volumen | × cantidad de registros | Uno por lectura |
| Atomicidad | Se pierde: registros leídos juntos viajan como hechos independientes y pueden llegar parcialmente | Se preserva: un pedido, una respuesta, un instante |
| Clave de dedup | Necesita incluir el código OBIS | `(medidor_id, instante_lectura)` alcanza |
| Agregar un registro nuevo | Sin cambios | Sin cambios: entra en el mapa |

**Por qué el mapa aunque hoy tenga una sola entrada.** El día que el medidor traiga también
`1.8.0` o `2.8.0`, no hay que cambiar el esquema ni tocar a los consumidores que solo miran
`15.8.0`. Eso **es** una estrategia de evolución de esquema —agregar registros deja de ser un
cambio de versión— y el enunciado pide una explícitamente.

**El costo, dicho honestamente:** la validación se complica un poco, porque hay que declarar
qué registros son obligatorios, y el tipo es más laxo que un campo plano. Con un solo registro
en uso es un costo chico, pero es real.

### ⚠️ Dos registros, dos etapas — no confundirlos

Con contador acumulado hay **dos formas de registro** y solo la primera viaja por el tópico de
entrada:

| | Qué es | Campo de tiempo | Dónde vive |
|---|---|---|---|
| **Lectura cruda** | El valor del contador en un instante | `instante_lectura` | Tópico de entrada, es lo que emite el simulador |
| **Consumo de intervalo** | Cuánto se consumió entre dos lecturas consecutivas | `inicio_intervalo` + `duracion_minutos` | Interno al pipeline, lo produce la etapa de diferenciación |

```
lectura(18:00) = 12843.271 kWh   ─┐
                                  ├─→  consumo[18:00–18:15) = 0.742 kWh  →  franja punta
lectura(18:15) = 12844.013 kWh   ─┘
```

**La asignación de franja se aplica al consumo, no a la lectura.** Una lectura suelta no
pertenece a ninguna franja: es un instante, y las franjas dividen períodos. El que tiene
franja es el intervalo que queda *entre* dos lecturas, y se lo atribuye por su inicio.

Y por eso la regla de alineación de la decisión 5 es lo que hace que esto funcione: si los
bordes de franja están alineados a la grilla de medición, **ninguna lectura cae en medio de un
intervalo que cruce dos franjas**, y atribuir por el inicio es exacto en lugar de aproximado.

### Lo que falta decidir — Clara

- **Los valores concretos de la política temporal**: tamaño de ventana, retraso del watermark,
  lateness exacta dentro del rango de 24 a 48 h, y el diseño de los triggers (cada cuánto un
  pane temprano, y si cada tardío dispara el suyo).
- Si el tablero lee este tópico directamente o si hay una materialización intermedia.
- `cleanup.policy` y retención del tópico de salida.
