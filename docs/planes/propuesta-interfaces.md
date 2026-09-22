# Propuesta: las tres interfaces

> ⚠️ **Es una propuesta, no una decisión.** Está escrita para que sea más rápido corregirla
> que redactarla de cero entre varios. Cada interfaz tiene al final **lo que queda por
> decidir**, que le corresponde a su dueño.
>
> Cuando se acuerden, esto se mueve a `docs/contratos.md` y deja de ser propuesta.

---

## 1. Contrato de evento de entrada — dueña: Clara

> ✅ **SUPERADA el 22/09 por [`../contratos.md`](../contratos.md) §1** (commit `f060d8b`).
> Los ítems de «lo que falta decidir» de esta sección **ya están resueltos**; lo que sigue se
> conserva solo como registro del borrador. **No tomar decisiones de acá.**


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
  "registros": [
    { "obis": "15.8.0", "valor": 12843.271, "unidad": "kWh" }
  ],
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
| `secuencia` | Número de pedido del concentrador para ese medidor. Da identidad y permite detectar pedidos perdidos |
| `instante_lectura` | **El tiempo de evento**: el momento en que se capturó el valor del contador. ISO-8601 con offset (decisión 4). No es un período — ver la nota de abajo |
| `registros` | Lista con **todos** los registros que trajo esa lectura, cada uno con su código OBIS, su valor y su unidad. Hoy solo `15.8.0`, cuyo valor es el del contador: acumulado, solo sube. Ver la nota de abajo |
| `calidad` | `ok` \| `estimado` \| `sin_sincronizar`. Los medidores reales marcan sus lecturas; permite decidir sin adivinar |
| `reportado_at` | Cuándo lo emitió el medidor. Contra `inicio_intervalo` da el desfase, que es el insumo de la regla de Daniel |

### Los casos inválidos son inválidos *respecto de este contrato*

El simulador los produce a propósito; la validación los manda a cuarentena.

| Caso | Cómo se ve |
|---|---|
| Sin instante | `instante_lectura` ausente o `null` |
| Sin offset | `"2026-09-20T18:00:00"` — hay hora, no se sabe de qué huso |
| Instante futuro | posterior a la recepción: concentrador desincronizado |
| **Pedido corrido** | `18:07:00` cuando el borde de franja era `18:00:00` |
| **Contador que retrocede** | `lectura_kwh` menor que la anterior del mismo medidor: es un reseteo |

> Los casos de **reloj del medidor** que figuraban antes acá se eliminaron el 20/09: el
> readout no trae timestamp, así que el reloj del medidor no interviene (decisión 3).

### Un mensaje por lectura, no uno por registro OBIS

Un medidor suele devolver **varios registros OBIS en respuesta a un mismo pedido**. Hay dos
formas de modelarlo y la propuesta elige la segunda:

| | Un mensaje por registro OBIS | **Un mensaje por lectura, con lista de registros** |
|---|---|---|
| Volumen | × cantidad de registros | Uno por lectura |
| Atomicidad | Se pierde: registros leídos juntos viajan como hechos independientes y pueden llegar parcialmente | Se preserva: un pedido, una respuesta, un instante |
| Clave de dedup | Necesita incluir el código OBIS | `(medidor_id, instante_lectura)` alcanza |
| Agregar un registro nuevo | Sin cambios | Sin cambios: entra en la lista |

**Por qué una lista de objetos y no un mapa `código → valor`.** Con un mapa el valor es un
escalar suelto y no hay dónde colgarle nada. Con una lista, cada entrada es un objeto y lleva
su propia metadata — y eso importa apenas aparece un segundo registro: `15.8.0` está en kWh,
pero un registro de potencia estaría en kW y uno de tensión en V. **La unidad es propiedad del
registro, no de la lectura.** Con un mapa habría que tenerla hardcodeada por código en el
consumidor, que es el tipo de conocimiento implícito que después se desactualiza. Lo mismo
valdría para un estado por registro, si alguna vez hace falta: en la realidad un registro
puede venir marcado como poco confiable mientras otro del mismo readout está bien.

Si alguna vez se pasa de JSON a Avro o protobuf, además, *array de registros* es la forma
idiomática; los mapas ahí son más incómodos.

**Por qué la lista aunque hoy tenga una sola entrada.** El día que el medidor traiga también
`1.8.0` o `2.8.0` no hay que cambiar el esquema ni tocar a los consumidores que solo miran
`15.8.0`. Eso **es** una estrategia de evolución de esquema —agregar registros deja de ser un
cambio de versión— y el enunciado pide una explícitamente.

⚠️ **Lo que la lista pierde y hay que compensar con validación:** un mapa garantizaba por
estructura que un código no apareciera dos veces. Con lista eso deja de ser gratis y pasa a
ser una **regla explícita: los códigos OBIS deben ser únicos dentro de una lectura.**

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
- **Qué registros son obligatorios**, y qué hacer con una lectura que no trae `15.8.0`: ¿es
  inválida, o es válida pero no aporta al cálculo?
- ✅ ~~De dónde sale la unidad~~ — **resuelto:** la manda el medidor en el propio dato. Se
  guarda tal como llegó. Validarla contra el código es **opcional**: sirve para detectar un
  equipo mal configurado, no para saber la unidad. Ojo que en la trama el `*` es un
  separador de campos **genérico** y su cantidad y orden varían entre fabricantes
  —`(0003.0844*kW)` contra `(005.180*26-05-06*kW)`—, lo que refuerza tener la unidad en un
  campo propio del contrato en lugar de implícita en una posición. Ver
  [`../dominio-medicion.md`](../dominio-medicion.md).
- Si el registro necesita un **instante propio** opcional. Algunos lo traen —la demanda máxima
  informa cuándo ocurrió el máximo—, y aunque `15.8.0` no lo use, admitirlo en el contrato
  cuesta un campo opcional y evita un cambio de esquema después.
- Si conviene declarar la **naturaleza** del registro (acumulado / instantáneo / máximo). El
  pipeline solo puede diferenciar los acumulados: restar dos corrientes instantáneas no
  significa nada. Hoy se sabe por el código, pero explicitarlo evita que alguien difiera lo
  que no debe.
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

- ⚠️ **El desvío tolerado respecto del borde de franja**, y qué hacer con una lectura que lo
  excede. Derivarlo de lo que cuesta equivocar la franja.
- Cómo se representa `CalendarioTarifario` en memoria, y si conviene precomputar una tabla
  por la grilla.
- Si `fecha_y_franja` es también la que valida el timestamp, o si eso es una función aparte
  que corre antes.

---

## 3. Contrato de salida — dueña: Clara

> ✅ **SUPERADA el 22/09 por [`../contratos.md`](../contratos.md) §2** (commit `f060d8b`).
> Mismo caso que la sección 1: se conserva como registro del borrador, no como fuente.


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

| | Un mensaje por registro OBIS | **Un mensaje por lectura, con lista de registros** |
|---|---|---|
| Volumen | × cantidad de registros | Uno por lectura |
| Atomicidad | Se pierde: registros leídos juntos viajan como hechos independientes y pueden llegar parcialmente | Se preserva: un pedido, una respuesta, un instante |
| Clave de dedup | Necesita incluir el código OBIS | `(medidor_id, instante_lectura)` alcanza |
| Agregar un registro nuevo | Sin cambios | Sin cambios: entra en la lista |

**Por qué una lista de objetos y no un mapa `código → valor`.** Con un mapa el valor es un
escalar suelto y no hay dónde colgarle nada. Con una lista, cada entrada es un objeto y lleva
su propia metadata — y eso importa apenas aparece un segundo registro: `15.8.0` está en kWh,
pero un registro de potencia estaría en kW y uno de tensión en V. **La unidad es propiedad del
registro, no de la lectura.** Con un mapa habría que tenerla hardcodeada por código en el
consumidor, que es el tipo de conocimiento implícito que después se desactualiza. Lo mismo
valdría para un estado por registro, si alguna vez hace falta: en la realidad un registro
puede venir marcado como poco confiable mientras otro del mismo readout está bien.

Si alguna vez se pasa de JSON a Avro o protobuf, además, *array de registros* es la forma
idiomática; los mapas ahí son más incómodos.

**Por qué la lista aunque hoy tenga una sola entrada.** El día que el medidor traiga también
`1.8.0` o `2.8.0` no hay que cambiar el esquema ni tocar a los consumidores que solo miran
`15.8.0`. Eso **es** una estrategia de evolución de esquema —agregar registros deja de ser un
cambio de versión— y el enunciado pide una explícitamente.

⚠️ **Lo que la lista pierde y hay que compensar con validación:** un mapa garantizaba por
estructura que un código no apareciera dos veces. Con lista eso deja de ser gratis y pasa a
ser una **regla explícita: los códigos OBIS deben ser únicos dentro de una lectura.**

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
