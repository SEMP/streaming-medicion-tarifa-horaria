# Propuesta: las tres interfaces

> ⚠️ **Es una propuesta, no una decisión.** Está escrita para que sea más rápido corregirla
> que redactarla de cero entre varios. Cada interfaz tiene al final **lo que queda por
> decidir**, que le corresponde a su dueño.
>
> Cuando se acuerden, esto se mueve a `docs/contratos.md` y deja de ser propuesta.

---

## 1. Contrato de evento de entrada — dueña: Clara

Tópico `medicion.lecturas.v1`, clave de particionamiento `medidor_id`.

```json
{
  "schema_version": 1,
  "event_id": "9f2c41a70b8d3e56",
  "medidor_id": "MED-000457",
  "lote_id": "LOTE-2026-09-20-000123",
  "secuencia": 42,
  "inicio_intervalo": "2026-09-20T18:00:00-03:00",
  "duracion_minutos": 15,
  "energia_kwh": 0.742,
  "calidad": "ok",
  "reportado_at": "2026-09-21T02:14:07.220-03:00"
}
```

Headers de Kafka: `schema_version`, `content_type`, `trace_id` — permiten a un consumidor
enrutar o rechazar sin deserializar el cuerpo.

### Qué hace cada campo, y por qué está

| Campo | Para qué |
|---|---|
| `event_id` | Identidad estable. `sha256("<medidor_id>\|<inicio_intervalo>")[:16]` — **determinista, no aleatorio**: un reintento del productor produce el mismo id, y por eso el duplicado es reconocible |
| `medidor_id` | Clave de particionamiento: manda todas las lecturas de un medidor a la misma partición y preserva su orden |
| `lote_id` | La descarga en la que vino. Es lo que permite demostrar el duplicado por reintento y rastrear un lote tardío completo |
| `secuencia` | Índice del registro dentro de la curva de carga del medidor. Da identidad cuando el timestamp falta |
| `inicio_intervalo` | **El tiempo de evento.** ISO-8601 con offset (decisión 4) |
| `duracion_minutos` | Hace el evento autodescriptivo: si la configuración cambia de 15 a 5 minutos, los eventos viejos siguen interpretándose bien |
| `energia_kwh` | La medición: consumo **del intervalo** |
| `calidad` | `ok` \| `estimado` \| `sin_sincronizar`. Los medidores reales marcan sus lecturas; permite decidir sin adivinar |
| `reportado_at` | Cuándo lo emitió el medidor. Contra `inicio_intervalo` da el desfase, que es el insumo de la regla de Daniel |

### Los casos inválidos son inválidos *respecto de este contrato*

El simulador los produce a propósito; la validación los manda a cuarentena.

| Caso | Cómo se ve |
|---|---|
| Sin timestamp | `inicio_intervalo` ausente o `null` |
| Sin offset | `"2026-09-20T18:00:00"` — hay hora, no se sabe de qué huso |
| Reloj desfasado | formato válido, valor corrido minutos u horas |
| Reloj absurdo | `"1970-01-01T00:00:00-03:00"` o una fecha futura |

### Lo que falta decidir — Clara

- **Número de particiones**, y con qué criterio. Ojo: el throughput de escritura de este
  caudal no es el criterio.
- **Nombres de tópicos** y convención de versionado.
- **Adónde va lo rechazado**: ¿tópico propio `medicion.cuarentena.v1`, o un campo de motivo en
  el mismo tópico?
- **Si llegan dos eventos con el mismo `event_id` y distinto `energia_kwh`** —el medidor
  corrigió una lectura—: ¿gana el primero o el último? Con dedup estricto gana el primero y la
  corrección se pierde.
- ✅ ~~Contador acumulado o consumo del intervalo~~ — **resuelto: contador acumulado**
  (OBIS `15.8.0`). Los ejemplos de arriba dicen `energia_kwh`: **hay que cambiarlo a
  `lectura_kwh`**, porque el valor es el del contador y no el consumo del intervalo. El
  consumo lo calcula la etapa de diferenciación del pipeline.

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

### Lo que falta decidir — Clara

- **Los valores concretos de la política temporal**: tamaño de ventana, retraso del watermark,
  lateness exacta dentro del rango de 24 a 48 h, y el diseño de los triggers (cada cuánto un
  pane temprano, y si cada tardío dispara el suyo).
- Si el tablero lee este tópico directamente o si hay una materialización intermedia.
- `cleanup.policy` y retención del tópico de salida.
