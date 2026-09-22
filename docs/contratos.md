# Contratos

Los dos contratos de los que depende el resto del sistema: el **evento de entrada**, que el
simulador produce y el pipeline consume, y el **registro de salida**, que el pipeline produce
y leen el tablero y la facturación.

Reemplaza a las secciones 1 y 3 de [`planes/propuesta-interfaces.md`](planes/propuesta-interfaces.md),
que era un borrador. La sección 2 de ese archivo —la asignación de franja— sigue siendo de
Daniel y no se toca acá.

> **Estado.** Las decisiones de este documento las toma Clara, que es la dueña de los dos
> contratos. Las que necesitan acuerdo de otro integrante están marcadas
> **⚠️ a confirmar con \<quien\>**. Todo lo demás está decidido y se puede construir encima.

**Las cifras que se citan están medidas**, no estimadas. Salen de una corrida del simulador
con 3 cabinas, 111 medidores y un día (`uv run python -m simulador --cabinas 3 --dias 1`),
que produjo 8.094 lecturas:

| Qué | Medido |
|---|---|
| Duplicados exactos (mismo `event_id`) | 163 · **2,0 %** |
| Eventos fuera de orden | 481 · **5,9 %** |
| Retraso de publicación | p50 1,7 s · p90 2,9 s · p99 2,9 h · **máx 3,97 h** |
| Deltas negativos (reseteo o truncación) | 48 |
| Separación entre lecturas del mismo medidor | p50 16 min · p90 29,7 min · **máx 249 min** |
| Reparto de `calidad` | `ok` 51 % · `checksum_no_verificado` 48 % · `truncada` 0,7 % |

---

# 1. Contrato de evento de entrada

**Tópico:** `medicion.lecturas.v1` · **Clave de particionamiento:** `medidor_id` · **4 particiones**

```json
{
  "schema_version": 1,
  "event_id": "0c12196f5ef41988",
  "medidor_id": "MED-0002-000",
  "cabina_id": "CAB-0002",
  "lote_id": "LOTE-CAB-0002-0000",
  "secuencia": 0,
  "instante_lectura": "2026-09-22T00:00:18-03:00",
  "registros": [
    { "obis": "15.8.0", "naturaleza": "acumulado", "valor": 10898.124, "unidad": "kWh" }
  ],
  "calidad": "ok",
  "publicado_at": "2026-09-22T00:00:19.269-03:00"
}
```

**Headers de Kafka:** `schema_version`, `content_type`, `trace_id`. Permiten a un consumidor
enrutar o rechazar sin deserializar el cuerpo.

## 1.1 Los campos

| Campo | Obligatorio | Para qué |
|---|---|---|
| `schema_version` | sí | Versión del esquema. Ver §1.3 |
| `event_id` | sí | Identidad estable: `sha256("<medidor_id>\|<instante_lectura>")[:16]`. **Determinista, no aleatorio**: un reintento de publicación produce el mismo id, y por eso el duplicado es reconocible. Es la mitad de la clave de deduplicación |
| `medidor_id` | sí | **Clave de particionamiento.** Ver §1.2 |
| `cabina_id` | sí | La cabina cuyo bus RS-485 comparte el medidor. No es decorativo: la duración de la ronda —y por lo tanto el error de atribución— es una propiedad **de la cabina**, y una caída de enlace afecta a todos sus medidores a la vez. Sin este campo no se puede distinguir «un medidor no contesta» de «se cayó la cabina entera» |
| `lote_id` | sí | La publicación en la que vino. Permite rastrear una ráfaga tardía completa y demostrar el duplicado por reintento |
| `secuencia` | sí | Número de pedido del concentrador para ese medidor. Permite detectar pedidos perdidos sin depender del tiempo |
| `instante_lectura` | sí | **El tiempo de evento.** El momento en que el concentrador obtuvo la respuesta. ISO-8601 con offset (decisión 4). Es un instante, no un período |
| `registros` | sí, no vacío | Lista de los registros que trajo la lectura. Ver §1.4 |
| `calidad` | sí | `ok` \| `checksum_no_verificado` \| `truncada`. Ver §1.5 |
| `publicado_at` | sí | Cuándo entró al tópico. Contra `instante_lectura` da el retraso de publicación, que es el insumo de la observabilidad y el que justifica la lateness |

Cada entrada de `registros`:

| Campo | Obligatorio | Para qué |
|---|---|---|
| `obis` | sí | Código OBIS del registro (IEC 62056-61). Único dentro de la lectura |
| `naturaleza` | sí | `acumulado` \| `instantaneo` \| `maximo`. Ver §1.7 |
| `valor` | sí | El número tal como lo devolvió el medidor |
| `unidad` | sí | La manda el medidor en el propio dato (`15.8.0(014380.81*kWh)`). Se guarda tal como llegó |
| `instante` | **no** | Instante propio del registro, cuando lo trae. Ver §1.6 |

## 1.2 Clave, particiones y el orden que se conserva

**Clave = `medidor_id`.**

El orden que este caso necesita es **por medidor**, y es un requisito duro, no una preferencia:
la etapa de diferenciación resta lecturas consecutivas del mismo medidor, así que necesita
verlas en secuencia. Kafka preserva el orden dentro de una partición, y con esta clave todas
las lecturas de un medidor caen siempre en la misma. Entre medidores distintos no hay relación
de orden que el dominio exija.

**No se usa `cabina_id` como clave**, aunque agruparía la ronda: las cabinas van de 1 a 199
medidores, así que produciría particiones desbalanceadas en un factor de 200. Con `medidor_id`
el reparto es parejo por construcción.

**4 particiones**, y el criterio **no es el throughput de escritura**. Aun el caso grande
—100.000 medidores en rondas continuas— son ~9,6 M mensajes/día, que para un broker es poco.
Lo que fija el número es otra cosa:

1. **El paralelismo útil está acotado por `min(particiones, slots, claves)`.** El stack de la
   clase 7 corre 2 TaskManagers con 2 slots. Con 4 particiones hay margen para duplicar los
   slots sin reparticionar, que es una operación que rompe el orden por clave.
2. **El estado vive por clave, no por partición**, así que agregar particiones no alivia
   memoria: solo reparte.
3. **Más particiones alargan la recuperación**: releer una partición es serial.

⚠️ **A confirmar con Sergio**, que es el dueño de `infra/`: el número tiene que coincidir con
lo que se cree en el `docker-compose`.

## 1.3 Tópicos y versionado

| Tópico | Para qué | Clave | Retención |
|---|---|---|---|
| `medicion.lecturas.v1` | Lecturas crudas | `medidor_id` | `delete`, 7 días |
| `medicion.consumo-franja.v1` | Resultado por medidor/día/franja | ver §2 | `compact,delete`, 90 días |
| `medicion.cuarentena.v1` | Todo lo rechazado | `medidor_id` si se conoce | `delete`, 30 días |

**Convención:** `<dominio>.<sustantivo>.v<mayor>`.

**Regla de versionado, que es la estrategia de evolución que pide el enunciado:**

- **Cambio compatible** → no cambia nada. Agregar un campo opcional, o agregar un registro
  OBIS a la lista, son cambios compatibles **por diseño** (§1.4): un consumidor que solo mira
  `15.8.0` no se entera.
- **Cambio incompatible** —quitar o renombrar un campo, cambiar un tipo o el significado de
  uno— → **tópico nuevo `.v2`**, y los dos conviven mientras dure la transición.
- `schema_version` viaja **en el cuerpo y en un header**. En el nombre del tópico para que un
  consumidor incompatible no se suscriba siquiera; en el header para que uno que sí puede
  leerlo decida sin deserializar; en el cuerpo para que el mensaje sea autodescriptivo cuando
  se lo mira suelto, en un archivo o en la cuarentena.

La retención de 7 días en la entrada no es arbitraria: tiene que cubrir la lateness de 36 h
(§2.4) **con margen para reprocesar**. Poder releer una semana entera es lo que permite
rehacer un cálculo cuando se corrige un bug, y es la forma barata de demostrar que el
reproceso converge al mismo resultado.

## 1.4 Un mensaje por lectura, con lista de registros

Un pedido produce una respuesta, y esa respuesta puede traer varios registros OBIS. Se modela
como **un mensaje con lista** y no como un mensaje por registro:

| | Un mensaje por registro | **Un mensaje por lectura, con lista** |
|---|---|---|
| Volumen | × cantidad de registros | Uno por lectura |
| Atomicidad | Se pierde: lo leído junto viaja separado y puede llegar a medias | Se preserva: un pedido, una respuesta, un instante |
| Clave de dedup | Tiene que incluir el código OBIS | `(medidor_id, instante_lectura)` alcanza |

**Lista de objetos y no mapa `código → valor`,** porque la unidad y la naturaleza son
propiedad **del registro**, no de la lectura: `15.8.0` viene en kWh, pero un registro de
potencia vendría en kW. Con un mapa habría que tener esa tabla hardcodeada en el consumidor,
que es exactamente el conocimiento implícito que se desactualiza.

⚠️ **Lo que la lista pierde y hay que compensar con validación:** un mapa garantizaba por
estructura que un código no apareciera dos veces. Con lista pasa a ser una **regla explícita
que el validador tiene que hacer cumplir: los códigos OBIS son únicos dentro de una lectura.**

## 1.5 `calidad`: lo que el simulador emite de verdad

| Valor | Qué significa | Qué hace el pipeline |
|---|---|---|
| `ok` | Trama completa y verificada | Procesa |
| `checksum_no_verificado` | La trama llegó entera pero no se pudo verificar su carácter de control | **Procesa**, y lo cuenta. Es el 48 % del tráfico: rechazarlo tiraría media muestra |
| `truncada` | La trama se cortó | **Cuarentena** |

⚠️ La propuesta anterior enumeraba `ok | estimado | sin_sincronizar`, que eran los valores del
modelo de perfil de carga. Estos son los del readout, verificados contra el corpus.

**`truncada` no alcanza como defensa**, y es el punto fino: una trama cortada en medio de un
número —`014380.81` → `01438`— sigue siendo un número válido y el simulador no siempre la
marca. Por eso hay una segunda defensa **aguas abajo**, en la etapa de diferenciación: un
contador no baja ni pega un salto imposible.

## 1.6 Instante propio por registro: sí, opcional

**Decisión: se admite un campo opcional `instante` en cada registro.** Si está ausente, el
instante del registro es el `instante_lectura` de la lectura.

**Por qué.** Algunos registros informan cuándo ocurrió *su* valor —la demanda máxima es el
caso típico—. Hoy no se usan, pero admitirlo cuesta **un campo opcional** y evita un cambio de
esquema el día que aparezcan.

**Lo que se resigna:** es un campo que hoy nadie escribe ni lee, y agregarlo «por las dudas»
es la clase de decisión que suele envejecer mal. Se acepta porque el costo es literalmente
cero para el productor actual y porque la alternativa —bumpear a `.v2` por un campo— es cara.

## 1.7 `naturaleza` del registro: sí, obligatorio

**Decisión: cada registro declara `acumulado | instantaneo | maximo`.**

**Por qué.** El pipeline solo puede diferenciar los acumulados: **restar dos mediciones
instantáneas de corriente no significa nada**. Hoy la naturaleza se deduce del código OBIS,
pero eso obliga a mantener una tabla código→naturaleza *dentro* del pipeline, y esa tabla se
desactualiza sin que nadie se entere. Con el campo explícito, la etapa de diferenciación se
maneja por dato y no por una lista hardcodeada.

**Lo que se resigna:** es redundante con el código OBIS, y un productor podría mentir. Se
mitiga validando el campo contra una tabla corta para los códigos conocidos, y confiando en él
para los que no.

## 1.8 Validación: qué se rechaza y qué no

El orden importa y es una decisión, no un detalle: **la validación ocurre antes de que el
evento participe del avance del watermark.** Un solo concentrador con el reloj adelantado
arrastraría el watermark hacia el futuro y haría que Beam descartara por tardías las lecturas
legítimas **de todos los demás medidores**.

| Caso | Qué se hace | Motivo de cuarentena |
|---|---|---|
| JSON ilegible | Cuarentena, como bytes crudos | `json_invalido` |
| `schema_version` desconocida | Cuarentena, sin intentar interpretar | `version_desconocida` |
| `instante_lectura` ausente, nulo o mal formado | Cuarentena | `instante_invalido` |
| `instante_lectura` **sin offset** | Cuarentena | `instante_sin_offset` |
| `instante_lectura` posterior a `publicado_at` | Cuarentena: concentrador desincronizado | `instante_futuro` |
| `registros` vacío | Cuarentena | `sin_registros` |
| Código OBIS repetido dentro de la lectura | Cuarentena | `obis_duplicado` |
| Falta `15.8.0` | Cuarentena | `sin_registro_util` |
| `calidad = truncada` | Cuarentena | `trama_truncada` |
| `calidad = checksum_no_verificado` | **Se procesa** y se cuenta | — |
| Delta negativo contra la lectura anterior | Cuarentena, **aguas abajo** (necesita estado) | `contador_retrocede` |
| Pedido corrido respecto del borde | **Se procesa.** No es inválido: alimenta el error declarado de §2.3 | — |

**Sobre «falta `15.8.0`» → cuarentena, y no «válido pero no aporta».** En este sistema
`15.8.0` es el único registro que se pide. Que no esté significa que la trama vino incompleta,
que es una anomalía que alguien debería mirar, no tráfico normal. **Esta regla cambia** el día
que se lean varios registros: ahí una lectura sin `15.8.0` pasaría a ser válida-pero-irrelevante.
Queda anotado porque es una decisión dependiente del alcance actual, no una verdad permanente.

## 1.9 Deduplicación, y el duplicado que no coincide

**Clave de dedup: `(medidor_id, instante_lectura)`**, que es exactamente lo que resume el
`event_id`. **Horizonte: 36 h**, el mismo que la lateness (§2.4). Fuera de ese horizonte no se
garantiza deduplicación, y se dice.

**El duplicado va antes de la diferenciación.** Si no, la resta de una lectura contra sí misma
emite un consumo de 0 que, con salida por *upsert*, **pisa el valor correcto**:

```
1ª vez:   consumo = R₂ − R₁        ✔
2ª vez:   consumo = R₂ − R₂ = 0    ✘  y el 0 reemplaza al bueno
```

**Si llegan dos eventos con el mismo `event_id` y `registros` distintos: gana el primero**, y
la copia divergente **va a cuarentena** con motivo `duplicado_divergente` en lugar de
descartarse en silencio.

**Por qué el primero y no el último.** Con identidad determinista, dos eventos con el mismo id
y distinto valor no son «una corrección»: son **la misma lectura reportada con dos valores
distintos**, o sea una contradicción. Una corrección genuina sería una lectura nueva, en un
instante nuevo. Dado que el contador solo sube y la lectura es un valor puntual, la causa más
probable de una divergencia es corrupción — y el caso `truncada` del corpus lo confirma.

**Lo que se resigna, dicho explícitamente:** si el primero en llegar es el corrupto, nos
quedamos con el corrupto. El daño está acotado porque la detección de delta imposible aguas
abajo lo manda igual a cuarentena, y porque la divergencia queda contada y visible.

---

# 2. Contrato del registro de salida

**Tópico:** `medicion.consumo-franja.v1` · **Clave:** `medidor_id|fecha_local|franja`

```json
{
  "schema_version": 1,
  "medidor_id": "MED-0002-000",
  "cabina_id": "CAB-0002",
  "fecha_local": "2026-09-22",
  "franja": "punta",
  "zona_horaria": "America/Asuncion",
  "energia_kwh": 3.184,
  "lecturas_usadas": 14,
  "cobertura_pct": 100.0,
  "separacion_max_minutos": 38.2,
  "error_atribucion_pct": 15.9,
  "interpolado": true,
  "indeterminado": false,
  "es_provisional": true,
  "pane_index": 2,
  "pane_timing": "LATE",
  "ventana_inicio": "2026-09-22T00:00:00-03:00",
  "ventana_fin": "2026-09-23T00:00:00-03:00",
  "watermark_at_emission": "2026-09-22T22:30:00-03:00",
  "emitted_at": "2026-09-23T02:15:11.804-03:00"
}
```

## 2.1 La clave es el contrato

`medidor_id|fecha_local|franja` es **estable a través de todos los panes de la misma celda**.
Todos van a la misma partición, se leen en orden, y **el último gana**. La semántica del
consumidor es **upsert, nunca insert**: eso es lo que hace que recalcular una ventana
*reemplace* su valor en lugar de sumar otro.

La clave **no incluye `cabina_id`**, aunque el campo viaje en el valor: un medidor podría
cambiar de cabina y la identidad del resultado no debe depender de eso.

## 2.2 Cobertura, en lugar de «intervalos esperados»

El borrador anterior proponía `intervalos_contados` contra `intervalos_esperados`. **Con
readout eso no se puede calcular**: no hay grilla fija de medición, hay rondas continuas cuya
duración depende del tamaño de la cabina, así que no existe un número de intervalos
«esperados».

Se reemplaza por **`cobertura_pct`**: qué porcentaje de la duración de la franja quedó
efectivamente cubierto por pares de lecturas. Cumple la misma función —distinguir «consumió
poco» de «todavía no llegó todo»— y sí es calculable. `lecturas_usadas` lo acompaña como dato
de diagnóstico.

## 2.3 Cada registro declara su propia incertidumbre

Lo exige la decisión 5, y es el campo que hace honesta a la salida.

```
error_atribucion_pct = separacion_max_minutos / duracion_franja_minutos
```

Con la franja punta de 4 h del calendario de ejemplo y una separación máxima de 38 min, da
15,9 %. **El número es distinto para cada medidor** porque depende del tamaño de su cabina, y
en el corpus medido va de 16 min (p50) a **249 min** en el peor caso — una cabina caída.

- `interpolado`: si el valor de algún borde se obtuvo interpolando entre las dos lecturas que
  lo rodean, en lugar de una lectura que cayera en el borde.
- `indeterminado`: si la separación superó el límite tolerado y el valor **no se inventó**.
  Cuando es `true`, `energia_kwh` es `null`.

⚠️ **El límite tolerado sigue abierto** en la decisión 5 y es la misma decisión que el desvío
respecto del borde de la decisión 3. Hasta que se cierre, el contrato ya prevé el campo: el
valor del umbral es configuración, no esquema.

## 2.4 La política temporal, con sus números

| Parámetro | Valor | Por qué |
|---|---|---|
| **Ventana** | Fija de 1 día, alineada al día local | La franja **no** es una ventana (decisión 6): es función pura del tiempo de evento y viaja en la clave. Beam ventanea sobre el instante absoluto, así que el día local se consigue con `FixedWindows(1 día)` desplazada 3 h, porque la medianoche de `America/Asuncion` es 03:00 UTC |
| **Allowed lateness** | **36 h** | La decisión 8 fija el rango 24–48 h. 24 h es exactamente la cadencia de recolección, sin margen: cualquier corte que dure un poco más pierde datos. 48 h duplica el estado sin evidencia de que haga falta. 36 h cubre un día entero de caída de enlace con media jornada de margen. El máximo medido en el corpus fue 3,97 h, un orden de magnitud por debajo |
| **Trigger temprano** | `AfterProcessingTime(60 s)` | El tablero tiene que moverse. Más rápido no compra nada: una ronda dura de 20 a 40 min, así que antes de 60 s rara vez hay información nueva |
| **Trigger tardío** | `AfterCount(1)` — un pane por cada tardío | Cada llegada tardía **corrige dinero**. Y hace visible el pane correctivo, que es la evidencia que pide el criterio 6 |
| **Acumulación** | `ACCUMULATING` | Cada pane es la revisión completa de la celda y reemplaza al anterior. Es lo que hace que el *upsert* del consumidor sea correcto |

**Lo que se resigna en el trigger tardío:** un pane por evento tardío significa que la ráfaga
de una cabina que vuelve de una caída produce una escritura por lectura. En producción
convendría agrupar los disparos tardíos con `AfterProcessingTime`, a costa de demorar la
corrección unos minutos — algo que a la facturación no le cambia nada. Se elige la versión por
evento **para la demostración**, y queda declarado como límite conocido.

**Sobre `es_provisional` y la finalidad.** Ningún pane anuncia que es el último: después del
último tardío simplemente no se emite nada. La finalidad la **deduce el consumidor** cuando su
reloj pasa `ventana_fin + 36 h`. Por eso hay dos lectores del mismo tópico con patrones
distintos: el tablero lee todos los panes y muestra un valor que cambia; la facturación lee una
sola vez, pasado ese horizonte.

## 2.5 El tablero lee el tópico directo

**Decisión: sin materialización intermedia en la v1.** El tablero consume
`medicion.consumo-franja.v1` y hace *upsert* en memoria por clave.

**Por qué.** Con el tópico compactado, la «vista actual» ya es una propiedad del tópico: un
consumidor nuevo que lea desde el principio converge exactamente a la misma vista. Meter una
base intermedia agregaría un componente que el enunciado no pide y que además habría que hacer
idempotente por separado.

**Lo que se resigna:** esto no escala a muchos lectores concurrentes ni admite consultas
ad-hoc. Va a «posibles mejoras».

## 2.6 `cleanup.policy` y retención

**`cleanup.policy=compact,delete`**, con `retention.ms` de 90 días.

El detalle que importa: **`compact` solo nunca borra una clave**. Como la clave incluye
`fecha_local`, el espacio de claves crece todos los días para siempre. La política combinada
conserva el último valor de cada clave *y* deja que las claves viejas caduquen.

`delete.retention.ms` debe superar la lateness de 36 h, para que un consumidor que se reconecta
no se pierda una corrección tardía.

---

# 3. Lo que este contrato deja sin resolver

- ⚠️ **El límite de separación tolerado** antes de marcar `indeterminado` (§2.3). Es la misma
  decisión abierta que el desvío respecto del borde de la decisión 3.
- ⚠️ **Qué se hace con los dos intervalos que quedan indeterminados** cuando falta una lectura
  (decisión 10, consecuencia 2): marcarlos, o imputar el consumo combinado al bloque completo
  —correcto en el total, pero puede caer sobre dos franjas distintas—. Toca la agregación.
- ⚠️ **A confirmar con Sergio:** el número de particiones (§1.2) tiene que coincidir con lo
  que cree el `docker-compose` de `infra/`.
- ⚠️ **A confirmar con Daniel:** `config/franjas.example.toml` todavía documenta la regla de
  **alineación a la grilla**, que la decisión 5 revirtió el 20/09. Con rondas continuas no hay
  grilla y los intervalos **sí** cruzan bordes: por eso la interpolación es obligatoria. La
  regla 1 de ese archivo habría que reescribirla como «la agenda cubre todos los bordes».
