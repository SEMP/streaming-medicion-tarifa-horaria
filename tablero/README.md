# Tablero del consumo por franja

El último eslabón del ciclo del dato: **producir → procesar → analizar**. Lee el tópico
derivado `medicion.consumo-franja.v1` y reconstruye la vista actual.

```bash
uv run --with marimo marimo run tablero/tablero.py     # verlo
uv run --with marimo marimo edit tablero/tablero.py    # tocarlo
```

No hace falta instalar nada: `--with marimo` lo resuelve en el momento y **no toca el entorno
del proyecto ni la imagen de Docker**, que no tienen por qué cargar con una dependencia de
visualización.

Necesita el stack levantado y datos en el tópico:

```bash
docker compose -f infra/docker-compose.yml up -d
docker compose -f infra/docker-compose.yml --profile demo up simulador pipeline
```

**Ojo con qué perfil corriste**, porque no todos escriben al mismo lugar. El perfil `demo`
—simulador y pipeline— usa los tópicos `.v1`, que son los que el tablero lee por defecto. El
recorrido `e2e` usa tópicos propios terminados en `.e2e`, aislados a propósito para no
ensuciar los de la demostración. Para mirar el resultado de ese recorrido hay que apuntar el
tablero ahí:

```bash
TOPICO_CONSUMO=medicion.consumo-franja.e2e \
TOPICO_CUARENTENA=medicion.cuarentena.e2e \
  uv run --with marimo marimo run tablero/tablero.py
```

## Qué demuestra, además de mostrar números

Es una extensión opcional: la arquitectura que pide el enunciado termina en el tópico
derivado. Existe porque hay tres propiedades del contrato que solo se ven del lado del
consumidor.

**1. El consumidor hace *upsert*, nunca suma.** La clave `medidor|fecha|franja` identifica una
celda, y cada mensaje con esa clave reemplaza al anterior. Verificado sobre el recorrido
completo: **5 mensajes colapsan en 2 celdas**, con 3,100 kWh en `punta` y 2,400 en `resto` —
exactamente lo que el recorrido declara como esperado. Los tres mensajes de más son las
revisiones que publicó cada lectura que cambió algo; si el tablero sumara, el total sería más
del doble.

**2. Releer desde el principio converge a la misma vista.** Cada actualización vuelve a leer el
tópico entero desde el offset 0, con una asignación de particiones nueva y sin confirmar
offsets. Es caro a propósito: es la propiedad de idempotencia que el proyecto declara,
ejecutándose a la vista en lugar de afirmarse en un documento.

**3. La cota del error de atribución la calcula el consumidor.** Es una **cota**, no el error:
lo que se sabe es que el error no puede superarla, porque la acota la separación entre las dos
lecturas que rodean el borde. El error real solo se conoce cuando llega una lectura sobre el
borde — en la demostración resultó ser 0,100 kWh, bastante menos que la cota. El mensaje trae
el numerador, `separacion_maxima_minutos`. El denominador es la duración de la franja, que vive en el
calendario tarifario y **no** en el evento — por eso el tablero carga `config/franjas.toml` con
el mismo `cargar_calendario` que usa el pipeline. Está explicado en `docs/contratos.md`,
sección 2.3: ponerlo en el mensaje obligaría a que pipeline y consumidor coincidan sobre qué
calendario rige en cada fecha.

## Lo que el tablero rechaza, y por qué lo dice

Valida el contrato de salida antes de aceptar un mensaje: clave de tres partes y un
`energia_kwh` en el cuerpo. Lo que no lo cumple **se cuenta y se informa**, no se descarta en
silencio.

No es paranoia: la **prueba de humo escribe en este mismo tópico** haciendo *passthrough* de
lecturas crudas, claveadas solo por medidor. Si se mezclan con las celdas aparecen filas sin
fecha ni franja, y el total pierde sentido. Cuando eso pasa, el tablero lo avisa y recomienda
arrancar con el tópico limpio:

```bash
docker compose -f infra/docker-compose.yml down -v
```

## El inyector de irregularidades

Seis botones que publican al tópico **crudo** una lectura del medidor `MED-DEMO-001`, para
pedir a mano lo que el simulador tira por probabilidad. La lógica vive en
[`inyector.py`](inyector.py), sin `import marimo`, y se prueba con `pytest` sin levantar nada.

| Botón | Qué tiene que pasar |
|---|---|
| Lectura normal | Aparece o crece la celda de la franja que corresponde |
| Duplicado de publicación | **Nada cambia**: mismo `event_id`, lo descarta el deduplicador |
| Tardía sobre el borde | Corrige el reparto **sin mover el total**; la celda pasa a `medida` |
| Reseteo de contador | A cuarentena, con motivo `contador_retrocede` |
| Trama truncada | A cuarentena, o un valor absurdo según dónde caiga el corte |
| Hueco largo | Celda `indeterminada`, con `minutos_indeterminados > 0` |

Lo que hace honesto al botón del duplicado: `event_id` es
`sha256("<medidor_id>|<instante_lectura>")[:16]`, así que republicar el mismo par produce el
mismo identificador **por construcción**. No imita un duplicado: produce uno.

### Requiere el pipeline corriendo y el simulador apagado

```bash
docker compose -f infra/docker-compose.yml up -d
docker compose -f infra/docker-compose.yml --profile demo up -d pipeline
```

Nombrar `pipeline` explícitamente **no arrastra al simulador**, y eso importa: una corrida del
perfil `demo` completo publica más de 21.000 lecturas, y entre ellas las inyectadas se pierden
de vista.

### Verificado de punta a punta el 30/09/2026

La secuencia `normal → normal → normal → duplicado → tardía` produjo **cinco revisiones para
seis mensajes publicados**, que es el resultado que el tablero promete:

| # | Celda | kWh | Origen | Intervalos |
|---|---|---:|---|---:|
| 1 | `MED-DEMO-001|2026-09-30|resto` | 0,900 | medida | 1 |
| 2 | `MED-DEMO-001|2026-09-30|resto` | 1,200 | **interpolada** | 2 |
| 3 | `MED-DEMO-001|2026-09-30|punta` | 0,600 | **interpolada** | 1 |
| 4 | `MED-DEMO-001|2026-09-30|resto` | 1,000 | **medida** | 2 |
| 5 | `MED-DEMO-001|2026-09-30|punta` | 0,800 | **medida** | 1 |

Las tres primeras son de las lecturas normales; **el duplicado no produjo ninguna**, que es
exactamente lo que tiene que pasar. Las dos últimas son de la tardía, y ahí está lo que el
proyecto existe para mostrar: el total se queda en **1,800 kWh** —0,900 + 0,900, la energía
que el contador acumuló— pero **0,200 kWh se mudan de `resto` a `punta`**, que es el
`DESVIO_EN_EL_BORDE`, y las dos celdas pasan de `interpolada` a `medida`. La cuarentena del
medidor quedó vacía.

### ⚠️ Con el tópico casi vacío la salida no aparece

Es la condición que hizo fallar el primer intento, y hay que conocerla: con el pipeline en
modo **no acotado** y muy pocos mensajes en el tópico, las lecturas se consumen —el grupo
`g-consumo-franja` queda con lag 0— pero **no salen de la etapa de lectura de KafkaIO**. Los
contadores de Flink lo muestran sin ambigüedad: el vértice `LeerLecturas` recibe los registros
y emite cero.

No es un defecto del inyector ni de la cadena: en cuanto hay volumen, las mismas lecturas
inyectadas salen con los valores de la tabla de arriba. Los otros tres caminos del proyecto no
tropiezan con esto porque leen acotado (`humo`, `e2e`, con `--max-messages`) o corren con el
simulador publicando miles de lecturas.

**Para una demostración en vivo, entonces, conviene dejar el simulador corriendo**:

```bash
docker compose -f infra/docker-compose.yml --profile demo up -d
```

El tablero filtra por medidor, así que `MED-DEMO-001` se sigue viendo entre las demás.

## Configuración

Todo por variables de entorno, con el mismo valor por defecto que el resto del proyecto:

| Variable | Por defecto |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:29092` |
| `TOPICO_CONSUMO` | `medicion.consumo-franja.v1` |
| `TOPICO_CUARENTENA` | `medicion.cuarentena.v1` |
| `TOPICO_LECTURAS` (lo usa el inyector) | `medicion.lecturas.v1` |
| `CONFIG_FRANJAS` | `config/franjas.example.toml` |

**Hay que correrlo desde la raíz del repositorio.** No por el `import` de `pipeline.franjas`
—`uv run` instala el paquete en el entorno y eso se resuelve solo—, sino porque
`CONFIG_FRANJAS` apunta a `config/franjas.example.toml`, que es una **ruta relativa**. Desde
otra carpeta falla con `no existe el archivo de calendario`. Verificado en los dos sentidos.
