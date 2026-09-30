# Consumo eléctrico por franja horaria, en streaming

Pipeline end-to-end con **Apache Kafka** y **Apache Beam** que calcula cuánta energía
consumió cada medidor en cada franja tarifaria, a partir de curvas de carga que llegan
**desordenadas, duplicadas y con horas de retraso**.

Proyecto integrador de la materia *Streaming de datos y sus aplicaciones* — Maestría en
Inteligencia Artificial, Facultad Politécnica (UNA).

---

## El problema

Una distribuidora eléctrica necesita facturar la energía a **precio diferenciado según la
hora del día**: no cuesta lo mismo un kWh en hora punta que de madrugada. Para eso hay que
saber cuánta energía consumió cada cliente **en cada franja**, y ahí empiezan las
dificultades.

Los medidores de este parque **no transmiten por su cuenta ni guardan una serie histórica**:
responden con el valor actual de sus contadores cuando se les pregunta. El consumo de un
período se obtiene **restando dos lecturas**, y de ahí sale el requisito central: hay que
pedir exactamente en los bordes de cada franja, porque sin lectura a las 18:00 y otra a las
22:00 el consumo en punta simplemente no existe como dato.

Hoy se pide una vez por día, lo que alcanza para facturar el consumo diario pero no para
discriminar por franja. Sobre eso se apilan las dificultades:

- **Los resultados llegan tarde y desordenados.** El concentrador que hace los pedidos pierde
  enlace y publica en ráfaga lo que juntó, con instantes de horas atrás.
- **Los reintentos duplican lecturas**, cuando una publicación no se confirmó.
- **Un pedido que se corre mueve consumo de franja.** El programado para las 18:00 que se
  resuelve a las 18:07 le atribuye a resto siete minutos de punta.
- **Ningún medidor acumula por franja.** La atribución la hace este pipeline, no el equipo en
  campo.

Nada de eso es un detalle de implementación: **si una medición se asigna a la franja
equivocada, al cliente se le factura mal.** Por eso el sistema procesa por *tiempo de evento*
y no por tiempo de llegada.

### El proyecto no solo procesa: también dice cómo recolectar

Como el consumo de una franja sale de restar dos lecturas, **la configuración tarifaria
determina la agenda de pedidos**: hay que preguntar en cada borde, y conviene repetir el
pedido alrededor de cada uno para que un fallo aislado no arruine la franja entera. Esa
recomendación operativa es parte del resultado del trabajo, no un detalle de implementación.

## Arquitectura

```
  SIMULADOR            KAFKA                 BEAM                    KAFKA
  de medidores    →   lecturas crudas   →   diferenciación,     →   consumo por
  (readout por        clave: medidor        franja, ventana,        medidor/día/franja
   pedido)                                  dedup                    clave estable
                                                 ↓
                                            cuarentena
                                        (timestamps inválidos)
```

## Estructura

```
simulador/    Productor de eventos sintéticos: lecturas por pedido, pedidos que se
              corren o fallan, duplicados y ráfagas tardías. Determinista.
pipeline/     Pipeline Beam: lectura con KafkaIO, validación, asignación de franja,
              agregación incremental por clave y salida idempotente.
config/       Calendario de franjas e intervalo de medición. Configurables, validados.
infra/        docker-compose: Kafka, Flink, job server de Beam.
tablero/      Consumidor en marimo: lee el tópico derivado y reconstruye la vista
              actual con upsert por clave. Extensión opcional.
tests/        Pruebas unitarias y de pipeline con TestStream.
docs/         Documento técnico, diagrama de arquitectura y decisiones de diseño.
datos/        Datos de ejemplo generados por el simulador (no versionados).
```

### Los otros README

Cada carpeta con vida propia tiene el suyo, y todos se alcanzan desde acá:

| Dónde | Para qué |
|---|---|
| [`infra/README.md`](infra/README.md) | Cómo está armado el stack, qué hace cada contenedor y resolución de problemas |
| [`simulador/README.md`](simulador/README.md) | Qué genera el simulador, sus escenarios y cómo se le piden fallas |
| [`tablero/README.md`](tablero/README.md) | El consumidor en marimo: cómo levantarlo y qué tres propiedades del contrato demuestra |
| [`docs/README.md`](docs/README.md) | Mapa de la documentación: qué documento es cada uno y en cuál escribir cada cosa |
| [`evidencia/LEEME.md`](evidencia/LEEME.md) | Las dos corridas capturadas y cómo leerlas |
| [`docs/planes/README.md`](docs/planes/README.md) | El reparto de trabajo del equipo. Es material de proceso, no de entrega |

## Estado

**El sistema corre de punta a punta.** El simulador produce lecturas con fallas inyectadas a
propósito, Kafka las transporta, el pipeline sobre Flink las deduplica, diferencia el contador
y las atribuye a su franja, y el resultado sale por el tópico derivado.

| Componente | Estado |
|---|---|
| Decisiones de diseño | Listo · [`docs/decisiones-de-diseno.md`](docs/decisiones-de-diseno.md) · 13 decisiones |
| Dominio | Listo · [`docs/dominio-medicion.md`](docs/dominio-medicion.md) |
| Simulador | Listo · [`simulador/`](simulador/) · dos escenarios, fallas deterministas |
| Infraestructura | Listo · [`infra/`](infra/) · Kafka + Flink + job server, con `KafkaIO` andando |
| Configuración de franjas | Listo · cargador con validación de cobertura y reparto por borde |
| Pipeline | Listo · parseo, cuarentena, ventana, deduplicación, diferenciación y celdas |
| Pruebas | Listo · **89**, incluidas las de `TestStream` para el comportamiento tardío |
| Evidencia de ejecución | Listo · [`evidencia/`](evidencia/) · cuatro corridas, en cuatro máquinas |
| Documento técnico | Listo · [`docs/tecnico/`](docs/tecnico/documento.md) · 11 páginas |
| Video | **Pendiente** · guion en [`docs/guion-video.md`](docs/guion-video.md) |

## Cómo levantarlo

### Prerrequisitos

Para el camino con Docker, que es el recomendado:

- **Docker Engine con el plugin Compose v2.** El comando es `docker compose`, con espacio, no
  `docker-compose`. Verificalo con `docker compose version`.
- **git**, para clonar.
- **Unos 12 GB de RAM libres.** Medido con `docker stats`: el stack solo consume 3,1 GB en
  reposo, pero **con el pipeline del paso 2 procesando llega a 8,9 GB**, porque los dos
  TaskManagers levantan un proceso del SDK de Python por slot. El resto es margen para el
  sistema. Con menos, el paso 2 puede quedarse sin memoria a mitad de camino.
- **Paciencia la primera vez: entre 10 y 15 minutos.** Medido en una máquina sin nada
  cacheado: unos 8 minutos el paso 1 —la construcción de Flink y la descarga del job server— y
  unos 7 más el paso 2. Descarga varios cientos de MB, incluido el runtime de Java que KafkaIO
  necesita. **No está colgado.** En las corridas siguientes son segundos.

No hace falta tener Python instalado: el camino con Docker no lo usa.

---

### Camino A — con Docker (recomendado)

Los pasos, en orden. **No bajar el stack hasta el final.**

**0. Clonar y entrar**

```bash
git clone https://github.com/SEMP/streaming-medicion-tarifa-horaria.git
cd streaming-medicion-tarifa-horaria
```

**Todos los comandos que siguen se corren desde la raíz del repositorio**, que es donde está
la carpeta `infra/`.

**1. Iniciar el entorno**

```bash
docker compose -f infra/docker-compose.yml up -d --build
```

La interfaz de Flink queda en <http://localhost:8081> y Kafka en `localhost:29092`.

> El `--build` está a propósito. Sin él, Compose reutiliza las imágenes que ya existan, así
> que **después de un `git pull` seguirías corriendo el código viejo** y parecería que la
> actualización no hizo nada. Cuando no hay cambios no cuesta casi nada.

**2. Producir eventos y procesarlos**

```bash
docker compose -f infra/docker-compose.yml --profile demo up -d --build
```

Levanta el simulador y el pipeline: el simulador publica unas 21.000 lecturas en
`medicion.lecturas.v1` y termina; el pipeline las consume y **queda corriendo**, porque es un
trabajo de streaming y no tiene por qué terminar.

Para mirar el avance:

```bash
docker compose -f infra/docker-compose.yml logs -f simulador pipeline
```

> El `-d` del paso anterior importa. **Sin él, la terminal queda tomada** y el reflejo de
> apretar `Ctrl+C` **detiene todo el stack, Kafka incluido**, con lo que los pasos 3 y 4
> fallan. Con `-d`, el `Ctrl+C` de este `logs -f` corta solo la vista y no toca nada.

**3. Ejecutar el pipeline de punta a punta, con verificación**

```bash
docker compose -f infra/docker-compose.yml --profile e2e run --rm extremo-a-extremo
```

Siembra cinco lecturas —con un duplicado y una tardía—, las procesa sobre Flink y **compara el
resultado contra el esperado**. Devuelve código de salida, así que sirve como prueba.

> Acá **no va `-d`**, a diferencia de los pasos 1 y 2, y la razón es que `run` y `up` hacen
> cosas distintas. `up` levanta servicios que quedan corriendo, así que el `-d` libera la
> terminal. `run` ejecuta **un comando que termina solo**: con `-d` verías un identificador de
> contenedor en lugar de la tabla de resultados, que es justo lo que viniste a mirar.

Y el replay, que muestra la idempotencia sobre el stack real:

```bash
docker compose -f infra/docker-compose.yml --profile e2e run --rm repeticion
```

Relee el mismo tópico desde el offset 0 con otro grupo de consumidor. Da las mismas dos celdas:
reprocesar no duplica ni corrige, converge.

**4. Observar la salida**

```bash
docker compose -f infra/docker-compose.yml exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic medicion.consumo-franja.v1 --from-beginning --property print.key=true \
  --max-messages 10
```

Cada línea es una celda `medidor|fecha|franja` con su consumo, su cobertura en minutos y si el
valor se midió o se interpoló.

**Una clave repetida no es un error**: es una revisión posterior del mismo resultado, y vale la
última — el consumidor hace *upsert*. Se ve enseguida en la muestra, porque las primeras celdas
de un medidor aparecen varias veces con valores que crecen.

El `--max-messages 10` es para ver **la forma** de la salida. Con el simulador del paso 2
completo hay del orden de **20.000 celdas**, así que pedirlas todas llena la pantalla.

Para ver lo que el pipeline **no** pudo procesar, el mismo comando sobre
`medicion.cuarentena.v1`. Cada registro lleva su motivo.

**Si querés el total en lugar de una muestra**, se cambia `--max-messages 10` por
`--timeout-ms 10000`, que lee todo y corta cuando deja de llegar nada nuevo:

```
Processed a total of 20785 messages
```

> Esa variante imprime antes un `ERROR … TimeoutException`. **No es un error** —el proceso
> termina con código 0— sino la forma que tiene esta herramienta de avisar que venció la
> espera. Por eso el comando de arriba usa `--max-messages`, que corta sin ruido.

**Opcional — la prueba de humo y la suite**

```bash
docker compose -f infra/docker-compose.yml --profile humo run --rm humo
docker compose -f infra/docker-compose.yml --profile pruebas run --rm pruebas
```

La primera verifica el cableado con un *passthrough*, sin lógica de dominio: separa «el
pipeline está mal» de «la infraestructura está mal». La segunda corre las 92 pruebas dentro del
contenedor, sin depender del Python del host.

**5. Bajar y limpiar** — recién acá, cuando ya no haga falta nada de lo anterior:

```bash
docker compose -f infra/docker-compose.yml --profile demo --profile humo \
  --profile e2e --profile pruebas down -v
```

---

### Camino B — con uv (opcional, para desarrollo)

**No levanta Kafka ni Flink.** Sirve para las pruebas, para generar lecturas a un archivo y
para la demostración, que corre con `DirectRunner`.

Instalar `uv`, que no necesita permisos de administrador:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Tampoco hace falta Python instalado: el proyecto pide `>=3.12,<3.13` y **`uv` descarga el
intérprete solo**. Si la máquina trae otra versión, no importa.

```bash
uv sync                                          # crea .venv e instala dependencias
uv run pytest                                    # las 92 pruebas
uv run python -m pipeline.demostracion           # los tres escenarios, en 2 segundos
uv run simulador --cabinas 8 --salida datos/lecturas.jsonl
```

**La demostración es lo primero que conviene mirar.** Narra los tres escenarios —lecturas
normales, un duplicado y una tardía— sobre un solo medidor, y muestra qué cambia en la tabla de
salida después de cada uno. Corre con `DirectRunner` y `TestStream`, así que el tiempo se
controla y **la salida es idéntica en cualquier máquina**.

> Hoy **no tiene versión con Docker**. Quien siga solo el camino A puede verla igual en la
> evidencia capturada, en [`evidencia/evidencia-ejecucion.txt`](evidencia/evidencia-ejecucion.txt).

### Lo que existe en los dos caminos

| Qué | Con Docker | Con uv |
|---|---|---|
| Las 92 pruebas | `--profile pruebas run --rm pruebas` | `uv run pytest` |
| Generar lecturas a un archivo | — | `uv run simulador --cabinas 8 --salida datos/lecturas.jsonl` |

El resto no es equivalente: el camino A levanta el sistema real y el B corre lógica aislada.

**Si algo no arranca**, la resolución de problemas está en
[`infra/README.md`](infra/README.md).

## Demostración

El guion del video está en [`docs/guion-video.md`](docs/guion-video.md): siete escenas, con
el comando de cada una y quién habla. La cátedra no fija duración —pide un «video breve»—;
estas escenas suman 9 min 45 s.

## Evidencia de ejecución

```bash
./evidencia/generar-evidencia.sh
```

Hay **dos corridas, en dos máquinas distintas** — ver [`evidencia/LEEME.md`](evidencia/LEEME.md).
La de Daniel valida el recorrido en Windows + WSL2 + Docker Desktop y da los mismos números.

El script deja [`evidencia/evidencia-ejecucion.txt`](evidencia/evidencia-ejecucion.txt) con una
corrida completa y fechada: entorno, las pruebas, la demostración de los tres escenarios, el stack
levantándose, los tópicos creados, la prueba de humo, el recorrido sobre Flink, cuántos
mensajes quedaron en cada tópico y las celdas de salida tal como las lee el consumidor.

Los offsets de los tópicos son la parte que el enunciado pide como «logs o métricas
suficientes para observar producción, consumo, procesamiento y errores»: dicen cuántos
mensajes entraron, cuántos salieron y cuántos cayeron en cuarentena.

## Equipo y contribuciones

| Integrante | Responsabilidad principal |
|---|---|
| Sergio Morel | Infraestructura (Kafka, Flink, KafkaIO), simulador, deduplicación con estado e idempotencia |
| Clara | Contrato de evento, tópicos y particiones; pipeline Beam de transformación; ventanas y política de datos tardíos |
| Daniel | Franjas horarias: asignación, configuración y validación; validación de timestamps y cuarentena; pruebas |

Cada integrante **commitea con su propia cuenta** en su área, escribe la sección del
documento técnico correspondiente y la presenta en la demostración.

## Planes de trabajo

Un encargo por integrante en [`docs/planes/`](docs/planes/): qué construir, con qué contrato
se conecta al resto y cómo se sabe que está listo. **No dicen el cómo** — las decisiones de
implementación son de quien toma el plan, y son las que cada uno defiende.

Las **tres interfaces** que permiten trabajar en paralelo están en
[`docs/planes/README.md`](docs/planes/README.md).

## Documentación

El mapa está en [`docs/README.md`](docs/README.md): qué es cada documento, quién lo mantiene,
en cuál escribir cada cosa y de dónde sale cada entregable.

## Decisiones de diseño

Las decisiones tomadas y su justificación están en
[`docs/decisiones-de-diseno.md`](docs/decisiones-de-diseno.md). Se actualiza a medida que el
equipo decide; lo que está abierto figura como tal.

## Licencia

[MIT](LICENSE) — Sergio Morel, Clara Almirón y Daniel Ramírez, 2026.

Es una licencia permisiva: las obras derivadas **no** están obligadas a usar la misma
licencia y pueden ser cerradas. La única condición es conservar el aviso de copyright.

### Contexto académico

Trabajo práctico integrador de la materia *Streaming de datos y sus aplicaciones*, Maestría
en Inteligencia Artificial, Facultad Politécnica — Universidad Nacional de Asunción.

## Datos

**Todos los datos de este repositorio son sintéticos**, generados por `simulador/`. El
proyecto no usa datos reales de ninguna distribuidora ni de ningún sistema en producción.
