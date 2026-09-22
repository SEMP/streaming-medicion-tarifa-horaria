# Infraestructura local

Levanta el stack completo: **Kafka + Flink + job server de Beam**, con `KafkaIO` funcionando
desde Python.

```bash
docker compose -f infra/docker-compose.yml up -d        # levantar
docker compose -f infra/docker-compose.yml logs -f      # mirar
docker compose -f infra/docker-compose.yml down -v      # bajar y limpiar
```

La interfaz de Flink queda en <http://localhost:8081>, y Kafka se alcanza desde el host en
`localhost:29092`.

## El problema que esto resuelve

**`KafkaIO` no es una librería Python.** Es una transformación *cross-language*: las etapas
de lectura y escritura las ejecuta el SDK **Java** de Beam, y el SDK Python se comunica con él
a través de un *expansion service*. No se arregla con `pip install`.

La solución acá es que la **imagen de Flink trae los dos SDK** —Java y Python— y los corre en
modo `PROCESS` *dentro* del TaskManager:

```
 ┌─ TaskManager ────────────────────────────────┐
 │  Flink                                       │
 │    ├── SDK Java   → las etapas de KafkaIO    │
 │    └── SDK Python → nuestras transformaciones│
 └──────────────────────────────────────────────┘
```

La alternativa por defecto de Beam es levantar **un contenedor por entorno de SDK**, lo que
obliga a darle al TaskManager acceso al demonio de Docker — Docker dentro de Docker. Se evita
justamente con eso.

El Dockerfile de la aplicación además **cachea el JAR del expansion service durante el build**.
Sin eso, el primer pipeline que arranca se queda esperando una descarga de Maven, y en una
demostración eso parece que el sistema se colgó.

## Servicios

| Servicio | Para qué | Puerto |
|---|---|---|
| `kafka` | Broker en modo KRaft, sin ZooKeeper | 29092 |
| `kafka-init` | Crea los tres tópicos y termina | — |
| `jobmanager` | Coordina Flink | 8081 |
| `taskmanager` | Ejecuta el trabajo (2 réplicas) | — |
| `beam-job-server` | Traduce el pipeline de Beam a un trabajo de Flink | 8099 |

Y dos perfiles opcionales, que no arrancan solos:

```bash
# Demostración: el simulador publica y el pipeline procesa
docker compose -f infra/docker-compose.yml --profile demo up

# Prueba de humo: verifica el recorrido completo y devuelve código de salida
docker compose -f infra/docker-compose.yml --profile humo up --build \
    --abort-on-container-exit --exit-code-from humo
```

## Los tópicos

| Tópico | Clave | Part. | Por qué esa clave |
|---|---|---|---|
| `medicion.lecturas.v1` | `medidor_id` | 4 | Preserva el orden de las lecturas de un mismo equipo, que es lo que permite diferenciarlas |
| `medicion.consumo-franja.v1` | `medidor\|fecha\|franja` | 4 | Todos los panes de una celda a la misma partición: el último gana |
| `medicion.cuarentena.v1` | `medidor_id` | 2 | Nada se descarta en silencio |

## Detalles que no son obvios

**`KAFKA_LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS` está en 24 h.** El simulador comprime el tiempo:
publica en segundos lecturas cuyo tiempo de evento abarca horas. Sin ese margen, el broker
rechazaría por timestamp fuera de rango los eventos que le parecen "del futuro".

**El checkpointing de Flink no es opcional acá.** El pipeline mantiene estado por medidor —la
última lectura para diferenciar, los ids vistos para deduplicar—, así que un reinicio sin
checkpoint pierde ese estado y las lecturas siguientes no tienen contra qué restarse.

**El código del pipeline viaja en la imagen del TaskManager**, no solo en la de la aplicación:
el TaskManager es el proceso que realmente ejecuta las transformaciones Python. Si cambiás
`pipeline/`, hay que reconstruir esa imagen.

## Memoria

El stack pide unos **8,6 GB** con la configuración por defecto (dos TaskManagers de 2,8 GB).
En una máquina ajustada, bajar a una réplica y `BEAM_PARALLELISM=1`:

```bash
BEAM_PARALLELISM=1 docker compose -f infra/docker-compose.yml up -d --scale taskmanager=1
```

## Si algo falla

1. **Correr primero la prueba de humo.** Separa "el pipeline está mal" de "la infraestructura
   está mal", que son dos problemas muy distintos y se confunden todo el tiempo.
2. **Mirar la interfaz de Flink** (<http://localhost:8081>): si el trabajo aparece ahí, el job
   server y el cableado funcionan y el problema es de la lógica.
3. **Un arranque lento la primera vez es normal**: se descargan las imágenes de Beam, que son
   grandes.
