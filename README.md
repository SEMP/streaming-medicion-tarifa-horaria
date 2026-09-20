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

Los medidores registran su consumo en intervalos regulares —una *curva de carga*— pero **no
lo transmiten en el momento**: lo acumulan y lo descargan cuando logran conectarse. Cuando
eso ocurre, llega de golpe un lote de mediciones cuyas marcas de tiempo abarcan las últimas
horas o el último día. Además:

- **Los relojes de los medidores no son confiables.** Algunos no reportan la hora, otros la
  tienen desfasada, otros directamente mal configurada.
- **Los reintentos duplican mediciones.** Si una descarga se corta a la mitad y se reintenta,
  los mismos intervalos llegan dos veces.
- **Ningún medidor acumula por franja.** La atribución de cada intervalo a su franja la hace
  este pipeline, no el equipo en campo.

Nada de eso es un detalle de implementación: **si una medición se asigna a la franja
equivocada, al cliente se le factura mal.** Por eso el sistema procesa por *tiempo de evento*
y no por tiempo de llegada.

## Arquitectura

```
  SIMULADOR            KAFKA                 BEAM                    KAFKA
  de medidores    →   lecturas crudas   →   validación,         →   consumo por
  (curva de carga)    clave: medidor        franja, ventana,        medidor/día/franja
                                            dedup                    clave estable
                                                 ↓
                                            cuarentena
                                        (timestamps inválidos)
```

## Estructura

```
simulador/    Productor de eventos sintéticos: curva de carga, casos de reloj,
              duplicados y lotes tardíos. Configurable y determinista.
pipeline/     Pipeline Beam: lectura con KafkaIO, validación, asignación de franja,
              agregación incremental por clave y salida idempotente.
config/       Calendario de franjas e intervalo de medición. Configurables, validados.
infra/        docker-compose: Kafka, Flink, job server de Beam.
tests/        Pruebas unitarias y de pipeline con TestStream.
docs/         Documento técnico, diagrama de arquitectura y decisiones de diseño.
datos/        Datos de ejemplo generados por el simulador (no versionados).
```

## Estado

🚧 **En construcción.** Este commit es el esqueleto del proyecto: estructura, dependencias y
las decisiones de diseño ya tomadas. Todavía no hay nada ejecutable.

| Componente | Estado |
|---|---|
| Decisiones de diseño | ✅ [`docs/decisiones-de-diseno.md`](docs/decisiones-de-diseno.md) |
| Configuración de franjas | 🚧 ejemplo en `config/` |
| Infraestructura | ⬜ |
| Simulador | ⬜ |
| Pipeline | ⬜ |
| Pruebas | ⬜ |
| Documento técnico | ⬜ |

## Cómo levantarlo

⬜ *Pendiente.* Cuando el stack esté armado, esta sección tiene que permitir que **alguien
ajeno al equipo** levante el entorno, produzca eventos, ejecute el pipeline y vea la salida
siguiendo únicamente estas instrucciones.

El entorno de Python se maneja con [`uv`](https://docs.astral.sh/uv/):

```bash
uv sync          # crea .venv e instala dependencias
uv run pytest    # corre las pruebas
```

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

## Decisiones de diseño

Las decisiones tomadas y su justificación están en
[`docs/decisiones-de-diseno.md`](docs/decisiones-de-diseno.md). Se actualiza a medida que el
equipo decide; lo que está abierto figura como tal.

## Datos

**Todos los datos de este repositorio son sintéticos**, generados por `simulador/`. El
proyecto no usa datos reales de ninguna distribuidora ni de ningún sistema en producción.
