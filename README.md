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
