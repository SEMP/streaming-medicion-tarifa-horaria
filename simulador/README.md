# Simulador de lectura remota

Genera lecturas sintéticas del registro acumulado `15.8.0`, tal como las obtendría un
concentrador que consulta medidores agrupados en **cabinas sobre un bus RS-485**.

Existe para **inyectar fallas a propósito**. Un simulador que se porta bien no sirve para
demostrar que el pipeline tolera duplicados, desorden y pérdidas — y demostrarlo es lo que
pide la entrega.

## Uso

```bash
uv run simulador --cabinas 20 --dias 1 --salida datos/lecturas.jsonl
uv run simulador --sin-fallas --salida datos/base.jsonl        # línea de base limpia
uv run simulador --escenario B --salida datos/dispositivo.jsonl
uv run simulador --help
```

Escribe **JSONL**, una lectura por línea. La publicación a Kafka se agrega cuando exista la
infraestructura: que funcione sin ella es deliberado, para que nadie quede bloqueado.

## Los dos escenarios

| | **A — bus compartido** (situación actual) | **B — dispositivo por medidor** |
|---|---|---|
| Estructura | Cabinas de 1 a 199 medidores sobre RS-485 | Un dispositivo dedicado por medidor |
| Lectura | Secuencial: el medidor *k* espera a los *k−1* anteriores | Independiente, sin contención |
| Cadencia | Rondas continuas; la ronda dura lo que dure | Configurable, 15 min por defecto |
| Para qué | Es lo que hay que procesar | **Prueba de concepto**: el mismo pipeline, sin cambios, sobre la arquitectura de recolección propuesta |

El escenario B no es una funcionalidad aparte: es **la misma simulación con cabinas de
tamaño uno**. Correr el pipeline contra los dos y comparar convierte el error de atribución
por franja de una estimación en una medición.

## Qué falla, y por qué está

Todo es configurable y **determinista**: la misma semilla produce exactamente los mismos
fallos. Sin eso, una prueba del pipeline que falla no se puede repetir.

| Falla | De dónde sale en la realidad |
|---|---|
| **Pedido que se corre** | El bus está ocupado con los otros medidores de la cabina |
| **Pedido que falla** | Se agota el tope por medidor tras varios reintentos |
| **Cabina caída** | El enlace es compartido: caen **todos** sus medidores a la vez |
| **Trama incompleta** | Enlace inestable; incluye el corte **en medio de un número** |
| **Reseteo de contador** | Cambio o reprogramación del equipo: la resta da negativo |
| **Ráfaga tardía** | El concentrador pierde enlace y publica después lo que juntó |
| **Duplicado** | Reintento de **publicación** — no de comunicación, que produce una lectura nueva |
| **Desorden** | Las cabinas avanzan en paralelo y publican cuando terminan |

`--sin-fallas` apaga **todas**, incluidas las que vienen del perfil del equipo, y da una
línea de base limpia contra la cual medir el efecto de cada una por separado.

## Evidencia de una corrida

Seis cabinas, un día, misma semilla:

```
                                eventos  medid   dupl  retro  tardías  separación
línea de base (sin fallas)        26610    381      0      0        0     20,6 min
escenario A (bus compartido)      26139    381    517    275     1159     20,9 min
escenario B (dispositivo)          5396     60     97     48      236     16,0 min
```

La línea de base en cero confirma que las fallas se inyectan y no se filtran de otro lado.
La **separación** es el tiempo entre dos lecturas consecutivas del mismo medidor: con bus
compartido son unos 21 minutos, y es la cota del error de atribución por franja.

## Cómo está armado

| Módulo | Qué hace |
|---|---|
| `consumo.py` | Curva de demanda diaria y contador acumulado. La punta es ~5× el valle: si fuera plano, separar por franja no mostraría nada |
| `parque.py` | Cabinas, tamaños, perfiles de medidor (`rapido`, `lento`, `inestable`) |
| `agenda.py` | La ronda sobre el bus secuencial. Es el corazón: de acá sale el problema temporal |
| `fallas.py` | Probabilidades y el truncado de tramas |
| `evento.py` | El evento del contrato y el `event_id` determinista |
| `cli.py` | Línea de comandos |

`ContadorMedidor.consumo_entre()` da el **consumo real**, que el pipeline no ve. Es la
verdad de referencia contra la cual medir cuánto se equivoca la atribución.

## Pruebas

```bash
uv run pytest tests/unit/test_simulador.py
```
