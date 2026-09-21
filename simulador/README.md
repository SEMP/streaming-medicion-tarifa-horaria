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
| **Escalera de reintentos** | Determinista: hueco largo alternando con uno fijo de 10 s, hasta el tope |
| **Pedido que falla** | Se agota el tope de dos minutos tras recorrer la escalera |
| **Cabina caída** | El enlace es compartido: caen **todos** sus medidores a la vez |
| **Trama incompleta** | Enlace inestable; incluye el corte **en medio de un número** |
| **Reseteo de contador** | Cambio o reprogramación del equipo: la resta da negativo |
| **Ráfaga tardía** | El concentrador pierde enlace y publica después lo que juntó |
| **Duplicado** | Reintento de **publicación** — no de comunicación, que produce una lectura nueva |
| **Desorden** | Las cabinas avanzan en paralelo y publican cuando terminan |

`--sin-fallas` apaga **todas**, incluidas las que vienen del perfil del equipo, y da una
línea de base limpia contra la cual medir el efecto de cada una por separado.

## Evidencia de una corrida

Ocho cabinas, un día. La **separación** es el tiempo entre dos lecturas consecutivas del mismo
medidor, o sea la cota del error de atribución de ese medidor:

```
  cabina    medidores   separación   error máx. sobre una punta de 4 h
  CAB-0000         18      3,9 min                              1,6%
  CAB-0004         33      5,1 min                              2,1%
  CAB-0003         48      8,8 min                              3,7%
  CAB-0007         97     14,7 min                              6,1%
  CAB-0002         85     23,0 min                              9,6%
```

Que dos cabinas de tamaño parecido (85 y 97) den 23 y 14,7 minutos no es ruido: es la
**calidad del enlace**, que se modela por cabina. Es justamente el efecto que se perdería si
la latencia se sorteara por medidor.

## El modelo de tiempos, y qué parte está medida

Los tiempos salen de capturas de comunicación real (bytes con estampa de tiempo, dos
fabricantes). Está separado qué es medido y qué es nuestro, porque no es lo mismo:

| | |
|---|---|
| **Medido** · el tope de dos minutos se alcanza tal cual | máximo observado: 119,61 s |
| **Medido** · la distribución es **bimodal** | o responde en 3,5–4,7 s, o cae en la escalera y consume 30–120 s. **Nada en el medio** |
| **Medido** · la escalera es determinista | hueco largo (14–26 s) alternando con uno fijo de 10,00 s, tope de 10 intentos, cada reintento reenvía el pedido completo. Sin *backoff* ni *jitter* |
| **Medido** · la latencia inicial es del enlace | ~2,2 s consistentes en los dos fabricantes → por eso se modela **por cabina** y no por medidor: las latencias están correlacionadas |
| **Nuestro** · la tasa de fallas | Las capturas muestran la forma de cada caso, no su frecuencia. Es el parámetro más influyente y el primero a calibrar |
| **Inferido** · un reintento no sale antes del tercer intento | Si saliera en el segundo, el total caería cerca de los 20 s y la zona vacía que se midió no estaría vacía |

**La consecuencia es el resultado central del proyecto:** como la mayoría de los medidores
responde en unos 4 segundos y solo las fallas cuestan decenas, **la duración de la ronda la
fija la tasa de fallas y no la velocidad media**. Un parque de equipos veloces con mal enlace
tarda mucho más que uno de equipos mediocres con buen enlace.

```
ronda de 50 medidores, según la tasa de fallas
  3%  →   6 min        25%  →  18 min
 10%  →   9 min        50%  →  37 min
```

Y como el error de atribución por franja está acotado por la duración de la ronda, el error de
facturación resulta ser una función de la **calidad del enlace**. Eso es accionable: mejorar el
enlace reduce el error de facturación, y el simulador permite estimar cuánto.

## Cómo está armado

| Módulo | Qué hace |
|---|---|
| `consumo.py` | Curva de demanda diaria y contador acumulado. La punta es ~5× el valle: si fuera plano, separar por franja no mostraría nada |
| `parque.py` | Cabinas, tamaños, calidad de enlace y perfiles de medidor (`confiable`, `intermitente`, `problematico`) |
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
