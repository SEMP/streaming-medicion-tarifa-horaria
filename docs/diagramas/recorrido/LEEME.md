# El recorrido de un dato, paso a paso

Se sigue **una sola lectura** —`MED-0042`, 25/09/2026 a las 17:55, con el contador en 101,5 kWh— desde que el concentrador la pide hasta que el consumidor la lee. Cada lámina muestra **qué entra, qué le pasa, qué sale y por qué**.

Es la misma lectura de `pipeline.demostracion` y de `pipeline.extremo_a_extremo`, así que los números coinciden con el documento técnico y con la evidencia.

```bash
uv run python docs/diagramas/generar-recorrido.py
```

| # | Lámina | Qué muestra |
|---|---|---|
| 00 | [`00-recorrido-completo.svg`](00-recorrido-completo.svg) | El recorrido entero en una lámina |
| 01 | [`01-simulador.svg`](01-simulador.svg) | Simulador — El dato nace porque alguien lo pidió |
| 02 | [`02-topico-crudo.svg`](02-topico-crudo.svg) | medicion.lecturas.v1 — El log crudo: lo que llegó, tal como llegó |
| 03 | [`03-entrada-al-pipeline.svg`](03-entrada-al-pipeline.svg) | Parsear y marcar el tiempo — De bytes a un par con clave, y con el tiempo del dominio |
| 04 | [`04-ventana-y-dedup.svg`](04-ventana-y-dedup.svg) | Ventana diaria y deduplicación — El duplicado muere acá, antes de poder hacer daño |
| 05 | [`05-diferenciar.svg`](05-diferenciar.svg) | Diferenciar el contador — Recién acá aparece el consumo, que no venía en el dato |
| 06 | [`06-celdas-vigentes.svg`](06-celdas-vigentes.svg) | Atribuir la franja y agregar — Donde el intervalo se convierte en plata, y donde puede cruzar un borde |
| 07 | [`07-topico-derivado.svg`](07-topico-derivado.svg) | medicion.consumo-franja.v1 — El log derivado: la clave es la celda, no el evento |
| 08 | [`08-consumidores.svg`](08-consumidores.svg) | Tablero y facturación — Dos lectores del mismo log, con necesidades opuestas |

## La salida de una lámina es la entrada de la siguiente

No está escrito dos veces: el generador **deriva** la entrada de cada estación de la salida de
la anterior, así que no pueden desincronizarse. Por eso el rótulo dice de dónde viene —«es la
salida de la 01»— y a dónde va. Lo que aparece en gris debajo de la salida es contexto que
**no viaja**: la política de retención de un tópico, el estado que queda guardado, la celda
que se reemitirá después.

## Los payloads salen del código, no de la memoria

El evento de entrada es la salida real de `Lectura.a_dict()`. El `Consumo` y las celdas son la salida real de pasar las tres lecturas por `DeduplicarLecturas`, `DiferenciarContador` y `CeldasVigentes` con `DirectRunner`. Cada lámina declara al pie de qué archivo y línea sale.

## Y las otras tres vistas, que muestran otra cosa

- [`../arquitectura.svg`](../arquitectura.svg): los componentes y los tópicos.
- [`../pipeline-dag.svg`](../pipeline-dag.svg): la topología real, dibujada por Beam desde el código.
- [`../etapas/`](../etapas/LEEME.md): el mecanismo de cada etapa, con su estado y sus temporizadores.
