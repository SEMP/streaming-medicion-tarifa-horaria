# Las etapas del pipeline, una por lámina

Cada lámina muestra **entrada, proceso y salida** de una etapa, con su estado, sus temporizadores y los motivos por los que manda algo a cuarentena. Se regeneran con:

```bash
uv run python docs/diagramas/generar-etapas.py
```

| # | Lámina | Etapa |
|---|---|---|
| 01 | [`01-leer-lecturas.svg`](01-leer-lecturas.svg) | Leer lecturas del tópico crudo |
| 02 | [`02-parsear.svg`](02-parsear.svg) | Parsear: de bytes a diccionario |
| 03 | [`03-marcar-tiempo.svg`](03-marcar-tiempo.svg) | Marcar el tiempo de evento |
| 04 | [`04-ventana-diaria.svg`](04-ventana-diaria.svg) | Ventana diaria y tipado de la clave |
| 05 | [`05-deduplicar.svg`](05-deduplicar.svg) | Deduplicar por instante de lectura |
| 06 | [`06-diferenciar.svg`](06-diferenciar.svg) | Diferenciar el contador acumulado |
| 07 | [`07-celdas-vigentes.svg`](07-celdas-vigentes.svg) | Celdas vigentes: atribución por franja |
| 08 | [`08-escribir-consumo.svg`](08-escribir-consumo.svg) | Escribir al tópico derivado |
| 09 | [`09-cuarentena.svg`](09-cuarentena.svg) | Cuarentena: la salida lateral |

## Los otros dos diagramas, que muestran otra cosa

- [`../arquitectura.svg`](../arquitectura.svg): los componentes y los tópicos. Se mantiene a mano.
- [`../pipeline-dag.svg`](../pipeline-dag.svg): la topología real, dibujada por Beam desde el mismo código que corre en producción (`uv run python -m pipeline.grafico`).

**Estas láminas se escriben a mano** y por lo tanto pueden desactualizarse. Cada una declara al pie el archivo y la línea de donde sale, para poder verificarla.
