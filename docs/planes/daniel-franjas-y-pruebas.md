# Plan — Daniel: franjas horarias, validación de tiempo y pruebas

Leer antes: [`../decisiones-de-diseno.md`](../decisiones-de-diseno.md), en particular las
decisiones 2, 3, 4 y 5, y [`../dominio-medicion.md`](../dominio-medicion.md).

---

## Parte 1 — Franjas (no depende de nada, se puede empezar ya)

Todo esto son **funciones puras**: se prueban con `pytest` común, sin Kafka, sin Beam y sin
Docker. Por eso podés arrancar el día uno.

**Cargar y validar la configuración.** El ejemplo y las reglas están en
[`../../config/franjas.example.toml`](../../config/franjas.example.toml). Son tres:
alineación de los bordes a la grilla de medición, cobertura exacta del día sin huecos ni
solapes, y que el intervalo divida a 1440. Una configuración inválida **se rechaza con un
mensaje que diga qué está mal** — no se corrige en silencio ni se prorratea.

**Asignar la franja.** La interfaz que consume Clara:

```python
def asignar_franja(instante, config) -> str
```

El instante es absoluto; la franja se deriva convirtiendo a la zona horaria de la
configuración. Ojo con los intervalos cerca de medianoche: son los que revelan si la
conversión está bien hecha.

**Decisiones que son tuyas:** cómo representás la configuración en memoria, cómo resolvés la
búsqueda de la franja (una tabla precomputada por la grilla es una opción, buscar linealmente
es otra), qué mensajes de error da la validación.

## Parte 2 — Validación del instante y desvío respecto del borde

> **Cambió el 20/09.** Antes esta parte era sobre relojes de medidores poco confiables. Ya no
> aplica: el readout no trae timestamp y el instante lo pone el concentrador, que está
> sincronizado (decisión 3). El problema es otro, y mejor.

Validaciones básicas, que son directas:

| Caso | Qué hacer |
|---|---|
| Instante ausente o mal formado | Cuarentena: sin instante no hay franja posible |
| Instante en el futuro respecto de la recepción | Cuarentena: indica un concentrador desincronizado |

⚠️ **Y la decisión que es tuya: el desvío tolerado respecto del borde de franja.**

El consumo de una franja sale de restar la lectura de su inicio y la de su fin. Si el pedido
programado para las 18:00 se resuelve a las 18:07, esa lectura **no marca el borde real**: los
siete minutos de consumo entre 18:00 y 18:07 terminan atribuidos a la franja anterior.

Hay que decidir cuánto desvío se acepta antes de considerar que la lectura no sirve para
cerrar la franja, y qué se hace cuando no sirve. Es un trade-off con consecuencia económica
medible en los dos sentidos:

- **Tolerar mucho** mete consumo de una franja en la otra y se factura mal.
- **Tolerar poco** descarta lecturas y deja franjas sin cerrar, y entonces no se factura nada.

Justificá el número; es material directo para tu sección del documento. Y ojo con el caso
peor: **si falta la lectura de un borde**, el consumo de las dos franjas adyacentes es
indistinguible — hay un solo número que abarca las dos. Prorratearlo asumiría consumo
uniforme, que es falso justamente en punta.

## Parte 3 — Pruebas

Son el respaldo de todo el proyecto y valen por sí mismas en la evaluación.

- **Unitarias** de tus funciones puras y de la lógica de agregación.
- **Con `TestStream`**, que permite controlar el avance del watermark y la llegada de cada
  evento:
  - un evento **duplicado** y la evidencia de que no se cuenta dos veces;
  - un evento **tardío dentro de la lateness** y la evidencia de que corrige su ventana;
  - un evento **tardío fuera de la lateness** y la evidencia de qué se hace con él;
  - eventos **fuera de orden** que igual caen en la ventana correcta.

Cada escenario tiene que dejar **evidencia legible**, no solo un test en verde: la entrega
exige mostrar el comportamiento, y un `assert` que pasa no se lo muestra a nadie en un video.
Conviene que cada prueba imprima la secuencia de lo que entró y lo que salió.

## Criterios de aceptación

- [ ] Una configuración mal alineada se rechaza con un mensaje que explica el problema.
- [ ] La asignación de franja está probada, incluidos los bordes y los cruces de medianoche.
- [ ] El desvío tolerado respecto del borde tiene un número justificado y probado.
- [ ] Está definido y probado qué pasa cuando falta la lectura de un borde.
- [ ] Existen los cuatro escenarios con `TestStream` y dejan evidencia legible.
- [ ] Tu sección del documento técnico justifica el umbral de corrección y la regla de
      alineación.
