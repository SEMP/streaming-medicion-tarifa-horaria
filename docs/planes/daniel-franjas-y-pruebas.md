# Plan — Daniel: franjas horarias, validación de tiempo y pruebas

Leer antes: [`../decisiones-de-diseno.md`](../decisiones-de-diseno.md), en particular las
decisiones 3, 4 y 5.

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

## Parte 2 — Validación de timestamps y cuarentena

La regla que decide qué hacer con cada caso de reloj de la decisión 3: sin timestamp, sin
offset, desfasado, absurdo.

⚠️ **Abierto y es tuyo resolverlo:** el **umbral** a partir del cual un desfase se corrige en
lugar de mandarse a cuarentena, y **contra qué referencia** se corrige. Es una decisión de
diseño real, con un trade-off: corregir de más ensucia los datos, corregir de menos descarta
lecturas buenas. Justificá la elección; es material directo para tu sección del documento.

Crítico, y la razón por la que esto va antes del pipeline: **un solo medidor con el reloj
adelantado arrastra el watermark hacia el futuro y provoca el descarte de lecturas legítimas
de todos los demás medidores.** La validación tiene que ocurrir antes de que el evento
participe del avance del watermark.

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
- [ ] Los cuatro casos de reloj tienen su regla, probada y documentada.
- [ ] Existen los cuatro escenarios con `TestStream` y dejan evidencia legible.
- [ ] Tu sección del documento técnico justifica el umbral de corrección y la regla de
      alineación.
