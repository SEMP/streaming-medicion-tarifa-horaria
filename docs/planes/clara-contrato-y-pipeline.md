# Plan — Clara: contrato de evento y pipeline de transformación

Leer antes: [`../decisiones-de-diseno.md`](../decisiones-de-diseno.md). Varias decisiones que
parecen obvias ya se tomaron al revés por una razón.

---

## Parte 1 — Contrato de evento (no depende de nada, se puede empezar ya)

Definir el JSON que viaja por el tópico de lecturas crudas, documentarlo, y definir también
el de salida.

**Tiene que incluir, como mínimo:**

- Identificador único del evento, **estable y determinista**, usable para deduplicar. Que sea
  determinista importa: si fuera aleatorio, un reintento del productor parecería un hecho
  distinto.
- Identificador del medidor — es además la clave de particionamiento.
- El instante de la medición, en ISO-8601 **con offset** (decisión 4).
- La medición en sí, y lo necesario para validarla.
- Una estrategia de **versionado del esquema**, aunque sea sencilla.

**Lo que hay que justificar por escrito** (va al documento técnico, y es tu sección):

- Por qué esa clave de particionamiento y qué orden garantiza. Kafka preserva el orden
  *dentro* de una partición, no entre particiones: ¿qué orden necesita este caso?
- Cuántas particiones y por qué. Ojo: el criterio no suele ser el throughput de escritura.
- Cuántos tópicos y por qué — de entrada, de salida, y qué pasa con lo que se rechaza.

**Decisiones que son tuyas:** el nombre y la forma de cada campo, cómo se versiona el
esquema, si los eventos rechazados van a un tópico propio o a otro lado, la convención de
nombres de tópicos.

⚠️ **Abierto, y lo resolvés vos con Sergio:** si el medidor reporta el **contador acumulado**
o el **consumo del intervalo** (decisión 10). Cambia el contrato y cambia el pipeline: con
contador acumulado hay que restar la lectura anterior, lo que exige estado por clave y
obliga a tratar los reseteos de contador.

## Parte 2 — Pipeline de transformación

Sobre el esqueleto que deja Sergio (lectura desde Kafka, escritura a Kafka, runner
configurado), armar la cadena:

1. **Validar** contra el contrato. Lo inválido **no se descarta en silencio**: va a una salida
   lateral y queda contado. Incluye los timestamps que Daniel marca como inválidos.
2. **Asignar la franja** llamando a la función de Daniel.
3. **Agregar** por clave `(medidor, fecha_local, franja)` con `CombinePerKey`, en ventana
   diaria (decisión 6).
4. **Escribir** el resultado con clave estable, para que recalcular reemplace en lugar de
   duplicar.

**También es tuya la política temporal:** ventana, watermark, lateness y triggers. La
decisión 8 fija el marco —lateness de 24 a 48 h, modo acumulativo— pero los valores concretos
y el diseño de los triggers los elegís y los justificás vos.

## Criterios de aceptación

- [ ] El contrato está documentado con un ejemplo de cada tipo de evento, incluidos los
      inválidos.
- [ ] El pipeline corre de punta a punta contra el simulador de Sergio.
- [ ] Un evento inválido aparece en la salida lateral y **no** en el agregado.
- [ ] Un evento tardío dentro de la lateness **corrige** el valor de su ventana.
- [ ] Reprocesar la misma entrada produce el mismo resultado, no valores duplicados.
- [ ] Tu sección del documento técnico explica las decisiones de arriba, con su alternativa
      descartada.
