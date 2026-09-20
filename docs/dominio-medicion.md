# El dominio: medición eléctrica remota

Contexto para quien no viene del rubro. Explica **cómo funciona de verdad** la lectura remota
de medidores, que es de dónde salen casi todas las decisiones de
[`decisiones-de-diseno.md`](decisiones-de-diseno.md).

---

## Qué es un medidor, desde el punto de vista de los datos

Un medidor eléctrico moderno es, para nosotros, dos cosas:

1. **Un conjunto de registros acumulados** que solo suben, como el cuentakilómetros de un
   auto. Cada registro tiene un código estandarizado.
2. **Una memoria de curva de carga**: el equipo guarda internamente el valor del registro cada
   N minutos, formando una serie temporal que después se descarga completa.

El medidor **no transmite en el momento**. Acumula y espera a que lo lean.

## Códigos OBIS

Los registros se identifican con códigos **OBIS** (*Object Identification System*, IEC
62056-61), que son un estándar internacional. Los que importan acá:

| Código | Qué mide |
|---|---|
| `1.8.0` | Energía activa **importada** (A+), total — lo que el cliente consume de la red |
| `2.8.0` | Energía activa **exportada** (A−), total — lo que el cliente inyecta a la red |
| `15.8.0` | Energía activa **absoluta**: \|A+\| + \|A−\|, total |
| `16.8.0` | Energía activa **neta**: A+ − A− |
| `1.8.1`, `1.8.2`… | Energía importada acumulada **por tarifa** 1, 2… |

**Este proyecto trabaja con `15.8.0`.** Es un registro **acumulado**: cada lectura es el valor
del contador en ese instante, no el consumo del período.

**Por qué alcanza con `15.8.0` y no hacen falta `1.8.0` y `2.8.0` por separado.** El registro
absoluto **suma** importación y exportación en lugar de netearlas, así que en un punto con
generación distribuida un cliente que inyecta energía haría subir el contador igual que uno
que consume. Eso acá no ocurre: **en el mercado modelado la distribuidora no compra energía a
los usuarios**, de modo que no hay inyección remunerada y la exportación es nula o
despreciable. `15.8.0` equivale entonces a energía consumida, y es el único registro que el
sistema necesita leer.

Vale la pena notar que esto es una propiedad **del marco regulatorio, no del equipamiento**.
Si se introdujera un esquema de medición neta —que la distribuidora compre la energía
inyectada—, el modelo dejaría de servir: habría que leer `1.8.0` y `2.8.0` por separado,
facturar cada sentido con su tarifa, y la suposición de que un consumo negativo siempre es un
reseteo de contador dejaría de valer. Queda anotado en "posibles mejoras".

Los registros `1.8.1`, `1.8.2`… muestran que **existen** medidores capaces de acumular por
franja tarifaria por sí mismos. Deliberadamente no dependemos de eso, y el porqué está en la
decisión 7 de `decisiones-de-diseno.md`.

## De contador acumulado a consumo: hay que restar

Como `15.8.0` es un contador, el consumo de un intervalo **no viene en el evento**: se obtiene
restando.

```
consumo(intervalo_n) = lectura(intervalo_n) − lectura(intervalo_n−1)
```

Eso trae tres problemas que el pipeline tiene que resolver, y que son material directo para
"límites conocidos":

**Necesita estado por medidor.** Hay que recordar la última lectura de cada uno para poder
restar la siguiente. No es un cálculo sin memoria.

**Un evento perdido arruina dos intervalos, no uno.** Si falta la lectura de las 18:15, no se
puede calcular el consumo de 18:00–18:15 *ni* el de 18:15–18:30: el primero pierde su final y
el segundo su inicio. La alternativa es imputar el consumo combinado al bloque completo, que
es correcto en total pero puede caer sobre dos franjas distintas.

**El contador se resetea.** Cuando se cambia un medidor, o se lo reprograma, el registro
vuelve a cero o arranca en otro valor. Una resta ingenua produce entonces un **consumo
negativo enorme**, que si entra al agregado lo destruye. Hay que detectarlo —una resta
negativa nunca es física— y mandarlo a cuarentena.

## La curva de carga y por qué llega tarde

El medidor guarda el valor del registro cada N minutos en su memoria interna. El sistema de
lectura se conecta periódicamente y **descarga el lote acumulado** desde la última vez.

Con intervalos de 15 minutos y descarga diaria, cada conexión trae **96 registros que abarcan
las últimas 24 horas**. Todos llegan en el mismo instante de reloj, pero sus tiempos de evento
se extienden un día hacia atrás.

De ahí salen, sin necesidad de inventar nada:

- **Datos tardíos y fuera de orden**, que son la condición normal y no la excepción.
- **Duplicados**, cuando una descarga se corta a la mitad y se reintenta: los intervalos que
  ya habían entrado vuelven a entrar.
- **Ráfagas**, en lugar de un caudal parejo: el sistema recibe nada durante horas y después un
  lote entero de golpe.

## Por qué los relojes no son confiables

El timestamp de cada registro de la curva de carga lo pone **el medidor**, con su propio
reloj. Y ese reloj:

- puede no estar sincronizado nunca desde la instalación;
- se desfasa con el tiempo, y sin conexión permanente nadie lo corrige;
- se pierde ante un corte de energía prolongado si la batería interna se agotó, y el equipo
  arranca en una fecha por defecto;
- en algunos equipos y configuraciones, directamente no acompaña a la lectura.

Es la razón de que el gateway tenga que validar y decidir **antes** de que el evento
participe del avance del watermark: un solo medidor con el reloj adelantado arrastraría el
watermark hacia el futuro y provocaría el descarte de lecturas legítimas de todos los demás.

## Por qué la franja horaria es un problema de tiempo de evento

La distribuidora quiere cobrar distinto según la hora: no cuesta lo mismo un kWh en hora punta
que de madrugada. Eso obliga a saber **cuándo ocurrió** cada consumo, no cuándo llegó el dato.

Y como el dato llega hasta un día después, agrupado en ráfagas y con relojes dudosos, la
distinción entre tiempo de evento y tiempo de procesamiento deja de ser una sutileza técnica:
**si una medición se asigna a la franja equivocada, al cliente se le factura mal.**

---

## Supuestos declarados

| Supuesto | Por qué se adopta |
|---|---|
| **No hay compra de energía al usuario** en el mercado modelado | Sin inyección remunerada la exportación es nula, así que `15.8.0` equivale a energía consumida. Es un rasgo regulatorio, no del equipo |
| **Ningún medidor** acumula por franja | Decisión 7: es incompatible con que las franjas sean configurables |
| **No rige horario de verano** | Decisión 4: evita días con franjas de duración distinta |
| Un consumo negativo **siempre** es un reseteo de contador, nunca una medición válida | Se desprende del anterior: sin exportación, el contador solo puede subir |

## Nota sobre el origen de este conocimiento

Este documento describe **cómo funciona la lectura remota de medidores** como industria: los
códigos OBIS son un estándar público (IEC 62056-61) y el resto es conocimiento del rubro. **No
contiene datos, esquemas ni código de ningún sistema en producción**, y todos los datos que
procesa este proyecto son sintéticos.
