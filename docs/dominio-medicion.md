# El dominio: medición eléctrica remota

Contexto para quien no viene del rubro. Explica **cómo funciona de verdad** la lectura remota
de medidores, que es de dónde salen casi todas las decisiones de
[`decisiones-de-diseno.md`](decisiones-de-diseno.md).

---

## Qué es un medidor, desde el punto de vista de los datos

Un medidor eléctrico moderno es, para nosotros, dos cosas:

1. **Un conjunto de registros acumulados** que solo suben, como el cuentakilómetros de un
   auto. Cada registro tiene un código estandarizado.
2. **Una capacidad de responder cuando se le pregunta.** El medidor **no transmite por su
   cuenta**: espera un pedido y devuelve el valor actual de sus registros.

⚠️ Muchos medidores tienen además una memoria de **perfil de carga** (objeto `99.1.0`) que
guarda internamente el valor cada N minutos, formando una serie fechada que se descarga
completa. **En este parque no se usa**: solo se dispone del modo readout, lo que cambia todo
el diseño. Ver más abajo.

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

## Cómo se ve una lectura, en concreto

Un readout en modo ASCII (IEC 62056-21) devuelve una lista de registros. Esquemáticamente:

```
96.1.0(000000000000)                       identificación del equipo
15.8.0(014380.81*kWh)                      energía activa absoluta, acumulada
3.8.0(004299.39*kvarh)                     energía reactiva, acumulada
1.6.0(0003.0844*kW)(26-05-08 23:00:00)     demanda máxima, y cuándo ocurrió
31.7.0(0000.86*A)                          corriente instantánea
```

Tres cosas que se leen de ahí y que condicionan el modelo:

**La unidad viaja en el dato**, después del `*`. No hay que inferirla de una tabla de
códigos: el medidor la informa. Por eso el contrato la guarda tal como llegó, y validarla
contra el código es opcional — sirve para detectar un equipo mal configurado, no para saber
en qué unidad está el valor.

**Los registros no son todos de la misma naturaleza.** Conviven acumulados (`15.8.0`,
`3.8.0`), un máximo con su propio instante de ocurrencia (`1.6.0`) y un valor instantáneo
(`31.7.0`). Restar dos lecturas consecutivas tiene sentido para un acumulado y **ninguno** para
una corriente instantánea. El pipeline solo diferencia los acumulados.

**Algunos registros traen su propio timestamp.** `1.6.0` lo necesita porque la demanda máxima
tiene que decir cuándo ocurrió el máximo, que es un instante distinto del de la lectura. El
modelo debe admitirlo aunque hoy no se use.

**No todos los medidores responden el mismo conjunto de registros.** El contrato no puede
asumir un conjunto fijo: por eso los registros van en una lista y no como campos.

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

## Solo readout: lo que eso implica

Si solo se puede preguntar por el valor actual, entonces **cada medición existe porque alguien
la pidió**, y el consumo de un período se obtiene restando dos pedidos:

```
consumo(18:00 → 22:00) = contador(22:00) − contador(18:00)
```

Eso tiene una consecuencia que parece un detalle y es el requisito central del proyecto:
**hay que pedir exactamente en los bordes de las franjas**. Sin lectura a las 18:00 y otra a
las 22:00, no hay manera de saber cuánto se consumió en punta — el dato simplemente no existe.

Un sistema con perfil de carga no tiene este problema: la frecuencia de medición la fija el
medidor internamente y la de recolección es independiente. **Acá son la misma cosa.**

### De dónde salen los datos tardíos y los duplicados

No del medidor, que no guarda nada, sino del **concentrador** que hace los pedidos y publica
los resultados:

- **Pierde enlace** y publica lo que juntó cuando se restablece: llega una ráfaga de lecturas
  cuyos instantes se extienden horas hacia atrás.
- **Reintenta** una publicación que no confirmó: las mismas lecturas entran dos veces.
- **Atiende muchos medidores en paralelo**, así que los resultados salen en el orden en que
  van respondiendo, no en orden de instante.

### Los dos modos de falla propios de este esquema

**El pedido se corre.** El programado para las 18:00 se resuelve a las 18:07 —el medidor tardó
en responder, el enlace estaba ocupado—. Esos siete minutos de consumo de punta terminan
contados en resto. El error es chico pero es **sistemático y económico**.

**El pedido falla.** Sin lectura en un borde, el consumo de las dos franjas adyacentes queda
**indistinguible**: se tiene un único número que abarca las dos y no hay forma de repartirlo
sin inventar. Prorratear asumiría consumo uniforme, y justamente la razón de que exista la
franja punta es que el consumo *no* es uniforme.

### El enlace es malo y los medidores no son homogéneos

Dos hechos del parque que condicionan cualquier agenda de pedidos:

**Las respuestas varían enormemente de tamaño.** Algunos modelos devuelven unos pocos
registros; otros, del orden de cientos de miles de datos. Sobre un enlace con tiempos de ida y
vuelta de un par de segundos, eso significa que **un pedido puede tardar segundos o muchos
minutos** según a quién se le pregunte. La duración de una ronda no es un número: es una
distribución que depende de qué modelos le tocaron a ese concentrador.

**Las tramas llegan incompletas con frecuencia**, y hacen falta reintentos. Una respuesta
truncada es más peligrosa que una ausente:

| Cómo se corta | Qué pasa |
|---|---|
| Falta el registro que interesa | Se detecta trivialmente: no está |
| Se corta **en medio de un número** | `014380.81` truncado a `014380.8` es un valor plausible y **diez veces menor**. Ninguna validación de formato lo detecta |

Dos defensas, y conviene usar las dos:

1. El **checksum de la trama** (el BCC que cierra la respuesta), que el protocolo ya provee.
2. **Comparar con la lectura anterior** del mismo medidor: un contador no puede bajar, ni
   saltar un valor imposible en unos minutos. Es exactamente la misma comprobación que la
   etapa de diferenciación necesita para detectar reseteos, así que **no cuesta nada extra**.

### Dos reintentos distintos, que no producen lo mismo

Conviene no confundirlos, porque solo uno genera duplicados:

| Reintento | Qué produce | Quién lo maneja |
|---|---|---|
| **De comunicación** — se vuelve a pedir al medidor | Una lectura **nueva**, en un instante posterior. **No es un duplicado** | La lógica de franja, tolerando que la lectura no esté donde se la esperaba |
| **De publicación** — se vuelve a publicar a Kafka | Un **duplicado real**: mismo instante, mismo valor | La deduplicación |

## Sobre el reloj: lo pone quien pregunta

**El readout no trae timestamp.** En el ejemplo de arriba, ninguna línea dice cuándo se hizo
la lectura, salvo `1.6.0`, que informa cuándo ocurrió *su* máximo.

Entonces el instante lo asigna el **concentrador** al recibir la respuesta, con su reloj, que
está sincronizado. **El reloj del medidor no participa.**

Vale la pena decirlo porque es un cambio respecto de lo que uno esperaría: en los sistemas que
descargan perfil de carga, el reloj del medidor **sí** fecha cada entrada, y como esos relojes
se desfasan, se pierden ante cortes prolongados o nunca se sincronizaron, hay que defenderse
de ellos. **Con readout ese problema no existe**, y no tiene sentido defenderse de él.

Lo que sí queda es que el instante del concentrador es confiable pero **no exacto**: hay
latencia entre el muestreo y la recepción, y un pedido puede resolverse tarde. Eso es lo que
hay que acotar.

## Por qué la franja horaria es un problema de tiempo de evento

La distribuidora quiere cobrar distinto según la hora: no cuesta lo mismo un kWh en hora punta
que de madrugada. Eso obliga a saber **cuándo ocurrió** cada consumo, no cuándo llegó el dato.

Y como los resultados pueden llegar horas después de tomados, en ráfagas y fuera de orden, la
distinción entre tiempo de evento y tiempo de procesamiento deja de ser una sutileza técnica:
**si una medición se asigna a la franja equivocada, al cliente se le factura mal.**

Hoy el sistema hace **un pedido por día**, que alcanza para facturar el consumo diario y no
para discriminar por franja. Habilitar la tarifa horaria exige pedir en cada borde: es el
cambio que este proyecto modela.

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
