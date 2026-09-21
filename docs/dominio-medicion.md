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

**La unidad viaja en el dato**, no hay que inferirla de una tabla de códigos: el medidor la
informa. Por eso el contrato la guarda tal como llegó, y validarla contra el código es
opcional — sirve para detectar un equipo mal configurado, no para saber en qué unidad está
el valor.

⚠️ **Pero el `*` no es un separador de valor y unidad: es un separador de campos genérico**,
y la cantidad y el orden de los campos **varía entre fabricantes**:

```
1.6.0(0003.0844*kW)(26-05-08 23:00:00)      valor · unidad, y el instante en un grupo aparte
15.6.0(005.180*26-05-06*kW)                 valor · fecha · unidad, todo en el mismo grupo
```

Partir por `*` y tomar el segundo campo como unidad funciona con el primer equipo y
**devuelve una fecha como unidad** con el segundo. Cualquier parser tiene que interpretar los
campos por su contenido —o por el modelo del equipo— y no por su posición.

Este proyecto no parsea tramas: el productor emite JSON con la unidad en un campo propio.
Eso es justamente lo que vuelve el problema inexistente aguas abajo, y es un argumento a
favor de que el contrato la lleve explícita en lugar de dejarla implícita en una posición.

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

### Las cabinas y el bus compartido

Los medidores no cuelgan de la red uno por uno: están agrupados en **cabinas**, y dentro de
cada cabina se comunican por un **bus RS-485**. Un bus RS-485 es compartido —solo un
dispositivo habla a la vez—, así que **los medidores de una cabina se leen en secuencia**, uno
después del otro. Cabinas distintas sí se consultan en paralelo.

Cada medidor tiene un **tiempo máximo configurable** para responder (del orden de dos
minutos), dentro del cual se hacen reintentos. Agotado ese tope, se pasa al siguiente.

De ahí sale el número que gobierna todo el diseño temporal:

```
duración de una ronda ≈ medidores de la cabina × tiempo por medidor
```

Y las cabinas son grandes. En un parque típico, **la mayoría de los medidores está en cabinas
de 30 a 100 equipos**, con una cola de cabinas de más de cien. Con un tiempo por medidor de
apenas medio minuto, una cabina de 50 a 99 tarda cerca de **40 minutos** en completar una
ronda.

**Consecuencia directa: no se puede leer a todos los medidores en el borde de una franja.**
El bus está ocupado con los demás. Un medidor cualquiera va a tener su lectura en algún punto
de la ronda, no donde uno quisiera.

Lo bueno es que **el desvío es sistemático, no aleatorio**: como la cabina se recorre en
orden, el medidor que ocupa la posición *k* de *N* se lee siempre alrededor de la misma
fracción de la ronda. Es predecible, y por lo tanto compensable.

### Los fallos están correlacionados

El bus y el enlace de la cabina son **compartidos**. Si la cabina se cae, **se caen todos sus
medidores a la vez**, para el mismo borde. No se pierde la franja de un cliente: se pierde la
de una cabina entera, que pueden ser decenas o cientos.

Por eso el `cabina_id` viaja en el evento aunque la clave de particionamiento sea el medidor:
sin él no se puede diagnosticar ni explicar una pérdida masiva, ni distinguirla de un problema
del pipeline.

### El enlace es malo y los medidores no son homogéneos

Dos hechos del parque que condicionan cualquier agenda de pedidos:

**El tiempo de un pedido es trimodal.** Medido sobre ~410.000 pedidos en 7 días:

| Resultado | Duración | Frecuencia |
|---|---|---|
| Éxito al primer intento | 5–10 s | **85,5%** |
| Éxito tras reintentar | 30–45 s | 3,8% |
| Muro del tope de tiempo | 120 s | **9,5%** |

Los valles entre las modas son reales (1,2% combinado): la forma es discreta, no una campana.

**Eso cambia cuál es la variable que importa: la duración de una ronda la fija la tasa de
fallas, no la velocidad media.** Casi todo responde en segundos; lo que consume el bus es el
9,5% que agota el tope de dos minutos. Un parque de equipos veloces con mal enlace tarda mucho
más que uno de equipos mediocres con buen enlace.

### La falla tiene tres estructuras, no una

Cruzando cada pedido con la alcanzabilidad de red de su enlace:

| Clase de enlace | % de medidores | Entrega dato | % de las fallas |
|---|---|---|---|
| Caído | 4,1% | 16,9% | 38% |
| Intermitente | 4,4% | 80,3% | 10% |
| **Sano** | 91,5% | 94,8% | **52%** |

**La caída de cabina es real y persistente:** 41 equipos muertos arrastran 1.203 medidores,
unos 29 cada uno, que es el tamaño de cabina típico. Confirma que el fallo correlacionado por
cabina existe y no es momentáneo.

⚠️ **Pero es menos de la mitad del problema.** El **52% de las lecturas fallidas ocurre sobre
enlaces que pinguean perfecto.** Un modelo que solo contemple la caída de cabina le falta la
mitad de la realidad: hace falta también un **fondo disperso** sobre equipos sanos.

### Y un tercer eje: la correlación por modelo de equipo

Con el mismo enlace y la misma configuración, **las tasas de entrega por modelo van de 65% a
94%**. Como los modelos están repartidos por todo el parque, esa correlación es
**espacialmente dispersa**: ninguna partición por ubicación la aísla.

Para un pipeline particionado por cabina, **ese patrón es invisible**. Es probablemente el
hallazgo más interesante del dominio para un trabajo de streaming: hay una estructura real en
los datos que la clave de particionamiento elegida no puede ver.

*(No se sabe por qué esos modelos fallan más: se descartaron red, configuración y truncamiento
de buffer. Para simular da igual — se modela como tasa por modelo, que es lo observado.)*

### Los reintentos: pocos, y no siempre sirven

**La media es 1,51 intentos por pedido**: la gran mayoría acierta al primero. Varía por
modelo entre 1,07 y 3,97, y eso es costo de bus directo — el peor modelo ocupa unas 4 veces lo
que el mejor, por lectura.

⚠️ **El presupuesto es tiempo, no un contador de intentos.** Lo que se configura es el límite
de tiempo por pedido, y dentro de él se reintenta las veces que entren. El máximo observado de
11 intentos es simplemente cuántos ciclos caben en ese presupuesto: **no es un tope configurado
ni una constante del sistema**, y modelarlo como contador se alejaría de la realidad.

⚠️ **Reintento y éxito están desacoplados.** No vale "más reintentos, peor modelo":

- un modelo hace 3,97 intentos y entrega 89,5%;
- otro hace 3,12 y entrega 65,5%;
- y el mejor entregador de todos está **por encima** de la media de intentos.

Para unos modelos reintentar funciona y para otros es tiempo tirado. **Asumir que el reintento
eventualmente rescata la lectura es falso para una parte del parque.**

Dato estructural: los reintentos ocurren **dentro del mismo pedido**. Un pedido que falló ya
agotó su presupuesto — no hay una segunda oportunidad programada después.

### La latencia es del enlace, y es plana a lo largo del día

RTT de red: p50 **1.665 ms**, p90 2.290, p99 3.661, 92,9% alcanzable. Latencia hasta la
primera respuesta del medidor: p50 2.260 ms. O sea que **el enlace explica ~74%** y el equipo
agrega unos 600 ms. Por eso se modela **por cabina**: las latencias están correlacionadas y no
se promedian.

✅ **No hay degradación en hora punta.** Medido en las 24 horas sobre 30 días: p50 entre 1.592
y 1.699 ms (±3%), alcanzabilidad entre 90,6% y 93,6%. Es buena noticia para este proyecto: la
ronda no depende de la hora, así que **el error de atribución no empeora justo en la franja que
más cuesta**, y el simulador no necesita término diurno.

Dos cosas más, contraintuitivas:

- **La cola larga no es de la red.** El p99 del ping es 3,7 s, pero el p99 de la primera
  respuesta detrás del gateway RS-485 es **23 s**. Los atascos son del gateway o del medidor.
- **Un enlace está arriba y rápido, o está caído.** La disponibilidad por equipo es bimodal
  —el 90% está ≥90% alcanzable, el 4,7% está muerto, el medio casi vacío— y el RTT **no**
  correlaciona con la disponibilidad. **No existe la población "enlace lento degradado".**

### ⚠️ La bandera de calidad que no significa corrupción

Es la trampa más peligrosa del dominio, y la más fácil de leer al revés.

En el reparto crudo de resultados, la categoría más grande —**~50% de las lecturas**— es una
marca de **checksum incorrecto**. Leerlo como corrupción es equivocarse **por un factor de
cinco**: la tasa real de fallas es 9,2%.

Lo que pasa es que un fabricante que es el 55% del parque **calcula el checksum distinto de lo
que el concentrador espera**. El dato se extrae completo y correcto, pero sale marcado.
Prácticamente el 100% de las lecturas de ese fabricante lleva la marca, y la aritmética cierra
al decimal.

**Las consecuencias para el pipeline son grandes:**

1. Un pipeline que **descarte por bandera de calidad tiraría la mitad de las lecturas buenas**.
2. La bandera es **sistemática y correlacionada por fabricante**, no aleatoria: no se puede
   tratar como ruido.
3. Describir eso como "problema de la red" sería atribuirle a la infraestructura un desajuste
   entre dos implementaciones del mismo estándar.

Por eso el contrato distingue valores de `calidad` en lugar de tener un booleano: hay una marca
que **no implica pérdida de dato** y otra, el truncamiento, que sí.

### Tamaño de las tramas

Del orden de **100–200 bytes** para un medidor monofásico, y hasta **~500** para un trifásico
con tensiones, corrientes y factor de potencia por fase. Un readout trae típicamente **entre 5
y 13 registros OBIS**, no decenas.

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
