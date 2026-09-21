# Decisiones de diseño

Qué se decidió y por qué. Lo que sigue abierto figura como tal.

---

## 1. Fuente: productor sintético

Los eventos los genera un simulador propio, no un replay de datos históricos.

La razón es que el sistema tiene que **demostrar** su comportamiento ante duplicados,
desorden y retrasos. Con un productor sintético esos casos se generan de forma
**determinista y reproducible**: una corrida con la misma semilla produce exactamente los
mismos fallos, y una prueba que falla se puede repetir.

Todos los datos del repositorio son sintéticos. El proyecto no usa datos de ninguna
distribuidora real.

## 2. Cadencia: pedidos en los bordes de franja

**Los medidores de este parque solo exponen el modo readout**: devuelven el valor actual de
sus registros cuando se les pregunta. **No se puede descargar el perfil de carga** (el objeto
`99.1.0`, que guardaría una serie de intervalos fechados por el propio medidor). Ver
[`dominio-medicion.md`](dominio-medicion.md).

Eso determina todo lo demás: **el consumo de una franja se obtiene restando dos lecturas.**

```
consumo(punta) = contador(22:00) − contador(18:00)
```

De ahí se sigue lo que parece un detalle y es el requisito central:

> **Hay que pedir exactamente en los bordes de franja.** Sin una lectura a las 18:00 y otra a
> las 22:00, no existe forma de saber cuánto se consumió en punta.

Hoy el sistema pide **una vez por día**, lo que alcanza para facturar el consumo diario pero
**no permite discriminar por franja**: una sola lectura diaria da un único número. Habilitar
la tarifa horaria exige **aumentar la frecuencia de pedidos**, y ese es el cambio que este
proyecto modela.

Es una diferencia importante con un sistema de perfil de carga, donde la frecuencia de
medición y la de recolección son independientes. Acá **son la misma cosa**: cada medición
existe porque alguien la pidió.

### Modos de falla, que salen del dominio sin inventarlos

| Falla | Qué produce |
|---|---|
| **El pedido se corre.** El de las 18:00 responde a las 18:07 | Siete minutos de punta se atribuyen a resto: error de facturación medible |
| **El pedido falla.** No hay lectura en un borde | El consumo de las dos franjas adyacentes es **indistinguible**: queda un único número combinado. La redundancia de la decisión 5 acota el daño |
| **El concentrador pierde enlace** y publica sus resultados más tarde | Datos tardíos y fuera de orden, en ráfaga |
| **Reintento de publicación** | Duplicados |

## 3. El instante lo pone el concentrador, no el medidor

**El readout no trae timestamp.** Ninguna línea de la respuesta dice cuándo se hizo la
lectura, salvo registros como la demanda máxima, que informan cuándo ocurrió *su* máximo — y
esos no se usan acá.

Por lo tanto **el tiempo de evento lo asigna el concentrador** en el momento en que obtiene la
respuesta, con su propio reloj, que está sincronizado. **El reloj del medidor no interviene en
este diseño**, y por eso no hay que defenderse de él.

> **Decisión revisada el 20/09.** La versión anterior describía un parque de relojes poco
> confiables, con reglas de corrección y cuarentena por desfase. Eso aplica a la **descarga de
> perfil de carga**, donde cada entrada la fecha el propio medidor. **Con readout no aplica**,
> y mantenerlo habría sido defendernos de un problema que este diseño no tiene.

### Lo que sí hay que validar

El instante es confiable, pero **no es exacto**: entre que el medidor muestrea su registro y
que el concentrador recibe la respuesta hay latencia, y un pedido puede resolverse tarde.

| Caso | Qué se hace |
|---|---|
| Instante ausente o mal formado | Cuarentena: sin instante no hay franja posible |
| Instante en el futuro respecto de la recepción | Cuarentena: indica un concentrador desincronizado |
| **Desvío respecto del borde de franja** | ⚠️ **Abierto:** cuánto se tolera antes de considerar que la lectura no sirve para cerrar la franja |

El último es el interesante, y reemplaza al viejo umbral de corrección de reloj. Tiene
consecuencia económica concreta: un pedido que se corre siete minutos mueve siete minutos de
consumo de una franja a la otra.

## 4. Tiempo: hora local con offset explícito

Los timestamps viajan en hora local **con offset explícito**, en ISO-8601:
`2026-09-20T18:00:00-03:00`.

"UTC o local" es una falsa disyuntiva cuando el timestamp lleva el offset:
`2026-09-20T18:00:00-03:00` y `2026-09-20T21:00:00Z` designan el **mismo instante** y las dos
formas son inequívocas. Se elige la local porque se **lee** mejor en la evidencia: un
`18:00-03:00` deja ver de inmediato que esa lectura cae en punta, mientras que con `21:00Z`
hay que hacer la resta mental para verificar que la franja está bien asignada.

Offset y zona horaria no son lo mismo, y se usan los dos:

| | Qué aporta | Dónde vive |
|---|---|---|
| Offset `-03:00` | Qué **instante** es | En cada evento: es parte del formato ISO-8601 |
| Zona IANA `America/Asuncion` | Con qué **regla** se derivan la fecha local y la franja | Una vez, en el contrato y en la configuración |

Se guarda el identificador IANA y no un offset fijo porque el identificador sobrevive a un
cambio de política horaria.

⚠️ **Supuesto:** no rige horario de verano. Si volviera, habría dos días al año con franjas
de duración distinta y la configuración tendría que contemplarlos.

## 5. Franjas configurables, y la agenda de pedidos que exigen

El calendario tarifario es **configuración, no código** (ver
[`config/franjas.example.toml`](../config/franjas.example.toml)).

**La restricción dejó de ser formal y pasó a ser operativa** (revisado el 20/09, tras la
decisión 2). Antes se enunciaba como una condición aritmética sobre una grilla de medición
fija. Con readout no hay grilla: hay una **agenda de pedidos**, y la condición es que esa
agenda **cubra todos los bordes de franja**.

```
para cada borde de franja:  existe un pedido programado en ese instante
```

Así el consumo de cada franja se obtiene restando dos lecturas que caen exactamente en sus
extremos, y la atribución es **exacta** en lugar de aproximada. Una configuración cuyos bordes
no estén cubiertos por la agenda se **rechaza**: no se prorratea en silencio.

Los pedidos **intermedios** entre bordes son libres: dan resolución de curva de carga, sirven
para análisis, y no afectan la facturación.

### Redundancia en los bordes: acotar el daño, no reducir la probabilidad

La agenda **repite el pedido alrededor de cada borde** —por ejemplo en 17:58, 18:00 y 18:02—
en lugar de confiar en uno solo. La cantidad y la separación son configurables.

**Lo que esto resuelve, y es lo importante: el pedido que falla.** Con un único pedido por
borde, si ese falla se pierde la separación entre las dos franjas adyacentes por completo — un
bloque de quince horas indistinguible. Con tres pedidos, si el del medio falla todavía se
conoce el consumo entre 17:58 y 18:02, y la ambigüedad baja **de quince horas a cuatro
minutos**.

No reduce la probabilidad del fallo: **le pone un techo al daño**. Es una diferencia
cualitativa, porque un error acotado se puede declarar y cuantificar, y uno no acotado no.

**Lo que mejora sin resolver: el pedido que se corre.** Como cada lectura viene fechada por el
concentrador, se elige la más cercana al borde y el error residual se achica. Pero siempre
queda alguno.

**El costo.** Tres pedidos en cada uno de cuatro bordes son 12 por medidor por día, contra 96
de la curva de carga completa: sigue siendo un octavo.

⚠️ **Abierto, y es una restricción física:** si el concentrador puede volver a pedirle al mismo
medidor en cuestión de minutos. Con enlaces lentos o rondas largas sobre muchos equipos, puede
que no llegue. La separación entre pedidos redundantes hay que ajustarla a eso.

### Prorratear: el problema es la magnitud, no el principio

La versión anterior de esta decisión rechazaba el prorrateo de plano. La posición honesta es
más fina:

| Situación | Qué supone interpolar | ¿Aceptable? |
|---|---|---|
| Hueco de 4 minutos entre dos lecturas redundantes | Consumo uniforme durante 4 minutos | **Sí**, con el margen declarado |
| Falta el borde y hay que repartir una franja de 4 horas | Consumo uniforme durante 4 horas | **No**: en punta sabemos que no lo es, y es la razón de que la franja exista |

Con redundancia en los bordes los huecos quedan acotados, así que la interpolación pasa a ser
defendible **dentro de un límite explícito**. Por encima de ese límite, el valor se marca como
indeterminado en lugar de inventarse.

⚠️ **Abierto:** cuál es ese límite. Es la misma decisión que el desvío tolerado respecto del
borde, vista desde el otro lado.

### Cuánta frecuencia hace falta

**La facturación necesita exactamente los bordes de franja, ni uno más.** Todo pedido
intermedio compra análisis de curva de carga, no precisión de facturación — y cuesta:

```
pedidos/día = medidores × pedidos por medidor

100.000 medidores ×  4 bordes         →   400 K/día   (mínimo para facturar)
100.000 medidores × 96 (cada 15 min)  →   9,6 M/día   (curva de carga completa)
```

Son **24 veces** de diferencia. Y a diferencia de un sistema de perfil de carga, acá cada
pedido es una **interacción real con un medidor**: no es solo volumen en Kafka, es tiempo de
comunicación y carga sobre el parque. Ese número dimensiona las particiones **y** decide si la
recolección es viable.

## 6. La franja no es una ventana

La franja es una **función pura del tiempo de evento**, no una ventana de Beam. La agregación
usa clave compuesta:

```
clave      = (medidor_id, fecha_local, franja)
ventana    = fija de 1 día
agregación = CombinePerKey → suma de energía por franja
```

El ventaneo de Beam opera siempre sobre el **instante absoluto**; la fecha local y la franja
son campos **derivados** del instante más la zona horaria, y viajan en la clave.

## 7. Ningún medidor acumula por franja

Se asume que **ningún medidor del parque tiene acumulación por tarifa**: el pipeline recibe
energía sin diferenciar y es él quien atribuye cada intervalo a su franja.

Existen medidores que acumulan internamente por franja y exponen los valores en registros
OBIS separados. No se depende de esa capacidad, y **no es una simplificación sino una
consecuencia de la decisión 5**: si las franjas son configurables, apoyarse en la acumulación
del medidor obligaría a reconfigurar todo el parque en campo cada vez que cambia el
calendario tarifario. Haciendo la diferenciación aguas abajo, un cambio surte efecto de
inmediato e incluso permite **recalcular el pasado**.

Franjas configurables y acumulación en el medidor son mutuamente incompatibles.

## 8. Datos tardíos: el resultado en vivo es provisional

La facturación ocurre días después del cierre del período, así que una llegada tardía **no
corrige algo ya facturado**. Eso define la política temporal:

1. La **lateness permitida debe cubrir la cadencia de recolección** — del orden de 24 a 48
   horas, no minutos. Es lo que permite que la ventana converja antes del corte.
2. El agregado en vivo es **provisional y se actualiza con cada llegada tardía**: modo
   **acumulativo** con *upsert* sobre una clave estable.
3. **Dos consumidores del mismo tópico con patrones distintos:** el tablero en vivo lee todos
   los panes y muestra un valor que cambia; la facturación lo lee una sola vez, pasado el
   horizonte de convergencia.

El costo del estado no es despreciable acá. Con ventanas diarias, ~48 h de lateness y claves
`medidores × franjas`:

```
100.000 medidores × 3 franjas × 3 ventanas abiertas × ~100 B ≈  90 MB
1.000.000 medidores                                          ≈ 900 MB
```

## 9. Deduplicación

Clave de deduplicación: **`(medidor_id, instante_lectura)`** — estable y determinista.

⚠️ Es `instante_lectura` y no `inicio_intervalo`: lo que llega por el tópico de entrada son
**lecturas del contador en un instante**, no consumos de un bloque (decisión 10). El intervalo
aparece recién después de diferenciar.

Los duplicados son intrínsecos al dominio: si una descarga se corta a la mitad y se
reintenta, los mismos intervalos llegan dos veces. La deduplicación usa estado por clave con
**temporizador de expiración**, acotado al mismo horizonte que la lateness permitida; no un
conjunto que crece sin límite.

## 10. El medidor entrega contador acumulado: el pipeline diferencia

**Decidido.** El medidor reporta el registro OBIS **`15.8.0`**, que es un **contador
acumulado**: cada lectura es el valor del contador en ese instante, no el consumo del
intervalo. El consumo se obtiene restando la lectura anterior del mismo medidor.

```
consumo(intervalo_n) = lectura(intervalo_n) − lectura(intervalo_n−1)
```

Ver [`dominio-medicion.md`](dominio-medicion.md) para el contexto completo de OBIS y la curva
de carga.

**Por qué no se simplifica a "el evento trae el consumo del intervalo":** porque no es lo que
entrega un medidor real, y el tratamiento de la diferencia es justamente donde está el
trabajo. Simplificarlo dejaría el pipeline sin nada sustantivo que hacer entre leer y agregar.

**Las tres consecuencias**, que son material de "límites conocidos":

1. **Necesita estado por medidor** para recordar la última lectura. Se apoya en el mismo
   mecanismo de estado con temporizador que ya usa la deduplicación (decisión 9).
2. **Un evento perdido arruina dos intervalos, no uno.** Si falta la lectura de las 18:15, no
   se puede calcular el consumo de 18:00–18:15 *ni* el de 18:15–18:30. ⚠️ **Abierto:** si esos
   dos intervalos se marcan como indeterminados o si se imputa el consumo combinado al bloque
   completo — correcto en total, pero puede caer sobre dos franjas distintas.
3. **El contador se resetea** al cambiar o reprogramar un medidor, y la resta da un consumo
   negativo enorme. Como en el mercado modelado no hay compra de energía al usuario, no hay
   exportación y el contador solo puede subir: **un consumo negativo siempre es un reseteo y
   nunca una medición válida**. Se detecta y va a cuarentena.

### Salida de emergencia, declarada de antemano

La diferenciación se implementa como una **etapa aislada y temprana** del pipeline, con un
interruptor de configuración que la saltea. Si no está funcionando a tiempo, el simulador
emite consumo por intervalo directamente y la etapa se desactiva. Está diseñada así a
propósito: es la pieza que acopla el pipeline al trabajo de estado, y conviene poder
desacoplarla sin rehacer nada.
