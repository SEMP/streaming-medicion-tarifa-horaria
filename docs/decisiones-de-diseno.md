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
| **Trama incompleta**: falta el registro que interesa | Cuarentena: no aporta al cálculo |
| **Trama truncada en medio de un número** | No se detecta por formato. Se ataca con el checksum de la trama y con la comparación contra la lectura anterior: un contador no baja ni salta un valor imposible |
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

### Rondas continuas e interpolación, no pedidos en el borde

**Decidido el 20/09, con los números del parque a la vista, y revierte la idea anterior de
repetir pedidos alrededor de cada borde.**

Como los medidores de una cabina comparten un bus RS-485 y se leen en secuencia, la ronda dura
`medidores × tiempo por medidor`. Con la mayoría de los medidores en cabinas de 30 a 100 y un
tiempo por medidor de medio minuto, eso da rondas de **20 a 40 minutos**. El bus no está libre
para volver a preguntarle al mismo medidor tres veces alrededor de las 18:00: está ocupado con
los otros cincuenta.

**Entonces no se pide en el borde. Se pide en rondas continuas, y el consumo de cada franja se
obtiene interpolando entre las dos lecturas que rodean el borde.**

Cuánto cuesta *no* interpolar, es decir tomar la lectura más cercana al borde, sobre una franja
punta de cuatro horas. La duración de la ronda **no la fija la velocidad de los medidores sino
la tasa de fallas**, porque los reintentos ocupan el bus y retrasan a todos los que vienen
detrás — así que el error depende de las dos cosas:

| Tamaño de cabina | 3% de fallas | 10% | 25% | 50% |
|---|---|---|---|---|
| 10–19 | 1 min · 0,5% | 2 min · 0,7% | 3 min · 1,2% | 7 min · 2,9% |
| 20–29 | 2 min · 0,9% | 3 min · 1,2% | 6 min · 2,3% | 12 min · 5,1% |
| 30–49 | 3 min · 1,4% | 5 min · 2,0% | 9 min · 3,8% | 20 min · 8,3% |
| **50–99** | **6 min · 2,7%** | **9 min · 3,8%** | **18 min · 7,3%** | **37 min · 15,2%** |
| 100–199 | 13 min · 5,5% | 19 min · 7,9% | 35 min · 14,4% | 73 min · 30,5% |

⚠️ **La tasa de fallas no está calibrada.** Las mediciones disponibles de comunicación real
muestran la *forma* de cada caso —o el medidor responde en unos 4 segundos, o cae en una
escalera de reintentos que consume decenas de segundos— pero **no permiten estimar con qué
frecuencia** ocurre cada uno. Es el parámetro más influyente del modelo y el primero que
habría que medir en operación.

Eso convierte el resultado en algo más útil que un número suelto: el proyecto no dice "el
error es tanto", dice **"el error es esta función de la calidad del enlace"** — y de paso deja
claro qué habría que medir para fijarlo.

Aun en el escenario optimista, con 3% de fallas, las cabinas grandes ya pierden varios puntos
porcentuales. **Por eso la interpolación es obligatoria y no una optimización.**

> Sobre la viabilidad: incluso en el peor caso, con todos los medidores agotando el tope de dos
> minutos, la cabina más grande completa casi cinco rondas diarias — suficiente para cubrir
> cuatro bordes de franja. **Facturar por franja es físicamente posible en todo el parque.** Lo
> que está en juego es la precisión, no la factibilidad.

### Cada resultado declara su propia incertidumbre

Como la ronda dura distinto en cada cabina, **el error de atribución es distinto para cada
medidor y es calculable**: lo acota la separación entre las dos lecturas que rodean el borde.

Ese número viaja **en el registro de salida**. El tablero lo muestra, y la facturación decide
si lo acepta. Es más honesto y más útil que declarar un límite global en un párrafo del
documento: acá cada valor dice cuánto se puede confiar en él.

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

## 11. Licencia: MIT

**Decidido el 20/09.** El proyecto se publica bajo [MIT](../LICENSE), con los tres integrantes
como titulares.

**Por qué una licencia permisiva y no copyleft.** Uno de los destinos previstos del simulador
es incorporarlo a un sistema de trabajo. Con GPL o AGPL eso obligaría a liberar ese sistema
entero; con MPL, los archivos modificados. Ninguna de las dos sirve para ese objetivo. Las
permisivas —MIT, Apache-2.0— permiten que una obra derivada sea cerrada.

**Por qué MIT y no Apache-2.0.** Se evaluaron las dos y en un primer momento se eligió
Apache-2.0, por su **concesión expresa de derechos de patente** (§3), que suele ser lo que una
revisión legal corporativa verifica antes de incorporar código externo. MIT no dice nada al
respecto: se suele interpretar que hay una licencia implícita, pero no está escrito.

Se optó finalmente por **MIT por simplicidad**: su única obligación es conservar el aviso de
copyright, mientras que Apache-2.0 agrega marcar los archivos modificados (§4b) y reproducir el
archivo `NOTICE` en las obras derivadas (§4d).

**Lo que se resigna, dicho explícitamente:** la concesión expresa de patentes. Si alguna vez una
revisión legal pregunta por ella, la respuesta será que MIT no la contempla.

**Sin sentido alguno en cualquiera de las dos:** ninguna obliga a que las obras derivadas usen
la misma licencia.

---

# Posibles mejoras

Lo que este diseño **no** hace, con su razón. No entra en el alcance del trabajo; se documenta
porque conocer el camino que se descartó es parte de justificar el que se tomó.

## Un dispositivo de lectura por medidor

La limitación de fondo de este diseño es el **bus compartido**: los medidores de una cabina se
leen en secuencia, la ronda dura decenas de minutos, y de ahí sale todo el error de
atribución que el pipeline tiene que acotar e informar.

Un dispositivo de lectura **dedicado a cada medidor** elimina esa limitación de raíz. Al no
compartir bus con nadie, puede consultar en los bordes exactos de cada franja, o incluso
acumular los parciales por franja localmente y exponerlos ya separados.

**Este trabajo incluye una prueba de concepto de ese escenario.** El simulador genera las dos
arquitecturas de recolección —bus compartido y dispositivo dedicado— y el **mismo pipeline, sin
cambios**, procesa ambas. La diferencia de error entre una y otra queda **medida**, no estimada.

**Lo que eso ahorraría, según la estimación previa:**

| Tamaño de cabina | Error de atribución hoy | Con dispositivo dedicado |
|---|---|---|
| 30–49 | 1,4% – 8,3% | ~0 |
| 50–99 | **2,7% – 15,2%** | ~0 |
| 100–199 | 5,5% – 30,5% | ~0 |

El rango va del escenario con 3% de fallas al de 50%. Un dispositivo dedicado elimina las dos
causas a la vez: no comparte bus, así que ni la cantidad de medidores ni la tasa de fallas de
los vecinos lo afectan.

Ese es el argumento económico de la inversión, y es un resultado del pipeline: sin medir el
error actual no se puede justificar el gasto de eliminarlo.

### Si se hace, conviene exponerlo en códigos OBIS propios

Un dispositivo que calcula los parciales por franja **no debería publicarlos en los registros
tarifarios estándar** (`1.8.1`, `1.8.2`…), sino en códigos propios. Tres razones:

1. **Auditabilidad.** Con registros estándar es imposible saber después si la separación por
   franja la hizo el medidor o el dispositivo. Con códigos propios, la procedencia del dato es
   explícita.
2. **Reconciliación, que es la más valiosa.** Permite leer las dos cosas —el total acumulado
   del medidor y los parciales del dispositivo— y **verificar que sumen**. Un dispositivo que
   se desincroniza o pierde un intervalo se detecta solo, porque su suma deja de coincidir con
   el total. Esa validación se pierde por completo si los valores son indistinguibles.
3. **El despliegue sería mixto.** Durante la transición convivirían medidores con dispositivo y
   sin él, y el pipeline necesita saber cuál es cuál para decidir si tiene que diferenciar o si
   los parciales ya vienen dados.

### Y arrastra un requisito

El dispositivo hereda el problema de la decisión 7: si acumula por franja, un cambio en el
calendario tarifario obliga a reconfigurarlos todos. Con configuración remota eso es viable —
sin ella, el esquema vuelve a ser incompatible con franjas configurables, que es justamente la
razón por la que hoy la atribución se hace aguas abajo.

## Medición neta

Si se introdujera compra de energía al usuario, `15.8.0` dejaría de equivaler a consumo: habría
que leer `1.8.0` y `2.8.0` por separado, facturar cada sentido con su tarifa, y la regla de que
un consumo negativo siempre es un reseteo de contador dejaría de valer. Ver la decisión 10.
