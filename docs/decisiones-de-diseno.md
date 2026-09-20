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

## 2. Cadencia: curva de carga, no lectura puntual

El medidor registra su consumo en intervalos regulares y **acumula** los registros; el
sistema los descarga cuando el equipo logra conectarse. Cada descarga trae un lote que cubre
el período desde la anterior: con intervalos de 15 minutos y descarga diaria, 96 registros
que abarcan 24 horas.

Esto importa porque **el desorden y el retraso no hay que inventarlos: son intrínsecos a
cómo funciona la recolección**. Al conectarse un medidor entran de golpe eventos cuyo tiempo
de evento se extiende un día hacia atrás.

La **frecuencia de medición es configurable**: tanto el intervalo de la curva de carga como
la cadencia de descarga son parámetros. Para la demostración se comprime el tiempo —
intervalos simulados de 15 minutos y descargas cada pocos minutos de reloj real.

### Cuánta frecuencia hace falta

Es contraintuitivo, así que conviene decirlo: **la frecuencia que necesita la facturación la
fija el borde de las franjas, no el deseo de tener más datos.** Si las franjas cambian solo
en hora en punto, lecturas horarias facturan igual de bien que las de cinco minutos. Todo lo
que esté por debajo de esa granularidad compra análisis de curva de carga, no precisión de
facturación — y cuesta:

```
eventos/día = medidores × (1440 / intervalo_en_minutos)

100.000 medidores @ 15 min →  9,6 M/día
100.000 medidores @  5 min → 28,8 M/día
```

Ese número es el que dimensiona las particiones de Kafka.

## 3. Relojes poco confiables: el gateway valida antes del watermark

El simulador conoce el tiempo de evento **verdadero**, pero lo que el medidor *reporta* puede
estar mal. Los casos que se generan:

| Caso | Qué emite el medidor |
|---|---|
| Normal | timestamp correcto |
| Sin timestamp | el campo viene ausente |
| Sin offset | hay hora, pero no de qué huso |
| Reloj desfasado | drift de minutos u horas |
| Reloj absurdo | timestamp en 1970 o en el futuro |

La validación **corrige con una regla documentada o desvía a cuarentena**, y ocurre **antes**
de que el evento participe del cálculo del watermark. El motivo es concreto: un solo medidor
con el reloj adelantado arrastraría el watermark hacia el futuro y provocaría el descarte de
lecturas legítimas de todos los demás medidores.

⚠️ **Abierto:** el umbral exacto a partir del cual un desfase se corrige en lugar de mandarse
a cuarentena, y contra qué referencia se corrige.

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

## 5. Franjas configurables, alineadas a la grilla

El calendario tarifario es **configuración, no código** (ver
[`config/franjas.example.toml`](../config/franjas.example.toml)).

**Restricción que se adopta:** los bordes de las franjas deben estar alineados a la grilla de
intervalos. Como el intervalo también es configurable, la validación es sobre la relación
entre ambos:

```
(borde de franja) módulo (intervalo de medición) == 0
```

Así **ningún intervalo de medición cruza un borde** y la atribución es exacta por
construcción. Un calendario mal alineado se **rechaza**.

**Por qué no prorratear** un intervalo que cruza un borde: prorratear asume consumo uniforme
dentro del intervalo, y los datos no respaldan ese supuesto. Queda disponible como opción
configurable, pero no como comportamiento por defecto.

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

Clave de deduplicación: **`(medidor_id, inicio_intervalo)`** — estable y determinista.

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
   negativo enorme. Como se asume que no hay generación distribuida, **un consumo negativo
   siempre es un reseteo y nunca una medición válida**: se detecta y va a cuarentena.

### Salida de emergencia, declarada de antemano

La diferenciación se implementa como una **etapa aislada y temprana** del pipeline, con un
interruptor de configuración que la saltea. Si no está funcionando a tiempo, el simulador
emite consumo por intervalo directamente y la etapa se desactiva. Está diseñada así a
propósito: es la pieza que acopla el pipeline al trabajo de estado, y conviene poder
desacoplarla sin rehacer nada.
