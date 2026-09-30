# Guion del video

El enunciado pide cuatro cosas de la demostración, y el guion está armado para que **ninguna
quede implícita**:

| Lo que pide | Dónde se cumple |
|---|---|
| Video breve del pipeline end-to-end | Escenas 4 y 5 |
| Evidencia visible de normales, duplicados y tardíos | Escena 3, y otra vez en la 5 |
| Caso de uso, arquitectura, decisiones y resultados | Escenas 1, 2 y 6 |
| | |
| **Para la defensa** | Clara escribió una versión extendida con el fondo conceptual de cada escena y las preguntas probables del profesor, con su respuesta. Está en su repositorio, en `TPF/guion_video_detallado.md` |
| Integrantes y contribuciones | Escena 7 |

**Las diapositivas están en [`presentacion/`](presentacion/)**: el PDF y
[`notas.md`](presentacion/notas.md), con el texto para leer en cada diapositiva y **quién
presenta cada una**. Agregan dos diapositivas que este guion no tiene —garantías y límites,
y la evidencia en números— y un anexo para la defensa que no va en el video. Si el guion y las
notas no coinciden en quién habla, mandan las notas: se repartió para que los tres presenten
parecido.

## Cuánto tiene que durar

**Atención — La cátedra no fija una duración.** Dice «video **breve** o demostración en vivo», y nada
más — ni el enunciado, ni la consigna del aula, ni ninguna lámina de las ocho clases. Conviene
saberlo antes de recortar algo por creer que hay un límite: no lo hay.

**Estas siete escenas suman 9 min 45 s** (60 + 90 + 150 + 45 + 120 + 90 + 30 s). No es un
objetivo, es lo que sale de darle a cada tema el tiempo que necesita. Parece razonable para
«breve» en un trabajo que integra ocho clases, pero es criterio nuestro.

Si hubiera que acortar, **sale de las escenas 2 y 6**, que son explicación. Las escenas 3 y 5
no se tocan: son la evidencia, y es lo que el criterio 6 puntúa.

**Hablan los tres.** El criterio pide contribuciones por persona, y que cada uno
explique su parte es la forma más creíble de mostrarlas. Cada escena dice quién habla.

---

## Antes de grabar

Esto es lo que evita los tiempos muertos, que es lo que arruina un video técnico.

```bash
# 1. Dejar el stack levantado y caliente ANTES de grabar. El primer build tarda
#    varios minutos y el primer trabajo de Beam paga el arranque de la JVM.
docker compose -f infra/docker-compose.yml up -d --build

# 2. Correr una vez el recorrido completo, para que las imágenes y los artefactos
#    ya estén en caché. Lo que se grabe después va a ser mucho más rápido.
docker compose -f infra/docker-compose.yml --profile e2e run --rm -T extremo-a-extremo

# 3. Terminal grande y legible: fuente en 16 pt o más, fondo claro.
#    Nadie puede leer una terminal de 10 pt en un video comprimido.
```

Tener abiertos y listos: el diagrama `docs/diagramas/arquitectura.svg`, el PDF del documento
técnico, y <http://localhost:8081> (la interfaz de Flink).

---

## Escena 1 — El problema · Sergio · 60 s

**Qué se ve:** la cara de quien habla, o el título del repositorio.

> Una distribuidora eléctrica necesita cobrar la energía a precios distintos según la hora del día. Para eso hace
> falta saber cuánta energía consumió cada cliente **en cada franja horaria** — y ese dato hoy
> no existe.
>
> No existe por una razón física. Los medidores de una cabina comparten un bus RS-485 y se
> leen **en secuencia**, de a uno. El concentrador da vueltas preguntando, y una vuelta
> completa tarda entre 6 y 60 minutos según cuántos medidores tenga la cabina y cuántos
> fallen. No hay forma de leerlos a todos a la vez.
>
> Entonces una lectura casi nunca cae justo sobre el borde de una franja. Cae en el medio, y
> hay que repartir. Eso introduce un error, y **ese error es plata**.

**El punto que tiene que quedar:** el sistema no entrega solamente el consumo por franja.
Entrega también **cuánto se puede confiar en ese número**, que es lo que permite discutirlo.

---

## Escena 2 — La arquitectura · Clara · 90 s

**Qué se ve:** `docs/diagramas/arquitectura.svg` en pantalla completa.

Recorrer el diagrama de izquierda a derecha, sin leerlo: contar **por qué** cada caja está ahí.

- **El simulador** ocupa el lugar del concentrador. Genera datos deterministas y, sobre todo,
  **inyecta fallas a propósito**: pedidos que se corren, cabinas que caen enteras, tramas
  truncadas, duplicados y ráfagas tardías.
- **Kafka en dos capas.** El tópico crudo conserva las lecturas tal como llegaron, claveadas
  por medidor para que su orden se preserve. El derivado lleva el resultado, claveado por la
  celda que identifica.
- **El pipeline**, Beam sobre Flink, con las etapas numeradas del diagrama.
- **La cuarentena**, que recibe todo lo que no se puede procesar, contado y con su motivo.

**Dos decisiones para nombrar acá**, porque son las que se defienden:

1. **La clave es el medidor y no la cabina.** La cabina agruparía la ronda, pero van de 1 a 199
   medidores: las particiones quedarían desbalanceadas en un factor de 200.
2. **La franja no es una ventana.** Es función pura del tiempo de evento, así que se calcula y
   viaja en la clave. Si fuera ventana, cambiar el calendario tarifario obligaría a redefinir
   el ventaneo.

---

## Escena 3 — Los tres escenarios · Daniel (o Sergio) · 150 s

**La escena más importante del video.** Es donde se cumple «evidencia visible de eventos
normales, duplicados y tardíos».

```bash
uv run python -m pipeline.demostracion
```

No necesita Docker y **termina en dos segundos**: toda la salida aparece de golpe. Así que la
mecánica es correrlo, volver arriba, y **recorrer los tres actos hacia abajo** explicando cada
tabla. Conviene tener la terminal con suficiente historial para que entre entera.

**Acto 1.** Tres lecturas normales. El contador es acumulado, así que el consumo sale de
restar. Señalar que el segundo intervalo **cruza las 18:00**, que es donde empieza `punta`, y
que por eso las dos celdas salen marcadas como `interpolado`.

**Acto 2.** Llega otra vez la lectura de las 17:55, idéntica. Es un reintento de publicación.

> Miren la tabla: **no se movió**. Y algo que no se ve pero importa: no apareció ningún
> intervalo de 0 kWh. Si dedujéramos *después* de diferenciar, el duplicado se restaría contra
> sí mismo, daría 0, y ese 0 pisaría el valor bueno.

**Acto 3.** Llega tarde la lectura de las 18:00, justo sobre el borde. Acá está el remate:

> El total no cambió: 5,500 kWh antes y después. Medir con más detalle no crea ni destruye
> energía. Lo que cambia es **a qué franja se le atribuye** — y cambió en 0,100 kWh.
>
> Ese número es el error de atribución, que es lo que este proyecto existe para medir. No es un
> defecto del pipeline: es la consecuencia de que el intervalo cruzaba el borde y hubo que
> suponer potencia constante. La lectura tardía es la que revela cuánto se erró.
>
> Y fíjense en la última columna: pasó de `interpolado` a `medido`. Como la lectura cayó justo
> sobre el borde, ya no hubo nada que estimar.

---

## Escena 4 — Que la infraestructura es real · Sergio · 45 s

**Qué se ve:** <http://localhost:8081>, la interfaz de Flink, y después la terminal.

> Esto no corre en un simulador de Beam: corre en un Flink de verdad, con dos TaskManagers.
> Kafka también es real, en modo KRaft.

```bash
docker compose -f infra/docker-compose.yml --profile humo run --rm -T humo
```

Mientras corre, contar qué prueba y qué **no**:

> Esta es la prueba de humo. Verifica el **cableado** —que KafkaIO levanta, que Flink acepta el
> trabajo, que los bytes entran y salen— con un passthrough, sin lógica de dominio. Sirve para
> separar «el pipeline está mal» de «la infraestructura está mal», que son dos problemas muy
> distintos.

Termina en `entraron 40 · salieron 40` y código de salida 0.

---

## Escena 5 — El recorrido completo, con la lógica puesta · Clara · 120 s

```bash
docker compose -f infra/docker-compose.yml --profile e2e run --rm -T extremo-a-extremo
```

Mientras corre —alrededor de un minuto y medio con el stack ya caliente— explicar qué pregunta
responde. Es el único momento del video con espera, y alcanza justo para esto:

> La demostración de recién probaba la lógica con el runner local. La prueba de humo probaba el
> cableado. Falta la pregunta que ninguna de las dos responde: **¿la lógica da lo mismo cuando
> la ejecuta Flink?**
>
> No es una pregunta retórica. El runner portable serializa las funciones y el estado hacia
> procesos que no comparten memoria con el que arma el pipeline. Hay cosas que andan en local y
> no allá.
>
> Esto siembra **las mismas cinco lecturas** de la escena 3 —con el duplicado y la tardía— pero
> a través de Kafka, y exige el mismo resultado.

Cuando termina, señalar las tres cosas:

```
  MED-0042|2026-09-25|punta             3.100      3.100  medido OK
  MED-0042|2026-09-25|resto             2.400      2.400  medido OK
  TOTAL                                 5.500      5.500
```

1. **Los mismos números** que con el runner local.
2. **La cuarentena quedó vacía** — nada se perdió por el camino.
3. **Cinco mensajes para dos celdas.** Son las revisiones sucesivas, una por cada lectura que
   cambió algo. El consumidor hace *upsert* y se queda con las dos últimas. Es la semántica del
   contrato funcionando sobre el stack real.

### Y el remate: releer lo mismo desde cero

Va inmediatamente después, sin bajar el stack:

```bash
docker compose -f infra/docker-compose.yml --profile e2e run --rm -T repeticion
```

> Esto no vuelve a sembrar nada. Relee **el mismo tópico desde el offset 0**, con otro grupo de
> consumidor, y vuelve a procesarlo entero.
>
> Da las mismas dos celdas: 2,400 y 3,100. **Eso es la idempotencia, medida en lugar de
> declarada.** Reprocesar no duplica, no suma y no corrige: converge al mismo resultado, porque
> la salida se identifica por la celda y no por el intento de escritura.

Es la evidencia más fuerte del criterio de confiabilidad, y cuesta un comando.

---

## Escena 6 — Lo que aprendimos · Sergio · 90 s

**Es la parte que distingue el trabajo.** Contar los dos errores que
encontramos **en nuestro propio código**, porque son la demostración de por qué el enunciado
insiste en que una corrida feliz no alcanza.

> El enunciado dice que una ejecución exitosa con datos ideales no es evidencia suficiente.
> Nosotros lo comprobamos sobre nuestro propio pipeline, dos veces.
>
> **El primero:** cuando llega una lectura tardía, el intervalo se parte en dos mitades — pero
> el intervalo grosero ya había salido, y Beam no tiene retractaciones. Creíamos que el *upsert*
> lo resolvía. No lo resuelve: opera sobre la celda, y los tres intervalos caen dentro de la
> misma celda. Sumaba 12 kWh donde el consumo real era 6. El doble, sobre un dato que se
> factura.
>
> **El segundo, peor:** la corrección de ese error también estaba mal. Encadenar dos
> agregaciones bajo un trigger acumulativo cuenta doble, porque cada pane de la primera llega a
> la segunda como un elemento nuevo. Y eso **no se ve con `TestStream`** avanzando el watermark
> a infinito, porque dispara un solo pane. Apareció recién al conectar la cadena a Kafka.
>
> Ninguno de los dos aparece con datos ideales. El primero necesita una lectura tardía; el
> segundo, además, que la ventana dispare más de una vez.

Cerrar con la conclusión que el sistema habilita y que no estaba en el enunciado:

> Como la separación entre lecturas la fija la duración de la ronda, y la ronda la fija sobre
> todo **la tasa de fallas** y no la velocidad del enlace: para cobrar por franja horaria,
> **mejorar la confiabilidad de la recolección vale más que acelerarla**. Eso sale de este
> pipeline, y es medible con él.

---

## Escena 7 — Quién hizo qué · los tres · 30 s

Cada uno dice su parte en una frase. Tiene que coincidir con la sección 8 del documento y con
`git shortlog -sn --no-merges`.

---

## Al terminar

```bash
docker compose -f infra/docker-compose.yml --profile demo --profile humo --profile e2e down -v
```

## Notas de grabación

- **No editar los números.** Si una corrida da distinto, mostrarla y explicar por qué; es más
  creíble que una toma perfecta.
- **Si algo falla en vivo, no cortar.** Que el pipeline mande algo a cuarentena y se vea es
  mejor evidencia que una corrida sin errores.
- Subir el archivo o dejar el enlace en el README, junto al resto de la entrega.
