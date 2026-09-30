# Consumo por franja horaria en streaming — notas para presentar

Texto para leer en cada diapositiva. Exportado de la presentación el 30/09/2026, junto con
[`presentacion.pdf`](presentacion.pdf). Cada nota empieza con la escena del
[guion del video](../guion-video.md) y quién la presenta.

## Quién presenta qué

| # | Diapositiva | Cuándo | Quién |
|---|---|---|---|
| 1 | Consumo eléctrico por franja horaria, en streaming | Escena 1 | los tres |
| 2 | El dato que hace falta para facturar no existe | Escena 1 | Sergio |
| 3 | El sistema no entrega solo el consumo. Entrega cuánto se puede confiar en él. | Escena 1 | Clara |
| 4 | Concentrador, log crudo, pipeline, log derivado | Escena 2 | Clara |
| 5 | Cuatro decisiones que se defienden | Escena 2 | Clara |
| 6 | El recorrido de una lectura, del medidor al consumidor | Escena 2 | Daniel |
| 7 | Tres escenarios, un solo medidor | Escena 3 | Daniel |
| 8 | La tardía corrige el reparto sin cambiar el total | Escena 3 | Daniel |
| 9 | Kafka y Flink de verdad, no un simulador de Beam | Escena 4 | Sergio |
| 10 | Las mismas cinco lecturas, ahora sobre Kafka y Flink | Escena 5 | Clara |
| 11 | Qué garantizamos, y dónde termina | Después de la escena 5 | Sergio |
| 12 | La evidencia, en números | Después de la escena 5 | Daniel |
| 13 | Una corrida exitosa con datos ideales no es evidencia | Escena 6 | los tres, una tarjeta cada uno |
| 14 | Quién hizo qué | Escena 7 | los tres, cada uno su parte en una frase |
| 15 | El recorrido de una lectura, estación por estación | Anexo | — |
| 16 | Simulador | Anexo, para la defensa | Sergio |
| 17 | Tópico crudo | Anexo, para la defensa | Sergio |
| 18 | Parsear y marcar el tiempo | Anexo, para la defensa | Clara |
| 19 | Ventana diaria y deduplicación | Anexo, para la defensa | Clara |
| 20 | Diferenciar el contador | Anexo, para la defensa | Clara |
| 21 | Atribuir la franja y agregar | Anexo, para la defensa | Daniel |
| 22 | Tópico derivado | Anexo, para la defensa | Daniel |
| 23 | Tablero y facturación | Anexo, para la defensa | Daniel |

Las diapositivas 16 a 23 son el **anexo**: no van en el video, están para responder
preguntas en la defensa.

## 1. Consumo eléctrico por franja horaria, en streaming

Escena 1 · Presentan los tres. Sergio: Buenas, soy Sergio Morel. Clara: Soy Clara Almirón. Daniel: Y yo, Daniel Ramírez. Sergio: Este es nuestro proyecto integrador de Streaming de datos y sus aplicaciones. Construimos un pipeline de streaming de punta a punta, con Apache Kafka y Apache Beam sobre Flink, que calcula cuánta energía consumió cada cliente en cada franja horaria. Lo interesante es que los datos le llegan como llegan en la realidad: desordenados, duplicados y con horas de retraso.

## 2. El dato que hace falta para facturar no existe

Escena 1 · Presenta Sergio. Una distribuidora eléctrica necesita cobrar la energía a precios distintos según la hora del día. Para eso hace falta saber cuánta energía consumió cada cliente en cada franja horaria, y ese dato hoy no existe. No existe por una razón física. Los medidores de una cabina comparten un bus RS-485 y se leen en secuencia, de a uno. El concentrador da vueltas preguntando, y una vuelta completa tarda entre 6 y 60 minutos, según cuántos medidores tenga la cabina y cuántos fallen. No hay forma de leerlos a todos a la vez. Entonces una lectura casi nunca cae justo sobre el borde de una franja: cae en el medio, y hay que repartir. Eso introduce un error, y ese error es plata, porque la energía de cada franja se factura a un precio distinto.

## 3. El sistema no entrega solo el consumo. Entrega cuánto se puede confiar en él.

Escena 1 · Presenta Clara. Y acá está el giro del trabajo. Como los intervalos entre lecturas cruzan los bordes de franja siempre, no se puede evitar repartir: se supone potencia constante durante el intervalo y la energía se divide según los minutos que cayeron de cada lado. Eso es una estimación, y lo honesto es decirlo. Por eso el sistema no entrega solamente el consumo por franja. Cada celda del resultado declara si se midió o se interpoló, y cuál fue la separación máxima entre las lecturas, que acota el error. El resultado viene con su margen de error declarado, y eso es lo que permite discutirlo.

## 4. Concentrador, log crudo, pipeline, log derivado

Escena 2 · Presenta Clara. El flujo va de izquierda a derecha: concentrador, log crudo, pipeline, log derivado y consumidores. El simulador ocupa el lugar del concentrador. Genera datos deterministas y, sobre todo, inyecta fallas a propósito: pedidos que se corren, cabinas que caen enteras, tramas truncadas, duplicados y ráfagas tardías. Un simulador que se porta bien no serviría para demostrar nada. Kafka está en dos capas. El tópico crudo conserva las lecturas tal como llegaron, con el medidor como clave para que su orden se preserve. El tópico derivado lleva el resultado, con la celda como clave, para que recalcular reemplace en lugar de duplicar. En el medio, el pipeline de Beam sobre Flink valida, deduplica, calcula el consumo y lo atribuye a una franja. Y abajo, la cuarentena recibe todo lo que no se puede procesar, contado y con su motivo. Su volumen es una señal operativa: si sube en una zona, el problema es la red, no el pipeline.

## 5. Cuatro decisiones que se defienden

Escena 2 · Presenta Clara. Cuatro decisiones que se pueden defender. La clave es el medidor y no la cabina: la cabina agruparía la ronda, pero van de 1 a 199 medidores, y el reparto quedaría desbalanceado en un factor de 200. La franja no es una ventana de Beam: es función pura del tiempo de evento, así que se calcula y viaja en la clave; si fuera ventana, cambiar el calendario tarifario obligaría a redefinir el ventaneo. La lateness es de 36 horas: 24 sería exactamente la cadencia de recolección, sin margen, y 48 duplicaría el estado sin evidencia de que haga falta; 36 cubre un día entero de caída de enlace con media jornada de holgura. Y la más fuerte, porque sale de medir y no de opinar: el umbral de 90 minutos. La duración de los intervalos que cruzan un borde tiene dos modas. Por debajo de 90 minutos está la ronda normal, con el 79 por ciento de los casos; por encima de 120, la cabina caída, con el 20 por ciento. Entre medio hay 13 casos de 1.146. El umbral va en ese valle, porque a cada lado hay un fenómeno distinto.

## 6. El recorrido de una lectura, del medidor al consumidor

Escena 2 · Presenta Daniel. Antes de ver qué pasa con duplicados y lecturas tardías, sigamos una sola lectura de punta a punta. Es la del medidor MED-0042, del 25 de septiembre a las 17:55, con el contador en 101,5 kilovatios hora. Es la misma lectura que usan la demostración y el recorrido sobre Flink, así que los números que van a ver son los mismos del documento técnico y de la evidencia. Pasa por ocho estaciones: el simulador la produce, el tópico crudo la guarda, el pipeline la parsea, la deduplica, calcula el consumo y lo atribuye a una franja, el tópico derivado guarda el resultado y el tablero lo lee. En el camino el dato cambia tres veces de naturaleza. Un contador acumulado se convierte en consumo. Un intervalo se convierte en una celda de franja. Y la clave pasa de ser el medidor a ser la celda, que es lo que hace idempotente la salida. Todo lo que no se puede procesar, en las estaciones tres, cuatro y cinco, va a la cuarentena, contado y con su motivo: nada se descarta en silencio. Cada estación, con el dato exacto que entra y que sale, está en el anexo del final, para las preguntas.

## 7. Tres escenarios, un solo medidor

Escena 3 · Presenta Daniel. Esta es la parte más importante: la evidencia de eventos normales, duplicados y tardíos. Corremos la demostración, que usa el runner local y TestStream, sin Docker, y termina en dos segundos. Es un solo medidor, con cinco lecturas alrededor de las 18:00, que es donde empieza la franja punta. En el acto uno llegan tres lecturas normales. El contador es acumulado, así que el consumo sale de restar, y el segundo intervalo cruza las 18:00: por eso las dos celdas salen interpoladas. En el acto dos llega otra vez la lectura de las 17:55, idéntica: es un reintento de publicación. Y la tabla no se movió. Tampoco apareció ningún intervalo de cero kilovatios hora, que es lo que pasaría si deduplicáramos después de calcular el consumo. En el acto tres llega tarde la lectura de las 18:00, justo sobre el borde, y parte el intervalo en dos.

## 8. La tardía corrige el reparto sin cambiar el total

Escena 3 · Presenta Daniel. Y este es el remate. El total no cambió: 5,5 kilovatios hora antes y después. Medir con más detalle no crea ni destruye energía; lo que cambia es a qué franja se le atribuye, y cambió en 0,1 kilovatios hora. Ese número es el error de atribución, que es lo que este proyecto existe para medir. No es un defecto del pipeline: es la consecuencia de que el intervalo cruzaba el borde y hubo que suponer potencia constante. La lectura tardía es la que revela cuánto se erró. Y fíjense en la columna de origen: pasó de interpolado a medido, porque la lectura cayó justo sobre el borde y ya no hubo nada que estimar. En un momento vamos a ver exactamente estos números otra vez, pero corriendo sobre Kafka y Flink.

## 9. Kafka y Flink de verdad, no un simulador de Beam

Escena 4 · Presenta Sergio · 45 segundos. Mostrar la interfaz de Flink y después la terminal. Esto no corre en un simulador de Beam: corre en un Flink de verdad, con dos TaskManagers de dos slots cada uno, y un Kafka real, en modo KRaft. El pipeline se envía a través del job server de Beam, y la lectura y escritura en Kafka las hace KafkaIO, que es una transformación cross-language: la ejecuta el SDK de Java dentro del TaskManager. Ahora corremos la prueba de humo. Verifica el cableado: que KafkaIO levanta, que Flink acepta el trabajo, que los bytes entran y salen. Lo hace con un passthrough, sin lógica de dominio, y eso es a propósito: sirve para separar «el pipeline está mal» de «la infraestructura está mal», que son dos problemas muy distintos. Termina en entraron 40, salieron 40, con código de salida cero.

## 10. Las mismas cinco lecturas, ahora sobre Kafka y Flink

Escena 5 · Presenta Clara · 120 segundos. Mientras corre el recorrido, que tarda alrededor de un minuto y medio: la demostración de recién probaba la lógica con el runner local, y la prueba de humo probaba el cableado. Falta la pregunta que ninguna de las dos responde: ¿la lógica da lo mismo cuando la ejecuta Flink? No es retórica. El runner portable serializa las funciones y el estado hacia procesos que no comparten memoria con el que arma el pipeline, y hay cosas que andan en local y no allá. Esto siembra las mismas cinco lecturas de antes, con el duplicado y la tardía, pero a través de Kafka, y exige el mismo resultado. Cuando termina, tres cosas. Los mismos números: 3,1 en punta, 2,4 en resto, 5,5 en total. La cuarentena quedó vacía, así que nada se perdió. Y el tópico de salida recibió cinco mensajes para dos celdas: son las revisiones sucesivas, una por cada lectura que cambió algo, y el consumidor se queda con la última. Y el remate, sin bajar el stack: la repetición no siembra nada. Relee el mismo tópico desde el offset cero, con otro grupo de consumidor y un trabajo sin estado previo, y da las mismas dos celdas. Eso es la idempotencia, medida en lugar de declarada.

## 11. Qué garantizamos, y dónde termina

Después de la escena 5 · Presenta Sergio · 60 segundos. Declaramos la garantía por tramo, sin sobreprometer. Del concentrador a Kafka, al menos una vez: el productor usa acks igual a all e idempotencia, así que lo confirmado está en todas las réplicas y sus reintentos internos no duplican. El que sí duplica es el reintento de publicación, que es otro envío, y ese lo absorbe la deduplicación. Dentro del pipeline, efectivamente una vez, pero con dos condiciones: dentro del horizonte de 36 horas, porque después el estado expira y un duplicado se contaría de nuevo, y mientras el trabajo no se reinicie. Hacia la salida, efectivamente una vez en el efecto observable, porque el upsert por clave estable hace que reescribir sea inocuo. Por eso no afirmamos exactly-once de punta a punta. Y el límite más serio lo encontramos tarde, el mismo día de la entrega: la configuración de checkpoints está puesta, Flink los dispara, pero ninguno completa. La causa probable es que no le pasamos el intervalo de checkpointing a Beam, donde está deshabilitado por defecto. En la práctica, el pipeline procesa y emite bien, verificado seis veces, pero si el trabajo se reinicia, el estado por medidor se pierde. No lo corregimos hoy porque invalidaba las seis corridas de evidencia. Para producción es lo primero que habría que cerrar, y preferimos decirlo nosotros.

## 12. La evidencia, en números

Después de la escena 5 · Presenta Daniel · 45 segundos. La evidencia, en cuatro números. Ciento dieciséis pruebas automáticas, entre ellas las de TestStream, que son las únicas que permiten probar el comportamiento tardío de forma determinista: con un reloj real habría que esperar, y el resultado dependería de la máquina. Seis corridas completas sobre Kafka y Flink, en cuatro máquinas distintas; una de ellas la hice yo en Windows con WSL2, con otras versiones de Docker, de Compose y de uv. Las seis dan 5,5 kilovatios hora con la cuarentena vacía. Tres defectos que encontró una persona ajena al equipo, siguiendo solo el README desde el git clone, en cuatro vueltas. Y el tablero, que construyó Clara en marimo, con un inyector que publica a pedido un duplicado o una tardía sobre el stack real: el duplicado no mueve nada, y la tardía muda 0,2 kilovatios hora de resto a punta sin cambiar el total, que queda en 1,8.

## 13. Una corrida exitosa con datos ideales no es evidencia

Escena 6 · Presentan los tres, una tarjeta cada uno. Sergio: El enunciado dice que una ejecución exitosa con datos ideales no es evidencia suficiente, y lo comprobamos sobre nuestro propio código. Encontramos dos errores de doble conteo. Cuando llegaba una lectura tardía, el intervalo se partía en dos, pero el intervalo original ya había salido y seguía sumando: 12 kilovatios hora donde el consumo real era 6. Y la primera corrección también estaba mal: encadenar dos agregaciones bajo un modo acumulativo vuelve a contar doble. Ninguno de los dos aparece con datos ideales. Daniel: La segunda es la prueba externa. Una persona ajena al equipo levantó el sistema siguiendo solo el README, y en quince minutos encontró un error que 89 pruebas no habían visto: el perfil de demostración moría al arrancar, porque las pruebas cubrían un punto de entrada y el README usaba otro. En cuatro vueltas encontró tres errores, todos con la misma forma: lo que recorrían nuestras pruebas y lo que mandaba hacer el README no eran el mismo camino. Clara: Y la tercera: cuando no sabemos, lo decimos. Si dos lecturas quedan a más de 90 minutos, suponer potencia constante deja de ser defendible, así que la energía de ese cruce no se reparte. La celda se marca indeterminada, dice cuántos minutos quedaron sin cubrir, y el resto de la celda se conserva. Preferimos decir no sé antes que inventar un número.

## 14. Quién hizo qué

Escena 7 · Presentan los tres, cada uno su parte en una frase. Sergio: Hice el simulador, la infraestructura de Kafka y Flink, la cadena del pipeline y las franjas, y encontré los dos errores de doble conteo. Clara: Hice los contratos de evento, la política temporal, con la ventana diaria y la lateness de 36 horas, y el tablero en marimo con su inyector de irregularidades. Daniel: Validé la reproducibilidad en otra máquina, con los mismos resultados, y encontré el problema que llevó al perfil de pruebas en contenedor. El detalle está en la sección 8 del documento técnico, y el repositorio es público: github.com/SEMP/streaming-medicion-tarifa-horaria.

## 15. El recorrido de una lectura, estación por estación

Separador del anexo. No se presenta en el video. Si en la defensa preguntan por una etapa en particular, saltar a su estación: 01 y 02 las presenta Sergio, de la 03 a la 05 Clara, y de la 06 a la 08 Daniel.

## 16. Simulador

Anexo, para la defensa · Presenta Sergio. Todo empieza con una pregunta. El medidor no transmite por su cuenta: el concentrador recorre la cabina, medidor por medidor, sobre un bus RS-485 compartido, y le pide el registro 15.8.0. Lo que el medidor responde es su contador acumulado: 101,5 kilovatios hora. Y eso no es lo que se consumió: es el total desde que el medidor existe. El consumo hay que sacarlo restando dos lecturas, y por eso más adelante el pipeline va a necesitar recordar la anterior. Con esa respuesta se arma el evento que ven a la derecha. Fíjense en el event_id: no es aleatorio, es un hash del medidor y del instante de la lectura. Si el concentrador reintenta la publicación, el reintento sale con el mismo id, y eso es lo que permite reconocerlo como duplicado. En nuestro proyecto el simulador ocupa el lugar del concentrador, y existe sobre todo para inyectar fallas a propósito: pedidos que se corren, cabinas que caen enteras, tramas truncadas, duplicados y ráfagas tardías.

## 17. Tópico crudo

Anexo, para la defensa · Presenta Sergio. La lectura llega al tópico crudo, medicion.lecturas.v1, tal como llegó. La clave es el identificador del medidor, en bytes, y el valor es el mismo JSON de antes, serializado. Kafka elige la partición por el hash de la clave, así que todas las lecturas de un mismo medidor caen siempre en la misma partición y se leen en orden. Ese orden es un requisito duro, porque la etapa que calcula el consumo resta lecturas consecutivas del mismo equipo. ¿Por qué el medidor y no la cabina? La cabina era tentadora, porque agrupa la ronda de lectura, pero las cabinas van de 1 a 199 medidores: el reparto entre particiones quedaría desbalanceado en un factor de 200. Con el medidor, el reparto es parejo por construcción. El tópico tiene cuatro particiones y siete días de retención, y el productor publica con acks igual a all e idempotencia.

## 18. Parsear y marcar el tiempo

Anexo, para la defensa · Presenta Clara. Acá la lectura entra al pipeline. Lo primero es parsear: el valor pasa de bytes a un diccionario, y el medidor se toma del cuerpo del mensaje; la clave de Kafka queda como respaldo. Después viene la decisión más importante de esta etapa: qué tiempo usar. El registro de Kafka trae cuándo se publicó el mensaje; el campo instante_lectura trae cuándo se midió. Entre los dos puede haber horas, porque una cabina que vuelve de una caída de enlace publica de golpe todo lo que fue juntando. Si ventaneáramos por el tiempo de publicación, ese consumo caería en el día equivocado, y eso es energía mal facturada. Por eso la lectura se sella con su tiempo de evento. Y se exige que el instante traiga su huso horario: si no lo trae, va a cuarentena, en lugar de interpretarse en la zona de la máquina donde corre el pipeline, que haría depender el resultado de dónde se ejecuta.

## 19. Ventana diaria y deduplicación

Anexo, para la defensa · Presenta Clara. La lectura cae en la ventana del día local. Es una ventana fija de 24 horas, pero desplazada tres horas, porque Beam corta sobre el instante absoluto: sin el desplazamiento, el día terminaría a medianoche UTC, que son las 21 horas en Asunción, en plena hora punta. El corte caería en el medio de la franja más cara. Después viene la deduplicación. El estado del medidor guarda los instantes que ya vio. Si este instante ya estaba, la lectura se cuenta como duplicado y no se emite; si no estaba, se agrega y sigue. Y el orden de las etapas no es intercambiable: si deduplicáramos después de calcular el consumo, el duplicado se restaría contra sí mismo, daría cero, y con salida por upsert ese cero pisaría el valor correcto. Un duplicado no es un error, así que no va a cuarentena, pero sí se cuenta. Y un temporizador borra ese estado 36 horas después del fin de la ventana, el mismo horizonte que la lateness.

## 20. Diferenciar el contador

Anexo, para la defensa · Presenta Clara. Recién acá aparece el consumo, que no venía en el dato. La etapa busca el registro 15.8.0, guarda la lectura en el estado del medidor y ordena todas las lecturas de ese medidor dentro de la ventana. La de las 17:40, con el contador en 100, ya estaba guardada desde su propio paso por esta etapa. Entonces: 101,5 menos 100 da 1,5 kilovatios hora en quince minutos. Guardar todas las lecturas, y no solo la última, es lo que permite que una lectura tardía caiga en el medio de un intervalo y lo parta en dos, aunque ese intervalo ya se haya emitido. Y el campo separacion_minutos no es decorativo: es la cota del error de atribución, el número que este proyecto existe para medir. Si el contador retrocede, la lectura va a cuarentena: sin compra de energía al usuario, un contador que baja es un reseteo del equipo o una trama truncada, nunca una medición.

## 21. Atribuir la franja y agregar

Anexo, para la defensa · Presenta Daniel. Esta es la etapa donde el intervalo se convierte en plata. Primero se fija si hay otro intervalo que empiece en el mismo instante y sea más corto; si lo hay, este quedó superado por una lectura tardía y deja de sumar. Esa regla es la que corrigió nuestro error de doble conteo. Después recalcula todas las celdas del medidor y reparte cada intervalo entre las franjas. Este intervalo, de 17:40 a 17:55, cae entero en resto, así que la celda sale como medida, con 1,5 kilovatios hora. Pero cuando llegue el siguiente, de 17:55 a 18:20 con 4 kilovatios hora, ese sí cruza el borde de punta. Se reparte por minutos, suponiendo potencia constante: 5 minutos de un lado y 20 del otro, o sea 0,8 a resto y 3,2 a punta, y las dos celdas pasan a interpoladas. Recalcular todo en lugar de solo lo que tocó la lectura nueva es más caro, y es a propósito: así el resultado no depende del orden en que llegan las lecturas, y un reproceso converge al mismo valor.

## 22. Tópico derivado

Anexo, para la defensa · Presenta Daniel. El resultado vuelve a Kafka, al tópico derivado. Pero fíjense que la clave cambió: ya no es el medidor, es la celda, que es medidor, fecha y franja. Y el valor es absoluto: el consumo total de esa celda, no un incremento. Esas dos cosas juntas son las que hacen idempotente a todo el sistema. Cada mensaje reemplaza al anterior en lugar de sumarse, así que volver a escribir es inocuo. Y lo verificamos sobre Kafka y Flink: reprocesar el log entero desde el offset cero, con un grupo de consumidor nuevo, converge a las mismas celdas. El tópico usa compactación más borrado a los 90 días. La compactación deja viva la última revisión de cada celda, y el borrado evita que las claves se acumulen para siempre, porque la clave incluye la fecha y cada día aparecen claves nuevas.

## 23. Tablero y facturación

Anexo, para la defensa · Presenta Daniel. Del tópico derivado leen dos consumidores con necesidades opuestas. El tablero, que construimos en marimo, relee el tópico desde el offset cero y hace upsert por clave: el último mensaje gana, nunca suma. Las cinco revisiones que publicó el pipeline en la demostración colapsan en dos celdas: 3,1 kilovatios hora en punta y 2,4 en resto, 5,5 en total. Si el tablero sumara en lugar de reemplazar, el total daría más del doble. La facturación la dejamos descripta pero no implementada: leería una sola vez, pasadas 36 horas del fin de la ventana, cuando el valor ya convergió. Y un último detalle de diseño: el error de atribución lo calcula el consumidor y no el pipeline, porque el denominador es la duración de la franja, que vive en el calendario tarifario y no en el evento. Ponerlo en el mensaje obligaría a que el pipeline y el consumidor coincidieran sobre qué calendario rige en cada fecha.
