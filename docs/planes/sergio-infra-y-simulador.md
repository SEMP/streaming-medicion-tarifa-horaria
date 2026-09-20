# Plan — Sergio: infraestructura, simulador y deduplicación

---

## Parte 1 — Infraestructura (es lo que bloquea al resto: va primero)

`docker-compose` con Kafka, Flink (jobmanager y taskmanager) y el job server de Beam, más el
cableado del *expansion service* para que `KafkaIO` funcione desde Python.

Referencia: <https://github.com/rparrapy/fpuna-clase7-taxi-streaming>, que ya lo tiene
resuelto y probado de punta a punta. **No reinventarlo.**

**Lo que el resto necesita de acá:** un esqueleto de pipeline que lea de Kafka y escriba a
Kafka, con el runner configurado, para que Clara enchufe transformaciones sin pelear con la
infraestructura.

## Parte 2 — Simulador

⚠️ **Se escribe de cero.** No se deriva de ningún simulador de proyectos laborales: este
repositorio es público y aquel código no es nuestro. Además son programas con objetivos
opuestos — aquel simula medidores para probar un sistema real y por lo tanto se comporta
bien; este existe para **inyectar fallas a propósito**.

Simula el **ciclo de pedidos** de un concentrador: para cada medidor, una lectura del contador
en cada borde de franja (más las intermedias que se configuren), y las publica al tópico de
entrada conformándose al contrato de Clara.

Debe poder inyectar, de forma **configurable y determinista** (misma semilla, mismos fallos):

- **pedidos que se corren**: el programado para las 18:00 se resuelve a las 18:07;
- **pedidos que fallan**: no hay lectura en ese borde;
- **ráfagas tardías**: el concentrador pierde enlace y publica de golpe lo que juntó, con
  instantes de horas atrás;
- **duplicados** por reintento de publicación;
- **desorden**: los resultados salen en el orden en que responden los medidores;
- **reseteos de contador**, que hacen que la resta dé negativo.

**Decisiones tuyas:** cómo se parametriza, si es un proceso continuo o por lotes, cómo se
controla la velocidad de simulación.

## Parte 3 — Deduplicación e idempotencia

Estado por clave `(medidor, inicio_intervalo)` con **temporizador de expiración** acotado al
horizonte de la lateness — no un conjunto que crece sin límite.

Y la escritura idempotente: clave estable para que recalcular una celda reemplace su valor
en lugar de agregar otro.

**Declarar la semántica de entrega alcanzada, sin sobreprometer.** Decir hasta dónde llega la
garantía en cada tramo es más valioso que afirmar una garantía fuerte que no se sostiene.

## Criterios de aceptación

- [ ] `docker compose up` levanta el stack y el pipeline de ejemplo corre end-to-end.
- [ ] El simulador reproduce exactamente la misma secuencia de fallos con la misma semilla.
- [ ] Un duplicado no altera el agregado.
- [ ] Reprocesar una ventana reemplaza su valor, no lo duplica.
- [ ] Tu sección del documento declara la semántica de entrega y sus límites.

## Riesgo declarado

Tenés la parte más pesada y la más riesgosa a la vez. **Si `KafkaIO` se complica y se come
tres días, el simulador es lo primero que se suelta**: es la pieza más autocontenida, no
depende de la infraestructura y se puede pasar a Clara o Daniel sin romper nada.
