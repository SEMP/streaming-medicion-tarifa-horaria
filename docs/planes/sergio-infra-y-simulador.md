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

⚠️ **Se escribe de cero, de forma independiente.** No deriva de ningún simulador de proyectos
laborales. Hay tres razones y la tercera es la que más importa:

1. Este repositorio es **público** y aquel código no es del equipo.
2. Son programas con **objetivos opuestos**: aquel simula medidores para probar un sistema
   real, y por lo tanto se comporta bien; este existe para **inyectar fallas a propósito**.
   Partir del otro sería pelear contra un diseño pensado para lo contrario.
3. **Habilita el camino de vuelta.** Si este simulador nace limpio, es obra del equipo bajo la
   licencia que el equipo elija, y puede incorporarse después a un sistema de trabajo sin
   ninguna pregunta pendiente. Si derivara del otro, esa vuelta quedaría enredada.

Simula el **ciclo de pedidos** de un concentrador: para cada medidor, lecturas del contador en
cada borde de franja —**con la redundancia configurada alrededor de cada borde**— más las
intermedias que se pidan, y las publica al tópico de entrada conformándose al contrato de
Clara.

Debe poder inyectar, de forma **configurable y determinista** (misma semilla, mismos fallos):

- **pedidos que se corren**: el programado para las 18:00 se resuelve a las 18:07;
- **pedidos que fallan**: no hay lectura en ese borde;
- **ráfagas tardías**: el concentrador pierde enlace y publica de golpe lo que juntó, con
  instantes de horas atrás;
- **duplicados** por reintento de publicación;
- **desorden**: los resultados salen en el orden en que responden los medidores;
- **reseteos de contador**, que hacen que la resta dé negativo;
- **tramas incompletas**, incluido el caso feo: la que se corta en medio de un número y deja
  un valor plausible pero equivocado.

### Perfiles de medidor

El parque no es homogéneo: hay modelos que responden unos pocos registros en segundos y otros
que devuelven cientos de miles de datos sobre un enlace con segundos de latencia, tardando
minutos y necesitando reintentos.

Eso se modela como **perfiles configurables** —rápido, lento, inestable— cada uno con su
tiempo de respuesta, su probabilidad de reintento y su probabilidad de trama incompleta. Es
barato de escribir y da evidencia de demo mucho mejor que un parque uniforme.

⚠️ **La heterogeneidad vive acá, no en el pipeline.** El pipeline trata a todos los medidores
igual. Es una decisión de alcance deliberada.

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
