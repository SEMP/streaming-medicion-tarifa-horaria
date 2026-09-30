# Evidencia de ejecución

Cuatro corridas, en **cuatro máquinas distintas**, dos de ellas hechas por personas que no
escribieron el código. Que el sistema dé lo mismo en todas es lo que hace verificable la
reproducibilidad, en lugar de afirmarla.

| Archivo | Quién | Qué cubre |
|---|---|---|
| [`evidencia-ejecucion.txt`](evidencia-ejecucion.txt) | Sergio | Corrida completa: pruebas, demostración, stack, tópicos, humo, recorrido sobre Flink, replay desde el offset 0, offsets y salida. Se regenera con [`generar-evidencia.sh`](generar-evidencia.sh) |
| [`evidencia-ejecucion-daniel-2026-09-29.txt`](evidencia-ejecucion-daniel-2026-09-29.txt) | Daniel | **Validación independiente** del recorrido end-to-end en otra máquina: Windows + WSL2 Ubuntu 24.04 + Docker Desktop |
| [`evidencia-ejecucion-clara-2026-09-30.txt`](evidencia-ejecucion-clara-2026-09-30.txt) | Clara | Recorrido completo sobre un tercer entorno, incluidas las dos corridas de replay y la suite dentro del contenedor |
| [`evidencia-ejecucion-francisco-2026-09-30.txt`](evidencia-ejecucion-francisco-2026-09-30.txt) | Francisco | **Verificación por una persona externa al equipo**, sin conocimiento previo del proyecto y siguiendo únicamente el `README.md` desde el `git clone` |

## Qué probó la segunda máquina

El recorrido dio **exactamente los mismos números** —2,400 kWh en `resto` y 3,100 en `punta`,
total 5,500— con otras versiones de Docker, de Compose y de `uv`. La cuarentena quedó vacía y
el tópico de salida recibió 5 mensajes por corrida, que son las revisiones sucesivas.

## Y encontró algo, que es de lo que sirve una segunda máquina

En su host WSL2, **`uv run pytest` abortó con un *segmentation fault* durante la colección**.
No es un fallo del proyecto —dentro del contenedor la misma suite pasa— pero sí de la
instrucción del README, que ofrecía esa vía como la forma de correr las pruebas sin Docker.

Por eso existe ahora el perfil `pruebas`:

```bash
docker compose -f infra/docker-compose.yml --profile pruebas run --rm -T pruebas
```

Es la única imagen que se construye con las dependencias de desarrollo, para no engordar la
de ejecución. Verificado: 92 pruebas en verde adentro del contenedor.

## La verificación externa, que es la que más encontró

El enunciado pide comprobar que una persona ajena al equipo pueda levantar el sistema con solo
el `README.md`. Se hizo en dos vueltas. La primera dejó siete correcciones, casi todas en el
README o en la configuración, y una de ellas era un error real del código: el perfil `demo`
moría al arrancar porque se llamaba como método a una propiedad. **Las pruebas no lo habían
visto**, porque cubrían un punto de entrada distinto del que usaba el README.

La segunda vuelta, que es la que está en este directorio, salió limpia de punta a punta: los
cinco pasos del Camino A, el recorrido sobre Flink, el replay, la prueba de humo con 40
lecturas de entrada y 40 de salida, y las 92 pruebas en verde.

## Un detalle de lectura

La corrida de Sergio y la de Daniel terminan con un `TimeoutException` del
`kafka-console-consumer`. **No es un error**: es el `--timeout-ms` venciendo después de leer
todo lo que había, que es como se le pide a esa herramienta que corte en lugar de quedarse
esperando; la línea anterior dice `Processed a total of N messages`. El paso 4 del README pasó
después a usar `--max-messages`, que corta solo y sin excepción, y por eso las corridas de
Clara y de Francisco terminan con un `Processed a total of 10 messages` y nada más.
`generar-evidencia.sh` sigue con `--timeout-ms` a propósito, porque ahí no se sabe de antemano
cuántos mensajes hay que leer.
