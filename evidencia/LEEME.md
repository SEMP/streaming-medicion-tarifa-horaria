# Evidencia de ejecución

Dos corridas, en **dos máquinas distintas**. Que el sistema dé lo mismo en las dos es lo que
hace verificable la reproducibilidad, en lugar de afirmarla.

| Archivo | Quién | Qué cubre |
|---|---|---|
| [`evidencia-ejecucion.txt`](evidencia-ejecucion.txt) | Sergio | Corrida completa: pruebas, demostración, stack, tópicos, humo, recorrido sobre Flink, offsets y salida. Se regenera con [`generar-evidencia.sh`](generar-evidencia.sh) |
| [`evidencia-ejecucion-daniel-2026-09-29.txt`](evidencia-ejecucion-daniel-2026-09-29.txt) | Daniel | **Validación independiente** del recorrido end-to-end en otra máquina: Windows + WSL2 Ubuntu 24.04 + Docker Desktop |

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
de ejecución. Verificado: 89 pruebas en verde adentro del contenedor.

## Un detalle de lectura

Las dos corridas terminan con un `TimeoutException` del `kafka-console-consumer`. **No es un
error**: es el `--timeout-ms` venciendo después de leer todo lo que había, que es cómo se le
pide a esa herramienta que corte en lugar de quedarse esperando. La línea anterior dice
`Processed a total of N messages`.
