# Daniel — puesta al día, 29/09

**Tu plan original ya está hecho.** Las franjas, la validación del instante, el umbral y las
pruebas se implementaron entre el 22 y el 27/09 porque el trabajo estaba en el camino crítico
y la fecha se venía. No es un reproche ni hace falta rehacerlo: está en
[`daniel-franjas-y-pruebas.md`](daniel-franjas-y-pruebas.md) si querés ver qué era, y en el
[tablero](../pendientes.md) queda registrado quién lo tomó.

**Queda un día.** Esto es lo que sí sirve que hagas, en orden.

## 1. Levantar el proyecto en tu máquina

```bash
git clone git@github.com:SEMP/streaming-medicion-tarifa-horaria.git
cd streaming-medicion-tarifa-horaria
uv sync
uv run pytest                              # 92 pruebas, sin Docker
uv run python -m pipeline.demostracion     # los tres escenarios, en 2 segundos
```

Con eso ya ves de qué se trata el trabajo sin leer una línea de código: la demostración
cuenta una historia de cinco lecturas y muestra qué pasa con un duplicado y con una lectura
tardía.

Para el recorrido completo sobre Kafka y Flink hace falta Docker:

```bash
docker compose -f infra/docker-compose.yml up -d --build
docker compose -f infra/docker-compose.yml --profile e2e run --rm -T extremo-a-extremo
docker compose -f infra/docker-compose.yml --profile e2e down -v
```

## 2. Lo más útil que podés aportar hoy: verificarlo en una máquina que no es la mía

**«Reproducibilidad» es un criterio puntuado, y hasta ahora el stack solo corrió en la
computadora de Sergio.** Que funcione en otra máquina es evidencia real, no un trámite — y es
justamente el tipo de cosa que falla: una versión distinta de Docker, otro sistema operativo,
un puerto ocupado.

Corré los comandos de arriba y anotá **todo lo que no haya salido como dice el README**: un
error, un paso que falte, algo que tarde muchísimo. Si algo falla, eso es un hallazgo y hay
que arreglarlo antes de entregar. Si todo anda, decilo igual: también es información.

## 3. El video — tenés una escena

El guion está en [`guion-video.md`](../guion-video.md). Son siete escenas y **la 3 es tuya**:
«Los tres escenarios», unos 150 segundos.

Es la escena más importante del video, porque es donde se cumple lo que el enunciado pide como
*evidencia visible de eventos normales, duplicados y tardíos*. El guion trae qué decir en cada
acto; alcanza con correr `uv run python -m pipeline.demostracion` y explicar la tabla.

Leelo antes de juntarse a grabar, porque entender por qué el total no cambia cuando llega la
lectura tardía es lo que hace que la escena se entienda.

## 4. Decir qué vas a poner en la sección 8

El documento técnico cierra con **integrantes y contribuciones principales de cada persona**,
y lo pide el enunciado. Hace falta que digas con qué te querés listar. Habllalo con Sergio y
Clara: es una conversación del equipo, no algo que se decida por vos.

## Dónde está cada cosa

| Qué | Dónde |
|---|---|
| Qué hace el proyecto y cómo levantarlo | [`README.md`](../../README.md) |
| El diagrama | [`docs/diagramas/arquitectura.svg`](../diagramas/arquitectura.svg) |
| Por qué cada decisión | [`docs/decisiones-de-diseno.md`](../decisiones-de-diseno.md) |
| El contrato de eventos | [`docs/contratos.md`](../contratos.md) |
| Quién hace qué y qué falta | [`docs/pendientes.md`](../pendientes.md) |
| El documento que se entrega | [`docs/tecnico/documento.md`](../tecnico/documento.md) |
| El guion del video | [`docs/guion-video.md`](../guion-video.md) |

## Si te queda tiempo y querés meter código

Dos cosas chicas y acotadas, que no bloquean a nadie:

- **La regla 1 de `config/franjas.example.toml`** quedó desactualizada: describe una
  validación de alineación a la grilla que ya no aplica, porque con *readout* no hay grilla.
  Está anotada en el tablero.
- **Correr la demostración con otros calendarios tarifarios** —tres franjas en vez de cuatro,
  bordes en otros horarios— y ver si algo se rompe. Nadie probó eso.

Avisá antes de empezar cualquiera de las dos, para no pisarnos.
