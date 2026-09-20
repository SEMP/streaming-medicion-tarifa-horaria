# CLAUDE.md

Guía para agentes de código que trabajen en este repositorio. Lo leen los tres integrantes,
cada uno desde su máquina.

## Qué es esto

Pipeline de streaming con **Kafka + Beam** que calcula consumo eléctrico por **franja
tarifaria** a partir de curvas de carga que llegan desordenadas, duplicadas y con horas de
retraso. El contexto y el problema están en [`README.md`](README.md); el **porqué** de cada
decisión, en [`docs/decisiones-de-diseno.md`](docs/decisiones-de-diseno.md).

**Antes de proponer un diseño, leer ese documento.** Varias decisiones que parecen obvias ya
se tomaron al revés por una razón — por ejemplo, la franja *no* es una ventana de Beam, y los
bordes de franja *no* se prorratean.

## Reglas duras

1. **Todos los datos son sintéticos.** Los genera `simulador/`. No se incorporan datos reales
   de ninguna distribuidora ni de ningún sistema en producción, en ninguna forma: ni
   archivos, ni volcados, ni fragmentos en pruebas o documentación.
2. **No se copia código de proyectos laborales de los integrantes.** Lo que se aporta es
   conocimiento del tema, no archivos. Este repositorio es público.
3. **Todo en español**: documentación, comentarios, nombres de archivo y mensajes de commit.
4. **Cada integrante commitea con su propia cuenta**, en su área. El historial es la
   evidencia de las contribuciones individuales, y la entrega lo exige explícitamente. No
   commitear trabajo de otro integrante bajo la propia firma.
5. **No inventar.** Si algo no está definido —un umbral, un formato, una regla—, marcarlo
   como `⚠️ ABIERTO` en `docs/decisiones-de-diseno.md` y preguntar. Hay tres decisiones
   abiertas hoy.

## Stack, y por qué está fijado

```
Python 3.12   ·   apache-beam 2.74.0   ·   Kafka 4.1.1   ·   Flink 1.19
```

Las versiones **no son elección libre**: las fija el job server de Beam
(`apache/beam_flink1.19_job_server:2.74.0`). Cambiar una obliga a cambiar el resto.

⚠️ **`KafkaIO` desde Python no es una librería Python.** Es una transformación
*cross-language*: corre el SDK de **Java** a través de un *expansion service*. No se resuelve
con `pip install`. El laboratorio de la cátedra
<https://github.com/rparrapy/fpuna-clase7-taxi-streaming> ya lo tiene resuelto y probado
—`docker-compose` con Kafka, Flink jobmanager/taskmanager, job server y
`default_io_expansion_service()`—: **es la referencia a seguir, no hay que reinventarlo.**

### Entorno

Se usa [`uv`](https://docs.astral.sh/uv/), no `pip` ni `venv` a mano:

```bash
uv sync                  # crea .venv e instala dependencias
uv run pytest            # pruebas
uv run ruff check .      # lint
```

## Estructura y quién es dueño de qué

La estructura coincide a propósito con el reparto de trabajo, para minimizar conflictos de
merge. **Antes de tocar una carpeta que no es la propia, avisar al dueño.**

| Carpeta | Qué va | Dueño |
|---|---|---|
| `simulador/` | Productor sintético: curva de carga, casos de reloj, duplicados, lotes tardíos | Sergio |
| `infra/` | `docker-compose`, Kafka, Flink, job server | Sergio |
| `pipeline/` | Pipeline Beam: validación, salida lateral, `CombinePerKey`, escritura, ventanas | Clara |
| `config/` | Calendario de franjas e intervalo, con su validación | Daniel |
| `tests/` | Unitarias y de pipeline con `TestStream` | Daniel |
| `docs/` | Documento técnico y diagrama — **cada uno escribe su sección** | los tres |

## Convenciones de código

- Los nombres del dominio van en español (`franja`, `medidor`, `lectura`, `intervalo`); los
  de la API de Beam quedan como son (`CombinePerKey`, `ParDo`).
- La configuración se carga con `tomllib` (biblioteca estándar en 3.12), no se agregan
  dependencias para leer TOML.
- **Las funciones puras se prueban sin infraestructura.** La asignación de franja y la
  validación de configuración no deben requerir Kafka ni Beam para testearse: eso es
  deliberado y permite trabajar en paralelo.
- `datos/` y `config/franjas.toml` están ignorados. El ejemplo versionado es
  `config/franjas.example.toml`.

## Al terminar una sesión

- No dejar contenedores Docker corriendo.
- Si se tomó una decisión de diseño, registrarla en `docs/decisiones-de-diseno.md` con su
  motivo. El documento técnico se arma de ahí.
