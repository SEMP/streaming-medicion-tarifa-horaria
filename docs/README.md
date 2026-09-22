# Los documentos del proyecto

Qué documento es cada uno, quién lo mantiene y **en cuál escribir cada cosa**. Si dudás dónde
va algo que acabás de aprender o decidir, la respuesta está acá.

---

## 1. Los documentos internos

Sostienen el trabajo. No se entregan tal cual, pero son de donde sale casi todo lo que sí se
entrega.

| Documento | Qué es | Dueño | Escribís acá cuando… |
|---|---|---|---|
| [`dominio-medicion.md`](dominio-medicion.md) | Cómo funciona de verdad la lectura remota de medidores: OBIS, readout, contador acumulado, por qué llega tarde | Sergio | Aprendiste algo **del dominio** que explica por qué una decisión es como es |
| [`decisiones-de-diseno.md`](decisiones-de-diseno.md) | Las decisiones numeradas, con su motivo y lo que se resigna. **Es el registro con autoridad** | Los tres | Cerraste una decisión de diseño. Va el **porqué**, no solo el qué |
| [`contratos.md`](contratos.md) | El evento de entrada y el registro de salida, con sus campos, reglas de validación y política temporal | Clara | Cambió algo **de contrato**: un campo, una clave, un tópico, un valor de la política temporal |
| [`pendientes.md`](pendientes.md) | Qué falta decidir, quién decide, a quién bloquea, y la bitácora de lo decidido | Los tres | Encontraste algo sin definir, o cerraste un pendiente |
| [`planes/`](planes/) | Un encargo por integrante: qué construir y cómo se sabe que está listo | Sergio los escribió; cada uno el suyo | Cambió el alcance de la parte de alguien |

## 2. La regla que evita que se pudran

**Cada cosa se explica en un solo lugar. Los demás la referencian.**

| Qué | Dónde vive | Dónde **no** |
|---|---|---|
| El **porqué** de una decisión | `decisiones-de-diseno.md`, o `contratos.md` si es de contrato | En `pendientes.md`, que solo lleva punteros |
| Conocimiento del **dominio** | `dominio-medicion.md` | Repetido en cada decisión que lo usa |
| **Estado** de una decisión | `pendientes.md` | Disperso en marcas sueltas por ahí |
| Cómo **correr** algo | El `README.md` de su carpeta | En `docs/` |

Ya nos pasó lo contrario: el borrador de interfaces y las decisiones explicaban lo mismo, el
modelo cambió a readout, uno se actualizó y el otro no. De ahí salieron tres correcciones el
22/09. **Si la decisión invalida algo escrito antes, se corrige en el mismo commit.**

Cuando un documento queda superado por otro, no se borra: se le pone arriba un aviso de
**SUPERADO**, con el archivo que lo reemplaza y el commit. Así el que llega por un enlace viejo
no toma decisiones de un texto muerto.

## 3. Los entregables, y de dónde sale cada uno

El enunciado pide cinco cosas. Ninguna se escribe de cero: se **arman** con lo que ya está.

| Entregable | Estado | De dónde sale |
|---|---|---|
| Enlace al repositorio | ✅ | — |
| [`../README.md`](../README.md) reproducible | 🚧 falta «Cómo levantarlo» | Sergio, cuando exista `infra/` |
| **Documento técnico** | ⬜ | Ver la tabla de abajo |
| **Diagrama de arquitectura** | ⬜ | `docs/diagramas/` — ⚠️ falta decidir el formato (`P10`) |
| **Evidencia de pruebas y ejecución** | ⬜ | Salida de `pytest` y de la corrida end-to-end |
| **Video breve** | ⬜ | Cada uno presenta su parte |
| Integrantes y contribuciones | ✅ en el README | El historial de git lo respalda |

### El documento técnico ya está escrito en un 70 %, disperso

El enunciado le pide cinco partes. Cada una tiene hoy su fuente:

| Parte que pide el enunciado | Fuente |
|---|---|
| Problema, usuarios del resultado, y qué decisiones habilita | [`../README.md`](../README.md) + [`dominio-medicion.md`](dominio-medicion.md) |
| Diagrama de arquitectura y descripción de cada componente | El diagrama, pendiente + [`../README.md`](../README.md) |
| Contrato de eventos, tópicos, claves, particiones y esquema de salida | [`contratos.md`](contratos.md) |
| Ventanas, lateness, deduplicación, idempotencia y semántica de entrega | [`contratos.md`](contratos.md) §1.9 y §2.4, y decisiones 8, 9 y 10 |
| Límites conocidos, supuestos y posibles mejoras | Los «**lo que se resigna**» de cada decisión, y la sección «Posibles mejoras» de [`decisiones-de-diseno.md`](decisiones-de-diseno.md) |

**Por eso cada decisión se escribe con su motivo y con lo que resigna**: no es prolijidad, es
el texto del documento técnico. El que escribe «decidido: 4 particiones» y nada más, va a
tener que reconstruir el argumento en la última semana.

**Quién escribe qué sección** —y quién la presenta en el video— sigue el reparto: Sergio la
infraestructura, el simulador y la confiabilidad; Clara los contratos, el pipeline y la
política temporal; Daniel las franjas, la validación y las pruebas. El criterio 7 evalúa
«defensa técnica equilibrada entre integrantes», así que nadie presenta lo que no escribió.

## 4. Convenciones

- **Todo en español**: texto, nombres de archivo, títulos y mensajes de commit.
- **Markdown**, con la prosa ajustada a **100 columnas**. Las tablas quedan libres: cortarlas
  las rompe.
- **Nombres de archivo en `kebab-case`**, descriptivos: `dominio-medicion.md`, no `doc2.md`.
- **Enlaces relativos** entre documentos, nunca URLs de GitHub: así funcionan también en un
  clon y en una vista previa local.
- **Citar el pendiente en el commit** cuando se cierra uno: `P3: fijar 4 particiones`.
- Los **enunciados de la cátedra no se editan**. Son solo lectura.

## 5. Qué no va al repositorio

Está en el [`CLAUDE.md`](../CLAUDE.md) como regla dura y se repite acá porque es lo más caro de
equivocar: **el repositorio es público**.

- **Datos reales** de ninguna distribuidora ni sistema en producción, en ninguna forma: ni
  archivos, ni volcados, ni fragmentos en pruebas o documentación. Todo lo que se procesa lo
  genera `simulador/`.
- **Código de proyectos laborales** de los integrantes. Lo que se aporta es conocimiento del
  tema, no archivos.
- Lo generado: `datos/*.jsonl`, `.venv/`, `output/`, checkpoints. Ya está en `.gitignore`.
- `config/franjas.toml` — el versionado es `config/franjas.example.toml`, para que cada uno
  pueda probar con calendarios distintos sin pisarse.
