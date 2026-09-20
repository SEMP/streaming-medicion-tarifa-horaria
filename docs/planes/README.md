# Planes de trabajo

Un plan por integrante. Cada uno dice **qué** hay que construir, **con qué contrato** se
conecta al resto y **cómo se sabe que está listo**.

> **Lo que estos planes NO dicen es el cómo.** Las decisiones de implementación son de quien
> toma el plan: qué estructuras usar, cómo organizar el código, qué casos cubrir de más. Eso
> es deliberado — en la presentación cada uno defiende lo suyo, y no se puede defender lo que
> no se decidió.
>
> Si algo del plan parece mal pensado, discutirlo. Un plan no es una orden.

## Las tres interfaces

Esto es lo que permite que los tres trabajemos en paralelo sin bloquearnos. **Hay que
acordarlas antes de escribir código**; una vez fijadas, cada uno construye detrás de la suya.

### 1. Contrato de evento de entrada — define Clara, consume Sergio

El JSON que el simulador publica en el tópico de lecturas crudas. El simulador debe
conformarse a él, incluidos los casos inválidos (que son inválidos **respecto de este
contrato**).

### 2. Asignación de franja — define Daniel, consume Clara

Una función pura, sin dependencias de Beam ni de Kafka:

```python
def asignar_franja(instante, config) -> str
```

Dado un instante y la configuración cargada, devuelve el nombre de la franja. Clara la llama
desde el pipeline sin saber cómo está implementada.

### 3. Contrato de salida — define Clara

El JSON que el pipeline publica en el tópico de resultados, con su clave. Es lo que lee el
tablero y lo que leería la facturación.

## Orden sugerido

**Días 1–2, en paralelo y sin bloqueos:** Sergio levanta la infraestructura, Clara escribe el
contrato de entrada y el de salida, Daniel implementa la asignación de franja y la validación
de configuración con sus pruebas unitarias. Nada de eso depende de lo otro.

**Desde el día 3:** el simulador se conforma al contrato, el pipeline se monta sobre la
infraestructura y llama a la asignación de franja. Ahí recién hay dependencias reales.
