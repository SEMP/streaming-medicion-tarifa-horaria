# Imagen de Flink que además trae los dos SDK de Beam.
#
# Es el truco que hace funcionar KafkaIO desde Python sin Docker dentro de Docker:
#
#   - el SDK **Java** provee las etapas de KafkaIO, que no existen en Python;
#   - el SDK **Python** ejecuta nuestras transformaciones;
#   - los dos corren en modo PROCESS *dentro* del TaskManager.
#
# Sin esto, Beam intentaría levantar un contenedor por entorno de SDK y el TaskManager
# necesitaría acceso al demonio de Docker.
ARG BEAM_VERSION=2.74.0
ARG FLINK_VERSION=1.19.3

FROM apache/beam_java17_sdk:${BEAM_VERSION} AS beam-java
FROM apache/beam_python3.12_sdk:${BEAM_VERSION} AS beam-python
FROM flink:${FLINK_VERSION}-java17

USER root
COPY --from=beam-java /opt/apache/beam /opt/apache/beam
COPY --from=beam-python /usr/local /usr/local
COPY --from=beam-python /opt/apache/beam /opt/medicion/python-sdk

# El código del pipeline viaja en la imagen del TaskManager: es el proceso que realmente
# ejecuta las transformaciones Python.
COPY pipeline/src /opt/medicion/src
COPY simulador/src /opt/medicion/simulador-src

ENV PYTHONPATH=/opt/medicion/src:/opt/medicion/simulador-src \
    RUN_PYTHON_SDK_IN_DEFAULT_ENVIRONMENT=1

RUN chmod 755 /opt/apache/beam/boot /opt/medicion/python-sdk/boot
USER flink
