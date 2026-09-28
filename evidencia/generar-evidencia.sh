#!/usr/bin/env bash
# Captura una corrida completa del sistema en un solo archivo.
#
# El enunciado pide "evidencia de pruebas y ejecución end-to-end" y "logs o métricas
# suficientes para observar producción, consumo, procesamiento y errores". Esto lo produce
# de una sola pasada, con marcas de tiempo, para que se pueda leer sin tener que levantar
# nada — y para que quien quiera, lo pueda repetir y comparar.
#
#     ./evidencia/generar-evidencia.sh
#
# Tarda unos minutos: levanta Kafka, Flink y el job server de Beam.

set -uo pipefail
cd "$(dirname "$0")/.."

SALIDA="evidencia/evidencia-ejecucion.txt"
COMPOSE="docker compose -f infra/docker-compose.yml"
: > "$SALIDA"

titulo() {
  { echo; echo "════════════════════════════════════════════════════════════════════"
    echo " $1"
    echo " $(date --iso-8601=seconds)"
    echo "════════════════════════════════════════════════════════════════════"; } | tee -a "$SALIDA"
}

correr() {
  # Se registra el comando antes de la salida: sin eso, la evidencia no dice qué se ejecutó.
  { echo; echo "\$ $*"; } | tee -a "$SALIDA"
  "$@" 2>&1 | tee -a "$SALIDA"
  local codigo=${PIPESTATUS[0]}
  echo "  → código de salida: $codigo" | tee -a "$SALIDA"
  return "$codigo"
}

{
  echo "Evidencia de ejecución — consumo eléctrico por franja horaria"
  echo "Generada por evidencia/generar-evidencia.sh"
  echo "Commit: $(git rev-parse --short HEAD 2>/dev/null || echo 'sin git')"
  echo "Fecha:  $(date --iso-8601=seconds)"
} | tee -a "$SALIDA"

titulo "1. Entorno"
correr uname -srm
correr docker --version
correr uv run python --version

titulo "2. Pruebas automáticas"
correr uv run pytest -q

titulo "3. Demostración: normales, duplicado y tardío (DirectRunner, sin Docker)"
correr uv run python -m pipeline.demostracion

titulo "4. Levantar Kafka, Flink y el job server de Beam"
correr $COMPOSE up -d
sleep 20
correr $COMPOSE ps

titulo "5. Tópicos creados"
correr $COMPOSE exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --list

titulo "6. Prueba de humo: el cableado, con passthrough"
correr $COMPOSE --profile humo run --rm -T humo

titulo "7. Recorrido completo con la lógica, sobre Flink"
correr $COMPOSE --profile e2e run --rm -T extremo-a-extremo

titulo "8. Qué quedó en los tópicos"
# Producción y consumo observables: cuántos mensajes entraron y cuántos salieron.
for t in medicion.lecturas.e2e medicion.consumo-franja.e2e medicion.cuarentena.e2e; do
  correr $COMPOSE exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server kafka:9092 --topic "$t"
done

titulo "9. Celdas de salida, tal como las lee el consumidor"
correr $COMPOSE exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic medicion.consumo-franja.e2e \
  --from-beginning --property print.key=true --timeout-ms 8000

titulo "10. Bajar el stack"
correr $COMPOSE --profile demo --profile humo --profile e2e down -v

titulo "Fin"
echo "  Evidencia en $SALIDA · $(wc -l < "$SALIDA") líneas" | tee -a "$SALIDA"
