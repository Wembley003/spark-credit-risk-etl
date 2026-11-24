#!/usr/bin/env bash
# run_etl.sh — corre el ETL de riesgo de crédito y deja constancia de todo.
#
# Esto es el "operador" del proyecto: revisa que estén Java y Python,
# corre el ETL (con Spark o, si se le pide, con la versión de pandas),
# guarda todo lo que salió en un log con fecha y devuelve el mismo
# código de salida que el ETL. Si algo falla, el que llama (un cron,
# un timer de systemd, una persona) se entera por el código, no por
# tener que adivinar.
#
# Uso:
#   ./run_etl.sh                                  # Spark, datos de ejemplo
#   ./run_etl.sh --input data/otra.csv --output-dir outputs/ene
#   ./run_etl.sh --reference                      # pandas, sin Java
#   ./run_etl.sh --master local[2]                # Spark con 2 hilos
#
# Códigos de salida: 0 = bien, 1 = error de ejecución o de entorno,
# 2 = argumentos mal, 3 = la cartera no pasó la validación.
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

INPUT="data/portfolio_sample.csv"
OUTPUT_DIR="outputs"
MASTER="local[*]"
REFERENCE=false

usage() {
    sed -n '2,15p' "$0"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --input) INPUT="$2"; shift 2 ;;
        --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
        --master) MASTER="$2"; shift 2 ;;
        --reference) REFERENCE=true; shift ;;
        --help|-h) usage; exit 0 ;;
        *) echo "Argumento desconocido: $1" >&2; usage >&2; exit 2 ;;
    esac
done

# --- Qué Python usar: el venv del proyecto si existe, si no el venv
# compartido de quant-projects, si no el python3 del sistema.
pick_python() {
    if [[ -x "$PROJECT_DIR/.venv/bin/python" ]]; then
        echo "$PROJECT_DIR/.venv/bin/python"
    elif [[ -x "$PROJECT_DIR/../.venv/bin/python" ]]; then
        echo "$PROJECT_DIR/../.venv/bin/python"
    else
        echo "python3"
    fi
}
PYTHON="$(pick_python)"

mkdir -p logs "$OUTPUT_DIR"
LOG_FILE="logs/etl-$(date +%Y%m%d-%H%M%S).log"

log() { echo "[$(date '+%F %T')] $*" | tee -a "$LOG_FILE"; }

log "Inicio del ETL (input=$INPUT, output=$OUTPUT_DIR, reference=$REFERENCE)"

# --- Preflight: no tiene caso arrancar Spark para descubrir a la mitad
# que falta Java o que el CSV no existe.
if [[ ! -f "$INPUT" ]]; then
    log "ERROR: no existe el archivo de entrada: $INPUT"
    exit 2
fi
if [[ "$REFERENCE" == false ]] && ! command -v java >/dev/null 2>&1; then
    log "ERROR: no se encontró Java; Spark lo necesita. Usa --reference para la versión de pandas."
    exit 1
fi
if ! "$PYTHON" -c "import pyspark" 2>/dev/null && [[ "$REFERENCE" == false ]]; then
    log "ERROR: el Python elegido ($PYTHON) no tiene pyspark instalado."
    exit 1
fi

# --- La corrida en sí. El set -e está apagado aquí a propósito para
# poder registrar el código de salida antes de devolverlo.
set +e
if [[ "$REFERENCE" == true ]]; then
    "$PYTHON" -m spark_credit_risk_etl.cli \
        --input "$INPUT" --output-dir "$OUTPUT_DIR" --reference 2>&1 | tee -a "$LOG_FILE"
    STATUS=${PIPESTATUS[0]}
elif command -v spark-submit >/dev/null 2>&1; then
    spark-submit --master "$MASTER" run_etl.py \
        --input "$INPUT" --output-dir "$OUTPUT_DIR" --master "$MASTER" 2>&1 | tee -a "$LOG_FILE"
    STATUS=${PIPESTATUS[0]}
else
    "$PYTHON" -m spark_credit_risk_etl.cli \
        --input "$INPUT" --output-dir "$OUTPUT_DIR" --master "$MASTER" 2>&1 | tee -a "$LOG_FILE"
    STATUS=${PIPESTATUS[0]}
fi
set -e

if [[ "$STATUS" -eq 0 ]]; then
    log "ETL terminado OK (código 0). Salidas en $OUTPUT_DIR. Log en $LOG_FILE."
else
    log "ETL terminó con error (código $STATUS). Revisa el log: $LOG_FILE."
fi
exit "$STATUS"
