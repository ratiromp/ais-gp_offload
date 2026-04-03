#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Default config file path
CONFIG_FILE="${SCRIPT_DIR}/../config/env_config.txt"

# Parse arguments to override CONFIG_FILE if --env is provided
args=("$@")
for ((i=0; i<${#args[@]}; i++)); do
    if [[ "${args[i]}" == "--env" ]]; then
        next_idx=$((i+1))
        if [[ $next_idx -lt ${#args[@]} ]]; then
            PROVIDED_ENV="${args[next_idx]}"
            # Check if it's a direct path, otherwise simulate Python's relative config resolution
            if [[ -f "$PROVIDED_ENV" ]]; then
                CONFIG_FILE="$PROVIDED_ENV"
            elif [[ -f "${SCRIPT_DIR}/../config/${PROVIDED_ENV}" ]]; then
                CONFIG_FILE="${SCRIPT_DIR}/../config/${PROVIDED_ENV}"
            else
                CONFIG_FILE="$PROVIDED_ENV"
            fi
        fi
        break
    fi
done

echo "Reading configuration from ${CONFIG_FILE}..."

# Parse the text file for specific keys, taking the value after the '='
# tr -d '\r' removes Windows line endings which can break bash commands
KEYTAB_PATH=$(grep '^KEYTAB_PATH=' "${CONFIG_FILE}" | cut -d'=' -f2- | tr -d '\r')
PRINCIPAL=$(grep '^PRINCIPAL=' "${CONFIG_FILE}" | cut -d'=' -f2- | tr -d '\r')

# Validate that the variables were found
if [ -z "$KEYTAB_PATH" ] || [ -z "$PRINCIPAL" ]; then
    echo "ERROR: KEYTAB_PATH or PRINCIPAL not found in ${CONFIG_FILE}"
    exit 1
fi

echo "KEYTAB_PATH: ${KEYTAB_PATH}"
echo "PRINCIPAL: ${PRINCIPAL}"

# ==============================================================================
# 1. Kerberos Configuration
# ==============================================================================
echo "Obtaining Kerberos ticket for ${PRINCIPAL}..."
kinit -kt "${KEYTAB_PATH}" "${PRINCIPAL}"
echo "Kerberos authentication successful."
echo "----------------------------------------------------------------------"

# ==============================================================================
# 2. PySpark Job Submission
# ==============================================================================
PYTHON_SCRIPT="${SCRIPT_DIR}/script_query_parquet.py"

echo "Submitting PySpark job: ${PYTHON_SCRIPT}"
echo "With arguments: $@"
echo "----------------------------------------------------------------------"

# Use "$@" to forward all command-line arguments to the Python script
# --conf ensures processTreeMetrics is set before SparkContext creation
spark-submit \
    --master yarn \
    --deploy-mode client \
    --num-executors 5 \
    --executor-cores 10 \
    --conf spark.executor.processTreeMetrics.enabled=true \
    --name "reconcile_parquet_job" \
    "${PYTHON_SCRIPT}" "$@"