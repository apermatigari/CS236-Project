#!/usr/bin/env bash
# run_all.sh - Section 2.2 pipeline: create tables -> load with Spark -> verify.
# Output is also saved to output/run_log.txt.
#
# Before running:
#   1. Phase 1 outputs exist (../Dataset-Processing/run_all.sh)
#   2. PostgreSQL is running (./start_postgres.sh)
#
# Usage: ./run_all.sh
# Optional environment variables: PYTHON, JAVA_HOME (as in Phase 1),
#   PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD (see db_common.py)
set -euo pipefail
cd "$(dirname "$0")"

# Search this folder and its parents for a path pattern; print the first match.
find_up() {
  local d="$PWD" m
  while [ "$d" != "/" ]; do
    for m in "$d"/$1; do [ -e "$m" ] && { echo "$m"; return 0; }; done
    d="$(dirname "$d")"
  done
  return 1
}

if [ -z "${JAVA_HOME:-}" ]; then
  JAVA_HOME="$(find_up '.jdk/jdk-17*' || true)"
  [ -n "$JAVA_HOME" ] && export JAVA_HOME
fi
PY="${PYTHON:-$(find_up '.venv/bin/python' || command -v python3)}"

mkdir -p output
: > output/run_log.txt
for step in load_to_postgres verify_db; do
  echo "=== $step ===" | tee -a output/run_log.txt
  # grep drops Spark startup warnings and Ivy's JDBC-driver download messages.
  "$PY" "$step.py" 2>&1 \
    | grep -vE $'WARN|setLogLevel|Setting default log level|^\t|^\\s*[-|]|^:: |Ivy Default|jars for the packages|added as a dependency' \
    | tee -a output/run_log.txt
done
