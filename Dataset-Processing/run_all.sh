#!/usr/bin/env bash
# run_all.sh - Run the full Section 1.3 pipeline:
#   clean_nyc.py -> clean_la.py -> merge.py -> validate.py
# Output from each step is also saved to output/run_log.txt.
#
# Usage: ./run_all.sh
# Optional environment variables:
#   PYTHON          Python interpreter with pyspark installed (default: nearest .venv, else python3)
#   JAVA_HOME       Java 17 install (default: nearest .jdk/jdk-17*, else the system Java)
#   CS236_DATA_DIR  folder with the raw CSVs (default: nearest 'datasets' folder above this one)
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
for step in clean_nyc clean_la merge validate; do
  echo "=== $step ===" | tee -a output/run_log.txt
  # grep drops Spark's progress bars and startup warnings from the log.
  "$PY" "$step.py" 2>&1 | grep -vE 'WARN|^\[Stage|setLogLevel|Setting default log level' \
    | tee -a output/run_log.txt
done
