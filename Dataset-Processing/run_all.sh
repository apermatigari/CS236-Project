#!/usr/bin/env bash
# Runs the full Section 1.3 pipeline. Usage: ./run_all.sh
set -euo pipefail
cd "$(dirname "$0")"
ROOT="$(cd .. && pwd)"
export JAVA_HOME="${JAVA_HOME:-$(ls -d "$ROOT"/.jdk/jdk-17* 2>/dev/null | head -1)}"
PY="${PYTHON:-$ROOT/.venv/bin/python}"
for step in clean_nyc clean_la merge validate; do
  echo "=== $step ==="; "$PY" "$step.py"
done
