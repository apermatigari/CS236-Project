#!/usr/bin/env bash
# One-time environment setup (no sudo needed): local JDK 17 + Python venv with PySpark 3.5.1.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
if ! ls -d .jdk/jdk-17* >/dev/null 2>&1; then
  mkdir -p .jdk
  curl -sL "https://api.adoptium.net/v3/binary/latest/17/ga/linux/x64/jdk/hotspot/normal/eclipse" | tar -xz -C .jdk
fi
python3 -m venv .venv
.venv/bin/pip install -q -r "Dataset Processing/requirements.txt"
echo "Done. JAVA_HOME=$(ls -d "$ROOT"/.jdk/jdk-17*)"
