#!/usr/bin/env bash
# setup_env.sh - One-time environment setup for the pipeline (no sudo needed).
#   * Creates a Python virtual environment at ./.venv with pyspark==3.5.1
#   * If no Java is available, downloads a local OpenJDK 17 (Temurin) into ./.jdk
# Run from the Dataset-Processing folder:  ./setup_env.sh
set -euo pipefail
cd "$(dirname "$0")"

if ! command -v java >/dev/null 2>&1 && [ -z "${JAVA_HOME:-}" ]; then
  case "$(uname -s)" in Darwin) os=mac ;; *) os=linux ;; esac
  case "$(uname -m)" in arm64|aarch64) arch=aarch64 ;; *) arch=x64 ;; esac
  echo "Downloading OpenJDK 17 ($os/$arch) into .jdk ..."
  mkdir -p .jdk
  curl -sL "https://api.adoptium.net/v3/binary/latest/17/ga/$os/$arch/jdk/hotspot/normal/eclipse" \
    | tar -xz -C .jdk
fi

python3 -m venv .venv
.venv/bin/pip install -q -r requirements.txt
if [ ! -f ../datasets/nyc_311_800k.csv ] || [ ! -f ../datasets/la_311_800k.csv ]; then
  echo "NOTE: raw CSVs not found in CS236-Project/datasets/."
  echo "      Download them from UCR eLearn (see ../datasets/README.md):"
  echo "      https://elearn.ucr.edu/courses/238702/files/26557259/download?download_frd=1"
fi
echo "Environment ready. Run ./run_all.sh"
