#!/usr/bin/env bash
set -euo pipefail

export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:$PATH"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON="/Users/lhuang6/Desktop/sto-env/bin/python"

cd "$SCRIPT_DIR"
exec "$PYTHON" run.py
