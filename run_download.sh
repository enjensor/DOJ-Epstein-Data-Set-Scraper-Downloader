#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="${1:-./_doj_epstein_pdfs}"

python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install playwright
python3 -m playwright install chromium

python3 epstein_scrape_and_download.py \
  --out "$OUT_DIR" \
  --dataset-start 1 \
  --dataset-end 12 \
  --headed \
  --use-chrome-channel