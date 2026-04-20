#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if command -v python3 >/dev/null 2>&1; then
  PYTHON_CMD="python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON_CMD="python"
else
  echo "Error: Python is not installed or not in PATH."
  echo "Please install Python 3 from https://www.python.org/downloads/ and try again."
  exit 127
fi

"${PYTHON_CMD}" -m pip install -r requirements.txt

if [ ! -f ".env" ]; then
  cp .env.example .env
  echo "created .env from .env.example"
else
  echo ".env already exists, keeping it"
fi

"${PYTHON_CMD}" init_live_bot_db.py

echo ""
echo "Next:"
echo "1. Edit .env and fill TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID"
echo "2. Run: ${PYTHON_CMD} live_s5_24h_bot.py"
echo "3. Check results: ${PYTHON_CMD} live_bot_summary.py"
