#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

python3 -m pip install -r requirements.txt

if [ ! -f ".env" ]; then
  cp .env.example .env
  echo "created .env from .env.example"
else
  echo ".env already exists, keeping it"
fi

python3 init_live_bot_db.py

echo ""
echo "Next:"
echo "1. Edit .env and fill TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID"
echo "2. Run: python3 live_s5_24h_bot.py"
echo "3. Check results: python3 live_bot_summary.py"
