#!/usr/bin/env bash
set -euo pipefail

# One-file setup for Google Cloud VM (Ubuntu/Debian)
# Usage:
#   chmod +x gcp_vm_onefile_setup.sh
#   ./gcp_vm_onefile_setup.sh

REPO_URL="${REPO_URL:-https://github.com/tangjy0108/signalbot.git}"
PROJECT_DIR="${PROJECT_DIR:-$HOME/signalbot}"
BOT_EXCHANGE_DEFAULT="${BOT_EXCHANGE_DEFAULT:-bitget}"

echo "[1/7] Installing system packages..."
sudo apt update
sudo apt -y upgrade
sudo apt install -y git python3 python3-venv python3-pip ca-certificates tzdata curl

echo "[2/7] Cloning project..."
if [ -d "$PROJECT_DIR/.git" ]; then
  echo "Project already exists at $PROJECT_DIR, pulling latest changes..."
  git -C "$PROJECT_DIR" pull --ff-only
else
  git clone "$REPO_URL" "$PROJECT_DIR"
fi

cd "$PROJECT_DIR"

echo "[3/7] Creating virtual environment..."
python3 -m venv .venv
# shellcheck disable=SC1091
source .venv/bin/activate

echo "[4/7] Installing Python dependencies..."
python -m pip install --upgrade pip setuptools wheel
pip install -r requirements.txt

echo "[5/7] Preparing .env..."
if [ ! -f .env ]; then
  cp .env.example .env
fi

# Ensure key defaults exist (append if missing)
grep -q '^BOT_EXCHANGE=' .env || echo "BOT_EXCHANGE=${BOT_EXCHANGE_DEFAULT}" >> .env
grep -q '^BOT_SYMBOLS=' .env || echo "BOT_SYMBOLS=BTCUSDT,ETHUSDT,ADAUSDT" >> .env
grep -q '^BOT_STRATEGIES=' .env || echo "BOT_STRATEGIES=s5,ict_killzone_opt3" >> .env
grep -q '^BOT_INTERVAL=' .env || echo "BOT_INTERVAL=15m" >> .env

echo "[6/7] Initializing local DB..."
python init_live_bot_db.py

echo "[7/7] Open .env in vi (fill TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)..."
vi .env

echo
echo "Setup complete. Next commands:"
echo "  cd $PROJECT_DIR"
echo "  source .venv/bin/activate"
echo "  python -u live_s5_24h_bot.py"
echo
echo "Run in background (optional):"
echo "  nohup ./.venv/bin/python live_s5_24h_bot.py > bot.out 2>&1 &"
echo "  echo \$! > bot.pid"
echo "  tail -f bot.out"
