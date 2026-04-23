#!/usr/bin/env bash
set -euo pipefail

# Clear trade history only (positions/events), keep bot_state and schema.
# Usage:
#   ./reset_trade_records.sh
#   ./reset_trade_records.sh path/to/live_s5_bot.db
#   BOT_DB_PATH=custom.db ./reset_trade_records.sh

DB_PATH="${1:-${BOT_DB_PATH:-live_s5_bot.db}}"

if [ ! -f "$DB_PATH" ]; then
  echo "ERROR: database file not found: $DB_PATH"
  exit 1
fi

python3 - "$DB_PATH" <<'PY'
import sqlite3
import sys

path = sys.argv[1]
conn = sqlite3.connect(path)
try:
    cur = conn.cursor()
    cur.execute("DELETE FROM positions")
    cur.execute("DELETE FROM events")
    cur.execute("DELETE FROM sqlite_sequence WHERE name IN ('positions', 'events')")
    conn.commit()
    # Reclaim free pages after deletes.
    conn.execute("VACUUM")
    conn.commit()
finally:
    conn.close()

print(f"Trade history cleared: {path}")
print("Tables cleared: positions, events")
print("Tables preserved: bot_state, schema")
PY
