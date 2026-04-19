from __future__ import annotations

import sqlite3

from live_s5_24h_bot import Config, init_db


def main() -> None:
    cfg = Config()
    conn = sqlite3.connect(cfg.db_path)
    init_db(conn)
    conn.close()
    print(f"initialized sqlite db: {cfg.db_path}")


if __name__ == "__main__":
    main()
