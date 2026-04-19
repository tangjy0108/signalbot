# Multi-strategy 24小時監控腳本（Python）

檔案：`live_s5_24h_bot.py`

用途：
- 24 小時輪詢 Binance 15m K 線
- 預設掃描 `BTCUSDT` / `ETHUSDT` / `ADAUSDT`
- 同時跑兩個 paper 策略：
  - S5（`BOS + FVG + RSI`, `RR=1.5`）
  - ICT Killzone Opt3（從 `BTC_ICT_Killzone_opt3.pine` 移植）
- 記錄開倉/平倉到 SQLite，可按 `symbol` / `strategy_id` 看結果
- 可選 Telegram 通知

> 目前是「訊號/模擬倉位」版，**不會**直接下單到交易所。

---

## 1) 安裝

```bash
python -m pip install pandas requests numpy
```

---

## 2) 環境變數

此腳本沿用 `souptradesignal` 的 Telegram 設定名稱：

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

你可以直接用 shell export：

```bash
export BOT_SYMBOLS=BTCUSDT,ETHUSDT,ADAUSDT
export BOT_STRATEGIES=s5,ict_killzone_opt3
export BOT_INTERVAL=15m
export BOT_NOTIONAL_USDT=200
export BOT_DB_PATH=live_s5_bot.db
export BOT_LOOP_SECONDS=20
export BOT_HEARTBEAT_MINUTES=60
export BOT_LOG_LEVEL=DEBUG
export BOT_LOG_PATH=live_s5_bot.log

# optional telegram
export TELEGRAM_BOT_TOKEN=123456:xxxx
export TELEGRAM_CHAT_ID=123456789
```

也可以建立本機 `.env`。腳本啟動時會自動讀取目前資料夾的 `.env`：

```bash
cp .env.example .env
```

然後把 `.env` 裡的 `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` 填上。`.env` 已加入 `.gitignore`，避免誤提交。

---

## 2.5) Debug log

啟動後 Terminal 會持續印出 debug log，也會同步寫入 `live_s5_bot.log`。你會看到每個幣種和策略的狀態，類似：

```text
INFO alive tick=1 symbol=BTCUSDT bars=600 closed_bar=2026-04-19T01:30:00+00:00 close=...
DEBUG checking signals symbol=BTCUSDT strategy=s5 closed_bar=...
DEBUG checking signals symbol=BTCUSDT strategy=ict_killzone_opt3 closed_bar=...
DEBUG tick=1 finished in 1.25s; sleeping 20s
```

如果想少一點訊息，可以把 `.env` 裡改成：

```bash
BOT_LOG_LEVEL=INFO
```

---

## 3) 啟動

```bash
python live_s5_24h_bot.py
```

建議用 `tmux` / `screen` / systemd / PM2 讓它長時間跑。

---

## 4) 查看 paper 結果

SQLite：`live_s5_bot.db`

- `positions`：每筆開/平倉（symbol/strategy/entry/stop/tp/exit/pnl）
- `events`：事件與錯誤日志
- `bot_state`：去重與 heartbeat 狀態

快速查看統計：

```bash
python live_bot_summary.py
```

也可以直接查 SQLite：

```bash
sqlite3 live_s5_bot.db "SELECT symbol, strategy_id, COUNT(*), ROUND(SUM(pnl_usdt), 4) FROM positions WHERE status='CLOSED' GROUP BY symbol, strategy_id;"
```

---

## 5) 和回測一致的關鍵

- 只用「已收線」15m K 判斷訊號（避免 future leak）
- 進場用下一根 open（含 slippage）
- 出場邏輯：同根同時 hit SL/TP 時，以 `stop` 優先（保守）
- ICT Killzone Opt3 會依 Pine 設定在 NY 11:00 ET 後強制平倉
- 手續費與滑點參數可用環境變數調整

---

## 6) 如果你要接「真實下單」

你可以在 `open_position(...)` 那段之前，改成先呼叫交易所 API 下單，並回填：
- 真實成交價
- 真實手續費
- 訂單 id / 交易 id

再把成交結果寫回 `positions`，這樣回測/實盤對帳會更準。
