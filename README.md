# ICT / App Strategy Notes

更新日期: 2026-03-23

## Clone 後啟動 live paper bot

這個 repo 可以在另一台機器 clone 後直接跑 multi-strategy paper signal bot。預設掃描：

- `BTCUSDT`
- `ETHUSDT`
- `ADAUSDT`

預設策略：

- `s5`
- `ict_killzone_opt3`

```bash
git clone https://github.com/tangjy0108/signalbot
cd signalbot
bash setup_new_machine.sh
```

接著編輯 `.env`，填入：

```bash
TELEGRAM_BOT_TOKEN=你的 token
TELEGRAM_CHAT_ID=你的 chat id
```

啟動：

```bash
python3 live_s5_24h_bot.py
```

查看 paper 結果：

```bash
python3 live_bot_summary.py
```

注意：`.env`、`*.db`、`*.log` 不會上傳到 GitHub，避免 Telegram token 和本機交易紀錄外洩。

## 今天做了什麼
- 把 BTC 的 ICT Killzone 模型從 Pine 研究版一路收斂到 `opt3`
- 把 `opt3` 接進 app，變成可在圖表與訊號卡上看到的即時策略
- 把 app 內 `opt3` 改成比較接近 Pine 的 state machine
- 簡化 cron，只保留比較核心的 `Killzone` 與 `Session Liquidity`

## 目前 Pine 檔案
- `BTC_ICT_Killzone_MVP.pine`
  - 最初版研究用，保留做基準
- `BTC_ICT_Killzone_baseline_audit.pine`
  - 用來拆 London / NY、Long / Short 的貢獻
- `BTC_ICT_Killzone_opt3.pine`
  - 目前最佳候選版
- `BTC_ICT_Killzone_opt5.pine`
  - 研究 NY short 修法的嘗試版，目前不是首選

## 目前結論
- `opt3` 是目前最值得保留的版本
- 真正有 edge 的不是「Killzone 這個名字」，而是：
  - `HTF bias`
  - `liquidity target`
  - `killzone time filter`
  - `sweep`
  - `displacement + MSS`
  - `FVG / retest`
  - `固定風控`
- 最有價值的結論之一：
  - `NY long` 比想像中強
  - `NY short` 目前偏弱，不值得硬修到很複雜

## 策略核心理解
- `Killzone` 不是訊號本身，是「值得專心看盤的時間窗」
- `Time` 提供效率最高的波動時段
- `Price` 提供市場想去拿的流動性
- `Structure` 幫你確認假突破有沒有真的結束
- 沒有 `sweep -> confirm -> retest`，就不要因為時間到了硬做

## BTC 版本的時間重點
以 2026-03-23 這段夏令時間為例:
- London Killzone: 台北 `14:00 - 17:00`
- 美國重要數據常見時間: 台北 `20:30`
- NY 現貨開盤 / ETF 開盤影響帶: 台北 `21:30`

註:
- 這是紐約夏令時間 `EDT / UTC-4`
- 到冬令時間後，台北對照會整體再晚 1 小時

## opt3 的實戰摘要
- `London`
  - 先看 Asia high / low 是否乾淨
  - 等 London 去掃 Asia 一側流動性
  - 再等 LTF `displacement + MSS`
  - 回踩 `FVG` 或有效入場區再做
- `NY AM`
  - 特別重視 `OR`、`20:30 數據`、`21:30 開盤`
  - `NY long` 有保留價值
  - `NY short` 暫時不要過度優化

## 小白版日內 Checklist
### A. 開盤前
- [ ] 今天是平日，不是週末
- [ ] 我知道今天有沒有 20:30 的美國數據
- [ ] 我已經畫好 `前日高 / 前日低`
- [ ] 我已經畫好 `Asia high / Asia low`
- [ ] 我知道 H1 / H4 偏多還是偏空
- [ ] 我有先決定今天優先看 `London` 還是 `NY`

### B. London
- [ ] 現在時間在 London Killzone
- [ ] Asia 高低點至少還有一邊是乾淨的
- [ ] 價格先去掃 Asia 高點或低點
- [ ] 掃完後有 `displacement`
- [ ] LTF 出現 `MSS`
- [ ] 有可用的 `FVG / retest`
- [ ] R/R 至少 `1:2`

### C. NY AM
- [ ] `OR` 已經建立
- [ ] 我知道今天是 `回補型` 還是 `延續型`
- [ ] 價格有先掃 `OR high / OR low` 或開盤流動性
- [ ] 出現 `displacement + MSS`
- [ ] 有回踩區可執行
- [ ] 沒有追第一根大 K

### D. 不做的情況
- [ ] 亞洲區間太髒，兩邊都掃過
- [ ] 沒有 `sweep`
- [ ] 有 `sweep` 但沒有 `MSS`
- [ ] 有結構但 R/R 太差
- [ ] 我只是怕錯過而想進場

## app 端目前進度
- `ICT Killzone Opt3` 已接進 app
- 圖上會顯示：
  - Asia Range
  - NY OR
  - Sweep Level
  - MSS
  - FVG
  - Entry / Stop / Target
- 右側會顯示狀態：
  - `WAITING_CONFIRM`
  - `WAITING_RETEST`
  - `ACTIVE_TRADE`
  - `LIVE_SIGNAL`
- `Signal Feed` 會記錄最近訊號
- 前端開著時，會依最新價格標記 `TP_HIT / SL_HIT`

## 目前還沒完成的正式化項目
- server-side TP / SL tracking
- signal 歷史資料庫
- 用最後一根已收 5m K 棒的 `high/low` 來判定 TP / SL
- 若同一根 K 同時碰到 TP 與 SL，要定義保守規則或標成 `AMBIGUOUS`

## 接下來最值得做
- 接一個資料庫，讓 open signal 可以被持久化
- 每 5 分鐘檢查 `OPEN` 訊號是否 hit TP / SL
- 把 Signal Feed 做成真正可回看的歷史面板
- 後續如果要再優化，不要先亂調 `opt3`
  - 先做更多區間驗證
  - 再決定是否要淘汰或弱化 `NY short`
