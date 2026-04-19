# 在新電腦 Clone 後啟動

```bash
git clone https://github.com/tangjy0108/signalbot
cd signalbot
bash setup_new_machine.sh
```

接著打開 `.env`，填入你的 Telegram：

```bash
TELEGRAM_BOT_TOKEN=你的 token
TELEGRAM_CHAT_ID=你的 chat id
```

啟動 bot：

```bash
python3 live_s5_24h_bot.py
```

查看 paper 結果：

```bash
python3 live_bot_summary.py
```

`.env`、`live_s5_bot.db`、`live_s5_bot.log` 會在新電腦本機產生，不需要從 GitHub 下載舊的。
