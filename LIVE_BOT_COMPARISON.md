# live_s5_24h_bot.py 版本对比

## 概述

- **用户提供版本**：简单的单策略、单交易所 bot（~300 行）
- **现有文件版本**：高级多策略、多交易所 bot（~900 行）

---

## 主要差异

### 1️⃣ 交易所支持

**用户版本：**

- 仅 Binance
- `BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"`

**现有版本：**

- 支持 Binance、KuCoin、Bitget
- `BINANCE_KLINES_URL`
- `KUCOIN_CANDLES_URL = "https://api.kucoin.com/api/v1/market/candles"`
- `BITGET_CANDLES_URL = "https://api.bitget.com/api/v2/spot/market/candles"`
- `SUPPORTED_EXCHANGES = {"binance", "kucoin", "bitget"}`
- 各交易所有独立的 `fetch_klines_*()` 函数和 interval map

### 2️⃣ 策略支持

**用户版本：**

- 仅 S5 策略
- `from research import combined_signals`
- 固定 `S5_PARAMS`

**现有版本：**

- S5 + ICT Killzone Opt3
- `from s5_strategy_core import combined_signals`
- `from ict_killzone_opt3_core import killzone_opt3_signals, should_force_flat_after_ny`
- 支持多策略：`BOT_STRATEGIES` 配置
- `STRATEGY_NAMES = {"s5": "S5...", "ict_killzone_opt3": "ICT Killzone Opt3"}`
- `strategy_signals()` 函数分发到不同策略

### 3️⃣ 币种支持

**用户版本：**

```python
@dataclass
class Config:
    symbol: str = os.getenv("BOT_SYMBOL", "BTCUSDT")
```

- 仅单个币种

**现在版本：**

```python
@dataclass
class Config:
    symbols_raw: str = os.getenv("BOT_SYMBOLS", "BTCUSDT,ETHUSDT,ADAUSDT")

    @property
    def symbols(self) -> list[str]:
        return [s.strip().upper() for s in self.symbols_raw.split(",") if s.strip()]
```

- 支持逗号分隔的多币种列表

### 4️⃣ 配置管理

**用户版本：**

- 直接从环境变量读取
- 无环境文件支持

**现在版本：**

```python
def load_env_file(path: Optional[str] = None) -> None:
    # 从 .env 文件加载配置
    env_path = path or os.getenv("BOT_ENV_PATH", ".env")
```

- 支持 `.env` 文件
- 定期环境变量覆盖现有的

### 5️⃣ 日志管理

**用户版本：**

- 无日志系统
- TG 消息作为唯一反馈

**现在版本：**

```python
LOGGER = logging.getLogger("multi_signal_bot")

def setup_logging(cfg: Config) -> None:
    # 支持控制台 + 文件日志
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if cfg.log_path:
        handlers.append(logging.FileHandler(cfg.log_path, encoding="utf-8"))
```

- 控制台日志
- 文件日志（`BOT_LOG_PATH`）
- 日志级别可配置（`BOT_LOG_LEVEL`）

### 6️⃣ 数据库字段扩展

**用户版本：**

```python
CREATE TABLE positions (
    id, symbol, side, entry_time_utc, entry_price,
    stop_price, tp_price, signal_bar_time_utc, status,
    exit_time_utc, exit_price, exit_reason, pnl_usdt
)
```

**现在版本：**

```python
CREATE TABLE positions (
    ...(上面的所有字段)...，
    strategy_id, strategy_name, setup_session, setup_type
    # 还有数据库索引: idx_positions_open_strategy_symbol
)
```

- 追踪使用的策略
- 追踪 Setup 信息（ICT Killzone Opt3 需要）
- 数据库迁移支持：`ensure_position_columns()`

### 7️⃣ 仓位管理

**用户版本：**

```python
# get_open_position 只查询一个仓位
while True:
    pos = get_open_position(conn)  # 直接获取
    if pos:
        # 管理仓位
    if pos:  # 再次检查
        # 开仓...
```

**现在版本：**

```python
# 多策略支持
def get_open_position(conn, symbol: str, strategy_id: str) -> Optional[sqlite3.Row]:
    # 按 symbol + strategy_id 查询

def manage_position(conn, cfg, pos, df, closed_idx, closed_ts) -> None:
    # 仓位管理逻辑独立成函数

def try_open_new_position(
    conn, cfg, symbol, strategy_id, df, closed_idx, open_idx,
    closed_ts, current_open_ts
) -> None:
    # 新仓位开启逻辑独立成函数
    # 包含签名验证和设置跟踪

# main() 循环：
for symbol in cfg.symbols:
    for strategy_id in cfg.strategies:
        pos = get_open_position(conn, symbol, strategy_id)
        if pos:
            manage_position(...)
        if not pos:
            try_open_new_position(...)
```

### 8️⃣ 止盈止损处理

**用户版本：**

```python
if side == "long":
    hit_sl = low <= stop_px
    hit_tp = high >= tp_px
else:
    hit_sl = high >= stop_px
    hit_tp = low <= tp_px
```

**现在版本：**

- 同样逻辑 ✓
- **加入 ICT Killzone 特殊处理**：
  ```python
  if raw_exit is None and strategy_id == "ict_killzone_opt3":
      if should_force_flat_after_ny(pd.Timestamp(closed_ts), KILLZONE_PARAMS):
          raw_exit, reason = close, "flat_after_ny"
  ```

### 9️⃣ Heartbeat 报告

**用户版本：**

```python
# 简单报告
pnl_today, trades, wins, win_rate
hb_msg = f"❤️ 機器人運作正常\n..."
```

**现在版本：**

```python
def send_heartbeat(conn, cfg) -> None:
    # 按 symbol + strategy_id 分组今日收益
    # 显示所有已平仓交易总损益
    # 显示目前持仓数
    # 显示累计总胜率
    # 更详细的分层报告
```

### 🔟 SSL/TLS 支持

**用户版本：**

- 无 SSL 控制

**现在版本：**

```python
@property
def ssl_verify(self) -> bool:
    return self.ssl_verify_raw.strip().lower() not in {"0", "false", "no", "off"}

@property
def requests_verify(self) -> bool | str:
    if not self.ssl_verify:
        return False
    if self.ca_bundle_path.strip():
        return self.ca_bundle_path.strip()  # 自定义 CA bundle
    return True
```

- 支持禁用 SSL 验证
- 支持自定义 CA bundle 路径

### 1️⃣1️⃣ 性能监控

**用户版本：**

- 无性能跟踪

**现在版本：**

```python
tick = 0
while True:
    tick += 1
    started = time.monotonic()
    try:
        # ... 处理逻辑 ...
    finally:
        elapsed = time.monotonic() - started
        LOGGER.debug("tick=%s finished in %.2fs", tick, elapsed)
        time.sleep(cfg.loop_seconds)
```

- 每次循环计时
- 记录处理时间

### 1️⃣2️⃣ 导入变化

**用户版本：**

```python
from research import combined_signals
from live_practical_session_report import apply_slippage
```

**现在版本：**

```python
from ict_killzone_opt3_core import DEFAULT_PARAMS as KILLZONE_PARAMS
from ict_killzone_opt3_core import killzone_opt3_signals, should_force_flat_after_ny
from s5_strategy_core import combined_signals
```

---

## 配置环境变量对比

### 用户版本支持的变量：

```
BOT_SYMBOL=BTCUSDT
BOT_INTERVAL=15m
BOT_KLINE_LIMIT=600
BOT_DB_PATH=live_s5_bot.db
BOT_LOOP_SECONDS=20
BOT_HEARTBEAT_MINUTES=60
BOT_NOTIONAL_USDT=200
BOT_FEE_PER_SIDE=0.0004
BOT_SLIPPAGE_PER_SIDE=0.0002
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
```

### 现有版本支持的变量：

```
# 以上所有变量，加上：
BOT_SYMBOLS=BTCUSDT,ETHUSDT,ADAUSDT          # 多币种
BOT_STRATEGIES=s5,ict_killzone_opt3          # 多策略
BOT_EXCHANGE=bitget                          # 交易所选择
BOT_LOG_LEVEL=DEBUG                          # 日志级别
BOT_LOG_PATH=live_s5_bot.log                 # 日志文件
BOT_SSL_VERIFY=true                          # SSL 验证
BOT_CA_BUNDLE=/path/to/ca-bundle.crt         # 自定义 CA
BOT_ENV_PATH=.env                            # 读取环境文件路径
```

---

## 代码量估算

| 版本     | 行数 | 特点                               |
| -------- | ---- | ---------------------------------- |
| 用户版本 | ~300 | 简洁、单策略、单交易所             |
| 现有版本 | ~900 | 企业级、多策略、多交易所、完整日志 |

---

## 迁移建议

如果要从用户版本迁移到现有版本，只需：

```bash
# 设置环境变量
export BOT_SYMBOLS=BTCUSDT  # 单币种模式
export BOT_STRATEGIES=s5    # 仅 S5 策略
export BOT_EXCHANGE=binance # Binance

# 运行
python live_s5_24h_bot.py
```

现有版本完全向后兼容（默认配置与用户版本相似）。
