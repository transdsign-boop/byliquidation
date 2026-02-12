# Bybit Liquidation Countertrader

Speed-optimized bot that scans Bybit USDT perpetuals for liquidation events and counter-trades them with automatic take-profit.

## How It Works

1. **Scanner** — WebSocket connection to Bybit's `allLiquidation` feed (all USDT perps, single connection)
2. **Filter** — Only acts on liquidations above configured USD threshold
3. **Counter-trade** — When longs get liquidated (price dumps), bot buys. When shorts get liquidated (price pumps), bot sells
4. **Take Profit** — Immediately sets TP at configured percentage from entry
5. **Monitor** — Polls positions every 2s to detect TP fills and track PnL

## Speed Optimizations

- Raw WebSocket with compression/validation disabled
- Fire-and-forget leverage setting (cached per symbol)
- Non-blocking TP placement (set in parallel, don't wait)
- Pre-cached instrument data (tick sizes, lot sizes)
- SSE streaming to dashboard (no polling overhead)
- Single process, no framework bloat

## Quick Start (Codespace)

```bash
# 1. Clone and install
npm install

# 2. Configure
cp .env.example .env
# Edit .env with your Bybit API credentials

# 3. Run (testnet by default)
npm run dev
# Dashboard: http://localhost:3000
```

## Configuration (.env)

| Variable | Default | Description |
|----------|---------|-------------|
| `NETWORK` | testnet | `testnet` or `mainnet` |
| `POSITION_SIZE_USD` | 50 | USD notional per trade |
| `TAKE_PROFIT_PCT` | 0.3 | TP percentage (0.3 = 0.3%) |
| `MAX_POSITIONS` | 5 | Max concurrent positions |
| `MIN_LIQ_VALUE_USD` | 10000 | Min liquidation size to trigger |
| `LEVERAGE` | 5 | Leverage multiplier |

## Deploy to fly.io

```bash
# 1. Install flyctl
curl -L https://fly.io/install.sh | sh

# 2. Login & launch
fly auth login
fly launch    # Accept defaults, choose sjc region

# 3. Set secrets
fly secrets set \
  BYBIT_API_KEY=your_key \
  BYBIT_API_SECRET=your_secret \
  NETWORK=mainnet

# 4. Deploy
fly deploy

# 5. Open dashboard
fly open
```

## Project Structure

```
├── src/
│   ├── index.js           # Entry point, HTTP server, SSE
│   ├── config.js          # Environment config
│   ├── api/
│   │   └── bybit.js       # REST API client (orders, positions)
│   └── core/
│       ├── scanner.js      # WebSocket liquidation scanner
│       ├── executor.js     # Trade execution + TP logic
│       ├── instruments.js  # Tick/lot size cache
│       └── monitor.js      # Position sync + PnL tracking
├── frontend/
│   └── index.html          # Dashboard (single file)
├── .env.example
├── Dockerfile
├── fly.toml
└── package.json
```

## Dashboard

Real-time monitoring dashboard at `http://localhost:3000`:
- Total PnL, win rate, trade count
- Live liquidation feed (all filtered events)
- Trade log (filled, skipped, failed)
- Open positions with entry/TP prices

## Next Steps / Ideas

- [ ] Add stop-loss in addition to TP
- [ ] Dynamic position sizing based on liq magnitude
- [ ] Cooldown per symbol after loss
- [ ] Multiple TP levels (partial closes)
- [ ] Trailing stop
- [ ] Persistent trade log (SQLite)
- [ ] Telegram/Discord alerts
