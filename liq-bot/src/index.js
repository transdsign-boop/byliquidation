import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import { config } from './config.js';
import { LiquidationScanner } from './core/scanner.js';
import { executeTrade, getTradeLog, resetTradeLog, getActivePositions, setInitialBalance, loadExistingPositions, hydrateTradeLog, hydratePositions, resetExecutorState } from './core/executor.js';
import { startMonitor, getStats, getPnlHistory, getTotalPnl, hydratePnl, resetPnl, reconcilePnl, resetMonitorState } from './core/monitor.js';
import { startPersistence, saveJSON, loadJSON } from './core/persistence.js';
import { instrumentCache } from './core/instruments.js';
import { loadVolumes, isLowVolume } from './core/volume-filter.js';
import { getWalletBalance } from './api/bybit.js';
import { connectTradeWs, disconnectTradeWs } from './api/ws-trade.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

async function main() {
  console.log('===========================================');
  console.log('  BYBIT LIQUIDATION COUNTERTRADER');
  console.log(`  Network: ${config.network.toUpperCase()}`);
  console.log(`  Position size: $${config.positionSizeUsd} (min ${config.minPositionPct}% of balance)`);
  console.log(`  Min TP profit: ${config.minTpPct}% of trade value`);
  console.log(`  TP fallback: ${config.takeProfitPct}%`);
  console.log(`  Max positions: ${config.maxPositions}`);
  console.log(`  Min liq value: $${config.minLiqValueUsd}`);
  console.log(`  Min 24h volume: $${(config.minTurnover24h / 1e6).toFixed(1)}M`);
  console.log(`  Leverage: ${config.leverage}x`);
  console.log(`  SL max risk: ${config.stopLossAccountPct}% of balance`);
  console.log(`  ATR period: ${config.atrPeriod} (${config.atrInterval}m candles)`);
  console.log(`  TP multiplier: ${config.tpAtrMultiplier}x ATR`);
  console.log(`  Trailing stop: ${config.trailingAtrMultiplier}x ATR`);
  console.log(`  Max hold time: ${config.maxHoldSeconds === 0 ? 'DISABLED' : config.maxHoldSeconds + 's'}`);
  console.log('===========================================');

  // 0. Restore persisted data from disk (including active positions)
  const saved = startPersistence({ getTradeLog, getPnlHistory, getTotalPnl, getActivePositions });
  hydrateTradeLog(saved.tradeLog);
  hydratePnl(saved.pnlHistory, saved.totalPnl);
  hydratePositions(saved.activePositions);

  // 0b. Restore runtime config overrides from disk
  const savedConfig = loadJSON('config_overrides.json');
  if (savedConfig) {
    if (savedConfig.minLiqValueUsd != null) config.minLiqValueUsd = savedConfig.minLiqValueUsd;
    if (savedConfig.maxHoldSeconds != null) config.maxHoldSeconds = savedConfig.maxHoldSeconds;
    if (savedConfig.maxPositions != null) config.maxPositions = savedConfig.maxPositions;
    if (savedConfig.trailingAtrMultiplier != null) config.trailingAtrMultiplier = savedConfig.trailingAtrMultiplier;
    if (savedConfig.atrInterval != null) config.atrInterval = String(savedConfig.atrInterval);
    if (savedConfig.entryOrderType != null) config.entryOrderType = savedConfig.entryOrderType;
    if (savedConfig.tpOrderType != null) config.tpOrderType = savedConfig.tpOrderType;
    if (savedConfig.timeExitOrderType != null) config.timeExitOrderType = savedConfig.timeExitOrderType;
    if (savedConfig.minTurnover24h != null) config.minTurnover24h = savedConfig.minTurnover24h;
    console.log(`[CONFIG] Restored overrides from disk: minLiq=$${config.minLiqValueUsd}, maxHold=${config.maxHoldSeconds}s`);
  }

  // 0c. Restore account credentials from disk (survives redeploy)
  const savedAccount = loadJSON('account_credentials.json');
  if (savedAccount && savedAccount.apiKey && savedAccount.apiSecret) {
    config.apiKey = savedAccount.apiKey;
    config.apiSecret = savedAccount.apiSecret;
    console.log(`[CONFIG] Restored account credentials from disk (key: ...${savedAccount.apiKey.slice(-4)})`);
  }

  // 1. Load instrument info
  await instrumentCache.load();

  // 1a. Load volume data for low-cap filter
  await loadVolumes();

  // 1b. Fetch initial account balance for SL calculation
  try {
    const walletRes = await getWalletBalance();
    if (walletRes.retCode === 0 && walletRes.result?.list?.[0]) {
      const balance = parseFloat(walletRes.result.list[0].totalWalletBalance);
      setInitialBalance(balance);
    } else {
      console.error('[INIT] Failed to fetch wallet balance:', walletRes.retMsg);
    }
  } catch (err) {
    console.error('[INIT] Wallet balance error:', err.message);
  }

  // 1c. Load any existing positions from Bybit into tracking
  await loadExistingPositions();

  // 1d. Reconcile pnlHistory with Bybit — backfill any missed trades
  await reconcilePnl();

  // Refresh instruments every 30 min
  setInterval(() => instrumentCache.load(), 30 * 60 * 1000);

  // 2. Connect trade WebSocket (orders via WS for lower latency)
  connectTradeWs();

  // 3. Start position monitor
  startMonitor();

  // 3. Start liquidation scanner
  const scanner = new LiquidationScanner();

  scanner.on('liquidation', (liqEvent) => {
    // Only trade on qualifying liquidations (above threshold)
    if (liqEvent.qualifies) {
      executeTrade(liqEvent).catch((err) => {
        console.error('[MAIN] Unhandled trade error:', err.message);
      });
    }

    // Enrich with filter status for dashboard
    const enriched = { ...liqEvent };
    if (instrumentCache.isBlocked(liqEvent.symbol)) {
      enriched.blocked = 'Pre-listing/blocked';
    } else if (isLowVolume(liqEvent.symbol)) {
      enriched.blocked = 'Low volume';
    }

    // Broadcast ALL liquidations to dashboard
    broadcastToClients({
      type: 'liquidation',
      data: enriched,
    });
  });

  scanner.connect();

  // 4. Start HTTP server (API + Dashboard)
  const app = express();

  // Serve dashboard
  app.use(express.static(path.join(__dirname, '..', 'frontend')));

  // API endpoints for dashboard
  app.get('/api/stats', (req, res) => {
    res.json(getStats());
  });

  app.get('/api/trades', (req, res) => {
    res.json(getTradeLog().slice(0, 100));
  });

  app.get('/api/positions', (req, res) => {
    res.json([...getActivePositions().values()]);
  });

  app.get('/api/pnl', (req, res) => {
    res.json({
      total: getPnlHistory().reduce((s, p) => s + p.pnl, 0),
      history: getPnlHistory().slice(0, 100),
    });
  });

  app.get('/api/scanner', (req, res) => {
    res.json({
      connected: scanner.alive,
      stats: scanner.stats,
    });
  });

  app.get('/api/config', (req, res) => {
    res.json({
      network: config.network,
      positionSizeUsd: config.positionSizeUsd,
      takeProfitPct: config.takeProfitPct,
      maxPositions: config.maxPositions,
      minLiqValueUsd: config.minLiqValueUsd,
      leverage: config.leverage,
      atrPeriod: config.atrPeriod,
      atrInterval: config.atrInterval,
      tpAtrMultiplier: config.tpAtrMultiplier,
      trailingAtrMultiplier: config.trailingAtrMultiplier,
      maxHoldSeconds: config.maxHoldSeconds,
      maxPositions: config.maxPositions,
      entryOrderType: config.entryOrderType,
      tpOrderType: config.tpOrderType,
      timeExitOrderType: config.timeExitOrderType,
      minTurnover24h: config.minTurnover24h,
      leverage: config.leverage,
    });
  });

  // API: update min liq threshold at runtime
  app.use(express.json());
  app.post('/api/config', (req, res) => {
    const updates = {};

    const { minLiqValueUsd, maxHoldSeconds, maxPositions, trailingAtrMultiplier, atrInterval, entryOrderType, tpOrderType, timeExitOrderType, minTurnover24h, leverage } = req.body;

    if (minLiqValueUsd != null && typeof minLiqValueUsd === 'number' && minLiqValueUsd >= 0) {
      const old = config.minLiqValueUsd;
      config.minLiqValueUsd = minLiqValueUsd;
      console.log(`[CONFIG] Min liq threshold changed: $${old} → $${minLiqValueUsd}`);
      updates.minLiqValueUsd = minLiqValueUsd;
    }

    if (maxHoldSeconds != null && typeof maxHoldSeconds === 'number' && maxHoldSeconds >= 0) {
      const old = config.maxHoldSeconds;
      config.maxHoldSeconds = maxHoldSeconds;
      console.log(`[CONFIG] Max hold time changed: ${old}s → ${maxHoldSeconds}s`);
      updates.maxHoldSeconds = maxHoldSeconds;
    }

    if (maxPositions != null && typeof maxPositions === 'number' && maxPositions >= 1) {
      const old = config.maxPositions;
      config.maxPositions = maxPositions;
      console.log(`[CONFIG] Max positions changed: ${old} → ${maxPositions}`);
      updates.maxPositions = maxPositions;
    }

    if (trailingAtrMultiplier != null && typeof trailingAtrMultiplier === 'number' && trailingAtrMultiplier > 0) {
      const old = config.trailingAtrMultiplier;
      config.trailingAtrMultiplier = trailingAtrMultiplier;
      console.log(`[CONFIG] Trailing ATR multiplier changed: ${old} → ${trailingAtrMultiplier}`);
      updates.trailingAtrMultiplier = trailingAtrMultiplier;
    }

    if (atrInterval != null && String(atrInterval).length > 0) {
      const old = config.atrInterval;
      config.atrInterval = String(atrInterval);
      console.log(`[CONFIG] ATR interval changed: ${old} → ${atrInterval}m`);
      updates.atrInterval = String(atrInterval);
    }

    if (entryOrderType != null && (entryOrderType === 'Market' || entryOrderType === 'Limit')) {
      const old = config.entryOrderType;
      config.entryOrderType = entryOrderType;
      console.log(`[CONFIG] Entry order type changed: ${old} → ${entryOrderType}`);
      updates.entryOrderType = entryOrderType;
    }

    if (tpOrderType != null && (tpOrderType === 'Market' || tpOrderType === 'Limit')) {
      const old = config.tpOrderType;
      config.tpOrderType = tpOrderType;
      console.log(`[CONFIG] TP order type changed: ${old} → ${tpOrderType}`);
      updates.tpOrderType = tpOrderType;
    }

    if (timeExitOrderType != null && (timeExitOrderType === 'Market' || timeExitOrderType === 'Limit')) {
      const old = config.timeExitOrderType;
      config.timeExitOrderType = timeExitOrderType;
      console.log(`[CONFIG] Time exit order type changed: ${old} → ${timeExitOrderType}`);
      updates.timeExitOrderType = timeExitOrderType;
    }

    if (minTurnover24h != null && typeof minTurnover24h === 'number' && minTurnover24h >= 0) {
      const old = config.minTurnover24h;
      config.minTurnover24h = minTurnover24h;
      console.log(`[CONFIG] Min turnover 24h changed: $${old} → $${minTurnover24h}`);
      updates.minTurnover24h = minTurnover24h;
    }

    if (leverage != null && typeof leverage === 'number' && leverage >= 1 && leverage <= 100) {
      const old = config.leverage;
      config.leverage = leverage;
      console.log(`[CONFIG] Leverage changed: ${old}x → ${leverage}x`);
      updates.leverage = leverage;
    }

    if (Object.keys(updates).length > 0) {
      // Persist overrides to disk so they survive restarts/deploys
      const existing = loadJSON('config_overrides.json') || {};
      saveJSON('config_overrides.json', { ...existing, ...updates });
      res.json({ ok: true, ...updates });
    } else {
      res.status(400).json({ ok: false, error: 'No valid config fields provided' });
    }
  });

  // API: reset all PnL and trade log data
  app.post('/api/reset', (req, res) => {
    resetPnl();
    resetTradeLog();
    // Also clear persisted files immediately
    import('./core/persistence.js').then(({ saveJSON }) => {
      saveJSON('total_pnl.json', 0);
      saveJSON('pnl_history.json', []);
      saveJSON('trade_log.json', []);
    });
    console.log('[API] All PnL and trade log data reset.');
    res.json({ ok: true });
  });

  // API: switch Bybit account (new API key/secret) at runtime
  app.post('/api/account-switch', async (req, res) => {
    const { apiKey, apiSecret } = req.body;
    if (!apiKey || !apiSecret || typeof apiKey !== 'string' || typeof apiSecret !== 'string') {
      return res.status(400).json({ ok: false, error: 'apiKey and apiSecret are required' });
    }

    try {
      console.log('[ACCOUNT-SWITCH] Switching account...');

      // 1. Update config credentials and persist to disk
      config.apiKey = apiKey;
      config.apiSecret = apiSecret;
      saveJSON('account_credentials.json', { apiKey, apiSecret });

      // 2. Reset ALL state — fresh start for new account
      resetExecutorState();
      resetMonitorState();

      // 3. Wipe ALL persisted data (trade data + config overrides)
      saveJSON('trade_log.json', []);
      saveJSON('pnl_history.json', []);
      saveJSON('total_pnl.json', 0);
      saveJSON('config_overrides.json', {});

      // 4. Reconnect trade WS with new credentials
      disconnectTradeWs();
      connectTradeWs();

      // 5. Fetch new wallet balance
      const walletRes = await getWalletBalance();
      let balance = 0;
      if (walletRes.retCode === 0 && walletRes.result?.list?.[0]) {
        balance = parseFloat(walletRes.result.list[0].totalWalletBalance);
        setInitialBalance(balance);
      } else {
        console.warn('[ACCOUNT-SWITCH] Could not fetch wallet balance:', walletRes.retMsg);
      }

      // 6. Load fresh data from new account
      await loadExistingPositions();
      await reconcilePnl();

      console.log(`[ACCOUNT-SWITCH] Done. Balance: $${balance.toFixed(2)}`);
      res.json({ ok: true, balance });
    } catch (err) {
      console.error('[ACCOUNT-SWITCH] Error:', err.message);
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Account balance
  let cachedAccount = null;

  async function fetchAccount() {
    try {
      const res = await getWalletBalance();
      if (res.retCode === 0 && res.result?.list?.[0]) {
        const acct = res.result.list[0];
        const coins = acct.coin || [];
        const usdt = coins.find(c => c.coin === 'USDT') || {};
        cachedAccount = {
          totalEquity: acct.totalEquity,
          totalWalletBalance: acct.totalWalletBalance,
          totalAvailableBalance: acct.totalAvailableBalance,
          totalMarginBalance: acct.totalMarginBalance,
          totalPerpUPL: acct.totalPerpUPL,
          usdtBalance: usdt.walletBalance || '0',
          usdtAvailable: usdt.availableToWithdraw || '0',
          usdtUPL: usdt.unrealisedPnl || '0',
        };

        // Keep executor balance in sync for position sizing
        const balance = parseFloat(acct.totalWalletBalance);
        if (balance > 0) setInitialBalance(balance);
      }
    } catch (err) {
      console.error('[ACCOUNT] Balance fetch error:', err.message);
    }
  }

  // Fetch immediately, then every 5 seconds
  fetchAccount();
  setInterval(fetchAccount, 5000);

  app.get('/api/account', (req, res) => {
    res.json(cachedAccount || {});
  });

  // SSE endpoint for real-time updates to dashboard
  const clients = new Set();

  app.get('/api/stream', (req, res) => {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      Connection: 'keep-alive',
    });
    res.write('data: {"type":"connected"}\n\n');

    clients.add(res);
    req.on('close', () => clients.delete(res));
  });

  function broadcastToClients(data) {
    const payload = `data: ${JSON.stringify(data)}\n\n`;
    for (const client of clients) {
      client.write(payload);
    }
  }

  // Broadcast stats every 2 seconds
  setInterval(() => {
    broadcastToClients({
      type: 'stats',
      data: getStats(),
    });
    broadcastToClients({
      type: 'positions',
      data: [...getActivePositions().values()],
    });
    if (cachedAccount) {
      broadcastToClients({
        type: 'account',
        data: cachedAccount,
      });
    }
  }, 2000);

  const port = config.dashboardPort;
  app.listen(port, '0.0.0.0', () => {
    console.log(`[SERVER] Dashboard + API running on http://0.0.0.0:${port}`);
  });
}

main().catch((err) => {
  console.error('[FATAL]', err);
  process.exit(1);
});
