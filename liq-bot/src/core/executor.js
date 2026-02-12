import { config } from '../config.js';
import { placeOrder, setLeverage, setTradingStop, switchToOneWayMode, getOrderbook, getPositions, cancelOrder, getOrderDetail } from '../api/bybit.js';
import { placeOrderWs, isTradeWsReady } from '../api/ws-trade.js';
import { instrumentCache } from './instruments.js';
import { isLowVolume, getTurnover } from './volume-filter.js';
import { getATR } from './atr.js';

/**
 * Trade Executor
 *
 * When a liquidation is detected:
 * 1. Determine counter-trade direction (opposite of liquidated side)
 * 2. Place market order immediately
 * 3. Set take-profit AND stop-loss via Bybit TP/SL
 *
 * Stop-loss rule (funded trader):
 * - Every position MUST have a SL set immediately
 * - Max risk per position = SL_ACCOUNT_PCT% of initial account balance (default 3%)
 * - Uses Bybit TP/SL (not conditional triggers)
 */

// Track which symbols we've already set leverage for
const leverageSet = new Set();

// Active positions tracking
const activePositions = new Map(); // symbol -> position info

// Trade log for dashboard (can be hydrated from persistence)
let tradeLog = [];
const MAX_LOG_SIZE = 500;

export function hydrateTradeLog(saved) {
  if (saved && saved.length) {
    tradeLog = saved;
    console.log(`[EXECUTOR] Restored ${saved.length} trade log entries from disk.`);
  }
}

// Account balance — refreshed periodically
let initialBalance = 0;

export function setInitialBalance(balance) {
  const prev = initialBalance;
  initialBalance = balance;
  if (prev === 0) {
    console.log(`[EXECUTOR] Account balance set: $${balance.toFixed(2)}`);
  } else if (Math.abs(balance - prev) > 0.01) {
    console.log(`[EXECUTOR] Account balance updated: $${prev.toFixed(2)} → $${balance.toFixed(2)}`);
  }
}

export function getInitialBalance() {
  return initialBalance;
}

export function getTradeLog() {
  return tradeLog;
}

export function resetTradeLog() {
  tradeLog = [];
  console.log('[EXECUTOR] Trade log reset.');
}

// Lock to prevent race conditions on concurrent liquidation events
const pendingSymbols = new Set();

export function resetExecutorState() {
  activePositions.clear();
  leverageSet.clear();
  pendingSymbols.clear();
  tradeLog = [];
  initialBalance = 0;
  console.log('[EXECUTOR] State reset for account switch.');
}

export function getActivePositions() {
  return activePositions;
}

export async function loadExistingPositions() {
  const { getPositions } = await import('../api/bybit.js');
  try {
    const res = await getPositions();
    if (res.retCode !== 0) {
      console.error('[EXECUTOR] Failed to load existing positions:', res.retMsg);
      return;
    }

    let count = 0;
    for (const p of res.result.list) {
      const size = parseFloat(p.size);
      if (size <= 0) continue;

      const symbol = p.symbol;
      if (activePositions.has(symbol)) continue;

      const entryPrice = parseFloat(p.avgPrice);
      const trailDist = parseFloat(p.trailingStop || '0') || null;
      let trailActive = null;
      if (trailDist) {
        trailActive = p.side === 'Buy'
          ? instrumentCache.roundPrice(symbol, entryPrice + trailDist)
          : instrumentCache.roundPrice(symbol, entryPrice - trailDist);
      }

      const position = {
        symbol,
        side: p.side,
        entryPrice,
        qty: size,
        tpPrice: parseFloat(p.takeProfit || '0') || null,
        slPrice: parseFloat(p.stopLoss || '0') || null,
        orderId: null,
        openTime: parseInt(p.createdTime) || Date.now(),
        liqUsdValue: 0,
        execTimeMs: 0,
        atr: null,
        trailingStop: trailDist,
        trailActivePrice: trailActive,
        tpMethod: 'existing',
      };
      activePositions.set(symbol, position);
      count++;

      console.log(
        `[EXECUTOR] Loaded existing position: ${p.side} ${size} ${symbol} @ ${position.entryPrice} | TP: ${position.tpPrice || '—'} | SL: ${position.slPrice || '—'}`
      );
    }

    if (count > 0) {
      console.log(`[EXECUTOR] Loaded ${count} existing position(s) from Bybit.`);
    }
  } catch (err) {
    console.error('[EXECUTOR] Error loading existing positions:', err.message);
  }
}

export async function executeTrade(liqEvent) {
  const { symbol, side, price, usdValue } = liqEvent;
  const startTime = Date.now();

  // Check max positions (count pending + active)
  if (activePositions.size + pendingSymbols.size >= config.maxPositions) {
    logTrade(liqEvent, 'SKIPPED', 'Max positions reached', 0);
    return null;
  }

  // Don't stack same symbol (check both active and pending)
  if (activePositions.has(symbol) || pendingSymbols.has(symbol)) {
    logTrade(liqEvent, 'SKIPPED', 'Already in position', 0);
    return null;
  }

  // Lock this symbol immediately (synchronous, before any await)
  pendingSymbols.add(symbol);

  try {
    // Check instrument exists
    const inst = instrumentCache.get(symbol);
    if (!inst) {
      logTrade(liqEvent, 'SKIPPED', 'Unknown instrument', 0);
      return null;
    }

    // Skip pre-listing / non-Trading coins
    if (instrumentCache.isBlocked(symbol)) {
      logTrade(liqEvent, 'SKIPPED', 'Pre-listing/blocked coin', 0);
      return null;
    }

    // Skip low-volume coins (funded trader rule)
    if (isLowVolume(symbol)) {
      const turnover = getTurnover(symbol);
      logTrade(liqEvent, 'SKIPPED', `Low volume (<$5M 24h) — $${(turnover / 1e6).toFixed(1)}M`, 0);
      return null;
    }

    // Need initial balance to calculate SL
    if (initialBalance <= 0) {
      logTrade(liqEvent, 'SKIPPED', 'No account balance loaded', 0);
      return null;
    }

    // Counter-trade direction:
    // If longs got liquidated (side='Buy'), price dumped -> we BUY (expect bounce)
    // If shorts got liquidated (side='Sell'), price pumped -> we SELL (expect pullback)
    const tradeSide = side === 'Buy' ? 'Buy' : 'Sell';

    // Calculate position size — must be at least MIN_POSITION_PCT% of initial balance (notional)
    const minNotional = initialBalance * (config.minPositionPct / 100);
    const configNotional = config.positionSizeUsd * config.leverage;
    const notional = Math.max(configNotional, minNotional);

    const qty = instrumentCache.roundQty(
      symbol,
      notional / price
    );

    if (qty < inst.minQty) {
      logTrade(liqEvent, 'SKIPPED', 'Qty below minimum', 0);
      return null;
    }

    // Set one-way mode and leverage (only once per symbol)
    if (!leverageSet.has(symbol)) {
      await switchToOneWayMode(symbol).catch(() => {});
      const lev = Math.min(config.leverage, inst.maxLeverage);
      await setLeverage(symbol, lev).catch(() => {});
      leverageSet.add(symbol);
    }

    // --- Calculate TP and SL prices (ATR-based with fallback) ---

    let atrValue = null;
    let tpPrice, trailingStopDist;
    let tpMethod = 'fixed';

    // Try ATR-based TP
    try {
      atrValue = await getATR(symbol);
    } catch (err) {
      console.error(`[EXECUTOR] ATR fetch error for ${symbol}:`, err.message);
    }

    if (atrValue && atrValue > 0) {
      // ATR-based TP: entry ± 1.5 × ATR
      const tpOffset = atrValue * config.tpAtrMultiplier;
      tpPrice = tradeSide === 'Buy'
        ? instrumentCache.roundPrice(symbol, price + tpOffset)
        : instrumentCache.roundPrice(symbol, price - tpOffset);

      // Trailing stop distance = 0.5 × ATR (minimum 1 tick)
      trailingStopDist = instrumentCache.roundPrice(symbol, atrValue * config.trailingAtrMultiplier);
      if (!trailingStopDist && inst.tickSize) {
        trailingStopDist = inst.tickSize;
        console.warn(`[EXECUTOR] ${symbol} trailing stop rounded to 0, using 1 tick: ${inst.tickSize}`);
      }
      tpMethod = 'ATR';

      console.log(`[EXECUTOR] ATR for ${symbol}: ${atrValue.toFixed(6)} | TP offset: ${tpOffset.toFixed(6)} | Trailing: ${trailingStopDist}`);
    } else {
      // Fallback: fixed percentage
      const tpMultiplier = tradeSide === 'Buy'
        ? 1 + config.takeProfitPct / 100
        : 1 - config.takeProfitPct / 100;
      tpPrice = instrumentCache.roundPrice(symbol, price * tpMultiplier);
      trailingStopDist = null;

      console.log(`[EXECUTOR] ATR unavailable for ${symbol}, using fixed ${config.takeProfitPct}% TP`);
    }

    // Enforce min TP: profit must be >= MIN_TP_PCT% of trade notional value
    const minProfitUsd = notional * (config.minTpPct / 100);
    const minTpOffset = minProfitUsd / qty;
    const currentTpOffset = Math.abs(tpPrice - price);

    if (currentTpOffset < minTpOffset) {
      const oldTp = tpPrice;
      tpPrice = tradeSide === 'Buy'
        ? instrumentCache.roundPrice(symbol, price + minTpOffset)
        : instrumentCache.roundPrice(symbol, price - minTpOffset);
      console.log(`[EXECUTOR] TP widened for ${symbol}: ${oldTp} → ${tpPrice} (min ${config.minTpPct}% of $${notional.toFixed(0)} = $${minProfitUsd.toFixed(2)} profit)`);
    }

    // Stop-loss: max risk = SL_ACCOUNT_PCT% of initial balance (minimum 1 tick)
    const maxLossUsd = initialBalance * (config.stopLossAccountPct / 100);
    let slOffset = maxLossUsd / qty;
    if (inst.tickSize && slOffset < inst.tickSize) {
      slOffset = inst.tickSize;
      console.warn(`[EXECUTOR] ${symbol} SL offset below tick size, using 1 tick: ${inst.tickSize}`);
    }
    const slPrice = tradeSide === 'Buy'
      ? instrumentCache.roundPrice(symbol, price - slOffset)
      : instrumentCache.roundPrice(symbol, price + slOffset);


    // --- EXECUTE ORDER (Limit PostOnly or Market) ---
    let orderResult;
    let orderVia;
    const entryType = config.entryOrderType || 'Market';

    if (entryType === 'Limit') {
      // Limit PostOnly at best bid (Buy) / best ask (Sell) for maker fees
      let limitPrice = null;
      try {
        const ob = await getOrderbook(symbol, 1);
        if (ob.retCode === 0 && ob.result) {
          limitPrice = tradeSide === 'Buy'
            ? parseFloat(ob.result.b[0][0])   // best bid
            : parseFloat(ob.result.a[0][0]);   // best ask
          limitPrice = instrumentCache.roundPrice(symbol, limitPrice);
        }
      } catch (err) {
        console.warn(`[EXECUTOR] Orderbook error for ${symbol}:`, err.message);
      }

      if (!limitPrice) {
        // Fallback to market if orderbook unavailable
        if (isTradeWsReady()) {
          try {
            orderResult = await placeOrderWs(symbol, tradeSide, qty);
            orderVia = 'WS(mkt-fallback)';
          } catch {
            orderResult = await placeOrder(symbol, tradeSide, qty);
            orderVia = 'REST(mkt-fallback)';
          }
        } else {
          orderResult = await placeOrder(symbol, tradeSide, qty);
          orderVia = 'REST(mkt-fallback)';
        }
      } else {
        const limitParams = { price: String(limitPrice), timeInForce: 'PostOnly' };
        if (isTradeWsReady()) {
          try {
            orderResult = await placeOrderWs(symbol, tradeSide, qty, 'Limit', limitParams);
            orderVia = 'WS(limit)';
          } catch {
            orderResult = await placeOrder(symbol, tradeSide, qty, 'Limit', limitParams);
            orderVia = 'REST(limit)';
          }
        } else {
          orderResult = await placeOrder(symbol, tradeSide, qty, 'Limit', limitParams);
          orderVia = 'REST(limit)';
        }

        if (orderResult.retCode !== 0) {
          console.error(`[EXECUTOR] Limit rejected for ${symbol}: ${orderResult.retMsg} (code ${orderResult.retCode})`);
          logTrade(liqEvent, 'SKIPPED', `Limit rejected: ${orderResult.retMsg}`, Date.now() - startTime);
          return null;
        }

        // Wait for fill — PostOnly may sit on the book
        const limitOrderId = orderResult.result.orderId;
        await new Promise(r => setTimeout(r, 2000));

        try {
          const orderCheck = await getOrderDetail(symbol, limitOrderId);
          if (orderCheck.retCode === 0 && orderCheck.result?.list?.length) {
            const status = orderCheck.result.list[0].orderStatus;
            if (status !== 'Filled') {
              await cancelOrder(symbol, limitOrderId).catch(() => {});
              logTrade(liqEvent, 'SKIPPED', `Limit not filled (${status})`, Date.now() - startTime);
              return null;
            }
          }
        } catch (err) {
          console.warn(`[EXECUTOR] Order status check failed for ${symbol}:`, err.message);
        }

        console.log(`[EXECUTOR] ${symbol} limit order filled @ ${limitPrice} (maker fee)`);
      }
    } else {
      // Market order (existing flow)
      if (isTradeWsReady()) {
        try {
          orderResult = await placeOrderWs(symbol, tradeSide, qty);
          orderVia = 'WS';
        } catch {
          orderResult = await placeOrder(symbol, tradeSide, qty);
          orderVia = 'REST(fallback)';
        }
      } else {
        orderResult = await placeOrder(symbol, tradeSide, qty);
        orderVia = 'REST';
      }
    }

    const execTime = Date.now() - startTime;

    if (orderResult.retCode !== 0) {
      console.error(`[EXECUTOR] Order FAILED for ${symbol}: ${orderResult.retMsg} (code ${orderResult.retCode}) | ${execTime}ms`);
      logTrade(liqEvent, 'FAILED', orderResult.retMsg, execTime);
      return null;
    }

    const orderId = orderResult.result.orderId;

    // Fetch real fill price from Bybit position data
    let fillPrice = price; // fallback to liq price
    try {
      const posRes = await getPositions();
      if (posRes.retCode === 0) {
        const pos = posRes.result.list.find(p => p.symbol === symbol && parseFloat(p.size) > 0);
        if (pos) {
          fillPrice = parseFloat(pos.avgPrice);
          console.log(`[EXECUTOR] ${symbol} fill price: ${fillPrice} (liq price was ${price})`);
        }
      }
    } catch (err) {
      console.warn(`[EXECUTOR] Could not fetch fill price for ${symbol}, using liq price`);
    }

    // Recalculate SL from fill price
    const slPrice2 = tradeSide === 'Buy'
      ? instrumentCache.roundPrice(symbol, fillPrice - slOffset)
      : instrumentCache.roundPrice(symbol, fillPrice + slOffset);

    // Recalculate TP from fill price — only used when trailing stop is unavailable
    if (trailingStopDist) {
      // Trailing stop active → no fixed TP, let winners run
      tpPrice = null;
      console.log(`[EXECUTOR] ${symbol} using trailing stop only (no TP cap) | Trail: ${trailingStopDist} | SL: ${slPrice2}`);
    } else {
      // No trailing → use fixed TP as exit
      if (atrValue && atrValue > 0) {
        const tpOffset = atrValue * config.tpAtrMultiplier;
        tpPrice = tradeSide === 'Buy'
          ? instrumentCache.roundPrice(symbol, fillPrice + tpOffset)
          : instrumentCache.roundPrice(symbol, fillPrice - tpOffset);
      } else {
        const tpMultiplier = tradeSide === 'Buy'
          ? 1 + config.takeProfitPct / 100
          : 1 - config.takeProfitPct / 100;
        tpPrice = instrumentCache.roundPrice(symbol, fillPrice * tpMultiplier);
      }

      // Enforce min TP
      const minTpOffset2 = (notional * (config.minTpPct / 100)) / qty;
      const currentTpOffset2 = Math.abs(tpPrice - fillPrice);
      if (currentTpOffset2 < minTpOffset2) {
        tpPrice = tradeSide === 'Buy'
          ? instrumentCache.roundPrice(symbol, fillPrice + minTpOffset2)
          : instrumentCache.roundPrice(symbol, fillPrice - minTpOffset2);
      }
    }

    // Set SL (+ TP if no trailing), then trailing stop in a separate call
    const stopParams = { stopLoss: slPrice2 };
    if (tpPrice) {
      stopParams.takeProfit = tpPrice;
      // Limit TP: Bybit places limit order when TP triggers (maker fee on exit)
      if (config.tpOrderType === 'Limit') {
        stopParams.tpOrderType = 1;
        stopParams.tpLimitPrice = tpPrice;
      }
    }

    // activePrice = entry + trailingDist → trailing activates once in the green by 1x trail distance
    // At activation: stop = activePrice - trail = fillPrice (worst case = entry, fees only)
    let trailActivePrice = null;
    if (trailingStopDist) {
      trailActivePrice = tradeSide === 'Buy'
        ? instrumentCache.roundPrice(symbol, fillPrice + trailingStopDist)
        : instrumentCache.roundPrice(symbol, fillPrice - trailingStopDist);
    }

    // Set SL first, then trailing stop — using await for reliable sequential execution
    try {
      const slRes = await setTradingStop(symbol, stopParams);
      if (slRes.retCode !== 0) {
        console.error(`[EXECUTOR] Failed to set SL${tpPrice ? '/TP' : ''} for ${symbol}: ${slRes.retMsg}`);
      } else {
        const tpType = tpPrice && config.tpOrderType === 'Limit' ? ' (limit)' : '';
        console.log(`[EXECUTOR] SL${tpPrice ? '/TP' : ''} set for ${symbol} | SL: ${slPrice2}${tpPrice ? ` | TP: ${tpPrice}${tpType}` : ''}`);
      }

      // Set trailing stop with activePrice — activates once price moves by 1x trail distance
      if (trailingStopDist && trailActivePrice) {
        const trailRes = await setTradingStop(symbol, { trailingStop: trailingStopDist, activePrice: trailActivePrice });
        if (trailRes.retCode !== 0) {
          console.error(`[EXECUTOR] Failed to set trailing stop for ${symbol}: ${trailRes.retMsg}`);
        } else {
          console.log(`[EXECUTOR] Trailing stop set for ${symbol} | Trail: ${trailingStopDist} | Activates @ ${trailActivePrice}`);
        }
      }
    } catch (err) {
      console.error(`[EXECUTOR] Failed to set SL/trailing for ${symbol}:`, err.message);
    }

    // Determine if entry was maker or taker
    const entryOrderMode = orderVia.includes('limit') ? 'maker' : 'taker';

    // Track position
    const position = {
      symbol,
      side: tradeSide,
      entryPrice: fillPrice,
      qty,
      tpPrice,
      slPrice: slPrice2,
      orderId,
      openTime: Date.now(),
      liqUsdValue: usdValue,
      execTimeMs: execTime,
      atr: atrValue,
      trailingStop: trailingStopDist,
      trailActivePrice,
      tpMethod,
      entryOrderMode,
    };
    activePositions.set(symbol, position);

    const tpDisplay = tpPrice || 'TRAIL';
    logTrade(liqEvent, 'FILLED', `Fill: ${fillPrice} | ${tpPrice ? `TP @ ${tpPrice}` : `Trail: ${trailingStopDist}`} | SL @ ${slPrice2} [${tpMethod}]`, execTime, position);

    console.log(
      `[TRADE] ${tradeSide} ${qty} ${symbol} @ ${fillPrice} (liq ${price}) | ${tpPrice ? `TP: ${tpPrice}` : `Trail: ${trailingStopDist}`} | SL: ${slPrice2} | ${tpMethod} | ${orderVia} | Exec: ${execTime}ms | Liq: $${usdValue.toFixed(0)}`
    );

    return position;
  } catch (err) {
    const execTime = Date.now() - startTime;
    logTrade(liqEvent, 'ERROR', err.message, execTime);
    console.error(`[EXECUTOR] Order error for ${symbol}:`, err.message);
    return null;
  } finally {
    pendingSymbols.delete(symbol);
  }
}

function logTrade(liqEvent, status, detail, execTimeMs, position = null) {
  const entry = {
    timestamp: Date.now(),
    symbol: liqEvent.symbol,
    liqSide: liqEvent.side,
    liqPrice: liqEvent.price,
    liqUsdValue: liqEvent.usdValue,
    status,
    detail,
    execTimeMs,
    position: position ? {
      orderId: position.orderId,
      side: position.side,
      qty: position.qty,
      entryPrice: position.entryPrice,
      tpPrice: position.tpPrice,
      slPrice: position.slPrice,
      atr: position.atr,
      trailingStop: position.trailingStop,
      tpMethod: position.tpMethod,
    } : null,
  };

  tradeLog.unshift(entry);
  if (tradeLog.length > MAX_LOG_SIZE) tradeLog.pop();
}
