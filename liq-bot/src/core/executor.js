import { config } from '../config.js';
import { placeOrder, setLeverage, setTradingStop, switchToOneWayMode, getOrderbook, getPositions, cancelOrder, getOrderDetail } from '../api/bybit.js';
import { placeOrderWs, isTradeWsReady } from '../api/ws-trade.js';
import { instrumentCache } from './instruments.js';
import { isLowVolume, getTurnover } from './volume-filter.js';
import { getATR } from './atr.js';
import { getVWAP } from './vwap.js';

/**
 * Trade Executor
 *
 * When a liquidation is detected:
 * 1. Determine counter-trade direction (opposite of liquidated side)
 * 2. Place market order immediately
 * 3. Set take-profit AND stop-loss via Bybit TP/SL
 *
 * DCA (Dollar-Cost Averaging):
 * - 3 pyramid entries: 20% → 30% → 50% of total position budget
 * - Each level triggered by a new qualifying liquidation on same symbol
 * - Price improvement required between entries
 * - After each add: recalculate SL and trailing stop from Bybit's new avgPrice
 *
 * Stop-loss rule (funded trader):
 * - Every position MUST have a SL set immediately
 * - Max risk per position = SL_ACCOUNT_PCT% of initial account balance (default 3%)
 * - Uses Bybit TP/SL (not conditional triggers)
 */

// DCA split ratios: 4 entries totaling 100% of position budget
// Pyramid up: start small, add more as price extends
const DCA_SPLITS = [0.10, 0.20, 0.30, 0.40];

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

// Serializable position state for persistence (survives deploys)
export function getPositionState() {
  const state = {};
  for (const [symbol, pos] of activePositions) {
    state[symbol] = {
      dcaLevel: pos.dcaLevel,
      totalBudget: pos.totalBudget,
      lastEntryPrice: pos.lastEntryPrice,
      atr: pos.atr,
    };
  }
  return state;
}

export function hydratePositionState(saved) {
  if (!saved) return;
  let count = 0;
  for (const [symbol, state] of Object.entries(saved)) {
    const pos = activePositions.get(symbol);
    if (!pos) continue;
    if (state.dcaLevel != null) pos.dcaLevel = state.dcaLevel;
    if (state.totalBudget != null) pos.totalBudget = state.totalBudget;
    if (state.lastEntryPrice != null) pos.lastEntryPrice = state.lastEntryPrice;
    if (state.atr != null) pos.atr = state.atr;
    count++;
  }
  if (count > 0) {
    console.log(`[EXECUTOR] Restored DCA state for ${count} position(s) from disk.`);
  }
}

export function resetTradeLog() {
  tradeLog = [];
  console.log('[EXECUTOR] Trade log reset.');
}

// Shared risk budget: divide total risk across all open positions
function getMaxLossPerPosition(positionCount) {
  const totalRiskUsd = initialBalance * (config.totalRiskPct / 100);
  return totalRiskUsd / positionCount;
}

async function tightenAllSLs(positionCount) {
  const maxLossUsd = getMaxLossPerPosition(positionCount);

  for (const [symbol, pos] of activePositions) {
    const inst = instrumentCache.get(symbol);
    if (!inst) continue;

    // Use totalExpectedQty (full budget qty) for consistent SL distance,
    // matching how initial entry SL is calculated. This way tightenAllSLs
    // actually tightens partially-filled positions instead of being a no-op.
    const expectedQty = pos.totalBudget > 0
      ? instrumentCache.roundQty(symbol, pos.totalBudget / pos.entryPrice)
      : pos.qty;
    let slOffset = maxLossUsd / expectedQty;
    if (inst.tickSize && slOffset < inst.tickSize) slOffset = inst.tickSize;
    const maxSlOffset = pos.entryPrice * 0.9;
    if (slOffset > maxSlOffset) slOffset = maxSlOffset;

    const newSL = pos.side === 'Buy'
      ? instrumentCache.roundPrice(symbol, pos.entryPrice - slOffset)
      : instrumentCache.roundPrice(symbol, pos.entryPrice + slOffset);

    if (newSL === pos.slPrice) continue;

    try {
      const res = await setTradingStop(symbol, { stopLoss: newSL });
      if (res.retCode === 0) {
        console.log(`[EXECUTOR] Tightened SL for ${symbol}: ${pos.slPrice} → ${newSL}`);
        pos.slPrice = newSL;
      }
    } catch (err) {
      console.error(`[EXECUTOR] Failed to tighten SL for ${symbol}:`, err.message);
    }
  }
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

export function getPendingSymbols() {
  return pendingSymbols;
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
        const feeBuffer = instrumentCache.roundPrice(symbol, entryPrice * 0.0015);
        trailActive = p.side === 'Buy'
          ? instrumentCache.roundPrice(symbol, entryPrice + trailDist + feeBuffer)
          : instrumentCache.roundPrice(symbol, entryPrice - trailDist - feeBuffer);
      }

      // Estimate budget from current position size (assumes level 0 = 10%)
      const currentNotional = size * entryPrice;
      const totalBudget = currentNotional / DCA_SPLITS[0];

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
        dcaLevel: 0,
        totalBudget,
        lastEntryPrice: entryPrice,
      };
      activePositions.set(symbol, position);
      count++;

      console.log(
        `[EXECUTOR] Loaded existing position: ${p.side} ${size} ${symbol} @ ${position.entryPrice} | TP: ${position.tpPrice || '-'} | SL: ${position.slPrice || '-'} | DCA: 0/${DCA_SPLITS.length}`
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

  // Granular latency tracking
  const timing = {
    preChecks: 0,
    leverage: 0,
    atr: 0,
    orderbook: 0,
    orderPlace: 0,
    orderFillWait: 0,
    positionFetch: 0,
    tpSlSet: 0,
    trailSet: 0,
  };

  // Check max positions (count pending + active)
  if (activePositions.size + pendingSymbols.size >= config.maxPositions) {
    logTrade(liqEvent, 'SKIPPED', 'Max positions reached', 0);
    return null;
  }

  // Don't allow concurrent trades on same symbol
  if (pendingSymbols.has(symbol)) {
    logTrade(liqEvent, 'SKIPPED', 'Trade pending on symbol', 0);
    return null;
  }

  const existingPos = activePositions.get(symbol);
  if (existingPos) {
    // DCA path: check if we can add to this position
    const dcaLevel = existingPos.dcaLevel || 0;
    if (dcaLevel >= DCA_SPLITS.length - 1) {
      logTrade(liqEvent, 'SKIPPED', `DCA fully filled (${DCA_SPLITS.length}/${DCA_SPLITS.length})`, 0);
      return null;
    }
    // Price improvement check using VWAP bands
    const vwapData = await getVWAP(symbol).catch(() => null);
    if (vwapData) {
      const { vwap, sd } = vwapData;
      const lowerBand = vwap - sd * config.dcaVwapSdMultiplier;
      const upperBand = vwap + sd * config.dcaVwapSdMultiplier;

      const priceImproved = existingPos.side === 'Buy'
        ? price <= lowerBand   // for longs, price must be at or below lower band
        : price >= upperBand;  // for shorts, price must be at or above upper band

      if (!priceImproved) {
        logTrade(liqEvent, 'SKIPPED', `DCA: price ${price.toFixed(4)} not at VWAP band (${existingPos.side === 'Buy' ? lowerBand.toFixed(4) : upperBand.toFixed(4)})`, 0);
        return null;
      }
      console.log(`[EXECUTOR] DCA VWAP check passed for ${symbol}: price ${price} ${existingPos.side === 'Buy' ? '<=' : '>='} band ${existingPos.side === 'Buy' ? lowerBand.toFixed(4) : upperBand.toFixed(4)}`);
    } else {
      // Fallback: simple price improvement
      const lastEntry = existingPos.lastEntryPrice || existingPos.entryPrice;
      const priceImproved = existingPos.side === 'Buy'
        ? price < lastEntry
        : price > lastEntry;
      if (!priceImproved) {
        logTrade(liqEvent, 'SKIPPED', `DCA price not improved (${price} vs last ${lastEntry})`, 0);
        return null;
      }
    }
    // Execute DCA add
    return executeDCA(liqEvent, existingPos);
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

    timing.preChecks = Date.now() - startTime;

    // Counter-trade direction:
    // If longs got liquidated (side='Buy'), price dumped -> we BUY (expect bounce)
    // If shorts got liquidated (side='Sell'), price pumped -> we SELL (expect pullback)
    const tradeSide = side === 'Buy' ? 'Buy' : 'Sell';

    // Calculate position size — must be at least MIN_POSITION_PCT% of initial balance (notional)
    const minNotional = initialBalance * (config.minPositionPct / 100);
    const configNotional = config.positionSizeUsd * config.leverage;
    const totalBudget = Math.max(configNotional, minNotional);
    const notional = totalBudget * DCA_SPLITS[0]; // 10% for first DCA entry

    const qty = instrumentCache.roundQty(
      symbol,
      notional / price
    );

    if (qty < inst.minQty) {
      logTrade(liqEvent, 'SKIPPED', 'Qty below minimum', 0);
      return null;
    }

    // Set one-way mode and leverage (only once per symbol)
    const leverageStart = Date.now();
    if (!leverageSet.has(symbol)) {
      await switchToOneWayMode(symbol).catch(() => {});
      const lev = Math.min(config.leverage, inst.maxLeverage);
      await setLeverage(symbol, lev).catch(() => {});
      leverageSet.add(symbol);
    }
    timing.leverage = Date.now() - leverageStart;

    // --- Calculate TP and SL prices (ATR-based with fallback) ---

    const atrStart = Date.now();
    let atrValue = null;
    let tpPrice, trailingStopDist;
    let tpMethod = 'fixed';

    // Try ATR-based TP
    try {
      atrValue = await getATR(symbol);
    } catch (err) {
      console.error(`[EXECUTOR] ATR fetch error for ${symbol}:`, err.message);
    }
    timing.atr = Date.now() - atrStart;

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

    // Stop-loss: shared risk budget divided by number of positions
    // Use total budget qty (not just DCA entry qty) so SL distance is reasonable.
    const positionCount = activePositions.size + 1; // +1 for this new position
    const maxLossUsd = getMaxLossPerPosition(positionCount);
    const totalExpectedQty = instrumentCache.roundQty(symbol, totalBudget / price);
    let slOffset = maxLossUsd / totalExpectedQty;
    if (inst.tickSize && slOffset < inst.tickSize) {
      slOffset = inst.tickSize;
      console.warn(`[EXECUTOR] ${symbol} SL offset below tick size, using 1 tick: ${inst.tickSize}`);
    }
    // Safety clamp: SL must never exceed 90% of entry price (avoids negative SL for buys / absurd SL for shorts)
    const maxSlOffset = price * 0.9;
    if (slOffset > maxSlOffset) {
      console.warn(`[EXECUTOR] ${symbol} SL offset ${slOffset.toFixed(6)} > 90% of price ${price}, clamping to ${maxSlOffset.toFixed(6)}`);
      slOffset = maxSlOffset;
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
      const obStart = Date.now();
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
      timing.orderbook = Date.now() - obStart;

      const orderStart = Date.now();
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
        timing.orderPlace = Date.now() - orderStart;

        if (orderResult.retCode !== 0) {
          console.error(`[EXECUTOR] Limit rejected for ${symbol}: ${orderResult.retMsg} (code ${orderResult.retCode})`);
          logTrade(liqEvent, 'SKIPPED', `Limit rejected: ${orderResult.retMsg}`, Date.now() - startTime);
          return null;
        }

        // Wait for fill — PostOnly may sit on the book
        const limitOrderId = orderResult.result.orderId;
        const fillWaitStart = Date.now();
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
        timing.orderFillWait = Date.now() - fillWaitStart;

        console.log(`[EXECUTOR] ${symbol} limit order filled @ ${limitPrice} (maker fee)`);
      }
    } else {
      // Market order (existing flow)
      const orderStart = Date.now();
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
      timing.orderPlace = Date.now() - orderStart;
    }

    const execTime = Date.now() - startTime;

    if (orderResult.retCode !== 0) {
      console.error(`[EXECUTOR] Order FAILED for ${symbol}: ${orderResult.retMsg} (code ${orderResult.retCode}) | ${execTime}ms`);
      logTrade(liqEvent, 'FAILED', orderResult.retMsg, execTime);
      return null;
    }

    const orderId = orderResult.result.orderId;

    // Fetch real fill price from Bybit position data
    const posFetchStart = Date.now();
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
    timing.positionFetch = Date.now() - posFetchStart;

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

    // activePrice = entry + trailingDist + feeBuffer
    // Fee buffer ensures that when trailing stop triggers at worst case (immediate reversal after activation),
    // the stop is at fillPrice + feeBuffer, giving enough gross profit to cover round-trip fees.
    // Estimated fees: maker entry (0.02%) + taker exit (0.055%) = 0.075%, use 0.1% for safety margin.
    let trailActivePrice = null;
    if (trailingStopDist) {
      const feeBuffer = instrumentCache.roundPrice(symbol, fillPrice * 0.0015);
      trailActivePrice = tradeSide === 'Buy'
        ? instrumentCache.roundPrice(symbol, fillPrice + trailingStopDist + feeBuffer)
        : instrumentCache.roundPrice(symbol, fillPrice - trailingStopDist - feeBuffer);
      console.log(`[EXECUTOR] ${symbol} trail activation includes fee buffer: ${feeBuffer} (0.15% of ${fillPrice})`);
    }

    // Set SL first, then trailing stop — using await for reliable sequential execution
    const tpSlStart = Date.now();
    try {
      const slRes = await setTradingStop(symbol, stopParams);
      if (slRes.retCode !== 0) {
        console.error(`[EXECUTOR] Failed to set SL${tpPrice ? '/TP' : ''} for ${symbol}: ${slRes.retMsg}`);
      } else {
        const tpType = tpPrice && config.tpOrderType === 'Limit' ? ' (limit)' : '';
        console.log(`[EXECUTOR] SL${tpPrice ? '/TP' : ''} set for ${symbol} | SL: ${slPrice2}${tpPrice ? ` | TP: ${tpPrice}${tpType}` : ''}`);
      }
      timing.tpSlSet = Date.now() - tpSlStart;

      // Set trailing stop with activePrice — activates once price moves by 1x trail distance
      if (trailingStopDist && trailActivePrice) {
        const trailStart = Date.now();
        const trailRes = await setTradingStop(symbol, { trailingStop: trailingStopDist, activePrice: trailActivePrice });
        if (trailRes.retCode !== 0) {
          console.error(`[EXECUTOR] Failed to set trailing stop for ${symbol}: ${trailRes.retMsg}`);
        } else {
          console.log(`[EXECUTOR] Trailing stop set for ${symbol} | Trail: ${trailingStopDist} | Activates @ ${trailActivePrice}`);
        }
        timing.trailSet = Date.now() - trailStart;
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
      dcaLevel: 0,
      totalBudget,
      lastEntryPrice: fillPrice,
      timing, // granular latency breakdown
    };
    activePositions.set(symbol, position);

    // Tighten SLs on all existing positions (shared risk budget)
    if (activePositions.size > 1) {
      tightenAllSLs(activePositions.size);
    }

    const tpDisplay = tpPrice || 'TRAIL';
    logTrade(liqEvent, 'FILLED', `Fill: ${fillPrice} | ${tpPrice ? `TP @ ${tpPrice}` : `Trail: ${trailingStopDist}`} | SL @ ${slPrice2} [${tpMethod}]`, execTime, position);

    console.log(
      `[TRADE] ${tradeSide} ${qty} ${symbol} @ ${fillPrice} (liq ${price}) | ${tpPrice ? `TP: ${tpPrice}` : `Trail: ${trailingStopDist}`} | SL: ${slPrice2} | ${tpMethod} | ${orderVia} | Exec: ${execTime}ms | Liq: $${usdValue.toFixed(0)}`
    );

    // Log granular latency breakdown
    const timingParts = [];
    if (timing.preChecks) timingParts.push(`pre:${timing.preChecks}ms`);
    if (timing.leverage) timingParts.push(`lev:${timing.leverage}ms`);
    if (timing.atr) timingParts.push(`atr:${timing.atr}ms`);
    if (timing.orderbook) timingParts.push(`ob:${timing.orderbook}ms`);
    if (timing.orderPlace) timingParts.push(`order:${timing.orderPlace}ms`);
    if (timing.orderFillWait) timingParts.push(`fillWait:${timing.orderFillWait}ms`);
    if (timing.positionFetch) timingParts.push(`posFetch:${timing.positionFetch}ms`);
    if (timing.tpSlSet) timingParts.push(`tpSl:${timing.tpSlSet}ms`);
    if (timing.trailSet) timingParts.push(`trail:${timing.trailSet}ms`);
    console.log(`[LATENCY] ${symbol} | ${timingParts.join(' | ')} | TOTAL: ${execTime}ms`);

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

async function executeDCA(liqEvent, existingPos) {
  const { symbol, price, usdValue } = liqEvent;
  const startTime = Date.now();
  const nextLevel = (existingPos.dcaLevel || 0) + 1;

  // Lock symbol
  pendingSymbols.add(symbol);

  try {
    const inst = instrumentCache.get(symbol);
    if (!inst) {
      logTrade(liqEvent, 'SKIPPED', 'DCA: Unknown instrument', 0);
      return null;
    }

    if (existingPos.totalBudget <= 0) {
      logTrade(liqEvent, 'SKIPPED', 'DCA: No budget info (pre-existing position)', 0);
      return null;
    }

    // Calculate qty for this DCA level
    const notional = existingPos.totalBudget * DCA_SPLITS[nextLevel];
    const qty = instrumentCache.roundQty(symbol, notional / price);

    if (qty < inst.minQty) {
      logTrade(liqEvent, 'SKIPPED', 'DCA: Qty below minimum', 0);
      return null;
    }

    const tradeSide = existingPos.side;

    // Place order (same side as existing position)
    let orderResult;
    let orderVia;
    const entryType = config.entryOrderType || 'Market';

    if (entryType === 'Limit') {
      let limitPrice = null;
      try {
        const ob = await getOrderbook(symbol, 1);
        if (ob.retCode === 0 && ob.result) {
          limitPrice = tradeSide === 'Buy'
            ? parseFloat(ob.result.b[0][0])
            : parseFloat(ob.result.a[0][0]);
          limitPrice = instrumentCache.roundPrice(symbol, limitPrice);
        }
      } catch (err) {
        console.warn(`[EXECUTOR] DCA orderbook error for ${symbol}:`, err.message);
      }

      if (!limitPrice) {
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
          logTrade(liqEvent, 'SKIPPED', `DCA limit rejected: ${orderResult.retMsg}`, Date.now() - startTime);
          return null;
        }

        // Wait for fill
        const limitOrderId = orderResult.result.orderId;
        await new Promise(r => setTimeout(r, 2000));

        try {
          const orderCheck = await getOrderDetail(symbol, limitOrderId);
          if (orderCheck.retCode === 0 && orderCheck.result?.list?.length) {
            const status = orderCheck.result.list[0].orderStatus;
            if (status !== 'Filled') {
              await cancelOrder(symbol, limitOrderId).catch(() => {});
              logTrade(liqEvent, 'SKIPPED', `DCA limit not filled (${status})`, Date.now() - startTime);
              return null;
            }
          }
        } catch (err) {
          console.warn(`[EXECUTOR] DCA order status check failed for ${symbol}:`, err.message);
        }
      }
    } else {
      // Market order
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
      console.error(`[EXECUTOR] DCA order FAILED for ${symbol}: ${orderResult.retMsg} (code ${orderResult.retCode}) | ${execTime}ms`);
      logTrade(liqEvent, 'FAILED', `DCA: ${orderResult.retMsg}`, execTime);
      return null;
    }

    // Fetch new avgPrice and size from Bybit position data
    let newAvgPrice = price;
    let newTotalQty = existingPos.qty + qty;
    try {
      const posRes = await getPositions();
      if (posRes.retCode === 0) {
        const pos = posRes.result.list.find(p => p.symbol === symbol && parseFloat(p.size) > 0);
        if (pos) {
          newAvgPrice = parseFloat(pos.avgPrice);
          newTotalQty = parseFloat(pos.size);
          console.log(`[EXECUTOR] DCA ${nextLevel + 1}/${DCA_SPLITS.length} ${symbol} | New avg: ${newAvgPrice} | Size: ${newTotalQty}`);
        }
      }
    } catch (err) {
      console.warn(`[EXECUTOR] DCA could not fetch position for ${symbol}, using estimates`);
    }

    // Recalculate SL from new avgPrice (shared risk budget)
    // Use totalExpectedQty (full budget qty) for consistent SL distance
    const maxLossUsd = getMaxLossPerPosition(activePositions.size);
    const totalExpectedQty = instrumentCache.roundQty(symbol, existingPos.totalBudget / newAvgPrice);
    let slOffset = maxLossUsd / totalExpectedQty;
    if (inst.tickSize && slOffset < inst.tickSize) {
      slOffset = inst.tickSize;
    }
    // Safety clamp: SL must never exceed 90% of avg price
    const maxSlOffset = newAvgPrice * 0.9;
    if (slOffset > maxSlOffset) {
      console.warn(`[EXECUTOR] DCA ${symbol} SL offset ${slOffset.toFixed(6)} > 90% of avgPrice ${newAvgPrice}, clamping`);
      slOffset = maxSlOffset;
    }
    const newSL = tradeSide === 'Buy'
      ? instrumentCache.roundPrice(symbol, newAvgPrice - slOffset)
      : instrumentCache.roundPrice(symbol, newAvgPrice + slOffset);

    // Recalculate trailing stop activation from new avgPrice
    const trailDist = existingPos.trailingStop;
    let newTrailActive = null;
    if (trailDist) {
      const feeBuffer = instrumentCache.roundPrice(symbol, newAvgPrice * 0.0015);
      newTrailActive = tradeSide === 'Buy'
        ? instrumentCache.roundPrice(symbol, newAvgPrice + trailDist + feeBuffer)
        : instrumentCache.roundPrice(symbol, newAvgPrice - trailDist - feeBuffer);
    }

    // Set SL, then trailing stop with new activePrice
    try {
      const slRes = await setTradingStop(symbol, { stopLoss: newSL });
      if (slRes.retCode !== 0) {
        console.error(`[EXECUTOR] DCA SL set failed for ${symbol}: ${slRes.retMsg}`);
      } else {
        console.log(`[EXECUTOR] DCA SL updated for ${symbol} @ ${newSL}`);
      }

      if (trailDist && newTrailActive) {
        const trailRes = await setTradingStop(symbol, { trailingStop: trailDist, activePrice: newTrailActive });
        if (trailRes.retCode !== 0) {
          console.error(`[EXECUTOR] DCA trailing stop failed for ${symbol}: ${trailRes.retMsg}`);
        } else {
          console.log(`[EXECUTOR] DCA trailing stop updated for ${symbol} | Trail: ${trailDist} | Activates @ ${newTrailActive}`);
        }
      }
    } catch (err) {
      console.error(`[EXECUTOR] DCA SL/trailing error for ${symbol}:`, err.message);
    }

    // Update position in activePositions
    existingPos.entryPrice = newAvgPrice;
    existingPos.qty = newTotalQty;
    existingPos.slPrice = newSL;
    existingPos.trailActivePrice = newTrailActive || existingPos.trailActivePrice;
    existingPos.dcaLevel = nextLevel;
    existingPos.lastEntryPrice = price;

    const dcaLabel = `DCA ${nextLevel + 1}/${DCA_SPLITS.length}`;
    logTrade(liqEvent, 'FILLED', `${dcaLabel} | Avg: ${newAvgPrice} | SL: ${newSL} | Qty: ${newTotalQty}`, execTime, existingPos);

    console.log(
      `[TRADE] ${dcaLabel} ${tradeSide} +${qty} ${symbol} @ ${price} | Avg: ${newAvgPrice} | SL: ${newSL} | Total: ${newTotalQty} | ${orderVia} | ${execTime}ms`
    );

    return existingPos;
  } catch (err) {
    const execTime = Date.now() - startTime;
    logTrade(liqEvent, 'ERROR', `DCA: ${err.message}`, execTime);
    console.error(`[EXECUTOR] DCA error for ${symbol}:`, err.message);
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
      dcaLevel: position.dcaLevel,
    } : null,
  };

  tradeLog.unshift(entry);
  if (tradeLog.length > MAX_LOG_SIZE) tradeLog.pop();
}
