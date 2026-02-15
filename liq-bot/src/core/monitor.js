import { config } from '../config.js';
import { getPositions, closePosition, setTradingStop, getClosedPnl, getOrderbook, cancelOrder, getExecutionList } from '../api/bybit.js';
import { instrumentCache } from './instruments.js';
import { getATR } from './atr.js';

// Track recently closed symbols to prevent duplicate close records
const recentlyClosedSymbols = new Map(); // symbol -> timestamp

/**
 * Fetch close data from Bybit APIs and calculate PnL from tracked entry + matched exit.
 * Instead of trusting Bybit's closedPnl value (which may be from a mismatched record),
 * we calculate grossPnl ourselves: (exit - entry) × qty, then subtract fees.
 *
 * Matching filters: only records created after position was opened + price/qty match.
 * Retries up to 5 times with 2s delay if no matching record found (API settle time).
 */
async function fetchBybitCloseData(symbol, entryOrderId, trackedEntryPrice = 0, trackedQty = 0, trackedSide = null, openTime = 0) {
  const data = {
    pnl: 0,
    grossPnl: 0,
    fees: { open: 0, close: 0, total: 0 },
    entryIsMaker: false,
    exitIsMaker: false,
    avgEntryPrice: 0,
    avgExitPrice: 0,
    closeOrderId: null,
  };

  // 1. Closed PnL — multi-tier matching strategy with extended retries
  let closeOrderId = null;
  const maxRetries = 6;
  const retryDelayMs = 3000;

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    if (attempt > 0) {
      await new Promise(r => setTimeout(r, retryDelayMs));
      console.log(`[MONITOR] ${symbol} retry ${attempt + 1}/${maxRetries} for closed PnL match...`);
    }

    try {
      const pnlRes = await getClosedPnl(symbol, 50);
      if (pnlRes.retCode !== 0 || !pnlRes.result?.list?.length) continue;

      // On early attempts: filter by openTime. On last 2 attempts: drop the time filter
      const relaxTimeFilter = attempt >= maxRetries - 2;

      const candidates = pnlRes.result.list.filter(rec => {
        if (usedCloseOrderIds.has(rec.orderId)) return false;
        if (!relaxTimeFilter && openTime > 0 && parseInt(rec.createdTime || '0') < openTime) return false;
        return true;
      });

      if (candidates.length === 0) continue;

      let bestMatch = null;
      let matchTier = 0;

      // Tier 1: Strict match (price 0.5% + qty 1%)
      for (const rec of candidates) {
        const recEntry = parseFloat(rec.avgEntryPrice || '0');
        const recQty = parseFloat(rec.qty || '0');
        if (trackedEntryPrice > 0 && recEntry > 0) {
          const priceDiff = Math.abs(recEntry - trackedEntryPrice) / trackedEntryPrice;
          const qtyDiff = trackedQty > 0 && recQty > 0 ? Math.abs(recQty - trackedQty) / trackedQty : 0;
          if (priceDiff < 0.005 && qtyDiff < 0.01) {
            bestMatch = rec;
            matchTier = 1;
            break;
          }
        }
      }

      // Tier 2: Relaxed match (price 5% + qty 20%) — wider for DCA
      if (!bestMatch) {
        for (const rec of candidates) {
          const recEntry = parseFloat(rec.avgEntryPrice || '0');
          const recQty = parseFloat(rec.qty || '0');
          if (trackedEntryPrice > 0 && recEntry > 0) {
            const priceDiff = Math.abs(recEntry - trackedEntryPrice) / trackedEntryPrice;
            const qtyDiff = trackedQty > 0 && recQty > 0 ? Math.abs(recQty - trackedQty) / trackedQty : 1;
            if (priceDiff < 0.05 && qtyDiff < 0.2) {
              bestMatch = rec;
              matchTier = 2;
              break;
            }
          }
        }
      }

      // Tier 3: Side match — any candidate with matching side direction
      if (!bestMatch && trackedSide) {
        for (const rec of candidates) {
          if (rec.side === trackedSide) {
            bestMatch = rec;
            matchTier = 3;
            break;
          }
        }
      }

      // Tier 4: Any candidate — take most recent (Bybit returns newest first)
      if (!bestMatch) {
        bestMatch = candidates[0];
        matchTier = 4;
      }

      if (bestMatch) {
        data.avgEntryPrice = parseFloat(bestMatch.avgEntryPrice || '0');
        data.avgExitPrice = parseFloat(bestMatch.avgExitPrice || '0');
        data.bybitClosedPnl = parseFloat(bestMatch.closedPnl || '0');
        closeOrderId = bestMatch.orderId;
        data.closeOrderId = closeOrderId;
        usedCloseOrderIds.add(closeOrderId);

        console.log(`[MONITOR] ${symbol} matched (tier ${matchTier}${attempt >= maxRetries - 2 ? ', relaxed time' : ''}) | Entry: ${data.avgEntryPrice} | Exit: ${data.avgExitPrice} | BybitPnL: ${data.bybitClosedPnl} | CloseOrderId: ${closeOrderId}`);
        break;
      }
    } catch (err) {
      console.warn(`[MONITOR] Could not fetch closed PnL for ${symbol}:`, err.message);
    }
  }

  if (!closeOrderId) {
    console.warn(`[MONITOR] ${symbol} no matching closed PnL record found after ${maxRetries} retries`);
  }

  // 2. Entry executions — exact fee + isMaker from Bybit execution records
  if (entryOrderId) {
    try {
      const entryExecs = await getExecutionList(symbol, entryOrderId);
      if (entryExecs.retCode === 0 && entryExecs.result?.list?.length) {
        for (const exec of entryExecs.result.list) {
          data.fees.open += parseFloat(exec.execFee || '0');
        }
        const first = entryExecs.result.list[0];
        data.entryIsMaker = first.isMaker === true || first.isMaker === 'true';
      }
    } catch (err) {
      console.warn(`[MONITOR] Could not fetch entry executions for ${symbol}:`, err.message);
    }
  }

  // 3. Close executions — exact fee + isMaker from Bybit execution records
  if (closeOrderId) {
    try {
      const closeExecs = await getExecutionList(symbol, closeOrderId);
      if (closeExecs.retCode === 0 && closeExecs.result?.list?.length) {
        for (const exec of closeExecs.result.list) {
          data.fees.close += parseFloat(exec.execFee || '0');
        }
        const first = closeExecs.result.list[0];
        data.exitIsMaker = first.isMaker === true || first.isMaker === 'true';
      }
    } catch (err) {
      console.warn(`[MONITOR] Could not fetch close executions for ${symbol}:`, err.message);
    }
  }

  data.fees.total = data.fees.open + data.fees.close;

  // PnL: ALWAYS use Bybit's closedPnl (source of truth), calculate our own only as fallback
  const entryPrice = trackedEntryPrice || data.avgEntryPrice;
  const exitPrice = data.avgExitPrice;

  if (data.bybitClosedPnl != null && data.bybitClosedPnl !== 0) {
    // Bybit's closedPnl is authoritative (already net of fees for that specific position)
    data.pnl = data.bybitClosedPnl;
    data.grossPnl = data.pnl + data.fees.total;
    // Log if our calculation would have differed significantly
    if (entryPrice > 0 && exitPrice > 0 && trackedQty > 0 && trackedSide) {
      const calcGross = trackedSide === 'Buy'
        ? (exitPrice - entryPrice) * trackedQty
        : (entryPrice - exitPrice) * trackedQty;
      const calcPnl = calcGross - data.fees.total;
      if (Math.abs(calcPnl - data.pnl) > 0.5) {
        console.warn(`[MONITOR] ${symbol} PnL mismatch: Bybit=${data.pnl.toFixed(4)} vs calc=${calcPnl.toFixed(4)} (using Bybit)`);
      }
    }
  } else if (entryPrice > 0 && exitPrice > 0 && trackedQty > 0 && trackedSide) {
    // Fallback: calculate from prices (only when Bybit closedPnl unavailable)
    data.grossPnl = trackedSide === 'Buy'
      ? (exitPrice - entryPrice) * trackedQty
      : (entryPrice - exitPrice) * trackedQty;
    data.pnl = data.grossPnl - data.fees.total;
    console.log(`[MONITOR] ${symbol} using calculated PnL (no Bybit closedPnl): ${data.pnl.toFixed(4)}`);
  }

  console.log(`[MONITOR] ${symbol} final | PnL: ${data.pnl.toFixed(4)} (Bybit: ${data.bybitClosedPnl ?? 'n/a'}) | Fees: ${data.fees.total.toFixed(6)} | Entry: ${entryPrice} | Exit: ${exitPrice}`);

  return data;
}

// Track close order IDs we've already recorded to prevent duplicate matching
const usedCloseOrderIds = new Set();

import { getActivePositions, getTradeLog, getPendingSymbols, getInitialBalance } from './executor.js';

/**
 * Calculate and set SL + Trailing Stop for a naked position.
 * Uses ATR-based SL (proportional to trailing stop) with risk-budget fallback.
 * Returns { slPrice, trailingStop, trailActivePrice } or null on failure.
 */
async function ensureProtectionOnPosition(symbol, side, entryPrice, qty) {
  const inst = instrumentCache.get(symbol);
  if (!inst) {
    console.warn(`[MONITOR] Cannot set protection for ${symbol} — unknown instrument`);
    return null;
  }

  // Fetch ATR for both SL and trailing stop
  let atrValue = null;
  try {
    atrValue = await getATR(symbol);
  } catch (err) {
    console.warn(`[MONITOR] ATR fetch failed for ${symbol}:`, err.message);
  }

  // Calculate SL: ATR-based (proportional to trailing), risk-budget fallback
  let slOffset;
  if (atrValue && atrValue > 0) {
    slOffset = atrValue * config.slAtrMultiplier;
  } else {
    // Fallback: risk-budget SL
    const balance = getInitialBalance();
    if (balance <= 0) {
      console.warn(`[MONITOR] Cannot set protection for ${symbol} — no balance and no ATR`);
      return null;
    }
    const activePositions = getActivePositions();
    const positionCount = Math.max(1, activePositions.size);
    const maxLossUsd = (balance * (config.totalRiskPct / 100)) / positionCount;
    slOffset = maxLossUsd / qty;
  }

  if (inst.tickSize && slOffset < inst.tickSize) {
    slOffset = inst.tickSize;
  }
  const maxSlOffset = entryPrice * 0.9;
  if (slOffset > maxSlOffset) {
    console.warn(`[MONITOR] ${symbol} SL offset ${slOffset.toFixed(6)} > 90% of entry, clamping to ${maxSlOffset.toFixed(6)}`);
    slOffset = maxSlOffset;
  }

  const slPrice = side === 'Buy'
    ? instrumentCache.roundPrice(symbol, entryPrice - slOffset)
    : instrumentCache.roundPrice(symbol, entryPrice + slOffset);

  // Calculate trailing stop based on ATR
  let trailingStop = null;
  let trailActivePrice = null;

  if (atrValue && atrValue > 0) {
    trailingStop = instrumentCache.roundPrice(symbol, atrValue * config.trailingAtrMultiplier);

    if (!trailingStop && inst.tickSize) {
      trailingStop = inst.tickSize;
    }

    if (trailingStop) {
      const feeBuffer = instrumentCache.roundPrice(symbol, entryPrice * 0.0015);
      trailActivePrice = side === 'Buy'
        ? instrumentCache.roundPrice(symbol, entryPrice + trailingStop + feeBuffer)
        : instrumentCache.roundPrice(symbol, entryPrice - trailingStop - feeBuffer);
    }
  }

  console.log(`[MONITOR] Setting protection on ${symbol} | Side: ${side} | Entry: ${entryPrice} | SL: ${slPrice} (${atrValue ? config.slAtrMultiplier + 'x ATR' : 'budget'}) | Trail: ${trailingStop || '—'}`);

  const result = { slPrice: null, trailingStop: null, trailActivePrice: null };

  // Set SL first
  try {
    const slRes = await setTradingStop(symbol, { stopLoss: slPrice });
    if (slRes.retCode !== 0) {
      console.error(`[MONITOR] Failed to set SL on ${symbol}: ${slRes.retMsg}`);
    } else {
      console.log(`[MONITOR] SL successfully set on ${symbol} @ ${slPrice}`);
      result.slPrice = slPrice;
    }
  } catch (err) {
    console.error(`[MONITOR] Error setting SL on ${symbol}:`, err.message);
  }

  // Set trailing stop if we have ATR data
  if (trailingStop && trailActivePrice) {
    try {
      const trailRes = await setTradingStop(symbol, { trailingStop, activePrice: trailActivePrice });
      if (trailRes.retCode !== 0) {
        console.error(`[MONITOR] Failed to set trailing stop on ${symbol}: ${trailRes.retMsg}`);
      } else {
        console.log(`[MONITOR] Trailing stop set on ${symbol} | Trail: ${trailingStop} | Activates @ ${trailActivePrice}`);
        result.trailingStop = trailingStop;
        result.trailActivePrice = trailActivePrice;
      }
    } catch (err) {
      console.error(`[MONITOR] Error setting trailing stop on ${symbol}:`, err.message);
    }
  }

  return result.slPrice ? result : null;
}

/**
 * Position Monitor
 *
 * Periodically syncs active positions with Bybit to:
 * - Detect when TP/SL/trailing has been hit (position closed)
 * - Track realized PnL (using real Bybit data when available)
 * - Force-close positions held longer than MAX_HOLD_SECONDS
 * - Clean up stale positions
 */

let pnlHistory = []; // { symbol, pnl, closedAt, ... }
let totalPnl = 0;
let resetTimestamp = 0; // When PnL was last reset — reconcilePnl ignores trades before this

export function hydratePnl(savedHistory, savedTotal, savedResetTimestamp) {
  if (savedHistory && savedHistory.length) {
    pnlHistory = savedHistory;
    // Seed usedCloseOrderIds so we don't re-match old records
    for (const rec of savedHistory) {
      if (rec.closeOrderId) usedCloseOrderIds.add(rec.closeOrderId);
    }
    console.log(`[MONITOR] Restored ${savedHistory.length} PnL records from disk (${usedCloseOrderIds.size} close order IDs).`);
  }
  if (typeof savedTotal === 'number') {
    totalPnl = savedTotal;
    console.log(`[MONITOR] Restored total PnL: ${totalPnl.toFixed(4)}`);
  }
  if (typeof savedResetTimestamp === 'number' && savedResetTimestamp > 0) {
    resetTimestamp = savedResetTimestamp;
    console.log(`[MONITOR] Restored reset timestamp: ${resetTimestamp} — ignoring trades before this.`);
  }
}


export function getPnlHistory() {
  return pnlHistory;
}

export function getTotalPnl() {
  return pnlHistory.reduce((s, p) => s + p.pnl, 0);
}

export function resetPnl() {
  pnlHistory = [];
  totalPnl = 0;
  usedCloseOrderIds.clear();
  resetTimestamp = Date.now();
  console.log(`[MONITOR] PnL data reset to zero. Ignoring trades before ${resetTimestamp}.`);
}

export function getResetTimestamp() {
  return resetTimestamp;
}

export function resetMonitorState() {
  pnlHistory = [];
  totalPnl = 0;
  recentlyClosedSymbols.clear();
  usedCloseOrderIds.clear();
  console.log('[MONITOR] State reset for account switch.');
}

export function getStats() {
  const tradeLog = getTradeLog();
  const filled = tradeLog.filter(t => t.status === 'FILLED');

  // Exclude unresolved records (pnl=0 with no exit price) from win/loss stats
  const resolved = pnlHistory.filter(p => p.pnl !== 0 || p.exitPrice);
  const unresolved = pnlHistory.length - resolved.length;
  const wins = resolved.filter(p => p.pnl > 0);
  const losses = resolved.filter(p => p.pnl < 0);
  const avgExecTime = filled.length > 0
    ? filled.reduce((s, t) => s + t.execTimeMs, 0) / filled.length
    : 0;

  const totalFees = pnlHistory.reduce((s, p) => s + (p.fees?.total || 0), 0);
  const totalGrossPnl = pnlHistory.reduce((s, p) => s + (p.grossPnl || (p.pnl + (p.fees?.total || 0))), 0);

  return {
    totalTrades: filled.length,
    openPositions: getActivePositions().size,
    closedTrades: pnlHistory.length,
    wins: wins.length,
    losses: losses.length,
    unresolved,
    winRate: resolved.length > 0 ? (wins.length / resolved.length * 100).toFixed(1) : '0',
    totalPnl: pnlHistory.reduce((s, p) => s + p.pnl, 0).toFixed(4),
    totalGrossPnl: totalGrossPnl.toFixed(4),
    totalFees: totalFees.toFixed(4),
    avgExecTimeMs: avgExecTime.toFixed(0),
  };
}

export async function syncPositions() {
  const activePositions = getActivePositions();

  try {
    const res = await getPositions();
    if (res.retCode !== 0) return;

    const bybitPositions = new Map();
    for (const p of res.result.list) {
      if (parseFloat(p.size) > 0) {
        bybitPositions.set(p.symbol, p);
      }
    }

    // Detect untracked Bybit positions and start tracking them
    const pendingSymbols = getPendingSymbols();
    for (const [symbol, p] of bybitPositions) {
      if (activePositions.has(symbol)) continue;
      if (pendingSymbols.has(symbol)) continue; // executor is currently processing this symbol

      const entryPrice = parseFloat(p.avgPrice);
      const size = parseFloat(p.size);
      const trailDist = parseFloat(p.trailingStop || '0') || null;
      let trailActive = null;
      if (trailDist) {
        const feeBuffer = instrumentCache.roundPrice(symbol, entryPrice * 0.0015);
        trailActive = p.side === 'Buy'
          ? instrumentCache.roundPrice(symbol, entryPrice + trailDist + feeBuffer)
          : instrumentCache.roundPrice(symbol, entryPrice - trailDist - feeBuffer);
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
        tpMethod: 'detected',
        unrealisedPnl: parseFloat(p.unrealisedPnl || '0'),
        markPrice: parseFloat(p.markPrice || '0'),
        dcaLevel: 0,
        totalBudget: 0,
        lastEntryPrice: entryPrice,
      };
      activePositions.set(symbol, position);

      console.log(
        `[MONITOR] Detected untracked position: ${p.side} ${size} ${symbol} @ ${entryPrice} | TP: ${position.tpPrice || '—'} | SL: ${position.slPrice || '—'}`
      );

      // CRITICAL: If detected position has no SL or trailing stop, set them immediately
      const needsSL = !position.slPrice;
      const needsTrail = !position.trailingStop;
      if (needsSL || needsTrail) {
        console.warn(`[MONITOR] NAKED POSITION DETECTED: ${symbol} missing ${needsSL ? 'SL' : ''}${needsSL && needsTrail ? ' + ' : ''}${needsTrail ? 'trailing stop' : ''} — setting now`);
        const protection = await ensureProtectionOnPosition(symbol, p.side, entryPrice, size);
        if (protection) {
          if (protection.slPrice) position.slPrice = protection.slPrice;
          if (protection.trailingStop) position.trailingStop = protection.trailingStop;
          if (protection.trailActivePrice) position.trailActivePrice = protection.trailActivePrice;
        }
      }
    }

    // Check each tracked position — collect closures, update open positions
    const closedEntries = [];

    for (const [symbol, tracked] of activePositions) {
      const bybitPos = bybitPositions.get(symbol);

      if (!bybitPos) {
        // Grace period: skip if position was opened less than 15s ago
        const ageMs = Date.now() - (tracked.openTime || 0);
        if (ageMs < 15000) continue;

        // Guard: prevent duplicate close records
        if (recentlyClosedSymbols.has(symbol) && Date.now() - recentlyClosedSymbols.get(symbol) < 10000) {
          activePositions.delete(symbol);
          continue;
        }
        recentlyClosedSymbols.set(symbol, Date.now());
        closedEntries.push({ symbol, tracked });
        continue;
      }

      // Update unrealised PnL from Bybit for dashboard display
      tracked.unrealisedPnl = parseFloat(bybitPos.unrealisedPnl || '0');
      tracked.markPrice = parseFloat(bybitPos.markPrice || '0');

      // Health check — re-apply SL and/or trailing stop if missing from Bybit
      const bybitSL = parseFloat(bybitPos.stopLoss || '0');
      const bybitTrail = parseFloat(bybitPos.trailingStop || '0');
      const slMissing = bybitSL === 0 && tracked.slPrice;
      const trailMissing = bybitTrail === 0 && tracked.trailingStop;

      // CRITICAL: Naked position check — no SL/trail on Bybit AND no tracked SL/trail
      const nakedSL = bybitSL === 0 && !tracked.slPrice;
      const nakedTrail = bybitTrail === 0 && !tracked.trailingStop;
      if (nakedSL || nakedTrail) {
        console.warn(`[MONITOR] NAKED POSITION: ${symbol} missing ${nakedSL ? 'SL' : ''}${nakedSL && nakedTrail ? ' + ' : ''}${nakedTrail ? 'trailing stop' : ''} — setting now`);
        const protection = await ensureProtectionOnPosition(symbol, tracked.side, tracked.entryPrice, tracked.qty);
        if (protection) {
          if (protection.slPrice) tracked.slPrice = protection.slPrice;
          if (protection.trailingStop) tracked.trailingStop = protection.trailingStop;
          if (protection.trailActivePrice) tracked.trailActivePrice = protection.trailActivePrice;
        }
      }

      if (slMissing || trailMissing) {
        if (slMissing) {
          console.warn(`[MONITOR] SL missing on ${symbol} — will restore @ ${tracked.slPrice}`);
        }
        if (trailMissing) {
          console.warn(`[MONITOR] Trailing stop missing on ${symbol} — will restore trail: ${tracked.trailingStop}`);
        }

        // First restore SL (critical), then trailing in a separate call
        if (slMissing) {
          setTradingStop(symbol, { stopLoss: tracked.slPrice }).then((slRes) => {
            if (slRes.retCode !== 0) {
              console.error(`[MONITOR] SL restore failed for ${symbol}: ${slRes.retMsg}`);
            } else {
              console.log(`[MONITOR] SL restored for ${symbol} @ ${tracked.slPrice}`);
            }

            if (trailMissing) {
              const trailParams = { trailingStop: tracked.trailingStop };
              if (tracked.trailActivePrice) trailParams.activePrice = tracked.trailActivePrice;
              setTradingStop(symbol, trailParams).then((trailRes) => {
                if (trailRes.retCode !== 0) {
                  console.error(`[MONITOR] Trailing stop restore failed for ${symbol}: ${trailRes.retMsg}`);
                } else {
                  console.log(`[MONITOR] Trailing stop restored for ${symbol} | Trail: ${tracked.trailingStop}`);
                }
              }).catch(err => console.error(`[MONITOR] Trailing stop restore error for ${symbol}:`, err.message));
            }
          }).catch(err => console.error(`[MONITOR] SL restore error for ${symbol}:`, err.message));
        } else if (trailMissing) {
          const trailParams = { trailingStop: tracked.trailingStop };
          if (tracked.trailActivePrice) trailParams.activePrice = tracked.trailActivePrice;
          setTradingStop(symbol, trailParams).then((trailRes) => {
            if (trailRes.retCode !== 0) {
              console.error(`[MONITOR] Trailing stop restore failed for ${symbol}: ${trailRes.retMsg}`);
            } else {
              console.log(`[MONITOR] Trailing stop restored for ${symbol} | Trail: ${tracked.trailingStop}`);
            }
          }).catch(err => console.error(`[MONITOR] Trailing stop restore error for ${symbol}:`, err.message));
        }
      }
    }

    // Process closures in parallel — don't block sync loop sequentially
    if (closedEntries.length > 0) {
      await new Promise(r => setTimeout(r, 2000)); // Single shared wait for Bybit to settle
      await Promise.all(closedEntries.map(async ({ symbol, tracked }) => {
        const closeData = await fetchBybitCloseData(symbol, tracked.orderId, tracked.entryPrice, tracked.qty, tracked.side, tracked.openTime);
        recordClose(symbol, tracked, closeData, 'TP/SL/TRAIL');
      }));
    }
  } catch (err) {
    console.error('[MONITOR] Sync error:', err.message);
  }
}

function recordClose(symbol, tracked, closeData, exitType) {
  const activePositions = getActivePositions();

  pnlHistory.unshift({
    symbol,
    orderId: tracked.orderId || null,
    closeOrderId: closeData.closeOrderId || null,
    side: tracked.side,
    entryPrice: tracked.entryPrice,
    exitPrice: closeData.avgExitPrice || 0,
    tpPrice: tracked.tpPrice,
    slPrice: tracked.slPrice,
    qty: tracked.qty,
    grossPnl: closeData.grossPnl,
    pnl: closeData.pnl,
    fees: closeData.fees,
    exitType,
    atr: tracked.atr,
    trailingStop: tracked.trailingStop,
    tpMethod: tracked.tpMethod,
    openTime: tracked.openTime,
    closedAt: Date.now(),
    holdTimeMs: Date.now() - tracked.openTime,
    liqUsdValue: tracked.liqUsdValue || 0,
    execTimeMs: tracked.execTimeMs || 0,
    // From Bybit execution list isMaker field
    entryIsMaker: closeData.entryIsMaker,
    exitIsMaker: closeData.exitIsMaker,
  });

  totalPnl += closeData.pnl;
  activePositions.delete(symbol);

  console.log(
    `[MONITOR] ${symbol} closed (${exitType}) | PnL: ${closeData.pnl.toFixed(4)} USDT | Fees: ${closeData.fees.total.toFixed(6)} (entry: ${closeData.entryIsMaker ? 'maker' : 'taker'}, exit: ${closeData.exitIsMaker ? 'maker' : 'taker'}) | Total: ${totalPnl.toFixed(4)}`
  );
}

/**
 * Reconcile pnlHistory with Bybit's closed PnL endpoint.
 * Backfills any trades that exist on Bybit but are missing from our records
 * (e.g. positions that closed during a deploy/restart).
 */
export async function reconcilePnl() {
  try {
    const pnlRes = await getClosedPnl(null, 200);
    if (pnlRes.retCode !== 0 || !pnlRes.result?.list?.length) {
      console.log('[RECONCILE] No closed PnL records from Bybit or API error.');
      return;
    }

    const bybitRecords = pnlRes.result.list;

    // Build time-based fallback lookup for records without closeOrderId (120s buckets ± 1)
    // IMPORTANT: Skip pnl:0 broken records — they need to be reconciled, not used as dedup keys
    const knownByTimeSymbol = new Set();
    for (const rec of pnlHistory) {
      if (rec.pnl === 0 && !rec.exitPrice) continue;
      if (rec.closedAt && rec.symbol) {
        const bucket = Math.floor(rec.closedAt / 120000); // 120s buckets
        knownByTimeSymbol.add(`${rec.symbol}_${bucket}`);
        knownByTimeSymbol.add(`${rec.symbol}_${bucket - 1}`);
        knownByTimeSymbol.add(`${rec.symbol}_${bucket + 1}`);
      }
    }

    let backfilled = 0;
    for (const rec of bybitRecords) {
      const closeOrderId = rec.orderId;
      const symbol = rec.symbol;
      const createdTime = parseInt(rec.createdTime || '0');
      const bucket = Math.floor(createdTime / 120000);

      // Skip trades that closed before the last PnL reset
      if (resetTimestamp > 0 && createdTime < resetTimestamp) continue;

      // Skip if we already have this trade (by closeOrderId or time proximity)
      if (usedCloseOrderIds.has(closeOrderId)) continue;
      if (knownByTimeSymbol.has(`${symbol}_${bucket}`)) continue;

      // Missing trade — backfill from Bybit data, calculate PnL ourselves
      const avgEntryPrice = parseFloat(rec.avgEntryPrice || '0');
      const avgExitPrice = parseFloat(rec.avgExitPrice || '0');
      const qty = parseFloat(rec.qty || '0');
      const side = rec.side; // position side

      // Fetch execution details for fees + maker/taker
      let fees = { open: 0, close: 0, total: 0 };
      let entryIsMaker = false;
      let exitIsMaker = false;

      // Fetch close executions
      try {
        const closeExecs = await getExecutionList(symbol, closeOrderId);
        if (closeExecs.retCode === 0 && closeExecs.result?.list?.length) {
          for (const exec of closeExecs.result.list) {
            fees.close += parseFloat(exec.execFee || '0');
          }
          exitIsMaker = closeExecs.result.list[0].isMaker === true || closeExecs.result.list[0].isMaker === 'true';
        }
      } catch {}

      fees.total = fees.open + fees.close;

      // Use Bybit's closedPnl directly (source of truth, already net of fees)
      const bybitPnl = parseFloat(rec.closedPnl || '0');
      const netPnl = bybitPnl;
      const grossPnl = netPnl + fees.total;

      // Check if there's an existing record with pnl: 0 for the same trade
      // (happens when fetchBybitCloseData couldn't find a match)
      // Use !p.exitPrice to catch both 0 and undefined/missing field
      // Wider tolerances (5% price, 30% qty) for DCA trades where avg price shifts
      const existingIdx = pnlHistory.findIndex(p =>
        p.symbol === symbol &&
        p.pnl === 0 &&
        !p.exitPrice &&
        ((avgEntryPrice > 0 && p.entryPrice > 0 && Math.abs(p.entryPrice - avgEntryPrice) / avgEntryPrice < 0.05) ||
         (qty > 0 && p.qty > 0 && Math.abs(p.qty - qty) / qty < 0.3))
      );

      if (existingIdx >= 0) {
        // Update the existing broken record instead of creating a duplicate
        const existing = pnlHistory[existingIdx];
        existing.exitPrice = avgExitPrice;
        existing.grossPnl = grossPnl;
        existing.pnl = netPnl;
        existing.closeOrderId = closeOrderId;
        existing.fees = { open: existing.fees?.open || 0, close: fees.close, total: (existing.fees?.open || 0) + fees.close };
        existing.exitIsMaker = exitIsMaker;
        console.log(`[RECONCILE] Updated existing pnl:0 record for ${symbol} | Gross: ${grossPnl.toFixed(4)} | Net: ${netPnl.toFixed(4)} | Entry: ${avgEntryPrice} | Exit: ${avgExitPrice}`);
      } else {
        pnlHistory.push({
          symbol,
          orderId: null,
          closeOrderId,
          side,
          entryPrice: avgEntryPrice,
          exitPrice: avgExitPrice,
          tpPrice: null,
          slPrice: null,
          qty,
          grossPnl,
          pnl: netPnl,
          fees,
          exitType: 'RECONCILED',
          atr: null,
          trailingStop: null,
          tpMethod: null,
          openTime: createdTime,
          closedAt: createdTime,
          holdTimeMs: 0,
          liqUsdValue: 0,
          execTimeMs: 0,
          entryIsMaker,
          exitIsMaker,
        });
        console.log(`[RECONCILE] Backfilled ${symbol} | Gross: ${grossPnl.toFixed(4)} | Net: ${netPnl.toFixed(4)} | Entry: ${avgEntryPrice} | Exit: ${avgExitPrice} | CloseOrderId: ${closeOrderId}`);
      }

      usedCloseOrderIds.add(closeOrderId);
      backfilled++;
    }

    if (backfilled > 0) {
      // Sort by closedAt descending (newest first)
      pnlHistory.sort((a, b) => (b.closedAt || 0) - (a.closedAt || 0));
      console.log(`[RECONCILE] Backfilled ${backfilled} missing trades from Bybit.`);
    } else {
      console.log('[RECONCILE] All Bybit trades already in history — nothing to backfill.');
    }
  } catch (err) {
    console.error('[RECONCILE] Error during reconciliation:', err.message);
  }
}

// Start polling positions every 2 seconds
export function startMonitor() {
  console.log(`[MONITOR] Starting position monitor (2s interval)...`);
  setInterval(syncPositions, 2000);

  // Periodic reconciliation every 2 minutes to catch any missed PnL
  setInterval(() => {
    reconcilePnl().catch(err => console.error('[MONITOR] Periodic reconciliation error:', err.message));
  }, 2 * 60 * 1000);
}
