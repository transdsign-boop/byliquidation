import { config } from '../config.js';
import { getPositions, closePosition, setTradingStop, getClosedPnl, getOrderbook, cancelOrder, getExecutionList } from '../api/bybit.js';
import { instrumentCache } from './instruments.js';

// Track recently closed symbols to prevent duplicate close records
const recentlyClosedSymbols = new Map(); // symbol -> timestamp

/**
 * Fetch all close data directly from Bybit APIs — no calculated values.
 * Matches the correct closed PnL record by entry price + qty to avoid
 * picking up the wrong record when multiple trades happen on the same symbol.
 *
 * Retries up to 3 times with 1s delay if no matching record found (API settle time).
 */
async function fetchBybitCloseData(symbol, entryOrderId, trackedEntryPrice = 0, trackedQty = 0) {
  const data = {
    pnl: 0,
    fees: { open: 0, close: 0, total: 0 },
    entryIsMaker: false,
    exitIsMaker: false,
    avgEntryPrice: 0,
    avgExitPrice: 0,
    closeOrderId: null,
  };

  // 1. Closed PnL — fetch multiple records and match by entry price + qty
  let closeOrderId = null;
  const maxRetries = 5;

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    if (attempt > 0) {
      await new Promise(r => setTimeout(r, 2000));
      console.log(`[MONITOR] ${symbol} retry ${attempt + 1}/${maxRetries} for closed PnL match...`);
    }

    try {
      const pnlRes = await getClosedPnl(symbol, 10);
      if (pnlRes.retCode !== 0 || !pnlRes.result?.list?.length) continue;

      // Find the record matching our tracked position
      let bestMatch = null;

      for (const rec of pnlRes.result.list) {
        const recEntry = parseFloat(rec.avgEntryPrice || '0');
        const recQty = parseFloat(rec.qty || '0');
        const recId = rec.orderId;

        // Skip records we've already recorded (by closeOrderId)
        if (usedCloseOrderIds.has(recId)) continue;

        // Match by entry price (within 0.5%) and qty (within 1%)
        if (trackedEntryPrice > 0 && recEntry > 0) {
          const priceDiff = Math.abs(recEntry - trackedEntryPrice) / trackedEntryPrice;
          const qtyDiff = trackedQty > 0 && recQty > 0 ? Math.abs(recQty - trackedQty) / trackedQty : 0;

          if (priceDiff < 0.005 && qtyDiff < 0.01) {
            bestMatch = rec;
            break; // exact match found
          }
        }
      }

      // Fallback: if no match by price/qty, use the most recent unused record
      if (!bestMatch) {
        for (const rec of pnlRes.result.list) {
          if (!usedCloseOrderIds.has(rec.orderId)) {
            bestMatch = rec;
            break;
          }
        }
      }

      if (bestMatch) {
        data.pnl = parseFloat(bestMatch.closedPnl || '0');
        data.avgEntryPrice = parseFloat(bestMatch.avgEntryPrice || '0');
        data.avgExitPrice = parseFloat(bestMatch.avgExitPrice || '0');
        closeOrderId = bestMatch.orderId;
        data.closeOrderId = closeOrderId;
        usedCloseOrderIds.add(closeOrderId);

        console.log(`[MONITOR] ${symbol} matched closed PnL | Entry: ${data.avgEntryPrice} | Exit: ${data.avgExitPrice} | PnL: ${data.pnl} | CloseOrderId: ${closeOrderId}`);
        break; // success
      }
    } catch (err) {
      console.warn(`[MONITOR] Could not fetch closed PnL for ${symbol}:`, err.message);
    }
  }

  if (!closeOrderId) {
    console.warn(`[MONITOR] ${symbol} no matching closed PnL record found after ${maxRetries} attempts`);
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

  console.log(`[MONITOR] ${symbol} final | PnL: ${data.pnl.toFixed(4)} | Open fee: ${data.fees.open.toFixed(6)} (${data.entryIsMaker ? 'MAKER' : 'TAKER'}) | Close fee: ${data.fees.close.toFixed(6)} (${data.exitIsMaker ? 'MAKER' : 'TAKER'}) | Entry: ${data.avgEntryPrice} | Exit: ${data.avgExitPrice}`);

  return data;
}

// Track close order IDs we've already recorded to prevent duplicate matching
const usedCloseOrderIds = new Set();

import { getActivePositions, getTradeLog } from './executor.js';

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

export function hydratePnl(savedHistory, savedTotal) {
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
}

// Read dynamically so runtime changes via API take effect
function getMaxHoldTimeMs() {
  return config.maxHoldSeconds * 1000;
}

export function getPnlHistory() {
  return pnlHistory;
}

export function getTotalPnl() {
  return totalPnl;
}

export function resetPnl() {
  pnlHistory = [];
  totalPnl = 0;
  console.log('[MONITOR] PnL data reset to zero.');
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
  const wins = pnlHistory.filter(p => p.pnl > 0);
  const losses = pnlHistory.filter(p => p.pnl <= 0);
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
    winRate: pnlHistory.length > 0 ? (wins.length / pnlHistory.length * 100).toFixed(1) : '0',
    totalPnl: totalPnl.toFixed(4),
    totalGrossPnl: totalGrossPnl.toFixed(4),
    totalFees: totalFees.toFixed(4),
    avgExecTimeMs: avgExecTime.toFixed(0),
  };
}

export async function syncPositions() {
  const activePositions = getActivePositions();
  if (activePositions.size === 0) return;

  try {
    const res = await getPositions();
    if (res.retCode !== 0) return;

    const bybitPositions = new Map();
    for (const p of res.result.list) {
      if (parseFloat(p.size) > 0) {
        bybitPositions.set(p.symbol, p);
      }
    }

    // Check each tracked position
    for (const [symbol, tracked] of activePositions) {
      const bybitPos = bybitPositions.get(symbol);

      if (!bybitPos) {
        // Guard: prevent duplicate close records (e.g. sync runs twice before deletion)
        if (recentlyClosedSymbols.has(symbol) && Date.now() - recentlyClosedSymbols.get(symbol) < 10000) {
          activePositions.delete(symbol);
          continue;
        }
        recentlyClosedSymbols.set(symbol, Date.now());

        // Position gone — fetch all close data from Bybit APIs
        // Wait 2s for Bybit to settle the closed PnL record
        await new Promise(r => setTimeout(r, 2000));
        const closeData = await fetchBybitCloseData(symbol, tracked.orderId, tracked.entryPrice, tracked.qty);
        recordClose(symbol, tracked, closeData, 'TP/SL/TRAIL');
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

      // Position still open — check time-based forced exit
      const holdTimeMs = Date.now() - tracked.openTime;
      if (holdTimeMs > getMaxHoldTimeMs()) {
        // Skip time exit if trailing stop has activated (position in profit, let trail handle it)
        const uPnl = tracked.unrealisedPnl || 0;
        const markPrice = tracked.markPrice || 0;
        let trailActivated = false;
        if (tracked.trailingStop && tracked.trailActivePrice && markPrice > 0) {
          trailActivated = tracked.side === 'Buy'
            ? markPrice >= tracked.trailActivePrice
            : markPrice <= tracked.trailActivePrice;
        }

        if (uPnl > 0 && trailActivated) {
          console.log(`[MONITOR] ${symbol} held ${(holdTimeMs / 1000).toFixed(0)}s but in profit (uPnl: ${uPnl.toFixed(4)}) with trailing active — skipping time exit`);
          continue;
        }

        console.log(`[MONITOR] ${symbol} held for ${(holdTimeMs / 1000).toFixed(0)}s — force-closing (uPnl: ${uPnl.toFixed(4)}, trail active: ${trailActivated})...`);

        // Mark as closing BEFORE async ops to prevent duplicate from "position gone" path
        recentlyClosedSymbols.set(symbol, Date.now());

        try {
          let closed = false;
          const exitType = config.timeExitOrderType || 'Market';

          if (exitType === 'Limit') {
            // Try limit close (PostOnly for maker fee)
            let limitPrice = null;
            try {
              const ob = await getOrderbook(symbol, 1);
              if (ob.retCode === 0 && ob.result) {
                // Close Buy → Sell at best ask (maker); Close Sell → Buy at best bid (maker)
                limitPrice = tracked.side === 'Buy'
                  ? parseFloat(ob.result.a[0][0])
                  : parseFloat(ob.result.b[0][0]);
                limitPrice = instrumentCache.roundPrice(symbol, limitPrice);
              }
            } catch (err) {
              console.warn(`[MONITOR] Orderbook error for ${symbol} on TIME_EXIT:`, err.message);
            }

            if (limitPrice) {
              const limitClose = await closePosition(symbol, tracked.side, tracked.qty, 'Limit', limitPrice);
              if (limitClose.retCode === 0) {
                await new Promise(r => setTimeout(r, 2000));

                // Check if position still exists (limit may not have filled)
                const posCheck = await getPositions();
                const stillOpen = posCheck.retCode === 0 &&
                  posCheck.result.list.some(p => p.symbol === symbol && parseFloat(p.size) > 0);

                if (stillOpen) {
                  await cancelOrder(symbol, limitClose.result.orderId).catch(() => {});
                  console.log(`[MONITOR] Limit close not filled for ${symbol}, falling back to market`);
                } else {
                  closed = true;
                  console.log(`[MONITOR] ${symbol} limit-closed @ ${limitPrice} (maker fee)`);
                }
              }
            }
          }

          // Market close (default or fallback from limit)
          if (!closed) {
            const closeResult = await closePosition(symbol, tracked.side, tracked.qty);
            if (closeResult.retCode !== 0) {
              console.error(`[MONITOR] Force-close failed for ${symbol}: ${closeResult.retMsg}`);
              continue;
            }
          }

          // Wait for Bybit to settle, then fetch all close data from Bybit APIs
          await new Promise(r => setTimeout(r, 2500));
          const closeData = await fetchBybitCloseData(symbol, tracked.orderId, tracked.entryPrice, tracked.qty);
          recordClose(symbol, tracked, closeData, 'TIME_EXIT');

          console.log(
            `[MONITOR] ${symbol} force-closed | PnL: ${closeData.pnl.toFixed(4)} | Fees: ${closeData.fees.total.toFixed(4)} | Held: ${(holdTimeMs / 1000).toFixed(0)}s`
          );
        } catch (err) {
          console.error(`[MONITOR] Force-close error for ${symbol}:`, err.message);
        }
      }
    }
  } catch (err) {
    console.error('[MONITOR] Sync error:', err.message);
  }
}

function recordClose(symbol, tracked, closeData, exitType) {
  const activePositions = getActivePositions();

  // Bybit closedPnl is NET (after fees). Compute gross for display.
  const grossPnl = closeData.pnl + closeData.fees.total;

  pnlHistory.unshift({
    symbol,
    orderId: tracked.orderId || null,
    closeOrderId: closeData.closeOrderId || null,
    side: tracked.side,
    // Bybit actual entry/exit prices
    entryPrice: closeData.avgEntryPrice || tracked.entryPrice,
    exitPrice: closeData.avgExitPrice || 0,
    tpPrice: tracked.tpPrice,
    slPrice: tracked.slPrice,
    qty: tracked.qty,
    // All from Bybit API — no calculated values
    grossPnl,
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
    const pnlRes = await getClosedPnl(null, 50);
    if (pnlRes.retCode !== 0 || !pnlRes.result?.list?.length) {
      console.log('[RECONCILE] No closed PnL records from Bybit or API error.');
      return;
    }

    const bybitRecords = pnlRes.result.list;

    // Build time-based fallback lookup for records without closeOrderId
    const knownByTimeSymbol = new Set();
    for (const rec of pnlHistory) {
      if (rec.closedAt && rec.symbol) {
        const bucket = Math.floor(rec.closedAt / 30000); // 30s buckets
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
      const bucket = Math.floor(createdTime / 30000);

      // Skip if we already have this trade (by closeOrderId or time proximity)
      if (usedCloseOrderIds.has(closeOrderId)) continue;
      if (knownByTimeSymbol.has(`${symbol}_${bucket}`)) continue;

      // Missing trade — backfill from Bybit data
      const netPnl = parseFloat(rec.closedPnl || '0');
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
      const grossPnl = netPnl + fees.total;

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

      totalPnl += netPnl;
      usedCloseOrderIds.add(closeOrderId);
      backfilled++;

      console.log(`[RECONCILE] Backfilled ${symbol} | Net PnL: ${netPnl.toFixed(4)} | Close: ${avgExitPrice} | CloseOrderId: ${closeOrderId}`);
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
  console.log(`[MONITOR] Starting position monitor (2s interval, ${config.maxHoldSeconds}s max hold)...`);
  setInterval(syncPositions, 2000);
}
