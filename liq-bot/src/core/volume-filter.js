import { getAllTickers } from '../api/bybit.js';
import { config } from '../config.js';

/**
 * Volume filter — skips trades on low-volume coins.
 * Funded trader challenges prohibit heavy exposure to illiquid assets.
 *
 * Fetches ALL linear tickers on startup, caches turnover24h per symbol,
 * refreshes every 5 minutes.
 */

const volumeCache = new Map(); // symbol -> turnover24h (USDT)
let refreshInterval = null;

async function fetchVolumes() {
  try {
    const res = await getAllTickers();
    if (res.retCode !== 0) {
      console.error('[VOLUME] Failed to fetch tickers:', res.retMsg);
      return;
    }

    let count = 0;
    for (const t of res.result.list) {
      volumeCache.set(t.symbol, parseFloat(t.turnover24h));
      count++;
    }

    console.log(`[VOLUME] Loaded volume data for ${count} symbols`);
  } catch (err) {
    console.error('[VOLUME] Fetch error:', err.message);
  }
}

export function isLowVolume(symbol) {
  const turnover = volumeCache.get(symbol);
  if (turnover == null) return false; // unknown symbol — don't block
  return turnover < config.minTurnover24h;
}

export function getTurnover(symbol) {
  return volumeCache.get(symbol);
}

export async function loadVolumes() {
  await fetchVolumes();
  // Refresh every 5 minutes
  refreshInterval = setInterval(fetchVolumes, 5 * 60 * 1000);
}
