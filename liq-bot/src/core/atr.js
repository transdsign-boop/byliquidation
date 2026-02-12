import { getKlines } from '../api/bybit.js';
import { config } from '../config.js';

/**
 * ATR (Average True Range) calculator with per-symbol caching.
 * Used for dynamic TP and trailing stop sizing.
 */

const atrCache = new Map(); // symbol -> { atr, timestamp }
const CACHE_TTL_MS = 60_000; // 60s cache

export async function getATR(symbol, period = config.atrPeriod, interval = config.atrInterval) {
  // Check cache
  const cached = atrCache.get(symbol);
  if (cached && Date.now() - cached.timestamp < CACHE_TTL_MS) {
    return cached.atr;
  }

  try {
    // Fetch enough candles for the ATR period (+ 1 spare)
    const res = await getKlines(symbol, interval, period + 1);

    if (res.retCode !== 0 || !res.result?.list?.length) {
      console.error(`[ATR] Failed to fetch klines for ${symbol}:`, res.retMsg);
      return null;
    }

    const candles = res.result.list;

    // Bybit returns candles newest-first: [timestamp, open, high, low, close, volume, turnover]
    // We need at least `period` candles
    if (candles.length < period) {
      console.error(`[ATR] Not enough candles for ${symbol}: got ${candles.length}, need ${period}`);
      return null;
    }

    // Calculate ATR = average of (high - low) over N candles
    let sumRange = 0;
    for (let i = 0; i < period; i++) {
      const high = parseFloat(candles[i][2]);
      const low = parseFloat(candles[i][3]);
      sumRange += high - low;
    }

    const atr = sumRange / period;

    // Cache result
    atrCache.set(symbol, { atr, timestamp: Date.now() });

    return atr;
  } catch (err) {
    console.error(`[ATR] Error calculating ATR for ${symbol}:`, err.message);
    return null;
  }
}
