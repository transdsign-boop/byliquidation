import { getKlines } from '../api/bybit.js';
import { config } from '../config.js';

/**
 * VWAP with Standard Deviation Bands.
 *
 * VWAP = Σ(typical_price × volume) / Σ(volume)
 * SD   = sqrt( Σ(volume × (typical_price - VWAP)²) / Σ(volume) )
 *
 * Bands: VWAP ± (multiplier × SD)
 *
 * DCA only triggers when price is beyond the band — statistically extended
 * from fair value based on each asset's own volatility around VWAP.
 */

const vwapCache = new Map(); // symbol -> { vwap, sd, timestamp }
const CACHE_TTL_MS = 30_000; // 30s cache

export async function getVWAP(symbol, interval = config.atrInterval, limit = 50) {
  // Check cache
  const cached = vwapCache.get(symbol);
  if (cached && Date.now() - cached.timestamp < CACHE_TTL_MS) {
    return cached;
  }

  try {
    const res = await getKlines(symbol, interval, limit);

    if (res.retCode !== 0 || !res.result?.list?.length) {
      console.error(`[VWAP] Failed to fetch klines for ${symbol}:`, res.retMsg);
      return null;
    }

    const candles = res.result.list;

    // Bybit returns candles newest-first: [timestamp, open, high, low, close, volume, turnover]
    // Pass 1: Calculate VWAP
    let sumTPV = 0;
    let sumVol = 0;
    const tps = []; // store typical prices and volumes for SD calculation
    const vols = [];

    for (const c of candles) {
      const high = parseFloat(c[2]);
      const low = parseFloat(c[3]);
      const close = parseFloat(c[4]);
      const volume = parseFloat(c[5]);

      const tp = (high + low + close) / 3;
      sumTPV += tp * volume;
      sumVol += volume;
      tps.push(tp);
      vols.push(volume);
    }

    if (sumVol === 0) {
      return null;
    }

    const vwap = sumTPV / sumVol;

    // Pass 2: Volume-weighted standard deviation
    let sumWeightedSqDev = 0;
    for (let i = 0; i < tps.length; i++) {
      const dev = tps[i] - vwap;
      sumWeightedSqDev += vols[i] * dev * dev;
    }
    const sd = Math.sqrt(sumWeightedSqDev / sumVol);

    const result = { vwap, sd, timestamp: Date.now() };
    vwapCache.set(symbol, result);

    return result;
  } catch (err) {
    console.error(`[VWAP] Error calculating VWAP for ${symbol}:`, err.message);
    return null;
  }
}
