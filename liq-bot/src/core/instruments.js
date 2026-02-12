import { getInstrumentsInfo } from '../api/bybit.js';

/**
 * Caches instrument info (tick size, lot size, min order qty) for fast lookups.
 * Refreshed on startup, then every 30 minutes.
 */
class InstrumentCache {
  constructor() {
    this.instruments = new Map(); // symbol -> { tickSize, lotSize, minQty, maxLeverage }
  }

  async load() {
    console.log('[INSTRUMENTS] Loading instrument info...');
    const res = await getInstrumentsInfo();

    if (res.retCode !== 0) {
      console.error('[INSTRUMENTS] Failed to load:', res.retMsg);
      return;
    }

    for (const inst of res.result.list) {
      this.instruments.set(inst.symbol, {
        tickSize: parseFloat(inst.priceFilter.tickSize),
        lotSize: parseFloat(inst.lotSizeFilter.qtyStep),
        minQty: parseFloat(inst.lotSizeFilter.minOrderQty),
        maxLeverage: parseFloat(inst.leverageFilter.maxLeverage),
        isPreListing: inst.isPreListing === true || inst.isPreListing === 'true',
        status: inst.status,
      });
    }

    console.log(`[INSTRUMENTS] Loaded ${this.instruments.size} instruments.`);
  }

  get(symbol) {
    return this.instruments.get(symbol);
  }

  has(symbol) {
    return this.instruments.has(symbol);
  }

  symbols() {
    return [...this.instruments.keys()];
  }

  isBlocked(symbol) {
    const inst = this.instruments.get(symbol);
    if (!inst) return false;
    return inst.isPreListing || inst.status !== 'Trading';
  }

  /**
   * Round qty to valid lot size
   */
  roundQty(symbol, qty) {
    const inst = this.instruments.get(symbol);
    if (!inst) return qty;
    const step = inst.lotSize;
    const decimals = step.toString().split('.')[1]?.length || 0;
    return parseFloat((Math.floor(qty / step) * step).toFixed(decimals));
  }

  /**
   * Round price to valid tick size
   */
  roundPrice(symbol, price) {
    const inst = this.instruments.get(symbol);
    if (!inst) return price;
    const tick = inst.tickSize;
    const decimals = tick.toString().split('.')[1]?.length || 0;
    return parseFloat((Math.round(price / tick) * tick).toFixed(decimals));
  }
}

export const instrumentCache = new InstrumentCache();
