import WebSocket from 'ws';
import { config } from '../config.js';
import { EventEmitter } from 'events';
import { instrumentCache } from './instruments.js';

/**
 * Liquidation Scanner
 *
 * Connects to Bybit's public WebSocket and listens for liquidation events
 * across ALL USDT perpetual pairs. Emits 'liquidation' events with parsed data.
 *
 * Subscribes per-symbol using allLiquidation.{SYMBOL} topic.
 * Bybit allows max 10 args per subscribe message, so we batch them.
 */
export class LiquidationScanner extends EventEmitter {
  constructor() {
    super();
    this.ws = null;
    this.alive = false;
    this.reconnectDelay = 1000;
    this.stats = { total: 0, filtered: 0, errors: 0 };
  }

  connect() {
    console.log(`[SCANNER] Connecting to ${config.endpoints.ws_public}...`);

    this.ws = new WebSocket(config.endpoints.ws_public, {
      perMessageDeflate: false,
      skipUTF8Validation: true,
    });

    this.ws.on('open', () => {
      console.log('[SCANNER] Connected. Subscribing to allLiquidation for all symbols...');
      this.alive = true;
      this.reconnectDelay = 1000;

      // Subscribe to all symbols in batches of 10 (Bybit limit per message)
      const symbols = instrumentCache.symbols();
      const BATCH_SIZE = 10;
      let batchCount = 0;

      for (let i = 0; i < symbols.length; i += BATCH_SIZE) {
        const batch = symbols.slice(i, i + BATCH_SIZE);
        const args = batch.map(s => `allLiquidation.${s}`);
        this.ws.send(JSON.stringify({ op: 'subscribe', args }));
        batchCount++;
      }

      console.log(`[SCANNER] Sent ${batchCount} subscription batches for ${symbols.length} symbols.`);

      this._startHeartbeat();
    });

    // --- HOT PATH: message handler ---
    this.ws.on('message', (raw) => {
      try {
        const msg = JSON.parse(raw);

        // Fast topic check — allLiquidation.{SYMBOL}
        if (!msg.topic || !msg.topic.startsWith('allLiquidation.')) return;

        const items = msg.data;
        if (!Array.isArray(items)) return;

        for (const d of items) {
          this.stats.total++;

          // API format: p=price, v=size, s=symbol, S=side, T=timestamp
          const price = parseFloat(d.p);
          const qty = parseFloat(d.v);
          const usdValue = price * qty;

          const qualifies = usdValue >= config.minLiqValueUsd;
          if (!qualifies) this.stats.filtered++;

          const liqEvent = {
            symbol: d.s,
            side: d.S,        // Buy = long liquidated, Sell = short liquidated
            price,
            qty,
            usdValue,
            qualifies,
            timestamp: Date.now(),
            updatedTime: d.T,
          };

          this.emit('liquidation', liqEvent);
        }
      } catch (e) {
        this.stats.errors++;
      }
    });

    this.ws.on('close', () => {
      console.log('[SCANNER] Disconnected. Reconnecting...');
      this.alive = false;
      this._reconnect();
    });

    this.ws.on('error', (err) => {
      console.error('[SCANNER] WS error:', err.message);
      this.stats.errors++;
    });
  }

  _startHeartbeat() {
    this._hbInterval = setInterval(() => {
      if (this.ws?.readyState === WebSocket.OPEN) {
        this.ws.send(JSON.stringify({ op: 'ping' }));
      }
    }, 20000);
  }

  _reconnect() {
    clearInterval(this._hbInterval);
    setTimeout(() => {
      this.reconnectDelay = Math.min(this.reconnectDelay * 2, 30000);
      this.connect();
    }, this.reconnectDelay);
  }

  disconnect() {
    clearInterval(this._hbInterval);
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
  }
}
