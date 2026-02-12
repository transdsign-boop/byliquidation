import crypto from 'crypto';
import WebSocket from 'ws';
import { config } from '../config.js';

/**
 * WebSocket Trade Client
 *
 * Places orders via Bybit's WebSocket trade endpoint instead of REST.
 * Saves ~30-50ms per order by skipping HTTP overhead.
 * Falls back to REST (via callback) if WS is not connected.
 */

let ws = null;
let authenticated = false;
let reconnectTimer = null;
let disabled = false; // stop reconnecting if endpoint doesn't exist
let reconnectAttempts = 0;
const MAX_RECONNECT_ATTEMPTS = 3;
const pendingOrders = new Map(); // reqId -> { resolve, reject, timer }
let reqCounter = 0;

export function isTradeWsReady() {
  return ws && ws.readyState === WebSocket.OPEN && authenticated;
}

export function connectTradeWs() {
  if (disabled) return;
  if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) {
    return;
  }

  const url = config.endpoints.ws_trade;
  console.log(`[WS-TRADE] Connecting to ${url}...`);

  ws = new WebSocket(url);

  ws.on('open', () => {
    console.log('[WS-TRADE] Connected. Authenticating...');
    reconnectAttempts = 0;
    authenticate();
  });

  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data.toString());

      // Auth response
      if (msg.op === 'auth') {
        if (msg.success) {
          authenticated = true;
          console.log('[WS-TRADE] Authenticated. Ready for orders.');
        } else {
          console.error('[WS-TRADE] Auth failed:', msg.retMsg);
          authenticated = false;
        }
        return;
      }

      // Order response
      if (msg.reqId && pendingOrders.has(msg.reqId)) {
        const pending = pendingOrders.get(msg.reqId);
        clearTimeout(pending.timer);
        pendingOrders.delete(msg.reqId);

        // Format response to match REST API shape
        pending.resolve({
          retCode: msg.retCode,
          retMsg: msg.retMsg,
          result: msg.data || {},
        });
        return;
      }

      // Pong
      if (msg.op === 'pong') return;

    } catch (err) {
      console.error('[WS-TRADE] Parse error:', err.message);
    }
  });

  ws.on('close', () => {
    authenticated = false;
    rejectAllPending('WebSocket disconnected');
    reconnectAttempts++;
    if (reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
      disabled = true;
      console.log('[WS-TRADE] Endpoint unavailable after 3 attempts — using REST only.');
    } else {
      console.log(`[WS-TRADE] Disconnected. Retry ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS}...`);
      scheduleReconnect();
    }
  });

  ws.on('error', (err) => {
    if (!disabled) console.error('[WS-TRADE] Error:', err.message);
    authenticated = false;
  });

  // Heartbeat
  const pingInterval = setInterval(() => {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ op: 'ping' }));
    } else {
      clearInterval(pingInterval);
    }
  }, 20000);
}

function authenticate() {
  const expires = Date.now() + 10000;
  const signature = crypto
    .createHmac('sha256', config.apiSecret)
    .update(`GET/realtime${expires}`)
    .digest('hex');

  ws.send(JSON.stringify({
    op: 'auth',
    args: [config.apiKey, expires, signature],
  }));
}

function scheduleReconnect() {
  if (reconnectTimer) return;
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connectTradeWs();
  }, 3000);
}

function rejectAllPending(reason) {
  for (const [reqId, pending] of pendingOrders) {
    clearTimeout(pending.timer);
    pending.reject(new Error(reason));
  }
  pendingOrders.clear();
}

export function disconnectTradeWs() {
  if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }
  if (ws) { ws.removeAllListeners(); ws.close(); ws = null; }
  authenticated = false;
  disabled = false;
  reconnectAttempts = 0;
  rejectAllPending('Account switch — WS disconnected');
  console.log('[WS-TRADE] Disconnected for account switch.');
}

/**
 * Place order via WebSocket. Returns same shape as REST placeOrder.
 * Rejects if WS not ready (caller should fall back to REST).
 */
export function placeOrderWs(symbol, side, qty, orderType = 'Market', extraParams = {}) {
  return new Promise((resolve, reject) => {
    if (!isTradeWsReady()) {
      return reject(new Error('Trade WS not ready'));
    }

    const reqId = `order_${++reqCounter}_${Date.now()}`;
    const timestamp = Date.now().toString();

    const msg = {
      reqId,
      header: {
        'X-BAPI-TIMESTAMP': timestamp,
        'X-BAPI-RECV-WINDOW': '5000',
      },
      op: 'order.create',
      args: [{
        category: 'linear',
        symbol,
        side,
        orderType,
        qty: String(qty),
        timeInForce: 'GTC',
        positionIdx: 0,
        ...extraParams,
      }],
    };

    // Timeout after 5s
    const timer = setTimeout(() => {
      pendingOrders.delete(reqId);
      reject(new Error('WS order timeout'));
    }, 5000);

    pendingOrders.set(reqId, { resolve, reject, timer });
    ws.send(JSON.stringify(msg));
  });
}
