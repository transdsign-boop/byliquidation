import crypto from 'crypto';
import { config } from '../config.js';

// --- Speed optimization: reuse HTTP agent with keep-alive ---
const RECV_WINDOW = '5000';

function sign(timestamp, params) {
  const raw = timestamp + config.apiKey + RECV_WINDOW + params;
  return crypto.createHmac('sha256', config.apiSecret).update(raw).digest('hex');
}

// Pre-built headers template (avoid object creation in hot path)
function authHeaders(timestamp, signature) {
  return {
    'Content-Type': 'application/json',
    'X-BAPI-API-KEY': config.apiKey,
    'X-BAPI-TIMESTAMP': timestamp,
    'X-BAPI-SIGN': signature,
    'X-BAPI-RECV-WINDOW': RECV_WINDOW,
  };
}

export async function switchToOneWayMode(symbol) {
  const timestamp = Date.now().toString();
  const body = {
    category: 'linear',
    symbol,
    mode: 0,  // 0 = one-way mode
  };
  const bodyStr = JSON.stringify(body);
  const signature = sign(timestamp, bodyStr);

  const res = await fetch(`${config.endpoints.rest}/v5/position/switch-mode`, {
    method: 'POST',
    headers: authHeaders(timestamp, signature),
    body: bodyStr,
  });
  return res.json();
}

export async function placeOrder(symbol, side, qty, orderType = 'Market', extraParams = {}) {
  const timestamp = Date.now().toString();
  const body = {
    category: 'linear',
    symbol,
    side,        // 'Buy' or 'Sell'
    orderType,   // 'Market' or 'Limit'
    qty: String(qty),
    timeInForce: 'GTC',
    positionIdx: 0,  // one-way mode
    ...extraParams,
  };
  const bodyStr = JSON.stringify(body);
  const signature = sign(timestamp, bodyStr);

  const res = await fetch(`${config.endpoints.rest}/v5/order/create`, {
    method: 'POST',
    headers: authHeaders(timestamp, signature),
    body: bodyStr,
  });
  return res.json();
}

export async function setLeverage(symbol, leverage) {
  const timestamp = Date.now().toString();
  const body = {
    category: 'linear',
    symbol,
    buyLeverage: String(leverage),
    sellLeverage: String(leverage),
  };
  const bodyStr = JSON.stringify(body);
  const signature = sign(timestamp, bodyStr);

  const res = await fetch(`${config.endpoints.rest}/v5/position/set-leverage`, {
    method: 'POST',
    headers: authHeaders(timestamp, signature),
    body: bodyStr,
  });
  return res.json();
}

export async function setTradingStop(symbol, { takeProfit, stopLoss, trailingStop, activePrice, tpOrderType, tpLimitPrice } = {}) {
  const timestamp = Date.now().toString();
  const body = {
    category: 'linear',
    symbol,
    positionIdx: 0,
  };
  if (takeProfit != null) body.takeProfit = String(takeProfit);
  if (stopLoss != null) body.stopLoss = String(stopLoss);
  if (trailingStop != null) body.trailingStop = String(trailingStop);
  if (activePrice != null) body.activePrice = String(activePrice);
  if (tpOrderType != null) body.tpOrderType = tpOrderType;
  if (tpLimitPrice != null) body.tpLimitPrice = String(tpLimitPrice);

  const bodyStr = JSON.stringify(body);
  const signature = sign(timestamp, bodyStr);

  const res = await fetch(`${config.endpoints.rest}/v5/position/trading-stop`, {
    method: 'POST',
    headers: authHeaders(timestamp, signature),
    body: bodyStr,
  });
  return res.json();
}

export async function getPositions() {
  const timestamp = Date.now().toString();
  const params = 'category=linear&settleCoin=USDT';
  const signature = sign(timestamp, params);

  const res = await fetch(`${config.endpoints.rest}/v5/position/list?${params}`, {
    method: 'GET',
    headers: authHeaders(timestamp, signature),
  });
  return res.json();
}

export async function getWalletBalance() {
  const timestamp = Date.now().toString();
  const params = 'accountType=UNIFIED';
  const signature = sign(timestamp, params);

  const res = await fetch(`${config.endpoints.rest}/v5/account/wallet-balance?${params}`, {
    method: 'GET',
    headers: authHeaders(timestamp, signature),
  });
  return res.json();
}

export async function getTickers(symbol) {
  const res = await fetch(`${config.endpoints.rest}/v5/market/tickers?category=linear&symbol=${symbol}`);
  return res.json();
}

export async function getAllTickers() {
  const res = await fetch(`${config.endpoints.rest}/v5/market/tickers?category=linear`);
  return res.json();
}

export async function getInstrumentsInfo() {
  const res = await fetch(`${config.endpoints.rest}/v5/market/instruments-info?category=linear&limit=1000`);
  return res.json();
}

export async function getKlines(symbol, interval = '1', limit = 20) {
  const params = `category=linear&symbol=${symbol}&interval=${interval}&limit=${limit}`;
  const res = await fetch(`${config.endpoints.rest}/v5/market/kline?${params}`);
  return res.json();
}

export async function closePosition(symbol, side, qty, orderType = 'Market', price = null) {
  const closeSide = side === 'Buy' ? 'Sell' : 'Buy';
  const extra = { reduceOnly: true };
  if (orderType === 'Limit' && price != null) {
    extra.price = String(price);
    extra.timeInForce = 'PostOnly';
  }
  return placeOrder(symbol, closeSide, qty, orderType, extra);
}

export async function cancelOrder(symbol, orderId) {
  const timestamp = Date.now().toString();
  const body = {
    category: 'linear',
    symbol,
    orderId,
  };
  const bodyStr = JSON.stringify(body);
  const signature = sign(timestamp, bodyStr);

  const res = await fetch(`${config.endpoints.rest}/v5/order/cancel`, {
    method: 'POST',
    headers: authHeaders(timestamp, signature),
    body: bodyStr,
  });
  return res.json();
}

export async function getExecutionList(symbol, orderId = null, limit = 20) {
  const timestamp = Date.now().toString();
  let params = `category=linear&symbol=${symbol}&limit=${limit}`;
  if (orderId) params += `&orderId=${orderId}`;
  const signature = sign(timestamp, params);

  const res = await fetch(`${config.endpoints.rest}/v5/execution/list?${params}`, {
    method: 'GET',
    headers: authHeaders(timestamp, signature),
  });
  return res.json();
}

export async function getOrderDetail(symbol, orderId) {
  const timestamp = Date.now().toString();
  const params = `category=linear&symbol=${symbol}&orderId=${orderId}`;
  const signature = sign(timestamp, params);

  const res = await fetch(`${config.endpoints.rest}/v5/order/realtime?${params}`, {
    method: 'GET',
    headers: authHeaders(timestamp, signature),
  });
  return res.json();
}

export async function getOrderbook(symbol, limit = 5) {
  const params = `category=linear&symbol=${symbol}&limit=${limit}`;
  const res = await fetch(`${config.endpoints.rest}/v5/market/orderbook?${params}`);
  return res.json();
}

export async function getClosedPnl(symbol, limit = 5) {
  const timestamp = Date.now().toString();
  let params = `category=linear&limit=${limit}`;
  if (symbol) params += `&symbol=${symbol}`;
  const signature = sign(timestamp, params);

  const res = await fetch(`${config.endpoints.rest}/v5/position/closed-pnl?${params}`, {
    method: 'GET',
    headers: authHeaders(timestamp, signature),
  });
  return res.json();
}
