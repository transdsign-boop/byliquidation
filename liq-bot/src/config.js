import 'dotenv/config';

const NETWORK = process.env.NETWORK || 'testnet';

const ENDPOINTS = {
  testnet: {
    rest: 'https://api-testnet.bybit.com',
    ws_public: 'wss://stream-testnet.bybit.com/v5/public/linear',
    ws_private: 'wss://stream-testnet.bybit.com/v5/private',
    ws_trade: 'wss://stream-testnet.bybit.com/v5/trade',
  },
  demo: {
    rest: 'https://api-demo.bybit.com',
    ws_public: 'wss://stream.bybit.com/v5/public/linear',
    ws_private: 'wss://stream-demo.bybit.com/v5/private',
    ws_trade: 'wss://stream-demo.bybit.com/v5/trade',
  },
  mainnet: {
    rest: 'https://api.bybit.com',
    ws_public: 'wss://stream.bybit.com/v5/public/linear',
    ws_private: 'wss://stream.bybit.com/v5/private',
    ws_trade: 'wss://stream.bybit.com/v5/trade',
  },
};

export const config = {
  network: NETWORK,
  apiKey: process.env.BYBIT_API_KEY,
  apiSecret: process.env.BYBIT_API_SECRET,

  endpoints: ENDPOINTS[NETWORK],

  // Trading params
  positionSizeUsd: parseFloat(process.env.POSITION_SIZE_USD || '50'),
  takeProfitPct: parseFloat(process.env.TAKE_PROFIT_PCT || '0.3'),
  totalRiskPct: parseFloat(process.env.TOTAL_RISK_PCT || '5'),
  maxPositions: parseInt(process.env.MAX_POSITIONS || '5'),
  minLiqValueUsd: parseFloat(process.env.MIN_LIQ_VALUE_USD || '10000'),
  minTurnover24h: parseFloat(process.env.MIN_TURNOVER_24H || '5000000'),
  leverage: parseInt(process.env.LEVERAGE || '5'),

  // Funded trader rules
  minPositionPct: parseFloat(process.env.MIN_POSITION_PCT || '50'),  // Total DCA budget = 50% of balance (first entry = 5%)
  minTpPct: parseFloat(process.env.MIN_TP_PCT || '1'),               // Min profit = 1% of trade value

  // ATR-based TP/SL/Trailing params
  atrPeriod: parseInt(process.env.ATR_PERIOD || '14'),
  atrInterval: process.env.ATR_INTERVAL || '1',
  tpAtrMultiplier: parseFloat(process.env.TP_ATR_MULTIPLIER || '1.5'),
  slAtrMultiplier: parseFloat(process.env.SL_ATR_MULTIPLIER || '1'),
  trailingAtrMultiplier: parseFloat(process.env.TRAILING_ATR_MULTIPLIER || '1.5'),
  dcaVwapSdMultiplier: parseFloat(process.env.DCA_VWAP_SD || '2'),

  // Order types: 'Market' or 'Limit' (Limit uses PostOnly for maker fees)
  entryOrderType: process.env.ENTRY_ORDER_TYPE || 'Limit',
  tpOrderType: process.env.TP_ORDER_TYPE || 'Limit',

  // Ports
  dashboardPort: parseInt(process.env.DASHBOARD_PORT || '3000'),
  botPort: parseInt(process.env.BOT_PORT || '3001'),
};
