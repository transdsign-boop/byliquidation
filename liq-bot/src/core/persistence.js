import fs from 'fs';
import path from 'path';

/**
 * File-based persistence for trade data.
 * Saves to /data (fly.io volume mount) or ./data (local fallback).
 */

const DATA_DIR = process.env.DATA_DIR || (fs.existsSync('/data') ? '/data' : './data');

// Ensure directory exists
try {
  fs.mkdirSync(DATA_DIR, { recursive: true });
} catch {}

function filePath(name) {
  return path.join(DATA_DIR, name);
}

export function saveJSON(name, data) {
  try {
    const tmp = filePath(name + '.tmp');
    const target = filePath(name);
    fs.writeFileSync(tmp, JSON.stringify(data));
    fs.renameSync(tmp, target); // atomic write
  } catch (err) {
    console.error(`[PERSIST] Failed to save ${name}:`, err.message);
  }
}

export function loadJSON(name, fallback = null) {
  try {
    const target = filePath(name);
    if (!fs.existsSync(target)) return fallback;
    const raw = fs.readFileSync(target, 'utf8');
    return JSON.parse(raw);
  } catch (err) {
    console.error(`[PERSIST] Failed to load ${name}:`, err.message);
    return fallback;
  }
}

/**
 * Auto-save: call periodically to persist current state.
 * Expects getter functions for each data source.
 */
let saveInterval = null;

export function startPersistence({ getTradeLog, getPnlHistory, getTotalPnl }) {
  // Load existing data on startup
  const savedPnl = loadJSON('pnl_history.json');
  const savedTrades = loadJSON('trade_log.json');
  const savedTotalPnl = loadJSON('total_pnl.json');

  console.log(`[PERSIST] Data dir: ${DATA_DIR}`);
  if (savedPnl) console.log(`[PERSIST] Loaded ${savedPnl.length} PnL records from disk.`);
  if (savedTrades) console.log(`[PERSIST] Loaded ${savedTrades.length} trade log entries from disk.`);

  // Save every 10 seconds
  saveInterval = setInterval(() => {
    saveJSON('trade_log.json', getTradeLog());
    saveJSON('pnl_history.json', getPnlHistory());
    saveJSON('total_pnl.json', getTotalPnl());
  }, 10000);

  return {
    pnlHistory: savedPnl || [],
    tradeLog: savedTrades || [],
    totalPnl: savedTotalPnl || 0,
  };
}
