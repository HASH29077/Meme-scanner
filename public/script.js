// =====================
// CONFIG
// =====================
// No API keys live here anymore. Every request that needs a provider
// key goes through this app's own /api/* serverless functions, which
// hold the real keys server-side (Vercel env vars). The browser never
// sees BIRDEYE_API_KEY or the Helius key.
const CONFIG = {};
const BIRDEYE_BASE    = '/api/birdeye'; // proxied — see api/birdeye.js
const HELIUS_RPC      = '/api/helius';  // proxied — see api/helius.js
const AUTO_INTERVAL   = 20;
const ALERT_THRESHOLD  = 70;

let startupDone = false;
let startupPassed = { dex: false, bird: false, helius: false };

// --- New-token discovery config ---
// Discovery refreshes independently of the full enrich+score scan cycle
// so freshly launched tokens show up within seconds instead of waiting
// on a full 20s scan (which is slowed by Birdeye/Helius rate limits).
const DISCOVERY_INTERVAL_MS   = 20000;   // 15-30s refresh window, see toggleLiveDiscovery()
const NEW_TOKEN_MAX_AGE_HOURS = 6;       // tokens younger than this are "new"
const MAX_NEW_TOKENS_TO_ENRICH = 15;     // always enrich the newest N regardless of liquidity
const MAX_LIQUIDITY_TOKENS_TO_ENRICH = 10; // plus top-liquidity N as before
const AGE_BUCKETS = [
  { key:'5m',  maxHours: 5/60,  label:'5M'  },
  { key:'15m', maxHours: 15/60, label:'15M' },
  { key:'30m', maxHours: 30/60, label:'30M' },
  { key:'1h',  maxHours: 1,     label:'1H'  },
  { key:'3h',  maxHours: 3,     label:'3H'  },
  { key:'6h',  maxHours: 6,     label:'6H'  },
];
function getAgeBucket(hours) {
  if (hours == null) return null;
  for (const b of AGE_BUCKETS) if (hours <= b.maxHours) return b.key;
  return null;
}

// --- Incremental discovery / early-detection config -------------------
// Purely about WHEN we fetch/enrich and HOW we rank discovery candidates.
// Does not touch computeAlphaScore, runFilter, or any scoring weight.
const VERY_FRESH_MINUTES        = 10;          // tokens under this age get an extra sharp priority kicker
const HOT_MONITOR_WINDOW_MS     = 30*60*1000;  // keep re-checking a token's metrics for its first 30 minutes
const HOT_MONITOR_HOURS         = HOT_MONITOR_WINDOW_MS / 3600000;
const HOT_REENRICH_COOLDOWN_MS  = 90*1000;     // don't re-enrich the same hot token more than once per ~90s
const POOL_RESOLVE_REFRESH_MS   = 5*60*1000;   // floor: re-resolve via DexScreener at least this often even if not "hot"
const SIGNIFICANT_CHANGE_RATIO  = 0.15;        // >15% move in liquidity/volume/buys counts as "changed significantly"
const ACCEL_HISTORY_SIZE        = 3;           // samples kept per pool for consistent-acceleration detection
const ACCEL_MAX_PRIORITY_BONUS  = 25;          // cap on how much acceleration can move a candidate up the discovery queue

// address -> { firstSeenAt, pairCreatedAt, lastResolvedAt, lastEnrichedAt,
//              lastPair, lastTokenData, history:[{t,liq,vol24h,buysM5,sellsM5,buys1h,sells1h}] }
// In-memory only (resets on reload) — this is a performance/diagnostics
// cache, not user-facing tracked data, so it deliberately doesn't share
// storage with the TRACKING system.
let poolCache = new Map();

// New diagnostics requested for this pass. Purely additive alongside the
// existing apiDiag/pipelineStats — nothing here feeds scoring or filters.
let discoveryDiag = {
  detectionDelayMsSamples: [],   // Date.now() at first sight - pairCreatedAt, per newly discovered pool
  poolAgeAtDetectionMinSamples: [],
  enrichmentTimeMsSamples: [],
  newPoolsThisScan: 0,
  cachedPoolsReusedThisScan: 0,
  accelerationPromotedThisScan: 0,
};
function pushCapped(arr, val, cap) { arr.push(val); if (arr.length > cap) arr.shift(); }
function avgOf(arr) { return arr.length ? arr.reduce((a,b)=>a+b,0)/arr.length : null; }

// Pipeline debug counters — reset every scan/discovery pass, surfaced in
// the dev panel and scan summary so drop-offs are visible at every stage.
let pipelineStats = {
  discovered: 0,   // raw candidates returned by any source, pre-dedup
  afterDedup: 0,   // unique Solana pairs after dedup/chain filter
  sentToEnrich: 0, // selected for Birdeye/Helius enrichment
  enriched: 0,     // enrichment completed without throwing
  scored: 0,       // got an alphaScore computed
  filtered: 0,     // failed runFilter() (rejected)
  displayed: 0,    // ended up rendered in the accepted grid
};
function resetPipelineStats() {
  pipelineStats = { discovered:0, afterDedup:0, sentToEnrich:0, enriched:0, scored:0, filtered:0, displayed:0 };
  discoveryDiag.newPoolsThisScan = 0;
  discoveryDiag.cachedPoolsReusedThisScan = 0;
  discoveryDiag.accelerationPromotedThisScan = 0;
}
function renderPipelineStats() {
  const el = document.getElementById('pipelineCounters');
  if (!el) return;
  el.innerHTML = Object.entries(pipelineStats).map(([k,v]) =>
    '<div>'+k+': <span style="color:var(--text2)">'+v+'</span></div>'
  ).join('');
  renderDiscoverySpeedStats();
}

// New early-detection diagnostics: how quickly discovery is finding
// pools relative to their creation time, how long enrichment takes, and
// how much work the incremental cache + acceleration ranking are saving
// or promoting. Purely observational — doesn't feed scoring or filters.
function renderDiscoverySpeedStats() {
  const el = document.getElementById('discoverySpeedStats');
  if (!el) return;
  const avgDelayMs = avgOf(discoveryDiag.detectionDelayMsSamples);
  const avgAgeMin = avgOf(discoveryDiag.poolAgeAtDetectionMinSamples);
  const avgEnrichMs = avgOf(discoveryDiag.enrichmentTimeMsSamples);
  const rows = [
    ['avg detection delay', avgDelayMs != null ? (avgDelayMs/1000).toFixed(1)+'s' : 'N/A'],
    ['avg pool age at detection', avgAgeMin != null ? avgAgeMin.toFixed(1)+'m' : 'N/A'],
    ['avg enrichment time', avgEnrichMs != null ? Math.round(avgEnrichMs)+'ms' : 'N/A'],
    ['new pools this scan', discoveryDiag.newPoolsThisScan],
    ['cached pools reused this scan', discoveryDiag.cachedPoolsReusedThisScan],
    ['promoted by acceleration this scan', discoveryDiag.accelerationPromotedThisScan],
  ];
  el.innerHTML = rows.map(([k,v]) => '<div>'+k+': <span style="color:var(--text2)">'+v+'</span></div>').join('');
}

// =====================
// STATE
// =====================
let ageFilterHours  = 6;
let scoreFilter     = 35;
let pumpOnly        = false;
let allResults      = [];
let alerts          = [];
let autoScanOn      = false;
let countdownTimer  = null;
let countdownSec    = AUTO_INTERVAL;
let unreadCount     = 0;
let alertedTokens   = new Set();
let alertTokenMap   = {};
let audioCtx        = null;
let scanState       = 'idle';    // idle | scanning | error
let debugMode       = false;
let devPanelOpen    = false;
let devLogs         = [];
let allScannedTokens = [];

// --- Live discovery feed state ---
let discoveryFeed     = [];   // lightweight, DexScreener-only entries shown immediately
let discoveryTimer    = null;
let liveDiscoveryOn   = false;
let discoverySeenAddrs = new Set(); // addresses already surfaced this session, avoids flicker/dupes
let enrichQueue       = [];   // addresses queued for background full enrichment
let enrichQueueBusy   = false;

// =====================
// TRACKING / SESSIONS / PERSISTENCE STATE
// =====================
// Purely observability. Nothing in this block feeds scoring, discovery,
// enrichment, or filtering — it only records what those systems produced.
const LS_KEYS = {
  sessions: 'solscan_sessions_v1',
  tracked:  'solscan_tracked_tokens_v1',
  results:  'solscan_last_results_v1',
  uiState:  'solscan_ui_state_v1',
  calibration: 'solscan_calibration_dataset_v1',
};
const MAX_SESSIONS       = 100;   // cap stored scan sessions
const MAX_TRACKED_TOKENS = 800;   // cap stored tracking records
const MAX_CALIBRATION_RECORDS = 2000; // cap stored calibration dataset entries
// A token re-surfacing in a later scan/discovery pass within this window
// reuses its existing tracking record instead of creating a duplicate —
// otherwise a 20s auto-scan cadence would spam a new "detection" for the
// same coin every pass while it's still in the fresh-token window.
const TRACKING_DEDUP_WINDOW_MS = 65 * 60 * 1000; // slightly over the 1h checkpoint
const TRACKING_CHECK_INTERVAL_MS = 60 * 1000;    // how often to look for due checkpoints
const CHECKPOINT_MS = { m15: 15*60*1000, m30: 30*60*1000, h1: 60*60*1000 };

let currentSessionId = null;
let sessionsList      = [];   // [{ sessionId, startedAt, endedAt, filters, tokenAddresses, stats }]
let trackedTokens     = [];   // [{ tokenAddress, symbol, scoreAtDetection, ..., checkpoints, classification }]
let calibrationDataset = [];  // [{ timestamp, name, mint, score, breakdown, liquidity, mcap, top10Pct, devPct, isPumpFun, detectionAgeMinutes, accepted, rejectionReasons }]
let trackingTimer     = null;
let trackingViewMode  = 'tokens'; // 'tokens' | 'sessions'
let trackingSessionFilter = null; // sessionId to filter the tokens view by, or null for all

// =====================
// HISTORICAL PATTERN FEEDBACK LOOP (winner/loser profile)
// =====================
// Learns from tracking outcomes (EARLY_WIN/CONFIRMED_WIN/HIGH_CONVICTION_
// RUNNER vs FAILED_SETUP) and nudges the Alpha Score toward tokens whose
// detection-time factors resemble past winners, away from ones that
// resemble past failed setups. This sits ON TOP of the existing 8 scoring
// components (liquidity, volume, buy pressure, momentum, holder safety,
// mcap, contract safety, red flags) as a bounded adjustment — none of
// those components are removed or reweighted internally.
const WINNER_CLASSES = ['EARLY_WIN', 'CONFIRMED_WIN', 'HIGH_CONVICTION_RUNNER'];
const LOSER_CLASSES  = ['FAILED_SETUP'];
const MIN_PROFILE_SAMPLE   = 4;   // need at least this many winners AND losers before the pattern layer does anything beyond neutral
const FULL_CONFIDENCE_SAMPLE = 12; // sample size (per side) at which the pattern layer reaches full strength
const PATTERN_MAX_ADJUSTMENT = 15; // max +/- points the pattern layer can move the base score
let trackedTokensVersion = 0;      // bumped whenever a record's classification changes
let winnerLoserProfileCache = { version: -1, profile: null };

// =====================
// DEV LOGGING
// =====================
function devLog(msg, level) {
  const ts = new Date().toLocaleTimeString();
  const prefix = level === 'err' ? '[ERR]' : level === 'warn' ? '[WARN]' : '[LOG]';
  devLogs.unshift(ts + ' ' + prefix + ' ' + msg);
  if (devLogs.length > 200) devLogs.pop();
  if (devPanelOpen) renderDevLogs();
}

function renderDevLogs() {
  const el = document.getElementById('devLogs');
  if (!el) return;
  el.innerHTML = devLogs.map(function(l) {
    const color = l.indexOf('[ERR]') !== -1 ? 'var(--red)' : l.indexOf('[WARN]') !== -1 ? 'var(--yellow)' : 'var(--text3)';
    return '<div style="color:' + color + '">' + l + '</div>';
  }).join('');
}

function toggleDebug() {
  debugMode = !debugMode;
  const btn = document.getElementById('debugToggleBtn');
  btn.textContent = 'DEBUG: ' + (debugMode ? 'ON' : 'OFF');
  btn.classList.toggle('active', debugMode);
  const list = document.getElementById('debugList');
  list.classList.toggle('hidden', !debugMode);
  if (debugMode && allScannedTokens.length) renderDebugList();
}

function toggleDevPanel() {
  devPanelOpen = !devPanelOpen;
  const panel = document.getElementById('devPanel');
  const btn   = document.getElementById('devPanelBtn');
  panel.classList.toggle('hidden', !devPanelOpen);
  btn.classList.toggle('active', devPanelOpen);
  if (devPanelOpen) { renderDevLogs(); renderPipelineStats(); }
}

// =====================
// PERSISTENCE LAYER (localStorage)
// =====================
// All reads/writes are wrapped — private browsing, quota limits, or a
// disabled storage API degrade to "persistence off" rather than a crash.
function safeLSGet(key) {
  try {
    if (typeof localStorage === 'undefined') return null;
    const raw = localStorage.getItem(key);
    return raw ? JSON.parse(raw) : null;
  } catch(e) {
    devLog('localStorage read failed for '+key+': '+e.message, 'warn');
    return null;
  }
}
function safeLSSet(key, val) {
  try {
    if (typeof localStorage === 'undefined') return false;
    localStorage.setItem(key, JSON.stringify(val));
    return true;
  } catch(e) {
    devLog('localStorage write failed for '+key+' (quota or disabled): '+e.message, 'warn');
    return false;
  }
}

function persistSessions() {
  if (sessionsList.length > MAX_SESSIONS) sessionsList = sessionsList.slice(0, MAX_SESSIONS);
  safeLSSet(LS_KEYS.sessions, sessionsList);
}
function persistTrackedTokens() {
  pruneTrackedTokens();
  safeLSSet(LS_KEYS.tracked, trackedTokens);
}
function persistLastResults(results) {
  // Store enough of each token to fully re-render its card without
  // re-fetching anything, so a page refresh doesn't lose the last scan.
  safeLSSet(LS_KEYS.results, {
    savedAt: Date.now(),
    stats: {
      scanned: document.getElementById('statScanned')?.textContent,
      gems: document.getElementById('statGems')?.textContent,
      avg: document.getElementById('statAvg')?.textContent,
      time: document.getElementById('statTime')?.textContent,
    },
    tokens: results,
  });
}
function persistUIState() {
  safeLSSet(LS_KEYS.uiState, { ageFilterHours, scoreFilter, pumpOnly, autoScanOn, sortBy: document.getElementById('sortSelect')?.value });
}

function pruneTrackedTokens() {
  if (trackedTokens.length <= MAX_TRACKED_TOKENS) return;
  // Drop oldest fully-classified (non-PENDING) records first; only trim
  // into still-pending records if we're still over the cap after that.
  const done = trackedTokens.filter(r => r.classification && r.classification !== 'PENDING')
    .sort((a,b) => a.timestampDetected - b.timestampDetected);
  const pending = trackedTokens.filter(r => !r.classification || r.classification === 'PENDING');
  let overBy = trackedTokens.length - MAX_TRACKED_TOKENS;
  const doneKeep = done.slice(Math.min(overBy, done.length));
  trackedTokens = [...doneKeep, ...pending]
    .sort((a,b) => b.timestampDetected - a.timestampDetected)
    .slice(0, MAX_TRACKED_TOKENS);
}

function snapshotApiDiag() {
  return JSON.parse(JSON.stringify(apiDiag));
}

// Restore everything on page load: UI filter selections, last scan
// results (so a refresh doesn't lose the grid), tracked tokens, and
// session history. Does not alter any scoring/discovery/filter logic —
// it only re-hydrates state that logic already produced.
function restoreAppState() {
  try {
    const ui = safeLSGet(LS_KEYS.uiState);
    if (ui) {
      if (typeof ui.ageFilterHours === 'number') ageFilterHours = ui.ageFilterHours;
      if (typeof ui.scoreFilter === 'number') scoreFilter = ui.scoreFilter;
      if (typeof ui.pumpOnly === 'boolean') pumpOnly = ui.pumpOnly;
      restoreFilterButtonUI(ui);
      devLog('Restored UI filter state from previous session');
    }

    const savedSessions = safeLSGet(LS_KEYS.sessions);
    if (Array.isArray(savedSessions)) sessionsList = savedSessions;

    const savedTracked = safeLSGet(LS_KEYS.tracked);
    if (Array.isArray(savedTracked)) trackedTokens = savedTracked;
    devLog('Restored '+sessionsList.length+' session(s) and '+trackedTokens.length+' tracked token record(s) from localStorage');

    const savedCalibration = safeLSGet(LS_KEYS.calibration);
    if (Array.isArray(savedCalibration)) calibrationDataset = savedCalibration;
    devLog('Restored '+calibrationDataset.length+' calibration dataset record(s) from localStorage');

    const savedResults = safeLSGet(LS_KEYS.results);
    if (savedResults && Array.isArray(savedResults.tokens) && savedResults.tokens.length) {
      allResults = savedResults.tokens;
      applySortAndRender();
      if (savedResults.stats) {
        if (savedResults.stats.scanned != null) document.getElementById('statScanned').textContent = savedResults.stats.scanned;
        if (savedResults.stats.gems != null) document.getElementById('statGems').textContent = savedResults.stats.gems;
        if (savedResults.stats.avg != null) document.getElementById('statAvg').textContent = savedResults.stats.avg;
        if (savedResults.stats.time != null) document.getElementById('statTime').textContent = savedResults.stats.time;
      }
      devLog('Restored last scan results ('+allResults.length+' tokens) from '+new Date(savedResults.savedAt).toLocaleTimeString());
    }

    renderTrackingUI();
  } catch(e) {
    devLog('restoreAppState failed: '+e.message, 'err');
  }
}

// Reflect restored filter values onto the actual buttons/select so the
// UI matches the restored state (purely visual sync, no behavior change).
function restoreFilterButtonUI(ui) {
  const ageKeyByHours = { [5/60]:'5m', [15/60]:'15m', [30/60]:'30m', 1:'1h', 3:'3h', 6:'6h' };
  const ageKey = ageKeyByHours[ui.ageFilterHours];
  if (ageKey) {
    document.querySelectorAll('[id^="f-age-"]').forEach(b=>b.classList.remove('active'));
    document.getElementById('f-age-'+ageKey)?.classList.add('active');
  }
  if (typeof ui.scoreFilter === 'number') {
    document.querySelectorAll('[id^="f-score-"]').forEach(b=>b.classList.remove('active'));
    document.getElementById('f-score-'+ui.scoreFilter)?.classList.add('active');
  }
  const pumpBtn = document.getElementById('f-pump');
  if (pumpBtn) pumpBtn.classList.toggle('active', !!ui.pumpOnly);
  const sortSel = document.getElementById('sortSelect');
  if (sortSel && ui.sortBy) sortSel.value = ui.sortBy;
}

function resumeAutoScanIfNeeded() {
  const ui = safeLSGet(LS_KEYS.uiState);
  if (ui && ui.autoScanOn && !autoScanOn) {
    devLog('Resuming AUTO-SCAN — was ON before reload');
    toggleAutoScan();
  }
}

// =====================
// TOKEN OUTCOME TRACKING
// =====================
// Records every scored token (accepted or rejected — "processed" means
// it went through the scoring pipeline) and follows its price at 15m/
// 30m/1h to classify whether it played out. This never influences the
// Alpha Score, discovery, enrichment, or filters — it's a read-only
// observer sitting downstream of all of them.

function findActiveTrackingRecord(address) {
  const now = Date.now();
  return trackedTokens.find(r => r.tokenAddress === address && (now - r.timestampDetected) < TRACKING_DEDUP_WINDOW_MS);
}

function addTrackingRecord(t, sessionId) {
  if (!t.address) return null;
  const existing = findActiveTrackingRecord(t.address);
  if (existing) return existing; // already being tracked this window — don't duplicate

  const rec = {
    id: sessionId + '_' + t.address,
    tokenAddress: t.address,
    symbol: t.symbol || '?',
    name: t.name || '?',
    scoreAtDetection: t.alphaScore ?? null,
    liquidity: t.liquidity ?? null,
    volume: t.volume24h ?? null,
    priceAtDetection: parseFloat(t.priceUsd) || null,
    timestampDetected: Date.now(),
    scanSessionId: sessionId,
    checkpoints: { m15: null, m30: null, h1: null },
    classification: 'PENDING',
    maxRatioSeen: 1,
    rawPercentChangeFinal: 0,
    // Snapshot of every raw factor computeAlphaScore() reads, captured at
    // the moment of detection. Lets the pattern feedback loop (and the
    // scoring backtest) reconstruct exactly what the score saw then,
    // without re-fetching or guessing. Purely additive — nothing above
    // this changes shape or meaning.
    detectionFactors: {
      mcap: t.mcap ?? null,
      liquidity: t.liquidity ?? null,
      volume24h: t.volume24h ?? null,
      volume1h: t.volume1h ?? null,
      buys1h: t.buys1h ?? null, sells1h: t.sells1h ?? null,
      buys6h: t.buys6h ?? null, sells6h: t.sells6h ?? null,
      buysM5: t.buysM5 ?? null, sellsM5: t.sellsM5 ?? null,
      buys24h: t.buys24h ?? null, sells24h: t.sells24h ?? null,
      priceChange5m: t.priceChange5m ?? null,
      priceChange1h: t.priceChange1h ?? null,
      priceChange6h: t.priceChange6h ?? null,
      priceChange24h: t.priceChange24h ?? null,
      pairAgeHours: t.pairAgeHours ?? null,
      top10Pct: t.top10Pct ?? null,
      devPct: t.devPct ?? null,
      holderCount: t.holderCount ?? null,
      _holderDataAvailable: t._holderDataAvailable === true,
      mintAuthority: t.mintAuthority ?? false,
      freezeAuthority: t.freezeAuthority ?? false,
      narrative: t.narrative ?? null,
    },
  };
  trackedTokens.unshift(rec);
  return rec;
}

// Given the peak ratio observed so far (not just the latest snapshot —
// "reaches 1.5x/2x/3x within the tracking window" means the best point
// seen, even if it later cools off), plus the h1 checkpoint if present.
function classifyRecord(rec) {
  const maxRatio = rec.maxRatioSeen || 1;
  if (maxRatio >= 3.0) rec.classification = 'HIGH_CONVICTION_RUNNER';
  else if (maxRatio >= 2.0) rec.classification = 'CONFIRMED_WIN';
  else if (maxRatio >= 1.5) rec.classification = 'EARLY_WIN';
  else if (rec.checkpoints.h1) {
    // The four requested categories don't cover "flat/mildly up at 1h" —
    // NEUTRAL is an explicit addition so every finished record still gets
    // a label, not a scoring-model change.
    rec.classification = (rec.checkpoints.h1.pctChange != null && rec.checkpoints.h1.pctChange < 0)
      ? 'FAILED_SETUP' : 'NEUTRAL';
  } else {
    rec.classification = 'PENDING';
  }
  rec.rawPercentChangeFinal = Math.round((maxRatio - 1) * 1000) / 10;
  trackedTokensVersion++;
}

function isRecordDone(rec) {
  return !!(rec.checkpoints.h1) || (rec.classification && rec.classification !== 'PENDING' && rec.classification !== 'NEUTRAL' && !!rec.checkpoints.h1);
}

async function processTrackingCheckpoints() {
  const now = Date.now();
  const due = [];
  for (const rec of trackedTokens) {
    if (rec.checkpoints.h1) continue; // fully tracked, nothing left to check
    const ageMs = now - rec.timestampDetected;
    if ((!rec.checkpoints.m15 && ageMs >= CHECKPOINT_MS.m15) ||
        (!rec.checkpoints.m30 && ageMs >= CHECKPOINT_MS.m30) ||
        (!rec.checkpoints.h1  && ageMs >= CHECKPOINT_MS.h1)) {
      due.push(rec);
    }
  }
  if (due.length === 0) return;

  devLog('Tracking: '+due.length+' checkpoint(s) due, fetching current prices...');
  const addrs = [...new Set(due.map(r => r.tokenAddress))];
  let pairs = [];
  try {
    pairs = await fetchDexScreenerBatch(addrs); // reuses the existing DexScreener batch helper — no new API source
  } catch(e) {
    devLog('Tracking checkpoint price fetch failed: '+e.message, 'warn');
    return;
  }

  const byAddr = new Map();
  for (const p of pairs) {
    const key = p.baseToken?.address;
    if (!key) continue;
    const existing = byAddr.get(key);
    if (!existing || (p.liquidity?.usd||0) > (existing.liquidity?.usd||0)) byAddr.set(key, p);
  }

  for (const rec of due) {
    const p = byAddr.get(rec.tokenAddress);
    const currentPrice = p ? parseFloat(p.priceUsd) : null;
    const ageMs = now - rec.timestampDetected;
    const pctChange = (currentPrice != null && rec.priceAtDetection)
      ? Math.round(((currentPrice / rec.priceAtDetection) - 1) * 10000) / 100
      : null;
    const cpData = { rawPrice: currentPrice, pctChange, capturedAt: now };

    if (!rec.checkpoints.m15 && ageMs >= CHECKPOINT_MS.m15) rec.checkpoints.m15 = cpData;
    if (!rec.checkpoints.m30 && ageMs >= CHECKPOINT_MS.m30) rec.checkpoints.m30 = cpData;
    if (!rec.checkpoints.h1 && ageMs >= CHECKPOINT_MS.h1) rec.checkpoints.h1 = cpData;

    if (currentPrice != null && rec.priceAtDetection) {
      rec.maxRatioSeen = Math.max(rec.maxRatioSeen || 1, currentPrice / rec.priceAtDetection);
    }
    classifyRecord(rec);
  }

  persistTrackedTokens();
  renderTrackingUI();
}

function startTrackingTimer() {
  if (trackingTimer) return;
  trackingTimer = setInterval(processTrackingCheckpoints, TRACKING_CHECK_INTERVAL_MS);
  devLog('Tracking checkpoint timer started (every '+(TRACKING_CHECK_INTERVAL_MS/1000)+'s)');
}

// =====================
// TRACKING UI (read-only observability view)
// =====================
const CLASSIFICATION_STYLE = {
  HIGH_CONVICTION_RUNNER: { color: '#00ff88', label: 'HIGH CONVICTION RUNNER (3x+)' },
  CONFIRMED_WIN:          { color: '#00e5ff', label: 'CONFIRMED WIN (2x+)' },
  EARLY_WIN:              { color: '#fbbf24', label: 'EARLY WIN (1.5x+)' },
  FAILED_SETUP:           { color: '#ff3b5c', label: 'FAILED SETUP' },
  NEUTRAL:                { color: '#94a3b8', label: 'NEUTRAL' },
  PENDING:                { color: '#475569', label: 'PENDING' },
};

function setTrackingView(mode) {
  trackingViewMode = mode;
  document.getElementById('trkViewTokens').classList.toggle('active', mode==='tokens');
  document.getElementById('trkViewSessions').classList.toggle('active', mode==='sessions');
  renderTrackingUI();
}

function clearTrackingSessionFilter() {
  trackingSessionFilter = null;
  renderTrackingUI();
}

function clearTrackingData() {
  trackedTokens = [];
  sessionsList = [];
  trackingSessionFilter = null;
  trackedTokensVersion++;
  safeLSSet(LS_KEYS.tracked, trackedTokens);
  safeLSSet(LS_KEYS.sessions, sessionsList);
  devLog('Tracking data cleared by user', 'warn');
  const bp = document.getElementById('backtestPanel');
  if (bp) bp.classList.add('hidden');
  renderTrackingUI();
}

// =====================
// SCORING BACKTEST (verification, read-only)
// =====================
// For every finished tracked token (winner or failed setup), recomputes
// its score with the CURRENT formula — leave-one-out, so a record is
// never compared against a winner/loser profile that includes its own
// outcome — and compares it against the score actually stored at
// detection time. Shows before vs. after, why it moved, and whether
// winners now separate from losers better. Does not modify any
// tracking record, session, or live scoring behavior.
function runScoringBacktest() {
  const panel = document.getElementById('backtestPanel');
  const records = trackedTokens.filter(r => r.detectionFactors &&
    (WINNER_CLASSES.includes(r.classification) || LOSER_CLASSES.includes(r.classification)));

  if (records.length < MIN_PROFILE_SAMPLE * 2) {
    panel.classList.remove('hidden');
    panel.innerHTML = '<div style="color:var(--text3);letter-spacing:0.1em;font-size:9px;margin-bottom:8px;">SCORING BACKTEST</div>' +
      '<div style="color:var(--yellow)">Not enough finished tracking history yet ('+records.length+' winner/loser record'+(records.length===1?'':'s')+', need at least '+(MIN_PROFILE_SAMPLE*2)+'). '+
      'Keep AUTO-SCAN or LIVE DISCOVERY running — this fills in automatically as tokens finish their 1-hour tracking window.</div>';
    return;
  }

  const rows = records.map(r => {
    const before = r.scoreAtDetection ?? 0;
    const newResult = computeAlphaScore({ ...r.detectionFactors, _backtestExcludeId: r.id });
    return {
      symbol: r.symbol, classification: r.classification,
      before, after: newResult.total, delta: newResult.total - before,
      pattern: newResult.pattern,
      isWinner: WINNER_CLASSES.includes(r.classification),
    };
  });

  function statsFor(key) {
    const winners = rows.filter(r=>r.isWinner).map(r=>r[key]);
    const losers  = rows.filter(r=>!r.isWinner).map(r=>r[key]);
    const avg = a => a.length ? a.reduce((x,y)=>x+y,0)/a.length : 0;
    let concordant = 0, total = 0;
    for (const w of winners) for (const l of losers) { total++; if (w > l) concordant++; else if (w === l) concordant += 0.5; }
    return { winnerAvg: avg(winners), loserAvg: avg(losers), separation: avg(winners)-avg(losers), concordance: total ? concordant/total : null };
  }

  const beforeStats = statsFor('before');
  const afterStats  = statsFor('after');
  const better = afterStats.separation > beforeStats.separation;

  const tableRows = rows
    .slice().sort((a,b) => Math.abs(b.delta) - Math.abs(a.delta)).slice(0, 40)
    .map(r => {
      const deltaColor = r.delta > 0 ? 'var(--green)' : r.delta < 0 ? 'var(--red)' : 'var(--text3)';
      const clsColor = r.isWinner ? 'var(--green)' : 'var(--red)';
      const why = r.pattern.sampleSize.winners >= MIN_PROFILE_SAMPLE
        ? (r.pattern.matchScore > 0.1 ? 'matched winner pattern' : r.pattern.matchScore < -0.1 ? 'matched loser pattern' : 'neutral pattern match')
        : 'insufficient history for pattern layer';
      return '<div style="display:grid;grid-template-columns:1.2fr 1fr 0.6fr 0.6fr 0.6fr 1.6fr;gap:8px;padding:4px 0;border-bottom:1px solid var(--border);">' +
        '<div>'+r.symbol+'</div>' +
        '<div style="color:'+clsColor+'">'+r.classification+'</div>' +
        '<div>'+r.before+'</div>' +
        '<div>'+r.after+'</div>' +
        '<div style="color:'+deltaColor+'">'+(r.delta>0?'+':'')+r.delta+'</div>' +
        '<div style="color:var(--text3)">'+why+' ('+(r.pattern.adjustment>0?'+':'')+r.pattern.adjustment+' pts)</div>' +
      '</div>';
    }).join('');

  panel.classList.remove('hidden');
  panel.innerHTML =
    '<div style="color:var(--text3);letter-spacing:0.1em;font-size:9px;margin-bottom:10px;">SCORING BACKTEST — '+rows.length+' finished tracked tokens</div>' +
    '<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:12px;">' +
      '<div class="sum-stat"><div class="sum-stat-val">'+beforeStats.winnerAvg.toFixed(1)+' vs '+beforeStats.loserAvg.toFixed(1)+'</div><div class="sum-stat-label">BEFORE: WIN AVG vs LOSE AVG</div></div>' +
      '<div class="sum-stat"><div class="sum-stat-val">'+afterStats.winnerAvg.toFixed(1)+' vs '+afterStats.loserAvg.toFixed(1)+'</div><div class="sum-stat-label">AFTER: WIN AVG vs LOSE AVG</div></div>' +
      '<div class="sum-stat"><div class="sum-stat-val" style="color:'+(better?'var(--green)':'var(--red)')+'">'+beforeStats.separation.toFixed(1)+' \u2192 '+afterStats.separation.toFixed(1)+'</div><div class="sum-stat-label">SEPARATION (WIN \u2212 LOSE AVG)</div></div>' +
      '<div class="sum-stat"><div class="sum-stat-val" style="color:'+(better?'var(--green)':'var(--yellow)')+'">'+
        (beforeStats.concordance!=null?Math.round(beforeStats.concordance*100):'--')+'% \u2192 '+(afterStats.concordance!=null?Math.round(afterStats.concordance*100):'--')+'%</div><div class="sum-stat-label">CONCORDANCE (WINNER &gt; LOSER)</div></div>' +
    '</div>' +
    '<div style="color:'+(better?'var(--green)':'var(--yellow)')+';margin-bottom:10px;">'+
      (better
        ? 'The new score separates winners from losers better than the old score on this tracked history.'
        : 'The new score does not yet clearly separate winners from losers better than the old score on this tracked history — likely still a small sample; separation improves as more outcomes are tracked.') +
    '</div>' +
    '<div style="display:grid;grid-template-columns:1.2fr 1fr 0.6fr 0.6fr 0.6fr 1.6fr;gap:8px;padding-bottom:4px;border-bottom:1px solid var(--border2);color:var(--text3);font-size:9px;">'+
      '<div>TOKEN</div><div>OUTCOME</div><div>BEFORE</div><div>NEW</div><div>\u0394</div><div>WHY</div>'+
    '</div>' +
    tableRows;
}

function showSessionTokens(sessionId) {
  trackingSessionFilter = sessionId;
  setTrackingView('tokens');
}

function renderTrackingUI() {
  const totalEl = document.getElementById('trkTotal');
  if (!totalEl) return; // page not in DOM yet on very first paint

  const counts = { HIGH_CONVICTION_RUNNER:0, CONFIRMED_WIN:0, EARLY_WIN:0, FAILED_SETUP:0, NEUTRAL:0, PENDING:0 };
  trackedTokens.forEach(r => { counts[r.classification||'PENDING'] = (counts[r.classification||'PENDING']||0)+1; });
  document.getElementById('trkTotal').textContent = trackedTokens.length;
  document.getElementById('trkEarly').textContent = counts.EARLY_WIN;
  document.getElementById('trkConfirmed').textContent = counts.CONFIRMED_WIN;
  document.getElementById('trkRunner').textContent = counts.HIGH_CONVICTION_RUNNER;
  document.getElementById('trkFailed').textContent = counts.FAILED_SETUP;

  const badge = document.getElementById('trkSessionFilterBadge');
  if (trackingSessionFilter) {
    badge.classList.remove('hidden');
    document.getElementById('trkSessionFilterLabel').textContent = trackingSessionFilter;
  } else {
    badge.classList.add('hidden');
  }

  const content = document.getElementById('trackingContent');
  if (trackingViewMode === 'sessions') {
    content.innerHTML = buildSessionsListHtml();
  } else {
    content.innerHTML = buildTrackedTokensListHtml();
  }
}

function buildSessionsListHtml() {
  if (sessionsList.length === 0) {
    return '<div class="empty-state">No scan sessions recorded yet. Run SCAN NOW or enable AUTO-SCAN.</div>';
  }
  return sessionsList.map(s => {
    const started = new Date(s.startedAt).toLocaleTimeString();
    const dur = s.endedAt ? ((s.endedAt-s.startedAt)/1000).toFixed(1)+'s' : 'in progress';
    return '<div class="debug-card" style="cursor:pointer" onclick="showSessionTokens(\''+s.sessionId+'\')">'+
      '<div class="debug-card-top">'+
        '<span class="debug-name">'+s.sessionId+'</span>'+
        '<span style="font-family:var(--mono);font-size:9px;color:var(--text3)">'+started+' · '+dur+'</span>'+
      '</div>'+
      '<div class="debug-grid">'+
        '<div>Processed: <span style="color:var(--text2)">'+(s.stats?.processed??'—')+'</span></div>'+
        '<div>Accepted: <span style="color:var(--green)">'+(s.stats?.accepted??'—')+'</span></div>'+
        '<div>Rejected: <span style="color:var(--red)">'+(s.stats?.rejected??'—')+'</span></div>'+
        '<div>Avg Score: <span style="color:var(--text2)">'+(s.stats?.avgScore??'—')+'</span></div>'+
        '<div>Tokens Tracked: <span style="color:var(--text2)">'+(s.tokenAddresses?.length??0)+'</span></div>'+
        '<div>Filters: <span style="color:var(--text2)">age≤'+(s.filters?.ageFilterHours??'—')+'h, score≥'+(s.filters?.scoreFilter??'—')+'</span></div>'+
      '</div>'+
    '</div>';
  }).join('');
}

function buildTrackedTokensListHtml() {
  let rows = trackingSessionFilter
    ? trackedTokens.filter(r => r.scanSessionId === trackingSessionFilter)
    : trackedTokens;
  if (rows.length === 0) {
    return '<div class="empty-state">No tracked tokens yet. Run a scan to start tracking outcomes.</div>';
  }
  rows = [...rows].sort((a,b) => b.timestampDetected - a.timestampDetected).slice(0, 200);
  return rows.map(r => buildTrackedTokenCard(r)).join('');
}

function cpLabel(cp) {
  if (!cp) return '<span style="color:var(--text3)">pending</span>';
  const col = cp.pctChange == null ? 'var(--text3)' : (cp.pctChange >= 0 ? 'var(--green)' : 'var(--red)');
  return '<span style="color:'+col+'">'+(cp.pctChange==null?'N/A':(cp.pctChange>=0?'+':'')+cp.pctChange.toFixed(1)+'%')+'</span>';
}

function buildTrackedTokenCard(r) {
  const style = CLASSIFICATION_STYLE[r.classification] || CLASSIFICATION_STYLE.PENDING;
  return '<div class="debug-card" style="border-left-color:'+style.color+'">'+
    '<div class="debug-card-top">'+
      '<span class="debug-name">'+(r.name||'?')+' <span style="color:var(--text3)">'+(r.symbol||'')+'</span></span>'+
      '<span class="debug-score" style="color:'+style.color+'">'+style.label+'</span>'+
    '</div>'+
    '<div class="debug-grid">'+
      '<div>Score @ detect: <span style="color:var(--text2)">'+(r.scoreAtDetection??'N/A')+'</span></div>'+
      '<div>Price @ detect: <span style="color:var(--text2)">$'+(r.priceAtDetection!=null?parseFloat(r.priceAtDetection).toFixed(8):'N/A')+'</span></div>'+
      '<div>Liquidity: <span style="color:var(--text2)">'+fmt(r.liquidity,'$')+'</span></div>'+
      '<div>Volume: <span style="color:var(--text2)">'+fmt(r.volume,'$')+'</span></div>'+
      '<div>Detected: <span style="color:var(--text2)">'+new Date(r.timestampDetected).toLocaleTimeString()+'</span></div>'+
      '<div>Session: <span style="color:var(--text2)">'+r.scanSessionId+'</span></div>'+
    '</div>'+
    '<div class="debug-checks" style="font-family:var(--mono);font-size:9px;color:var(--text3);gap:14px;">'+
      '<span>15m: '+cpLabel(r.checkpoints.m15)+'</span>'+
      '<span>30m: '+cpLabel(r.checkpoints.m30)+'</span>'+
      '<span>1h: '+cpLabel(r.checkpoints.h1)+'</span>'+
      '<span>Peak: <span style="color:var(--accent)">'+(r.rawPercentChangeFinal>=0?'+':'')+r.rawPercentChangeFinal+'%</span></span>'+
    '</div>'+
  '</div>';
}


// =====================
// TABS
// =====================
function showTab(tab) {
  ['scanner','analyze','alerts','tracking'].forEach(t => {
    document.getElementById('page-'+t).classList.toggle('hidden', t!==tab);
  });
  document.querySelectorAll('.tab').forEach((el,i) => {
    const tabs = ['scanner','analyze','alerts','tracking'];
    el.classList.toggle('active', tabs[i]===tab);
  });
  if (tab === 'alerts') clearBadge();
  if (tab === 'tracking') renderTrackingUI();
}

// =====================
// FILTERS
// =====================
function setAgeFilter(val, btn) {
  const map = { '5m': 5/60, '15m': 15/60, '30m': 30/60, '1h': 1, '3h': 3, '6h': 6 };
  ageFilterHours = map[val] ?? 6;
  document.querySelectorAll('[id^="f-age-"]').forEach(b=>b.classList.remove('active'));
  btn.classList.add('active');
  persistUIState();
}

function setScoreFilter(val, btn) {
  scoreFilter = val;
  document.querySelectorAll('[id^="f-score-"]').forEach(b=>b.classList.remove('active'));
  btn.classList.add('active');
  applySortAndRender();
  persistUIState();
}

function togglePump(btn) {
  pumpOnly = !pumpOnly;
  btn.classList.toggle('active', pumpOnly);
  persistUIState();
}

// =====================
// FORMAT
// =====================
function fmt(n, prefix='') {
  if (n==null||isNaN(n)) return 'N/A';
  if (n>=1e9) return prefix+(n/1e9).toFixed(2)+'B';
  if (n>=1e6) return prefix+(n/1e6).toFixed(2)+'M';
  if (n>=1e3) return prefix+(n/1e3).toFixed(1)+'K';
  return prefix+parseFloat(n).toFixed(4);
}

function fmtAge(h) {
  if (h==null) return 'N/A';
  if (h<1/60) return Math.round(h*3600)+'s';
  if (h<1) return Math.round(h*60)+'m';
  return h.toFixed(1)+'h';
}

function fmtPct(n) {
  if (n==null) return 'N/A';
  return (n>0?'+':'')+n.toFixed(2)+'%';
}

function sleep(ms) { return new Promise(r=>setTimeout(r,ms)); }

// =====================
// NARRATIVE CLASSIFIER
// =====================
function classifyNarrative(name, sym) {
  const s = ((name||'')+(sym||'')).toLowerCase();
  if (/ai|gpt|neural|agent|llm|agi|robot|bot/.test(s)) return 'AI';
  if (/game|play|quest|arena|battle|rpg|nft|pixel/.test(s)) return 'GAMING';
  if (/defi|swap|yield|lend|vault|farm|pool/.test(s)) return 'DEFI';
  if (/rwa|real|estate|gold|silver|asset/.test(s)) return 'RWA';
  if (/pin|sensor|iot|network|node|infra/.test(s)) return 'DEPIN';
  if (/social|chat|friend|dao|vote|gov/.test(s)) return 'SOCIAL';
  if (/pay|cash|send|transfer|wallet/.test(s)) return 'PAYMENTS';
  if (/priv|anon|mask|zero|zk/.test(s)) return 'PRIVACY';
  if (/dog|cat|pepe|frog|ape|monkey|shib|bonk|wif|mog|popcat/.test(s)) return 'MEME';
  return 'MEME';
}

// =====================
// HISTORICAL PATTERN FEEDBACK LOOP
// =====================
// Learns from TRACKING outcomes and produces a bounded adjustment plus a
// confidence readout. Fully additive on top of the 8 components below —
// it never edits their weights or logic, only nudges the final total.

// Mirrors the bestTxWindow() picker inside computeAlphaScore, exposed at
// module scope so the pattern layer (and backtesting) can reuse it on
// any d-shaped object, live or stored.
function pickBestTxWindowFor(d) {
  if (d.buys1h != null || d.sells1h != null)   return { buys: d.buys1h||0,  sells: d.sells1h||0 };
  if (d.buys6h != null || d.sells6h != null)   return { buys: d.buys6h||0,  sells: d.sells6h||0 };
  if (d.buysM5 != null || d.sellsM5 != null)   return { buys: d.buysM5||0,  sells: d.sellsM5||0 };
  if (d.buys24h != null || d.sells24h != null) return { buys: d.buys24h||0, sells: d.sells24h||0 };
  return { buys: 0, sells: 0 };
}

// The requested factor list, computed identically for live scoring data
// and for stored detectionFactors snapshots. Note: "holder growth" is
// requested but no data source here provides holder counts over time
// (DexScreener doesn't expose holders at all; polling Birdeye per
// tracked token per checkpoint would multiply the enrichment API budget
// this project explicitly locks) — holderCount is therefore a
// detection-time snapshot, not a growth rate. Documented rather than
// silently approximated.
function extractPatternFactors(d) {
  const liq = d.liquidity || 0;
  const mcap = d.mcap || 0;
  const win = pickBestTxWindowFor(d);
  const totalTx = win.buys + win.sells;
  const m5Total = (d.buysM5||0) + (d.sellsM5||0);
  const h1Total = (d.buys1h||0) + (d.sells1h||0);
  return {
    liquidity:     liq > 0 ? liq : null,
    volLiqRatio:   (liq > 0 && d.volume24h != null) ? d.volume24h / liq : null,
    buySellRatio:  totalTx >= 3 ? win.buys / Math.max(win.sells, 1) : null,
    txAcceleration:(m5Total > 0 && h1Total >= 5) ? (m5Total * 12) / h1Total : null,
    pairAgeHours:  d.pairAgeHours != null ? d.pairAgeHours : null,
    mcap:          mcap > 0 ? mcap : null,
    priceChange5m: d.priceChange5m != null ? d.priceChange5m : null,
    priceChange1h: d.priceChange1h != null ? d.priceChange1h : null,
    holderCount:   d.holderCount != null ? d.holderCount : null,
    narrative:     d.narrative || null,
  };
}

const PATTERN_NUMERIC_FACTORS = ['liquidity','volLiqRatio','buySellRatio','txAcceleration','pairAgeHours','mcap','priceChange5m','priceChange1h','holderCount'];

function median(arr) {
  const a = [...arr].sort((x,y)=>x-y);
  const n = a.length;
  if (n === 0) return null;
  const mid = Math.floor(n/2);
  return n % 2 ? a[mid] : (a[mid-1]+a[mid])/2;
}
function medianAbsDeviation(arr, med) {
  if (arr.length === 0) return 0;
  return median(arr.map(x => Math.abs(x - med))) || 0;
}

// Median + MAD instead of mean/stddev — robust to the outliers a small,
// early-stage tracking sample will inevitably contain.
function buildProfileFromRecords(records) {
  const numeric = {};
  for (const key of PATTERN_NUMERIC_FACTORS) {
    const vals = records.map(r => r.factors[key]).filter(v => v != null && isFinite(v));
    if (vals.length === 0) { numeric[key] = null; continue; }
    const med = median(vals);
    const mad = medianAbsDeviation(vals, med);
    numeric[key] = { median: med, spread: Math.max(mad, Math.abs(med)*0.15, 1e-6) };
  }
  const narrativeCounts = {};
  for (const r of records) {
    if (r.factors.narrative) narrativeCounts[r.factors.narrative] = (narrativeCounts[r.factors.narrative]||0) + 1;
  }
  return { numeric, narrativeCounts, sampleSize: records.length };
}

function getTerminalRecords(excludeId) {
  return trackedTokens
    .filter(r => r.id !== excludeId && r.detectionFactors)
    .filter(r => WINNER_CLASSES.includes(r.classification) || LOSER_CLASSES.includes(r.classification))
    .map(r => ({ id: r.id, isWinner: WINNER_CLASSES.includes(r.classification), factors: extractPatternFactors(r.detectionFactors) }));
}

// excludeId set => always a fresh, uncached leave-one-out build (used
// only when backtesting that specific historical record, so it's never
// judged against a profile that includes its own outcome).
function buildWinnerLoserProfile(excludeId) {
  if (excludeId) {
    const records = getTerminalRecords(excludeId);
    return {
      winners: buildProfileFromRecords(records.filter(r=>r.isWinner)),
      losers:  buildProfileFromRecords(records.filter(r=>!r.isWinner)),
    };
  }
  if (winnerLoserProfileCache.version === trackedTokensVersion && winnerLoserProfileCache.profile) {
    return winnerLoserProfileCache.profile;
  }
  const records = getTerminalRecords(null);
  const profile = {
    winners: buildProfileFromRecords(records.filter(r=>r.isWinner)),
    losers:  buildProfileFromRecords(records.filter(r=>!r.isWinner)),
  };
  winnerLoserProfileCache = { version: trackedTokensVersion, profile };
  return profile;
}

// Roughly -1 (resembles losers) .. +1 (resembles winners). Missing data
// on a factor is always neutral (0) — never penalized for what we don't
// know. This is a similarity measure, not a "bigger number is better"
// rule, which is what keeps high-volume-alone from inflating the score.
function factorAffinity(value, winnerStat, loserStat) {
  if (value == null || !isFinite(value) || !winnerStat) return 0;
  const distWinner = Math.abs(value - winnerStat.median) / winnerStat.spread;
  if (!loserStat) return Math.max(-1, 1 - distWinner);
  const distLoser = Math.abs(value - loserStat.median) / winnerStat.spread;
  return Math.max(-1, Math.min(1, (distLoser - distWinner) / (distLoser + distWinner + 1)));
}

function narrativeAffinity(narrative, winnerProfile, loserProfile) {
  if (!narrative) return 0;
  const wTotal = winnerProfile.sampleSize || 0, lTotal = loserProfile.sampleSize || 0;
  if (wTotal + lTotal < MIN_PROFILE_SAMPLE) return 0;
  const wShare = wTotal ? (winnerProfile.narrativeCounts[narrative]||0) / wTotal : 0;
  const lShare = lTotal ? (loserProfile.narrativeCounts[narrative]||0) / lTotal : 0;
  if (wShare === 0 && lShare === 0) return 0;
  return Math.max(-1, Math.min(1, (wShare - lShare) * 2));
}

// excludeId: pass a tracking record id for leave-one-out backtesting;
// omit for live scoring of a brand-new, untracked token.
function computePatternMatch(d, excludeId) {
  const profile = buildWinnerLoserProfile(excludeId || null);
  const wCount = profile.winners.sampleSize, lCount = profile.losers.sampleSize;
  const sampleFloor = Math.min(wCount, lCount);
  if (sampleFloor < MIN_PROFILE_SAMPLE) {
    return { adjustment: 0, matchScore: 0, confidence: 0, sampleSize: { winners: wCount, losers: lCount }, factors: [] };
  }
  const factors = extractPatternFactors(d);
  const details = [];
  let sum = 0, count = 0;
  for (const key of PATTERN_NUMERIC_FACTORS) {
    const val = factors[key];
    const aff = factorAffinity(val, profile.winners.numeric[key], profile.losers.numeric[key]);
    if (val != null) { sum += aff; count++; details.push({ factor: key, value: val, affinity: aff }); }
  }
  const narAff = narrativeAffinity(factors.narrative, profile.winners, profile.losers);
  if (factors.narrative) { sum += narAff; count++; details.push({ factor: 'narrative', value: factors.narrative, affinity: narAff }); }

  const matchScore = count > 0 ? sum / count : 0;
  const confidence = Math.max(0, Math.min(1, (sampleFloor - MIN_PROFILE_SAMPLE) / (FULL_CONFIDENCE_SAMPLE - MIN_PROFILE_SAMPLE)));
  const adjustment = Math.round(matchScore * PATTERN_MAX_ADJUSTMENT * confidence);
  return { adjustment, matchScore, confidence, sampleSize: { winners: wCount, losers: lCount }, factors: details };
}

// Empirical win rate among finished tracking records that landed a
// similar base score — used as one input to the confidence rating, only
// once there's a real sample to draw from.
function empiricalWinRateNearScore(score) {
  const band = trackedTokens.filter(r => {
    if (r.scoreAtDetection == null) return false;
    if (!(WINNER_CLASSES.includes(r.classification) || LOSER_CLASSES.includes(r.classification) || r.classification === 'NEUTRAL')) return false;
    return Math.abs(r.scoreAtDetection - score) <= 15;
  });
  if (band.length < 6) return null;
  const wins = band.filter(r => WINNER_CLASSES.includes(r.classification)).length;
  return { rate: Math.round((wins/band.length)*100), sample: band.length };
}

function computeConfidence(scoreResult) {
  const { rawTotal, pattern, components } = scoreResult;
  const empirical = empiricalWinRateNearScore(rawTotal);
  let prob = empirical != null ? Math.round(empirical.rate*0.6 + rawTotal*0.4) : rawTotal;
  prob += Math.round(pattern.matchScore * 20 * pattern.confidence);
  prob = Math.max(2, Math.min(97, prob));

  let riskLevel;
  if (prob >= 70 && pattern.matchScore >= 0.2) riskLevel = 'LOW';
  else if (prob >= 50) riskLevel = 'MEDIUM';
  else if (prob >= 30) riskLevel = 'MEDIUM-HIGH';
  else riskLevel = 'HIGH';

  const ranked = Object.values(components).map(c => ({ label: c.label, ratio: c.max ? c.score/c.max : 0 })).sort((a,b)=>b.ratio-a.ratio);
  const strong = ranked.filter(c => c.ratio >= 0.6).slice(0,2).map(c=>c.label);
  const weak = ranked.filter(c => c.ratio < 0.35).slice(-2).map(c=>c.label);

  const parts = [];
  if (strong.length) parts.push('Strong: ' + strong.join(', '));
  if (weak.length) parts.push('Weak: ' + weak.join(', '));
  if (pattern.sampleSize.winners >= MIN_PROFILE_SAMPLE && pattern.sampleSize.losers >= MIN_PROFILE_SAMPLE) {
    if (pattern.matchScore > 0.15) parts.push('Resembles past winners ('+pattern.sampleSize.winners+' tracked)');
    else if (pattern.matchScore < -0.15) parts.push('Resembles past failed setups ('+pattern.sampleSize.losers+' tracked)');
    else parts.push('No strong resemblance to past winners or losers yet');
  } else {
    parts.push('Pattern learning: only '+pattern.sampleSize.winners+'W/'+pattern.sampleSize.losers+'L tracked so far (needs '+MIN_PROFILE_SAMPLE+'+ each)');
  }
  return { alphaProbability: prob, riskLevel, reason: parts.join(' — ') };
}

// =====================
// ALPHA SCORING ENGINE
// =====================
// Weights: Liquidity Health 16 | Volume Quality 16 | Buy Pressure 10 |
// Momentum & Trend 20 | Holder Safety 15 | MCap Range 8 |
// Contract Safety 5 | Red Flags (anti-wash/dead-vol) 10
// Total: 100
//
// Design goal: quality over quantity. Momentum & Trend and Red Flags
// carry the most weight because the point of this scanner is to find
// early tokens with *genuine* building momentum, not just tokens that
// happen to clear a liquidity/volume bar. Weak, noisy, or manipulated-
// looking tokens should score meaningfully lower than strong ones, even
// if their raw liquidity/volume numbers look superficially similar.
//
// --- CALIBRATION PHASE: every weight below is now a named constant so
// it can be tuned without touching the logic that uses it. Values are
// UNCHANGED from the previous version. Tier tables are used only where
// verified boundary-safe (Liquidity, Volume, Buy Pressure, Holder Count,
// Top-10%); Dev% and MCap keep their exact original if/else shape as
// scalar constants because their original logic has non-obvious
// boundaries (Dev% has a 20–35% "dead zone" that adds neither a bonus
// nor a penalty; MCap's first branch has a `mcap>0` guard that a naive
// tier table would silently drop, changing how mcap=0 scores) — those
// two would have broken by converting them to a generic tier lookup, so
// they intentionally were not converted, to guarantee zero behavior
// change. Verified byte-for-byte identical to the prior implementation.

// 1. LIQUIDITY HEALTH
const SCORE_LIQUIDITY_MAX = 16;
const SCORE_LIQUIDITY_TIERS = [   // checked in order, first match (liq >= min) wins
  { min: 50000, pts: 16 },
  { min: 25000, pts: 13 },
  { min: 15000, pts: 11 },
  { min: 8000,  pts: 8  },
  { min: 3000,  pts: 5  },
  { min: 1000,  pts: 2  },
  { min: 0,     pts: 0  },
];
const SCORE_LIQ_MCAP_RATIO_VERY_THIN = 0.02;  // liq/mcap below this = fragile pool
const SCORE_LIQ_MCAP_RATIO_VERY_THIN_MULT = 0.5;
const SCORE_LIQ_MCAP_RATIO_THIN = 0.05;
const SCORE_LIQ_MCAP_RATIO_THIN_MULT = 0.8;

// 2. VOLUME QUALITY
const SCORE_VOLUME_MAX = 16;
const SCORE_VOLUME_VM_TIERS = [   // vm = 24h volume / mcap
  { min: 5,    pts: 16   },
  { min: 2.5,  pts: 13.5 },
  { min: 1.2,  pts: 11   },
  { min: 0.6,  pts: 8    },
  { min: 0.2,  pts: 5    },
  { min: 0.05, pts: 2.5  },
  { min: 0,    pts: 0    },
];
const SCORE_VOLUME_TREND_GROWING_RATIO = 1.3;   // implied-daily-from-1h vs actual 24h
const SCORE_VOLUME_TREND_GROWING_MULT = 1.15;
const SCORE_VOLUME_TREND_FADING_RATIO = 0.5;
const SCORE_VOLUME_TREND_FADING_MULT = 0.6;

// 3. BUY PRESSURE
const SCORE_BUYPRESSURE_MAX = 10;
const SCORE_BUYPRESSURE_TIERS = [   // bsR = buys / sells in the best-available window
  { min: 3,   pts: 10  },
  { min: 2,   pts: 8   },
  { min: 1.3, pts: 6   },
  { min: 1.0, pts: 4   },
  { min: 0.7, pts: 2   },
  { min: 0,   pts: 0.5 },
];
const SCORE_BUYPRESSURE_LOWTX_VERY_LOW = 5;    // totalTx below this = ratio is noise
const SCORE_BUYPRESSURE_LOWTX_VERY_LOW_MULT = 0.4;
const SCORE_BUYPRESSURE_LOWTX_LOW = 10;
const SCORE_BUYPRESSURE_LOWTX_LOW_MULT = 0.7;
const SCORE_BUYPRESSURE_TXGROWTH_MIN_H1TOTAL = 5;  // need this many h1 trades to trust a growth ratio
const SCORE_BUYPRESSURE_TXGROWTH_RISING_RATIO = 1.5;
const SCORE_BUYPRESSURE_TXGROWTH_RISING_BONUS = 1.5;
const SCORE_BUYPRESSURE_TXGROWTH_FALLING_RATIO = 0.4;
const SCORE_BUYPRESSURE_TXGROWTH_FALLING_PENALTY = -1.5;

// 4. MOMENTUM & TREND
const SCORE_MOMENTUM_MAX = 20;
const SCORE_MOMENTUM_BASE_STRONG_P5 = 3;    const SCORE_MOMENTUM_BASE_STRONG_P1 = 5;    const SCORE_MOMENTUM_BASE_STRONG_PTS = 14;
const SCORE_MOMENTUM_BASE_GOOD_P1 = 2;      const SCORE_MOMENTUM_BASE_GOOD_PTS = 10;     // s5m > 0 && s1h > GOOD_P1
const SCORE_MOMENTUM_BASE_MILD_PTS = 7;      // s5m > 0 && s1h > 0
const SCORE_MOMENTUM_BASE_H1ONLY_PTS = 4;    // s1h > 0
const SCORE_MOMENTUM_BASE_FLAT_FLOOR = -3;   const SCORE_MOMENTUM_BASE_FLAT_PTS = 2;      // s5m > FLAT_FLOOR
const SCORE_MOMENTUM_BASE_NONE_PTS = 0;
const SCORE_MOMENTUM_FADE_BIG_P24 = 15;      const SCORE_MOMENTUM_FADE_BIG_PENALTY = -6;
const SCORE_MOMENTUM_FADE_MED_PENALTY = -3;  // elif p6h>0 && s1h < p6h/3 && s5m<=0
const SCORE_MOMENTUM_ACCEL_BONUS_1 = 4;      // p6h>0 && s1h>p6h && s5m>0
const SCORE_MOMENTUM_ACCEL_BONUS_2 = 2;      // s1h>0 && p6h>0 && p24h>0

// 5. HOLDER SAFETY
const SCORE_HOLDERSAFETY_MAX = 15;
const SCORE_HOLDERSAFETY_NEUTRAL_DEFAULT = 9; // used when Birdeye holder data is unavailable
const SCORE_HOLDER_TOP10_DEFAULT = 80;        // assumed-worst-case if holder data available but field missing
const SCORE_HOLDER_DEV_DEFAULT = 30;
const SCORE_HOLDERCOUNT_TIERS = [
  { min: 300, pts: 4 }, { min: 100, pts: 3 }, { min: 30, pts: 2 }, { min: 10, pts: 1 }, { min: 0, pts: 0 },
];
const SCORE_TOP10_TIERS = [   // lower top-10 concentration = better = more points; exhaustive ascending chain
  { max: 35, pts: 7 }, { max: 55, pts: 5 }, { max: 75, pts: 3 }, { max: Infinity, pts: 1 },
];
// Dev%/largest-holder tiers: kept as explicit constants (not a tier
// table) because the original has a 20–35% dead zone that adds neither
// bonus nor penalty (only "<8", "<20", and ">35" are checked — nothing
// covers 20–35 inclusive of the 35 boundary itself, since ">35" is
// strict). A generic ascending tier table would score devPct===35 as a
// penalty; the original does not.
const SCORE_DEV_LOW = 8;          const SCORE_DEV_LOW_BONUS = 4;
const SCORE_DEV_MED = 20;         const SCORE_DEV_MED_BONUS = 2;
const SCORE_DEV_HIGH = 35;        const SCORE_DEV_HIGH_PENALTY = -2; // fires only when devPct > SCORE_DEV_HIGH (strict)

// 6. MCAP RANGE — kept as explicit constants (not a tier table) because
// the first branch has a `mcap > 0` guard that only applies to itself;
// mcap === 0 falls through to the second branch (<150000) in the
// original, scoring 7, not 0 or 8. A naive tier table would not
// reproduce this.
const SCORE_MCAP_MAX = 8;
const SCORE_MCAP_T1 = 50000;   const SCORE_MCAP_T1_PTS = 8;
const SCORE_MCAP_T2 = 150000;  const SCORE_MCAP_T2_PTS = 7;
const SCORE_MCAP_T3 = 400000;  const SCORE_MCAP_T3_PTS = 5;
const SCORE_MCAP_T4 = 800000;  const SCORE_MCAP_T4_PTS = 2;
const SCORE_MCAP_T5_PTS = 0;

// 7. CONTRACT SAFETY
const SCORE_CONTRACT_MAX = 5;
const SCORE_CONTRACT_BASE = 5;
const SCORE_CONTRACT_MINT_PENALTY = -3;
const SCORE_CONTRACT_FREEZE_PENALTY = -2;
const SCORE_CONTRACT_LP_UNLOCKED_PENALTY = -3; // lpLockedPct known and below threshold
const SCORE_CONTRACT_LP_LOCK_THRESHOLD = 50;   // % — below this counts as "not locked enough"
const SCORE_REDFLAGS_RUGCHECK_PENALTY = -4;    // per RugCheck DANGER/HIGH risk flag
const SCORE_REDFLAGS_RUGCHECK_MAX_PENALTY = -12; // cap so one noisy source can't zero the component alone

// 8. RED FLAGS (anti-wash-trading / dead-volume)
const SCORE_REDFLAGS_MAX = 10;
const SCORE_REDFLAGS_BASE = 10;
const SCORE_REDFLAGS_TURNOVER_THRESHOLD = 50;      // 24h volume / liquidity
const SCORE_REDFLAGS_TURNOVER_PENALTY = -4;
const SCORE_REDFLAGS_THINPOOL_VM = 15;
const SCORE_REDFLAGS_THINPOOL_LIQ = 10000;
const SCORE_REDFLAGS_THINPOOL_PENALTY = -3;
const SCORE_REDFLAGS_DEADVOL_VOLUME = 500;
const SCORE_REDFLAGS_DEADVOL_AGE_HOURS = 1;
const SCORE_REDFLAGS_DEADVOL_PENALTY = -4;
const SCORE_REDFLAGS_OVERVALUED_RATIO = 50;        // mcap / liquidity
const SCORE_REDFLAGS_OVERVALUED_PENALTY = -3;
const SCORE_REDFLAGS_PUMPDUMP_P24 = 30;
const SCORE_REDFLAGS_PUMPDUMP_S1H = -10;
const SCORE_REDFLAGS_PUMPDUMP_PENALTY = -3;
const SCORE_REDFLAGS_NOACTIVITY_AGE_HOURS = 6;
const SCORE_REDFLAGS_NOACTIVITY_PENALTY = -2;

// Tier lookup for the 5 components verified boundary-safe above (clean,
// exhaustive, single-variable chains with no compound guards).
function tierLookupMin(value, tiers) {
  for (const t of tiers) if (value >= t.min) return t;
  return tiers[tiers.length - 1];
}
function tierLookupMax(value, tiers) {
  for (const t of tiers) if (value < t.max) return t;
  return tiers[tiers.length - 1];
}
// Ensures a component's listed reasons always sum to EXACTLY its final
// (rounded/clamped) score — required so the displayed breakdown total
// always equals the displayed score, even across rounding/floor/cap.
function reconcileReasons(reasons, finalScore) {
  const sum = reasons.reduce((s, r) => s + r.points, 0);
  const diff = Math.round((finalScore - sum) * 10) / 10;
  if (diff !== 0) reasons.push({ label: '(rounding/clamp adjustment)', points: diff });
}

function computeAlphaScore(d) {
  const components = {};
  let total = 0;
  const holderDataAvailable = d._holderDataAvailable === true;
  const mcap = d.mcap || 0;
  const liq  = d.liquidity || 0;

  // Best-available buy/sell window: prefer the tightest window that
  // actually has data so the ratio reflects *current* pressure, not a
  // stale 24h aggregate that may include activity from hours ago.
  function bestTxWindow() {
    if (d.buys1h != null || d.sells1h != null)   return { buys: d.buys1h||0,  sells: d.sells1h||0 };
    if (d.buys6h != null || d.sells6h != null)   return { buys: d.buys6h||0,  sells: d.sells6h||0 };
    if (d.buysM5 != null || d.sellsM5 != null)   return { buys: d.buysM5||0,  sells: d.sellsM5||0 };
    if (d.buys24h != null || d.sells24h != null) return { buys: d.buys24h||0, sells: d.sells24h||0 };
    return { buys: 0, sells: 0 };
  }
  const win = bestTxWindow();
  const totalTx = win.buys + win.sells;
  const vm = d.volume24h && mcap ? d.volume24h / mcap : 0;

  // ---------- 1. LIQUIDITY HEALTH ----------
  const liqReasons = [];
  const liqTier = tierLookupMin(liq, SCORE_LIQUIDITY_TIERS);
  let liqScore = liqTier.pts;
  liqReasons.push({ label: 'Liquidity ('+fmt(liq,'$')+')', points: liqScore });
  // Liquidity should be proportionate to the reported market cap. A
  // token with a large mcap sitting on a razor-thin pool is fragile —
  // easy to move, easy to rug — regardless of the raw liquidity figure.
  if (mcap > 0) {
    const liqToMcap = liq / mcap;
    const before = liqScore;
    if (liqToMcap < SCORE_LIQ_MCAP_RATIO_VERY_THIN) liqScore *= SCORE_LIQ_MCAP_RATIO_VERY_THIN_MULT;
    else if (liqToMcap < SCORE_LIQ_MCAP_RATIO_THIN) liqScore *= SCORE_LIQ_MCAP_RATIO_THIN_MULT;
    const delta = liqScore - before;
    if (delta !== 0) liqReasons.push({ label: 'Thin liquidity vs market cap', points: Math.round(delta*10)/10 });
  }
  liqScore = Math.round(Math.max(0, Math.min(SCORE_LIQUIDITY_MAX, liqScore)));
  reconcileReasons(liqReasons, liqScore);
  components.liquidity = { score: liqScore, max: SCORE_LIQUIDITY_MAX, label: 'Liquidity Health', reasons: liqReasons };
  total += liqScore;

  // ---------- 2. VOLUME QUALITY ----------
  const volReasons = [];
  const volTier = tierLookupMin(vm, SCORE_VOLUME_VM_TIERS);
  let volScore = volTier.pts;
  volReasons.push({ label: 'Volume/MCap ratio ('+vm.toFixed(2)+'x)', points: volScore });
  // Reward volume that's genuinely growing (recent hourly pace outpacing
  // the day's average) and penalize volume that's fading — a token that
  // pumped hours ago and has since gone quiet is a much weaker bet than
  // one still accelerating right now.
  if (d.volume1h != null && d.volume24h) {
    const impliedDaily = d.volume1h * 24;
    const trendRatio = impliedDaily / d.volume24h;
    const before = volScore;
    if (trendRatio > SCORE_VOLUME_TREND_GROWING_RATIO) volScore *= SCORE_VOLUME_TREND_GROWING_MULT;
    else if (trendRatio < SCORE_VOLUME_TREND_FADING_RATIO) volScore *= SCORE_VOLUME_TREND_FADING_MULT;
    const delta = volScore - before;
    if (delta !== 0) volReasons.push({ label: trendRatio > 1 ? 'Volume accelerating' : 'Volume fading', points: Math.round(delta*10)/10 });
  }
  volScore = Math.round(Math.max(0, Math.min(SCORE_VOLUME_MAX, volScore)));
  reconcileReasons(volReasons, volScore);
  components.volume = { score: volScore, max: SCORE_VOLUME_MAX, label: 'Volume Quality', reasons: volReasons };
  total += volScore;

  // ---------- 3. BUY PRESSURE ----------
  const bpReasons = [];
  const bsR = win.buys / Math.max(win.sells, 1);
  const bsTier = tierLookupMin(bsR, SCORE_BUYPRESSURE_TIERS);
  let bsScore = bsTier.pts;
  bpReasons.push({ label: 'Buy/sell ratio ('+bsR.toFixed(2)+'x)', points: bsScore });
  // Low transaction counts are noise, not signal. A 2:1 buy/sell ratio
  // from a handful of trades shouldn't score the same as a 2:1 ratio
  // built on hundreds of real trades — discount until there's enough
  // activity to trust the ratio.
  {
    const before = bsScore;
    if (totalTx < SCORE_BUYPRESSURE_LOWTX_VERY_LOW) bsScore *= SCORE_BUYPRESSURE_LOWTX_VERY_LOW_MULT;
    else if (totalTx < SCORE_BUYPRESSURE_LOWTX_LOW) bsScore *= SCORE_BUYPRESSURE_LOWTX_LOW_MULT;
    const delta = bsScore - before;
    if (delta !== 0) bpReasons.push({ label: 'Low transaction count ('+totalTx+' trades)', points: Math.round(delta*10)/10 });
  }
  // Transaction growth: is trading activity accelerating right now, or
  // dying off? Compare the last-5-minute trade rate (annualized to an
  // hourly pace) against the last-hour rate. A ratio holding up over a
  // handful of trades an hour ago means much less than the same ratio
  // with activity actively picking up.
  const m5Total = (d.buysM5||0) + (d.sellsM5||0);
  const h1Total = (d.buys1h||0) + (d.sells1h||0);
  if (m5Total > 0 && h1Total >= SCORE_BUYPRESSURE_TXGROWTH_MIN_H1TOTAL) {
    const txGrowth = (m5Total * 12) / h1Total;
    if (txGrowth >= SCORE_BUYPRESSURE_TXGROWTH_RISING_RATIO) { bsScore += SCORE_BUYPRESSURE_TXGROWTH_RISING_BONUS; bpReasons.push({ label: 'Transaction rate accelerating', points: SCORE_BUYPRESSURE_TXGROWTH_RISING_BONUS }); }
    else if (txGrowth < SCORE_BUYPRESSURE_TXGROWTH_FALLING_RATIO) { bsScore += SCORE_BUYPRESSURE_TXGROWTH_FALLING_PENALTY; bpReasons.push({ label: 'Transaction rate decelerating', points: SCORE_BUYPRESSURE_TXGROWTH_FALLING_PENALTY }); }
  }
  bsScore = Math.round(Math.max(0, Math.min(SCORE_BUYPRESSURE_MAX, bsScore)));
  reconcileReasons(bpReasons, bsScore);
  components.buySell = { score: bsScore, max: SCORE_BUYPRESSURE_MAX, label: 'Buy Pressure', reasons: bpReasons };
  total += bsScore;

  // ---------- 4. MOMENTUM & TREND ----------
  const momReasons = [];
  const p5m  = d.priceChange5m;
  const p1h  = d.priceChange1h;
  const p6h  = d.priceChange6h;
  const p24h = d.priceChange24h;
  const s5m = p5m ?? 0, s1h = p1h ?? 0;
  let momScore;
  if (s5m > SCORE_MOMENTUM_BASE_STRONG_P5 && s1h > SCORE_MOMENTUM_BASE_STRONG_P1) { momScore = SCORE_MOMENTUM_BASE_STRONG_PTS; momReasons.push({label:'Strong 5m+1h momentum', points: momScore}); }
  else if (s5m > 0 && s1h > SCORE_MOMENTUM_BASE_GOOD_P1) { momScore = SCORE_MOMENTUM_BASE_GOOD_PTS; momReasons.push({label:'Good 5m+1h momentum', points: momScore}); }
  else if (s5m > 0 && s1h > 0) { momScore = SCORE_MOMENTUM_BASE_MILD_PTS; momReasons.push({label:'Mild positive momentum', points: momScore}); }
  else if (s1h > 0) { momScore = SCORE_MOMENTUM_BASE_H1ONLY_PTS; momReasons.push({label:'1h positive only', points: momScore}); }
  else if (s5m > SCORE_MOMENTUM_BASE_FLAT_FLOOR) { momScore = SCORE_MOMENTUM_BASE_FLAT_PTS; momReasons.push({label:'Roughly flat', points: momScore}); }
  else { momScore = SCORE_MOMENTUM_BASE_NONE_PTS; momReasons.push({label:'Negative momentum', points: momScore}); }

  // Fading pump: was up big on the day/6h but is reversing right now —
  // this is a topping/dumping pattern, not a runner in the making.
  if (p24h != null && p24h > SCORE_MOMENTUM_FADE_BIG_P24 && (s1h < 0 || s5m < -2)) { momScore += SCORE_MOMENTUM_FADE_BIG_PENALTY; momReasons.push({label:'Fading after a big 24h pump', points: SCORE_MOMENTUM_FADE_BIG_PENALTY}); }
  else if (p6h != null && p6h > 0 && s1h < (p6h/3) && s5m <= 0) { momScore += SCORE_MOMENTUM_FADE_MED_PENALTY; momReasons.push({label:'Momentum cooling vs 6h pace', points: SCORE_MOMENTUM_FADE_MED_PENALTY}); }

  // Genuine acceleration: fresh strength building on top of an already
  // positive base across more than one timeframe — this is the pattern
  // that actually precedes a real move, not noise.
  if (p6h != null && p6h > 0 && s1h > p6h && s5m > 0) { momScore += SCORE_MOMENTUM_ACCEL_BONUS_1; momReasons.push({label:'Accelerating above 6h pace', points: SCORE_MOMENTUM_ACCEL_BONUS_1}); }
  if (s1h > 0 && p6h != null && p6h > 0 && p24h != null && p24h > 0) { momScore += SCORE_MOMENTUM_ACCEL_BONUS_2; momReasons.push({label:'Positive across all timeframes', points: SCORE_MOMENTUM_ACCEL_BONUS_2}); }

  momScore = Math.round(Math.max(0, Math.min(SCORE_MOMENTUM_MAX, momScore)));
  reconcileReasons(momReasons, momScore);
  components.momentum = { score: momScore, max: SCORE_MOMENTUM_MAX, label: 'Momentum & Trend', reasons: momReasons };
  total += momScore;

  // ---------- 5. HOLDER SAFETY ----------
  // If Birdeye holder data is available use it. If not, award a
  // neutral default so a token isn't punished for a Birdeye 404/429
  // that has nothing to do with token quality.
  const hdReasons = [];
  let hdScore = SCORE_HOLDERSAFETY_NEUTRAL_DEFAULT;
  if (!holderDataAvailable) {
    hdReasons.push({ label: 'Holder data unavailable (neutral default)', points: hdScore });
  } else {
    const top10pct = d.top10Pct != null ? d.top10Pct : SCORE_HOLDER_TOP10_DEFAULT;
    const devPct   = d.devPct   != null ? d.devPct   : SCORE_HOLDER_DEV_DEFAULT;
    const holders  = d.holderCount || 0;
    hdScore = 0;
    const hcTier = tierLookupMin(holders, SCORE_HOLDERCOUNT_TIERS);
    hdScore += hcTier.pts;
    if (hcTier.pts !== 0) hdReasons.push({ label: 'Holder count ('+holders+')', points: hcTier.pts });
    const t10Tier = tierLookupMax(top10pct, SCORE_TOP10_TIERS);
    hdScore += t10Tier.pts;
    hdReasons.push({ label: 'Top-10 holder concentration ('+top10pct+'%)', points: t10Tier.pts });
    if (devPct < SCORE_DEV_LOW) { hdScore += SCORE_DEV_LOW_BONUS; hdReasons.push({ label: 'Low developer concentration ('+devPct+'%)', points: SCORE_DEV_LOW_BONUS }); }
    else if (devPct < SCORE_DEV_MED) { hdScore += SCORE_DEV_MED_BONUS; hdReasons.push({ label: 'Moderate developer concentration ('+devPct+'%)', points: SCORE_DEV_MED_BONUS }); }
    else if (devPct > SCORE_DEV_HIGH) { hdScore += SCORE_DEV_HIGH_PENALTY; hdReasons.push({ label: 'High developer concentration ('+devPct+'%)', points: SCORE_DEV_HIGH_PENALTY }); }
    hdScore = Math.max(0, Math.min(SCORE_HOLDERSAFETY_MAX, hdScore));
  }
  reconcileReasons(hdReasons, Math.round(hdScore));
  hdScore = Math.round(hdScore);
  components.holderSafety = { score: hdScore, max: SCORE_HOLDERSAFETY_MAX, label: 'Holder Safety', reasons: hdReasons };
  total += hdScore;

  // ---------- 6. MCAP RANGE ----------
  // Steeper falloff than before — excessive market cap relative to how
  // early/small a token is gets explicitly penalized rather than just
  // trailing off slowly.
  const mcapReasons = [];
  let mcapScore = 0;
  if (mcap > 0 && mcap < SCORE_MCAP_T1)      { mcapScore = SCORE_MCAP_T1_PTS; mcapReasons.push({label:'MCap under '+fmt(SCORE_MCAP_T1,'$'), points: mcapScore}); }
  else if (mcap < SCORE_MCAP_T2)             { mcapScore = SCORE_MCAP_T2_PTS; mcapReasons.push({label:'MCap under '+fmt(SCORE_MCAP_T2,'$'), points: mcapScore}); }
  else if (mcap < SCORE_MCAP_T3)             { mcapScore = SCORE_MCAP_T3_PTS; mcapReasons.push({label:'MCap under '+fmt(SCORE_MCAP_T3,'$'), points: mcapScore}); }
  else if (mcap < SCORE_MCAP_T4)             { mcapScore = SCORE_MCAP_T4_PTS; mcapReasons.push({label:'MCap under '+fmt(SCORE_MCAP_T4,'$'), points: mcapScore}); }
  else { mcapScore = SCORE_MCAP_T5_PTS; mcapReasons.push({label:'MCap above '+fmt(SCORE_MCAP_T4,'$'), points: mcapScore}); }
  reconcileReasons(mcapReasons, mcapScore);
  components.mcap = { score: mcapScore, max: SCORE_MCAP_MAX, label: 'MCap Range', reasons: mcapReasons };
  total += mcapScore;

  // ---------- 7. CONTRACT SAFETY ----------
  const safeReasons = [];
  let safeScore = SCORE_CONTRACT_BASE;
  if (d.mintAuthority)   { safeScore += SCORE_CONTRACT_MINT_PENALTY; safeReasons.push({label:'Mint authority enabled', points: SCORE_CONTRACT_MINT_PENALTY}); }
  if (d.freezeAuthority) { safeScore += SCORE_CONTRACT_FREEZE_PENALTY; safeReasons.push({label:'Freeze authority enabled', points: SCORE_CONTRACT_FREEZE_PENALTY}); }
  if (d.lpLockedPct != null && d.lpLockedPct < SCORE_CONTRACT_LP_LOCK_THRESHOLD) {
    safeScore += SCORE_CONTRACT_LP_UNLOCKED_PENALTY;
    safeReasons.push({label:'LP locked/burned only '+d.lpLockedPct+'%', points: SCORE_CONTRACT_LP_UNLOCKED_PENALTY});
  }
  // d.lpLockedPct == null means RugCheck has no pool data yet — treated
  // as unknown, not penalized, so brand-new pairs aren't punished for a
  // data lag rather than an actual risk.
  if (safeReasons.length === 0) safeReasons.push({label:'No mint/freeze/LP risk', points: SCORE_CONTRACT_BASE});
  else safeReasons.unshift({label:'Base', points: SCORE_CONTRACT_BASE});
  safeScore = Math.max(0, safeScore);
  reconcileReasons(safeReasons, safeScore);
  components.safety = { score: safeScore, max: SCORE_CONTRACT_MAX, label: 'Contract Safety', reasons: safeReasons };
  total += safeScore;

  // ---------- 8. RED FLAGS: anti-wash-trading / dead-volume ----------
  // Starts at BASE (no red flags detected) and loses points per
  // suspicious pattern, floored at 0. Kept on the same "higher is
  // better" scale as every other component so the existing score-bar UI
  // needs no changes.
  const flagReasons = [{ label: 'Base (no flags)', points: SCORE_REDFLAGS_BASE }];
  let flagScore = SCORE_REDFLAGS_BASE;
  const turnover = liq > 0 ? (d.volume24h||0) / liq : 0;
  if (turnover > SCORE_REDFLAGS_TURNOVER_THRESHOLD) { flagScore += SCORE_REDFLAGS_TURNOVER_PENALTY; flagReasons.push({label:'Volume far exceeds plausible organic turnover', points: SCORE_REDFLAGS_TURNOVER_PENALTY}); }
  if (vm > SCORE_REDFLAGS_THINPOOL_VM && liq > 0 && liq < SCORE_REDFLAGS_THINPOOL_LIQ) { flagScore += SCORE_REDFLAGS_THINPOOL_PENALTY; flagReasons.push({label:'Huge volume vs razor-thin pool', points: SCORE_REDFLAGS_THINPOOL_PENALTY}); }
  if ((d.volume24h||0) < SCORE_REDFLAGS_DEADVOL_VOLUME && (d.pairAgeHours||0) > SCORE_REDFLAGS_DEADVOL_AGE_HOURS) { flagScore += SCORE_REDFLAGS_DEADVOL_PENALTY; flagReasons.push({label:'Effectively no real trading interest', points: SCORE_REDFLAGS_DEADVOL_PENALTY}); }
  if (mcap > 0 && liq > 0 && (mcap/liq) > SCORE_REDFLAGS_OVERVALUED_RATIO) { flagScore += SCORE_REDFLAGS_OVERVALUED_PENALTY; flagReasons.push({label:'Valuation wildly outsizes liquidity', points: SCORE_REDFLAGS_OVERVALUED_PENALTY}); }
  if (p24h != null && p24h > SCORE_REDFLAGS_PUMPDUMP_P24 && s1h < SCORE_REDFLAGS_PUMPDUMP_S1H) { flagScore += SCORE_REDFLAGS_PUMPDUMP_PENALTY; flagReasons.push({label:'Classic pump-and-dump shape', points: SCORE_REDFLAGS_PUMPDUMP_PENALTY}); }
  if (totalTx === 0 && (d.pairAgeHours||0) < SCORE_REDFLAGS_NOACTIVITY_AGE_HOURS) { flagScore += SCORE_REDFLAGS_NOACTIVITY_PENALTY; flagReasons.push({label:'Fresh pair with zero trading activity', points: SCORE_REDFLAGS_NOACTIVITY_PENALTY}); }
  if (d.rugcheckHighRiskCount > 0) {
    const rugPenalty = Math.max(SCORE_REDFLAGS_RUGCHECK_MAX_PENALTY, d.rugcheckHighRiskCount * SCORE_REDFLAGS_RUGCHECK_PENALTY);
    flagScore += rugPenalty;
    flagReasons.push({label:'RugCheck flagged '+d.rugcheckHighRiskCount+' high-risk issue(s)', points: rugPenalty});
  }
  flagScore = Math.round(Math.max(0, Math.min(SCORE_REDFLAGS_MAX, flagScore)));
  reconcileReasons(flagReasons, flagScore);
  components.redFlags = { score: flagScore, max: SCORE_REDFLAGS_MAX, label: 'Red Flags (Wash/Dead Vol)', reasons: flagReasons };
  total += flagScore;

  const rawTotal = Math.min(100, Math.max(0, Math.round(total)));
  const pattern = computePatternMatch(d, d._backtestExcludeId);
  const finalTotal = Math.min(100, Math.max(0, rawTotal + pattern.adjustment));

  // Full explainability breakdown: every component's reasons, plus the
  // historical-pattern adjustment, in one flat signed list. Guaranteed
  // (via reconcileReasons on each component, and the final check below)
  // to sum to exactly `finalTotal`.
  const breakdown = [];
  for (const key of Object.keys(components)) {
    for (const r of components[key].reasons) breakdown.push(r);
  }
  if (pattern.adjustment !== 0) breakdown.push({ label: 'Historical pattern match', points: pattern.adjustment });
  const breakdownSum = breakdown.reduce((s,r)=>s+r.points, 0);
  const finalDiff = Math.round((finalTotal - breakdownSum) * 10) / 10;
  if (finalDiff !== 0) breakdown.push({ label: '(final clamp adjustment)', points: finalDiff });

  return { total: finalTotal, rawTotal, components, pattern, breakdown };
}

// =====================
// CALIBRATION DATASET (score-calibration phase — logging only)
// =====================
// Records every accepted AND rejected token with its full score
// breakdown so accept/reject behavior can be backtested against real
// tracked outcomes before any weight or threshold is ever adjusted.
// This never influences scoring, filtering, or discovery — it only
// observes and records what already happened.
function formatBreakdownString(breakdown) {
  return breakdown
    .filter(r => r.points !== 0)
    .map(r => (r.points > 0 ? '+' : '') + r.points + ' ' + r.label)
    .join(', ');
}

function registerCalibrationRecord(t, accepted, rejectionReasons) {
  try {
    const record = {
      timestamp: Date.now(),
      name: t.name || '?',
      mint: t.address || null,
      score: t.alphaScore ?? null,
      breakdown: t.breakdown || [],
      liquidity: t.liquidity ?? null,
      marketCap: t.mcap ?? null,
      holderConcentrationTop10Pct: t.top10Pct ?? null,
      developerPct: t.devPct ?? null,
      isPumpFun: !!(t.dexId && t.dexId.includes('pump')),
      detectionAgeMinutes: t.pairAgeHours != null ? Math.round(t.pairAgeHours * 60) : null,
      accepted: !!accepted,
      rejectionReasons: accepted ? [] : (rejectionReasons || []),
    };
    calibrationDataset.push(record);
    if (calibrationDataset.length > MAX_CALIBRATION_RECORDS) {
      calibrationDataset = calibrationDataset.slice(calibrationDataset.length - MAX_CALIBRATION_RECORDS);
    }
    return record;
  } catch(e) {
    devLog('Calibration record failed for '+(t?.name||'?')+': '+e.message, 'warn');
    return null;
  }
}

function persistCalibrationDataset() {
  safeLSSet(LS_KEYS.calibration, calibrationDataset);
}

// Logged after every scan (devLog only — existing log stream, no new UI).
// Mirrors the exact format requested: "Score N  Reasons: +x A, -y B, ..."
function logCalibrationReport(scanRecords) {
  devLog('=== CALIBRATION REPORT ===');
  for (const r of scanRecords) {
    const tag = r.accepted ? 'Accepted' : 'Rejected';
    devLog(tag + ': Score ' + r.score + '  Reasons: ' + formatBreakdownString(r.breakdown));
  }
}

// Highest-scoring feature across accepted tokens, largest single
// penalty applied (accepted or rejected), average score, acceptance
// rate, and most common rejection reason — computed fresh from this
// scan's calibration records only.
function logCalibrationSummary(scanRecords) {
  if (scanRecords.length === 0) { devLog('=== CALIBRATION SUMMARY: no scored tokens this scan ==='); return; }

  const accepted = scanRecords.filter(r => r.accepted);
  const rejected = scanRecords.filter(r => !r.accepted);

  let topFeature = null;
  for (const r of accepted) {
    for (const item of r.breakdown) {
      if (item.points > 0 && (!topFeature || item.points > topFeature.points)) {
        topFeature = { label: item.label, points: item.points, token: r.name };
      }
    }
  }

  let biggestPenalty = null;
  for (const r of scanRecords) {
    for (const item of r.breakdown) {
      if (item.points < 0 && (!biggestPenalty || item.points < biggestPenalty.points)) {
        biggestPenalty = { label: item.label, points: item.points, token: r.name };
      }
    }
  }

  const avgScore = scanRecords.reduce((s,r) => s + (r.score||0), 0) / scanRecords.length;
  const acceptanceRate = (accepted.length / scanRecords.length) * 100;

  const rejectionCounts = {};
  for (const r of rejected) {
    for (const reason of r.rejectionReasons) {
      const key = String(reason).split('(')[0].trim();
      rejectionCounts[key] = (rejectionCounts[key]||0) + 1;
    }
  }
  let topRejection = null;
  for (const [reason, count] of Object.entries(rejectionCounts)) {
    if (!topRejection || count > topRejection.count) topRejection = { reason, count };
  }

  devLog('=== CALIBRATION SUMMARY ===');
  devLog('Highest scoring feature (accepted tokens): ' + (topFeature ? '+'+topFeature.points+' '+topFeature.label+' ('+topFeature.token+')' : 'n/a'));
  devLog('Largest penalty applied: ' + (biggestPenalty ? biggestPenalty.points+' '+biggestPenalty.label+' ('+biggestPenalty.token+')' : 'n/a'));
  devLog('Average score: ' + avgScore.toFixed(1));
  devLog('Acceptance rate: ' + acceptanceRate.toFixed(1) + '% (' + accepted.length + '/' + scanRecords.length + ')');
  devLog('Most common rejection reason: ' + (topRejection ? topRejection.reason + ' (' + topRejection.count + 'x)' : 'n/a'));
}

// Preliminary score computed from DEX Screener data only, before any
// Birdeye/Helius calls are made. Used to decide whether a token is
// worth enriching at all. Reuses computeAlphaScore with holder/contract
// data unavailable, so the number lines up with the final score scale.
function computePreScore(pair) {
  const d = {
    liquidity: pair.liquidity?.usd || 0,
    mcap: pair.marketCap || null,
    volume24h: pair.volume?.h24 || null,
    volume1h: pair.volume?.h1 || null,
    volume5m: pair.volume?.m5 || null,
    buys1h: pair.txns?.h1?.buys || null,
    sells1h: pair.txns?.h1?.sells || null,
    buys6h: pair.txns?.h6?.buys || null,
    sells6h: pair.txns?.h6?.sells || null,
    buysM5: pair.txns?.m5?.buys || null,
    sellsM5: pair.txns?.m5?.sells || null,
    priceChange5m: pair.priceChange?.m5 || null,
    priceChange1h: pair.priceChange?.h1 || null,
    priceChange6h: pair.priceChange?.h6 || null,
    pairAgeHours: pair.pairCreatedAt ? (Date.now() - pair.pairCreatedAt) / 3600000 : null,
    _holderDataAvailable: false,
    mintAuthority: false,
    freezeAuthority: false,
  };
  return computeAlphaScore(d).total;
}

// --- Pool cache: snapshot history + acceleration detection -----------
// Records a lightweight snapshot of a resolved pair every time discovery
// sees it, and derives multi-sample "is this accelerating" signals from
// it. This is what makes rate-of-change signals possible at all — a
// single snapshot can only tell you a level (e.g. "volume is $40k"), not
// a trend ("volume per minute just doubled"), which is what actually
// distinguishes a token about to run from one that's already fading.
function getOrCreatePoolCacheEntry(addr, pair) {
  let entry = poolCache.get(addr);
  const isNew = !entry;
  if (isNew) {
    entry = { firstSeenAt: Date.now(), pairCreatedAt: pair.pairCreatedAt || null, lastResolvedAt: 0, lastEnrichedAt: 0, lastPair: null, lastTokenData: null, history: [] };
    poolCache.set(addr, entry);
    discoveryDiag.newPoolsThisScan++;
    if (pair.pairCreatedAt) {
      const delayMs = Date.now() - pair.pairCreatedAt;
      pushCapped(discoveryDiag.detectionDelayMsSamples, Math.max(0, delayMs), 300);
      pushCapped(discoveryDiag.poolAgeAtDetectionMinSamples, Math.max(0, delayMs) / 60000, 300);
    }
  }
  return entry;
}

function recordPoolSnapshot(addr, pair) {
  const entry = getOrCreatePoolCacheEntry(addr, pair);
  entry.lastResolvedAt = Date.now();
  entry.lastPair = pair;
  const snap = {
    t: Date.now(),
    liq: pair.liquidity?.usd || 0,
    vol24h: pair.volume?.h24 || 0,
    volM5: pair.volume?.m5 || 0,
    buysM5: pair.txns?.m5?.buys || 0,
    sellsM5: pair.txns?.m5?.sells || 0,
    buys1h: pair.txns?.h1?.buys || 0,
    sells1h: pair.txns?.h1?.sells || 0,
  };
  entry.history.push(snap);
  if (entry.history.length > ACCEL_HISTORY_SIZE) entry.history.shift();
  return entry;
}

// True only when EACH successive sample increases over the last (a
// genuine climbing trend), not just "now > first" — a single big spike
// between two points looks the same as a real trend with only 2 samples,
// so this explicitly rewards *consistency* once 3 samples are available.
function isConsistentlyIncreasing(values) {
  if (values.length < 2) return false;
  for (let i=1;i<values.length;i++) if (values[i] < values[i-1]) return false;
  return values[values.length-1] > values[0];
}

// Returns a 0..1 acceleration strength plus the individual signals that
// fired, for a pool with at least 2 recorded snapshots. Never called by
// computeAlphaScore — discovery ranking only.
function computeAccelerationSignal(addr) {
  const entry = poolCache.get(addr);
  if (!entry || entry.history.length < 2) return { score: 0, consistent: false, reasons: [] };
  const h = entry.history;
  const first = h[0], last = h[h.length-1];
  const minutesElapsed = Math.max((last.t - first.t) / 60000, 0.25);

  const buysPerMinRates = h.map((s,i) => i===0 ? null : s.buysM5 / 5).filter(v=>v!=null);
  const txVelocities = h.map(s => s.buysM5 + s.sellsM5);
  const liqValues = h.map(s => s.liq);
  const volRates = h.map((s,i) => i===0 ? s.volM5/5 : s.volM5/5); // $ per minute, m5-window based so it's a rate, not a cumulative total

  const reasons = [];
  let score = 0;

  const buysIncreasing = isConsistentlyIncreasing(h.map(s=>s.buysM5));
  if (buysIncreasing) { score += 0.3; reasons.push('buys/min rising'); }

  const txVelocityIncreasing = isConsistentlyIncreasing(txVelocities);
  if (txVelocityIncreasing) { score += 0.25; reasons.push('tx velocity rising'); }

  const liqGrowthRate = liqValues[0] > 0 ? (liqValues[liqValues.length-1] - liqValues[0]) / liqValues[0] / minutesElapsed : 0;
  const liqGrowing = isConsistentlyIncreasing(liqValues) && liqGrowthRate > 0.02; // >2%/min sustained
  if (liqGrowing) { score += 0.25; reasons.push('liquidity growing'); }

  const volAccelerating = isConsistentlyIncreasing(volRates);
  if (volAccelerating) { score += 0.2; reasons.push('volume rate accelerating'); }

  const consistent = h.length >= ACCEL_HISTORY_SIZE && [buysIncreasing, txVelocityIncreasing, liqGrowing, volAccelerating].filter(Boolean).length >= 2;
  return { score: Math.min(1, score), consistent, reasons };
}

// True if DEX-only metrics moved enough since the last full enrichment
// to justify spending another Birdeye/Helius call, OR if acceleration is
// actively detected (a strengthening token is exactly what "continuously
// monitor for the first 30 minutes" exists to catch).
function hasChangedSignificantly(addr, pair) {
  const entry = poolCache.get(addr);
  if (!entry || !entry.lastTokenData) return true; // never enriched — always worth doing once
  const prev = entry.lastTokenData;
  const liqNow = pair.liquidity?.usd || 0, volNow = pair.volume?.h24 || 0;
  const buysNow = pair.txns?.h1?.buys || 0;
  const rel = (a,b) => b > 0 ? Math.abs(a-b)/b : (a > 0 ? 1 : 0);
  const liqChanged = rel(liqNow, prev.liquidity||0) > SIGNIFICANT_CHANGE_RATIO;
  const volChanged = rel(volNow, prev.volume24h||0) > SIGNIFICANT_CHANGE_RATIO;
  const buysChanged = rel(buysNow, prev.buys1h||0) > SIGNIFICANT_CHANGE_RATIO;
  const accel = computeAccelerationSignal(addr);
  return liqChanged || volChanged || buysChanged || accel.consistent;
}

// Pre-enrichment ranking ONLY — decides which of many brand-new
// candidates get a scarce Birdeye/Helius enrichment slot. Deliberately
// reuses computePreScore() (the locked Alpha scoring model) rather than
// introducing a second scoring formula, and adds an explicit recency
// bonus on top so a token that formed seconds ago isn't buried under
// slightly-older tokens that have simply had more time to accrue volume.
// This never touches or replaces the Alpha Score itself.
function computeDiscoveryPriority(pair, ageHours) {
  const age = ageHours ?? NEW_TOKEN_MAX_AGE_HOURS;
  const freshnessBonus = Math.max(0, (NEW_TOKEN_MAX_AGE_HOURS - age) / NEW_TOKEN_MAX_AGE_HOURS) * 30;
  // Sharp extra kicker for the sub-10-minute window specifically — the
  // linear freshnessBonus above barely distinguishes a 2-minute-old pool
  // from a 25-minute-old one; this makes "just launched" unambiguously
  // outrank "launched a while ago", which is the actual objective.
  const ageMinutes = age * 60;
  const veryFreshBonus = ageMinutes <= VERY_FRESH_MINUTES ? (VERY_FRESH_MINUTES - ageMinutes) * 2.5 : 0;
  const addr = pair.baseToken?.address;
  const accel = addr ? computeAccelerationSignal(addr) : { score: 0, consistent: false };
  const accelBonus = accel.score * ACCEL_MAX_PRIORITY_BONUS;
  if (accel.consistent) discoveryDiag.accelerationPromotedThisScan++;
  return freshnessBonus + veryFreshBonus + computePreScore(pair) + accelBonus;
}

function getVerdict(score) {
  if (score >= 80) return { label: 'ALPHA GEM',       color: '#00ff88' };
  if (score >= 65) return { label: 'HIGH CONVICTION',  color: '#00e5ff' };
  if (score >= 50) return { label: 'WATCHLIST',        color: '#fbbf24' };
  if (score >= 35) return { label: 'SPECULATIVE',      color: '#f97316' };
  return { label: 'SKIP', color: '#ff3b5c' };
}

function getRiskLevel(d, score) {
  const flags = [];
  if ((d.top10Pct||100) > 60) flags.push('WHALE CONC');
  if ((d.liquidity||0) < 20000) flags.push('LOW LIQ');
  if ((d.pairAgeHours||0) < 0.25) flags.push('VERY EARLY');
  if (d.mintAuthority) flags.push('MINT RISK');
  if (d.lpLockedPct != null && d.lpLockedPct < SCORE_CONTRACT_LP_LOCK_THRESHOLD) flags.push('LP UNLOCKED');
  if (score < 65) return { level: 'HIGH', color: '#ff3b5c' };
  if (flags.length > 1) return { level: 'MEDIUM-HIGH', color: '#f97316' };
  if (score >= 80) return { level: 'MEDIUM', color: '#fbbf24' };
  return { level: 'MEDIUM', color: '#fbbf24' };
}

// =====================
// ENTRY / EXIT LOGIC
// =====================
function generateEntryExit(d, score) {
  let entry, exit;

  if (score >= 80) {
    entry = 'Enter now with small position. Confirm 5m price not dropping before buying. MCap under $'+(fmt(d.mcap)).replace('$','');
    exit = '1st TP at 2x. 2nd TP at 4x. Stop loss at -30% from entry.';
  } else if (score >= 65) {
    entry = 'Wait for a 10-15% pullback then enter. Watch buy/sell ratio stays above 1.5x.';
    exit = '1st TP at 1.5x. Hard stop at -25%.';
  } else {
    entry = 'Do not enter. Score too low.';
    exit = 'N/A. No position.';
  }
  return { entry, exit };
}

// =====================
// DEX SCREENER FETCH (Analyze CA tab)
// =====================
async function fetchDexScreener(address) {
  const t0 = Date.now();
  const res = await fetch('https://api.dexscreener.com/latest/dex/tokens/'+address);
  const ms = Date.now()-t0;
  if (!res.ok) throw new Error('DEX Screener HTTP '+res.status+' ('+ms+'ms)');
  const json = await res.json();
  const pairs = json?.pairs;
  if (!pairs || pairs.length === 0) return null;
  return [...pairs].sort((a,b)=>(b.liquidity?.usd||0)-(a.liquidity?.usd||0))[0];
}

// =====================
// MULTI-SOURCE NEW-TOKEN DISCOVERY
// =====================
// DexScreener's /dex/search endpoint (used by the main scanner) is a
// keyword search, not a "newest pairs" feed — it was the reason brand
// new launches were getting missed. These functions pull candidate
// mint addresses from feeds that are actually ordered by recency, then
// resolve each address to real pair data via DexScreener's token
// lookup (batched, up to 30 addresses per call).
//
// NOTE: the Pump.fun, Raydium and Meteora endpoints below are their
// public/undocumented JSON APIs. They can change shape or be rate
// limited/CORS-blocked without notice — each is wrapped so a failure
// there never breaks discovery from the other sources.

async function fetchDexScreenerProfilesLatest() {
  const label = 'DexScreener/token-profiles';
  try {
    const t0 = Date.now();
    const res = await fetch('https://api.dexscreener.com/token-profiles/latest/v1');
    const ms = Date.now()-t0;
    if (!res.ok) { recordApiResult('dex', false, ms, 0); devLog(label+' HTTP '+res.status, 'warn'); return []; }
    const json = await res.json();
    recordApiResult('dex', true, ms, 0);
    const items = Array.isArray(json) ? json : [];
    const addrs = items.filter(x=>x.chainId==='solana' && x.tokenAddress).map(x=>x.tokenAddress);
    devLog(label+': '+addrs.length+' solana addresses');
    return addrs;
  } catch(e) { devLog(label+' FAILED: '+e.message, 'warn'); return []; }
}

async function fetchDexScreenerBoostsLatest() {
  const label = 'DexScreener/token-boosts';
  try {
    const t0 = Date.now();
    const res = await fetch('https://api.dexscreener.com/token-boosts/latest/v1');
    const ms = Date.now()-t0;
    if (!res.ok) { recordApiResult('dex', false, ms, 0); devLog(label+' HTTP '+res.status, 'warn'); return []; }
    const json = await res.json();
    recordApiResult('dex', true, ms, 0);
    const items = Array.isArray(json) ? json : [];
    const addrs = items.filter(x=>x.chainId==='solana' && x.tokenAddress).map(x=>x.tokenAddress);
    devLog(label+': '+addrs.length+' solana addresses');
    return addrs;
  } catch(e) { devLog(label+' FAILED: '+e.message, 'warn'); return []; }
}

async function fetchBirdeyeNewListings() {
  const label = 'Birdeye/new_listing';
  const cacheKey = 'bird-new-listing';
  const cached = getCached(cacheKey, 'bird');
  if (cached) { birdeyeDedupedTotal++; return cached; }
  try {
    const r = await birdFetch(BIRDEYE_BASE+'/defi/v2/tokens/new_listing?limit=20', label, 'bird');
    if (!r || !r.data) return [];
    const items = r.data.items || r.data || [];
    const addrs = (Array.isArray(items)?items:[]).map(x=>x.address).filter(Boolean);
    devLog(label+': '+addrs.length+' addresses');
    if (addrs.length) setCache(cacheKey, addrs);
    return addrs;
  } catch(e) { devLog(label+' FAILED: '+e.message, 'warn'); return []; }
}

async function fetchPumpFunLatest() {
  const label = 'Pump.fun/coins';
  try {
    const t0 = Date.now();
    // Undocumented public frontend API. Sorted by creation time, newest first.
    const res = await fetch('https://frontend-api.pump.fun/coins?offset=0&limit=40&sort=created_timestamp&order=DESC&includeNsfw=false');
    const ms = Date.now()-t0;
    if (!res.ok) { devLog(label+' HTTP '+res.status+' ('+ms+'ms) — endpoint may be blocked/changed', 'warn'); return []; }
    const json = await res.json();
    const items = Array.isArray(json) ? json : [];
    const addrs = items.map(x=>x.mint).filter(Boolean);
    devLog(label+': '+addrs.length+' addresses ('+ms+'ms)');
    return addrs;
  } catch(e) {
    devLog(label+' FAILED (likely CORS or endpoint change): '+e.message, 'warn');
    return [];
  }
}

// Freshly-migrated Pump.fun coins: bonding curve completed and the coin
// has graduated to a real Raydium/PumpSwap pool. Migration only happens
// after a coin accumulates real buy volume on the curve, so these are
// typically the highest-quality "just launched" candidates available —
// distinct from fetchPumpFunLatest() above, which is dominated by
// brand-new bonding-curve coins that mostly never migrate at all.
async function fetchPumpFunMigrations() {
  const label = 'Pump.fun/migrations';
  try {
    const t0 = Date.now();
    const res = await fetch('https://frontend-api.pump.fun/coins?offset=0&limit=50&sort=last_trade_timestamp&order=DESC&includeNsfw=false');
    const ms = Date.now()-t0;
    if (!res.ok) { devLog(label+' HTTP '+res.status+' ('+ms+'ms) — endpoint may be blocked/changed', 'warn'); return []; }
    const json = await res.json();
    const items = Array.isArray(json) ? json : [];
    // `complete: true` is Pump.fun's own flag for "bonding curve finished,
    // migrated". Guarded with optional chaining so a schema change just
    // yields zero results here instead of breaking discovery.
    const migrated = items.filter(x => x?.complete === true && x?.mint);
    const addrs = migrated.map(x => x.mint);
    devLog(label+': '+addrs.length+'/'+items.length+' recently-active coins have migrated ('+ms+'ms)');
    return addrs;
  } catch(e) {
    devLog(label+' FAILED (likely CORS or endpoint change): '+e.message, 'warn');
    return [];
  }
}

async function fetchRaydiumNewPools() {
  const label = 'Raydium/pools';
  try {
    const t0 = Date.now();
    // Sorted by liquidity ASCENDING (not the old 'default desc'). Sorting
    // desc-by-default biases toward large, established pools — exactly
    // the opposite of what a "new launches" feed needs, since a pool
    // that's minutes old almost always has the smallest liquidity in the
    // whole set. Ascending liquidity is a cheap, effective proxy for
    // newest when the API doesn't expose a direct creation-time sort.
    const res = await fetch('https://api-v3.raydium.io/pools/info/list?poolType=all&poolSortField=liquidity&sortType=asc&pageSize=100&page=1');
    const ms = Date.now()-t0;
    if (!res.ok) { devLog(label+' HTTP '+res.status+' ('+ms+'ms)', 'warn'); return []; }
    const json = await res.json();
    const items = json?.data?.data || [];
    // mintA/mintB — take whichever side isn't SOL/USDC/USDT as the candidate token
    const majors = new Set(['So11111111111111111111111111111111111111112','EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v','Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB']);
    const addrs = items.map(p => {
      const a = p.mintA?.address, b = p.mintB?.address;
      if (a && !majors.has(a)) return a;
      if (b && !majors.has(b)) return b;
      return null;
    }).filter(Boolean);
    devLog(label+': '+addrs.length+' candidate addresses ('+ms+'ms)');
    return addrs;
  } catch(e) { devLog(label+' FAILED (endpoint may have changed): '+e.message, 'warn'); return []; }
}

async function fetchMeteoraNewPools() {
  const label = 'Meteora/pools';
  try {
    const t0 = Date.now();
    const res = await fetch('https://amm-v2.meteora.ag/pools');
    const ms = Date.now()-t0;
    if (!res.ok) { devLog(label+' HTTP '+res.status+' ('+ms+'ms)', 'warn'); return []; }
    const json = await res.json();
    let items = Array.isArray(json) ? json : (json?.data || []);
    // Rank by whatever TVL-ish field the payload exposes, ascending, for
    // the same reason as Raydium above: the API doesn't expose a direct
    // creation-time sort, but a near-empty pool is a strong proxy for
    // "just created". Falls back to natural order if no such field exists.
    const tvlOf = p => p.pool_tvl ?? p.tvl ?? p.total_liquidity ?? null;
    if (items.length && tvlOf(items[0]) != null) {
      items = [...items].sort((a,b) => (tvlOf(a) ?? Infinity) - (tvlOf(b) ?? Infinity));
    }
    const majors = new Set(['So11111111111111111111111111111111111111112','EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v']);
    const addrs = items.slice(0,120).map(p => {
      const a = p.pool_token_mints?.[0] || p.mint_x || p.token_a_mint;
      const b = p.pool_token_mints?.[1] || p.mint_y || p.token_b_mint;
      if (a && !majors.has(a)) return a;
      if (b && !majors.has(b)) return b;
      return null;
    }).filter(Boolean);
    devLog(label+': '+addrs.length+' candidate addresses ('+ms+'ms)');
    return addrs;
  } catch(e) { devLog(label+' FAILED (endpoint may have changed): '+e.message, 'warn'); return []; }
}

// Batch-resolve up to 30 addresses per DexScreener call into full pair
// objects (liquidity, pairCreatedAt, volume, etc). This is what lets
// addresses sourced from Pump.fun/Raydium/Meteora/Birdeye feed into the
// exact same scoring pipeline as the DexScreener-search-based pairs.
async function fetchDexScreenerBatch(addresses) {
  const uniq = [...new Set(addresses)].filter(Boolean);
  if (uniq.length === 0) return [];
  const chunks = [];
  for (let i=0; i<uniq.length; i+=30) chunks.push(uniq.slice(i, i+30));

  const out = [];
  for (const chunk of chunks) {
    const t0 = Date.now();
    try {
      const res = await fetchWithRetry(
        'https://api.dexscreener.com/latest/dex/tokens/'+chunk.join(','),
        {}, 'DexScreener/tokens-batch', 2
      );
      recordApiResult('dex', true, Date.now()-t0, res.retries, res.rateLimited ? 'rateLimited' : undefined);
      const pairs = res.data?.pairs || [];
      out.push(...pairs);
    } catch(e) {
      recordApiResult('dex', false, Date.now()-t0, 2, e.rateLimited ? 'rateLimited' : undefined);
      devLog('DexScreener batch lookup failed for '+chunk.length+' addresses: '+e.message, 'warn');
    }
    await sleep(150);
  }
  return out;
}

// Pulls every discovery source in parallel, resolves addresses into
// pair objects, and returns a de-duplicated, newest-first list. This is
// the single entry point used by both the "NEW LAUNCHES" live feed and
// the main scanner's candidate pool.
async function gatherDiscoveryPairs() {
  const [profiles, boosts, birdNew, pumpFun, pumpMigrations, raydium, meteora] = await Promise.all([
    fetchDexScreenerProfilesLatest(),
    fetchDexScreenerBoostsLatest(),
    fetchBirdeyeNewListings(),
    fetchPumpFunLatest(),
    fetchPumpFunMigrations(),
    fetchRaydiumNewPools(),
    fetchMeteoraNewPools(),
  ]);

  const candidateAddrs = [...new Set([...profiles, ...boosts, ...birdNew, ...pumpFun, ...pumpMigrations, ...raydium, ...meteora])];
  pipelineStats.discovered += candidateAddrs.length;
  devLog('Discovery sources: profiles='+profiles.length+' boosts='+boosts.length+' birdeyeNew='+birdNew.length+
    ' pumpfun='+pumpFun.length+' pumpMigrations='+pumpMigrations.length+' raydium='+raydium.length+' meteora='+meteora.length);

  if (candidateAddrs.length === 0) {
    devLog('All discovery sources returned 0 addresses this pass (rate limited, CORS, or endpoints changed).', 'warn');
    return [];
  }

  // --- Incremental resolution -----------------------------------------
  // None of these source APIs support "give me only what changed since
  // X" — so the closest honest version of incremental discovery achievable
  // here is: don't re-hit DexScreener for an address we already resolved
  // recently, UNLESS it's still in its first 30 minutes (where we
  // deliberately want continuous monitoring) or a refresh floor has
  // elapsed. Brand-new addresses are always resolved immediately.
  const now = Date.now();
  const mustResolve = [];
  const reusedAddrs = [];
  for (const addr of candidateAddrs) {
    const entry = poolCache.get(addr);
    if (!entry || !entry.lastPair) { mustResolve.push(addr); continue; }
    const ageMs = entry.pairCreatedAt ? (now - entry.pairCreatedAt) : Infinity;
    const isHot = ageMs <= HOT_MONITOR_WINDOW_MS;
    const sinceResolved = now - entry.lastResolvedAt;
    const dueForHotRefresh = isHot && sinceResolved >= DISCOVERY_INTERVAL_MS;
    const dueForFloorRefresh = sinceResolved >= POOL_RESOLVE_REFRESH_MS;
    if (dueForHotRefresh || dueForFloorRefresh) mustResolve.push(addr); else reusedAddrs.push(addr);
  }
  discoveryDiag.cachedPoolsReusedThisScan += reusedAddrs.length;
  devLog('Incremental resolve: '+mustResolve.length+' to fetch fresh, '+reusedAddrs.length+' reused from cache (no DexScreener call)');

  const freshPairs = mustResolve.length ? await fetchDexScreenerBatch(mustResolve) : [];
  for (const p of freshPairs) {
    const addr = p.baseToken?.address;
    if (addr) recordPoolSnapshot(addr, p);
  }
  const reusedPairs = reusedAddrs.map(a => poolCache.get(a)?.lastPair).filter(Boolean);
  const pairs = [...freshPairs, ...reusedPairs];

  // dedupe at the TOKEN level (not pair level): keep the highest-liquidity pair per base token
  const byToken = new Map();
  for (const p of pairs) {
    if (p.chainId !== 'solana') continue;
    const key = p.baseToken?.address;
    if (!key) continue;
    const existing = byToken.get(key);
    if (!existing || (p.liquidity?.usd||0) > (existing.liquidity?.usd||0)) byToken.set(key, p);
  }
  const result = [...byToken.values()].sort((a,b) => (b.pairCreatedAt||0) - (a.pairCreatedAt||0));
  devLog('Discovery resolved to '+result.length+' unique Solana pairs after batch lookup + dedup');
  return result;
}

// =====================
// API DIAGNOSTICS STATE
// =====================
const apiDiag = {
  dex:      { ok: 0, fail: 0, notAvailable: 0, rateLimited: 0, cacheHits: 0, requests: 0, lastOk: null, lastFail: null, lastMs: null, retries: 0 },
  bird:     { ok: 0, fail: 0, notAvailable: 0, rateLimited: 0, cacheHits: 0, requests: 0, lastOk: null, lastFail: null, lastMs: null, retries: 0 },
  helius:   { ok: 0, fail: 0, notAvailable: 0, rateLimited: 0, cacheHits: 0, requests: 0, lastOk: null, lastFail: null, lastMs: null, retries: 0 },
  rugcheck: { ok: 0, fail: 0, notAvailable: 0, rateLimited: 0, cacheHits: 0, requests: 0, lastOk: null, lastFail: null, lastMs: null, retries: 0 },
};

// success=true/false marks real outcomes for the health dot.
// "expected" outcomes (404 Not Available, 429 Rate Limited) are tracked
// separately so they never inflate the failure rate shown to the user.
function recordApiResult(api, success, ms, retries, kind) {
  const d = apiDiag[api];
  d.requests++;
  d.lastMs = ms;
  d.retries += retries;

  if (kind === 'notAvailable') {
    d.notAvailable++;
    updateHealthUI(api, 'ok', ms);
    renderApiDiag();
    return;
  }
  if (kind === 'rateLimited') {
    d.rateLimited++;
    updateHealthUI(api, 'warn', ms);
    renderApiDiag();
    return;
  }

  if (success) {
    d.ok++;
    d.lastOk = new Date().toLocaleTimeString();
    updateHealthUI(api, 'ok', ms);
  } else {
    d.fail++;
    d.lastFail = new Date().toLocaleTimeString();
    const total = d.ok + d.fail;
    updateHealthUI(api, total > 0 && d.fail/total > 0.5 ? 'err' : 'warn', ms);
  }
  renderApiDiag();
}

function recordCacheHit(api) {
  apiDiag[api].cacheHits++;
  renderApiDiag();
}

function renderApiDiag() {
  const el = document.getElementById('apiCounters');
  if (el) {
    el.innerHTML = Object.entries(apiDiag).map(([api, d]) => {
      const totalLookups = d.requests + d.cacheHits;
      const cacheRatio = totalLookups ? Math.round((d.cacheHits/totalLookups)*100) : 0;
      return '<div>'+api.toUpperCase()+': '+d.requests+' requests, '+d.cacheHits+' cache hits ('+cacheRatio+'% cache ratio), '+
        d.ok+' ok, '+d.notAvailable+' not available (404), '+d.rateLimited+' rate limited (429), '+
        d.fail+' errors, '+d.retries+' retries</div>';
    }).join('');
  }
  if (!devPanelOpen) return;
  renderDevLogs();
}

// =====================
// HEALTH UI
// =====================
function updateHealthUI(api, status, ms) {
  const dot  = document.getElementById('hdot-' + api);
  const msEl = document.getElementById('hms-'  + api);
  if (!dot || !msEl) return;
  dot.className = 'health-dot ' + (status === 'ok' ? 'ok' : status === 'warn' ? 'warn' : 'err');
  msEl.textContent = ms != null ? ms + 'ms' : '--';
}

function getApiDiagSummary() {
  return Object.entries(apiDiag).map(([api, d]) => {
    const total = d.ok + d.fail;
    const pct = total ? Math.round((d.ok/total)*100) : 0;
    return api.toUpperCase()+': '+pct+'% success ('+d.ok+' ok / '+d.fail+' fail) '+
      d.notAvailable+' notAvail / '+d.rateLimited+' rateLimited, requests='+d.requests+
      ' cacheHits='+d.cacheHits+' retries='+d.retries+
      ' lastOk='+( d.lastOk||'never')+' lastFail='+(d.lastFail||'never')+' '+
      (d.lastMs!=null?d.lastMs+'ms':'');
  }).join(' | ');
}

// =====================
// FETCH WITH EXPONENTIAL BACKOFF RETRY
// =====================
async function fetchWithRetry(url, options, label, maxRetries=3) {
  let lastErr = null;
  let hitRateLimit = false; // diagnostics-only: did any attempt see HTTP 429?
  for (let attempt=0; attempt<=maxRetries; attempt++) {
    const t0 = Date.now();
    try {
      const res = await fetch(url, options);
      const ms = Date.now()-t0;
      if (res.status === 429) hitRateLimit = true;
      if (!res.ok) {
        let body = '';
        try { body = await res.text(); } catch(_) {}
        const msg = label+' HTTP '+res.status+' ('+ms+'ms) body='+body.slice(0,200);
        devLog(msg, 'err');
        lastErr = new Error(msg);
        lastErr.rateLimited = hitRateLimit;
        if (attempt < maxRetries) {
          const wait = Math.pow(2, attempt)*400;
          devLog(label+' retry '+(attempt+1)+'/'+maxRetries+' in '+wait+'ms', 'warn');
          await sleep(wait);
          continue;
        }
        throw lastErr;
      }
      const data = await res.json();
      if (attempt > 0) devLog(label+' succeeded on retry '+attempt);
      return { data, ms, retries: attempt, rateLimited: hitRateLimit };
    } catch(e) {
      const ms = Date.now()-t0;
      lastErr = e;
      if (lastErr.rateLimited === undefined) lastErr.rateLimited = hitRateLimit;
      if (attempt < maxRetries) {
        const wait = Math.pow(2, attempt)*400;
        devLog(label+' error attempt '+(attempt+1)+': '+e.message+' ('+ms+'ms), retry in '+wait+'ms', 'warn');
        await sleep(wait);
      } else {
        devLog(label+' failed after '+(maxRetries+1)+' attempts: '+e.message+' ('+ms+'ms)', 'err');
      }
    }
  }
  throw lastErr || new Error(label+' failed');
}

// =====================
// REQUEST CACHE
// =====================
const requestCache = {};
const CACHE_TTL = 60000;

function getCached(key, api) {
  const entry = requestCache[key];
  if (entry && Date.now()-entry.ts < CACHE_TTL) {
    devLog('Cache hit: '+key);
    if (api) recordCacheHit(api);
    return entry.data;
  }
  return null;
}
function setCache(key, data) {
  requestCache[key] = { data, ts: Date.now() };
}

// =====================
// BIRDEYE AUTH HEADERS
// =====================
function birdHeaders() {
  // No API key here on purpose — api/birdeye.js attaches it server-side.
  // 'x-chain' still gets forwarded through by the proxy.
  return { 'x-chain': 'solana' };
}

// =====================
// GLOBAL BIRDEYE RATE LIMITER (queue-based)
// =====================
// Replaces the old fixed-gap birdThrottle(). That mechanism only
// serialized calls that were already sequential (one awaited after
// another) — it did NOT serialize two independent call chains invoking
// it concurrently (e.g. the main scanner's enrichBatch running at the
// same time as the live-discovery feed's background enrichment): both
// could read the same stale "last call" timestamp and dispatch close
// together. On top of that, its 900ms gap allows up to 60000/900 ≈ 66.7
// requests/minute in the best case ALONE — already above Birdeye's 60
// RPM limit before any concurrency is even considered.
//
// This queue funnels every Birdeye request from every caller through a
// single consumer and guarantees a hard ceiling of BIRDEYE_MAX_RPM
// requests in any rolling 60-second window, regardless of how many
// independent code paths call it at once.
const BIRDEYE_MAX_RPM = 55; // stay safely under Birdeye's 60 RPM limit
const BIRDEYE_MIN_GAP_MS = Math.ceil(60000 / BIRDEYE_MAX_RPM) + 10; // ~1101ms, small safety margin
let birdeyeQueue = [];              // pending resolvers waiting for their dispatch slot
let birdeyeQueueProcessing = false;
let birdeyeLastDispatchAt = 0;
let birdeyeDispatchTimestamps = []; // rolling log of actual dispatch times (last 60s)
let birdeyeRequestCountTotal = 0;   // every real HTTP request actually dispatched to Birdeye
let birdeyeDedupedTotal = 0;        // requests avoided via cache reuse / in-flight coalescing

function birdeyeScheduleSlot() {
  return new Promise(resolve => {
    birdeyeQueue.push(resolve);
    processBirdeyeQueue();
  });
}

async function processBirdeyeQueue() {
  if (birdeyeQueueProcessing) return;
  birdeyeQueueProcessing = true;
  while (birdeyeQueue.length > 0) {
    const now = Date.now();
    birdeyeDispatchTimestamps = birdeyeDispatchTimestamps.filter(t => now - t < 60000);
    let wait = BIRDEYE_MIN_GAP_MS - (now - birdeyeLastDispatchAt);
    if (birdeyeDispatchTimestamps.length >= BIRDEYE_MAX_RPM) {
      const oldest = birdeyeDispatchTimestamps[0];
      wait = Math.max(wait, 60000 - (now - oldest) + 5);
    }
    if (wait > 0) await sleep(wait);
    const resolve = birdeyeQueue.shift();
    const dispatchAt = Date.now();
    birdeyeLastDispatchAt = dispatchAt;
    birdeyeDispatchTimestamps.push(dispatchAt);
    birdeyeRequestCountTotal++;
    resolve();
  }
  birdeyeQueueProcessing = false;
}

// Fetch a single Birdeye endpoint. Returns { data, status, ms, retries }.
// 404 = "Not Available", a normal expected outcome, never retried.
// 429 = "Rate Limited", retried with exponential backoff (2s, 4s), up
// to 2 attempts, since this is a temporary condition.
// 5xx / network errors also get exponential backoff retries.
async function birdFetch(url, label, apiKey='bird', customHeaders=null) {
  const MAX_429_RETRIES = 2;
  const MAX_5XX_RETRIES = 2;
  let retries = 0;

  for (let attempt = 0; attempt <= Math.max(MAX_429_RETRIES, MAX_5XX_RETRIES); attempt++) {
    await birdeyeScheduleSlot();
    const t0 = Date.now();
    try {
      const requestHeaders = customHeaders || birdHeaders();
      const res = await fetch(url, { headers: requestHeaders });
      const ms  = Date.now() - t0;

      // === TEMPORARY DIAGNOSTIC LOGGING — birdFetch() instrumentation ===
      // Logs the complete request and response for every Birdeye call, so
      // the exact server-side rejection reason is visible. Purely
      // read-only: uses res.clone() to read the body, so the original
      // `res` object below is completely unaffected — status branching,
      // retry logic, and the parsed return value are all untouched.
      {
        const reqHeadersRaw = requestHeaders;
        // Key is held server-side by api/birdeye.js now — nothing to mask.
        const reqHeadersMasked = { ...reqHeadersRaw, 'X-API-KEY': '(proxied)', 'Authorization': '(proxied)' };
        const resHeaders = {};
        try { for (const [hk, hv] of res.headers.entries()) resHeaders[hk] = hv; } catch(_) {}
        let rawBody = '';
        try { rawBody = await res.clone().text(); } catch(bodyErr) { rawBody = '(could not read response body: ' + bodyErr.message + ')'; }

        console.log('[BIRDEYE_DEBUG] ----- ' + label + ' (attempt ' + attempt + ') -----');
        console.log('[BIRDEYE_DEBUG] request URL:', url);
        console.log('[BIRDEYE_DEBUG] request method: GET');
        console.log('[BIRDEYE_DEBUG] request headers:', reqHeadersMasked);
        console.log('[BIRDEYE_DEBUG] response status:', res.status, res.statusText);
        console.log('[BIRDEYE_DEBUG] response headers:', resHeaders);
        console.log('[BIRDEYE_DEBUG] raw response body (pre-parse):', rawBody);

        if (!res.ok) {
          let parsedError = null;
          try { parsedError = JSON.parse(rawBody); } catch(_) { /* not JSON, handled below */ }
          if (parsedError) {
            console.error('[BIRDEYE_DEBUG] ' + label + ' — Birdeye returned a JSON error object:', JSON.stringify(parsedError, null, 2));
          } else if (rawBody) {
            console.error('[BIRDEYE_DEBUG] ' + label + ' — Birdeye returned a non-JSON error body:', rawBody);
          } else {
            console.error('[BIRDEYE_DEBUG] ' + label + ' — Birdeye returned HTTP ' + res.status + ' with an empty body.');
          }
        }
      }
      // === END TEMPORARY DIAGNOSTIC LOGGING ==============================

      if (res.status === 404) {
        devLog('Birdeye '+label+' 404 Not Available ('+ms+'ms)');
        recordApiResult(apiKey, true, ms, retries, 'notAvailable');
        return { data: null, status: 404, ms, retries };
      }

      if (res.status === 429) {
        if (attempt < MAX_429_RETRIES) {
          const wait = Math.pow(2, attempt+1) * 1000; // 2s, 4s
          devLog('Birdeye '+label+' 429 Rate Limited, retry '+(attempt+1)+'/'+MAX_429_RETRIES+' in '+wait+'ms', 'warn');
          retries++;
          await sleep(wait);
          continue;
        }
        devLog('Birdeye '+label+' still rate limited after '+MAX_429_RETRIES+' retries', 'warn');
        recordApiResult(apiKey, false, ms, retries, 'rateLimited');
        return { data: null, status: 429, ms, retries };
      }

      if (!res.ok) {
        if (res.status >= 500 && attempt < MAX_5XX_RETRIES) {
          const wait = Math.pow(2, attempt+1) * 500;
          devLog('Birdeye '+label+' HTTP '+res.status+', retry '+(attempt+1)+'/'+MAX_5XX_RETRIES+' in '+wait+'ms', 'warn');
          retries++;
          await sleep(wait);
          continue;
        }
        devLog('Birdeye '+label+' HTTP '+res.status+' ('+ms+'ms)', 'err');
        recordApiResult(apiKey, false, ms, retries);
        return { data: null, status: res.status, ms, retries };
      }

      const j = await res.json();
      devLog('Birdeye '+label+' OK '+ms+'ms success='+j?.success);
      recordApiResult(apiKey, true, ms, retries);
      return { data: j?.data ?? null, status: res.status, ms, retries };

    } catch(e) {
      const ms = Date.now() - t0;

      // === TEMPORARY DIAGNOSTIC LOGGING — birdFetch() instrumentation ===
      // Same as above but for requests that threw before any response was
      // received at all (network error, DNS failure, CORS block, etc.).
      // Read-only; does not change the retry/error handling below.
      {
        const reqHeadersRaw = customHeaders || birdHeaders();
        // Key is held server-side by api/birdeye.js now — nothing to mask.
        const reqHeadersMasked = { ...reqHeadersRaw, 'X-API-KEY': '(proxied)', 'Authorization': '(proxied)' };
        console.error('[BIRDEYE_DEBUG] ----- ' + label + ' (attempt ' + attempt + ') — THREW before a response was received -----');
        console.error('[BIRDEYE_DEBUG] request URL:', url);
        console.error('[BIRDEYE_DEBUG] request method: GET');
        console.error('[BIRDEYE_DEBUG] request headers:', reqHeadersMasked);
        console.error('[BIRDEYE_DEBUG] exception message:', e.message);
        console.error('[BIRDEYE_DEBUG] exception (full):', e);
      }
      // === END TEMPORARY DIAGNOSTIC LOGGING ==============================

      if (attempt < MAX_5XX_RETRIES) {
        const wait = Math.pow(2, attempt+1) * 500;
        devLog('Birdeye '+label+' network error: '+e.message+', retry '+(attempt+1)+'/'+MAX_5XX_RETRIES+' in '+wait+'ms', 'warn');
        retries++;
        await sleep(wait);
        continue;
      }
      devLog('Birdeye '+label+' network error: '+e.message+' ('+ms+'ms)', 'err');
      recordApiResult(apiKey, false, ms, retries);
      return { data: null, status: 0, ms, retries };
    }
  }
}

let birdeyeInFlight = new Map(); // address -> Promise, for requests currently being fetched

async function fetchBirdeye(address) {
  const cacheKey = 'bird-' + address;
  const cached = getCached(cacheKey, 'bird');
  if (cached) { birdeyeDedupedTotal++; return cached; }

  // Coalesce concurrent requests for the same address instead of letting
  // two callers (e.g. the main scanner and the live-discovery feed both
  // enriching the same freshly-launched token around the same time) each
  // issue a real Birdeye request before either has populated the cache.
  if (birdeyeInFlight.has(address)) {
    birdeyeDedupedTotal++;
    devLog('Birdeye: reusing in-flight request for ' + address.slice(0,8) + ' (duplicate avoided)');
    return birdeyeInFlight.get(address);
  }

  const fetchPromise = (async () => {
    // Overview (always attempt). Diagnostics are recorded inside birdFetch.
    const overviewRes = await birdFetch(
      BIRDEYE_BASE + '/defi/token_overview?address=' + address,
      'overview/' + address.slice(0, 8)
    );
    const overviewData = overviewRes.data;

    // Holders. Skip entirely if overview already came back rate limited,
    // to avoid hammering an API that is already throttling us.
    let holdersData = null;
    let holderDataAvailable = false;
    if (overviewRes.status !== 429) {
      // TEMP: v3/token/holder returns HTTP 400 while every other Birdeye
      // call (including this same file's token_overview, using identical
      // headers) succeeds. Every documented query param for this endpoint
      // (address, offset 0-10000, limit 1-100) matches our request, so
      // the params are not the confirmed cause. The one structural
      // anomaly in our requests generally is the non-standard
      // 'Authorization: Bearer' header — it appears in no official
      // Birdeye example anywhere, only X-API-KEY does. Testing with the
      // minimal, documented header set on this endpoint only, since v3
      // routes may validate headers more strictly than the legacy v1
      // route token_overview still uses. UNCONFIRMED without seeing the
      // live response body — see the [BIRDEYE_DEBUG] log this produces.
      const holderHeaders = { 'x-chain': 'solana' }; // key attached by api/birdeye.js
      const holdersRes = await birdFetch(
        BIRDEYE_BASE + '/defi/v3/token/holder?address=' + address + '&offset=0&limit=10',
        'holders/' + address.slice(0, 8),
        'bird',
        holderHeaders
      );
      if (holdersRes.status === 200 && holdersRes.data) {
        holdersData = holdersRes.data;
        holderDataAvailable = true;
        devLog('Birdeye holders OK: items=' + (holdersData?.items?.length||0) + ' total=' + holdersData?.total);
      }
      // 404 = Not Available, already logged and counted inside birdFetch.
      // No retry is issued for 404, so this stays a single request.
    }

    const result = { overview: overviewData, holders: holdersData, holderDataAvailable };
    // Cache as long as we got at least something
    if (overviewData || holdersData) setCache(cacheKey, result);
    return result;
  })();

  birdeyeInFlight.set(address, fetchPromise);
  try {
    return await fetchPromise;
  } finally {
    birdeyeInFlight.delete(address);
  }
}

// =====================
// HELIUS FETCH
// =====================
async function fetchHelius(address) {
  const cached = getCached('helius-'+address, 'helius');
  if (cached) return cached;

  const t0 = Date.now();
  const payload = {
    jsonrpc: '2.0', id: 1,
    method: 'getAccountInfo',
    params: [address, { encoding: 'jsonParsed' }]
  };

  try {
    const result = await fetchWithRetry(
      HELIUS_RPC,
      { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) },
      'Helius/getAccountInfo/'+address.slice(0,8)
    );
    recordApiResult('helius', true, result.ms, result.retries, result.rateLimited ? 'rateLimited' : undefined);

    const rpcResult = result.data;
    if (rpcResult?.error) {
      devLog('Helius RPC error for '+address.slice(0,8)+': code='+rpcResult.error.code+' msg='+rpcResult.error.message, 'err');
      return { mintAuthority: false, freezeAuthority: false };
    }

    const info = rpcResult?.result?.value?.data?.parsed?.info;
    devLog('Helius OK '+address.slice(0,8)+' mintAuth='+(info?.mintAuthority??'null')+' freezeAuth='+(info?.freezeAuthority??'null'));

    if (!info) devLog('Helius: parsed info is null for '+address.slice(0,8)+', account may not be a token mint', 'warn');

    const out = {
      mintAuthority: info?.mintAuthority != null,
      freezeAuthority: info?.freezeAuthority != null,
      supply: info?.supply ?? null,
      decimals: info?.decimals ?? null,
    };
    setCache('helius-'+address, out);
    return out;
  } catch(e) {
    recordApiResult('helius', false, Date.now()-t0, 3, e.rateLimited ? 'rateLimited' : undefined);
    devLog('Helius FAIL '+address.slice(0,8)+': '+e.message, 'err');
    return { mintAuthority: false, freezeAuthority: false };
  }
}

// =====================
// RUGCHECK — LP lock/burn + risk flags
// =====================
// RugCheck's report/summary endpoint is free and needs no API key, so
// api/rugcheck.js is a plain pass-through (no secret to protect) — it
// exists only to dodge browser CORS, not to hide a key.
async function fetchRugCheck(address) {
  const cached = getCached('rugcheck-'+address, 'rugcheck');
  if (cached) return cached;

  const t0 = Date.now();
  try {
    const result = await fetchWithRetry(
      '/api/rugcheck?address=' + address,
      { method: 'GET' },
      'RugCheck/report-summary/'+address.slice(0,8)
    );
    recordApiResult('rugcheck', true, result.ms, result.retries, result.rateLimited ? 'rateLimited' : undefined);

    const d = result.data || {};
    const risks = Array.isArray(d.risks) ? d.risks : [];
    // lpLockedPct isn't always present on every token (e.g. no pool
    // indexed yet) — null means "unknown", not "0% locked".
    const lpLockedPct = typeof d.lpLockedPct === 'number' ? d.lpLockedPct
      : typeof d.markets?.[0]?.lp?.lpLockedPct === 'number' ? d.markets[0].lp.lpLockedPct
      : null;
    const highRisks = risks.filter(r => /danger|high/i.test(r.level || ''));

    const out = {
      score: typeof d.score_normalised === 'number' ? d.score_normalised
        : typeof d.score === 'number' ? d.score : null, // 0-100, higher = riskier
      lpLockedPct,
      risks: risks.map(r => ({ name: r.name, level: r.level })),
      highRiskCount: highRisks.length,
    };
    setCache('rugcheck-'+address, out);
    devLog('RugCheck OK '+address.slice(0,8)+' score='+out.score+' lpLocked='+lpLockedPct+'% highRisks='+highRisks.length);
    return out;
  } catch(e) {
    recordApiResult('rugcheck', false, Date.now()-t0, 3, e.rateLimited ? 'rateLimited' : undefined);
    devLog('RugCheck FAIL '+address.slice(0,8)+': '+e.message, 'err');
    // Unknown, not "unsafe" — the scorer treats null lpLockedPct as
    // neutral rather than penalizing tokens when RugCheck itself is down.
    return { score: null, lpLockedPct: null, risks: [], highRiskCount: 0 };
  }
}

// =====================
// BUILD TOKEN DATA
// =====================
function buildTokenData(address, pair, birdData, heliusData, rugData) {
  const bird = birdData?.overview ?? null;
  const holderData = birdData?.holders ?? null;
  const holderDataAvailable = birdData?.holderDataAvailable === true;

  let top10Pct = null, devPct = null, holderCount = null;

  if (holderData?.items && holderData.items.length > 0) {
    const totalSupply = holderData.items.reduce((s,h) => s + (parseFloat(h.uiAmount ?? h.amount) || 0), 0) || 1;
    const top10sum = holderData.items.slice(0,10).reduce((s,h) => s + (parseFloat(h.uiAmount ?? h.amount) || 0), 0);
    top10Pct = Math.min(100, Math.round((top10sum / totalSupply) * 100));
    const biggest = holderData.items[0];
    if (biggest) devPct = Math.min(100, Math.round(((parseFloat(biggest.uiAmount ?? biggest.amount) || 0) / totalSupply) * 100));
    devLog('Holder calc: top10='+top10Pct+'% dev='+devPct+'% from '+holderData.items.length+' items');
  } else {
    devLog('No holder items for '+address.slice(0,8)+', holder scores will use defaults', 'warn');
  }

  holderCount = holderData?.total ?? bird?.holder ?? null;

  const ageHours = pair.pairCreatedAt ? (Date.now() - pair.pairCreatedAt) / 3600000 : null;
  const name = pair.baseToken?.name || 'Unknown';
  const sym  = pair.baseToken?.symbol || '?';

  const d = {
    address,
    name,
    symbol: sym,
    narrative: classifyNarrative(name, sym),
    priceUsd:       pair.priceUsd ?? null,
    priceChange5m:  pair.priceChange?.m5  ?? null,
    priceChange1h:  pair.priceChange?.h1  ?? null,
    priceChange24h: pair.priceChange?.h24 ?? null,
    priceChange6h:  pair.priceChange?.h6  ?? null,
    volume24h:  pair.volume?.h24      ?? null,
    volume1h:   pair.volume?.h1       ?? null,
    volume5m:   pair.volume?.m5       ?? null,
    mcap:       pair.marketCap        ?? bird?.mc   ?? null,
    fdv:        pair.fdv              ?? null,
    liquidity:  pair.liquidity?.usd   ?? null,
    buys1h:     pair.txns?.h1?.buys   ?? null,
    sells1h:    pair.txns?.h1?.sells  ?? null,
    buys6h:     pair.txns?.h6?.buys   ?? null,
    sells6h:    pair.txns?.h6?.sells  ?? null,
    buysM5:     pair.txns?.m5?.buys   ?? null,
    sellsM5:    pair.txns?.m5?.sells  ?? null,
    buys24h:    pair.txns?.h24?.buys  ?? null,
    sells24h:   pair.txns?.h24?.sells ?? null,
    pairAgeHours: ageHours,
    url:    pair.url    ?? null,
    dexId:  pair.dexId  ?? '',
    chainId: pair.chainId ?? 'solana',
    top10Pct,
    devPct,
    holderCount,
    mintAuthority:   heliusData?.mintAuthority   ?? false,
    freezeAuthority: heliusData?.freezeAuthority ?? false,
    lpLockedPct:     rugData?.lpLockedPct ?? null, // null = unknown, not "0% locked"
    rugcheckScore:   rugData?.score ?? null,        // 0-100, higher = riskier
    rugcheckRisks:   rugData?.risks ?? [],
    rugcheckHighRiskCount: rugData?.highRiskCount ?? 0,
    logoUrl: bird?.logoURI ?? null,
    _birdOk:            bird !== null,
    _heliusOk:          heliusData?.supply !== undefined,
    _rugcheckOk:        rugData?.score !== null && rugData?.score !== undefined,
    _holderDataAvailable: holderDataAvailable,
  };

  const scoreResult = computeAlphaScore(d);
  const { total, rawTotal, components, pattern, breakdown } = scoreResult;
  d.alphaScore = total;
  d.scoreRaw = rawTotal;
  d.scoreComponents = components;
  d.pattern = pattern;
  d.breakdown = breakdown;
  d.confidence = computeConfidence(scoreResult);
  d.verdict = getVerdict(total);
  d.risk = getRiskLevel(d, total);
  const ee = generateEntryExit(d, total);
  d.entry = ee.entry;
  d.exit  = ee.exit;

  devLog('buildTokenData '+name+' ('+sym+') score='+total+
    ' liq='+fmt(d.liquidity,'$')+' mcap='+fmt(d.mcap,'$')+
    ' birdOk='+d._birdOk+' heliusOk='+d._heliusOk);

  return d;
}

// =====================
// FILTER CHECKS (applied AFTER scoring)
// =====================
// Hard rejections: only absolute disqualifiers.
// Everything else is weighted in the score; no soft rejection.
function runFilter(t, pair) {
  const failed = [];
  const sym = (t.symbol||'').toUpperCase();

  // Hard reject: known non-meme assets
  if (['SOL','USDC','USDT','WSOL','WBTC','WETH','BONK','JUP'].includes(sym)) {
    failed.push('Known major asset');
  }

  // Hard reject: wrong chain
  if (pair.chainId !== 'solana') failed.push('Not Solana');

  // Hard reject: pump-only filter if active
  if (pumpOnly && !(pair.url||'').includes('pump') && !(pair.dexId||'').includes('pump')) {
    failed.push('Not pump.fun');
  }

  // Hard reject: zero liquidity (untradeable)
  if (!t.liquidity || t.liquidity < 1000) failed.push('No liquidity ($'+Math.round(t.liquidity||0)+')');

  // Hard reject: score below user threshold
  if (t.alphaScore < scoreFilter) {
    failed.push('Score '+t.alphaScore+' below threshold '+scoreFilter);
  }

  // Informational notes (do NOT cause rejection — shown in debug only)
  t._notes = [];
  if (t.pairAgeHours != null && t.pairAgeHours > ageFilterHours) {
    t._notes.push('Age '+fmtAge(t.pairAgeHours)+' exceeds filter');
  }
  if (t.mcap && t.mcap > 1000000) t._notes.push('MCap above $1M');

  return failed;
}

// =====================
// CONCURRENT ENRICHMENT
// =====================
// Minimum DEX-only pre-score required before we spend a Birdeye +
// Helius call on a token. Baseline for a token with zero signal is
// around 14 (neutral holder + neutral contract points), so 18 only
// screens out tokens with no liquidity, no volume and no momentum
// at all. This is intentionally low: the goal is to cut wasted API
// calls on dead tokens, not to reintroduce hard rejection.
const PRESCAN_MIN_SCORE = 18;

async function enrichBatch(pairs, onProgress) {
  const CONCURRENCY = 1;
  const results = new Array(pairs.length).fill(null);
  let idx = 0;

  async function worker() {
    while (idx < pairs.length) {
      const i = idx++;
      const pair = pairs[i];
      const addr = pair.baseToken?.address;
      if (!addr) {
        devLog('Pair '+(i+1)+' has no token address, skipping.', 'warn');
        results[i] = { error: 'No address', name: pair.baseToken?.name||'?', symbol: pair.baseToken?.symbol||'?' };
        onProgress(i+1);
        continue;
      }

      // Obvious disqualifiers, checked before any enrichment call is made.
      if (pair.chainId !== 'solana' || !(pair.liquidity?.usd) || pair.liquidity.usd < 1000) {
        devLog('Skipping enrichment for '+(pair.baseToken?.name||addr.slice(0,8))+': fails basic chain/liquidity check', 'warn');
        const tokenData = buildTokenData(addr, pair, { overview: null, holders: null, holderDataAvailable: false }, { mintAuthority: false, freezeAuthority: false });
        tokenData._skippedEnrichment = true;
        results[i] = tokenData;
        onProgress(i+1);
        continue;
      }

      const preScore = computePreScore(pair);
      if (preScore < PRESCAN_MIN_SCORE) {
        devLog('Pre-score '+preScore+' below '+PRESCAN_MIN_SCORE+' for '+(pair.baseToken?.name||addr.slice(0,8))+', skipping Birdeye/Helius calls');
        const tokenData = buildTokenData(addr, pair, { overview: null, holders: null, holderDataAvailable: false }, { mintAuthority: false, freezeAuthority: false });
        tokenData._skippedEnrichment = true;
        results[i] = tokenData;
        onProgress(i+1);
        continue;
      }

      // --- Incremental re-enrichment ------------------------------------
      // If this pool was fully enriched recently and its DEX-only metrics
      // haven't moved significantly (and it isn't showing acceleration —
      // acceleration always forces a fresh look, since that's exactly the
      // "strengthening shortly after launch" case worth re-scoring), reuse
      // the last enrichment instead of spending another Birdeye+Helius
      // call. Scoring logic itself is untouched — this only decides
      // whether to recompute it right now.
      const cacheEntry = poolCache.get(addr);
      const hasFreshCachedEnrichment = cacheEntry?.lastTokenData && !cacheEntry.lastTokenData._skippedEnrichment &&
        (Date.now() - cacheEntry.lastEnrichedAt) < 10*60*1000;
      if (hasFreshCachedEnrichment && !hasChangedSignificantly(addr, pair)) {
        devLog('Reusing cached enrichment for '+(pair.baseToken?.name||addr.slice(0,8))+' — no significant change since last enrichment ('+Math.round((Date.now()-cacheEntry.lastEnrichedAt)/1000)+'s ago)');
        const reused = { ...cacheEntry.lastTokenData, _reusedEnrichment: true,
          pairAgeHours: pair.pairCreatedAt ? (Date.now()-pair.pairCreatedAt)/3600000 : cacheEntry.lastTokenData.pairAgeHours };
        results[i] = reused;
        discoveryDiag.cachedPoolsReusedThisScan++;
        onProgress(i+1);
        continue;
      }

      devLog('Enriching '+(i+1)+'/'+pairs.length+': '+(pair.baseToken?.name||addr.slice(0,8))+' (pre-score '+preScore+')');
      const enrichT0 = Date.now();
      try {
        const [birdData, heliusData, rugData] = await Promise.all([
          fetchBirdeye(addr),
          fetchHelius(addr),
          fetchRugCheck(addr)
        ]);
        const tokenData = buildTokenData(addr, pair, birdData, heliusData, rugData);
        pushCapped(discoveryDiag.enrichmentTimeMsSamples, Date.now()-enrichT0, 200);
        if (cacheEntry) { cacheEntry.lastTokenData = tokenData; cacheEntry.lastEnrichedAt = Date.now(); }
        results[i] = tokenData;
      } catch(e) {
        devLog('Enrichment exception for '+(pair.baseToken?.name||addr.slice(0,8))+': '+e.message, 'err');
        // Still score with whatever DEX data we have
        try {
          const tokenData = buildTokenData(addr, pair, { overview: null, holders: null }, { mintAuthority: false, freezeAuthority: false });
          tokenData._enrichError = e.message;
          results[i] = tokenData;
        } catch(e2) {
          results[i] = { error: e2.message, name: pair.baseToken?.name||'?', symbol: pair.baseToken?.symbol||'?', address: addr, alphaScore: 0 };
        }
      }
      onProgress(i+1);
    }
  }

  const workerCount = Math.min(CONCURRENCY, pairs.length);
  await Promise.all(Array.from({ length: workerCount }, () => worker()));
  return results;
}

// =====================
// SCANNER
// =====================
async function runScanner() {
  if (!startupDone) {
    devLog('Scan blocked: startup diagnostics still running.', 'warn');
    return;
  }
  if (scanState === 'scanning') {
    devLog('Scan already running. Skipping.', 'warn');
    return;
  }
  scanState = 'scanning';

  const btn = document.getElementById('scanBtn');
  btn.disabled = true;
  document.getElementById('scanError').classList.add('hidden');
  document.getElementById('progressWrap').classList.remove('hidden');
  document.getElementById('scanStatus').classList.remove('hidden');
  document.getElementById('scanSummary').classList.add('hidden');
  setProgress(0, 'Starting scan...');
  devLog('=== SCAN START ===');
  devLog('Filters: age='+ageFilterHours+'h scoreMin='+scoreFilter+' pumpOnly='+pumpOnly);
  resetPipelineStats();
  const birdeyeCountAtScanStart = birdeyeRequestCountTotal;
  const birdeyeDedupedAtScanStart = birdeyeDedupedTotal;

  // --- Observability: start a new scan session ---------------------
  // Purely bookkeeping — records what this scan run produced so it can
  // be reviewed/resumed later in the TRACKING tab. Does not feed back
  // into scoring, discovery, enrichment, or filtering.
  const sessionId = 'sess_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
  currentSessionId = sessionId;
  const sessionRecord = {
    sessionId,
    startedAt: Date.now(),
    endedAt: null,
    filters: { ageFilterHours, scoreFilter, pumpOnly },
    tokenAddresses: [],
    stats: null,
  };
  sessionsList.unshift(sessionRecord);
  if (sessionsList.length > MAX_SESSIONS) sessionsList = sessionsList.slice(0, MAX_SESSIONS);
  persistSessions();
  devLog('Session started: ' + sessionId);

  const scanStart = Date.now();

  try {
    // DEX Screener keyword discovery (broad recall, but NOT recency-ordered)
    const queries = ['pump solana','sol meme new','pumpswap sol'];
    let allPairs = [];

    for (let i=0; i<queries.length; i++) {
      setProgress(3+(i*7), 'DEX Screener query '+(i+1)+'/'+queries.length+'...');
      const cacheKey = 'dexsearch-'+queries[i];
      const cached = getCached(cacheKey, 'dex');
      if (cached) {
        allPairs = allPairs.concat(cached);
        devLog('DEX query "'+queries[i]+'": '+cached.length+' pairs (cache)');
        continue;
      }
      const t0 = Date.now();
      try {
        const res = await fetchWithRetry(
          'https://api.dexscreener.com/latest/dex/search?q='+encodeURIComponent(queries[i]),
          {},
          'DexScreener/search/'+queries[i]
        );
        const ms = Date.now()-t0;
        const pairsFound = res.data?.pairs || [];
        recordApiResult('dex', true, ms, res.retries, res.rateLimited ? 'rateLimited' : undefined);
        setCache(cacheKey, pairsFound);
        allPairs = allPairs.concat(pairsFound);
        devLog('DEX query "'+queries[i]+'": '+pairsFound.length+' pairs ('+ms+'ms)');
      } catch(e) {
        recordApiResult('dex', false, Date.now()-t0, 3, e.rateLimited ? 'rateLimited' : undefined);
        devLog('DEX query "'+queries[i]+'" FAILED: '+e.message, 'err');
      }
      await sleep(200);
    }
    pipelineStats.discovered += allPairs.length;

    // Multi-source recency-ordered discovery: DexScreener profiles/boosts,
    // Birdeye new listings, Pump.fun, Raydium, Meteora. This is what
    // actually surfaces freshly launched tokens — keyword search alone
    // routinely misses them because they haven't accumulated the volume
    // that would make them match a search query yet.
    setProgress(25, 'Pulling new-launch feeds (Pump.fun, Raydium, Meteora, Birdeye)...');
    const discoveryPairs = await gatherDiscoveryPairs();
    allPairs = allPairs.concat(discoveryPairs);

    // Keyword-search pairs bypass gatherDiscoveryPairs' incremental cache
    // (that function only wraps the new-launch feeds), so snapshot them
    // into poolCache here too — otherwise acceleration detection would
    // never see tokens that were only ever found via keyword search.
    for (const p of allPairs) {
      const addr = p.baseToken?.address;
      if (addr && !poolCache.has(addr)) recordPoolSnapshot(addr, p);
    }

    devLog('Total raw pairs before dedup: '+allPairs.length);
    setProgress(35, 'Deduplicating...');

    // De-duplicate at the TOKEN level, not the pair level. The same coin
    // can surface twice — once via a Raydium pool, once via PumpSwap, or
    // once from keyword search and again from a new-launch feed — and
    // the old pairAddress-only dedup let both through as separate
    // "tokens", producing duplicate cards for one coin. Keep only the
    // highest-liquidity pair per base token address.
    allPairs = allPairs.filter(p => p.chainId === 'solana' && p.baseToken?.address);
    const byTokenAddr = new Map();
    for (const p of allPairs) {
      const key = p.baseToken.address;
      const existing = byTokenAddr.get(key);
      if (!existing || (p.liquidity?.usd||0) > (existing.liquidity?.usd||0)) byTokenAddr.set(key, p);
    }
    allPairs = [...byTokenAddr.values()];
    pipelineStats.afterDedup = allPairs.length;
    devLog('After dedup+chain filter: '+allPairs.length+' unique Solana tokens');

    // Build the enrichment pool: the newest tokens are GUARANTEED a slot
    // regardless of liquidity, then the remainder is filled by liquidity
    // ranking. The old logic only took the top-20-by-liquidity, which
    // systematically excluded brand new tokens (they rarely have deep
    // liquidity in their first minutes) — that was the main reason new
    // launches never showed up.
    const now = Date.now();
    const withAge = allPairs.map(p => ({
      pair: p,
      ageHours: p.pairCreatedAt ? (now - p.pairCreatedAt) / 3600000 : null
    }));

    // Within the "fresh" window, rank by priority (recency + real early
    // signal — liquidity, buy pressure, momentum via the locked pre-score
    // model) instead of pure age. A 4-minute-old token already showing
    // buy activity is a better use of a scarce enrichment slot than a
    // 30-second-old token that hasn't traded at all yet.
    const newest = withAge
      .filter(x => x.ageHours != null && x.ageHours <= NEW_TOKEN_MAX_AGE_HOURS)
      .map(x => ({ ...x, priority: computeDiscoveryPriority(x.pair, x.ageHours) }))
      .sort((a,b) => b.priority - a.priority)
      .slice(0, MAX_NEW_TOKENS_TO_ENRICH)
      .map(x => x.pair);
    const newestKeys = new Set(newest.map(p => p.baseToken.address));

    const byLiquidity = [...allPairs]
      .filter(p => !newestKeys.has(p.baseToken.address))
      .sort((a,b) => (b.liquidity?.usd||0) - (a.liquidity?.usd||0))
      .slice(0, MAX_LIQUIDITY_TOKENS_TO_ENRICH);

    const toEnrich = [...newest, ...byLiquidity];
    pipelineStats.sentToEnrich = toEnrich.length;

    setProgress(40, 'Enriching '+toEnrich.length+' tokens ('+newest.length+' new + '+byLiquidity.length+' high-liquidity)...');
    devLog('Enriching '+toEnrich.length+' tokens with Birdeye + Helius ('+newest.length+' prioritized for freshness+signal, '+byLiquidity.length+' for liquidity)...');

    // Enrich ALL candidates. Score first, filter after.
    const enriched = await enrichBatch(toEnrich, (n) => {
      setProgress(40+Math.round((n/Math.max(toEnrich.length,1))*50), 'Scoring token '+n+'/'+toEnrich.length+'...');
    });

    devLog('Enrichment complete. Processing results...');

    const scored   = [];
    const errored  = [];
    const accepted = [];
    const rejected = [];
    const rejectionReasons = {};

    for (let i=0; i<enriched.length; i++) {
      const t = enriched[i];
      if (!t || t.error) {
        errored.push(t);
        devLog('DROP at enrichment: '+(t?.name||'unknown')+' — '+(t?.error||'no data returned'), 'warn');
        continue;
      }
      pipelineStats.enriched++;
      scored.push(t);
      pipelineStats.scored++;

      const failedChecks = runFilter(t, toEnrich[i]);
      if (failedChecks.length === 0) {
        accepted.push(t);
        t._accepted = true;
        t._reasons = [];
        pipelineStats.displayed++;
      } else {
        rejected.push(t);
        t._accepted = false;
        t._reasons = failedChecks;
        pipelineStats.filtered++;
        devLog('DROP at filter: '+t.name+' ('+t.symbol+') — '+failedChecks.join('; '), 'warn');
        failedChecks.forEach(r => {
          const key = r.split('(')[0].trim();
          rejectionReasons[key] = (rejectionReasons[key]||0)+1;
        });
      }
    }

    // --- Observability: register outcome-tracking records ------------
    // Every token that made it through scoring (accepted or rejected)
    // gets a tracking record with 15m/30m/1h price checkpoints. This is
    // read-only downstream bookkeeping; it does not affect `accepted`,
    // `rejected`, or any filter/scoring result computed above.
    for (const t of scored) {
      const rec = addTrackingRecord(t, sessionId);
      if (rec && !sessionRecord.tokenAddresses.includes(t.address)) {
        sessionRecord.tokenAddresses.push(t.address);
      }
    }
    persistTrackedTokens();
    persistSessions();
    if (document.getElementById('page-tracking') && !document.getElementById('page-tracking').classList.contains('hidden')) {
      renderTrackingUI();
    }

    // --- Score calibration phase: log every accepted/rejected token's
    // full breakdown into the calibration dataset, then report on it.
    // Read-only alongside the tracking-record loop above; does not
    // affect accepted/rejected or any scoring result.
    const scanCalibrationRecords = scored.map(t => registerCalibrationRecord(t, t._accepted, t._reasons)).filter(Boolean);
    persistCalibrationDataset();
    logCalibrationReport(scanCalibrationRecords);
    logCalibrationSummary(scanCalibrationRecords);

    renderPipelineStats();

    // Build full debug list (scored + pre-filtered drops from DEX)
    const preFilteredPairs = allPairs.filter(p => !toEnrich.includes(p));
    allScannedTokens = [
      ...scored.map(t => ({ ...t, _preRejected: false })),
      ...preFilteredPairs.map(p => ({
        name: p.baseToken?.name||'?', symbol: p.baseToken?.symbol||'?',
        alphaScore: null,
        liquidity: p.liquidity?.usd||null, mcap: p.marketCap||null,
        pairAgeHours: p.pairCreatedAt?(Date.now()-p.pairCreatedAt)/3600000:null,
        priceChange5m: p.priceChange?.m5||null, priceChange1h: p.priceChange?.h1||null,
        volume24h: p.volume?.h24||null,
        holderCount: null, devPct: null, top10Pct: null,
        mintAuthority: false, freezeAuthority: false,
        _accepted: false, _preRejected: true,
        _reasons: ['Not selected for enrichment (low liquidity rank)']
      }))
    ];

    // Integrity checks
    const allZero = scored.length > 0 && scored.every(t => t.alphaScore === 0);
    if (allZero) devLog('INTEGRITY: All Alpha Scores are 0. Check computeAlphaScore input data.', 'err');

    const birdFailRate = apiDiag.bird.fail / Math.max(apiDiag.bird.ok + apiDiag.bird.fail, 1);
    if (birdFailRate > 0.5) devLog('INTEGRITY: Birdeye fail rate >50% ('+Math.round(birdFailRate*100)+'%). Check API key.', 'err');

    const heliusFailRate = apiDiag.helius.fail / Math.max(apiDiag.helius.ok + apiDiag.helius.fail, 1);
    if (heliusFailRate > 0.5) devLog('INTEGRITY: Helius fail rate >50% ('+Math.round(heliusFailRate*100)+'%). Check RPC endpoint.', 'err');

    const noBirdData = scored.filter(t => !t._birdOk).length;
    if (noBirdData === scored.length && scored.length > 0) devLog('INTEGRITY: No token received Birdeye data. Holder scores are defaulting to worst case.', 'err');

    // Compute stats across all scored tokens
    const allScores = scored.map(t => t.alphaScore);
    const avgScore = allScores.length ? Math.round(allScores.reduce((a,b)=>a+b,0)/allScores.length) : 0;
    const highScore = allScores.length ? Math.max(...allScores) : 0;
    const lowScore  = allScores.length ? Math.min(...allScores) : 0;
    const gems = scored.filter(t => t.alphaScore >= 80).length;
    const scanMs = Date.now()-scanStart;

    // Update stats bar
    document.getElementById('statScanned').textContent = scored.length + errored.length;
    document.getElementById('statGems').textContent = gems;
    document.getElementById('statAvg').textContent = avgScore;
    document.getElementById('statTime').textContent = new Date().toLocaleTimeString();

    // Pipeline summary log
    const skippedCount = scored.filter(t => t._skippedEnrichment).length;
    devLog('=== SCAN PIPELINE SUMMARY ===');
    devLog('Discovered (raw, pre-dedup): '+pipelineStats.discovered);
    devLog('After dedup + chain filter: '+pipelineStats.afterDedup);
    devLog('Sent to enrichment: '+pipelineStats.sentToEnrich+' ('+newest.length+' new-age priority, '+byLiquidity.length+' liquidity priority)');
    devLog('Skipped Birdeye/Helius (pre-score gate): '+skippedCount);
    devLog('Birdeye requests this scan: '+(birdeyeRequestCountTotal-birdeyeCountAtScanStart)+' dispatched, '+(birdeyeDedupedTotal-birdeyeDedupedAtScanStart)+' deduped/reused (queue cap: '+BIRDEYE_MAX_RPM+' RPM)');
    devLog('Enriched successfully: '+pipelineStats.enriched);
    devLog('Enrichment errors: '+errored.length);
    devLog('Scored: '+pipelineStats.scored);
    devLog('Filtered out (rejected): '+pipelineStats.filtered);
    devLog('Displayed (accepted): '+pipelineStats.displayed);
    devLog('Avg score: '+avgScore+' | High: '+highScore+' | Low: '+lowScore);
    devLog('API state: '+getApiDiagSummary());
    devLog('Scan time: '+(scanMs/1000).toFixed(1)+'s');

    // Alerts for high scorers
    for (const t of accepted) {
      if (t.alphaScore >= ALERT_THRESHOLD && !alertedTokens.has(t.address)) {
        alertedTokens.add(t.address);
        alertTokenMap[t.address] = t;
        const type = t.alphaScore >= 90 ? 'ALPHA GEM' : 'HIGH CONVICTION';
        const icon = t.alphaScore >= 90 ? '🚨' : '🔥';
        addAlert(icon, t.name+' ('+t.symbol+') scored '+t.alphaScore+'. '+t.verdict.label, type, t);
      }
    }

    allResults = accepted;

    // --- Observability: close out the session + persist for refresh ---
    sessionRecord.endedAt = Date.now();
    sessionRecord.stats = {
      processed: scored.length + errored.length,
      accepted: accepted.length,
      rejected: rejected.length + errored.length,
      avgScore, highScore, lowScore, scanMs,
    };
    persistSessions();
    persistLastResults(allResults);
    persistUIState();

    renderScanSummary({
      processed: scored.length + errored.length,
      accepted: accepted.length,
      rejected: rejected.length + errored.length,
      avgScore, highScore, lowScore,
      scanMs, rejectionReasons
    });

    setProgress(100, 'Complete. '+accepted.length+' tokens accepted from '+scored.length+' scored.');
    await sleep(400);
    document.getElementById('progressWrap').classList.add('hidden');
    document.getElementById('scanStatus').classList.add('hidden');

    applySortAndRender();
    if (debugMode) renderDebugList();

    scanState = 'idle';

  } catch(e) {
    devLog('SCAN CRASHED: '+e.message, 'err');
    document.getElementById('progressWrap').classList.add('hidden');
    document.getElementById('scanStatus').classList.add('hidden');
    document.getElementById('scanError').textContent = 'Scan failed: '+e.message;
    document.getElementById('scanError').classList.remove('hidden');
    scanState = 'error';
    if (sessionRecord && !sessionRecord.endedAt) {
      sessionRecord.endedAt = Date.now();
      sessionRecord.stats = sessionRecord.stats || { error: e.message };
      persistSessions();
    }
  } finally {
    btn.disabled = false;
    resetCountdown();
    if (devPanelOpen) renderDevLogs();
  }
}

function setProgress(pct, text) {
  document.getElementById('progressFill').style.width = pct+'%';
  document.getElementById('scanStatus').textContent = text;
}

// =====================
// SCAN SUMMARY
// =====================
function renderScanSummary(s) {
  const stats = [
    { val: s.processed, label: 'PROCESSED' },
    { val: s.accepted, label: 'ACCEPTED' },
    { val: s.rejected, label: 'REJECTED' },
    { val: s.avgScore, label: 'AVG SCORE' },
    { val: s.highScore, label: 'HIGH SCORE' },
    { val: (s.scanMs/1000).toFixed(1)+'s', label: 'SCAN TIME' },
  ];

  document.getElementById('summaryStats').innerHTML = stats.map(st =>
    '<div class="sum-stat"><div class="sum-stat-val">'+st.val+'</div><div class="sum-stat-label">'+st.label+'</div></div>'
  ).join('');

  const rejs = Object.entries(s.rejectionReasons).sort((a,b)=>b[1]-a[1]).slice(0,8);
  document.getElementById('summaryRejections').innerHTML = rejs.length
    ? rejs.map(([r,n]) => '<div class="rej-pill"><span>'+n+'x</span>'+r+'</div>').join('')
    : '<div style="color:var(--text3);font-size:9px;">No rejections recorded.</div>';

  document.getElementById('scanSummary').classList.remove('hidden');
}

// =====================
// DEBUG LIST
// =====================
function renderDebugList() {
  const el = document.getElementById('debugList');
  if (!el || !debugMode) return;

  el.innerHTML = '<div style="font-family:var(--mono);font-size:9px;color:var(--text3);letter-spacing:0.12em;margin-bottom:8px;">DEBUG: ALL SCANNED TOKENS ('+allScannedTokens.length+')</div>' +
    allScannedTokens.map(t => buildDebugCard(t)).join('');
}

function buildDebugCard(t) {
  const accepted = t._accepted && !t._preRejected;
  const col = accepted ? 'var(--green)' : 'var(--red)';
  const statusLabel = accepted ? 'ACCEPTED' : 'REJECTED';

  const fields = [
    ['Liquidity', fmt(t.liquidity,'$')],
    ['MCap', fmt(t.mcap,'$')],
    ['Age', fmtAge(t.pairAgeHours)],
    ['Vol 24h', fmt(t.volume24h,'$')],
    ['5m', fmtPct(t.priceChange5m)],
    ['1h', fmtPct(t.priceChange1h)],
    ['Holders', t.holderCount||'N/A'],
    ['Largest', t.devPct!=null?t.devPct+'%':'N/A'],
    ['Top 10', t.top10Pct!=null?t.top10Pct+'%':'N/A'],
    ['Mint Auth', t.mintAuthority?'YES':'NO'],
    ['Freeze', t.freezeAuthority?'YES':'NO'],
  ];

  const bsRatio = (t.buys1h||t.buys24h) && (t.sells1h||t.sells24h)
    ? ((t.buys1h||t.buys24h)/Math.max(t.sells1h||t.sells24h,1)).toFixed(2)+'x'
    : 'N/A';
  const vmRatio = t.volume24h && t.mcap ? (t.volume24h/t.mcap).toFixed(2)+'x' : 'N/A';

  const skippedNote = t._skippedEnrichment
    ? '<span class="check-pill check-warn">SKIPPED BIRDEYE/HELIUS (pre-score gate)</span>'
    : '';

  const reasons = t._preRejected
    ? t._reasons.map(r => '<span class="check-pill check-fail">'+r+'</span>').join('')
    : (t.scoreComponents
      ? skippedNote + Object.values(t.scoreComponents).map(c =>
          '<span class="check-pill '+(c.score/c.max > 0.5?'check-pass':'check-fail')+'">'+c.label+' '+c.score+'/'+c.max+'</span>'
        ).join('')
      : '');

  return '<div class="debug-card '+(accepted?'accepted':'rejected')+'">' +
    '<div class="debug-card-top">' +
      '<span class="debug-name">'+(t.name||'?')+' <span style="color:var(--text3)">'+(t.symbol||'')+'</span></span>' +
      '<span>' +
        '<span class="debug-score" style="color:'+col+'">'+(t._preRejected?'--':t.alphaScore)+'</span>' +
        '<span style="font-family:var(--mono);font-size:9px;color:'+col+';margin-left:8px;">'+statusLabel+'</span>' +
      '</span>' +
    '</div>' +
    '<div class="debug-grid">' +
      fields.map(([l,v]) => '<div style="color:var(--text3)">'+l+': <span style="color:var(--text2)">'+v+'</span></div>').join('') +
      '<div style="color:var(--text3)">B/S: <span style="color:var(--text2)">'+bsRatio+'</span></div>' +
      '<div style="color:var(--text3)">Vol/MC: <span style="color:var(--text2)">'+vmRatio+'</span></div>' +
    '</div>' +
    '<div class="debug-checks">'+reasons+'</div>' +
  '</div>';
}

// =====================
// SORT AND RENDER
// =====================
function applySortAndRender() {
  const sortBy = document.getElementById('sortSelect').value;
  let filtered = allResults.filter(t => t.alphaScore >= scoreFilter);

  filtered.sort((a,b) => {
    if (sortBy === 'age') return (a.pairAgeHours??999) - (b.pairAgeHours??999);
    if (sortBy === 'volume') return (b.volume24h||0) - (a.volume24h||0);
    if (sortBy === 'momentum') return ((b.priceChange1h||0)+(b.priceChange5m||0)) - ((a.priceChange1h||0)+(a.priceChange5m||0));
    if (sortBy === 'liquidity') return (b.liquidity||0) - (a.liquidity||0);
    return b.alphaScore - a.alphaScore;
  });

  renderTokens(filtered);
  persistUIState();
}

// =====================
// RENDER TOKENS
// =====================
function renderTokens(tokens) {
  const grid = document.getElementById('tokensGrid');

  if (tokens.length === 0) {
    grid.innerHTML = '<div class="empty-state">No tokens found matching your filters.<br>Try lowering the Alpha Score threshold or expanding the age filter.</div>';
    return;
  }

  grid.innerHTML = tokens.map((t,i) => buildCard(t, i)).join('');
}

function buildCard(t, i) {
  const verdict = t.verdict;
  const bsRatio = (t.buys1h||t.buys24h) && (t.sells1h||t.sells24h)
    ? ((t.buys1h||t.buys24h)/Math.max(t.sells1h||t.sells24h,1)).toFixed(1)+'x'
    : 'N/A';
  const vmRatio = t.volume24h && t.mcap ? (t.volume24h/t.mcap).toFixed(1)+'x' : 'N/A';
  const p1hColor = (t.priceChange1h||0) >= 0 ? 'green' : 'red';
  const p5mColor = (t.priceChange5m||0) >= 0 ? 'green' : 'red';
  const initials = (t.symbol||'?').slice(0,3).toUpperCase();

  const checks = [
    { label: 'LIQ', pass: (t.liquidity||0) >= 15000 },
    { label: 'B/S', pass: (t.buys1h||0)/Math.max(t.sells1h||1,1) >= 1.5 },
    { label: 'VOL', pass: (t.volume24h||0) >= 20000 },
    { label: '1H+', pass: (t.priceChange1h||0) > 0 },
    { label: 'MCap', pass: (t.mcap||0) >= 10000 && (t.mcap||0) <= 500000 },
    { label: 'MINT', pass: !t.mintAuthority, warn: t.mintAuthority },
  ];

  const pillsHtml = checks.map(c =>
    '<span class="check-pill '+(c.warn?'check-warn':c.pass?'check-pass':'check-fail')+'">'+c.label+' '+(c.warn?'⚠':c.pass?'✓':'✗')+'</span>'
  ).join('');

  const dexUrl = t.url || 'https://dexscreener.com/solana/'+t.address;
  const birdUrl = 'https://birdeye.so/token/'+t.address+'?chain=solana';
  const jupUrl = 'https://jup.ag/swap/SOL-'+t.address;
  const pumpUrl = 'https://pump.fun/'+t.address;

  return `<div class="token-card fade-up" id="card-${i}" style="--score-color:${verdict.color}">
    <div class="card-top">
      <div class="token-info">
        <div class="token-icon">${t.logoUrl?'<img src="'+t.logoUrl+'" onerror="this.parentElement.textContent=\''+initials+'\'">':initials}</div>
        <div>
          <div class="token-name">${t.name}</div>
          <div class="token-sym">${t.symbol}</div>
          <div class="token-badges">
            <span class="badge badge-narrative">${t.narrative}</span>
            <span class="badge badge-age">${fmtAge(t.pairAgeHours)}</span>
            ${t.dexId.includes('pump')?'<span class="badge badge-pump">PUMP.FUN</span>':''}
          </div>
        </div>
      </div>
      <div class="alpha-block">
        <div class="alpha-score" style="color:${verdict.color}">${t.alphaScore}</div>
        <div class="alpha-label" style="color:${verdict.color}">${verdict.label}</div>
        <div class="alpha-label" style="color:${t.risk.color};margin-top:2px">RISK: ${t.risk.level}</div>
        ${t.confidence ? `<div class="alpha-label" style="color:var(--accent);margin-top:2px">${t.confidence.alphaProbability}% PROB</div>` : ''}
      </div>
    </div>

    <div class="metrics-row">
      <div class="metric"><div class="metric-label">MCAP</div><div class="metric-val">${fmt(t.mcap,'$')}</div></div>
      <div class="metric"><div class="metric-label">LIQUIDITY</div><div class="metric-val">${fmt(t.liquidity,'$')}</div></div>
      <div class="metric"><div class="metric-label">VOLUME</div><div class="metric-val">${fmt(t.volume24h,'$')}</div></div>
      <div class="metric"><div class="metric-label">B/S RATIO</div><div class="metric-val ${(t.buys1h||0)>=(t.sells1h||1)?'green':'red'}">${bsRatio}</div></div>
      <div class="metric"><div class="metric-label">1H CHANGE</div><div class="metric-val ${p1hColor}">${fmtPct(t.priceChange1h)}</div></div>
      <div class="metric"><div class="metric-label">5M CHANGE</div><div class="metric-val ${p5mColor}">${fmtPct(t.priceChange5m)}</div></div>
    </div>

    <div class="checks-row">${pillsHtml}</div>

    <div class="links-row">
      <a class="ext-link" href="${dexUrl}" target="_blank" onclick="event.stopPropagation()">DEXSCREENER</a>
      <a class="ext-link" href="${birdUrl}" target="_blank" onclick="event.stopPropagation()">BIRDEYE</a>
      <a class="ext-link" href="${jupUrl}" target="_blank" onclick="event.stopPropagation()">JUPITER</a>
      <a class="ext-link" href="${pumpUrl}" target="_blank" onclick="event.stopPropagation()">PUMP.FUN</a>
      <span class="ext-link" style="cursor:pointer;margin-left:auto" onclick="toggleDetail(${i}, event)">DETAILS ▼</span>
    </div>

    <div id="detail-${i}" class="detail-panel hidden">
      ${buildDetailPanel(t)}
    </div>
  </div>`;
}

function buildDetailPanel(t) {
  const ee = { entry: t.entry, exit: t.exit };
  const liqRatio = t.liquidity && t.mcap ? ((t.liquidity/t.mcap)*100).toFixed(1)+'%' : 'N/A';
  const vmRatio = t.volume24h && t.mcap ? (t.volume24h/t.mcap).toFixed(2)+'x' : 'N/A';

  const scoreRows = Object.values(t.scoreComponents||{}).map(c =>
    `<div class="score-row">
      <div class="score-row-label">${c.label}</div>
      <div class="score-bar-wrap"><div class="score-bar-fill" style="width:${(c.score/c.max)*100}%;background:${c.score/c.max>0.6?'var(--green)':c.score/c.max>0.3?'var(--yellow)':'var(--red)'}"></div></div>
      <div class="score-row-val">${c.score}/${c.max}</div>
    </div>`
  ).join('');

  const confidenceSection = t.confidence ? `
  <div class="detail-title">AI CONFIDENCE &amp; PATTERN MATCH</div>
  <div class="detail-grid" style="margin-bottom:14px">
    <div class="detail-section">
      <div class="detail-row"><span class="detail-key">Alpha Probability</span><span class="detail-val" style="color:var(--accent)">${t.confidence.alphaProbability}%</span></div>
      <div class="detail-row"><span class="detail-key">Risk Level</span><span class="detail-val">${t.confidence.riskLevel}</span></div>
      <div class="detail-row"><span class="detail-key">Base Score (pre-pattern)</span><span class="detail-val">${t.scoreRaw}</span></div>
      <div class="detail-row"><span class="detail-key">Pattern Adjustment</span><span class="detail-val" style="color:${(t.pattern?.adjustment||0)>0?'var(--green)':(t.pattern?.adjustment||0)<0?'var(--red)':'var(--text2)'}">${(t.pattern?.adjustment||0)>0?'+':''}${t.pattern?.adjustment||0}</span></div>
      <div class="detail-row"><span class="detail-key">Tracked Sample</span><span class="detail-val">${t.pattern?.sampleSize?.winners||0}W / ${t.pattern?.sampleSize?.losers||0}L</span></div>
    </div>
    <div class="detail-section">
      <div class="detail-title" style="border:none;padding:0;margin-bottom:6px;">REASON FOR SCORE</div>
      <div class="ee-text" style="line-height:1.6">${t.confidence.reason}</div>
    </div>
  </div>` : '';

  return `<div class="detail-grid">
    <div class="detail-section">
      <div class="detail-title">TOKEN HEALTH</div>
      <div class="detail-row"><span class="detail-key">Price</span><span class="detail-val">$${parseFloat(t.priceUsd||0).toFixed(8)}</span></div>
      <div class="detail-row"><span class="detail-key">Market Cap</span><span class="detail-val">${fmt(t.mcap,'$')}</span></div>
      <div class="detail-row"><span class="detail-key">FDV</span><span class="detail-val">${fmt(t.fdv,'$')}</span></div>
      <div class="detail-row"><span class="detail-key">Liquidity</span><span class="detail-val">${fmt(t.liquidity,'$')}</span></div>
      <div class="detail-row"><span class="detail-key">Liq/MCap Ratio</span><span class="detail-val">${liqRatio}</span></div>
      <div class="detail-row"><span class="detail-key">Volume/MCap</span><span class="detail-val">${vmRatio}</span></div>
      <div class="detail-row"><span class="detail-key">Pair Age</span><span class="detail-val">${fmtAge(t.pairAgeHours)}</span></div>
      <div class="detail-row"><span class="detail-key">Narrative</span><span class="detail-val">${t.narrative}</span></div>
    </div>
    <div class="detail-section">
      <div class="detail-title">HOLDER ANALYSIS</div>
      <div class="detail-row"><span class="detail-key">Unique Holders</span><span class="detail-val">${t.holderCount||'N/A'}</span></div>
      <div class="detail-row"><span class="detail-key">Top 10 Wallets</span><span class="detail-val" style="color:${(t.top10Pct||0)>60?'var(--red)':'var(--text2)'}">${t.top10Pct!=null?t.top10Pct+'%':'N/A'}</span></div>
      <div class="detail-row"><span class="detail-key">Largest Holder</span><span class="detail-val" style="color:${(t.devPct||0)>20?'var(--red)':'var(--text2)'}">${t.devPct!=null?t.devPct+'%':'N/A'}</span></div>
      <div class="detail-row"><span class="detail-key">Mint Authority</span><span class="detail-val" style="color:${t.mintAuthority?'var(--red)':'var(--green)'}">${t.mintAuthority?'ENABLED ⚠':'DISABLED ✓'}</span></div>
      <div class="detail-row"><span class="detail-key">Freeze Auth</span><span class="detail-val" style="color:${t.freezeAuthority?'var(--red)':'var(--green)'}">${t.freezeAuthority?'ENABLED ⚠':'DISABLED ✓'}</span></div>
      <div class="detail-row"><span class="detail-key">LP Locked/Burned</span><span class="detail-val" style="color:${t.lpLockedPct==null?'var(--text3)':t.lpLockedPct<50?'var(--red)':'var(--green)'}">${t.lpLockedPct!=null?t.lpLockedPct+'%':'UNKNOWN'}</span></div>
      <div class="detail-row"><span class="detail-key">RugCheck Flags</span><span class="detail-val" style="color:${t.rugcheckHighRiskCount>0?'var(--red)':'var(--text2)'}">${t.rugcheckHighRiskCount>0?t.rugcheckHighRiskCount+' HIGH RISK ⚠':'clean'}</span></div>
      <div class="detail-row"><span class="detail-key">Buys 1h</span><span class="detail-val">${t.buys1h||'N/A'}</span></div>
      <div class="detail-row"><span class="detail-key">Sells 1h</span><span class="detail-val">${t.sells1h||'N/A'}</span></div>
      <div class="detail-row"><span class="detail-key">24h Txns</span><span class="detail-val">${((t.buys24h||0)+(t.sells24h||0))||'N/A'}</span></div>
    </div>
  </div>
  ${confidenceSection}
  <div class="detail-title">ALPHA SCORE BREAKDOWN</div>
  <div class="score-breakdown" style="margin-bottom:14px">${scoreRows}</div>

  <div class="entry-exit-grid">
    <div class="entry-box">
      <div class="ee-label green">ENTRY</div>
      <div class="ee-text">${t.entry}</div>
    </div>
    <div class="exit-box">
      <div class="ee-label red">EXIT</div>
      <div class="ee-text">${t.exit}</div>
    </div>
  </div>`;
}

function toggleDetail(i, e) {
  e.stopPropagation();
  const panel = document.getElementById('detail-'+i);
  panel.classList.toggle('hidden');
}

// =====================
// ANALYZE CA
// =====================
async function analyzeCA() {
  const ca = document.getElementById('caInput').value.trim();
  if (!ca) return;
  const btn = document.getElementById('analyzeBtn');
  btn.disabled = true;
  document.getElementById('analyzeError').classList.add('hidden');
  document.getElementById('analyzeResult').classList.add('hidden');
  const spin = document.getElementById('analyzeSpin');
  spin.classList.remove('hidden');

  try {
    document.getElementById('analyzeSpinText').textContent = 'Fetching DEX Screener data...';
    const pair = await fetchDexScreener(ca);
    if (!pair) throw new Error('No pairs found for this address.');

    document.getElementById('analyzeSpinText').textContent = 'Fetching Birdeye holder data...';
    const [birdData, heliusData, rugData] = await Promise.all([
      fetchBirdeye(ca),
      fetchHelius(ca),
      fetchRugCheck(ca)
    ]);

    document.getElementById('analyzeSpinText').textContent = 'Computing Alpha Score...';
    const t = buildTokenData(ca, pair, birdData, heliusData, rugData);

    spin.classList.add('hidden');

    const resultDiv = document.getElementById('analyzeResult');
    resultDiv.innerHTML = buildCard(t, 'a0');
    resultDiv.classList.remove('hidden');

  } catch(e) {
    spin.classList.add('hidden');
    document.getElementById('analyzeError').textContent = e.message || 'Analysis failed.';
    document.getElementById('analyzeError').classList.remove('hidden');
  } finally {
    btn.disabled = false;
  }
}

// =====================
// AUTO-SCAN
// =====================
function toggleAutoScan() {
  autoScanOn = !autoScanOn;
  const btn = document.getElementById('autoScanBtn');

  if (autoScanOn) {
    btn.textContent = 'AUTO: ON';
    btn.classList.add('active');
    requestNotificationPermission();
    startCountdown();
    runScanner(); // immediate first scan
  } else {
    btn.textContent = 'AUTO: OFF';
    btn.classList.remove('active');
    stopCountdown();
    document.getElementById('autoStatus').textContent = 'AUTO-SCAN: OFF';
    document.getElementById('countdownBar').style.display = 'none';
  }
  persistUIState();
}

function startCountdown() {
  document.getElementById('countdownBar').style.display = 'block';
  countdownSec = AUTO_INTERVAL;
  updateCountdownUI();

  countdownTimer = setInterval(() => {
    countdownSec--;
    updateCountdownUI();
    if (countdownSec <= 0) {
      countdownSec = AUTO_INTERVAL;
      runScanner();
    }
  }, 1000);
}

function stopCountdown() {
  clearInterval(countdownTimer);
  countdownTimer = null;
}

function updateCountdownUI() {
  const pct = (countdownSec / AUTO_INTERVAL) * 100;
  document.getElementById('countdownFill').style.width = pct + '%';
  document.getElementById('autoStatus').textContent =
    'AUTO-SCAN: ON — next scan in ' + countdownSec + 's';
}

// Reset countdown after each scan completes
function resetCountdown() {
  if (!autoScanOn) return;
  countdownSec = AUTO_INTERVAL;
  updateCountdownUI();
}

// =====================
// LIVE DISCOVERY FEED
// =====================
// Independent of the full SCAN NOW / AUTO cycle. Refreshes every
// DISCOVERY_INTERVAL_MS (15-30s), shows tokens the moment they're found
// with a DEX-only pre-score, then quietly enriches each with Birdeye +
// Helius in the background and upgrades the card in place once real
// data (holder safety, contract safety) comes back.

function toggleLiveDiscovery() {
  liveDiscoveryOn = !liveDiscoveryOn;
  const btn = document.getElementById('liveDiscoveryBtn');
  const section = document.getElementById('discoverySection');
  btn.textContent = 'LIVE DISCOVERY: ' + (liveDiscoveryOn ? 'ON' : 'OFF');
  btn.classList.toggle('active', liveDiscoveryOn);
  section.classList.toggle('hidden', !liveDiscoveryOn);

  if (liveDiscoveryOn) {
    runDiscoveryScan();
    discoveryTimer = setInterval(runDiscoveryScan, DISCOVERY_INTERVAL_MS);
  } else {
    clearInterval(discoveryTimer);
    discoveryTimer = null;
    document.getElementById('discoveryStatus').textContent = 'idle';
  }
}

async function runDiscoveryScan() {
  const statusEl = document.getElementById('discoveryStatus');
  statusEl.textContent = 'refreshing...';
  devLog('=== DISCOVERY PASS START ===');
  const birdeyeCountAtPassStart = birdeyeRequestCountTotal;
  const birdeyeDedupedAtPassStart = birdeyeDedupedTotal;

  let pairs;
  try {
    pairs = await gatherDiscoveryPairs();
  } catch(e) {
    devLog('Discovery pass FAILED: '+e.message, 'err');
    statusEl.textContent = 'error — see dev panel';
    return;
  }

  // Only keep genuinely new tokens for this feed (older tokens belong
  // in the main scored grid, not here).
  const now = Date.now();
  const fresh = pairs.filter(p => {
    const addr = p.baseToken?.address;
    if (!addr) return false;
    const ageHours = p.pairCreatedAt ? (now - p.pairCreatedAt)/3600000 : null;
    return ageHours != null && ageHours <= NEW_TOKEN_MAX_AGE_HOURS;
  });

  devLog('Discovery pass: '+fresh.length+' tokens within '+NEW_TOKEN_MAX_AGE_HOURS+'h window');

  // Merge into the feed: update existing entries in place (age advances,
  // liquidity/volume update), add new ones at the front, never lose the
  // enrichment that's already been done on an entry.
  const byAddr = new Map(discoveryFeed.map(t => [t.address, t]));
  for (const p of fresh) {
    const addr = p.baseToken.address;
    const ageHours = (now - p.pairCreatedAt) / 3600000;
    const existing = byAddr.get(addr);
    if (existing && existing._fullyEnriched) {
      // keep the enriched version, just refresh its age/metrics display fields
      existing.pairAgeHours = ageHours;
      // Continuous monitoring: for a token's first 30 minutes, queue a
      // background re-enrichment if its metrics moved significantly or
      // it's showing consistent acceleration — many winners strengthen
      // shortly after launch, so a score frozen from minute 2 shouldn't
      // stand unchallenged for the rest of the tracking window.
      if (ageHours <= HOT_MONITOR_HOURS) {
        const cacheEntry = poolCache.get(addr);
        const cooledDown = !cacheEntry?.lastEnrichedAt || (now - cacheEntry.lastEnrichedAt) >= HOT_REENRICH_COOLDOWN_MS;
        if (cooledDown && hasChangedSignificantly(addr, p)) {
          existing._pair = p;
          existing._pendingRefresh = true;
          if (!enrichQueue.includes(addr)) enrichQueue.push(addr);
        }
      }
      continue;
    }
    const preScore = computePreScore(p);
    const entry = existing || { address: addr, _fullyEnriched: false };
    Object.assign(entry, {
      name: p.baseToken?.name || 'Unknown',
      symbol: p.baseToken?.symbol || '?',
      pairAgeHours: ageHours,
      ageBucket: getAgeBucket(ageHours),
      liquidity: p.liquidity?.usd ?? null,
      mcap: p.marketCap ?? null,
      volume24h: p.volume?.h24 ?? null,
      priceChange5m: p.priceChange?.m5 ?? null,
      priceChange1h: p.priceChange?.h1 ?? null,
      url: p.url ?? null,
      dexId: p.dexId ?? '',
      preScore,
      _pair: p,
    });
    byAddr.set(addr, entry);
    if (!existing) {
      devLog('Discovery: NEW token surfaced '+entry.name+' ('+entry.symbol+') age='+fmtAge(ageHours)+' preScore='+preScore);
      enrichQueue.push(addr);
    }
  }

  // Re-prioritize the background enrichment queue: the freshest + most
  // promising candidate (by the locked pre-score model, same helper the
  // main scanner uses) goes first, rather than strict arrival order —
  // otherwise an early-momentum token can sit behind several quieter
  // ones that just happened to be discovered a moment sooner.
  enrichQueue.sort((a, b) => {
    const ea = byAddr.get(a), eb = byAddr.get(b);
    const pa = ea?._pair ? computeDiscoveryPriority(ea._pair, ea.pairAgeHours) : 0;
    const pb = eb?._pair ? computeDiscoveryPriority(eb._pair, eb.pairAgeHours) : 0;
    return pb - pa;
  });

  // Sort the visible feed by the same freshness+signal priority instead
  // of pure age, so a silent 30-second-old mint doesn't outrank a
  // 4-minute-old token that's already showing real buy activity.
  discoveryFeed = [...byAddr.values()]
    .sort((a,b) => {
      const qa = (a._fullyEnriched ? a.alphaScore : a.preScore) ?? 0;
      const qb = (b._fullyEnriched ? b.alphaScore : b.preScore) ?? 0;
      const freshA = Math.max(0, (NEW_TOKEN_MAX_AGE_HOURS - (a.pairAgeHours ?? NEW_TOKEN_MAX_AGE_HOURS)) / NEW_TOKEN_MAX_AGE_HOURS) * 30;
      const freshB = Math.max(0, (NEW_TOKEN_MAX_AGE_HOURS - (b.pairAgeHours ?? NEW_TOKEN_MAX_AGE_HOURS)) / NEW_TOKEN_MAX_AGE_HOURS) * 30;
      return (qb+freshB) - (qa+freshA);
    })
    .slice(0, 60);

  document.getElementById('discoveryCount').textContent = '('+discoveryFeed.length+')';
  renderDiscoveryFeed();
  statusEl.textContent = 'last refresh ' + new Date().toLocaleTimeString();
  devLog('Birdeye requests this discovery pass: '+(birdeyeRequestCountTotal-birdeyeCountAtPassStart)+' dispatched, '+(birdeyeDedupedTotal-birdeyeDedupedAtPassStart)+' deduped/reused');
  devLog('=== DISCOVERY PASS END ===');

  processEnrichQueue();
}

// Background, low-concurrency enrichment so the live feed doesn't hammer
// Birdeye/Helius on top of whatever the main scanner is doing.
async function processEnrichQueue() {
  if (enrichQueueBusy) return;
  enrichQueueBusy = true;
  try {
    while (enrichQueue.length > 0) {
      const addr = enrichQueue.shift();
      const entry = discoveryFeed.find(t => t.address === addr);
      if (!entry || !entry._pair) continue;
      if (entry._fullyEnriched && !entry._pendingRefresh) continue; // nothing new to do
      try {
        const [birdData, heliusData, rugData] = await Promise.all([fetchBirdeye(addr), fetchHelius(addr), fetchRugCheck(addr)]);
        const full = buildTokenData(addr, entry._pair, birdData, heliusData, rugData);
        const wasRefresh = entry._fullyEnriched === true;
        Object.assign(entry, full, { _fullyEnriched: true, _pendingRefresh: false, pairAgeHours: entry.pairAgeHours });
        const ce = poolCache.get(addr);
        if (ce) { ce.lastTokenData = full; ce.lastEnrichedAt = Date.now(); }
        devLog('Discovery: '+(wasRefresh?'re-':'')+'enriched '+full.name+' -> score '+full.alphaScore);
        renderDiscoveryFeed();
      } catch(e) {
        devLog('Discovery enrichment failed for '+addr.slice(0,8)+': '+e.message, 'warn');
      }
    }
  } finally {
    enrichQueueBusy = false;
  }
}

function renderDiscoveryFeed() {
  const grid = document.getElementById('discoveryGrid');
  if (!grid) return;
  if (discoveryFeed.length === 0) {
    grid.innerHTML = '<div class="empty-state">No new launches found yet. Sources: DexScreener profiles/boosts, Birdeye new listings, Pump.fun, Raydium, Meteora.</div>';
    return;
  }
  grid.innerHTML = discoveryFeed.map((t,i) => buildDiscoveryCard(t,i)).join('');
}

function buildDiscoveryCard(t, i) {
  if (t._fullyEnriched && t.verdict) {
    // Reuse the full card once real scoring data is in, with a NEW badge.
    const html = buildCard(t, 'disc-'+i);
    return html.replace('<div class="token-badges">', '<div class="token-badges"><span class="badge" style="background:#0a1a20;color:var(--accent);border:1px solid #0d3040">LIVE</span>');
  }
  const score = t.preScore ?? 0;
  const verdict = getVerdict(score);
  const ageLabel = fmtAge(t.pairAgeHours);
  const dexUrl = t.url || 'https://dexscreener.com/solana/'+t.address;
  return `<div class="token-card fade-up" style="--score-color:${verdict.color}">
    <div class="card-top">
      <div class="token-info">
        <div class="token-icon">${(t.symbol||'?').slice(0,3).toUpperCase()}</div>
        <div>
          <div class="token-name">${t.name}</div>
          <div class="token-sym">${t.symbol}</div>
          <div class="token-badges">
            <span class="badge" style="background:#0a1a20;color:var(--accent);border:1px solid #0d3040">LIVE</span>
            <span class="badge badge-age">${ageLabel}</span>
            <span class="badge" style="background:#1a1200;color:var(--yellow);border:1px solid #332400">SCORING...</span>
          </div>
        </div>
      </div>
      <div class="alpha-block">
        <div class="alpha-score" style="color:${verdict.color}">${score}</div>
        <div class="alpha-label" style="color:${verdict.color}">PRE-SCORE</div>
      </div>
    </div>
    <div class="metrics-row">
      <div class="metric"><div class="metric-label">MCAP</div><div class="metric-val">${fmt(t.mcap,'$')}</div></div>
      <div class="metric"><div class="metric-label">LIQUIDITY</div><div class="metric-val">${fmt(t.liquidity,'$')}</div></div>
      <div class="metric"><div class="metric-label">VOLUME</div><div class="metric-val">${fmt(t.volume24h,'$')}</div></div>
      <div class="metric"><div class="metric-label">5M CHANGE</div><div class="metric-val ${(t.priceChange5m||0)>=0?'green':'red'}">${fmtPct(t.priceChange5m)}</div></div>
    </div>
    <div class="links-row">
      <a class="ext-link" href="${dexUrl}" target="_blank">DEXSCREENER</a>
      <span class="ext-link" style="margin-left:auto;color:var(--text3)">Full score pending Birdeye/Helius...</span>
    </div>
  </div>`;
}

// =====================
// SOUND
// =====================
function playAlertSound() {
  try {
    if (!audioCtx) audioCtx = new (window.AudioContext || window.webkitAudioContext)();
    const osc = audioCtx.createOscillator();
    const gain = audioCtx.createGain();
    osc.connect(gain);
    gain.connect(audioCtx.destination);
    osc.type = 'sine';
    osc.frequency.setValueAtTime(880, audioCtx.currentTime);
    osc.frequency.exponentialRampToValueAtTime(440, audioCtx.currentTime + 0.15);
    gain.gain.setValueAtTime(0.3, audioCtx.currentTime);
    gain.gain.exponentialRampToValueAtTime(0.001, audioCtx.currentTime + 0.4);
    osc.start(audioCtx.currentTime);
    osc.stop(audioCtx.currentTime + 0.4);
  } catch(e) {}
}

// =====================
// BROWSER NOTIFICATIONS
// =====================
function requestNotificationPermission() {
  if ('Notification' in window && Notification.permission === 'default') {
    Notification.requestPermission();
  }
}

function sendBrowserNotification(token) {
  if (!('Notification' in window) || Notification.permission !== 'granted') return;
  try {
    const n = new Notification('ALPHA GEM DETECTED', {
      body: token.name + ' (' + token.symbol + ') scored ' + token.alphaScore + ' — ' + token.verdict.label,
      icon: token.logoUrl || '',
      tag: token.address,
      requireInteraction: false
    });
    n.onclick = () => {
      window.focus();
      showTab('alerts');
      n.close();
    };
  } catch(e) {}
}

// =====================
// BADGE
// =====================
function incrementBadge() {
  unreadCount++;
  const badge = document.getElementById('alertBadge');
  badge.textContent = unreadCount > 99 ? '99+' : unreadCount;
  badge.classList.remove('hidden');
}

function clearBadge() {
  unreadCount = 0;
  document.getElementById('alertBadge').classList.add('hidden');
  // Mark all as read
  alerts = alerts.map(a => ({ ...a, unread: false }));
}

// =====================
// ALERTS
// =====================
function addAlert(icon, text, type, token) {
  const time = new Date().toLocaleTimeString();
  const addr = token ? token.address : null;
  alerts.unshift({ icon, text, type, time, token, unread: true });
  if (alerts.length > 100) alerts.pop();

  // Badge + sound + browser notification only for high-score alerts
  if (type === 'ALPHA GEM' || type === 'HIGH CONVICTION') {
    incrementBadge();
    playAlertSound();
    if (token) sendBrowserNotification(token);
  }

  renderAlerts();
}

function clearAlerts() {
  alerts = [];
  unreadCount = 0;
  document.getElementById('alertBadge').classList.add('hidden');
  renderAlerts();
}

function renderAlerts() {
  const list = document.getElementById('alertsList');
  if (alerts.length === 0) {
    list.innerHTML = '<div class="empty-state">No alerts yet. Enable AUTO-SCAN or run the scanner manually.</div>';
    return;
  }
  list.innerHTML = alerts.map((a, i) => {
    const unreadClass = a.unread ? 'unread' : 'read';
    return '<div class="alert-item '+unreadClass+'" onclick="openAlertToken('+i+')">' +
      '<div class="alert-icon">'+a.icon+'</div>' +
      '<div class="alert-text">'+a.text+'</div>' +
      '<div class="alert-time">'+a.time+'</div>' +
      '</div>';
  }).join('');
}

function openAlertToken(i) {
  const alert = alerts[i];
  if (!alert || !alert.token) return;
  alerts[i].unread = false;
  // Switch to analyze tab and pre-fill CA
  showTab('analyze');
  document.getElementById('caInput').value = alert.token.address;
  // Show the result directly since we have the data
  const resultDiv = document.getElementById('analyzeResult');
  resultDiv.innerHTML = buildCard(alert.token, 'alert-'+i);
  resultDiv.classList.remove('hidden');
  document.getElementById('analyzeInput').querySelector('.input-row').style.display = 'flex';
}

// =====================
// LOCAL FILE DETECTION
// =====================
function isLocalFile() {
  const proto = window.location.protocol;
  return proto === 'file:' || proto === 'content:';
}

// =====================
// STARTUP DIAGNOSTICS
// =====================
async function runStartupDiagnostics(manual) {
  const diagEl  = document.getElementById('diagResults');
  const noteEl  = document.getElementById('diagNote');
  const rawEl   = document.getElementById('testRawOutput');
  const warnEl  = document.getElementById('localModeWarn');
  const testBtn = document.getElementById('testApisBtn');

  const local = isLocalFile();
  warnEl.style.display = local ? 'block' : 'none';

  if (testBtn) testBtn.disabled = true;
  diagEl.innerHTML = '<span style="color:var(--text3)">Checking APIs...</span>';
  if (rawEl) rawEl.style.display = 'none';

  const results  = [];
  const rawLines = [];

  // Row builder
  function diagRow(label, ok, ms, detail, localFail) {
    const col  = localFail ? 'var(--yellow)' : ok ? 'var(--green)' : 'var(--red)';
    const icon = localFail ? '!' : ok ? '✓' : '✗';
    const text = localFail ? 'Unavailable in local mode' : (detail || '');
    return '<span style="display:inline-flex;align-items:center;gap:6px;color:'+col+'">'+
      icon+' '+label+
      '<span style="color:var(--text3);font-size:9px;">'+(ms!=null?ms+'ms ':'')+text+'</span>'+
    '</span>';
  }

  // Raw output block builder
  function rawBlock(label, status, ms, body) {
    const col = (status >= 200 && status < 300) ? 'var(--green)' : 'var(--red)';
    return '<div style="margin-bottom:12px;">'+
      '<div style="color:var(--text2);font-size:9px;margin-bottom:4px;letter-spacing:0.1em;">'+label+'</div>'+
      '<div style="font-size:9px;color:'+col+';margin-bottom:2px;">HTTP '+status+' &nbsp; '+ms+'ms</div>'+
      '<div style="font-size:8px;color:var(--text3);white-space:pre-wrap;word-break:break-all;'+
           'max-height:120px;overflow-y:auto;background:var(--bg);padding:6px;border-radius:3px;border:1px solid var(--border);">'+
        escHtml(body.slice(0, 600))+(body.length>600?' ...(truncated)':'')+'</div>'+
    '</div>';
  }

  function escHtml(s) {
    return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  }

  // Generic test with full response capture
  async function testEndpoint(label, url, opts) {
    const t0 = Date.now();
    let status = 0, body = '', ms = 0, ok = false, localFail = false;
    try {
      const res = await fetch(url, opts);
      ms = Date.now()-t0;
      status = res.status;
      ok = res.ok;
      try { body = await res.text(); } catch(_) { body = '(could not read body)'; }
    } catch(e) {
      ms = Date.now()-t0;
      body = e.message;
      status = 0;
      // CORS / network error from local file
      if (local && (e.message.includes('Failed to fetch') || e.message.includes('NetworkError') || e.message.includes('CORS'))) {
        localFail = true;
        body = 'CORS blocked. File opened from '+window.location.protocol+' origin. '+e.message;
        devLog(label+' CORS/network blocked in local mode: '+e.message, 'warn');
      } else {
        devLog(label+' FAIL: '+e.message, 'err');
      }
    }
    devLog(label+' status='+status+' ms='+ms+' ok='+ok+' body='+body.slice(0,150));
    return { ok, status, ms, body, localFail };
  }

  // 1. DexScreener
  diagEl.innerHTML = '<span style="color:var(--text3)">Checking DexScreener...</span>';
  {
    const r = await testEndpoint(
      'DexScreener',
      'https://api.dexscreener.com/latest/dex/search?q=sol',
      {}
    );
    startupPassed.dex = r.ok;
    recordApiResult('dex', r.ok, r.ms, 0);
    results.push(diagRow('DexScreener', r.ok, r.ms, r.ok ? '' : ('HTTP '+r.status), r.localFail));
    rawLines.push(rawBlock('DEXSCREENER /dex/search?q=sol', r.status||0, r.ms, r.body));
    diagEl.innerHTML = results.join('&nbsp;&nbsp;&nbsp;')+'&nbsp;&nbsp;&nbsp;<span style="color:var(--text3)">Checking Birdeye...</span>';
  }

  // 2. Birdeye
  {
    const testAddr = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
    const r = await testEndpoint(
      'Birdeye',
      BIRDEYE_BASE+'/defi/token_overview?address='+testAddr,
      { headers: birdHeaders() }
    );
    startupPassed.bird = r.ok;
    recordApiResult('bird', r.ok, r.ms, 0);
    // Parse detail for OK case
    let detail = '';
    if (r.ok) {
      try {
        const p = JSON.parse(r.body);
        detail = 'hasData='+(p?.data!=null)+' success='+p?.success;
        devLog('Birdeye startup: '+detail);
      } catch(_) {}
    } else if (!r.localFail) {
      detail = 'HTTP '+r.status;
    }
    results.push(diagRow('Birdeye', r.ok, r.ms, detail, r.localFail));
    rawLines.push(rawBlock('BIRDEYE /defi/token_overview (USDC)', r.status||0, r.ms, r.body));
    diagEl.innerHTML = results.join('&nbsp;&nbsp;&nbsp;')+'&nbsp;&nbsp;&nbsp;<span style="color:var(--text3)">Checking Helius...</span>';
  }

  // 3. Helius
  {
    const r = await testEndpoint(
      'Helius RPC',
      HELIUS_RPC,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'getVersion', params: [] })
      }
    );
    let detail = '';
    let ok = r.ok;
    if (r.ok) {
      try {
        const p = JSON.parse(r.body);
        if (p?.error) {
          ok = false;
          detail = 'RPC err: '+p.error.message;
        } else {
          detail = 'v'+(p?.result?.['solana-core']||'?');
        }
      } catch(_) {}
    } else if (!r.localFail) {
      detail = 'HTTP '+r.status;
    }
    startupPassed.helius = ok;
    recordApiResult('helius', ok, r.ms, 0);
    results.push(diagRow('Helius RPC', ok, r.ms, detail, r.localFail));
    rawLines.push(rawBlock('HELIUS RPC getVersion', r.status||0, r.ms, r.body));
  }

  // Final status row
  diagEl.innerHTML = results.join('&nbsp;&nbsp;&nbsp;');

  // Notes
  const notes = [];
  if (local) notes.push('Running from local file. APIs may be blocked by browser security.');
  if (!startupPassed.bird)   notes.push('Birdeye offline: holder scores default to worst case.');
  if (!startupPassed.helius) notes.push('Helius offline: contract safety checks disabled.');
  if (!startupPassed.dex)    notes.push('DexScreener offline: scanner cannot fetch pairs.');
  noteEl.textContent = notes.join(' ');

  // Show raw output if Test APIs was clicked
  if (manual && rawEl) {
    rawEl.style.display = 'block';
    rawEl.innerHTML = '<div style="color:var(--text3);letter-spacing:0.1em;font-size:9px;margin-bottom:10px;">RAW API RESPONSES</div>'+rawLines.join('');
  }

  devLog('Diagnostics done. DEX='+startupPassed.dex+' Bird='+startupPassed.bird+' Helius='+startupPassed.helius+' local='+local);

  startupDone = true;
  if (testBtn) testBtn.disabled = false;
}

window.addEventListener('load', () => {
  runStartupDiagnostics(false);
  // --- Observability: restore persisted state on reload -------------
  // Re-hydrates filters, last scan results, tracked tokens, and session
  // history from localStorage, then resumes the checkpoint timer and
  // AUTO-SCAN if it was on before the reload. None of this touches
  // scoring, discovery, enrichment, or filter logic — it only restores
  // state those systems already produced.
  restoreAppState();
  startTrackingTimer();
  resumeAutoScanIfNeeded();

});
