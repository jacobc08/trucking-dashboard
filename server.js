'use strict';

const express = require('express');
const https   = require('https');
const multer  = require('multer');
const XLSX    = require('xlsx');
const { startSyncCron, runSync, pg } = require('./sync');

const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 10 * 1024 * 1024 } });

const app  = express();
const PORT = process.env.PORT || 3000;

const MOTIVE_API_KEY = process.env.MOTIVE_API_KEY || '';
const MOTIVE_BASE    = 'api.keeptruckin.com';

// Tender Acceptance account names
const TA_ACCOUNTS = ['Greenbush GAF Tender Acceptance', 'AMNS Tender Acceptance'];
const TA_LIST     = TA_ACCOUNTS.map(a => `'${a.replace(/'/g, "''")}'`).join(', ');

// Oct 1 2025 midnight Chicago time (CDT = UTC-5) = 2025-10-01 05:00:00 UTC
const DATA_CUTOFF_UTC = '2025-10-01 05:00:00';
const cutoffWhere = col => `"${col}" >= '${DATA_CUTOFF_UTC}'`;

// Returns a SQL WHERE clause fragment filtering by mode
function accountFilter(mode) {
  return mode === 'ta'
    ? `account_name IN (${TA_LIST})`
    : `account_name NOT IN (${TA_LIST})`;
}

// Column name candidates for bid dashboard auto-detection
const COL_CANDIDATES = {
  amount: ['bid_submitted', 'amount', 'bid_amount', 'rate', 'rate_per_mile', 'price', 'total', 'load_rate', 'spot_rate', 'all_in_rate'],
  origin: ['origin_city', 'origin', 'pickup', 'from_city', 'shipper_city', 'pickup_city', 'o_city'],
  dest:   ['destination_city', 'destination', 'dest', 'delivery', 'to_city', 'consignee_city', 'delivery_city', 'd_city'],
  date:   ['bot_processed_record_at_raw', 'created_at', 'bid_date', 'date', 'timestamp', 'submitted_at', 'date_created', 'insert_date', 'bid_time'],
};

// ── Serve static files ───────────────────────────────────────────────
app.use(express.static(__dirname));

// ── Motive API proxy ─────────────────────────────────────────────────
app.get('/api/motive/*', (req, res) => {
  if (!MOTIVE_API_KEY) {
    return res.status(503).json({ error: 'MOTIVE_API_KEY not configured on server.' });
  }
  const motivePath = req.path.replace(/^\/api\/motive/, '') +
    (req.url.includes('?') ? '?' + req.url.split('?')[1] : '');
  const options = {
    hostname: MOTIVE_BASE,
    path: motivePath,
    method: 'GET',
    headers: {
      'Authorization': `Bearer ${MOTIVE_API_KEY}`,
      'Content-Type': 'application/json',
      'Accept': 'application/json',
    },
  };
  const proxyReq = https.request(options, proxyRes => {
    res.status(proxyRes.statusCode);
    res.set('Content-Type', 'application/json');
    proxyRes.pipe(res);
  });
  proxyReq.on('error', err => {
    console.error('Motive proxy error:', err.message);
    res.status(502).json({ error: 'Failed to reach Motive API.' });
  });
  proxyReq.end();
});

// ── ZIP → Market reference table ─────────────────────────────────────

app.get('/api/markets/status', async (req, res) => {
  try {
    const r = await pg.query(`SELECT COUNT(*) AS cnt FROM zip_markets`);
    res.json({ ok: true, count: parseInt(r.rows[0].cnt, 10) });
  } catch {
    res.json({ ok: true, count: 0 });
  }
});

app.post('/api/markets/upload', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ ok: false, error: 'No file uploaded.' });

    const wb   = XLSX.read(req.file.buffer, { type: 'buffer' });
    const ws   = wb.Sheets[wb.SheetNames[0]];
    const rows = XLSX.utils.sheet_to_json(ws, { defval: '' });

    if (!rows.length) return res.status(400).json({ ok: false, error: 'Spreadsheet appears empty.' });

    const first = rows[0];
    if (!('Zip Code' in first) || !('MKT' in first)) {
      return res.status(400).json({ ok: false, error: `Expected columns "Zip Code" and "MKT". Found: ${Object.keys(first).join(', ')}` });
    }

    await pg.query(`
      CREATE TABLE IF NOT EXISTS zip_markets (
        zip_prefix TEXT PRIMARY KEY,
        market     TEXT NOT NULL
      )
    `);
    await pg.query(`TRUNCATE zip_markets`);

    const client = await pg.connect();
    try {
      await client.query('BEGIN');
      for (const row of rows) {
        const prefix = String(row['Zip Code']).trim().slice(0, 3);
        const market = String(row['MKT']).trim();
        if (!prefix || !market) continue;
        await client.query(
          `INSERT INTO zip_markets (zip_prefix, market) VALUES ($1, $2) ON CONFLICT (zip_prefix) DO UPDATE SET market = $2`,
          [prefix, market]
        );
      }
      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }

    const count = await pg.query('SELECT COUNT(*) AS cnt FROM zip_markets');
    res.json({ ok: true, count: parseInt(count.rows[0].cnt, 10) });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ── Sync status ──────────────────────────────────────────────────────
app.get('/api/sync/status', async (req, res) => {
  try {
    const r = await pg.query('SELECT * FROM _sync_state ORDER BY updated_at DESC');
    res.json({ ok: true, tables: r.rows });
  } catch (err) {
    res.json({ ok: false, tables: [], error: err.message });
  }
});

app.post('/api/sync/run', async (req, res) => {
  const result = await runSync();
  res.json(result);
});

// ── Debug: see exactly what's synced and detected ────────────────────
app.get('/api/bids/debug', async (req, res) => {
  const info = {};
  try {
    const state = await pg.query('SELECT * FROM _sync_state');
    info.syncState = state.rows;
  } catch (err) {
    info.syncStateError = err.message;
  }
  try {
    const ctx = await getBidContext();
    info.bidContext = ctx;
  } catch (err) {
    info.bidContextError = err.message;
  }
  res.json(info);
});

// ── Bid dashboard helpers ────────────────────────────────────────────

// Find the primary bid table and detect its columns
async function getBidContext() {
  let stateRows;
  try {
    const r = await pg.query('SELECT * FROM _sync_state ORDER BY row_count DESC');
    stateRows = r.rows;
  } catch {
    return null; // _sync_state doesn't exist yet
  }

  if (!stateRows.length) return null;

  // Prefer a table with 'bid' in the name, else use the one with most rows
  const bidRow = stateRows.find(r => r.pg_table.includes('bid')) || stateRows[0];
  const pgTable = bidRow.pg_table;

  // Introspect columns
  const colResult = await pg.query(
    `SELECT column_name FROM information_schema.columns WHERE table_name = $1`,
    [pgTable]
  );
  const cols = colResult.rows.map(r => r.column_name.toLowerCase());

  function detect(candidates) {
    return candidates.find(c => cols.includes(c)) || null;
  }

  return {
    pgTable,
    lastSync: bidRow.last_sync,
    amountCol: detect(COL_CANDIDATES.amount),
    originCol: detect(COL_CANDIDATES.origin),
    destCol:   detect(COL_CANDIDATES.dest),
    dateCol:   detect(COL_CANDIDATES.date),
    allTables: stateRows,
  };
}

// Shared helper: builds a deduped CTE keeping only the latest record per load_id
// make_bid and bid_submitted reflect the bot's most recent decision on each load
function dedupCTE(pgTable, dateCol, amountCol, originCol, destCol, whereClause) {
  const amountSel = amountCol ? `, "${amountCol}"::numeric AS amount` : '';
  const laneSel   = (originCol && destCol) ? `, "${originCol}" AS origin, "origin_state", "origin_postal_code", "${destCol}" AS dest, "destination_state", "destination_postal_code"` : '';
  return `
    WITH deduped AS (
      SELECT DISTINCT ON (load_id)
        load_id, make_bid ${amountSel}
        ${dateCol ? `, "${dateCol}" AS ts` : ''}
        ${laneSel}
      FROM "${pgTable}"
      WHERE ${whereClause}
      ORDER BY load_id, ${dateCol ? `"${dateCol}"` : '1'} DESC NULLS LAST
    )
  `;
}

const BID_FILTER = `LOWER(make_bid::text) IN ('1', 'true', 'yes')`;

// ── GET /api/bids/summary ────────────────────────────────────────────
app.get('/api/bids/summary', async (req, res) => {
  try {
  const ctx = await getBidContext();
  if (!ctx) return res.json({ ok: false, reason: 'no_data' });

  const { pgTable, amountCol, originCol, destCol, dateCol } = ctx;
  const mode      = req.query.mode === 'ta' ? 'ta' : 'spot';
  const dateF     = dateCol ? `"${dateCol}" >= NOW() - INTERVAL '30 days'` : 'TRUE';
  const baseWhere = `${accountFilter(mode)} AND ${dateF}`;
  const cte       = dedupCTE(pgTable, dateCol, amountCol, originCol, destCol, baseWhere);

  const avgSel   = amountCol ? `, AVG(amount) FILTER (WHERE ${BID_FILTER}) AS avg_amount` : '';
  const lanesSel = (originCol && destCol) ? `, COUNT(DISTINCT origin || '-' || dest) FILTER (WHERE ${BID_FILTER}) AS lanes` : '';

  const r = await pg.query(`${cte}
    SELECT COUNT(*) AS opportunities,
           COUNT(*) FILTER (WHERE ${BID_FILTER}) AS bids
           ${avgSel} ${lanesSel}
    FROM deduped
  `);

  const row = r.rows[0];
  res.json({
    ok: true,
    opportunities: parseInt(row.opportunities, 10),
    totalBids:     parseInt(row.bids, 10),
    avgAmount:     row.avg_amount ? parseFloat(row.avg_amount) : null,
    activeLanes:   row.lanes ? parseInt(row.lanes, 10) : null,
    lastSync:      ctx.lastSync,
    detectedCols:  { amountCol, originCol, destCol, dateCol },
  });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ── GET /api/bids/per-day ────────────────────────────────────────────
app.get('/api/bids/per-day', async (req, res) => {
  try {
  const ctx = await getBidContext();
  if (!ctx || !ctx.dateCol) return res.json({ ok: false, reason: 'no_date_col', rows: [] });

  const { pgTable, dateCol } = ctx;
  const mode      = req.query.mode === 'ta' ? 'ta' : 'spot';
  const baseWhere = `${accountFilter(mode)} AND ${cutoffWhere(dateCol)}`;
  const cte       = dedupCTE(pgTable, dateCol, null, null, null, baseWhere);

  const r = await pg.query(`${cte}
    SELECT DATE(ts AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago') AS day,
           COUNT(*) AS opportunities,
           COUNT(*) FILTER (WHERE ${BID_FILTER}) AS bids
    FROM deduped
    GROUP BY DATE(ts AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago')
    ORDER BY day ASC
  `);

  res.json({ ok: true, rows: r.rows.map(row => ({
    day:           row.day,
    opportunities: parseInt(row.opportunities, 10),
    bids:          parseInt(row.bids, 10),
  }))});
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ── GET /api/bids/top-lanes ──────────────────────────────────────────
app.get('/api/bids/top-lanes', async (req, res) => {
  try {
  const ctx = await getBidContext();
  if (!ctx || !ctx.originCol || !ctx.destCol) {
    return res.json({ ok: false, reason: 'no_lane_cols', rows: [] });
  }

  const { pgTable, originCol, destCol, amountCol, dateCol } = ctx;
  const mode      = req.query.mode === 'ta' ? 'ta' : 'spot';
  const cte       = dedupCTE(pgTable, dateCol, amountCol, originCol, destCol, accountFilter(mode));
  const avgSel    = amountCol ? `, AVG(amount) FILTER (WHERE ${BID_FILTER}) AS avg_amount` : '';

  const r = await pg.query(`${cte}
    SELECT origin, origin_state, dest, destination_state,
           COUNT(*) AS opportunities,
           COUNT(*) FILTER (WHERE ${BID_FILTER}) AS bids
           ${avgSel}
    FROM deduped
    GROUP BY origin, origin_state, dest, destination_state
    ORDER BY bids DESC
    LIMIT 10
  `);

  res.json({
    ok: true,
    rows: r.rows.map(row => ({
      lane:          `${row.origin}, ${row.origin_state} → ${row.dest}, ${row.destination_state}`,
      opportunities: parseInt(row.opportunities, 10),
      bids:          parseInt(row.bids, 10),
      avgAmount:     row.avg_amount ? parseFloat(row.avg_amount) : null,
    })),
  });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ── GET /api/bids/activity ───────────────────────────────────────────
app.get('/api/bids/activity', async (req, res) => {
  try {
  const ctx = await getBidContext();
  if (!ctx || !ctx.originCol || !ctx.destCol) return res.json({ ok: false, reason: 'no_lane_cols', rows: [] });

  const { pgTable, dateCol, amountCol, originCol, destCol } = ctx;
  const mode      = req.query.mode === 'ta' ? 'ta' : 'spot';
  const baseWhere = dateCol
    ? `${accountFilter(mode)} AND "${dateCol}" >= NOW() - INTERVAL '30 days'`
    : accountFilter(mode);
  const cte     = dedupCTE(pgTable, dateCol, amountCol, originCol, destCol, baseWhere);
  const avgSel  = (amountCol && mode === 'spot')
    ? `, AVG(amount) FILTER (WHERE ${BID_FILTER}) AS avg_amount
     , MIN(amount) FILTER (WHERE ${BID_FILTER}) AS min_amount
     , MAX(amount) FILTER (WHERE ${BID_FILTER}) AS max_amount`
    : '';

  // Lane-level top 10
  const laneRes = await pg.query(`${cte}
    SELECT origin, origin_state, dest, destination_state,
           COUNT(*) AS opportunities,
           COUNT(*) FILTER (WHERE ${BID_FILTER}) AS bids
    FROM deduped
    GROUP BY origin, origin_state, dest, destination_state
    HAVING COUNT(*) >= 5
    ORDER BY opportunities DESC
    LIMIT 10
  `);

  // Market-level top 10
  const mktRes = await pg.query(`${cte},
    by_zip AS (
      SELECT LEFT(MIN(origin_postal_code), 3)      AS orig_zip3,
             LEFT(MIN(destination_postal_code), 3) AS dest_zip3,
             COUNT(*) AS opportunities,
             COUNT(*) FILTER (WHERE ${BID_FILTER}) AS bids
      FROM deduped
      GROUP BY LEFT(origin_postal_code, 3), LEFT(destination_postal_code, 3)
    )
    SELECT zm_o.market AS orig_mkt, zm_d.market AS dest_mkt,
           SUM(b.opportunities) AS opportunities,
           SUM(b.bids) AS bids
    FROM by_zip b
    LEFT JOIN zip_markets zm_o ON b.orig_zip3 = zm_o.zip_prefix
    LEFT JOIN zip_markets zm_d ON b.dest_zip3 = zm_d.zip_prefix
    WHERE zm_o.market IS NOT NULL AND zm_d.market IS NOT NULL
    GROUP BY zm_o.market, zm_d.market
    ORDER BY opportunities DESC
    LIMIT 10
  `);

  res.json({
    ok: true,
    mode,
    rows: laneRes.rows.map(row => ({
      lane:          `${row.origin}, ${row.origin_state} → ${row.dest}, ${row.destination_state}`,
      opportunities: parseInt(row.opportunities, 10),
      bids:          parseInt(row.bids, 10),
    })),
    mktRows: mktRes.rows.map(row => ({
      mkt:           `${row.orig_mkt} → ${row.dest_mkt}`,
      opportunities: parseInt(row.opportunities, 10),
      bids:          parseInt(row.bids, 10),
    })),
  });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ── Start ────────────────────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`Greenbush Brokerage Center running on port ${PORT}`);
  if (process.env.SQLSERVER_HOST && process.env.DATABASE_URL) {
    startSyncCron();
  } else {
    console.warn('[sync] Skipping — SQLSERVER_HOST or DATABASE_URL not set.');
  }
});
