'use strict';

const express = require('express');
const https   = require('https');
const multer  = require('multer');
const XLSX    = require('xlsx');
const { startSyncCron, runSync, pg } = require('./sync');
const bcrypt     = require('bcryptjs');
const session    = require('express-session');
const PgSession  = require('connect-pg-simple')(session);

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

// ── Body parser & session ────────────────────────────────────────────
app.use(express.json());
app.set('trust proxy', 1);
app.use(session({
  store: new PgSession({
    pool: pg,
    tableName: 'user_sessions',
    createTableIfMissing: true,
  }),
  secret: process.env.SESSION_SECRET || 'gb-fallback-change-me',
  resave: false,
  saveUninitialized: false,
  cookie: {
    httpOnly: true,
    sameSite: 'strict',
    maxAge: 8 * 60 * 60 * 1000, // 8 hours
    secure: process.env.NODE_ENV === 'production',
  },
}));

// ── Users table + admin seed ─────────────────────────────────────────
async function ensureUsers() {
  await pg.query(`
    CREATE TABLE IF NOT EXISTS users (
      id            SERIAL PRIMARY KEY,
      email         TEXT UNIQUE NOT NULL,
      password_hash TEXT NOT NULL,
      full_name     TEXT NOT NULL DEFAULT '',
      role          TEXT NOT NULL DEFAULT 'user',
      created_at    TIMESTAMP DEFAULT NOW()
    )
  `);
  const r = await pg.query('SELECT COUNT(*) AS cnt FROM users');
  if (parseInt(r.rows[0].cnt, 10) === 0) {
    const hash = await bcrypt.hash('test123', 12);
    await pg.query(
      `INSERT INTO users (email, password_hash, full_name, role) VALUES ($1,$2,$3,'admin')`,
      ['jcolson@greenbushlogistics.com', hash, 'Jacob Colson']
    );
    console.log('[auth] Admin seeded — jcolson@greenbushlogistics.com / test123');
  }
}

// ── In-memory rate limiter (per email) ──────────────────────────────
const _rl = {};
function rlCheck(key) {
  const rec = _rl[key];
  if (!rec) return { locked: false };
  if (rec.lockedUntil > Date.now()) return { locked: true, mins: Math.ceil((rec.lockedUntil - Date.now()) / 60000) };
  return { locked: false };
}
function rlFail(key) {
  if (!_rl[key]) _rl[key] = { count: 0, lockedUntil: 0 };
  _rl[key].count++;
  if (_rl[key].count >= 5) _rl[key].lockedUntil = Date.now() + 15 * 60 * 1000;
}
function rlClear(key) { delete _rl[key]; }

// ── Auth middleware ──────────────────────────────────────────────────
function requireAuth(req, res, next) {
  if (req.session?.userId) return next();
  res.status(401).json({ error: 'Unauthorized' });
}
function requireAdmin(req, res, next) {
  if (req.session?.role !== 'admin') return res.status(403).json({ error: 'Forbidden' });
  next();
}

// Protect all /api/* except /api/auth/* with session auth
app.use((req, res, next) => {
  if (!req.path.startsWith('/api/') || req.path.startsWith('/api/auth/')) return next();
  requireAuth(req, res, next);
});

// ── POST /api/auth/login ─────────────────────────────────────────────
app.post('/api/auth/login', async (req, res) => {
  try {
    const { email, password } = req.body || {};
    if (!email || !password) return res.status(400).json({ error: 'Email and password required.' });
    const key = email.toLowerCase().trim();
    const rl = rlCheck(key);
    if (rl.locked) return res.status(429).json({ error: `Too many attempts. Try again in ${rl.mins} minute${rl.mins !== 1 ? 's' : ''}.` });

    const r = await pg.query('SELECT * FROM users WHERE email = $1', [key]);
    const user = r.rows[0];
    if (!user || !(await bcrypt.compare(password, user.password_hash))) {
      rlFail(key);
      const left = Math.max(0, 5 - (_rl[key]?.count || 0));
      if (left === 0) return res.status(429).json({ error: 'Too many failed attempts. Account locked for 15 minutes.' });
      return res.status(401).json({ error: `Incorrect email or password. ${left} attempt${left !== 1 ? 's' : ''} remaining.` });
    }

    rlClear(key);
    req.session.userId   = user.id;
    req.session.email    = user.email;
    req.session.role     = user.role;
    req.session.fullName = user.full_name;
    res.json({ ok: true, email: user.email, fullName: user.full_name, role: user.role });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── POST /api/auth/logout ────────────────────────────────────────────
app.post('/api/auth/logout', (req, res) => {
  req.session.destroy(() => res.json({ ok: true }));
});

// ── GET /api/auth/me ─────────────────────────────────────────────────
app.get('/api/auth/me', requireAuth, (req, res) => {
  res.json({ email: req.session.email, fullName: req.session.fullName, role: req.session.role });
});

// ── POST /api/auth/change-password ──────────────────────────────────
app.post('/api/auth/change-password', requireAuth, async (req, res) => {
  try {
    const { currentPassword, newPassword } = req.body || {};
    if (!currentPassword || !newPassword) return res.status(400).json({ error: 'Both passwords required.' });
    if (newPassword.length < 8) return res.status(400).json({ error: 'New password must be at least 8 characters.' });
    if (currentPassword === newPassword) return res.status(400).json({ error: 'New password must differ from current.' });
    const r = await pg.query('SELECT password_hash FROM users WHERE id = $1', [req.session.userId]);
    if (!r.rows[0] || !(await bcrypt.compare(currentPassword, r.rows[0].password_hash))) {
      return res.status(401).json({ error: 'Current password is incorrect.' });
    }
    const hash = await bcrypt.hash(newPassword, 12);
    await pg.query('UPDATE users SET password_hash = $1 WHERE id = $2', [hash, req.session.userId]);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── GET /api/admin/users ─────────────────────────────────────────────
app.get('/api/admin/users', requireAdmin, async (req, res) => {
  try {
    const r = await pg.query('SELECT id, email, full_name, role, created_at FROM users ORDER BY created_at');
    res.json({ ok: true, users: r.rows });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── POST /api/admin/users ────────────────────────────────────────────
app.post('/api/admin/users', requireAdmin, async (req, res) => {
  try {
    const { email, fullName } = req.body || {};
    if (!email || !fullName) return res.status(400).json({ error: 'Email and full name required.' });
    const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghjkmnpqrstuvwxyz23456789';
    let pwd = '';
    for (let i = 0; i < 10; i++) pwd += chars[Math.floor(Math.random() * chars.length)];
    const hash = await bcrypt.hash(pwd, 12);
    try {
      const r = await pg.query(
        `INSERT INTO users (email, password_hash, full_name, role) VALUES ($1,$2,$3,'user') RETURNING id, email, full_name, role`,
        [email.toLowerCase().trim(), hash, fullName.trim()]
      );
      res.json({ ok: true, user: r.rows[0], tempPassword: pwd });
    } catch (e) {
      if (e.code === '23505') return res.status(409).json({ error: 'A user with that email already exists.' });
      throw e;
    }
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── DELETE /api/admin/users/:id ──────────────────────────────────────
app.delete('/api/admin/users/:id', requireAdmin, async (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    if (id === req.session.userId) return res.status(400).json({ error: 'Cannot delete your own account.' });
    await pg.query('DELETE FROM users WHERE id = $1', [id]);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── POST /api/admin/users/:id/reset-password ─────────────────────────
app.post('/api/admin/users/:id/reset-password', requireAdmin, async (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghjkmnpqrstuvwxyz23456789';
    let pwd = '';
    for (let i = 0; i < 10; i++) pwd += chars[Math.floor(Math.random() * chars.length)];
    const hash = await bcrypt.hash(pwd, 12);
    const r = await pg.query('UPDATE users SET password_hash = $1 WHERE id = $2 RETURNING id', [hash, id]);
    if (!r.rows.length) return res.status(404).json({ error: 'User not found.' });
    res.json({ ok: true, tempPassword: pwd });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

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
  const laneSel   = (originCol && destCol)
    ? `, INITCAP(TRIM("${originCol}")) AS origin, UPPER(TRIM("origin_state")) AS origin_state, "origin_postal_code", INITCAP(TRIM("${destCol}")) AS dest, UPPER(TRIM("destination_state")) AS destination_state, "destination_postal_code"`
    : '';
  return `
    WITH deduped AS (
      SELECT DISTINCT ON (load_id)
        load_id, make_bid ${amountSel}
        ${dateCol ? `, "${dateCol}" AS ts` : ''}
        ${laneSel}
      FROM "${pgTable}"
      WHERE ${whereClause} AND ${US_FILTER}
      ORDER BY load_id, ${dateCol ? `"${dateCol}"` : '1'} DESC NULLS LAST
    )
  `;
}

const BID_FILTER = `LOWER(make_bid::text) IN ('1', 'true', 'yes')`;
// Canadian postal codes start with a letter — exclude non-US rows
const US_FILTER = `COALESCE(origin_postal_code, '') !~ '^[A-Za-z]' AND COALESCE(destination_postal_code, '') !~ '^[A-Za-z]'`;

// ── GET /api/bids/summary ────────────────────────────────────────────
app.get('/api/bids/summary', async (req, res) => {
  try {
  const ctx = await getBidContext();
  if (!ctx) return res.json({ ok: false, reason: 'no_data' });

  const { pgTable, amountCol, originCol, destCol, dateCol } = ctx;
  const mode      = req.query.mode === 'ta' ? 'ta' : 'spot';
  const days      = [7, 30, 90].includes(parseInt(req.query.days, 10)) ? parseInt(req.query.days, 10) : 30;
  const dateF     = dateCol ? `"${dateCol}" >= NOW() - INTERVAL '${days} days'` : 'TRUE';
  const baseWhere = `${accountFilter(mode)} AND ${dateF}`;
  const cte       = dedupCTE(pgTable, dateCol, amountCol, originCol, destCol, baseWhere);

  const avgSel   = amountCol ? `, AVG(amount) FILTER (WHERE ${BID_FILTER}) AS avg_amount` : '';
  const lanesSel = (originCol && destCol) ? `, COUNT(DISTINCT origin || '-' || dest) FILTER (WHERE ${BID_FILTER}) AS lanes` : '';
  const mktSel   = (originCol && destCol) ? `, COUNT(DISTINCT zm_o.market || '->' || zm_d.market) FILTER (WHERE ${BID_FILTER} AND zm_o.market IS NOT NULL AND zm_d.market IS NOT NULL) AS active_mkts` : '';
  const mktJoin  = (originCol && destCol) ? `LEFT JOIN zip_markets zm_o ON LEFT(deduped.origin_postal_code, 3) = zm_o.zip_prefix
    LEFT JOIN zip_markets zm_d ON LEFT(deduped.destination_postal_code, 3) = zm_d.zip_prefix` : '';

  const r = await pg.query(`${cte}
    SELECT COUNT(*) AS opportunities,
           COUNT(*) FILTER (WHERE ${BID_FILTER}) AS bids
           ${avgSel} ${lanesSel} ${mktSel}
    FROM deduped
    ${mktJoin}
  `);

  const row = r.rows[0];
  res.json({
    ok: true,
    opportunities: parseInt(row.opportunities, 10),
    totalBids:     parseInt(row.bids, 10),
    avgAmount:     row.avg_amount ? parseFloat(row.avg_amount) : null,
    activeLanes:   row.lanes ? parseInt(row.lanes, 10) : null,
    activeMkts:    row.active_mkts ? parseInt(row.active_mkts, 10) : null,
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
  const days      = [7, 30, 90].includes(parseInt(req.query.days, 10)) ? parseInt(req.query.days, 10) : 30;
  const baseWhere = dateCol
    ? `${accountFilter(mode)} AND "${dateCol}" >= NOW() - INTERVAL '${days} days'`
    : accountFilter(mode);
  const cte     = dedupCTE(pgTable, dateCol, amountCol, originCol, destCol, baseWhere);
  const avgSel  = (amountCol && mode === 'spot')
    ? `, AVG(amount) FILTER (WHERE ${BID_FILTER}) AS avg_amount
     , MIN(amount) FILTER (WHERE ${BID_FILTER}) AS min_amount
     , MAX(amount) FILTER (WHERE ${BID_FILTER}) AS max_amount`
    : '';

  // Lane-level top 50
  const laneRes = await pg.query(`${cte}
    SELECT origin, origin_state, dest, destination_state,
           COUNT(*) AS opportunities,
           COUNT(*) FILTER (WHERE ${BID_FILTER}) AS bids
    FROM deduped
    GROUP BY origin, origin_state, dest, destination_state
    HAVING COUNT(*) >= 5
    ORDER BY opportunities DESC
    LIMIT 50
  `);

  // 3DZ-level top 50
  const zipRes = await pg.query(`${cte}
    SELECT LEFT(origin_postal_code, 3)      AS orig_zip3,
           LEFT(destination_postal_code, 3) AS dest_zip3,
           COUNT(*) AS opportunities,
           COUNT(*) FILTER (WHERE ${BID_FILTER}) AS bids
    FROM deduped
    WHERE origin_postal_code IS NOT NULL AND destination_postal_code IS NOT NULL
    GROUP BY LEFT(origin_postal_code, 3), LEFT(destination_postal_code, 3)
    ORDER BY opportunities DESC
    LIMIT 50
  `);

  // Market-level top 50
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
    LIMIT 50
  `);

  res.json({
    ok: true,
    mode,
    rows: laneRes.rows.map(row => ({
      lane:          `${row.origin}, ${row.origin_state} → ${row.dest}, ${row.destination_state}`,
      opportunities: parseInt(row.opportunities, 10),
      bids:          parseInt(row.bids, 10),
    })),
    zipRows: zipRes.rows.map(row => ({
      zip:           `${row.orig_zip3} → ${row.dest_zip3}`,
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

// ── GET /api/bids/orders ─────────────────────────────────────────────
app.get('/api/bids/orders', async (req, res) => {
  try {
  const ctx = await getBidContext();
  if (!ctx) return res.json({ ok: false, reason: 'no_data', rows: [] });

  const { pgTable, dateCol } = ctx;
  const days       = [7, 30, 90].includes(parseInt(req.query.days, 10)) ? parseInt(req.query.days, 10) : 30;
  const dateFilter = dateCol ? `"${dateCol}" >= NOW() - INTERVAL '${days} days'` : 'TRUE';

  const r = await pg.query(`
    WITH orders AS (
      SELECT DISTINCT ON (load_id)
        load_id, account_name,
        ("bot_processed_record_at_raw" AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago') AS ts_chicago,
        INITCAP(TRIM(origin_city)) AS origin_city, UPPER(TRIM(origin_state)) AS origin_state, origin_postal_code,
        INITCAP(TRIM(destination_city)) AS destination_city, UPPER(TRIM(destination_state)) AS destination_state, destination_postal_code,
        distance_mi, make_bid, base_rate, bid_submitted
      FROM "${pgTable}"
      WHERE ${accountFilter(req.query.mode === 'ta' ? 'ta' : 'spot')} AND ${dateFilter} AND ${US_FILTER}
      ORDER BY load_id, "bot_processed_record_at_raw" DESC NULLS LAST
    )
    SELECT o.*,
      zm_o.market AS orig_mkt,
      zm_d.market AS dest_mkt
    FROM orders o
    LEFT JOIN zip_markets zm_o ON LEFT(o.origin_postal_code, 3) = zm_o.zip_prefix
    LEFT JOIN zip_markets zm_d ON LEFT(o.destination_postal_code, 3) = zm_d.zip_prefix
    ORDER BY ts_chicago DESC
  `);

  const bidVal = v => {
    const s = String(v ?? '').toLowerCase();
    if (['1','true','yes'].includes(s)) return 'Yes';
    if (['0','false','no'].includes(s)) return 'No';
    return s || '—';
  };
  const fmt = v => v != null && v !== '' ? v : '—';
  const fmtNum = v => v != null ? parseFloat(v).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : '—';

  res.json({
    ok: true,
    rows: r.rows.map(row => ({
      loadId:      fmt(row.load_id),
      date:        row.ts_chicago ? new Date(row.ts_chicago).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' }) : '—',
      accountName: fmt(row.account_name),
      origin:      row.origin_city ? `${row.origin_city}, ${row.origin_state}` : '—',
      destination: row.destination_city ? `${row.destination_city}, ${row.destination_state}` : '—',
      origMkt:     fmt(row.orig_mkt),
      destMkt:     fmt(row.dest_mkt),
      distanceMi:  row.distance_mi != null ? parseFloat(row.distance_mi).toLocaleString('en-US', { maximumFractionDigits: 0 }) : '—',
      makeBid:     bidVal(row.make_bid),
      baseRate:    fmtNum(row.base_rate),
      bidSubmitted:fmtNum(row.bid_submitted),
    })),
  });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ── Start ────────────────────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`Greenbush Brokerage Center running on port ${PORT}`);
  if (process.env.DATABASE_URL) {
    ensureUsers().catch(err => console.error('[auth] Setup error:', err.message));
  }
  if (process.env.SQLSERVER_HOST && process.env.DATABASE_URL) {
    startSyncCron();
  } else {
    console.warn('[sync] Skipping — SQLSERVER_HOST or DATABASE_URL not set.');
  }
});
