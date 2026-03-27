'use strict';

const sql  = require('mssql');
const { Pool } = require('pg');
const cron = require('node-cron');

// ── SQL Server config ────────────────────────────────────────────────
const MSSQL_CONFIG = {
  server:   process.env.SQLSERVER_HOST,
  database: process.env.SQLSERVER_DB,
  user:     process.env.SQLSERVER_USER,
  password: process.env.SQLSERVER_PASSWORD,
  port:     parseInt(process.env.SQLSERVER_PORT || '1433', 10),
  options: {
    encrypt: false,
    trustServerCertificate: true,
    connectTimeout: 30000,
    requestTimeout: 120000,
  },
};

// ── Sync configuration ───────────────────────────────────────────────
const SYNC_TABLE   = 'spotbidding';
const PG_TABLE     = 'sb_spotbidding';
const TIMESTAMP_COL = 'bot_processed_record_at_raw';
const CUTOFF_DATE  = '2025-10-01T05:00:00Z'; // midnight Chicago CDT (UTC-5)

const SYNC_COLUMNS = [
  'load_id',
  'shipper_code',
  'account_name',
  'pickup_date_time',
  'delivery_date_time',
  'origin_city',
  'origin_state',
  'origin_postal_code',
  'destination_city',
  'destination_state',
  'destination_postal_code',
  'accessorials',
  'stops',
  'equipment_type',
  'distance_mi',
  'make_bid',
  'bid_failure_reason',
  'base_rate',
  'bid_submitted',
  'pa_total',
  'bot_processed_record_at_raw',
  'submit_enabled',
  'action_type',
  'dat_timeframe',
];

// ── PostgreSQL pool ──────────────────────────────────────────────────
const pg = new Pool({ connectionString: process.env.DATABASE_URL });

// Strip unsafe characters from identifier names
function safeIdent(name) {
  return name.replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase();
}

// ── Ensure sync state table exists ──────────────────────────────────
async function ensureSyncStateTable() {
  await pg.query(`
    CREATE TABLE IF NOT EXISTS _sync_state (
      table_name TEXT PRIMARY KEY,
      pg_table   TEXT,
      last_sync  TIMESTAMP,
      row_count  INTEGER DEFAULT 0,
      ts_col     TEXT,
      updated_at TIMESTAMP DEFAULT NOW()
    )
  `);
}

// ── Create the mirror table in PostgreSQL if it doesn't exist ────────
async function ensureMirrorTable() {
  await pg.query(`
    CREATE TABLE IF NOT EXISTS "${PG_TABLE}" (
      load_id                    TEXT,
      shipper_code               TEXT,
      account_name               TEXT,
      pickup_date_time           TIMESTAMP,
      delivery_date_time         TIMESTAMP,
      origin_city                TEXT,
      origin_state               TEXT,
      origin_postal_code         TEXT,
      destination_city           TEXT,
      destination_state          TEXT,
      destination_postal_code    TEXT,
      accessorials               TEXT,
      stops                      TEXT,
      equipment_type             TEXT,
      distance_mi                NUMERIC(10,2),
      make_bid                   TEXT,
      bid_failure_reason         TEXT,
      base_rate                  NUMERIC(18,4),
      bid_submitted              TEXT,
      pa_total                   NUMERIC(18,4),
      bot_processed_record_at_raw TIMESTAMP,
      submit_enabled             TEXT,
      action_type                TEXT,
      dat_timeframe              TEXT
    )
  `);
}

// ── Get last sync timestamp ──────────────────────────────────────────
async function getLastSync() {
  const r = await pg.query(
    'SELECT last_sync FROM _sync_state WHERE table_name = $1',
    [SYNC_TABLE]
  );
  return r.rows[0]?.last_sync || null;
}

// ── Persist sync state ───────────────────────────────────────────────
async function updateSyncState(rowCount) {
  await pg.query(`
    INSERT INTO _sync_state (table_name, pg_table, last_sync, row_count, ts_col, updated_at)
    VALUES ($1, $2, NOW(), $3, $4, NOW())
    ON CONFLICT (table_name) DO UPDATE
      SET pg_table = $2, last_sync = NOW(), row_count = $3, ts_col = $4, updated_at = NOW()
  `, [SYNC_TABLE, PG_TABLE, rowCount, TIMESTAMP_COL]);
}

// ── Upsert a batch of rows into PostgreSQL ───────────────────────────
async function upsertBatch(rows) {
  const pgColNames = SYNC_COLUMNS.map(c => `"${safeIdent(c)}"`);

  const client = await pg.connect();
  try {
    await client.query('BEGIN');
    for (const row of rows) {
      const vals   = SYNC_COLUMNS.map(c => {
        const v = row[c];
        if (v === undefined || v === null || v === '') return null;
        // Reject invalid date objects (NaN dates from mssql driver)
        if (v instanceof Date) return isNaN(v.getTime()) ? null : v;
        // Reject NaN strings
        if (typeof v === 'string' && v.includes('NaN')) return null;
        return v;
      });
      const params = vals.map((_, i) => `$${i + 1}`).join(', ');

      // Upsert on load_id + bot_processed_record_at_raw as a composite key
      // since there's no declared PK — use INSERT ... ON CONFLICT DO NOTHING
      await client.query(
        `INSERT INTO "${PG_TABLE}" (${pgColNames.join(', ')}) VALUES (${params}) ON CONFLICT DO NOTHING`,
        vals
      );
    }
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

// ── Main sync ────────────────────────────────────────────────────────
async function runSync() {
  const start = Date.now();
  console.log(`[sync] Starting at ${new Date().toISOString()}`);

  let pool;
  try {
    pool = await sql.connect(MSSQL_CONFIG);
    await ensureSyncStateTable();
    await ensureMirrorTable();

    const lastSync = await getLastSync();
    const colSelect = SYNC_COLUMNS.map(c => `[${c}]`).join(', ');

    // Always filter to >= CUTOFF_DATE and optionally > lastSync for incremental
    const request = pool.request();
    request.input('cutoff', sql.DateTime2, new Date(CUTOFF_DATE));

    let query;
    if (lastSync) {
      request.input('lastSync', sql.DateTime2, lastSync);
      query = `SELECT ${colSelect} FROM [${SYNC_TABLE}]
               WHERE [${TIMESTAMP_COL}] >= @cutoff
               AND [${TIMESTAMP_COL}] > @lastSync`;
      console.log(`[sync] Incremental: pulling records after ${lastSync.toISOString()}`);
    } else {
      query = `SELECT ${colSelect} FROM [${SYNC_TABLE}]
               WHERE [${TIMESTAMP_COL}] >= @cutoff`;
      console.log(`[sync] Initial load: pulling all records from ${CUTOFF_DATE}`);
    }

    // Stream rows in batches to keep memory low
    const BATCH = 200;
    let batch = [];
    let totalSynced = 0;

    await new Promise((resolve, reject) => {
      request.stream = true;
      request.query(query);

      request.on('row', row => {
        batch.push(row);
        if (batch.length >= BATCH) {
          request.pause();
          const chunk = batch.splice(0);
          upsertBatch(chunk)
            .then(() => {
              totalSynced += chunk.length;
              console.log(`[sync] ${totalSynced} rows written…`);
              request.resume();
            })
            .catch(reject);
        }
      });

      request.on('done', () => {
        upsertBatch(batch)
          .then(() => {
            totalSynced += batch.length;
            resolve();
          })
          .catch(reject);
      });

      request.on('error', reject);
    });

    await updateSyncState(totalSynced);
    const duration = Date.now() - start;
    console.log(`[sync] Done — ${totalSynced} rows in ${duration}ms`);
    return { ok: true, synced: totalSynced, duration };

  } catch (err) {
    console.error('[sync] Fatal:', err.message);
    return { ok: false, error: err.message, duration: Date.now() - start };
  } finally {
    if (pool) {
      try { await pool.close(); } catch (_) {}
    }
  }
}

// ── Start cron: run immediately on startup, then every 30 minutes ────
function startSyncCron() {
  console.log('[sync] Cron started — running now, then every 30 minutes.');
  runSync();
  cron.schedule('*/30 * * * *', () => runSync());
}

module.exports = { startSyncCron, runSync, pg };
