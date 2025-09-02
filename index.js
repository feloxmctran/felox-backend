require("dotenv").config();
const express = require("express");
const cors = require("cors");
const bodyParser = require("body-parser");
const { Pool } = require("pg");
const crypto = require("crypto");


const app = express();
app.use(cors());
app.use(express.json({ limit: "5mb" }));
app.use(bodyParser.urlencoded({ extended: true, limit: "5mb" }));
// === SSE (Server-Sent Events) — duello bildirimleri ===
const sseClients = new Map(); // Map<userId:number, Set<res>>

function sseAdd(userId, res) {
  const uid = Number(userId);
  if (!sseClients.has(uid)) sseClients.set(uid, new Set());
  sseClients.get(uid).add(res);
}

function sseRemove(userId, res) {
  const uid = Number(userId);
  const set = sseClients.get(uid);
  if (!set) return;
  set.delete(res);
  if (set.size === 0) sseClients.delete(uid);
}

function sseEmit(userId, event, payload = {}) {
  const uid = Number(userId);
  const set = sseClients.get(uid);
  if (!set || set.size === 0) return;

  // Named event
  const named = `event: ${event}\ndata: ${JSON.stringify(payload)}\n\n`;

  // Fallback: default "message" event (type alanı ile)
  const fallback = `data: ${JSON.stringify({ type: event, ...payload })}\n\n`;

  for (const r of set) {
    try {
      r.write(named);
      r.write(fallback); // sadece onmessage dinleyen FE için
    } catch (_) {}
  }
}



/* ---------- PG CONNECTION ---------- */
if (!process.env.DATABASE_URL) {
  console.error("DATABASE_URL env yok! Neon connection string'i .env içine ekleyin.");
  process.exit(1);
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  keepAlive: true,
  max: 5,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
});

pool.on("error", (err) => {
  console.error("PG pool error:", err.message);
});

pool
  .query("SELECT 1")
  .then(() => console.log("PostgreSQL bağlantısı başarılı"))
  .catch((e) => {
    console.error("PostgreSQL bağlantı hatası:", e);
    process.exit(1);
  });

/* ---------- MINI DB HELPERS ---------- */
async function run(sql, params = []) { await pool.query(sql, params); return { success: true }; }
async function get(sql, params = []) { const { rows } = await pool.query(sql, params); return rows[0] || null; }
async function all(sql, params = []) { const { rows } = await pool.query(sql, params); return rows; }

function randomReadableCode(len = 6) {
  const alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"; // O, I, 0, 1 yok
  let out = "";
  for (let i = 0; i < len; i++) {
    out += alphabet[crypto.randomInt(0, alphabet.length)];
  }
  return out;
}

async function ensureUserCodeForUser(userId) {
  const row = await get(`SELECT user_code FROM users WHERE id=$1`, [userId]);
  if (!row) return null;
  if (row.user_code) return row.user_code;

  // Çakışmaları önlemek için 10 kez dene
  for (let i = 0; i < 10; i++) {
    const code = randomReadableCode(6);
    const upd = await get(
      `UPDATE users
          SET user_code = $2
        WHERE id = $1
          AND user_code IS NULL
          AND NOT EXISTS (SELECT 1 FROM users WHERE user_code = $2)
      RETURNING user_code`,
      [userId, code]
    );
    if (upd?.user_code) return upd.user_code;
  }
  // eşzamanlı üretim olduysa son halini dön
  const last = await get(`SELECT user_code FROM users WHERE id=$1`, [userId]);
  return last?.user_code || null;
}


/* ---------- ENV (Günlük Yarışma) ---------- */
const DAILY_CONTEST_SIZE = Math.max(1, parseInt(process.env.DAILY_CONTEST_SIZE || "128", 10));
const DAILY_CONTEST_SECRET = process.env.DAILY_CONTEST_SECRET || "felox-secret";

/* ---------- ENV (Düello) ---------- */
// Idle kalan maçları kaç saniyede "abandoned" yapalım? (min 30, max 600)
const DUELLO_IDLE_ABORT_SEC = Math.max(
  30,
  Math.min(600, parseInt(process.env.DUELLO_IDLE_ABORT_SEC || "90", 10))
);

/* ---------- ENV (Düello zamanlama) ---------- */
const DUELLO_PER_Q_SEC  = Math.max(5, parseInt(process.env.DUELLO_PER_Q_SEC || "16", 10));
const DUELLO_REVEAL_SEC = Math.max(1, parseInt(process.env.DUELLO_REVEAL_SEC || "3", 10));



/* ---------- HEALTH ---------- */
app.get("/", (_req, res) => res.send("OK"));
app.get("/healthz", (_req, res) => res.send("healthy"));


// === DUELLO SSE stream ===
// Tarayıcı: new EventSource(`${API}/api/duello/events/${userId}`)
app.get("/api/duello/events/:userId", (req, res) => {
  const uid = Number(req.params.userId);
  if (!uid) return res.status(400).end();

  // SSE başlıkları
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.setHeader("X-Accel-Buffering", "no"); // proxy buffer kapatma
  // CORS (bazı proxy katmanlarında gerekli olabiliyor)
  res.setHeader("Access-Control-Allow-Origin", "*");

  // İlk flush (varsa)
  try { res.flushHeaders?.(); } catch {}

  // Kayda al
  sseAdd(uid, res);

  // Hoş geldin olayı
  res.write(`event: ready\ndata: {}\n\n`);

  // Bağlantıyı canlı tutmak için ping (Render/Vercel timeoutlarını önler)
  const ping = setInterval(() => {
    try { res.write(`: ping ${Date.now()}\n\n`); } catch {}
  }, 25000);

  // Bağlantı kapanınca temizle
  req.on("close", () => {
    clearInterval(ping);
    sseRemove(uid, res);
    try { res.end(); } catch {}
  });
});


/* ---------- HELPERS ---------- */
function periodSql(period, alias = "a") {
  const col = `${alias}.created_at`;
  const tzNow = `timezone('Europe/Istanbul', now())`;
  switch ((period || "all")) {
    case "today":
      return `AND ${col} >= date_trunc('day', ${tzNow})
              AND ${col} <  date_trunc('day', ${tzNow}) + interval '1 day'`;
    case "week":
      return `AND ${col} >= date_trunc('week', ${tzNow})
              AND ${col} <  date_trunc('week', ${tzNow}) + interval '1 week'`;
    case "month":
      return `AND ${col} >= date_trunc('month', ${tzNow})
              AND ${col} <  date_trunc('month', ${tzNow}) + interval '1 month'`;
    case "year":
      return `AND ${col} >= date_trunc('year', ${tzNow})
              AND ${col} <  date_trunc('year', ${tzNow}) + interval '1 year'`;
    default:
      return "";
  }
}

function normalizeAnswer(v) {
  if (v == null) return "";
  let s = String(v).trim().toLowerCase();
  if (s === "hayir") s = "hayır";
  if (["evet", "hayır", "bilmem"].includes(s)) return s;
  if (["yes", "true", "1"].includes(s)) return "evet";
  if (["no", "false", "0"].includes(s)) return "hayır";
  if (["dontknow", "unknown", "idk", "skip", "empty", "null"].includes(s)) return "bilmem";
  return s;
}

async function getDayKey() {
  const row = await get(`SELECT to_char(timezone('Europe/Istanbul', now()), 'YYYY-MM-DD') AS day`);
  return row?.day || new Date().toISOString().slice(0, 10);
}

/* === bonus hesabı + streak güncelleme === */
function streakBonus(s) {
  if (s >= 365) return 10;
  if (s >= 100) return 6;
  if (s >= 60)  return 5;
  if (s >= 30)  return 4;
  if (s >= 14)  return 3;
  if (s >= 7)   return 2;
  if (s >= 3)   return 1;
  return 0;
}

/** Bugünü bitirince streak’i günceller (idempotent) */
async function upsertDailyStreakOnFinish(userId, dayKey) {
  const st = await get(
    `SELECT COALESCE(current_streak,0) AS cur,
            COALESCE(longest_streak,0) AS best,
            last_day_key::text         AS last
       FROM user_daily_streak
      WHERE user_id=$1`,
    [userId]
  );

  if (st?.last === dayKey) return;

  let next = 1;
  if (st?.last) {
    const cont = await get(
      `SELECT (($1::date - interval '1 day')::date = $2::date) AS ok`,
      [dayKey, st.last]
    );
    next = cont?.ok ? (st.cur + 1) : 1;
  }

  const longest = Math.max(st?.best || 0, next);

  await run(
    `INSERT INTO user_daily_streak (user_id, current_streak, longest_streak, last_day_key)
     VALUES ($1,$2,$3,$4)
     ON CONFLICT (user_id) DO UPDATE
       SET current_streak = EXCLUDED.current_streak,
           longest_streak = GREATEST(user_daily_streak.longest_streak, EXCLUDED.longest_streak),
           last_day_key   = EXCLUDED.last_day_key
    `,
    [userId, next, longest, dayKey]
  );
}

/* === YESTERDAY KEY === */
async function getYesterdayKey() {
  const row = await get(`SELECT to_char(timezone('Europe/Istanbul', now()) - interval '1 day', 'YYYY-MM-DD') AS day`);
  return row?.day;
}

/* ---------- DB INIT (tablolar) ---------- */
async function init() {
  await run(`CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    ad TEXT,
    soyad TEXT,
    yas INTEGER,
    cinsiyet TEXT,
    meslek TEXT,
    sehir TEXT,
    email TEXT UNIQUE,
    password TEXT,
    role TEXT
  )`);

await run(`
  ALTER TABLE users
    ADD COLUMN IF NOT EXISTS user_code TEXT
`);
await run(`
  CREATE UNIQUE INDEX IF NOT EXISTS uq_users_user_code
  ON users(user_code) WHERE user_code IS NOT NULL
`);


  await run(`CREATE TABLE IF NOT EXISTS surveys (
    id SERIAL PRIMARY KEY,
    editor_id INTEGER REFERENCES users(id),
    title TEXT,
    start_date TEXT,
    end_date TEXT,
    category TEXT,
    status TEXT DEFAULT 'pending'
  )`);

  await run(`CREATE TABLE IF NOT EXISTS questions (
    id SERIAL PRIMARY KEY,
    survey_id INTEGER REFERENCES surveys(id) ON DELETE CASCADE,
    question TEXT,
    correct_answer TEXT,
    point INTEGER DEFAULT 1
  )`);

  await run(`
    ALTER TABLE questions
    ADD COLUMN IF NOT EXISTS qtype integer DEFAULT 1
  `);

  await run(`CREATE TABLE IF NOT EXISTS answers (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    question_id INTEGER REFERENCES questions(id) ON DELETE CASCADE,
    answer TEXT,
    is_correct INTEGER,
    created_at TIMESTAMP DEFAULT NOW(),
    max_time_seconds integer,
    time_left_seconds integer,
    earned_seconds integer,
    is_daily boolean DEFAULT false,
    daily_key text
  )`);

  await run(`
    ALTER TABLE answers
      ADD COLUMN IF NOT EXISTS max_time_seconds integer,
      ADD COLUMN IF NOT EXISTS time_left_seconds integer,
      ADD COLUMN IF NOT EXISTS earned_seconds integer,
      ADD COLUMN IF NOT EXISTS is_daily boolean DEFAULT false,
      ADD COLUMN IF NOT EXISTS daily_key text
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS daily_contest_questions (
      day_key     TEXT NOT NULL,
      contest_date DATE,
      pos         INTEGER,
      seq         INTEGER,
      question_id INTEGER REFERENCES questions(id) ON DELETE CASCADE,
      PRIMARY KEY (day_key, pos)
    )
  `);
  await run(`CREATE INDEX IF NOT EXISTS idx_dcq_day ON daily_contest_questions (day_key)`);

  await run(`
    CREATE TABLE IF NOT EXISTS daily_sessions (
      id SERIAL PRIMARY KEY,
      user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
      day_key TEXT NOT NULL,
      current_index INTEGER DEFAULT 0,
      started_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW(),
      finished BOOLEAN DEFAULT false,
      UNIQUE(user_id, day_key)
    )
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS daily_finisher_awards (
      user_id   INTEGER REFERENCES users(id) ON DELETE CASCADE,
      day_key   TEXT NOT NULL,
      amount    INTEGER NOT NULL DEFAULT 2,
      created_at TIMESTAMP DEFAULT timezone('Europe/Istanbul', now()),
      PRIMARY KEY (user_id, day_key)
    )
  `);

  await run(`CREATE TABLE IF NOT EXISTS quotes (
    id SERIAL PRIMARY KEY,
    text TEXT NOT NULL,
    author TEXT
  )`);

  await run(`
    CREATE TABLE IF NOT EXISTS impdays (
      day_key TEXT PRIMARY KEY,
      daytitle TEXT NOT NULL,
      description TEXT
    )
  `);

  await run(`
    ALTER TABLE users
    ADD COLUMN IF NOT EXISTS books integer DEFAULT 0
  `);

  await run(`
    ALTER TABLE users
    ADD COLUMN IF NOT EXISTS ladder_best_level integer DEFAULT 0
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS book_awards (
      id SERIAL PRIMARY KEY,
      user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
      day_key TEXT NOT NULL,
      rank INTEGER NOT NULL,
      amount INTEGER NOT NULL,
      created_at TIMESTAMP DEFAULT NOW(),
      UNIQUE(user_id, day_key)
    )
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS book_spends (
      id SERIAL PRIMARY KEY,
      user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
      question_id INTEGER REFERENCES questions(id) ON DELETE SET NULL,
      day_key TEXT,
      amount INTEGER DEFAULT 1,
      created_at TIMESTAMP DEFAULT NOW()
    )
  `);

  await run(`
    ALTER TABLE answers
      ADD COLUMN IF NOT EXISTS bonus_points integer DEFAULT 0,
      ADD COLUMN IF NOT EXISTS streak_len_at_answer integer
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS user_daily_streak (
      user_id        INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
      current_streak INTEGER NOT NULL DEFAULT 0,
      longest_streak INTEGER NOT NULL DEFAULT 0,
      last_day_key   DATE
    )
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS ladder_sessions (
      user_id        INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
      run_started_at TIMESTAMP DEFAULT timezone('Europe/Istanbul', now()),
      current_level  INTEGER NOT NULL DEFAULT 1,
      attempts       INTEGER NOT NULL DEFAULT 0,  -- bilmem hariç deneme
      correct        INTEGER NOT NULL DEFAULT 0,  -- doğru sayısı
      updated_at     TIMESTAMP DEFAULT timezone('Europe/Istanbul', now())
    )
  `);

  await run(`CREATE INDEX IF NOT EXISTS idx_answers_daily ON answers (is_daily, daily_key, user_id)`);
  await run(`CREATE INDEX IF NOT EXISTS idx_answers_user_q ON answers (user_id, question_id)`);
  await run(`CREATE INDEX IF NOT EXISTS idx_book_awards_rank_day ON book_awards (rank, day_key)`);
  await run(`CREATE INDEX IF NOT EXISTS idx_book_awards_user ON book_awards (user_id)`);
  await run(`CREATE INDEX IF NOT EXISTS idx_ladder_sessions_user_level ON ladder_sessions (user_id, current_level)`);

// === DUELLO SCHEMA ===

// 1) MATCHES (önce ana tablo)
await run(`
  CREATE TABLE IF NOT EXISTS duello_matches (
    id               BIGSERIAL PRIMARY KEY,
    mode             TEXT    NOT NULL CHECK (mode IN ('info','speed')),
    user_a_id        INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    user_b_id        INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT timezone('Europe/Istanbul', now()),
    finished_at      TIMESTAMPTZ,
    state            TEXT    NOT NULL DEFAULT 'active' CHECK (state IN ('active','finished','abandoned')),
    total_questions  INTEGER NOT NULL DEFAULT 12,
    current_index    INTEGER NOT NULL DEFAULT 0,
    last_seen_a      TIMESTAMPTZ,
    last_seen_b      TIMESTAMPTZ,
    last_activity_at TIMESTAMPTZ,
    abandoned_at     TIMESTAMPTZ,
    ended_reason     TEXT,
    CHECK (user_a_id <> user_b_id)
  )
`);

// 2) INVITES
await run(`
  CREATE TABLE IF NOT EXISTS duello_invites (
    id            BIGSERIAL PRIMARY KEY,
    from_user_id  INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    to_user_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    mode          TEXT    NOT NULL CHECK (mode IN ('info','speed')),
    status        TEXT    NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','accepted','rejected','cancelled','expired')),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT timezone('Europe/Istanbul', now()),
    expire_at     TIMESTAMPTZ NOT NULL DEFAULT (timezone('Europe/Istanbul', now()) + interval '5 minutes'),
    accepted_at   TIMESTAMPTZ,
    rejected_at   TIMESTAMPTZ,
    cancelled_at  TIMESTAMPTZ,
    match_id      INTEGER  -- isteğe bağlı; FK yok
  )
`);

// 3) MATCH_QUESTIONS (matches -> questions FK)
await run(`
  CREATE TABLE IF NOT EXISTS duello_match_questions (
    match_id    INTEGER NOT NULL REFERENCES duello_matches(id) ON DELETE CASCADE,
    pos         INTEGER NOT NULL,
    question_id INTEGER NOT NULL REFERENCES questions(id) ON DELETE CASCADE,
    PRIMARY KEY (match_id, pos)
  )
`);

// 4) ANSWERS (matches & questions FK)
await run(`
  CREATE TABLE IF NOT EXISTS duello_answers (
    id                BIGSERIAL PRIMARY KEY,
    match_id          INTEGER NOT NULL REFERENCES duello_matches(id) ON DELETE CASCADE,
    question_id       INTEGER NOT NULL REFERENCES questions(id) ON DELETE CASCADE,
    user_id           INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    answer            TEXT    NOT NULL,
    is_correct        INTEGER NOT NULL DEFAULT 0,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT timezone('Europe/Istanbul', now()),
    max_time_seconds  INTEGER,
    time_left_seconds INTEGER,
    earned_seconds    INTEGER
  )
`);

// 5) PROFILES
await run(`
  CREATE TABLE IF NOT EXISTS duello_profiles (
    user_id         INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    ready           BOOLEAN NOT NULL DEFAULT FALSE,
    visibility_mode TEXT    NOT NULL DEFAULT 'public' CHECK (visibility_mode IN ('public','friends','none')),
    last_ready_at   TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT timezone('Europe/Istanbul', now()),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT timezone('Europe/Istanbul', now())
  )
`);

// --- DUELLO: Eski tablolar için güvenli migrasyonlar (kolon eklemeleri) ---
await run(`
  ALTER TABLE duello_matches
    ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS state TEXT,
    ADD COLUMN IF NOT EXISTS total_questions INTEGER,
    ADD COLUMN IF NOT EXISTS current_index INTEGER,
    ADD COLUMN IF NOT EXISTS last_seen_a TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_seen_b TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_activity_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS abandoned_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS ended_reason TEXT
`);


await run(`UPDATE duello_matches SET state = COALESCE(state,'active')`);
await run(`ALTER TABLE duello_matches ALTER COLUMN state SET DEFAULT 'active'`);
await run(`ALTER TABLE duello_matches ALTER COLUMN state SET NOT NULL`);

await run(`UPDATE duello_matches SET total_questions = COALESCE(total_questions,12)`);
await run(`ALTER TABLE duello_matches ALTER COLUMN total_questions SET DEFAULT 12`);
await run(`ALTER TABLE duello_matches ALTER COLUMN total_questions SET NOT NULL`);

await run(`UPDATE duello_matches SET current_index = COALESCE(current_index,0)`);
await run(`ALTER TABLE duello_matches ALTER COLUMN current_index SET DEFAULT 0`);
await run(`ALTER TABLE duello_matches ALTER COLUMN current_index SET NOT NULL`);

await run(`
  ALTER TABLE duello_invites
    ADD COLUMN IF NOT EXISTS mode TEXT,
    ADD COLUMN IF NOT EXISTS status TEXT,
    ADD COLUMN IF NOT EXISTS expire_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS accepted_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS rejected_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS cancelled_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS match_id INTEGER
`);
await run(`UPDATE duello_invites SET mode = COALESCE(mode,'info')`);
await run(`UPDATE duello_invites SET status = COALESCE(status,'pending')`);
await run(`UPDATE duello_invites SET expire_at = COALESCE(expire_at, timezone('Europe/Istanbul', now()) + interval '5 minutes')`);

await run(`
  ALTER TABLE duello_answers
    ADD COLUMN IF NOT EXISTS max_time_seconds INTEGER,
    ADD COLUMN IF NOT EXISTS time_left_seconds INTEGER,
    ADD COLUMN IF NOT EXISTS earned_seconds INTEGER
`);

// --- DUELLO: CHECK constraint düzeltmeleri + veri normalizasyonu ---
try {
  // state değerlerini güvene al
  await run(`UPDATE duello_matches
               SET state = COALESCE(state,'active')
             WHERE state IS NULL
                OR state NOT IN ('active','finished','abandoned')`);

  // mevcut "state" CHECK kısıtını kaldır ve yenisini ekle
  await run(`ALTER TABLE duello_matches DROP CONSTRAINT IF EXISTS duello_matches_state_chk`);
  await run(`ALTER TABLE duello_matches
               ADD CONSTRAINT duello_matches_state_chk
               CHECK (state IN ('active','finished','abandoned'))`);
  await run(`ALTER TABLE duello_matches ALTER COLUMN state SET DEFAULT 'active'`);

  // mode için de benzer güvenlik (eski şemalardan gelebilir)
  await run(`UPDATE duello_matches
               SET mode = CASE WHEN mode IN ('info','speed') THEN mode ELSE 'info' END`);
  await run(`ALTER TABLE duello_matches DROP CONSTRAINT IF EXISTS duello_matches_mode_check`);
  await run(`ALTER TABLE duello_matches DROP CONSTRAINT IF EXISTS duello_matches_mode_chk`);
  await run(`ALTER TABLE duello_matches
               ADD CONSTRAINT duello_matches_mode_chk
               CHECK (mode IN ('info','speed'))`);
} catch (e) {
  console.error("duello_matches constraint fix:", e.message);
}


// --- Indexler ---
await run(`
  CREATE UNIQUE INDEX IF NOT EXISTS uq_duello_invites_pending_pair
    ON duello_invites (from_user_id, to_user_id)
    WHERE status = 'pending'
`);
await run(`
  CREATE INDEX IF NOT EXISTS idx_duello_invites_to_pending
    ON duello_invites (to_user_id, status, expire_at DESC)
`);
await run(`
  CREATE INDEX IF NOT EXISTS idx_duello_invites_from_pending
    ON duello_invites (from_user_id, status, expire_at DESC)
`);
await run(`
  CREATE UNIQUE INDEX IF NOT EXISTS uq_duello_answers_once
    ON duello_answers (match_id, question_id, user_id)
`);
await run(`
  CREATE INDEX IF NOT EXISTS idx_duello_answers_q
    ON duello_answers (match_id, question_id)
`);
await run(`
  CREATE INDEX IF NOT EXISTS idx_duello_answers_u
    ON duello_answers (match_id, user_id)
`);
await run(`CREATE INDEX IF NOT EXISTS idx_duello_active_a   ON duello_matches (user_a_id) WHERE state='active'`);
await run(`CREATE INDEX IF NOT EXISTS idx_duello_active_b   ON duello_matches (user_b_id) WHERE state='active'`);
await run(`CREATE INDEX IF NOT EXISTS idx_duello_active_last ON duello_matches (state, last_activity_at)`);

}



init()
  .then(() => {
    console.log("PostgreSQL tablolar hazır");
    awardSchedulerTick();
    setInterval(awardSchedulerTick, 5 * 60 * 1000);
       // Düello: idle/abandon süpürücü (her 30 sn’de bir)
    const idleSweepMs = 30 * 1000;
    duelloIdleAbortTick();               // bir defa hemen çalıştır
    setInterval(duelloIdleAbortTick, idleSweepMs);

  })
  .catch(e => { console.error(e); process.exit(1); });

// --- DUELLO: idle-timeout yardımcı fonksiyon (şimdilik sadece tanımlı) ---
async function duelloIdleAbortTick() {
  try {
    const sec = DUELLO_IDLE_ABORT_SEC;
    const updated = await all(`
      WITH upd AS (
        UPDATE duello_matches
           SET state='abandoned',
               ended_reason='idle_timeout',
               finished_at = timezone('Europe/Istanbul', now()),
               abandoned_at = timezone('Europe/Istanbul', now())
         WHERE state='active'
           AND (
                COALESCE(last_seen_a, TIMESTAMP 'epoch') < timezone('Europe/Istanbul', now()) - interval '${sec} seconds'
             OR COALESCE(last_seen_b, TIMESTAMP 'epoch') < timezone('Europe/Istanbul', now()) - interval '${sec} seconds'
           )
           AND current_index < total_questions
         RETURNING id, user_a_id, user_b_id
      )
      SELECT id, user_a_id, user_b_id FROM upd
    `);

    if (updated?.length) {
      for (const m of updated) {
        try {
          sseEmit(Number(m.user_a_id), "match:abandoned", { match_id: Number(m.id) });
          sseEmit(Number(m.user_b_id), "match:abandoned", { match_id: Number(m.id) });
        } catch (_) {}
      }
    }
  } catch (e) {
    console.error("duelloIdleAbortTick hata:", e.message);
  }
}



/* ---------- AUTH ---------- */
app.post("/api/register", async (req, res) => {
  try {
    const { ad, soyad, yas, cinsiyet, meslek, sehir, email, password, role } = req.body;
    await run(
      `INSERT INTO users (ad, soyad, yas, cinsiyet, meslek, sehir, email, password, role)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
      [ad, soyad, yas, cinsiyet, meslek, sehir, email, password, role]
    );
    const user = await get(
  `SELECT id, ad, soyad, email, role, cinsiyet, user_code FROM users WHERE email=$1`,
  [email]
);

if (user && !user.user_code) {
  user.user_code = await ensureUserCodeForUser(user.id);
}


    res.json({ success: true, user });
  } catch (err) {
    if (String(err.message).includes("duplicate key")) return res.status(400).json({ error: "Bu e-posta zaten kayıtlı." });
    res.status(500).json({ error: "Kayıt başarısız." });
  }
});

app.get("/api/user/:userId/exists", async (req, res) => {
  const user = await get(`SELECT id FROM users WHERE id=$1`, [req.params.userId]);
  res.json({ exists: !!user });
});

app.post("/api/login", async (req, res) => {
  try {
    const { email, password } = req.body;
    const user = await get(
  `SELECT id, ad, soyad, email, role, cinsiyet, user_code
   FROM users WHERE email=$1 AND password=$2`,
  [email, password]
);

if (user && !user.user_code) {
  user.user_code = await ensureUserCodeForUser(user.id);
}


    if (!user) return res.status(401).json({ error: "E-posta veya şifre yanlış." });
    res.json({ success: true, user });
  } catch {
    res.status(500).json({ error: "Sunucu hatası." });
  }
});

/* ---------- EDITOR ---------- */
app.post("/api/surveys", async (req, res) => {
  try {
    const { editor_id, title, start_date, end_date, category, questions } = req.body;
    const ins = await get(
      `INSERT INTO surveys (editor_id, title, start_date, end_date, category, status)
       VALUES ($1,$2,$3,$4,$5,'pending') RETURNING id`,
      [editor_id, title, start_date, end_date, category]
    );
    const surveyId = ins.id;
    if (Array.isArray(questions) && questions.length) {
      for (const q of questions) {
        let p = 1;
        if (typeof q.point === "number" && q.point >= 1 && q.point <= 10) p = q.point;
        const ca = normalizeAnswer(q.correct_answer);
        await run(
          `INSERT INTO questions (survey_id, question, correct_answer, point) VALUES ($1,$2,$3,$4)`,
          [surveyId, q.question, ca, p]
        );
      }
    }
    res.json({ success: true, survey_id: surveyId });
  } catch {
    res.status(500).json({ error: "Anket kaydedilemedi!" });
  }
});

app.get("/api/editor/:editorId/surveys", async (req, res) => {
  try {
    const rows = await all(
      `SELECT * FROM surveys WHERE editor_id=$1 AND status!='deleted' ORDER BY id DESC`,
      [req.params.editorId]
    );
    res.json({ success: true, surveys: rows });
  } catch { res.status(500).json({ error: "Listeleme hatası!" }); }
});

app.get("/api/surveys/:surveyId/details", async (req, res) => {
  try {
    const surveyId = req.params.surveyId;
    const survey = await get(`SELECT * FROM surveys WHERE id=$1`, [surveyId]);
    if (!survey) return res.status(404).json({ error: "Anket bulunamadı" });
    const questions = await all(`SELECT * FROM questions WHERE survey_id=$1 ORDER BY id ASC`, [surveyId]);
    res.json({ success: true, survey, questions });
  } catch { res.status(500).json({ error: "Sorular bulunamadı!" }); }
});

// === SURVEY QUESTIONS (FE'nin beklediği uç) ===
app.get("/api/surveys/:surveyId/questions", async (req, res) => {
  try {
    const surveyId = req.params.surveyId;

    // Anket var mı? (silinmemiş)
    const sv = await get(
      `SELECT id FROM surveys WHERE id = $1 AND status != 'deleted'`,
      [surveyId]
    );
    if (!sv) return res.status(404).json({ error: "Anket bulunamadı" });

    // FE'nin kullandığı alanlar: id, survey_id, question, point
    const rows = await all(
      `SELECT id, survey_id, question, point
         FROM questions
        WHERE survey_id = $1
        ORDER BY id ASC`,
      [surveyId]
    );

    res.json({ success: true, questions: rows });
  } catch (e) {
    res.status(500).json({ error: "Sorular alınamadı: " + e.message });
  }
});

app.post("/api/surveys/:surveyId/delete", async (req, res) => {
  try { await run(`UPDATE surveys SET status='deleted' WHERE id=$1`, [req.params.surveyId]); res.json({ success: true }); }
  catch { res.status(500).json({ error: "Silinemedi." }); }
});

app.get("/api/admin/surveys", async (_req, res) => {
  try {
    const rows = await all(
      `SELECT surveys.*, users.ad as editor_ad, users.soyad as editor_soyad
       FROM surveys
       LEFT JOIN users ON surveys.editor_id = users.id
       WHERE surveys.status != 'deleted'
       ORDER BY surveys.id DESC`
    );
    res.json({ success: true, surveys: rows });
  } catch { res.status(500).json({ error: "Listeleme hatası!" }); }
});

app.post("/api/surveys/:surveyId/status", async (req, res) => {
  const { status } = req.body;
  if (!["approved", "rejected"].includes(status)) return res.status(400).json({ error: "Geçersiz durum!" });
  try { await run(`UPDATE surveys SET status=$1 WHERE id=$2`, [status, req.params.surveyId]); res.json({ success: true }); }
  catch { res.status(500).json({ error: "Durum güncellenemedi." }); }
});

app.post("/api/questions/:questionId/delete", async (req, res) => {
  try { await run(`DELETE FROM questions WHERE id=$1`, [req.params.questionId]); res.json({ success: true }); }
  catch { res.status(500).json({ error: "Soru silinemedi." }); }
});

app.post("/api/surveys/:surveyId/questions/bulk", async (req, res) => {
  const surveyId = req.params.surveyId;
  const { questions } = req.body;
  if (!Array.isArray(questions) || !questions.length) return res.status(400).json({ error: "questions boş olamaz." });
  const sv = await get(`SELECT id FROM surveys WHERE id=$1 AND status!='deleted'`, [surveyId]);
  if (!sv) return res.status(404).json({ error: "Anket bulunamadı." });
  try {
    for (const q of questions) {
      const text = (q.question || "").toString().trim();
      let point = Number(q.point) || 1; point = Math.min(10, Math.max(1, point));
      const ca = normalizeAnswer(q.correct_answer);
      if (!text) return res.status(400).json({ error: "Boş question var" });
      if (!["evet", "hayır", "bilmem"].includes(ca)) return res.status(400).json({ error: "correct_answer evet/hayır/bilmem olmalı" });
      await run(`INSERT INTO questions (survey_id, question, correct_answer, point) VALUES ($1,$2,$3,$4)`,
        [surveyId, text, ca, point]);
    }
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: "Toplu ekleme hatası: " + e.message }); }
});

/* ---------- QUOTES ---------- */
app.get("/api/quotes/random", async (_req, res) => {
  try {
    const row = await get(`SELECT text, author FROM quotes ORDER BY RANDOM() LIMIT 1`);
    if (!row) return res.status(404).json({ error: "Henüz hiç söz eklenmemiş." });
    res.json(row);
  } catch (err) {
    console.error("Quote çekme hatası:", err);
    res.status(500).json({ error: "Söz alınamadı." });
  }
});

/* ---------- USER ---------- */

// -- RO: user_code oku
app.get("/api/user/:userId/user-code", async (req, res) => {
  try {
    const uid = Number(req.params.userId);
    const u = await get(`SELECT id FROM users WHERE id=$1`, [uid]);
    if (!u) return res.status(404).json({ error: "Kullanıcı bulunamadı" });

    const code = await ensureUserCodeForUser(uid);
    res.json({ success: true, user_code: code });
  } catch (e) {
    res.status(500).json({ error: "user_code alınamadı" });
  }
});


// Onaylı kategoriler handler'ı: 3 route aynı fonksiyonu kullanır
async function handleApprovedSurveys(req, res) {
  try {
    // 1) Şemayı kesinleştir (public) + basit JOIN
    const rows = await all(`
      SELECT s.id,
             s.title,
             COALESCE(COUNT(q.id),0)::int AS question_count
        FROM public.surveys   s
   LEFT JOIN public.questions q ON q.survey_id = s.id
       WHERE s.status = 'approved'
    GROUP BY s.id, s.title
    ORDER BY s.id DESC
    `);

    // 2) Hiç kayıt dönmediyse: eski iki-aşamalı yöntemle fallback
    if (!rows || rows.length === 0) {
      const surveys = await all(`
        SELECT id, title
          FROM public.surveys
         WHERE status = 'approved'
      ORDER BY id DESC
      `);

      const out = [];
      for (const s of surveys) {
        const cnt = await get(
          `SELECT COUNT(*)::int AS c FROM public.questions WHERE survey_id = $1`,
          [s.id]
        );
        out.push({ id: s.id, title: s.title, question_count: cnt?.c || 0 });
      }

      if (String(req.query.debug) === "1") {
        return res.json({
          success: true,
          surveys: out,
          debug: { strategy: "fallback", approved_count: surveys.length },
        });
      }
      return res.json({ success: true, surveys: out });
    }

    if (String(req.query.debug) === "1") {
      return res.json({
        success: true,
        surveys: rows,
        debug: { strategy: "join", count: rows.length },
      });
    }

    return res.json({ success: true, surveys: rows });
  } catch (e) {
    console.error("approved-surveys fail:", e);
    res.status(500).json({ error: "Onaylı kategoriler alınamadı" });
  }
}

// ÜÇ route'u aynı handler'a bağla
app.get("/api/user/approved-surveys", handleApprovedSurveys);
app.get("/api/approved-surveys", handleApprovedSurveys);
app.get("/api/categories/approved", handleApprovedSurveys);

app.get("/api/debug/whoami", async (_req, res) => {
  try {
    const a = await get(`SELECT COUNT(*)::int AS c FROM public.surveys WHERE status='approved'`);
    const b = await get(`SELECT COUNT(*)::int AS c FROM public.questions`);
    res.json({
      ok: true,
      approved_surveys: a?.c || 0,
      questions: b?.c || 0
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});


/* ---------- DUELLO PROFILE (ready + visibility) ---------- */

/** duello_profiles kaydı yoksa oluşturur ve döner */
async function ensureDuelloProfile(userId) {
  await run(
    `INSERT INTO duello_profiles (user_id)
     VALUES ($1)
     ON CONFLICT (user_id) DO NOTHING`,
    [userId]
  );
  return await get(
    `SELECT user_id, ready, visibility_mode, last_ready_at
       FROM duello_profiles
      WHERE user_id = $1`,
    [userId]
  );
}

/** Profili getir (yoksa oluştur) */
app.get("/api/duello/profile/:userId", async (req, res) => {
  try {
    const uid = Number(req.params.userId);
    if (!uid) return res.status(400).json({ error: "Geçersiz userId" });

    // kullanıcı var mı?
    const u = await get(`SELECT id FROM users WHERE id=$1`, [uid]);
    if (!u) return res.status(404).json({ error: "Kullanıcı bulunamadı" });

    const prof = await ensureDuelloProfile(uid);
    return res.json({ success: true, profile: prof });
  } catch (e) {
    res.status(500).json({ error: "Düello profili alınamadı: " + e.message });
  }
});

/** Hazırım anahtarı: true/false */
app.post("/api/duello/ready", async (req, res) => {
  try {
    const uid = Number(req.body?.user_id);
    const ready = req.body?.ready === true || String(req.body?.ready) === "true";
    if (!uid) return res.status(400).json({ error: "user_id zorunlu" });

    // kullanıcı doğrula
    const u = await get(`SELECT id FROM users WHERE id=$1`, [uid]);
    if (!u) return res.status(404).json({ error: "Kullanıcı bulunamadı" });

    await ensureDuelloProfile(uid);

    const row = await get(
      `UPDATE duello_profiles
          SET ready = $2,
              last_ready_at = CASE WHEN $2 THEN timezone('Europe/Istanbul', now())
                                   ELSE last_ready_at END
        WHERE user_id = $1
      RETURNING user_id, ready, visibility_mode, last_ready_at`,
      [uid, ready]
    );

    return res.json({ success: true, profile: row });
  } catch (e) {
    res.status(500).json({ error: "Hazır durumu güncellenemedi: " + e.message });
  }
});

/** Görünürlük: public | friends | none */
app.post("/api/duello/visibility", async (req, res) => {
  try {
    const uid = Number(req.body?.user_id);
    const mode = String(req.body?.visibility_mode || "").toLowerCase();
    if (!uid) return res.status(400).json({ error: "user_id zorunlu" });
    if (!["public", "friends", "none"].includes(mode)) {
      return res.status(400).json({ error: "visibility_mode public|friends|none olmalı" });
    }

    const u = await get(`SELECT id FROM users WHERE id=$1`, [uid]);
    if (!u) return res.status(404).json({ error: "Kullanıcı bulunamadı" });

    await ensureDuelloProfile(uid);

    const row = await get(
      `UPDATE duello_profiles
          SET visibility_mode = $2
        WHERE user_id = $1
      RETURNING user_id, ready, visibility_mode, last_ready_at`,
      [uid, mode]
    );

    return res.json({ success: true, profile: row });
  } catch (e) {
    res.status(500).json({ error: "Görünürlük güncellenemedi: " + e.message });
  }
});

/* ---------- DUELLO INVITES (create/respond/cancel + inbox/outbox) ---------- */

/** PENDING -> EXPIRED temizlik */
async function expireOldInvites() {
  await run(
    `UPDATE duello_invites
        SET status='expired'
      WHERE status='pending'
        AND expire_at <= timezone('Europe/Istanbul', now())`
  );
}

/** Kullanıcının aktif maçı var mı? (tek aktif maç kuralı) */
async function hasActiveMatch(userId) {
  const row = await get(
    `SELECT 1
       FROM duello_matches
      WHERE state = 'active'
        AND (user_a_id = $1 OR user_b_id = $1)
      LIMIT 1`,
    [userId]
  );
  return !!row;
}

/** İki kullanıcı arasında açık (süresi dolmamış) davet var mı? */
async function hasFreshPendingInvite(a, b) {
  const row = await get(
    `SELECT 1
       FROM duello_invites
      WHERE status='pending'
        AND expire_at > timezone('Europe/Istanbul', now())
        AND (
          (from_user_id=$1 AND to_user_id=$2) OR
          (from_user_id=$2 AND to_user_id=$1)
        )
      LIMIT 1`,
    [a, b]
  );
  return !!row;
}

/** Kullanıcı kodundan kullanıcıyı bulur */
async function findUserByIdOrCode({ user_id, user_code }) {
  if (user_id) {
    return await get(`SELECT id, ad, soyad, user_code FROM users WHERE id=$1`, [user_id]);
  }
  if (user_code) {
    const code = String(user_code).trim().toUpperCase();
    return await get(
      `SELECT id, ad, soyad, user_code FROM users WHERE user_code = $1`,
      [code]
    );
  }
  return null;
}

// --- Rastgele hazır rakip bulucu (visibility şartı olmadan, sadece ready)
async function findRandomReadyOpponent(excludeUserId) {
  return await get(
    `
    SELECT u.id, u.ad, u.soyad, u.user_code
      FROM duello_profiles p
      JOIN users u ON u.id = p.user_id
     WHERE p.ready = TRUE
       AND u.id <> $1
       AND NOT EXISTS (
         SELECT 1 FROM duello_matches m
          WHERE m.state = 'active'
            AND (m.user_a_id = u.id OR m.user_b_id = u.id)
       )
     ORDER BY random()
     LIMIT 1
    `,
    [excludeUserId]
  );
}

// GET: sadece ad-soyad/kod döner (istersen FE’de kullanırsın)
app.get("/api/duello/random-ready", async (req, res) => {
  try {
    const exclude = Number(req.query.exclude_user_id || req.query.exclude);
    const opp = await findRandomReadyOpponent(exclude || 0);
    res.json({ success: true, user: opp || null });
  } catch (e) {
    res.status(500).json({ error: "random-ready başarısız: " + e.message });
  }
});

// POST: eşleşmeyi hemen başlatır ve match_id döner
app.post("/api/duello/random-ready/start", (req, res) => {
  return res.status(410).json({
    error: "Bu uç kapatıldı. Yeni akış: GET /api/duello/random-ready + POST /api/duello/invite"
  });
});


    /**
 * Davet oluştur (doğrudan user_code ile).
 * Kural: Tek aktif maç — hem gönderenin hem alıcının aktif maçı olmamalı.
 * Not: Alıcı 'ready=OFF' olsa bile user_code ile doğrudan davet GİDER (kuralınız).
 */
app.post("/api/duello/invite", async (req, res) => {
  try {
    const fromId = Number(req.body?.from_user_id);
    const mode   = String(req.body?.mode || "info").toLowerCase(); // 'info' | 'speed'
    const toUser = await findUserByIdOrCode({
      user_id: req.body?.to_user_id,
      user_code: req.body?.to_user_code
    });

    if (!fromId) return res.status(400).json({ error: "from_user_id zorunlu" });
    if (!toUser) return res.status(404).json({ error: "Hedef kullanıcı bulunamadı" });
    if (!["info", "speed"].includes(mode))
      return res.status(400).json({ error: "mode 'info' veya 'speed' olmalı" });

    const toId = Number(toUser.id);
    if (fromId === toId) return res.status(400).json({ error: "Kendinize davet gönderemezsiniz" });

    // durumları tazele
    await expireOldInvites();

// ... /api/duello/invite içinde, hasActiveMatch() kontrollerinden ÖNCE:
await run(`
  UPDATE duello_matches
     SET state='finished', finished_at = timezone('Europe/Istanbul', now())
   WHERE state = 'active'
     AND current_index >= total_questions
`);


    // tek aktif maç kuralı
    if (await hasActiveMatch(fromId)) return res.status(409).json({ error: "Gönderenin aktif düellosu var" });
    if (await hasActiveMatch(toId))   return res.status(409).json({ error: "Alıcının aktif düellosu var" });

    // tekrar eden açık davet engeli
    if (await hasFreshPendingInvite(fromId, toId)) {
      return res.status(409).json({ error: "Zaten açık bir davet var" });
    }

    // 5 dk geçerli
    const inv = await get(
      `INSERT INTO duello_invites
         (from_user_id, to_user_id, mode, created_at, expire_at, status)
       VALUES ($1,$2,$3, timezone('Europe/Istanbul', now()),
               timezone('Europe/Istanbul', now()) + interval '5 minutes',
               'pending')
       RETURNING id, from_user_id, to_user_id, mode, expire_at`,
      [fromId, toId, mode]
    );

// --- SSE: alıcıyı bilgilendir
try {
  const fromUser = await get(
    `SELECT id, ad, soyad, user_code FROM users WHERE id=$1`,
    [fromId]
  );
  sseEmit(toId, "invite:new", {
    invite: inv,
    from: fromUser || { id: fromId },
  });
} catch (_) { /* sessiz geç */ }


    return res.json({ success: true, invite: inv });
  } catch (e) {
    res.status(500).json({ error: "Davet oluşturulamadı: " + e.message });
  }
});

/**
 * Daveti cevapla: accept | reject
 * - Yalnızca alıcı (to_user_id) yanıtlayabilir
 * - Süresi geçmiş ya da yanıtlanmış davet reddedilir
 * - Kabulde tek aktif maç kuralı yeniden kontrol
 * - Kabulde duello_matches kaydı oluşturulur (soru seti bir sonraki adımda)
 */
app.post("/api/duello/invite/respond", async (req, res) => {
  try {
    const inviteId = Number(req.body?.invite_id);
    const userId   = Number(req.body?.user_id);
    const action   = String(req.body?.action || "").toLowerCase(); // 'accept' | 'reject'
    if (!inviteId || !userId) return res.status(400).json({ error: "invite_id ve user_id zorunlu" });
    if (!["accept", "reject"].includes(action))
      return res.status(400).json({ error: "action 'accept' veya 'reject' olmalı" });

    await expireOldInvites();

    const inv = await get(
      `SELECT *
         FROM duello_invites
        WHERE id=$1`,
      [inviteId]
    );
    if (!inv) return res.status(404).json({ error: "Davet bulunamadı" });
    if (Number(inv.to_user_id) !== userId) return res.status(403).json({ error: "Bu daveti yanıtlama yetkiniz yok" });
    if (inv.status !== "pending") return res.status(409).json({ error: "Davet artık pending değil" });

    // süresi geçti mi?
    const expired = await get(
      `SELECT (expire_at <= timezone('Europe/Istanbul', now())) AS exp FROM duello_invites WHERE id=$1`,
      [inviteId]
    );
    if (expired?.exp) {
      await run(`UPDATE duello_invites SET status='expired' WHERE id=$1`, [inviteId]);
      return res.status(410).json({ error: "Davetin süresi dolmuş" });
    }

    if (action === "reject") {
      const row = await get(
        `UPDATE duello_invites
            SET status='rejected', rejected_at=timezone('Europe/Istanbul', now())
          WHERE id=$1
        RETURNING id, status, rejected_at`,
        [inviteId]
      );

      // --- SSE: gönderene davetin reddedildiğini bildir
try {
  sseEmit(inv.from_user_id, "invite:rejected", { invite_id: inviteId });
} catch (_) {}

      return res.json({ success: true, invite: row });
    }

        // ACCEPT
    const fromId = Number(inv.from_user_id);
    const toId   = Number(inv.to_user_id);

    // === Atomik kabul: transaction + advisory lock ===
    const client = await pool.connect();
    try {
      await client.query('BEGIN');

      // Deadlock olmasın diye küçük-id önce
      const lo = Math.min(fromId, toId), hi = Math.max(fromId, toId);
      await client.query('SELECT pg_advisory_xact_lock($1)', [lo]);
      await client.query('SELECT pg_advisory_xact_lock($1)', [hi]);

      // Daveti tekrar kilitle-ve-kontrol
      const invRow = await client.query(
        `SELECT id, status, expire_at, mode, from_user_id, to_user_id
           FROM duello_invites
          WHERE id=$1
          FOR UPDATE`,
        [inviteId]
      );
      if (!invRow.rows.length) {
        await client.query('ROLLBACK');
        return res.status(404).json({ error: "Davet bulunamadı" });
      }
      const lockedInv = invRow.rows[0];
      if (lockedInv.status !== 'pending') {
        await client.query('ROLLBACK');
        return res.status(409).json({ error: "Davet artık pending değil" });
      }

      // Süresi dolmuş mu?
      const exp = await client.query(
        `SELECT (expire_at <= timezone('Europe/Istanbul', now())) AS expired
           FROM duello_invites WHERE id=$1`,
        [inviteId]
      );
      if (exp.rows[0]?.expired) {
        await client.query(
          `UPDATE duello_invites
              SET status='expired'
            WHERE id=$1`,
          [inviteId]
        );
        await client.query('COMMIT');
        return res.status(410).json({ error: "Davetin süresi dolmuş" });
      }

      // Tek aktif maç kuralını tx içinde kontrol et
      const actA = await client.query(
        `SELECT 1 FROM duello_matches
          WHERE state='active' AND (user_a_id=$1 OR user_b_id=$1) LIMIT 1`,
        [fromId]
      );
      if (actA.rows.length) {
        await client.query('ROLLBACK');
        return res.status(409).json({ error: "Gönderenin aktif düellosu var" });
      }
      const actB = await client.query(
        `SELECT 1 FROM duello_matches
          WHERE state='active' AND (user_a_id=$1 OR user_b_id=$1) LIMIT 1`,
        [toId]
      );
      if (actB.rows.length) {
        await client.query('ROLLBACK');
        return res.status(409).json({ error: "Alıcının aktif düellosu var" });
      }

      // Maçı oluştur
      const matchIns = await client.query(
        `INSERT INTO duello_matches
          (mode, user_a_id, user_b_id, created_at, state, total_questions, current_index,
           last_seen_a, last_seen_b, last_activity_at)
         VALUES
          ($1,$2,$3, timezone('Europe/Istanbul', now()), 'active', 12, 0,
           timezone('Europe/Istanbul', now()),
           timezone('Europe/Istanbul', now()),
           timezone('Europe/Istanbul', now()))
         RETURNING id, mode, user_a_id, user_b_id, state, total_questions`,
        [lockedInv.mode, fromId, toId]
      );
      const match = matchIns.rows[0];

      // Daveti accepted yap + match_id ekle
      await client.query(
        `UPDATE duello_invites
            SET status='accepted',
                accepted_at=timezone('Europe/Istanbul', now()),
                match_id=$2
          WHERE id=$1`,
        [inviteId, match.id]
      );

      // Tarafların diğer pending davetlerini iptal et
      await client.query(
        `UPDATE duello_invites
            SET status='cancelled', cancelled_at=timezone('Europe/Istanbul', now())
          WHERE status='pending'
            AND expire_at > timezone('Europe/Istanbul', now())
            AND id <> $1
            AND (from_user_id IN ($2,$3) OR to_user_id IN ($2,$3))`,
        [inviteId, fromId, toId]
      );

      await client.query('COMMIT');

      // SSE (tx dışı)
      try {
        sseEmit(fromId, "invite:accepted", { invite_id: inviteId, match_id: match.id });
        sseEmit(toId,   "invite:accepted", { invite_id: inviteId, match_id: match.id });
      } catch (_) {}

      return res.json({ success: true, invite: { id: inviteId, status: 'accepted' }, match });
    } catch (err) {
      try { await client.query('ROLLBACK'); } catch {}
      throw err;
    } finally {
      client.release();
    }

  } catch (e) {
    res.status(500).json({ error: "Davet yanıtlanamadı: " + e.message });
  }
});

/** Gelen kutusu: pending ve süresi geçmemiş davetler */
app.get("/api/duello/inbox/:userId", async (req, res) => {
  try {
    const uid = Number(req.params.userId);
    await expireOldInvites();
    const rows = await all(
      `SELECT i.id, i.from_user_id, i.to_user_id, i.mode, i.created_at, i.expire_at, i.status,
              u.ad AS from_ad, u.soyad AS from_soyad, u.user_code AS from_user_code
         FROM duello_invites i
         JOIN users u ON u.id = i.from_user_id
        WHERE i.to_user_id = $1
          AND i.status = 'pending'
          AND i.expire_at > timezone('Europe/Istanbul', now())
        ORDER BY i.created_at DESC`,
      [uid]
    );
    // FE uyumluluğu: hem "invites" hem "inbox" döndür
res.json({ success: true, invites: rows, inbox: rows });

  } catch (e) {
    res.status(500).json({ error: "Gelen davetler alınamadı: " + e.message });
  }
});

/** Giden kutusu: pending ve süresi geçmemiş davetler */
app.get("/api/duello/outbox/:userId", async (req, res) => {
  try {
    const uid = Number(req.params.userId);
    await expireOldInvites();
    const rows = await all(
      `SELECT i.id, i.from_user_id, i.to_user_id, i.mode, i.created_at, i.expire_at, i.status,
              u.ad AS to_ad, u.soyad AS to_soyad, u.user_code AS to_user_code
         FROM duello_invites i
         JOIN users u ON u.id = i.to_user_id
        WHERE i.from_user_id = $1
          AND i.status = 'pending'
          AND i.expire_at > timezone('Europe/Istanbul', now())
        ORDER BY i.created_at DESC`,
      [uid]
    );
    // FE uyumluluğu: hem "invites" hem "outbox" döndür
res.json({ success: true, invites: rows, outbox: rows });

  } catch (e) {
    res.status(500).json({ error: "Giden davetler alınamadı: " + e.message });
  }
});

// === DUELLO: Rastgele hazır rakip bul ve maçı başlat ===
// FE: POST /api/duello/random-ready  body: { user_id, mode: 'info'|'speed' }
app.post("/api/duello/random-ready", (req, res) => {
  return res.status(410).json({
    error: "Bu uç kapatıldı. Yeni akış: GET /api/duello/random-ready + POST /api/duello/invite"
  });
});



/* --- DUELLO: kullanıcının aktif maçı var mı? (inviter için polling) --- */
app.get("/api/duello/active/:userId", async (req, res) => {
  try {
    const uid = Number(req.params.userId);
    if (!uid) return res.status(400).json({ error: "userId zorunlu" });

    const row = await get(
      `SELECT id
         FROM duello_matches
        WHERE state='active' AND (user_a_id=$1 OR user_b_id=$1)
        ORDER BY id DESC
        LIMIT 1`,
      [uid]
    );

    res.json({ success: true, match_id: row?.id || null });
  } catch (e) {
    res.status(500).json({ error: "Aktif maç kontrolü hatası: " + e.message });
  }
});


/* ---------- DUELLO MATCH: status / answer / reveal ---------- */

/* ——— Helpers ——— */

/** Maçı oku */
async function duelloGetMatch(matchId) {
  return await get(`SELECT * FROM duello_matches WHERE id=$1`, [matchId]);
}

/** Kullanıcı maçta mı? */
function inThisMatch(m, uid) {
  return Number(m.user_a_id) === Number(uid) || Number(m.user_b_id) === Number(uid);
}

/** Rakibi bul */
function opponentId(m, uid) {
  return Number(m.user_a_id) === Number(uid) ? Number(m.user_b_id) : Number(m.user_a_id);
}

/** Pos -> question_id getir */
async function duelloGetQuestionIdByPos(matchId, pos) {
  const row = await get(
    `SELECT question_id FROM duello_match_questions WHERE match_id=$1 AND pos=$2`,
    [matchId, pos]
  );
  return row?.question_id || null;
}

/** Soru setini üret (yoksa) */
async function ensureDuelloQuestionSet(match) {
  const existing = await all(
    `SELECT pos, question_id
       FROM duello_match_questions
      WHERE match_id=$1
      ORDER BY pos ASC`,
    [match.id]
  );
  if (existing.length > 0) return existing;

  const total = Math.max(1, Number(match.total_questions) || 12);

  // Onaylı havuzdan deterministik rastgele
  const rows = await all(
    `
    SELECT q.id
      FROM questions q
      JOIN surveys s ON s.id = q.survey_id
     WHERE s.status='approved'
     ORDER BY md5($1::text || '-' || q.id::text)
     LIMIT $2
    `,
    [String(match.id), total]
  );

  const ids = rows.map(r => r.id);
  for (let i = 0; i < ids.length; i++) {
    await run(
      `INSERT INTO duello_match_questions (match_id, pos, question_id)
       VALUES ($1,$2,$3)
       ON CONFLICT (match_id, pos) DO NOTHING`,
      [match.id, i + 1, ids[i]]
    );
  }
  return await all(
    `SELECT pos, question_id FROM duello_match_questions WHERE match_id=$1 ORDER BY pos ASC`,
    [match.id]
  );
}

/** Bu soruya verilen cevap(lar) */
async function duelloGetAnswers(matchId, questionId) {
  return await all(
    `SELECT user_id, answer, is_correct, max_time_seconds, time_left_seconds
       FROM duello_answers
      WHERE match_id=$1 AND question_id=$2`,
    [matchId, questionId]
  );
}

/** Skorları hesapla (toplam) */
async function duelloScores(matchId, aId, bId) {
  const row = await get(
    `
    SELECT
      COALESCE(SUM(
        CASE WHEN da.user_id = $2 THEN
          CASE WHEN da.answer='bilmem' THEN 0
               WHEN da.is_correct=1       THEN q.point
               ELSE -q.point END
        ELSE 0 END
      ),0)::int AS score_a,
      COALESCE(SUM(
        CASE WHEN da.user_id = $3 THEN
          CASE WHEN da.answer='bilmem' THEN 0
               WHEN da.is_correct=1       THEN q.point
               ELSE -q.point END
        ELSE 0 END
      ),0)::int AS score_b
    FROM duello_answers da
    JOIN questions q ON q.id = da.question_id
    WHERE da.match_id = $1
    `,
    [matchId, aId, bId]
  );
  return { score_a: row?.score_a || 0, score_b: row?.score_b || 0 };
}

/** Cevabı normalize et (bilgi+hız modunda kullanıcıdan 'bilmem' kabul etmeyeceğiz) */
function normalizeDuelloAnswer(mode, raw) {
  const s = normalizeAnswer(raw);
  if (mode === "speed" && s === "bilmem") {
    // kullanıcıdan gelen bilmem yok; sistem gerektiğinde otomatik atar
    return null;
  }
  return s;
}

/* ——— STATUS ——— */
/**
 * Maç durumunu döner: soru seti, aktif soru, verilen cevaplar, skorlar.
 * FE: 24 sn sayaç sizde; server zaman saymıyor.
 */
app.get("/api/duello/match/:matchId/status", async (req, res) => {
  try {
    const matchId = Number(req.params.matchId);
    const userId  = Number(req.query.user_id);
    if (!matchId || !userId) return res.status(400).json({ error: "matchId ve user_id zorunlu" });

    const m = await duelloGetMatch(matchId);
    if (!m) return res.status(404).json({ error: "Maç bulunamadı" });
    if (!inThisMatch(m, userId)) return res.status(403).json({ error: "Bu maça erişiminiz yok" });

await run(
  `UPDATE duello_matches
      SET last_activity_at = COALESCE(last_activity_at, timezone('Europe/Istanbul', now())),
          last_seen_a = CASE WHEN user_a_id=$2 THEN timezone('Europe/Istanbul', now()) ELSE last_seen_a END,
          last_seen_b = CASE WHEN user_b_id=$2 THEN timezone('Europe/Istanbul', now()) ELSE last_seen_b END
    WHERE id=$1`,
  [matchId, userId]
);


    // Soru setini garantiye al
    const set = await ensureDuelloQuestionSet(m);

    // Skorlar
    const aId = Number(m.user_a_id), bId = Number(m.user_b_id);
    const scores = await duelloScores(matchId, aId, bId);

    // Bitmiş mi?
    if (m.state === "finished" || Number(m.current_index) >= Number(m.total_questions)) {
      const finished = {
        success: true,
        finished: true,
        match: {
          id: m.id, mode: m.mode, total_questions: m.total_questions,
          user_a_id: aId, user_b_id: bId, state: "finished"
        },
        scores
      };
      return res.json(finished);
    }

    const currentPos = Number(m.current_index) + 1;
    const qid = await duelloGetQuestionIdByPos(m.id, currentPos);
    if (!qid) {
      return res.json({
        success: true,
        finished: true,
        match: { id: m.id, mode: m.mode, total_questions: m.total_questions, state: "finished" },
        scores
      });
    }

    const q = await get(
      `SELECT q.id, q.question, q.point, q.survey_id, s.title AS survey_title
         FROM questions q
         LEFT JOIN surveys s ON s.id = q.survey_id
        WHERE q.id=$1`,
      [qid]
    );

    const ans = await duelloGetAnswers(m.id, qid);
    const myAns  = ans.find(a => Number(a.user_id) === userId) || null;
    const oppAns = ans.find(a => Number(a.user_id) === opponentId(m, userId)) || null;

    const me  = await get(`SELECT id, ad, soyad, user_code FROM users WHERE id=$1`, [userId]);
    const opp = await get(`SELECT id, ad, soyad, user_code FROM users WHERE id=$1`, [opponentId(m, userId)]);

    return res.json({
      success: true,
      finished: false,
      match: {
        id: m.id, mode: m.mode, state: m.state,
        total_questions: Number(m.total_questions),
        current_index: Number(m.current_index),
        user_a_id: aId, user_b_id: bId
      },
      you: me,
      opponent: opp,
      question: { pos: currentPos, ...q },
      answers: { mine: myAns, opponent: oppAns },
      scores,
      ui: { per_question_seconds: DUELLO_PER_Q_SEC, reveal_seconds: DUELLO_REVEAL_SEC }
    });
  } catch (e) {
    res.status(500).json({ error: "Maç durumu alınamadı: " + e.message });
  }
});

/* ——— ANSWER ——— */
/**
 * Cevabı kaydeder.
 * - Bilgi modunda iki taraf da 24 sn içinde cevap verebilir (bilmem serbest).
 * - Hız modunda ilk gelen cevap soruyu KİLİTLER; rakibe sistem 'bilmem' yazar.
 * Skor güncellemesi ve ilerletme REVEAL ile yapılır (idempotent).
 */
app.post("/api/duello/match/:matchId/answer", async (req, res) => {
  try {
    const matchId = Number(req.params.matchId);
    const userId  = Number(req.body?.user_id);
    const answer  = req.body?.answer;
    const tls     = Number(req.body?.time_left_seconds);
    const mls     = Number(req.body?.max_time_seconds);

    if (!matchId || !userId) return res.status(400).json({ error: "matchId ve user_id zorunlu" });

    const m = await duelloGetMatch(matchId);
    if (!m) return res.status(404).json({ error: "Maç bulunamadı" });
    if (m.state !== "active") return res.status(409).json({ error: "Maç aktif değil" });
    if (!inThisMatch(m, userId)) return res.status(403).json({ error: "Bu maça erişiminiz yok" });

await run(
  `UPDATE duello_matches
      SET last_activity_at = timezone('Europe/Istanbul', now()),
          last_seen_a = CASE WHEN user_a_id=$2 THEN timezone('Europe/Istanbul', now()) ELSE last_seen_a END,
          last_seen_b = CASE WHEN user_b_id=$2 THEN timezone('Europe/Istanbul', now()) ELSE last_seen_b END
    WHERE id=$1`,
  [matchId, userId]
);



    // Aktif soru
    const currentPos = Number(m.current_index) + 1;
    const qid = await duelloGetQuestionIdByPos(m.id, currentPos);
    if (!qid) return res.status(400).json({ error: "Aktif soru yok" });

    // Cevabı normalize et
    const norm = normalizeDuelloAnswer(String(m.mode), answer);
    if (norm == null) return res.status(400).json({ error: "Hız modunda 'bilmem' seçeneği yok" });

    // Zaten cevap vermiş mi?
    const myPrev = await get(
      `SELECT 1 FROM duello_answers WHERE match_id=$1 AND question_id=$2 AND user_id=$3`,
      [matchId, qid, userId]
    );
    if (myPrev) return res.status(409).json({ error: "Bu soruya zaten cevap verdiniz" });

    // Hız modunda ilk gelen kazanır: önce başka bir cevap var mı?
    const prev = await all(
      `SELECT user_id FROM duello_answers WHERE match_id=$1 AND question_id=$2`,
      [matchId, qid]
    );

    // Doğruluk
    const qc = await get(`SELECT correct_answer FROM questions WHERE id=$1`, [qid]);
    const isCorrect = qc?.correct_answer === norm ? 1 : 0;

    // Zaman alanları
    const maxSec = Number.isFinite(mls) ? Math.max(1, Math.min(120, Math.round(mls))) : DUELLO_PER_Q_SEC;
    const leftRaw = Number.isFinite(tls) ? Math.round(tls) : 0;
    const leftSec = Math.max(0, Math.min(leftRaw, maxSec));

    if (String(m.mode) === "speed") {
  // 1) İlk cevabı atomik şekilde yaz (NOT EXISTS ile)
  const ins = await get(`
    WITH ins AS (
      INSERT INTO duello_answers
        (match_id, question_id, user_id, answer, is_correct, created_at,
         max_time_seconds, time_left_seconds, earned_seconds)
      SELECT $1,$2,$3,$4,$5, timezone('Europe/Istanbul', now()),
             $6,$7,$7
      WHERE NOT EXISTS (
        SELECT 1 FROM duello_answers WHERE match_id=$1 AND question_id=$2
      )
      RETURNING 1 AS ok
    )
    SELECT COALESCE((SELECT ok FROM ins), 0) AS ok
  `, [matchId, qid, userId, norm, isCorrect, maxSec, leftSec]);

  if (!ins?.ok) {
    return res.status(409).json({ error: "Soru kilitlendi (hız modu)" });
  }

  // 2) Rakibe anında 'bilmem' (yoksa)
  const oppId = opponentId(m, userId);
  const oppPrev = await get(
    `SELECT 1 FROM duello_answers WHERE match_id=$1 AND question_id=$2 AND user_id=$3`,
    [matchId, qid, oppId]
  );
  if (!oppPrev) {
    await run(
      `INSERT INTO duello_answers
         (match_id, question_id, user_id, answer, is_correct, created_at,
          max_time_seconds, time_left_seconds, earned_seconds)
       VALUES ($1,$2,$3,'bilmem',0, timezone('Europe/Istanbul', now()), $4,0,0)`,
      [matchId, qid, oppId, maxSec]
    );
  }

  return res.json({ success: true, locked: true });
}


    // bilgi modu: serbest, sadece kaydet
    await run(
      `INSERT INTO duello_answers
         (match_id, question_id, user_id, answer, is_correct, created_at,
          max_time_seconds, time_left_seconds, earned_seconds)
       VALUES ($1,$2,$3,$4,$5, timezone('Europe/Istanbul', now()),
               $6,$7,$7)`,
      [matchId, qid, userId, norm, isCorrect, maxSec, leftSec]
    );

    return res.json({ success: true, locked: false });
  } catch (e) {
    res.status(500).json({ error: "Cevap kaydedilemedi: " + e.message });
  }
});

/* ——— REVEAL / NEXT ——— */
/**
 * 24 sn bittiğinde FE bu ucu çağırır:
 * - Eksik cevap(lar) varsa INFO modunda 'bilmem' ile tamamlanır.
 * - (SCORE) Skor toplamları status'ta hesaplandığı için burada yalnızca ilerletiriz.
 * - Son sorudan sonra maçı bitirir (state='finished').
 * Idempotent: Aynı soruya ikinci kez çağrı ilerletmez.
 */
app.post("/api/duello/match/:matchId/reveal", async (req, res) => {
  try {
    const matchId = Number(req.params.matchId);
    const userId  = Number(req.body?.user_id);
    if (!matchId || !userId) return res.status(400).json({ error: "matchId ve user_id zorunlu" });

    const m = await duelloGetMatch(matchId);
    if (!m) return res.status(404).json({ error: "Maç bulunamadı" });
    if (m.state !== "active") {
      try {
  sseEmit(Number(m.user_a_id), "match:finished", { match_id: Number(m.id) });
  sseEmit(Number(m.user_b_id), "match:finished", { match_id: Number(m.id) });
} catch (_) {}

      return res.json({ success: true, finished: true, current_index: Number(m.current_index) });
    }
    if (!inThisMatch(m, userId)) return res.status(403).json({ error: "Bu maça erişiminiz yok" });

await run(
  `UPDATE duello_matches
      SET last_activity_at = timezone('Europe/Istanbul', now()),
          last_seen_a = CASE WHEN user_a_id=$2 THEN timezone('Europe/Istanbul', now()) ELSE last_seen_a END,
          last_seen_b = CASE WHEN user_b_id=$2 THEN timezone('Europe/Istanbul', now()) ELSE last_seen_b END
    WHERE id=$1`,
  [matchId, userId]
);


    const total = Number(m.total_questions);
    const currentPos = Number(m.current_index) + 1;
    if (currentPos > total) {
  await run(`UPDATE duello_matches SET state='finished', finished_at=timezone('Europe/Istanbul', now()) WHERE id=$1`, [matchId]);
  try {
    sseEmit(Number(m.user_a_id), "match:finished", { match_id: Number(m.id) });
    sseEmit(Number(m.user_b_id), "match:finished", { match_id: Number(m.id) });
  } catch (_) {}
  return res.json({ success: true, finished: true, current_index: total });
}


    const qid = await duelloGetQuestionIdByPos(matchId, currentPos);
    if (!qid) {
  await run(`UPDATE duello_matches SET state='finished', finished_at=timezone('Europe/Istanbul', now()) WHERE id=$1`, [matchId]);
  try {
    sseEmit(Number(m.user_a_id), "match:finished", { match_id: Number(m.id) });
    sseEmit(Number(m.user_b_id), "match:finished", { match_id: Number(m.id) });
  } catch (_) {}
  return res.json({ success: true, finished: true, current_index: total });
}


    // INFO modunda eksikler 'bilmem' ile tamamlanır
    if (String(m.mode) === "info") {
      const aId = Number(m.user_a_id), bId = Number(m.user_b_id);
      const aAns = await get(
        `SELECT 1 FROM duello_answers WHERE match_id=$1 AND question_id=$2 AND user_id=$3`,
        [matchId, qid, aId]
      );
      const bAns = await get(
        `SELECT 1 FROM duello_answers WHERE match_id=$1 AND question_id=$2 AND user_id=$3`,
        [matchId, qid, bId]
      );

      const maxSec = DUELLO_PER_Q_SEC;
      if (!aAns) {
        await run(
          `INSERT INTO duello_answers
             (match_id, question_id, user_id, answer, is_correct, created_at,
              max_time_seconds, time_left_seconds, earned_seconds)
           VALUES ($1,$2,$3,'bilmem',0, timezone('Europe/Istanbul', now()), $4,0,0)`,
          [matchId, qid, aId, maxSec]
        );
      }
      if (!bAns) {
        await run(
          `INSERT INTO duello_answers
             (match_id, question_id, user_id, answer, is_correct, created_at,
              max_time_seconds, time_left_seconds, earned_seconds)
           VALUES ($1,$2,$3,'bilmem',0, timezone('Europe/Istanbul', now()), $4,0,0)`,
          [matchId, qid, bId, maxSec]
        );
      }
    }

    // Bu soruya iki cevap var mı? (bilgi: 2 kullanıcı; hız: biri + otomatik bilmem)
    const cnt = await get(
      `SELECT COUNT(*)::int AS c FROM duello_answers WHERE match_id=$1 AND question_id=$2`,
      [matchId, qid]
    );

    if ((cnt?.c || 0) >= 2) {
      const nextIdx = Number(m.current_index) + 1;
      const isLast  = nextIdx >= Number(m.total_questions);

      if (isLast) {
  await run(
    `UPDATE duello_matches
        SET current_index=$2, state='finished', finished_at=timezone('Europe/Istanbul', now())
      WHERE id=$1`,
    [matchId, nextIdx]
  );
  try {
    sseEmit(Number(m.user_a_id), "match:finished", { match_id: Number(m.id) });
    sseEmit(Number(m.user_b_id), "match:finished", { match_id: Number(m.id) });
  } catch (_) {}
  return res.json({ success: true, finished: true, current_index: nextIdx });
}


      await run(`UPDATE duello_matches SET current_index=$2 WHERE id=$1`, [matchId, nextIdx]);
      return res.json({ success: true, finished: false, current_index: nextIdx });
    }

    // henüz iki cevap yoksa ilerletme yok
    return res.json({ success: true, finished: false, current_index: Number(m.current_index) });
  } catch (e) {
    res.status(500).json({ error: "Reveal işlemi yapılamadı: " + e.message });
  }
});



/* --------- CEVAP KAYDI ---------- */
async function insertAnswer({
  user_id, question_id, norm_answer, is_correct,
  maxSec, leftSec, earned, isDaily = false, dailyKey = null,
  bonusPoints = 0, streakLenAtAnswer = null
}) {
  await run(`
    INSERT INTO answers
      (user_id, question_id, answer, is_correct, created_at,
       max_time_seconds, time_left_seconds, earned_seconds,
       is_daily, daily_key, bonus_points, streak_len_at_answer)
    VALUES ($1,$2,$3,$4, timezone('Europe/Istanbul', now()),
            $5,$6,$7,$8,$9,$10,$11)
  `, [
    user_id, question_id, norm_answer, is_correct,
    maxSec, leftSec, earned, isDaily, dailyKey,
    bonusPoints, streakLenAtAnswer
  ]);
}

app.post("/api/answers", async (req, res) => {
  const { user_id, question_id, answer, time_left_seconds, max_time_seconds } = req.body;
  try {
    const q = await get(`SELECT correct_answer FROM questions WHERE id=$1`, [question_id]);
    if (!q) return res.status(400).json({ error: "Soru bulunamadı!" });

    const norm = normalizeAnswer(answer);
    const is_correct = q.correct_answer === norm ? 1 : 0;

    const parsedMax = Number(max_time_seconds);
    const parsedLeft = Number(time_left_seconds);
    const maxSec = Number.isFinite(parsedMax) ? Math.max(1, Math.min(120, Math.round(parsedMax))) : 24;
    const leftSecRaw = Number.isFinite(parsedLeft) ? Math.round(parsedLeft) : 0;
    const leftSec = Math.max(0, Math.min(leftSecRaw, maxSec));
    const earned = leftSec;

    await insertAnswer({
      user_id, question_id, norm_answer: norm, is_correct,
      maxSec, leftSec, earned, isDaily: false, dailyKey: null
    });

    res.json({ success: true, is_correct, time_left_seconds: leftSec, max_time_seconds: maxSec, earned_seconds: earned });
  } catch (e) {
    res.status(500).json({ error: "Cevap kaydedilemedi! " + e.message });
  }
});

// === KADEMELİ: cevap kaydı + oturum sayaçları ===
app.post("/api/ladder/answer", async (req, res) => {
  try {
    const { user_id, question_id, answer, time_left_seconds, max_time_seconds, reset } = req.body || {};
    if (!user_id || !question_id) {
      return res.status(400).json({ error: "user_id ve question_id zorunludur." });
    }

    // Soru puanı (seviye)
    const q = await get(`SELECT id, point FROM questions WHERE id=$1`, [question_id]);
    if (!q) return res.status(404).json({ error: "Soru bulunamadı" });
    const level = Math.max(1, Math.min(10, Number(q.point) || 1));

    // Oturumu hazırla (reset parametresi gelirse sıfırla)
    const forceReset =
      reset === true || reset === 1 || String(reset).trim() === "1";
    await ensureLadderSession(user_id, level, forceReset);

    // Cevabı normalize et ve doğruluğu hesapla
    const qc = await get(`SELECT correct_answer FROM questions WHERE id=$1`, [question_id]);
    const norm = normalizeAnswer(answer);
    const is_correct = qc?.correct_answer === norm ? 1 : 0;

    // Süre hesapları (answers endpoint’i ile aynı mantık)
    const parsedMax = Number(max_time_seconds);
    const parsedLeft = Number(time_left_seconds);
    const maxSec = Number.isFinite(parsedMax) ? Math.max(1, Math.min(120, Math.round(parsedMax))) : 24;
    const leftSecRaw = Number.isFinite(parsedLeft) ? Math.round(parsedLeft) : 0;
    const leftSec = Math.max(0, Math.min(leftSecRaw, maxSec));
    const earned = leftSec;

    // Standart cevap kaydı (is_daily = false)
    await insertAnswer({
      user_id,
      question_id,
      norm_answer: norm,
      is_correct,
      maxSec,
      leftSec,
      earned,
      isDaily: false,
      dailyKey: null
    });

    // Oturum sayaçlarını artır (bilmem = deneme sayılmaz)
    const incAttempt = norm !== "bilmem" ? 1 : 0;
    const incCorrect = norm !== "bilmem" && is_correct === 1 ? 1 : 0;

    await run(
      `UPDATE ladder_sessions
         SET attempts = attempts + $2,
             correct  = correct  + $3,
             updated_at = timezone('Europe/Istanbul', now())
       WHERE user_id = $1`,
      [user_id, incAttempt, incCorrect]
    );

    const sess = await get(
      `SELECT current_level, attempts, correct FROM ladder_sessions WHERE user_id=$1`,
      [user_id]
    );
    const attempts = Number(sess?.attempts || 0);
    const correct = Number(sess?.correct || 0);
    const success_rate = attempts > 0 ? correct / attempts : 0;

    return res.json({
      success: true,
      is_correct,
      attempts,
      correct,
      success_rate,
      level: level
    });
  } catch (e) {
    console.error("ladder/answer fail:", e);
    return res.status(500).json({ error: "Kademeli cevap kaydedilemedi" });
  }
});

app.get("/api/user/:userId/answers", async (req, res) => {
  try {
    const rows = await all(`SELECT question_id, is_correct, answer FROM answers WHERE user_id=$1`, [req.params.userId]);
    res.json({ success: true, answers: rows });
  } catch { res.status(500).json({ error: "Listeleme hatası!" }); }
});

app.get("/api/user/:userId/answered", async (req, res) => {
  try {
    const rows = await all(`SELECT question_id FROM answers WHERE user_id=$1`, [req.params.userId]);
    res.json({ success: true, answered: rows.map(r => r.question_id) });
  } catch { res.status(500).json({ error: "Listeleme hatası!" }); }
});

app.get("/api/user/:userId/total-points", async (req, res) => {
  try {
    const rows = await all(
      `SELECT a.answer, a.is_correct, a.bonus_points, q.point
         FROM answers a
         INNER JOIN questions q ON a.question_id = q.id
        WHERE a.user_id=$1`,
      [req.params.userId]
    );

    let total = 0;
    rows.forEach(r => {
      if (r.answer === "bilmem") return;
      if (r.is_correct === 1) total += (r.point || 1) + (r.bonus_points || 0);
      else                    total -= (r.point || 1);
    });

    res.json({ success: true, totalPoints: total, answeredCount: rows.length });
  } catch (e) {
    res.status(500).json({ error: "Puan alınamadı: " + e.message });
  }
});

app.get("/api/user/:userId/score", async (req, res) => {
  try {
    const row = await get(
      `SELECT
         COUNT(*)::int AS total,
         COALESCE(SUM(is_correct),0)::int AS correct,
         (COUNT(*) - COALESCE(SUM(is_correct),0))::int AS wrong
       FROM answers WHERE user_id=$1`, [req.params.userId]
    );
    res.json({ success: true, ...row });
  } catch { res.status(500).json({ error: "Skor hatası!" }); }
});

app.get("/api/surveys/:surveyId/answers-report", async (req, res) => {
  try {
    const surveyId = req.params.surveyId;
    const questions = await all(`SELECT id, question FROM questions WHERE survey_id=$1 ORDER BY id ASC`, [surveyId]);
    const answers = await all(
      `SELECT 
         a.user_id, u.ad, u.soyad, u.yas, u.cinsiyet, u.sehir,
         a.question_id, q.question, a.answer
       FROM answers a
       INNER JOIN users u ON a.user_id = u.id
       INNER JOIN questions q ON a.question_id = q.id
       WHERE q.survey_id=$1
       ORDER BY a.user_id, a.question_id`, [surveyId]
    );
    const participantsMap = {};
    for (const ans of answers) {
      if (!participantsMap[ans.user_id]) {
        participantsMap[ans.user_id] = {
          user_id: ans.user_id, ad: ans.ad, soyad: ans.soyad,
          yas: ans.yas, cinsiyet: ans.cinsiyet, sehir: ans.sehir, answers: {},
        };
      }
      participantsMap[ans.user_id].answers[ans.question_id] = ans.answer;
    }
    res.json({
      success: true,
      questions,
      participants: Object.values(participantsMap),
      total_participants: Object.keys(participantsMap).length
    });
  } catch { res.status(500).json({ error: "Cevaplar alınamadı" }); }
});

/* ---------- PUANLARIM (title bazında) ---------- */
app.get("/api/user/:userId/performance", async (req, res) => {
  try {
    const userId = req.params.userId;
    const rows = await all(
      `
      SELECT
        s.id AS survey_id,
        s.title,
        COUNT(a.*)::int AS answered,
        COALESCE(SUM(CASE WHEN a.answer = 'bilmem' THEN 1 ELSE 0 END),0)::int AS bilmem,
        COALESCE(SUM(CASE WHEN a.answer != 'bilmem' THEN 1 ELSE 0 END),0)::int AS attempted,
        COALESCE(SUM(CASE WHEN a.is_correct = 1 THEN 1 ELSE 0 END),0)::int AS correct,
        COALESCE(SUM(CASE WHEN a.is_correct = 0 AND a.answer != 'bilmem' THEN 1 ELSE 0 END),0)::int AS wrong,
        COALESCE(SUM(CASE WHEN a.answer = 'bilmem' THEN 0
                          WHEN a.is_correct = 1 THEN q.point + COALESCE(a.bonus_points,0) ELSE 0 END),0)::int AS earned_points,
        COALESCE(SUM(CASE WHEN a.answer = 'bilmem' THEN 0 ELSE q.point END),0)::int AS possible_points,
        COALESCE(SUM(CASE
            WHEN a.answer = 'bilmem' THEN 0
            WHEN a.is_correct = 1 THEN q.point + COALESCE(a.bonus_points,0)
            WHEN a.is_correct = 0 THEN -q.point
            ELSE 0
        END),0)::int AS net_points
      FROM answers a
      INNER JOIN questions q ON q.id = a.question_id
      INNER JOIN surveys  s ON s.id = q.survey_id
      WHERE a.user_id = $1
      GROUP BY s.id, s.title
      `,
      [userId]
    );

    const perf = rows.map(r => {
      const pct = r.possible_points > 0
        ? Math.round((r.earned_points * 100.0) / r.possible_points)
        : null;
      return { ...r, score_percent: pct };
    }).sort((A, B) => {
      if (B.net_points !== A.net_points) return B.net_points - A.net_points;
      if (B.attempted !== A.attempted)   return B.attempted - A.attempted;
      return (A.title || "").localeCompare(B.title || "", "tr");
    });

    res.json({ success: true, performance: perf });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "Performans listesi alınamadı" });
  }
});

// --- KADEMELİ: seviye atlama şartları ---
const LADDER_MIN_ATTEMPTS = Number(process.env.LADDER_MIN_ATTEMPTS ?? 10); // varsayılanı 100 (ürün kuralı)
const LADDER_REQUIRED_RATE = Number(process.env.LADDER_REQUIRED_RATE || 0.8); // %80 başarı
const LADDER_DEFAULT_LIMIT = Math.max(1, parseInt(process.env.LADDER_QUESTIONS_LIMIT || "20", 10));

// En iyi (erişilen) seviye günceller
async function bumpLadderBest(userId, level) {
  const safe = Math.max(0, Math.min(10, Number(level) || 0));
  await run(
    `UPDATE users
       SET ladder_best_level = GREATEST(COALESCE(ladder_best_level,0), $2)
     WHERE id = $1`,
    [userId, safe]
  );
}

// === KADEMELİ: oturum (run) yöneticisi ===
// Her kullanıcı için tek bir aktif ladder oturumu tutar.
// level değişirse ya da forceReset=true ise attempts/correct sıfırlanır.
async function ensureLadderSession(userId, level, forceReset = false) {
  const lvl = Math.max(1, Math.min(10, Number(level) || 1));

  let row = await get(
    `SELECT * FROM ladder_sessions WHERE user_id=$1`,
    [userId]
  );

  if (!row) {
    await run(
      `INSERT INTO ladder_sessions (user_id, current_level, attempts, correct)
       VALUES ($1,$2,0,0)`,
      [userId, lvl]
    );
  } else if (forceReset || Number(row.current_level) !== lvl) {
    await run(
      `UPDATE ladder_sessions
         SET current_level=$2,
             attempts=0,
             correct=0,
             run_started_at=timezone('Europe/Istanbul', now()),
             updated_at=timezone('Europe/Istanbul', now())
       WHERE user_id=$1`,
      [userId, lvl]
    );
  }

  return await get(
    `SELECT * FROM ladder_sessions WHERE user_id=$1`,
    [userId]
  );
}

/* ---------- KADEMELİ YARIŞ ---------- */

// --- KADEMELİ: oturum başlat (sayaçları 0'a çek) ---
app.post("/api/ladder/session/start", async (req, res) => {
  try {
    const userId = Number(req.body?.user_id);
    const lvl = Math.max(1, Math.min(10, Number(req.body?.level) || 1));

    if (!userId) {
      return res.status(400).json({ error: "user_id zorunlu." });
    }

    // mevcut şemaya uygun: tek satırlık oturum, seviye değişince sayaçlar sıfırlanır
    await ensureLadderSession(userId, lvl, true);


        // En iyi seviyeyi güvenceye al (geri düşürmez)
    try {
      await bumpLadderBest(userId, lvl);
    } catch (e) {
      console.error("bumpLadderBest fail (/ladder/session/start):", e.message);
    }


    return res.json({ success: true });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: "Kademeli oturum başlatılamadı: " + e.message });
  }
});


app.get("/api/user/:userId/kademeli-questions", async (req, res) => {
  try {
    const userId = Number(req.params.userId);
    const point = Number(req.query.point || 1);
    const limit = Math.min(1000, Math.max(10, Number(req.query.limit || LADDER_DEFAULT_LIMIT)));
    if (!point || point < 1 || point > 10) {
      return res.status(400).json({ error: "Geçersiz point. 1-10 arası olmalı." });
    }

    const rows = await all(
      `
      SELECT q.id, q.survey_id, q.question, q.point
        FROM questions q
        INNER JOIN surveys s ON s.id = q.survey_id
       WHERE s.status = 'approved'
         AND q.point = $2
         AND q.id NOT IN (
           SELECT a.question_id FROM answers a
            WHERE a.user_id = $1 AND a.is_correct = 1
         )
       ORDER BY md5($1::text || '-' || q.id::text)
       LIMIT $3
      `,
      [userId, point, limit]
    );

    res.json({ success: true, questions: rows });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "Kademeli sorular alınamadı" });
  }
});

app.get("/api/user/:userId/kademeli-progress", async (req, res) => {
  try {
    const userId = Number(req.params.userId);
    const point = Number(req.query.point || 1);
    if (!point || point < 1 || point > 10) {
      return res.status(400).json({ error: "Geçersiz point. 1-10 arası olmalı." });
    }

    // Yalnızca user_id ile oku; seviye eşleşmiyorsa 409
    const st = await get(
      `SELECT current_level, attempts, correct
         FROM ladder_sessions
        WHERE user_id = $1`,
      [userId]
    );

    if (!st) {
      return res.json({
        success: true,
        point,
        status: "no-session",
        attempted: 0,
        correct: 0,
        success_rate: 0,
        can_level_up: false
      });
    }

    if (Number(st.current_level) !== Number(point)) {
      return res.status(409).json({
        error: "Seviye uyumsuz. Aktif oturum seviyesi farklı.",
        current_level: Number(st.current_level),
        requested_point: Number(point)
      });
    }

    const attempts = Number(st.attempts || 0);
    const correct  = Number(st.correct  || 0);
    const success_rate = attempts > 0 ? (correct / attempts) : 0;

    return res.json({
      success: true,
      point,
      attempted: attempts, // FE eski alan adı
      correct,
      success_rate,
      can_level_up: (attempts >= LADDER_MIN_ATTEMPTS && success_rate >= LADDER_REQUIRED_RATE)
    });

  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "Kademeli ilerleme alınamadı" });
  }
});

app.get("/api/user/:userId/kademeli-next", async (req, res) => {
  try {
    const userId = Number(req.params.userId);
    const point = Number(req.query.point || 1);
    if (!point || point < 1 || point > 10) {
      return res.status(400).json({ error: "Geçersiz point. 1-10 arası olmalı." });
    }

    // Yalnızca user_id ile oku; seviye eşleşmiyorsa 409
    const st = await get(
      `SELECT current_level, attempts, correct
         FROM ladder_sessions
        WHERE user_id = $1`,
      [userId]
    );

    if (!st) {
      const bestRow = await get(
        `SELECT COALESCE(ladder_best_level,0)::int AS best FROM users WHERE id=$1`,
        [userId]
      );
      return res.json({
        success: true,
        status: "no-session",
        can_level_up: false,
        next_point: point,
        best_level: bestRow?.best || 0
      });
    }

    if (Number(st.current_level) !== Number(point)) {
      return res.status(409).json({
        error: "Seviye uyumsuz. Aktif oturum seviyesi farklı.",
        current_level: Number(st.current_level),
        requested_point: Number(point)
      });
    }

    const attempts = Number(st.attempts || 0);
    const correct  = Number(st.correct  || 0);
    const success_rate = attempts > 0 ? (correct / attempts) : 0;
    const ok = (attempts >= LADDER_MIN_ATTEMPTS && success_rate >= LADDER_REQUIRED_RATE);

    if (ok) {
      const next = (point >= 10) ? 10 : (point + 1);
      try { await bumpLadderBest(userId, next); } catch (_e) { /* sessiz */ }

      if (point >= 10) {
        return res.json({
          success: true,
          status: "genius",
          can_level_up: true,
          next_point: 10,
          best_level: next
        });
      }
      return res.json({
        success: true,
        status: "ok",
        can_level_up: true,
        next_point: point + 1,
        best_level: next
      });
    }

    // Oturum var ama şart sağlanmadı: kal
    const bestRow = await get(
      `SELECT COALESCE(ladder_best_level,0)::int AS best FROM users WHERE id=$1`,
      [userId]
    );
    return res.json({
      success: true,
      status: "stay",
      can_level_up: false,
      next_point: point,
      best_level: bestRow?.best || 0
    });

  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "Seviye kontrolü yapılamadı" });
  }
});

// --- KADEMELİ: oturumu başlat/sıfırla ---
app.post("/api/ladder/start", async (req, res) => {
  try {
    const user_id = Number(req.body?.user_id);
    const level   = Math.max(1, Math.min(10, Number(req.body?.level) || 1));

    if (!user_id) {
      return res.status(400).json({ error: "user_id zorunlu." });
    }

    // reset=true -> attempts=0, correct=0, current_level=level, start_ts=now()
    await ensureLadderSession(user_id, level, true);

        // En iyi seviyeyi güvenceye al (geri düşürmez)
    try {
      await bumpLadderBest(user_id, level);
    } catch (e) {
      console.error("bumpLadderBest fail (/ladder/start):", e.message);
    }


    // dönen başlangıç bilgisi (hepsi 0)
    return res.json({
      success: true,
      level,
      attempts: 0,
      correct: 0
    });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: "Kademeli oturum başlatılamadı" });
  }
});

// --- KADEMELİ: en iyi (erişilen) seviye: sadece oku ---
app.get("/api/user/:userId/ladder-best-level", async (req, res) => {
  try {
    const row = await get(
      `SELECT COALESCE(ladder_best_level,0)::int AS best_level
         FROM users
        WHERE id = $1`,
      [req.params.userId]
    );
    res.json({ success: true, best_level: row?.best_level || 0 });
  } catch (e) {
    res.status(500).json({ error: "En iyi kademe alınamadı" });
  }
});




// --- KADEMELİ: en iyi (erişilen) seviye: yaz (frontend'in beklediği) ---
app.post("/api/user/:userId/ladder-best-level", async (req, res) => {
  try {
    const uid = Number(req.params.userId);
    const level = Math.max(0, Math.min(10, Number(req.body?.level) || 0));
    await run(
      `UPDATE users
         SET ladder_best_level = GREATEST(COALESCE(ladder_best_level,0), $2)
       WHERE id = $1`,
      [uid, level]
    );
    res.json({ success: true, best_level: level });
  } catch (e) {
    res.status(500).json({ error: "En iyi kademe yazılamadı: " + e.message });
  }
});

// === DUELLO: Kullanıcının son rakipleri (ad/soyad + user_code) ===
// GET /api/duello/user/:userId/recents?limit=12
app.get("/api/duello/user/:userId/recents", async (req, res) => {
  try {
    const uid = Number(req.params.userId);
    const limit = Math.max(1, Math.min(50, parseInt(req.query.limit || "12", 10)));

    // Her rakip için EN SON maç zamanı
    const rows = await all(
      `
      WITH my_matches AS (
        SELECT m.*,
               COALESCE(m.finished_at, m.last_activity_at, m.created_at) AS ts
          FROM duello_matches m
         WHERE (m.user_a_id = $1 OR m.user_b_id = $1)
      ),
      last_per_opp AS (
        SELECT DISTINCT ON (opp.id)
               opp.id, opp.ad, opp.soyad, opp.user_code, my.ts
          FROM my_matches my
          JOIN users opp
            ON opp.id = CASE WHEN my.user_a_id = $1 THEN my.user_b_id ELSE my.user_a_id END
         ORDER BY opp.id, my.ts DESC
      )
      SELECT * FROM last_per_opp
      ORDER BY user_code IS NULL, user_code ASC NULLS LAST, id ASC
      LIMIT $2
      `,
      [uid, limit]
    );

    return res.json({ success: true, recents: rows });
  } catch (e) {
    return res.status(500).json({ error: "Son rakipler alınamadı: " + e.message });
  }
});

/* ---------- DUELLO: match summary & basic user stats (read-only) ---------- */

/** Maç genel istatistiklerini (iki oyuncu için) hesapla */
async function duelloMatchTotals(matchId) {
  const rows = await all(
    `
    SELECT
      da.user_id,
      COUNT(*) FILTER (WHERE da.answer = 'bilmem')::int AS bilmem,
      COUNT(*) FILTER (WHERE da.answer != 'bilmem' AND da.is_correct=1)::int AS correct,
      COUNT(*) FILTER (WHERE da.answer != 'bilmem' AND da.is_correct=0)::int AS wrong,
      COALESCE(SUM(
        CASE
          WHEN da.answer='bilmem' THEN 0
          WHEN da.is_correct=1   THEN q.point
          ELSE -q.point
        END
      ),0)::int AS score
    FROM duello_answers da
    JOIN questions q ON q.id = da.question_id
    WHERE da.match_id = $1
    GROUP BY da.user_id
    `,
    [matchId]
  );

  const map = {};
  for (const r of rows) map[Number(r.user_id)] = r;
  return map; // { [user_id]: {bilmem, correct, wrong, score} }
}

/** Maç özeti: skorlar + kazanan */
app.get("/api/duello/match/:matchId/summary", async (req, res) => {
  try {
    const matchId = Number(req.params.matchId);
    const userId  = Number(req.query.user_id);
    if (!matchId || !userId) return res.status(400).json({ error: "matchId ve user_id zorunlu" });

    const m = await get(`SELECT * FROM duello_matches WHERE id=$1`, [matchId]);
    if (!m) return res.status(404).json({ error: "Maç bulunamadı" });
    if (Number(m.user_a_id) !== userId && Number(m.user_b_id) !== userId) {
      return res.status(403).json({ error: "Bu maça erişiminiz yok" });
    }

    // Soru setini garanti et (fonksiyon yoksa sessizce geç)
    if (typeof ensureDuelloQuestionSet === "function") {
      await ensureDuelloQuestionSet(m);
    }

    const totals = await duelloMatchTotals(matchId);
    const aId = Number(m.user_a_id);
    const bId = Number(m.user_b_id);

    const aStats = totals[aId] || { bilmem:0, correct:0, wrong:0, score:0 };
    const bStats = totals[bId] || { bilmem:0, correct:0, wrong:0, score:0 };

    let result = { winner_user_id: null, code: "pending" }; // "a_win" | "b_win" | "draw" | "pending"
    const finished = (m.state === "finished") || (Number(m.current_index) >= Number(m.total_questions));

    if (finished) {
      if (aStats.score > bStats.score)      result = { winner_user_id: aId, code: "a_win" };
      else if (bStats.score > aStats.score) result = { winner_user_id: bId, code: "b_win" };
      else                                  result = { winner_user_id: null, code: "draw" };
    }

    const you = await get(`SELECT id, ad, soyad, user_code FROM users WHERE id=$1`, [userId]);
    const opp = await get(
      `SELECT id, ad, soyad, user_code FROM users WHERE id=$1`,
      [userId === aId ? bId : aId]
    );

    return res.json({
      success: true,
      finished,
      mode: String(m.mode),
      total_questions: Number(m.total_questions),
      current_index: Number(m.current_index),
      users: {
        a: { id: aId, stats: aStats },
        b: { id: bId, stats: bStats }
      },
      you,
      opponent: opp,
      result
    });
  } catch (e) {
    res.status(500).json({ error: "Maç özeti alınamadı: " + e.message });
  }
});

/**
 * Kullanıcı bazlı temel düello istatistikleri (read-only)
 *  - mode bazında: played, wins, losses, draws
 *  - en çok karşılaşılan rakipler (head-to-head kısa özet)
 */
app.get("/api/duello/user/:userId/basic-stats", async (req, res) => {
  try {
    const userId = Number(req.params.userId);
    if (!userId) return res.status(400).json({ error: "userId zorunlu" });

    // 1) Mode bazında W/L/D (sadece bitmiş maçlar)
    const modeRows = await all(
      `
      WITH scores AS (
        SELECT
          m.id,
          m.mode,
          m.user_a_id,
          m.user_b_id,
          SUM(
            CASE
              WHEN da.user_id = m.user_a_id THEN
                CASE WHEN da.answer='bilmem' THEN 0
                     WHEN da.is_correct=1   THEN q.point
                     ELSE -q.point
                END
              ELSE 0
            END
          ) AS score_a,
          SUM(
            CASE
              WHEN da.user_id = m.user_b_id THEN
                CASE WHEN da.answer='bilmem' THEN 0
                     WHEN da.is_correct=1   THEN q.point
                     ELSE -q.point
                END
              ELSE 0
            END
          ) AS score_b
        FROM duello_matches m
        JOIN duello_answers da ON da.match_id = m.id
        JOIN questions q ON q.id = da.question_id
        WHERE m.state='finished' AND (m.user_a_id = $1 OR m.user_b_id = $1)
        GROUP BY m.id, m.mode, m.user_a_id, m.user_b_id
      )
      SELECT
        mode,
        COUNT(*)::int AS played,
        COUNT(*) FILTER (
          WHERE (user_a_id=$1 AND score_a>score_b) OR (user_b_id=$1 AND score_b>score_a)
        )::int AS wins,
        COUNT(*) FILTER (
          WHERE (user_a_id=$1 AND score_a<score_b) OR (user_b_id=$1 AND score_b<score_a)
        )::int AS losses,
        COUNT(*) FILTER (WHERE score_a=score_b)::int AS draws
      FROM scores
      GROUP BY mode
      `,
      [userId]
    );

    // 2) En çok karşılaşılan rakipler + H2H özeti (top 10)
    const h2h = await all(
      `
      WITH scores AS (
        SELECT
          m.id,
          m.mode,
          m.user_a_id,
          m.user_b_id,
          SUM(
            CASE WHEN da.user_id = m.user_a_id THEN
              CASE WHEN da.answer='bilmem' THEN 0
                   WHEN da.is_correct=1   THEN q.point
                   ELSE -q.point
              END ELSE 0 END
          ) AS score_a,
          SUM(
            CASE WHEN da.user_id = m.user_b_id THEN
              CASE WHEN da.answer='bilmem' THEN 0
                   WHEN da.is_correct=1   THEN q.point
                   ELSE -q.point
              END ELSE 0 END
          ) AS score_b
        FROM duello_matches m
        JOIN duello_answers da ON da.match_id = m.id
        JOIN questions q ON q.id = da.question_id
        WHERE m.state='finished' AND (m.user_a_id=$1 OR m.user_b_id=$1)
        GROUP BY m.id, m.mode, m.user_a_id, m.user_b_id
      )
      SELECT
        CASE WHEN user_a_id=$1 THEN user_b_id ELSE user_a_id END AS opponent_id,
        COUNT(*)::int AS played,
        COUNT(*) FILTER (
          WHERE (user_a_id=$1 AND score_a>score_b) OR (user_b_id=$1 AND score_b>score_a)
        )::int AS wins,
        COUNT(*) FILTER (
          WHERE (user_a_id=$1 AND score_a<score_b) OR (user_b_id=$1 AND score_b<score_a)
        )::int AS losses,
        COUNT(*) FILTER (WHERE score_a=score_b)::int AS draws
      FROM scores
      GROUP BY opponent_id
      ORDER BY played DESC
      LIMIT 10
      `,
      [userId]
    );

    // opponent bilgilerini getir
    const oppIds = h2h.map(r => Number(r.opponent_id));
    let oppMap = {};
    if (oppIds.length) {
      const rows = await all(
        `SELECT id, ad, soyad, user_code FROM users WHERE id = ANY($1::int[])`,
        [oppIds]
      );
      for (const r of rows) oppMap[Number(r.id)] = r;
    }

    return res.json({
      success: true,
      user_id: userId,
      by_mode: modeRows,     // [{ mode:'info'|'speed', played, wins, losses, draws }]
      top_opponents: h2h.map(x => ({
        opponent: oppMap[Number(x.opponent_id)] || { id: Number(x.opponent_id) },
        played: x.played, wins: x.wins, losses: x.losses, draws: x.draws
      }))
    });
  } catch (e) {
    res.status(500).json({ error: "Kullanıcı düello istatistikleri alınamadı: " + e.message });
  }
});



/* ---------- GÜNLÜK YARIŞMA ---------- */
async function getOrCreateDailySetIds(dayKey, size) {
  const existing = await all(
    `SELECT question_id
       FROM daily_contest_questions
      WHERE (day_key = $1 OR contest_date = $2::date)
      ORDER BY COALESCE(pos, seq + 1) ASC`,
    [dayKey, dayKey]
  );
  if (existing.length > 0) return existing.map(r => r.question_id);

  const preferred = await all(
    `
    SELECT q.id
      FROM questions q
      JOIN surveys s ON s.id = q.survey_id
     WHERE s.status='approved'
       AND COALESCE(q.qtype,1) = 2
     ORDER BY md5($1 || '-' || $2 || '-' || q.id::text)
     LIMIT $3
    `,
    [DAILY_CONTEST_SECRET, dayKey, size]
  );
  const need = size - preferred.length;

  const fillers = need > 0 ? await all(
    `
    SELECT q.id
      FROM questions q
      JOIN surveys s ON s.id = q.survey_id
     WHERE s.status='approved'
       AND COALESCE(q.qtype,1) = 1
       AND q.id <> ALL($4::int[])
     ORDER BY md5($1 || '-' || $2 || '-' || q.id::text)
     LIMIT $3
    `,
    [DAILY_CONTEST_SECRET, dayKey, need, preferred.map(r => r.id)]
  ) : [];

  const ids = preferred.concat(fillers).map(r => r.id).slice(0, size);

  for (let i = 0; i < ids.length; i++) {
    await run(
      `
      INSERT INTO daily_contest_questions (day_key, contest_date, pos, seq, question_id)
      SELECT $1::text, $2::date, $3::int, $4::int, $5::int
      WHERE NOT EXISTS (
        SELECT 1 FROM daily_contest_questions
         WHERE (day_key = $1 OR contest_date = $2::date)
           AND COALESCE(pos, seq + 1) = $3
      )
      `,
      [dayKey, dayKey, i + 1, i, ids[i]]
    );
  }

  const finalRows = await all(
    `SELECT question_id
       FROM daily_contest_questions
      WHERE (day_key = $1 OR contest_date = $2::date)
      ORDER BY COALESCE(pos, seq + 1) ASC`,
    [dayKey, dayKey]
  );
  return finalRows.map(r => r.question_id);
}

async function getOrCreateDailySession(userId, dayKey) {
  let s = await get(
    `SELECT * FROM daily_sessions WHERE user_id=$1 AND day_key=$2`,
    [userId, dayKey]
  );
  if (!s) {
    await run(
      `INSERT INTO daily_sessions (user_id, day_key, current_index, finished)
       VALUES ($1,$2,0,false)`,
      [userId, dayKey]
    );
    s = await get(
      `SELECT * FROM daily_sessions WHERE user_id=$1 AND day_key=$2`,
      [userId, dayKey]
    );
  }
  return s;
}

app.get("/api/daily/status", async (req, res) => {
  try {
    const user_id = Number(req.query.user_id);
    if (!user_id) return res.status(400).json({ error: "user_id zorunlu" });

    const dayKey = await getDayKey();
    const size = DAILY_CONTEST_SIZE;

    const session = await getOrCreateDailySession(user_id, dayKey);
    const idx = Math.max(0, Number(session.current_index || 0));
    const ids = await getOrCreateDailySetIds(dayKey, size);
    const finished = !!session.finished || idx >= ids.length;

    const st = await get(
      `SELECT COALESCE(current_streak,0) AS cur, last_day_key::text AS last
         FROM user_daily_streak WHERE user_id=$1`,
      [user_id]
    );
    const streak_current = st?.cur || 0;
    const todayBonusBase = (st?.last === dayKey) ? Math.max(0, streak_current - 1) : streak_current;
    const today_bonus_per_correct = streakBonus(todayBonusBase);

    if (finished) {
      return res.json({
        success: true,
        day_key: dayKey,
        finished: true,
        index: Math.min(idx, ids.length),
        size: ids.length,
        question: null,
        streak_current,
        today_bonus_per_correct
      });
    }

    const q = await get(`
      SELECT q.id, q.question, q.point, q.survey_id, s.title AS survey_title
        FROM questions q
        LEFT JOIN surveys s ON s.id = q.survey_id
       WHERE q.id = $1
    `, [ids[idx]]);

    if (!q) return res.status(500).json({ error: "Soru setinde geçersiz id" });

    res.json({
      success: true,
      day_key: dayKey,
      finished: false,
      index: idx,
      size: ids.length,
      question: q,
      streak_current,
      today_bonus_per_correct
    });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "Günlük durum alınamadı: " + e.message });
  }
});

app.post("/api/daily/answer", async (req, res) => {
  try {
    const { user_id, question_id, answer, time_left_seconds, max_time_seconds } = req.body;
    if (!user_id || !question_id) {
      return res.status(400).json({ error: "user_id ve question_id zorunludur." });
    }

    const dayKey = await getDayKey();
    const size = DAILY_CONTEST_SIZE;
    const session = await getOrCreateDailySession(user_id, dayKey);
    const ids = await getOrCreateDailySetIds(dayKey, size);

    // set zaten bitmişse: idempotent streak + dönüş
    if (session.finished || session.current_index >= ids.length) {
      try { await upsertDailyStreakOnFinish(user_id, dayKey); } catch (err) {
        console.error("streak update fail:", err.message);
      }
      return res.json({ success: true, finished: true, message: "Bugünün yarışması tamamlandı" });
    }

    const expectedId = ids[session.current_index];
    if (!expectedId || Number(expectedId) !== Number(question_id)) {
      return res.status(409).json({ error: "Soru senkron değil. Sayfayı yenileyin." });
    }

    const q = await get(`SELECT correct_answer FROM questions WHERE id=$1`, [question_id]);
    if (!q) return res.status(400).json({ error: "Soru bulunamadı!" });

    const norm = normalizeAnswer(answer);
    const is_correct = q.correct_answer === norm ? 1 : 0;

    const parsedMax = Number(max_time_seconds);
    const parsedLeft = Number(time_left_seconds);
    const maxSec = Number.isFinite(parsedMax) ? Math.max(1, Math.min(120, Math.round(parsedMax))) : 24;
    const leftSecRaw = Number.isFinite(parsedLeft) ? Math.round(parsedLeft) : 0;
    const leftSec = Math.max(0, Math.min(leftSecRaw, maxSec));
    const earned = leftSec;

    // bonus: o anki streak (dün dahil)
    const st = await get(
      `SELECT COALESCE(current_streak,0) AS cur, last_day_key::text AS last
         FROM user_daily_streak WHERE user_id=$1`,
      [user_id]
    );
    const curStreak = st?.cur || 0;
    const bonusPerCorrect = streakBonus(curStreak);
    const bonusPoints = (is_correct === 1) ? bonusPerCorrect : 0;

    await insertAnswer({
      user_id,
      question_id,
      norm_answer: norm,
      is_correct,
      maxSec,
      leftSec,
      earned,
      isDaily: true,
      dailyKey: dayKey,
      bonusPoints,
      streakLenAtAnswer: curStreak
    });

    const nextIndex = session.current_index + 1;
    const isFinished = nextIndex >= ids.length;
    await run(
      `UPDATE daily_sessions
         SET current_index=$1, finished=$2, updated_at=timezone('Europe/Istanbul', now())
       WHERE id=$3`,
      [nextIndex, isFinished, session.id]
    );

    let awarded_books = 0;
    if (isFinished) {
      const ins = await get(`
        INSERT INTO daily_finisher_awards (user_id, day_key, amount)
        VALUES ($1, $2, 2)
        ON CONFLICT (user_id, day_key) DO NOTHING
        RETURNING 1 AS ok
      `, [user_id, dayKey]);

      if (ins?.ok) {
        await run(`UPDATE users SET books = COALESCE(books,0) + 2 WHERE id=$1`, [user_id]);
        awarded_books = 2;
      }

      // set bitti: streak’i artır
      try { await upsertDailyStreakOnFinish(user_id, dayKey); } catch (err) {
        console.error("streak update fail:", err.message);
      }
    }

    return res.json({
      success: true,
      is_correct,
      index: nextIndex,
      finished: isFinished,
      awarded_books,
      bonus_points: bonusPoints
    });

  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "Günlük cevap kaydedilemedi" });
  }
});

app.post("/api/daily/skip", async (req, res) => {
  try {
    const { user_id, question_id, time_left_seconds, max_time_seconds } = req.body;
    if (!user_id || !question_id) {
      return res.status(400).json({ error: "user_id ve question_id zorunludur." });
    }

    const dayKey = await getDayKey();
    const size = DAILY_CONTEST_SIZE;
    const session = await getOrCreateDailySession(user_id, dayKey);
    const ids = await getOrCreateDailySetIds(dayKey, size);

    // Set zaten bitmişse: idempotent streak + dönüş
    if (session.finished || session.current_index >= ids.length) {
      try { await upsertDailyStreakOnFinish(user_id, dayKey); } catch (err) {
        console.error("streak update fail:", err.message);
      }
      return res.json({ success: true, finished: true });
    }

    // Doğru sıradaki soruyu kontrol et
    const expectedId = ids[session.current_index];
    if (!expectedId || Number(expectedId) !== Number(question_id)) {
      return res.status(409).json({ error: "Soru senkron değil. Sayfayı yenileyin." });
    }

    // Süre hesapları
    const parsedMax = Number(max_time_seconds);
    const parsedLeft = Number(time_left_seconds);
    const maxSec = Number.isFinite(parsedMax) ? Math.max(1, Math.min(120, Math.round(parsedMax))) : 24;
    const leftSecRaw = Number.isFinite(parsedLeft) ? Math.round(parsedLeft) : 0;
    const leftSec = Math.max(0, Math.min(leftSecRaw, maxSec));
    const earned = leftSec;

    // Skip = "bilmem", puan yazılmaz ama kayıt atılır
    await insertAnswer({
      user_id,
      question_id,
      norm_answer: "bilmem",
      is_correct: 0,
      maxSec,
      leftSec,
      earned,
      isDaily: true,
      dailyKey: dayKey
    });

    // İlerlet
    const nextIndex = session.current_index + 1;
    const isFinished = nextIndex >= ids.length;

    await run(
      `UPDATE daily_sessions
         SET current_index=$1, finished=$2, updated_at=timezone('Europe/Istanbul', now())
       WHERE id=$3`,
      [nextIndex, isFinished, session.id]
    );

    // Bittiyse günlük tamamlama ödülü + streak güncelle
    let awarded_books = 0;
    if (isFinished) {
      const ins = await get(`
        INSERT INTO daily_finisher_awards (user_id, day_key, amount)
        VALUES ($1, $2, 2)
        ON CONFLICT (user_id, day_key) DO NOTHING
        RETURNING 1 AS ok
      `, [user_id, dayKey]);

      if (ins?.ok) {
        await run(`UPDATE users SET books = COALESCE(books,0) + 2 WHERE id=$1`, [user_id]);
        awarded_books = 2;
      }

      try { await upsertDailyStreakOnFinish(user_id, dayKey); } catch (err) {
        console.error("streak update fail:", err.message);
      }
    }

    return res.json({ success: true, index: nextIndex, finished: isFinished, awarded_books });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: "Günlük skip kaydedilemedi" });
  }
});

/* ---------- LEADERBOARDS & STATS ---------- */
app.get("/api/daily/leaderboard", async (req, res) => {
  try {
    const dayKey = req.query.day || await getDayKey();
    const rows = await all(
      `
      SELECT
        u.id,
        u.ad,
        u.soyad,
        COUNT(a.*)::int AS answered_count,
        COALESCE(SUM(
          CASE
            WHEN a.answer = 'bilmem' THEN 0
            WHEN a.is_correct = 1 THEN q.point + COALESCE(a.bonus_points,0)
            WHEN a.is_correct = 0 THEN -q.point
            ELSE 0
          END
        ),0)::int AS total_points,
        COALESCE(SUM(GREATEST(a.max_time_seconds,0) - GREATEST(a.time_left_seconds,0)),0)::int AS time_spent
      FROM users u
      INNER JOIN answers a ON a.user_id = u.id
      INNER JOIN questions q ON q.id = a.question_id
      WHERE a.is_daily = true
        AND a.daily_key = $1
      GROUP BY u.id, u.ad, u.soyad
      HAVING COUNT(a.*) > 0
      ORDER BY total_points DESC, time_spent ASC, u.id ASC
      LIMIT 100
      `,
      [dayKey]
    );
    res.json({ success: true, day_key: dayKey, leaderboard: rows });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "Günlük leaderboard alınamadı" });
  }
});

app.get("/api/daily/champions", async (req, res) => {
  try {
    const limit = 10;
    const since = req.query.since || null;

    const params = [];
    let where = `b.rank = 1`;
    if (since) { where += ` AND b.day_key >= $1`; params.push(since); }

    const rows = await all(
      `
      SELECT
        u.id,
        u.ad,
        u.soyad,
        COUNT(*)::int AS wins,
        COUNT(*)::int AS first_wins,
        MIN(b.day_key) AS first_win_day,
        MAX(b.day_key) AS last_win_day
      FROM book_awards b
      JOIN users u ON u.id = b.user_id
      WHERE ${where}
      GROUP BY u.id, u.ad, u.soyad
      ORDER BY wins DESC, last_win_day DESC, u.id ASC
      LIMIT $${params.length + 1}
      `,
      [...params, limit]
    );

    res.json({ success: true, champions: rows });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "Günün birincileri alınamadı: " + e.message });
  }
});

app.get("/api/important-day", async (req, res) => {
  try {
    const dayKey = req.query.day || await getDayKey();
    const row = await get(
      `SELECT day_key, daytitle, description FROM impdays WHERE day_key=$1`,
      [dayKey]
    );
    return res.json({ success: true, day_key: dayKey, record: row || null });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "Important day alınamadı" });
  }
});

app.get("/api/user/:userId/books", async (req, res) => {
  try {
    const row = await get(`SELECT COALESCE(books,0)::int AS books FROM users WHERE id=$1`, [req.params.userId]);
    res.json({ success: true, books: row?.books || 0 });
  } catch {
    res.status(500).json({ error: "Kitap bilgisi alınamadı" });
  }
});

app.post("/api/books/spend", async (req, res) => {
  try {
    const { user_id, question_id } = req.body || {};
    if (!user_id || !question_id) return res.status(400).json({ error: "user_id ve question_id zorunludur." });

    const q = await get(`SELECT correct_answer FROM questions WHERE id=$1`, [question_id]);
    if (!q) return res.status(404).json({ error: "Soru bulunamadı" });

    const dec = await get(
      `UPDATE users SET books = COALESCE(books,0) - 1
       WHERE id=$1 AND COALESCE(books,0) > 0
       RETURNING COALESCE(books,0)::int AS books`,
      [user_id]
    );
    if (!dec) return res.status(400).json({ error: "Yetersiz kitap." });

    const dayKey = await getDayKey();
    await run(`INSERT INTO book_spends (user_id, question_id, day_key, amount) VALUES ($1,$2,$3,1)`,
      [user_id, question_id, dayKey]);

    res.json({ success: true, remaining: dec.books, correct_answer: q.correct_answer });
  } catch (e) {
    res.status(500).json({ error: "Kitap kullanılamadı: " + e.message });
  }
});

app.post("/api/daily/award-books", async (req, res) => {
  try {
    // Basit secret kontrolü (manuel tetiklemeyi koru)
    if (req.body?.secret !== process.env.ADMIN_SECRET) {
      return res.status(403).json({ error: "Yetkisiz" });
    }

    const targetDay = req.body?.day || await getYesterdayKey();
    if (!targetDay) return res.status(400).json({ error: "day belirlenemedi" });

    const winners = await all(
      `
      SELECT
        u.id,
        COALESCE(SUM(
          CASE
            WHEN a.answer = 'bilmem' THEN 0
            WHEN a.is_correct = 1 THEN q.point + COALESCE(a.bonus_points,0)
            WHEN a.is_correct = 0 THEN -q.point
            ELSE 0
          END
        ),0)::int AS total_points,
        COALESCE(SUM(GREATEST(a.max_time_seconds,0) - GREATEST(a.time_left_seconds,0)),0)::int AS time_spent
      FROM users u
      INNER JOIN answers a ON a.user_id = u.id
      INNER JOIN questions q ON q.id = a.question_id
      WHERE a.is_daily = true
        AND a.daily_key = $1
      GROUP BY u.id
      HAVING COUNT(a.*) > 0
      ORDER BY total_points DESC, time_spent ASC, u.id ASC
      LIMIT 3
      `,
      [targetDay]
    );

    const prizes = [5, 3, 1];
    const awarded = [];

    for (let i = 0; i < winners.length; i++) {
      const u = winners[i];
      const amount = prizes[i] || 0;
      if (amount <= 0) continue;

      const ins = await get(
        `INSERT INTO book_awards (user_id, day_key, rank, amount)
         VALUES ($1,$2,$3,$4)
         ON CONFLICT (user_id, day_key) DO NOTHING
         RETURNING 1 AS ok`,
        [u.id, targetDay, i + 1, amount]
      );

      if (ins?.ok) {
        await run(`UPDATE users SET books = COALESCE(books,0) + $1 WHERE id=$2`, [amount, u.id]);
        awarded.push({ user_id: u.id, rank: i + 1, amount });
      }
    }

    res.json({ success: true, day: targetDay, awarded });
  } catch (e) {
    res.status(500).json({ error: "Ödül verilemedi: " + e.message });
  }
});

/* ---------- SPEED TIER ---------- */
app.get("/api/user/:userId/speed-tier", async (req, res) => {
  try {
    const userId = Number(req.params.userId);

    const thr = await get(
      `
      WITH agg AS (
        SELECT user_id, AVG(earned_seconds)::numeric AS avg_earned
        FROM answers
        WHERE earned_seconds IS NOT NULL
          AND answer != 'bilmem'
        GROUP BY user_id
      )
      SELECT
        percentile_disc(0.25) WITHIN GROUP (ORDER BY avg_earned) AS p25,
        percentile_disc(0.50) WITHIN GROUP (ORDER BY avg_earned) AS p50,
        percentile_disc(0.75) WITHIN GROUP (ORDER BY avg_earned) AS p75
      FROM agg
      `
    );

    const me = await get(
      `
      SELECT
        ROUND(AVG(earned_seconds)::numeric, 2) AS avg_earned_seconds,
        ROUND(AVG(GREATEST(max_time_seconds,0) - GREATEST(time_left_seconds,0))::numeric, 2)
          AS avg_spent_seconds,
        COUNT(*)::int AS answers_count
      FROM answers
      WHERE user_id = $1
        AND earned_seconds IS NOT NULL
        AND max_time_seconds IS NOT NULL
        AND time_left_seconds IS NOT NULL
        AND answer != 'bilmem'
      `,
      [userId]
    );

    if (!me || !me.answers_count) {
      return res.json({
        success: true,
        tier: null,
        avg_earned_seconds: null,
        avg_spent_seconds: null,
        thresholds: thr || null,
        answers_count: 0,
      });
    }

    const avg = Number(me.avg_earned_seconds);
    const p25 = Number(thr?.p25 ?? 6);
    const p50 = Number(thr?.p50 ?? 10);
    const p75 = Number(thr?.p75 ?? 16);

    let tier = "garantici";
    if (avg >= p75)       tier = "alim";
    else if (avg >= p50)  tier = "cesur";
    else if (avg >= p25)  tier = "tedbirli";

    return res.json({
      success: true,
      tier,
      avg_earned_seconds: avg,
      avg_spent_seconds: Number(me.avg_spent_seconds),
      thresholds: { p25, p50, p75 },
      answers_count: me.answers_count
    });
  } catch (e) {
    return res.status(500).json({ error: "Hız kademesi hesaplanamadı: " + e.message });
  }
});

/* ---------- ADMIN STATS & LEADERBOARDS ---------- */
app.get("/api/admin/statistics", async (_req, res) => {
  try {
    const a = await get(`SELECT COUNT(*)::int AS count FROM users`);
    const b = await get(`SELECT COUNT(DISTINCT user_id)::int AS count FROM answers`);
    const c = await get(`SELECT COUNT(*)::int AS count FROM surveys WHERE status='approved'`);
    const d = await get(`SELECT COUNT(*)::int AS count FROM questions`);
    const e = await get(`SELECT COUNT(*)::int AS count FROM answers`);
    const f = await get(`SELECT COUNT(*)::int AS count FROM answers WHERE is_correct=1`);
    const g = await get(`SELECT COUNT(*)::int AS count FROM answers WHERE is_correct=0 AND answer!='bilmem'`);
    const h = await get(`SELECT COUNT(*)::int AS count FROM answers WHERE answer='bilmem'`);
    res.json({
      success: true,
      total_users: a?.count || 0,
      total_active_users: b?.count || 0,
      total_approved_surveys: c?.count || 0,
      total_questions: d?.count || 0,
      total_answers: e?.count || 0,
      total_correct_answers: f?.count || 0,
      total_wrong_answers: g?.count || 0,
      total_bilmem: h?.count || 0,
    });
  } catch { res.status(500).json({ error: "İstatistik hatası!" }); }
});

app.get("/api/leaderboard", async (req, res) => {
  try {
    const period = req.query.period || "all";
    const periodClause = periodSql(period, "a");

    const rows = await all(
      `
      SELECT u.id, u.ad, u.soyad,
        COALESCE(SUM(
          CASE
            WHEN a.answer = 'bilmem' THEN 0
            WHEN a.is_correct = 1 THEN q.point + COALESCE(a.bonus_points,0)
            WHEN a.is_correct = 0 THEN -q.point
            ELSE 0
          END
        ), 0)::int AS total_points
      FROM users u
      LEFT JOIN answers a ON a.user_id = u.id
      LEFT JOIN questions q ON a.question_id = q.id
      WHERE 1=1
        ${periodClause}
      GROUP BY u.id
      HAVING COALESCE(SUM(
          CASE
            WHEN a.answer = 'bilmem' THEN 0
            WHEN a.is_correct = 1 THEN q.point + COALESCE(a.bonus_points,0)
            WHEN a.is_correct = 0 THEN -q.point
            ELSE 0
          END
        ), 0) != 0
      ORDER BY total_points DESC, u.id ASC
      `
    );
    res.json({ success: true, leaderboard: rows });
  } catch {
    res.status(500).json({ error: "Liste alınamadı" });
  }
});

app.get("/api/user/:userId/rank", async (req, res) => {
  try {
    const userId = req.params.userId;
    const period = req.query.period || "all";
    const periodClause = periodSql(period, "a");

    const rows = await all(
      `
      SELECT u.id,
        COALESCE(SUM(
          CASE
            WHEN a.answer = 'bilmem' THEN 0
            WHEN a.is_correct = 1 THEN q.point + COALESCE(a.bonus_points,0)
            WHEN a.is_correct = 0 THEN -q.point
            ELSE 0
          END
        ), 0)::int AS total_points
      FROM users u
      LEFT JOIN answers a ON a.user_id = u.id
      LEFT JOIN questions q ON a.question_id = q.id
      WHERE 1=1
        ${periodClause}
      GROUP BY u.id
      ORDER BY total_points DESC, u.id ASC
      `
    );

    const rank = rows.findIndex(r => String(r.id) === String(userId)) + 1;
    const total_users = rows.length;
    const user_points = rank > 0 ? rows[rank - 1].total_points : 0;
    res.json({ success: true, rank, total_users, user_points });
  } catch {
    res.status(500).json({ error: "Sıralama alınamadı" });
  }
});

app.get("/api/surveys/:surveyId/leaderboard", async (req, res) => {
  try {
    const surveyId = req.params.surveyId;
    const period = req.query.period || "all";
    const periodClause = periodSql(period, "a");

    const rows = await all(
      `
      SELECT u.id, u.ad, u.soyad,
        COALESCE(SUM(
          CASE
            WHEN a.answer = 'bilmem' THEN 0
            WHEN a.is_correct = 1 THEN q.point + COALESCE(a.bonus_points,0)
            WHEN a.is_correct = 0 THEN -q.point
            ELSE 0
          END
        ), 0)::int AS total_points
      FROM users u
      INNER JOIN answers a ON a.user_id = u.id
      INNER JOIN questions q ON a.question_id = q.id
      WHERE 1=1
        ${periodClause}
        AND a.question_id IN (SELECT id FROM questions WHERE survey_id = $1)
      GROUP BY u.id
      HAVING COALESCE(SUM(
          CASE
            WHEN a.answer = 'bilmem' THEN 0
            WHEN a.is_correct = 1 THEN q.point + COALESCE(a.bonus_points,0)
            WHEN a.is_correct = 0 THEN -q.point
            ELSE 0
          END
        ), 0) != 0
      ORDER BY total_points DESC, u.id ASC
      `,
      [surveyId]
    );
    res.json({ success: true, leaderboard: rows });
  } catch {
    res.status(500).json({ error: "Anket leaderboard alınamadı!" });
  }
});

/* === AWARD SCHEDULER === */
let lastAwardedFor = null;

async function awardSchedulerTick() {
  try {
    const yKey = await getYesterdayKey();
    if (!yKey) return;

    if (lastAwardedFor === yKey) return;

    const already = await get(`SELECT 1 FROM book_awards WHERE day_key=$1 LIMIT 1`, [yKey]);
    if (already) { lastAwardedFor = yKey; return; }

    const winners = await all(
      `
      SELECT
        u.id,
        COALESCE(SUM(
          CASE
            WHEN a.answer = 'bilmem' THEN 0
            WHEN a.is_correct = 1 THEN q.point + COALESCE(a.bonus_points,0)
            WHEN a.is_correct = 0 THEN -q.point
            ELSE 0
          END
        ),0)::int AS total_points,
        COALESCE(SUM(GREATEST(a.max_time_seconds,0) - GREATEST(a.time_left_seconds,0)),0)::int AS time_spent
      FROM users u
      INNER JOIN answers a ON a.user_id = u.id
      INNER JOIN questions q ON q.id = a.question_id
      WHERE a.is_daily = true
        AND a.daily_key = $1
      GROUP BY u.id
      HAVING COUNT(a.*) > 0
      ORDER BY total_points DESC, time_spent ASC, u.id ASC
      LIMIT 3
      `,
      [yKey]
    );

    const prizes = [5, 3, 1];

    for (let i = 0; i < winners.length; i++) {
      const u = winners[i];
      const amount = prizes[i] || 0;
      if (amount <= 0) continue;

      const ins = await get(
        `INSERT INTO book_awards (user_id, day_key, rank, amount)
         VALUES ($1,$2,$3,$4)
         ON CONFLICT (user_id, day_key) DO NOTHING
         RETURNING 1 AS ok`,
        [u.id, yKey, i + 1, amount]
      );

      if (ins?.ok) {
        await run(`UPDATE users SET books = COALESCE(books,0) + $1 WHERE id=$2`, [amount, u.id]);
        console.log("Ödül verildi:", yKey, "user", u.id, "rank", i + 1, "amount", amount);
      }
    }

    lastAwardedFor = yKey;
  } catch (e) {
    console.error("awardSchedulerTick hata:", e.message);
  }
}

/* ---------- START ---------- */
const PORT = process.env.PORT || 5000;
app.listen(PORT, "0.0.0.0", () => {
  console.log(`Backend http://0.0.0.0:${PORT} üzerinde çalışıyor`);
});
