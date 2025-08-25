require("dotenv").config();
const express = require("express");
const cors = require("cors");
const bodyParser = require("body-parser");
const { Pool } = require("pg");

const app = express();
app.use(cors());
app.use(express.json({ limit: "5mb" }));
app.use(bodyParser.urlencoded({ extended: true, limit: "5mb" }));

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

/* ---------- ENV (Günlük Yarışma) ---------- */
const DAILY_CONTEST_SIZE = Math.max(1, parseInt(process.env.DAILY_CONTEST_SIZE || "128", 10));
const DAILY_CONTEST_SECRET = process.env.DAILY_CONTEST_SECRET || "felox-secret";

/* ---------- HEALTH ---------- */
app.get("/", (_req, res) => res.send("OK"));
app.get("/healthz", (_req, res) => res.send("healthy"));

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

  await run(`CREATE INDEX IF NOT EXISTS idx_answers_daily ON answers (is_daily, daily_key, user_id)`);
  await run(`CREATE INDEX IF NOT EXISTS idx_answers_user_q ON answers (user_id, question_id)`);
  await run(`CREATE INDEX IF NOT EXISTS idx_book_awards_rank_day ON book_awards (rank, day_key)`);
  await run(`CREATE INDEX IF NOT EXISTS idx_book_awards_user ON book_awards (user_id)`);
}

init()
  .then(() => {
    console.log("PostgreSQL tablolar hazır");
    awardSchedulerTick();
    setInterval(awardSchedulerTick, 5 * 60 * 1000);
  })
  .catch(e => { console.error(e); process.exit(1); });

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
      `SELECT id, ad, soyad, email, role, cinsiyet FROM users WHERE email=$1`,
      [email]
    );
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
      `SELECT id, ad, soyad, email, role, cinsiyet
       FROM users WHERE email=$1 AND password=$2`,
      [email, password]
    );
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
app.get("/api/surveys/:surveyId/questions", async (req, res) => {
  try {
    const rows = await all(`SELECT * FROM questions WHERE survey_id=$1 ORDER BY id ASC`, [req.params.surveyId]);
    res.json({ success: true, questions: rows });
  } catch { res.status(500).json({ error: "Soru listesi hatası!" }); }
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
                          WHEN a.is_correct = 1 THEN q.point ELSE 0 END),0)::int AS earned_points,
        COALESCE(SUM(CASE WHEN a.answer = 'bilmem' THEN 0 ELSE q.point END),0)::int AS possible_points,
        COALESCE(SUM(CASE
            WHEN a.answer = 'bilmem' THEN 0
            WHEN a.is_correct = 1 THEN q.point
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

/* ---------- KADEMELİ YARIŞ ---------- */
app.get("/api/user/:userId/kademeli-questions", async (req, res) => {
  try {
    const userId = Number(req.params.userId);
    const point = Number(req.query.point || 1);
    const limit = Math.min(1000, Math.max(10, Number(req.query.limit || 200)));
    if (!point || point < 1 || point > 10) {
      return res.status(400).json({ error: "Geçersiz point. 1-10 arası olmalı." });
    }

    const rows = await all(
      `
      SELECT q.*
      FROM questions q
      INNER JOIN surveys s ON s.id = q.survey_id
      WHERE s.status = 'approved'
        AND q.point = $2
        AND q.id NOT IN (
          SELECT a.question_id FROM answers a
          WHERE a.user_id = $1 AND a.is_correct = 1
        )
      ORDER BY RANDOM()
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

    const row = await get(
      `
      SELECT
        COALESCE(SUM(CASE WHEN a.answer != 'bilmem' THEN 1 ELSE 0 END),0)::int AS attempted,
        COALESCE(SUM(CASE WHEN a.is_correct = 1 THEN 1 ELSE 0 END),0)::int AS correct
      FROM answers a
      INNER JOIN questions q ON q.id = a.question_id
      WHERE a.user_id = $1 AND q.point = $2
      `,
      [userId, point]
    );

    const attempted = row?.attempted || 0;
    const correct = row?.correct || 0;
    const success_rate = attempted > 0 ? correct / attempted : 0;

    res.json({
      success: true,
      point,
      attempted,
      correct,
      success_rate,
      can_level_up: (attempted >= 100 && success_rate >= 0.8)
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

    const row = await get(
      `
      SELECT
        COALESCE(SUM(CASE WHEN a.answer != 'bilmem' THEN 1 ELSE 0 END),0)::int AS attempted,
        COALESCE(SUM(CASE WHEN a.is_correct = 1 THEN 1 ELSE 0 END),0)::int AS correct
      FROM answers a
      INNER JOIN questions q ON q.id = a.question_id
      WHERE a.user_id = $1 AND q.point = $2
      `,
      [userId, point]
    );

    const attempted = row?.attempted || 0;
    const correct = row?.correct || 0;
    const success_rate = attempted > 0 ? correct / attempted : 0;
    const ok = (attempted >= 100 && success_rate >= 0.8);

    if (ok) {
      if (point >= 10) {
        return res.json({ success: true, status: "genius", can_level_up: true, next_point: 10 });
      }
      return res.json({ success: true, status: "ok", can_level_up: true, next_point: point + 1 });
    }
    res.json({ success: true, status: "stay", can_level_up: false, next_point: point });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "Seviye kontrolü yapılamadı" });
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

    const q = await get(`SELECT id, question, point FROM questions WHERE id=$1`, [ids[idx]]);
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

    return res.json({ success: true, is_correct, index: nextIndex, finished: isFinished, awarded_books });
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
            WHEN a.is_correct = 1 THEN q.point
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
    const targetDay = req.body?.day || await getYesterdayKey();
    if (!targetDay) return res.status(400).json({ error: "day belirlenemedi" });

    const winners = await all(
      `
      SELECT
        u.id,
        COALESCE(SUM(
          CASE
            WHEN a.answer = 'bilmem' THEN 0
            WHEN a.is_correct = 1 THEN q.point
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
            WHEN a.is_correct = 1 THEN q.point
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
