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

/* sqlite benzeri yardımcılar */
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

  // Güvenli şekilde ALTER (eski şemalardan gelenler için)
  await run(`
    ALTER TABLE answers
      ADD COLUMN IF NOT EXISTS max_time_seconds integer,
      ADD COLUMN IF NOT EXISTS time_left_seconds integer,
      ADD COLUMN IF NOT EXISTS earned_seconds integer,
      ADD COLUMN IF NOT EXISTS is_daily boolean DEFAULT false,
      ADD COLUMN IF NOT EXISTS daily_key text
  `);

  // Günlük yarışma oturumları
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

  // QUOTES tablosu
  await run(`CREATE TABLE IF NOT EXISTS quotes (
    id SERIAL PRIMARY KEY,
    text TEXT NOT NULL,
    author TEXT
  )`);

  // (İsteğe bağlı) Sorgu hızlandırıcı index'ler
  await run(`CREATE INDEX IF NOT EXISTS idx_answers_daily ON answers (is_daily, daily_key, user_id)`);
  await run(`CREATE INDEX IF NOT EXISTS idx_answers_user_q ON answers (user_id, question_id)`);

  console.log("PostgreSQL tablolar hazır");
}
init().catch(e => { console.error(e); process.exit(1); });

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

/* --------- CEVAP KAYDI: süre bilgisi + earned_seconds --------- */
async function insertAnswer({
  user_id, question_id, norm_answer, is_correct,
  maxSec, leftSec, earned, isDaily = false, dailyKey = null
}) {
  await run(
    `INSERT INTO answers
       (user_id, question_id, answer, is_correct, created_at,
        max_time_seconds, time_left_seconds, earned_seconds,
        is_daily, daily_key)
     VALUES ($1,$2,$3,$4,timezone('Europe/Istanbul', now()),
             $5,$6,$7,$8,$9)`,
    [user_id, question_id, norm_answer, is_correct, maxSec, leftSec, earned, isDaily, dailyKey]
  );
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
      `SELECT a.answer, a.is_correct, q.point
       FROM answers a
       INNER JOIN questions q ON a.question_id = q.id
       WHERE a.user_id=$1`, [req.params.userId]
    );
    let total = 0;
    rows.forEach(r => {
      if (r.answer === "bilmem") { /* 0 */ }
      else if (r.is_correct === 1) total += r.point || 1;
      else total -= r.point || 1;
    });
    res.json({ success: true, totalPoints: total, answeredCount: rows.length });
  } catch { res.status(500).json({ error: "Puan alınamadı" }); }
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

// Günün soru seti (deterministik ve herkes için aynı)
async function dailyQuestionSet(dayKey, limit) {
  return await all(
    `
    SELECT q.id, q.question, q.point
    FROM questions q
    INNER JOIN surveys s ON s.id = q.survey_id
    WHERE s.status='approved'
    ORDER BY md5($1 || '-' || $2 || '-' || q.id::text)
    LIMIT $3
    `,
    [DAILY_CONTEST_SECRET, dayKey, Math.max(1, limit)]
  );
}

// Oturum getir/oluştur
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

// Status: index, soru, bitiş
app.get("/api/daily/status", async (req, res) => {
  try {
    const user_id = Number(req.query.user_id);
    if (!user_id) return res.status(400).json({ error: "user_id zorunlu" });

    const dayKey = await getDayKey();
    const size = DAILY_CONTEST_SIZE;

    const session = await getOrCreateDailySession(user_id, dayKey);
    let idx = Math.max(0, Number(session.current_index || 0));
    const finished = !!session.finished || idx >= size;

    if (finished) {
      return res.json({
        success: true,
        day_key: dayKey,
        finished: true,
        index: Math.min(idx, size),
        size,
        question: null,
      });
    }

    const set = await dailyQuestionSet(dayKey, size);
    if (!set || set.length === 0) {
      return res.json({ success: true, day_key: dayKey, finished: true, index: 0, size: 0, question: null });
    }

    // Emniyet: idx üst sınırı aşmışsa bitir
    if (idx >= set.length) {
      await run(
        `UPDATE daily_sessions SET current_index=$1, finished=true, updated_at=timezone('Europe/Istanbul', now())
         WHERE id=$2`,
        [set.length, session.id]
      );
      return res.json({
        success: true,
        day_key: dayKey,
        finished: true,
        index: set.length,
        size: set.length,
        question: null,
      });
    }

    const q = set[idx];
    res.json({
      success: true,
      day_key: dayKey,
      finished: false,
      index: idx,
      size: set.length,
      question: q,
    });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "Günlük durum alınamadı" });
  }
});

// Answer (E/H/B)
app.post("/api/daily/answer", async (req, res) => {
  try {
    const { user_id, question_id, answer, time_left_seconds, max_time_seconds } = req.body;
    if (!user_id || !question_id) {
      return res.status(400).json({ error: "user_id ve question_id zorunludur." });
    }
    const dayKey = await getDayKey();
    const size = DAILY_CONTEST_SIZE;

    const session = await getOrCreateDailySession(user_id, dayKey);
    if (session.finished || session.current_index >= size) {
      return res.json({ success: true, finished: true, message: "Bugünün yarışması tamamlandı" });
    }

    const set = await dailyQuestionSet(dayKey, size);
    const expected = set[session.current_index];
    if (!expected || Number(expected.id) !== Number(question_id)) {
      return res.status(409).json({ error: "Soru senkron değil. Sayfayı yenileyin." });
    }

    const q = await get(`SELECT correct_answer, point FROM questions WHERE id=$1`, [question_id]);
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
      user_id,
      question_id,
      norm_answer: norm,
      is_correct,
      maxSec,
      leftSec,
      earned,
      isDaily: true,
      dailyKey: dayKey
    });

    // ilerlet
    const nextIndex = session.current_index + 1;
    const isFinished = nextIndex >= set.length;
    await run(
      `UPDATE daily_sessions
         SET current_index=$1, finished=$2, updated_at=timezone('Europe/Istanbul', now())
       WHERE id=$3`,
      [nextIndex, isFinished, session.id]
    );

    res.json({
      success: true,
      is_correct,
      index: nextIndex,
      finished: isFinished
    });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "Günlük cevap kaydedilemedi" });
  }
});

// Skip (Şimdilik bu kadar): mevcut soruyu bilmem say
app.post("/api/daily/skip", async (req, res) => {
  try {
    const { user_id, question_id, time_left_seconds, max_time_seconds } = req.body;
    if (!user_id || !question_id) {
      return res.status(400).json({ error: "user_id ve question_id zorunludur." });
    }
    const dayKey = await getDayKey();
    const size = DAILY_CONTEST_SIZE;

    const session = await getOrCreateDailySession(user_id, dayKey);
    if (session.finished || session.current_index >= size) {
      return res.json({ success: true, finished: true });
    }

    const set = await dailyQuestionSet(dayKey, size);
    const expected = set[session.current_index];
    if (!expected || Number(expected.id) !== Number(question_id)) {
      return res.status(409).json({ error: "Soru senkron değil. Sayfayı yenileyin." });
    }

    const parsedMax = Number(max_time_seconds);
    const parsedLeft = Number(time_left_seconds);
    const maxSec = Number.isFinite(parsedMax) ? Math.max(1, Math.min(120, Math.round(parsedMax))) : 24;
    const leftSecRaw = Number.isFinite(parsedLeft) ? Math.round(parsedLeft) : 0;
    const leftSec = Math.max(0, Math.min(leftSecRaw, maxSec));
    const earned = leftSec;

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

    const nextIndex = session.current_index + 1;
    const isFinished = nextIndex >= set.length;
    await run(
      `UPDATE daily_sessions
         SET current_index=$1, finished=$2, updated_at=timezone('Europe/Istanbul', now())
       WHERE id=$3`,
      [nextIndex, isFinished, session.id]
    );

    res.json({ success: true, index: nextIndex, finished: isFinished });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "Günlük skip kaydedilemedi" });
  }
});

// Günlük Leaderboard (bugün)  << answered_count eklendi
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

/* ---------- STATS & LEADERBOARDS (İstanbul TZ ile) ---------- */
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

// GENEL PUAN TABLOSU
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
            WHEN a.is_correct = 1 THEN q.point
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
          WHEN a.is_correct = 1 THEN q.point
          WHEN a.is_correct = 0 THEN -q.point
          ELSE 0
        END
      ), 0) != 0
      ORDER BY total_points DESC, u.id ASC
      LIMIT 100
      `
    );
    res.json({ success: true, leaderboard: rows });
  } catch {
    res.status(500).json({ error: "Liste alınamadı" });
  }
});

// GENEL RANK
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
            WHEN a.is_correct = 1 THEN q.point
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

// KATEGORİ (ANKET) BAZLI PUAN TABLOSU
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
            WHEN a.is_correct = 1 THEN q.point
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
          WHEN a.is_correct = 1 THEN q.point
          WHEN a.is_correct = 0 THEN -q.point
          ELSE 0
        END
      ), 0) != 0
      ORDER BY total_points DESC, u.id ASC
      LIMIT 100
      `,
      [surveyId]
    );
    res.json({ success: true, leaderboard: rows });
  } catch {
    res.status(500).json({ error: "Anket leaderboard alınamadı!" });
  }
});

app.get("/api/user/approved-surveys", async (_req, res) => {
  try {
    const surveys = await all(`SELECT * FROM surveys WHERE status='approved' ORDER BY id DESC`);
    if (!surveys.length) return res.json({ success: true, surveys: [] });
    const filtered = [];
    for (const survey of surveys) {
      const row = await get(`SELECT COUNT(*)::int AS question_count FROM questions WHERE survey_id=$1`, [survey.id]);
      if ((row?.question_count || 0) > 0) filtered.push({ ...survey, question_count: row.question_count });
    }
    return res.json({ success: true, surveys: filtered });
  } catch { res.status(500).json({ error: "Listeleme hatası!" }); }
});

/* ---------- START ---------- */
const PORT = process.env.PORT || 5000;
app.listen(PORT, "0.0.0.0", () => {
  console.log(`Backend http://0.0.0.0:${PORT} üzerinde çalışıyor`);
});
