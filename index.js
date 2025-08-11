require('dotenv').config();
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
});
pool.connect()
  .then(() => console.log("PostgreSQL bağlantısı başarılı"))
  .catch((e) => { console.error("PostgreSQL bağlantı hatası:", e); process.exit(1); });

/* sqlite benzeri yardımcılar */
async function run(sql, params = []) { await pool.query(sql, params); return { success: true }; }
async function get(sql, params = []) { const { rows } = await pool.query(sql, params); return rows[0] || null; }
async function all(sql, params = []) { const { rows } = await pool.query(sql, params); return rows; }

/* ---------- HEALTH ---------- */
app.get("/", (_req, res) => res.send("OK"));
app.get("/healthz", (_req, res) => res.send("healthy"));

/* ---------- HELPERS ---------- */
function getDateRange(type) {
  const now = new Date();
  let start, end;
  end = now.toISOString().slice(0, 10) + " 23:59:59";
  if (type === "today") {
    start = now.toISOString().slice(0, 10) + " 00:00:00";
  } else if (type === "week") {
    const diff = now.getDay() === 0 ? 6 : now.getDay() - 1;
    const monday = new Date(now); monday.setDate(now.getDate() - diff);
    start = monday.toISOString().slice(0, 10) + " 00:00:00";
  } else if (type === "month") {
    start = now.getFullYear() + "-" + String(now.getMonth() + 1).padStart(2, "0") + "-01 00:00:00";
  } else if (type === "year") {
    start = now.getFullYear() + "-01-01 00:00:00";
  } else {
    start = "1970-01-01 00:00:00";
  }
  return { start, end };
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
    created_at TIMESTAMP DEFAULT NOW()
  )`);
}
init().then(() => console.log("PostgreSQL tablolar hazır")).catch(e => { console.error(e); process.exit(1); });

/* ---------- AUTH ---------- */
app.post("/api/register", async (req, res) => {
  try {
    const { ad, soyad, yas, cinsiyet, meslek, sehir, email, password, role } = req.body;
    await run(
      `INSERT INTO users (ad, soyad, yas, cinsiyet, meslek, sehir, email, password, role)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
      [ad, soyad, yas, cinsiyet, meslek, sehir, email, password, role]
    );
    const user = await get(`SELECT id, ad, soyad, email, role FROM users WHERE email=$1`, [email]);
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
    const user = await get(`SELECT * FROM users WHERE email=$1 AND password=$2`, [email, password]);
    if (!user) return res.status(401).json({ error: "E-posta veya şifre yanlış." });
    res.json({ success: true, user: { id: user.id, ad: user.ad, soyad: user.soyad, role: user.role, email: user.email } });
  } catch { res.status(500).json({ error: "Sunucu hatası." }); }
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
  } catch { res.status(500).json({ error: "Anket kaydedilemedi!" }); }
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

/* ---------- USER ---------- */
app.get("/api/surveys/:surveyId/questions", async (req, res) => {
  try {
    const rows = await all(`SELECT * FROM questions WHERE survey_id=$1 ORDER BY id ASC`, [req.params.surveyId]);
    res.json({ success: true, questions: rows });
  } catch { res.status(500).json({ error: "Soru listesi hatası!" }); }
});
app.post("/api/answers", async (req, res) => {
  const { user_id, question_id, answer } = req.body;
  try {
    const q = await get(`SELECT correct_answer FROM questions WHERE id=$1`, [question_id]);
    if (!q) return res.status(400).json({ error: "Soru bulunamadı!" });
    const norm = normalizeAnswer(answer);
    const is_correct = q.correct_answer === norm ? 1 : 0;
    await run(
      `INSERT INTO answers (user_id, question_id, answer, is_correct, created_at)
       VALUES ($1,$2,$3,$4,NOW())`,
      [user_id, question_id, norm, is_correct]
    );
    res.json({ success: true, is_correct });
  } catch (e) { res.status(500).json({ error: "Cevap kaydedilemedi! " + e.message }); }
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
      else if (r.is_correct == 1) total += r.point || 1;
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

// index.js içinde uygun yere ekle
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
        COALESCE(SUM(CASE WHEN a.answer = 'bilmem' THEN 0 ELSE q.point END),0)::int AS possible_points
      FROM answers a
      INNER JOIN questions q ON q.id = a.question_id
      INNER JOIN surveys  s ON s.id = q.survey_id
      WHERE a.user_id = $1
      GROUP BY s.id, s.title
      `,
      [userId]
    );

    // Yüzde + sıralama
    const perf = rows.map(r => {
      const pct = r.possible_points > 0 ? Math.round((r.earned_points * 100.0) / r.possible_points) : null;
      return { ...r, score_percent: pct };
    }).sort((A, B) => {
      // 1) yüzdelere göre (null en sona)
      if (A.score_percent == null && B.score_percent != null) return 1;
      if (A.score_percent != null && B.score_percent == null) return -1;
      if (A.score_percent != null && B.score_percent != null && B.score_percent !== A.score_percent)
        return B.score_percent - A.score_percent;
      // 2) attempted (bilmem hariç) – çoktan aza
      if (B.attempted !== A.attempted) return B.attempted - A.attempted;
      // 3) alfabetik
      return (A.title || "").localeCompare(B.title || "", "tr");
    });

    res.json({ success: true, performance: perf });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "Performans listesi alınamadı" });
  }
});



/* ---------- STATS & LEADERBOARDS ---------- */
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
    const { start, end } = getDateRange(period);
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
      LEFT JOIN answers a ON a.user_id = u.id AND (a.created_at BETWEEN $1 AND $2)
      LEFT JOIN questions q ON a.question_id = q.id
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
      [start, end]
    );
    res.json({ success: true, leaderboard: rows });
  } catch { res.status(500).json({ error: "Liste alınamadı" }); }
});
app.get("/api/user/:userId/rank", async (req, res) => {
  try {
    const userId = req.params.userId;
    const period = req.query.period || "all";
    const { start, end } = getDateRange(period);
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
      LEFT JOIN answers a ON a.user_id = u.id AND (a.created_at BETWEEN $1 AND $2)
      LEFT JOIN questions q ON a.question_id = q.id
      GROUP BY u.id
      ORDER BY total_points DESC, u.id ASC
      `,
      [start, end]
    );
    const rank = rows.findIndex(r => String(r.id) === String(userId)) + 1;
    const total_users = rows.length;
    const user_points = rank > 0 ? rows[rank - 1].total_points : 0;
    res.json({ success: true, rank, total_users, user_points });
  } catch { res.status(500).json({ error: "Sıralama alınamadı" }); }
});
app.get("/api/user/approved-surveys", async (_req, res) => {
  try {
    const surveys = await all(`SELECT * FROM surveys WHERE status='approved' ORDER BY id DESC`);
    if (!surveys.length) return res.json({ success: true, surveys: [] });
    const filtered = [];
    for (const survey of surveys) {
      const row = await get(`SELECT COUNT(*)::int AS question_count FROM questions WHERE survey_id=$1`, [survey.id]);
      if ((row?.question_count || 0) > 0) filteredSurveysPush(filtered, survey, row.question_count);
    }
    return res.json({ success: true, surveys: filtered });
  } catch { res.status(500).json({ error: "Listeleme hatası!" }); }
});
function filteredSurveysPush(arr, survey, count) {
  arr.push({ ...survey, question_count: count });
}
app.get("/api/surveys/:surveyId/leaderboard", async (req, res) => {
  try {
    const surveyId = req.params.surveyId;
    const period = req.query.period || "all";
    const { start, end } = getDateRange(period);
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
      LEFT JOIN answers a
        ON a.user_id = u.id
        AND (a.created_at BETWEEN $1 AND $2)
        AND a.question_id IN (SELECT id FROM questions WHERE survey_id = $3)
      LEFT JOIN questions q ON a.question_id = q.id
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
      [start, end, surveyId]
    );
    res.json({ success: true, leaderboard: rows });
  } catch { res.status(500).json({ error: "Anket leaderboard alınamadı!" }); }
});

/* ---------- START ---------- */
const PORT = process.env.PORT || 5000;
app.listen(PORT, "0.0.0.0", () => {
  console.log(`Backend http://0.0.0.0:${PORT} üzerinde çalışıyor`);
});
