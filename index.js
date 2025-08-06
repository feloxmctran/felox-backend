const express = require("express");
const cors = require("cors");
const sqlite3 = require("sqlite3").verbose();
const bodyParser = require("body-parser");
const path = require('path');

const app = express();
app.use(cors());
app.use(express.json());
app.use(bodyParser.urlencoded({ extended: true }));

console.log("Kullanılan veritabanı yolu:", path.resolve("./felox.db"));

const db = new sqlite3.Database("./felox.db", (err) => {
  if (err) return console.error("DB bağlantı hatası:", err.message);
  console.log("SQLite DB bağlantısı başarılı");
});

db.serialize(() => {
  db.run(`CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
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
  db.run(`CREATE TABLE IF NOT EXISTS surveys (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    editor_id INTEGER,
    title TEXT,
    start_date TEXT,
    end_date TEXT,
    category TEXT,
    status TEXT DEFAULT 'pending'
  )`);
  db.run(`CREATE TABLE IF NOT EXISTS questions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    survey_id INTEGER,
    question TEXT,
    correct_answer TEXT,
    point INTEGER DEFAULT 1
  )`);
  db.run(`CREATE TABLE IF NOT EXISTS answers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    question_id INTEGER,
    answer TEXT,
    is_correct INTEGER,
    created_at TEXT DEFAULT (datetime('now', 'localtime'))
  )`);
});

// Yardımcı: Tarih aralığı getir
function getDateRange(type) {
  const now = new Date();
  let start, end;
  end = now.toISOString().slice(0, 10) + " 23:59:59";
  if (type === "today") {
    start = now.toISOString().slice(0, 10) + " 00:00:00";
  } else if (type === "week") {
    const diff = now.getDay() === 0 ? 6 : now.getDay() - 1;
    const monday = new Date(now);
    monday.setDate(now.getDate() - diff);
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

// ---- KAYIT (register): Kullanıcıyı hemen döndür! ----
// Kayıt
app.post("/api/register", (req, res) => {
  const { ad, soyad, yas, cinsiyet, meslek, sehir, email, password, role } = req.body;
  db.run(
    `INSERT INTO users (ad, soyad, yas, cinsiyet, meslek, sehir, email, password, role)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [ad, soyad, yas, cinsiyet, meslek, sehir, email, password, role],
    function (err) {
      if (err) {
        if (err.message.includes("UNIQUE")) {
          return res.status(400).json({ error: "Bu e-posta zaten kayıtlı." });
        }
        return res.status(500).json({ error: "Kayıt başarısız." });
      }
      // Yeni kullanıcıyı dön!
      db.get(
        `SELECT id, ad, soyad, email, role FROM users WHERE id = ?`,
        [this.lastID],
        (err2, user) => {
          if (err2 || !user) {
            return res.json({ success: true });
          }
          res.json({ success: true, user });
        }
      );
    }
  );
});


// Giriş
app.post("/api/login", (req, res) => {
  const { email, password } = req.body;
  db.get(
    `SELECT * FROM users WHERE email = ? AND password = ?`,
    [email, password],
    (err, user) => {
      if (err) return res.status(500).json({ error: "Sunucu hatası." });
      if (!user) return res.status(401).json({ error: "E-posta veya şifre yanlış." });
      res.json({
        success: true,
        user: {
          id: user.id,
          ad: user.ad,
          soyad: user.soyad,
          role: user.role,
          email: user.email,
        },
      });
    }
  );
});

// --------------- EDITOR ENDPOINTLERİ ---------------
app.post("/api/surveys", (req, res) => {
  const { editor_id, title, start_date, end_date, category, questions } = req.body;
  db.run(
    `INSERT INTO surveys (editor_id, title, start_date, end_date, category, status)
    VALUES (?, ?, ?, ?, ?, 'pending')`,
    [editor_id, title, start_date, end_date, category],
    function (err) {
      if (err) return res.status(500).json({ error: "Anket kaydedilemedi!" });
      const surveyId = this.lastID;
      const stmt = db.prepare(
        `INSERT INTO questions (survey_id, question, correct_answer, point) VALUES (?, ?, ?, ?)`
      );
      questions.forEach((q) => {
        let p = 1;
        if (typeof q.point === "number" && q.point >= 1 && q.point <= 10) p = q.point;
        stmt.run([surveyId, q.question, q.correct_answer, p]);
      });
      stmt.finalize();
      res.json({ success: true });
    }
  );
});

app.get("/api/editor/:editorId/surveys", (req, res) => {
  const editorId = req.params.editorId;
  db.all(
    `SELECT * FROM surveys WHERE editor_id = ? AND status != 'deleted' ORDER BY id DESC`,
    [editorId],
    (err, rows) => {
      if (err) return res.status(500).json({ error: "Listeleme hatası!" });
      res.json({ success: true, surveys: rows });
    }
  );
});

app.get("/api/surveys/:surveyId/details", (req, res) => {
  const surveyId = req.params.surveyId;
  db.get(`SELECT * FROM surveys WHERE id = ?`, [surveyId], (err, survey) => {
    if (err || !survey) return res.status(404).json({ error: "Anket bulunamadı" });
    db.all(`SELECT * FROM questions WHERE survey_id = ?`, [surveyId], (err2, questions) => {
      if (err2) return res.status(500).json({ error: "Sorular bulunamadı!" });
      res.json({ success: true, survey, questions });
    });
  });
});

app.post("/api/surveys/:surveyId/delete", (req, res) => {
  const surveyId = req.params.surveyId;
  db.run(
    `UPDATE surveys SET status = 'deleted' WHERE id = ?`,
    [surveyId],
    function (err) {
      if (err) return res.status(500).json({ error: "Silinemedi." });
      res.json({ success: true });
    }
  );
});

// --------------- ADMIN ENDPOINTLERİ ---------------
app.get("/api/admin/surveys", (req, res) => {
  db.all(
    `SELECT surveys.*, users.ad as editor_ad, users.soyad as editor_soyad
     FROM surveys
     LEFT JOIN users ON surveys.editor_id = users.id
     WHERE surveys.status != 'deleted'
     ORDER BY surveys.id DESC`,
    [],
    (err, rows) => {
      if (err) return res.status(500).json({ error: "Listeleme hatası!" });
      res.json({ success: true, surveys: rows });
    }
  );
});

app.post("/api/surveys/:surveyId/status", (req, res) => {
  const surveyId = req.params.surveyId;
  const { status } = req.body;
  if (!["approved", "rejected"].includes(status))
    return res.status(400).json({ error: "Geçersiz durum!" });
  db.run(
    `UPDATE surveys SET status = ? WHERE id = ?`,
    [status, surveyId],
    function (err) {
      if (err) return res.status(500).json({ error: "Durum güncellenemedi." });
      res.json({ success: true });
    }
  );
});

app.post("/api/questions/:questionId/delete", (req, res) => {
  const questionId = req.params.questionId;
  db.run(
    `DELETE FROM questions WHERE id = ?`,
    [questionId],
    function (err) {
      if (err) return res.status(500).json({ error: "Soru silinemedi." });
      res.json({ success: true });
    }
  );
});

// --------------- USER ENDPOINTLERİ ---------------

// Bir anketin sorularını getir (puanlı!)
app.get("/api/surveys/:surveyId/questions", (req, res) => {
  const surveyId = req.params.surveyId;
  db.all(
    `SELECT * FROM questions WHERE survey_id = ?`,
    [surveyId],
    (err, rows) => {
      if (err) return res.status(500).json({ error: "Soru listesi hatası!" });
      res.json({ success: true, questions: rows });
    }
  );
});

// Kullanıcı cevaplarını kaydet (created_at!)
app.post("/api/answers", (req, res) => {
  const { user_id, question_id, answer } = req.body;
  db.get(`SELECT correct_answer FROM questions WHERE id = ?`, [question_id], (err, q) => {
    if (err || !q) return res.status(400).json({ error: "Soru bulunamadı!" });
    const is_correct = q.correct_answer === answer ? 1 : 0;
    db.run(
      `INSERT INTO answers (user_id, question_id, answer, is_correct, created_at) VALUES (?, ?, ?, ?, datetime('now', 'localtime'))`,
      [user_id, question_id, answer, is_correct],
      function (err2) {
        if (err2) return res.status(500).json({ error: "Cevap kaydedilemedi! " + err2.message });
        res.json({ success: true, is_correct });
      }
    );
  });
});

// Kullanıcının sadece doğru cevapladığı soruları _frontend için kolayca filtreleyebilmen için yeni endpoint!_
app.get("/api/user/:userId/answers", (req, res) => {
  const userId = req.params.userId;
  db.all(
    `SELECT question_id, is_correct, answer FROM answers WHERE user_id = ?`,
    [userId],
    (err, rows) => {
      if (err) return res.status(500).json({ error: "Listeleme hatası!" });
      res.json({ success: true, answers: rows });
    }
  );
});

app.get("/api/user/:userId/answered", (req, res) => {
  const userId = req.params.userId;
  db.all(
    `SELECT question_id FROM answers WHERE user_id = ?`,
    [userId],
    (err, rows) => {
      if (err) return res.status(500).json({ error: "Listeleme hatası!" });
      res.json({ success: true, answered: rows.map(r => r.question_id) });
    }
  );
});

app.get("/api/user/:userId/total-points", (req, res) => {
  const userId = req.params.userId;
  db.all(
    `
    SELECT a.answer, a.is_correct, q.point
    FROM answers a
    INNER JOIN questions q ON a.question_id = q.id
    WHERE a.user_id = ?
    `,
    [userId],
    (err, rows) => {
      if (err) return res.status(500).json({ error: "Puan alınamadı" });
      let total = 0;
      rows.forEach(r => {
        if (r.answer === "bilmem") {
          // Etki yok
        } else if (r.is_correct == 1) {
          total += r.point || 1;
        } else {
          total -= r.point || 1;
        }
      });
      res.json({ success: true, totalPoints: total, answeredCount: rows.length });
    }
  );
});

app.get("/api/user/:userId/score", (req, res) => {
  const userId = req.params.userId;
  db.get(
    `SELECT
      COUNT(*) AS total,
      SUM(is_correct) AS correct,
      COUNT(*) - SUM(is_correct) AS wrong
    FROM answers WHERE user_id = ?`,
    [userId],
    (err, row) => {
      if (err) return res.status(500).json({ error: "Skor hatası!" });
      res.json({ success: true, ...row });
    }
  );
});

app.get("/api/surveys/:surveyId/answers-report", (req, res) => {
  const surveyId = req.params.surveyId;
  db.all(
    `SELECT id, question FROM questions WHERE survey_id = ?`,
    [surveyId],
    (err, questions) => {
      if (err) return res.status(500).json({ error: "Soru listesi alınamadı" });
      db.all(
        `SELECT 
            a.user_id, u.ad, u.soyad, u.yas, u.cinsiyet, u.sehir,
            a.question_id, q.question,
            a.answer
          FROM answers a
          INNER JOIN users u ON a.user_id = u.id
          INNER JOIN questions q ON a.question_id = q.id
          WHERE q.survey_id = ?
          ORDER BY a.user_id, a.question_id
        `,
        [surveyId],
        (err2, answers) => {
          if (err2) return res.status(500).json({ error: "Cevaplar alınamadı" });

          const participantsMap = {};
          for (const ans of answers) {
            if (!participantsMap[ans.user_id]) {
              participantsMap[ans.user_id] = {
                user_id: ans.user_id,
                ad: ans.ad,
                soyad: ans.soyad,
                yas: ans.yas,
                cinsiyet: ans.cinsiyet,
                sehir: ans.sehir,
                answers: {},
              };
            }
            participantsMap[ans.user_id].answers[ans.question_id] = ans.answer;
          }

          res.json({
            success: true,
            questions: questions,
            participants: Object.values(participantsMap),
            total_participants: Object.keys(participantsMap).length
          });
        }
      );
    }
  );
});

// ---- ADMIN İSTATİSTİKLERİ ----
app.get("/api/admin/statistics", (req, res) => {
  let stats = {};
  db.get(`SELECT COUNT(*) as count FROM users`, (err, row) => {
    stats.total_users = row.count;

    db.get(`SELECT COUNT(DISTINCT user_id) as count FROM answers`, (err2, row2) => {
      stats.total_active_users = row2.count;

      db.get(`SELECT COUNT(*) as count FROM surveys WHERE status = 'approved'`, (err3, row3) => {
        stats.total_approved_surveys = row3.count;

        db.get(`SELECT COUNT(*) as count FROM questions`, (err4, row4) => {
          stats.total_questions = row4.count;

          db.get(`SELECT COUNT(*) as count FROM answers`, (err5, row5) => {
            stats.total_answers = row5.count;

            db.get(`SELECT COUNT(*) as count FROM answers WHERE is_correct = 1`, (err6, row6) => {
              stats.total_correct_answers = row6.count;

              db.get(`SELECT COUNT(*) as count FROM answers WHERE is_correct = 0 AND answer != 'bilmem'`, (err7, row7) => {
                stats.total_wrong_answers = row7.count;

                db.get(`SELECT COUNT(*) as count FROM answers WHERE answer = 'bilmem'`, (err8, row8) => {
                  stats.total_bilmem = row8.count;
                  res.json({ success: true, ...stats });
                });
              });
            });
          });
        });
      });
    });
  });
});

// ------ LEADERBOARD: FİLTRELİ ------
app.get("/api/leaderboard", (req, res) => {
  const period = req.query.period || "all";
  const { start, end } = getDateRange(period);
  db.all(
    `
    SELECT u.id, u.ad, u.soyad,
      COALESCE(SUM(
        CASE
          WHEN a.answer = 'bilmem' THEN 0
          WHEN a.is_correct = 1 THEN q.point
          WHEN a.is_correct = 0 THEN -q.point
          ELSE 0
        END
      ), 0) AS total_points
    FROM users u
    LEFT JOIN answers a ON a.user_id = u.id AND (a.created_at BETWEEN ? AND ?)
    LEFT JOIN questions q ON a.question_id = q.id
    GROUP BY u.id
    ORDER BY total_points DESC, u.id ASC
    LIMIT 100
    `,
    [start, end],
    (err, rows) => {
      if (err) return res.status(500).json({ error: "Liste alınamadı" });
      res.json({ success: true, leaderboard: rows });
    }
  );
});

// ------ Kullanıcının sıralamadaki yeri: FİLTRELİ ------
app.get("/api/user/:userId/rank", (req, res) => {
  const userId = req.params.userId;
  const period = req.query.period || "all";
  const { start, end } = getDateRange(period);
  db.all(
    `
    SELECT u.id,
      COALESCE(SUM(
        CASE
          WHEN a.answer = 'bilmem' THEN 0
          WHEN a.is_correct = 1 THEN q.point
          WHEN a.is_correct = 0 THEN -q.point
          ELSE 0
        END
      ), 0) AS total_points
    FROM users u
    LEFT JOIN answers a ON a.user_id = u.id AND (a.created_at BETWEEN ? AND ?)
    LEFT JOIN questions q ON a.question_id = q.id
    GROUP BY u.id
    ORDER BY total_points DESC, u.id ASC
    `,
    [start, end],
    (err, rows) => {
      if (err) return res.status(500).json({ error: "Sıralama alınamadı" });
      const rank = rows.findIndex(r => r.id == userId) + 1;
      const total_users = rows.length;
      let user_points = 0;
      if (rank > 0) user_points = rows[rank - 1].total_points;
      res.json({ success: true, rank, total_users, user_points });
    }
  );
});

// Onaylanmış (approved) anketleri getir VE HER ANKETTE SORU SAYISI OLSUN
app.get("/api/user/approved-surveys", (req, res) => {
  db.all(
    `SELECT * FROM surveys WHERE status = 'approved' ORDER BY id DESC`,
    [],
    (err, surveys) => {
      if (err) return res.status(500).json({ error: "Listeleme hatası!" });
      if (surveys.length === 0) return res.json({ success: true, surveys: [] });

      let done = 0;
      surveys.forEach((survey, idx) => {
        db.get(
          `SELECT COUNT(*) as question_count FROM questions WHERE survey_id = ?`,
          [survey.id],
          (err2, row) => {
            survey.question_count = row ? row.question_count : 0;
            done++;
            if (done === surveys.length) {
              res.json({ success: true, surveys });
            }
          }
        );
      });
    }
  );
});

const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
  console.log(`Backend http://localhost:${PORT} üzerinde çalışıyor`);
});
