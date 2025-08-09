// db.js - PostgreSQL katmanı (sqlite benzeri util'ler)
const { Pool } = require('pg');
const connectionString = process.env.DATABASE_URL;

if (!connectionString) {
  console.error('DATABASE_URL env değişkeni yok! Neon connection string ekleyin.');
  process.exit(1);
}

const pool = new Pool({
  connectionString,
  ssl: { rejectUnauthorized: false }, // Neon/Render için gerekli
});

// sqlite benzeri yardımcılar
async function run(sql, params = []) {
  await pool.query(sql, params);
  return { success: true };
}

async function get(sql, params = []) {
  const { rows } = await pool.query(sql, params);
  return rows[0] || null;
}

async function all(sql, params = []) {
  const { rows } = await pool.query(sql, params);
  return rows;
}

async function init() {
  // Tabloları oluştur
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

module.exports = { pool, run, get, all, init };
