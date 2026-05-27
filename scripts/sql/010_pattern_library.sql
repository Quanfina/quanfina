-- Migration 010 — KARAR ADAY #714: Pattern Library DB iskelet
-- Mark canon 7 pattern (TLSMW Ch 10) + O'Neil Cup-with-Handle
-- 28 May 2026 — Sprint 4-bis.7 (e) maddesi iskelet
--
-- Kaynak: notebook/Sprint_4_bis_7_Mark_4_Kitap_Tam_Sentez.md sat. 193-215
-- + Mark "Trade Like a Stock Market Wizard" Ch 10 (s.195-258)
-- + William O'Neil "How to Make Money in Stocks" Cup-with-Handle
--
-- Tablo amacı:
--   compute_vcp_pass + detect_tennis_ball + compute_power_play_pass detector'ları
--   pattern_library tablosundan canon parametre okur (contraction_count_min/max,
--   base_weeks_min/max). Hardcoded sayı YOK (KALICI İLKE #4 Matematik Uydurmama
--   + Kural #26 anayasa uyumu — Mark kitap birebir kaynak).
--
-- Idempotent disiplin (KARAR #461):
--   CREATE TABLE IF NOT EXISTS + INSERT ON CONFLICT DO NOTHING

CREATE TABLE IF NOT EXISTS pattern_library (
    id                    SERIAL PRIMARY KEY,
    pattern_name          TEXT NOT NULL UNIQUE,
    mark_book_ref         TEXT,            -- "TLSMW Ch 10 s.195"
    contraction_count_min INT,
    contraction_count_max INT,
    base_weeks_min        INT,
    base_weeks_max        INT,
    notes                 TEXT,
    created_at            TIMESTAMP DEFAULT NOW()
);

-- 7 Mark + O'Neil canon pattern (idempotent INSERT)
INSERT INTO pattern_library
    (pattern_name, mark_book_ref, contraction_count_min, contraction_count_max,
     base_weeks_min, base_weeks_max, notes)
VALUES
    ('Standard VCP',         'TLSMW Ch 10 s.195',     2, 4, 7,  65, 'Mark canon — 2-4 contraction, daralan ranje'),
    ('Cup-with-Handle',      'TLSMW Ch 10 s.~220',    2, 3, 7,  65, 'O''Neil Dream Pattern — kupanin sapi'),
    ('Cup Completion Cheat', 'TLSMW Ch 10 + Mark X',  2, 3, 7,  65, '3-C early entry (Cup Completion)'),
    ('Low Cheat',            'TLSMW Ch 10',           2, 3, 7,  65, 'Handle lower 1/3 (Mark Low Cheat)'),
    ('Power Play (HTF)',     'TLSMW Ch 10',           2, 3, 3,  6,  'POLE 100% + FLAG 10-25% (High Tight Flag)'),
    ('Double Bottom',        'TLSMW Ch 10',           2, 2, 4,  12, 'W shape — iki dip ayni seviyede'),
    ('Square Box',           'TLSMW Ch 10',           1, 1, 4,  7,  '10-15% range — kisa konsolidasyon')
ON CONFLICT (pattern_name) DO NOTHING;
