-- ============================================================================
-- Migration 006 — KARAR #467 Power Play (High Tight Flag) Mark canon
-- ============================================================================
--
-- KARAR #467 (20 May 2026 ~02:00) — FMP_Matematik.md Konu 20 HAZIR KOD
-- referans (Trade Like a Stock Market Wizard Bolum 10 Mark canon KESIN).
--
-- POLE (direk): 8 hafta max, %100 min yukselis
-- FLAG (bayrak): 2-6 hafta, %10-25 duzeltme
-- Pivot = Flag High
--
-- scanner.py'de compute_power_play_pass(pvh) hesaplar, bu kolona yazar.
-- 12. Ready screen `power_play_ready` filter: WHERE power_play_pass = TRUE
--
-- Idempotent: ADD COLUMN IF NOT EXISTS, FALSE default.
-- ============================================================================

ALTER TABLE minervini_scans
    ADD COLUMN IF NOT EXISTS power_play_pass BOOLEAN DEFAULT FALSE;

-- Dogrulama:
-- SELECT power_play_pass, COUNT(*) FROM minervini_scans
-- WHERE scan_date = (SELECT MAX(scan_date) FROM minervini_scans)
-- GROUP BY power_play_pass;
--
-- Beklenen veri dagilimi: TRUE %1-3 (rare formation), FALSE cogunluk
