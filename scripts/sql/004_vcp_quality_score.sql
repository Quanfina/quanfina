-- ============================================================================
-- Migration 004 — KARAR #466 VCP Kalite Skoru kolonu
-- ============================================================================
--
-- KARAR #466 (20 May 2026 ~00:00) — 3 KANAL SENTEZI:
--   Master "0.70 muhafazakar yansima + 0.50 ideal" +
--   Minervini Uzmani "0.70 guvenli filtre + 0.50 ideal gun" +
--   Bonus FMP "0.40-0.50 altin standart"
--
-- Sentez: compute_vcp_quality(pvh) -> "EXCELLENT" | "PASS" | NULL
--   - EXCELLENT: VOL_DRY 0.50 (Mark canon "%50 alti en siki")
--   - PASS: VOL_DRY 0.70 (Brandon muhafazakar filtre)
--   - NULL: yetersiz veri veya sartlar saglanmadi
--
-- Idempotent: ADD COLUMN IF NOT EXISTS.
-- Backward compat: NULL default — eski kayitlar etkilenmez.
-- ============================================================================

ALTER TABLE minervini_scans
    ADD COLUMN IF NOT EXISTS vcp_quality_score TEXT DEFAULT NULL;

-- Index gereksiz: WHERE vcp_quality_score = 'EXCELLENT' filtresi tipik
-- scan_date filtresinin tabi - PostgreSQL planner uygun bulursa otomatik
-- BitmapScan kullanir.

-- Dogrulama sorgusu (run_migration.py manuel kontrol):
-- SELECT column_name, data_type, column_default, is_nullable
-- FROM information_schema.columns
-- WHERE table_name = 'minervini_scans' AND column_name = 'vcp_quality_score';

-- Beklenen veri dagilimi (gunluk tarama sonrasi):
-- SELECT vcp_quality_score, COUNT(*) FROM minervini_scans
-- WHERE scan_date = (SELECT MAX(scan_date) FROM minervini_scans)
-- GROUP BY vcp_quality_score
-- ORDER BY vcp_quality_score NULLS LAST;
--   EXCELLENT  |  ~5-20 (en sik formasyonlar)
--   PASS       |  ~30-100
--   NULL       |  cogunluk (sartlari saglamayan)
