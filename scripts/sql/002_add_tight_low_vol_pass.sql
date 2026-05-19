-- ============================================================================
-- Migration 002 — Sprint 4-bis.4 Tight Price Low Vol (VCP) Pre-Compute kolonu
-- ============================================================================
--
-- KARAR #461 (Master NotebookLM Sprint 4-bis.4 cevabi, 19 May 2026 ~21:00):
--   Tight Price Low Vol ekrani Python (scanner.py) katmaninda hesaplanir,
--   sonuc bu BOOLEAN kolonuna yazilir.
--
-- SQL ekran sorgusu: SELECT ... FROM minervini_scans WHERE tight_low_vol_pass = TRUE
--
-- Bagimlilik: minervini_scans.price_volume_history JSONB kolonu zaten var
--             (Sprint 4.7e.3 commit c1c7aa6 ile).
--
-- Idempotent: IF NOT EXISTS kullanildi - tekrar calistirilirsa hata vermez.
-- ============================================================================

ALTER TABLE minervini_scans
    ADD COLUMN IF NOT EXISTS tight_low_vol_pass BOOLEAN DEFAULT FALSE;

-- Index gereksiz: WHERE tight_low_vol_pass = TRUE filtresi typical scan_date
-- filtresinin tabi - PostgreSQL planner bunu uygun goruyorsa otomatik
-- BitmapScan + tarih index combinasyonu kullanir.

-- Dogrulama sorgusu (run_migration.py manuel kontrol icin):
-- SELECT column_name, data_type, column_default, is_nullable
-- FROM information_schema.columns
-- WHERE table_name = 'minervini_scans' AND column_name = 'tight_low_vol_pass';
