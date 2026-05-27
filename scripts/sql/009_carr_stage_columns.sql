-- Migration 009 — KARAR #733: Carr Stage Analizi (Stan Weinstein 4-Stage)
-- compute_carr_stage scanner.py entegrasyonu sonucu kolonlari
--
-- Kolon tanimlari (compute_carr_stage dict cikti birebir):
--   carr_stage              SMALLINT       -- 1|2|3|4|NULL (yetersiz veri)
--   carr_stage_label        TEXT           -- 'Stage 2 (Advancing)' vb.
--   carr_slope_pct_per_year NUMERIC(8,2)   -- 30W MA yillik slope %
--   carr_ma_value           NUMERIC(12,4)  -- 30W SMA degeri
--   carr_price_vs_ma_pct    NUMERIC(8,2)   -- price - MA fark %
--
-- Idempotent: ADD COLUMN IF NOT EXISTS Postgres 9.6+ destekli.
-- Geri uyum: mevcut satirlar carr_stage NULL kalir, scanner sonraki run'da doldurur.
--
-- Kaynak: quanfina_math.py compute_carr_stage + notebook/kitaplar/Carr.md

ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS carr_stage SMALLINT;
ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS carr_stage_label TEXT;
ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS carr_slope_pct_per_year NUMERIC(8,2);
ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS carr_ma_value NUMERIC(12,4);
ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS carr_price_vs_ma_pct NUMERIC(8,2);
