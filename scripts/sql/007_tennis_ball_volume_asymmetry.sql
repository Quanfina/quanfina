-- ============================================================================
-- Migration 007 — KARAR ADAY #893 (Tennis Ball) + #882 (Volume Asymmetry)
-- ============================================================================
--
-- Sprint 4-bis.7 Faz 2 genisletme canli veri entegrasyonu.
-- Vizyon v22.03 + v22.04 tescili.
--
-- Mark KESIN paten'ler PVH (price-volume history) gerektirir, ZATEN MEVCUT.
-- yfinance quarterly EPS gerek YOK (Faz 2.1 ayri paket).
--
-- KARAR ADAY #893 — Tennis Ball Detector (TLSMW s.253)
--   tennis_ball_pattern: TEXT
--     - 'TENNIS_BALL': Saglikli pullback + recovery (Mark KESIN ideal)
--     - 'EGG': Pullback > 14g, recovery yok (SAT warning)
--     - 'STILL_RUNNING': Henuz pullback yok
--     - 'INVALID': Veri yetersiz veya pre-breakout
--
-- KARAR ADAY #882 — Volume Asymmetry Tracker (TLSMW s.234)
--   volume_asymmetry_ratio: NUMERIC(8,3)
--     Up volume / Down volume oran (lookback 20 gun)
--   volume_asymmetry_tier: TEXT
--     - 'healthy': ratio >= 1.5 (institutional accumulation)
--     - 'neutral': 0.8-1.5
--     - 'distribution': < 0.8 (institutional selling warning)
--     - 'invalid': yetersiz veri
--
-- scanner.py'de detect_tennis_ball + compute_volume_asymmetry her ticker icin
-- hesaplar, bu kolonlara yazar.
--
-- Yeni Ready screens:
--   - 'tennis_ball_active': WHERE tennis_ball_pattern = 'TENNIS_BALL'
--   - 'healthy_accumulation': WHERE volume_asymmetry_tier = 'healthy'
--
-- Idempotent: ADD COLUMN IF NOT EXISTS, NULL default.
-- ============================================================================

ALTER TABLE minervini_scans
    ADD COLUMN IF NOT EXISTS tennis_ball_pattern TEXT DEFAULT NULL;

ALTER TABLE minervini_scans
    ADD COLUMN IF NOT EXISTS volume_asymmetry_ratio NUMERIC(8,3) DEFAULT NULL;

ALTER TABLE minervini_scans
    ADD COLUMN IF NOT EXISTS volume_asymmetry_tier TEXT DEFAULT NULL;

-- Dogrulama:
-- SELECT tennis_ball_pattern, COUNT(*) FROM minervini_scans
-- WHERE scan_date = (SELECT MAX(scan_date) FROM minervini_scans)
-- GROUP BY tennis_ball_pattern;
--
-- SELECT volume_asymmetry_tier, COUNT(*) FROM minervini_scans
-- WHERE scan_date = (SELECT MAX(scan_date) FROM minervini_scans)
-- GROUP BY volume_asymmetry_tier;
--
-- Beklenen daglim:
-- tennis_ball_pattern:
--   TENNIS_BALL: %5-10 (breakout sonrasi healthy bounce yakaladigi anlamli sayida)
--   STILL_RUNNING: cogunluk
--   EGG: %2-5
--
-- volume_asymmetry_tier:
--   healthy: %15-25 (institutional accumulation iyi market regime)
--   neutral: cogunluk
--   distribution: %5-10 (warning signals)
