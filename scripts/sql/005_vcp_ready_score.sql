-- ============================================================================
-- Migration 005 — KARAR #465 VCP Ready Score kolonu
-- ============================================================================
--
-- KARAR #465 (20 May 2026 ~01:00) — Minervini Uzmani onerisi:
--   Inside Day (Mark canon "arzin tukenmesinin kesin kaniti") +
--   Outside Day Negative Reversal (TTLC Bolum 1 "Violations") +
--   VCP Ready Score 0-100 (50 Inside + 30 V-Dry + 20 Tight)
--
-- vcp_ready_score INTEGER 0-100:
--   - 70+: "Ready" filtre (11. Ready screen `vcp_ready_high`)
--   - 50-69: "Yaklaşan" — yakin takip
--   - 0-49: "Hazır değil"
--   - NULL: yetersiz veri (eski close-only PVH veya hata)
--
-- Idempotent: ADD COLUMN IF NOT EXISTS, NULL default.
-- ============================================================================

ALTER TABLE minervini_scans
    ADD COLUMN IF NOT EXISTS vcp_ready_score INTEGER DEFAULT NULL;

-- Index gereksiz: tipik sorgu "WHERE vcp_ready_score >= 70" scan_date'e tabi.

-- Dogrulama:
-- SELECT vcp_ready_score, COUNT(*) FROM minervini_scans
-- WHERE scan_date = (SELECT MAX(scan_date) FROM minervini_scans)
-- GROUP BY
--   CASE
--     WHEN vcp_ready_score IS NULL THEN 'NULL'
--     WHEN vcp_ready_score >= 70 THEN 'READY (70+)'
--     WHEN vcp_ready_score >= 50 THEN 'YAKLAŞAN (50-69)'
--     ELSE 'DUSUK (0-49)'
--   END
-- ORDER BY 1;
