-- Migration 008 — KARAR ADAY #717: Trade Plan Required Fields
-- Mark TTLC Sec 1 birebir: "Always Go in with a Plan"
-- Mark birebir alıntı: "Without a written plan, you have only hope"
-- 24 May 2026 — Sprint 4-bis.7 Uygulama Dönemi Adım 1
--
-- Mark'ın 6 zorunlu alanı (TTLC Sec 1):
--   1. Entry trigger      -> plan_entry_trigger TEXT
--   2. Stop loss          -> plan_stop NUMERIC(12,4)
--   3. Profit target      -> plan_target NUMERIC(12,4)
--   4. Position size      -> plan_size_pct NUMERIC(5,2)
--   5. Exit strategy      -> plan_exit_strategy TEXT
--   6. Time horizon       -> plan_time_horizon VARCHAR(20)
--
-- Geri uyum: Mevcut trade'ler NULL ile kalir, kademe NOT NULL gecisi
-- sonraki migration'da (mevcut backfill sonrasi).
--
-- Idempotent: ADD COLUMN IF NOT EXISTS Postgres 9.6+ destekli.

ALTER TABLE web_trades ADD COLUMN IF NOT EXISTS plan_entry_trigger TEXT;
ALTER TABLE web_trades ADD COLUMN IF NOT EXISTS plan_stop NUMERIC(12,4);
ALTER TABLE web_trades ADD COLUMN IF NOT EXISTS plan_target NUMERIC(12,4);
ALTER TABLE web_trades ADD COLUMN IF NOT EXISTS plan_size_pct NUMERIC(5,2);
ALTER TABLE web_trades ADD COLUMN IF NOT EXISTS plan_exit_strategy TEXT;
ALTER TABLE web_trades ADD COLUMN IF NOT EXISTS plan_time_horizon VARCHAR(20);

-- Geri uyum kontrol — tum mevcut trade'ler icin plan_* alanlari NULL kalmasi
-- normaldir (Mark felsefesi: "yeni trade'ler plan'siz girilemez")
-- API katmani plan_* alanlari yeni trade insert'lerinde zorunlu kilacak
-- (Pydantic Field required), eski trade'ler dokunulmaz.
