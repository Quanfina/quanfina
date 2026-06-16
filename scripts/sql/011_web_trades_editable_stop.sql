-- Migration 011 (16 Haz 2026) — web_trades editable stop/target + audible_reason
-- KARAR ADAY #960 (Audible Lock) + Acik Pozisyon Yonetimi ailesi kok altyapisi.
--
-- Neden: web_trades'te plan_stop/plan_target VAR (orijinal plan, degismez) ama
-- pozisyon acildiktan SONRA AKTIF stop/hedef DUZENLENEMIYOR. Mark "audible" disiplini:
-- stop/hedef ayarlanabilir ama SEBEP loglanir (sebepsiz degisiklik = disiplinsizlik).
--
-- Kodlama Standardi #2: sadece ADD COLUMN (DROP yok). Additive + geri-uyumlu + idempotent.
ALTER TABLE web_trades ADD COLUMN IF NOT EXISTS stop_loss NUMERIC;
ALTER TABLE web_trades ADD COLUMN IF NOT EXISTS target_price NUMERIC;
ALTER TABLE web_trades ADD COLUMN IF NOT EXISTS audible_reason TEXT;

-- Mevcut pozisyonlar: aktif stop/hedef baslangici = plan (henuz audible yapilmadi).
UPDATE web_trades SET stop_loss = plan_stop
    WHERE stop_loss IS NULL AND plan_stop IS NOT NULL;
UPDATE web_trades SET target_price = plan_target
    WHERE target_price IS NULL AND plan_target IS NOT NULL;
