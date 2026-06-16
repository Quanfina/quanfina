-- Migration 012 (KARAR #477, P482): web_trades.signal_source
-- Trade kokeni: strategy / manual_self / manual_external (UX Bolum 7).
-- "Hangi giris tipi daha karli?" analizi icin. TradeCreate ZORUNLU aliyordu ama
-- DB'de kolon YOKTU -> deger sessizce dusuyordu (denetim bulgu #15, KARAR #477 kirik).
-- Additive (Kodlama Standardi #2 — sadece ADD COLUMN), idempotent (IF NOT EXISTS).
ALTER TABLE web_trades ADD COLUMN IF NOT EXISTS signal_source TEXT;
