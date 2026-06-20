-- Migration 013 (P538, 20 Haz 2026): web_trades.invest_type — SHORT paper trading
-- 1 = LONG (default, geriye uyum) / 2 = SHORT. Legacy `trades` tablosu konvansiyonu
-- (invest_type SMALLINT 1=LONG 2=SHORT) ile birebir. Carr SHORT setuplari (Blue Sea /
-- Gap Down / Rising Wedge) paper-trade edilebilsin diye. SHORT P&L = (entry-exit)*qty.
-- Additive (Kodlama Standardi #2 — sadece ADD COLUMN), idempotent (IF NOT EXISTS).
-- NOT NULL DEFAULT 1: mevcut tum satirlar LONG olarak isaretlenir (dogru — sistem
-- bugune kadar sadece LONG kaydetti).
ALTER TABLE web_trades ADD COLUMN IF NOT EXISTS invest_type SMALLINT NOT NULL DEFAULT 1;
