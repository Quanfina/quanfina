-- Quanfina POC sonrası ilk PostgreSQL migration
-- 17 May 2026 — Faz 1: web_watchlist + web_trades
-- AÇIK KONU #59 (Notebook A v20.37)

-- ============================================================
-- web_watchlist — POC ADIM 8 watchlist CRUD persistence
-- ============================================================
CREATE TABLE IF NOT EXISTS web_watchlist (
    symbol                VARCHAR(10)    NOT NULL,
    strategy              VARCHAR(20)    NOT NULL,
    status                VARCHAR(20)    NOT NULL,
    price                 NUMERIC(12,4),
    added_date            DATE           NOT NULL,
    setup_type            VARCHAR(50),
    pivot_price           NUMERIC(12,4),
    note                  TEXT,
    rs_rating             SMALLINT,
    consensus_count       SMALLINT       NOT NULL DEFAULT 1,
    consensus_strategies  JSONB          NOT NULL DEFAULT '[]'::jsonb,
    created_at            TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    PRIMARY KEY (symbol, strategy)
);

CREATE INDEX IF NOT EXISTS idx_web_watchlist_symbol ON web_watchlist(symbol);
CREATE INDEX IF NOT EXISTS idx_web_watchlist_status ON web_watchlist(status);

-- ============================================================
-- web_trades — POC ADIM 9 trade journal persistence
-- ============================================================
CREATE TABLE IF NOT EXISTS web_trades (
    id                    SERIAL         PRIMARY KEY,
    symbol                VARCHAR(10)    NOT NULL,
    strategy              VARCHAR(20)    NOT NULL,
    setup_type            VARCHAR(50)    NOT NULL,
    entry_date            DATE           NOT NULL,
    entry_price           NUMERIC(12,4)  NOT NULL,
    exit_date             DATE,
    exit_price            NUMERIC(12,4),
    shares                INTEGER        NOT NULL,
    status                VARCHAR(10)    NOT NULL,
    pl_dollar             NUMERIC(14,4),
    pl_pct                NUMERIC(8,4),
    grade                 VARCHAR(2),
    exit_reason           VARCHAR(50),
    lessons               TEXT,
    created_at            TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_web_trades_symbol ON web_trades(symbol);
CREATE INDEX IF NOT EXISTS idx_web_trades_status ON web_trades(status);
CREATE INDEX IF NOT EXISTS idx_web_trades_entry_date ON web_trades(entry_date);

-- ============================================================
-- updated_at otomatik güncelleme trigger'ı (her iki tablo için)
-- ============================================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_web_watchlist_updated_at ON web_watchlist;
CREATE TRIGGER trg_web_watchlist_updated_at
    BEFORE UPDATE ON web_watchlist
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS trg_web_trades_updated_at ON web_trades;
CREATE TRIGGER trg_web_trades_updated_at
    BEFORE UPDATE ON web_trades
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
