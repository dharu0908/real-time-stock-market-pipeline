-- docker/init.sql
-- Runs automatically when the Postgres container starts for the first time.
-- Creates the 3 tables that the streaming jobs write to.

-- ── SILVER: individual cleaned ticks ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS silver_ticks (
    event_id        TEXT,
    ticker          TEXT         NOT NULL,
    price           NUMERIC(12,4) NOT NULL,
    volume          BIGINT,
    bid             NUMERIC(12,4),
    ask             NUMERIC(12,4),
    spread          NUMERIC(12,4),
    spread_pct      NUMERIC(10,4),
    notional_value  NUMERIC(18,2),
    price_bucket    TEXT,
    event_ts        TIMESTAMPTZ  NOT NULL,
    trade_date      DATE,
    trade_hour      SMALLINT,
    trade_minute    SMALLINT
);

CREATE INDEX IF NOT EXISTS idx_silver_ticker_ts
    ON silver_ticks (ticker, event_ts DESC);

-- ── GOLD: 1-minute OHLCV candles ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold_ohlcv_1min (
    window_start     TIMESTAMPTZ NOT NULL,
    window_end       TIMESTAMPTZ NOT NULL,
    ticker           TEXT        NOT NULL,
    open             NUMERIC(12,4),
    high             NUMERIC(12,4),
    low              NUMERIC(12,4),
    close            NUMERIC(12,4),
    volume           BIGINT,
    tick_count       INTEGER,
    vwap             NUMERIC(12,4),
    avg_spread_pct   NUMERIC(10,4),
    total_notional   NUMERIC(18,2),
    price_range      NUMERIC(12,4),
    price_range_pct  NUMERIC(10,4),
    candle_direction TEXT
);

CREATE INDEX IF NOT EXISTS idx_ohlcv_ticker_window
    ON gold_ohlcv_1min (ticker, window_start DESC);

-- ── GOLD: top movers ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold_top_movers (
    window_start     TIMESTAMPTZ,
    window_end       TIMESTAMPTZ,
    ticker           TEXT,
    open             NUMERIC(12,4),
    high             NUMERIC(12,4),
    low              NUMERIC(12,4),
    close            NUMERIC(12,4),
    volume           BIGINT,
    pct_change       NUMERIC(10,4),
    abs_pct_change   NUMERIC(10,4),
    candle_direction TEXT
);

CREATE INDEX IF NOT EXISTS idx_movers_window
    ON gold_top_movers (window_start DESC, abs_pct_change DESC);
