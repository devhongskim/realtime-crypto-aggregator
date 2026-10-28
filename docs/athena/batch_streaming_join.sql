-- =====================================================================
-- Batch + streaming convergence in Athena
-- Run each statement separately in the Athena query editor.
-- =====================================================================

-- 1) Register the batch project's Gold output as an Athena table.
--    Layout written by analytics.py:
--      s3://hongskim-stock-pipeline/gold/stocks/date=YYYY-MM-DD/stocks_metrics_YYYY-MM-DD.parquet
--    `date` is a reserved word: backticks in DDL, double quotes in queries.
--    trading_date is intentionally left out (pandas may write it as a
--    timestamp rather than a date); the `date` partition carries the same value.
CREATE EXTERNAL TABLE IF NOT EXISTS stock_gold_metrics (
    ticker STRING,
    daily_return DOUBLE COMMENT 'open-to-close % change',
    intraday_volatility DOUBLE COMMENT '(high - low) / open, %'
)
PARTITIONED BY (`date` STRING)
STORED AS PARQUET
LOCATION 's3://hongskim-stock-pipeline/gold/stocks/'
TBLPROPERTIES (
    'projection.enabled'        = 'true',
    'projection.date.type'      = 'date',
    'projection.date.format'    = 'yyyy-MM-dd',
    'projection.date.range'     = '2026-07-01,NOW',
    'storage.location.template' = 's3://hongskim-stock-pipeline/gold/stocks/date=${date}/'
);

-- Sanity check:
-- SELECT * FROM stock_gold_metrics WHERE "date" BETWEEN '2026-07-01' AND '2026-08-07' LIMIT 20;


-- 2) The join: each stock's open-to-close return vs BTC's return over the
--    same NYSE session (09:30-16:00 New York time), per trading day.
--    AT TIME ZONE handles the EDT->EST switch (Nov 1) automatically.
WITH btc_session AS (
    SELECT
        CAST(date(window_start AT TIME ZONE 'America/New_York') AS VARCHAR) AS trading_date,
        min_by(vwap, window_start) AS session_open_vwap,
        max_by(vwap, window_start) AS session_close_vwap,
        count(*)                   AS windows_captured   -- full session = 2,340
    FROM streaming_gold_trades
    WHERE symbol = 'BTC-USD'
      AND dt BETWEEN '2026-10-28' AND '2026-11-30'          -- partition pruning
      AND date_format(window_start AT TIME ZONE 'America/New_York', '%H:%i')
          BETWEEN '09:30' AND '15:59'
    GROUP BY 1
)
SELECT
    s."date"                                AS trading_date,
    s.ticker,
    s.daily_return                          AS stock_return_pct,
    ROUND((b.session_close_vwap - b.session_open_vwap)
          / b.session_open_vwap * 100, 2)   AS btc_return_pct,
    b.windows_captured,
    ROUND(b.windows_captured / 2340.0 * 100, 1) AS btc_session_coverage_pct
FROM stock_gold_metrics s
JOIN btc_session b
  ON s."date" = b.trading_date
WHERE s."date" BETWEEN '2026-10-28' AND '2026-11-30'        -- partition pruning
ORDER BY trading_date, s.ticker;

-- 3) Demo join that works with the data available now (no date overlap needed):
--    BTC's move and volatility today (streaming Gold) vs each stock's typical
--    day over its Jul 1 - Aug 7 history (batch Gold). Same metric definitions
--    on both sides: return = (close - open) / open, volatility = (high - low) / open.
WITH btc_today AS (
    SELECT
        min(window_start) AS first_window,
        max(window_end)   AS last_window,
        count(*)          AS btc_windows,
        ROUND((max_by(vwap, window_start) - min_by(vwap, window_start))
              / min_by(vwap, window_start) * 100, 2) AS btc_return_pct,
        ROUND((max(high) - min(low))
              / min_by(vwap, window_start) * 100, 2) AS btc_volatility_pct
    FROM streaming_gold_trades
    WHERE dt = '2026-10-28'               -- partition pruning (streaming)
      AND symbol = 'BTC-USD'
),
stock_history AS (
    SELECT
        ticker,
        count(*)                            AS trading_days,
        ROUND(avg(daily_return), 2)         AS avg_return_pct,
        ROUND(avg(intraday_volatility), 2)  AS avg_volatility_pct,
        ROUND(max(intraday_volatility), 2)  AS max_volatility_pct
    FROM stock_gold_metrics
    WHERE "date" BETWEEN '2026-07-01' AND '2026-08-07'   -- partition pruning (batch)
      AND intraday_volatility IS NOT NULL                -- skips the incomplete 7/10 rows
    GROUP BY ticker
)
SELECT
    s.ticker,
    s.trading_days,
    s.avg_return_pct,
    s.avg_volatility_pct,
    s.max_volatility_pct,
    b.btc_return_pct,
    b.btc_volatility_pct,
    ROUND(b.btc_volatility_pct / NULLIF(s.avg_volatility_pct, 0), 2) AS btc_vol_vs_stock_avg,
    b.first_window,
    b.last_window,
    b.btc_windows
FROM stock_history s
CROSS JOIN btc_today b
ORDER BY s.avg_volatility_pct DESC;
