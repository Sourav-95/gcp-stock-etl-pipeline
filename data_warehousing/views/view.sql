-- 1. Fundamental View — margins, growth, returns joined with sector/industry
CREATE OR REPLACE VIEW `your_project.your_dataset.fundamental_view` AS
SELECT
  q.ticker_id,
  q.ingestion_date,
  d.sector,
  d.industry,
  q.revenueGrowth,
  q.revenue_growth_category,
  q.profitMargins,
  q.grossMargins,
  q.operatingMargins,
  q.returnOnEquity,
  q.roe_category,
  q.returnOnAssets,
  q.market_cap_cr,
  q.revenue_cr,
  q.net_cash_cr,
  q.dividendYield,
  q.payoutRatio
FROM `your_project.your_dataset.quantitative_table` q
LEFT JOIN `your_project.your_dataset.dimension_table` d ON q.ticker_id = d.ticker_id;


-- 2. Technical View — price action, moving averages, 52-week metrics
CREATE OR REPLACE VIEW `your_project.your_dataset.technical_view` AS
SELECT
  ticker_id,
  ingestion_date,
  currentPrice,
  previousClose,
  regularMarketChangePercent,
  beta,
  trailingPE,
  forwardPE,
  pe_category,
  fiftyDayAverage,
  twoHundredDayAverage,
  above_200dma,
  week52_position_score,
  discount_from_52w_high_pct,
  week52_change,
  sandp_52week_change
FROM `your_project.your_dataset.quantitative_table`;


-- 3. Analyst View — target prices, recommendations, upside
CREATE OR REPLACE VIEW `your_project.your_dataset.analyst_view` AS
SELECT
  q.ticker_id,
  q.ingestion_date,
  d.sector,
  q.currentPrice,
  q.targetMeanPrice,
  q.targetHighPrice,
  q.targetLowPrice,
  q.analyst_upside_pct,
  q.recommendationMean,
  q.numberOfAnalystOpinions,
  q.is_strong_buy
FROM `your_project.your_dataset.quantitative_table` q
LEFT JOIN `your_project.your_dataset.dimension_table` d ON q.ticker_id = d.ticker_id;


-- 4. Undervalued Stocks Screener
-- Ranks by lowest PE + highest revenue growth within each sector.
-- Pins to each ticker's latest ingestion_date to avoid mixing stale data
-- across multiple days as the table accumulates.
CREATE OR REPLACE VIEW `your_project.your_dataset.undervalued_stocks_view` AS
WITH latest AS (
  SELECT ticker_id, MAX(ingestion_date) AS latest_date
  FROM `your_project.your_dataset.quantitative_table`
  GROUP BY ticker_id
),
latest_data AS (
  SELECT q.*
  FROM `your_project.your_dataset.quantitative_table` q
  INNER JOIN latest l ON q.ticker_id = l.ticker_id AND q.ingestion_date = l.latest_date
),
ranked AS (
  SELECT
    d.sector,
    q.ticker_id,
    q.ingestion_date,
    q.currentPrice,
    q.trailingPE,
    q.revenueGrowth,
    q.returnOnEquity,
    q.analyst_upside_pct,
    q.is_strong_buy,
    q.week52_position_score,
    q.market_cap_cr,
    ROW_NUMBER() OVER (
      PARTITION BY d.sector
      ORDER BY q.trailingPE ASC, q.revenueGrowth DESC
    ) AS rank
  FROM latest_data q
  JOIN `your_project.your_dataset.dimension_table` d ON q.ticker_id = d.ticker_id
  WHERE q.trailingPE IS NOT NULL    -- exclude tickers with no PE (pre-profit)
)
SELECT * FROM ranked WHERE rank = 1;
