# Triggered by ETL_Orchestrate.py after transformation completes.
# SQL is defined as string constants here — NOT loaded via open() at module
# scope, which would crash Airflow DAG parsing if the file path is missing.

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

default_args = {
    "start_date": datetime(2024, 1, 1),
    "retries": 1
}

# ── SQL Definitions ───────────────────────────────────────────────────────────
# Defined at module level as string constants (not open()) — safe for Airflow parsing.
# For very long SQL, store in GCS and reference via gcs_uri in the job config.

_QUANTITATIVE_SQL = """
MERGE `your_project.your_dataset.quantitative_table` T
USING (
  SELECT
    ticker_id,
    CAST(ingestion_date AS DATE)   AS ingestion_date,
    currentPrice, previousClose, regularMarketChangePercent,
    marketCap, market_cap_cr,
    trailingPE, forwardPE, pegRatio, priceToBook,
    totalRevenue, totalDebt, totalCash, freeCashflow,
    profitMargins, grossMargins, operatingMargins, ebitdaMargins,
    returnOnAssets, returnOnEquity, debtToEquity,
    currentRatio, quickRatio,
    revenueGrowth, earningsGrowth, earningsQuarterlyGrowth,
    dividendYield, dividendRate, payoutRatio,
    beta, overallRisk,
    recommendationMean, numberOfAnalystOpinions,
    targetMeanPrice, targetHighPrice, targetLowPrice,
    heldPercentInsiders, heldPercentInstitutions,
    fiftyTwoWeekLow, fiftyTwoWeekHigh,
    -- Engineered columns
    market_cap_cr, revenue_cr, net_cash_cr,
    pe_category, roe_category, revenue_growth_category,
    week52_position_score, discount_from_52w_high_pct, above_200dma,
    analyst_upside_pct, is_strong_buy, earnings_yield,
    avg_governance_risk,
    week52_change, sandp_52week_change
  FROM `your_project.your_dataset.yfinance_flat_table`
) S
ON  T.ticker_id      = S.ticker_id
AND T.ingestion_date = S.ingestion_date
WHEN MATCHED THEN
  UPDATE SET
    currentPrice = S.currentPrice,
    marketCap = S.marketCap,
    market_cap_cr = S.market_cap_cr,
    trailingPE = S.trailingPE,
    revenueGrowth = S.revenueGrowth,
    profitMargins = S.profitMargins,
    pe_category = S.pe_category,
    roe_category = S.roe_category,
    revenue_growth_category = S.revenue_growth_category,
    week52_position_score = S.week52_position_score,
    discount_from_52w_high_pct = S.discount_from_52w_high_pct,
    above_200dma = S.above_200dma,
    analyst_upside_pct = S.analyst_upside_pct,
    is_strong_buy = S.is_strong_buy,
    earnings_yield = S.earnings_yield,
    avg_governance_risk = S.avg_governance_risk
WHEN NOT MATCHED THEN
  INSERT ROW;
"""

_DIMENSION_SQL = """
-- Full refresh of dimension table (slowly changing but not tracked)
CREATE OR REPLACE TABLE `your_project.your_dataset.dimension_table` AS
SELECT DISTINCT
  ticker_id, symbol, longName, shortName,
  sector, sectorKey, industry, industryKey,
  country, city, currency, financialCurrency,
  exchange, fullExchangeName, quoteType, market, website
FROM `your_project.your_dataset.yfinance_flat_table`;
"""

_VIEWS_SQL = """
CREATE OR REPLACE VIEW `your_project.your_dataset.fundamental_view` AS
SELECT
  q.ticker_id, q.ingestion_date,
  d.sector, d.industry,
  q.revenueGrowth, q.revenue_growth_category,
  q.profitMargins, q.grossMargins,
  q.returnOnEquity, q.roe_category,
  q.market_cap_cr, q.dividendYield
FROM `your_project.your_dataset.quantitative_table` q
LEFT JOIN `your_project.your_dataset.dimension_table` d ON q.ticker_id = d.ticker_id;

CREATE OR REPLACE VIEW `your_project.your_dataset.technical_view` AS
SELECT
  ticker_id, ingestion_date,
  currentPrice, beta,
  trailingPE, forwardPE, pe_category,
  week52_position_score, discount_from_52w_high_pct, above_200dma,
  week52_change, sandp_52week_change
FROM `your_project.your_dataset.quantitative_table`;

-- Screener view: undervalued, high-growth, analyst-backed stocks
-- Pins to each ticker's latest ingestion date to avoid mixing stale rows
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
    d.sector, q.ticker_id, q.ingestion_date,
    q.trailingPE, q.revenueGrowth, q.returnOnEquity,
    q.analyst_upside_pct, q.is_strong_buy,
    q.week52_position_score, q.discount_from_52w_high_pct,
    ROW_NUMBER() OVER (
      PARTITION BY d.sector
      ORDER BY q.trailingPE ASC, q.revenueGrowth DESC
    ) AS rank
  FROM latest_data q
  JOIN `your_project.your_dataset.dimension_table` d ON q.ticker_id = d.ticker_id
  WHERE q.trailingPE IS NOT NULL
)
SELECT * FROM ranked WHERE rank = 1;
"""

# ── DAG ───────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="yfinance_warehousing",
    schedule_interval=None,
    default_args=default_args,
    catchup=False
) as dag:

    load_quantitative = BigQueryInsertJobOperator(
        task_id="load_quantitative",
        configuration={"query": {"query": _QUANTITATIVE_SQL, "useLegacySql": False}}
    )

    load_dimension = BigQueryInsertJobOperator(
        task_id="load_dimension",
        configuration={"query": {"query": _DIMENSION_SQL, "useLegacySql": False}}
    )

    create_views = BigQueryInsertJobOperator(
        task_id="create_views",
        configuration={"query": {"query": _VIEWS_SQL, "useLegacySql": False}}
    )

    load_quantitative >> load_dimension >> create_views
