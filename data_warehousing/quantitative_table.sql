-- Stores daily price, valuation, financials, and all engineered metrics.
-- Partitioned by ingestion_date + clustered by ticker_id for cost-efficient queries.

CREATE TABLE IF NOT EXISTS `your_project.your_dataset.quantitative_table`
(
  ticker_id        STRING,
  ingestion_date   DATE,         -- PARTITION BY column, must be DATE
  currentPrice     FLOAT64,
  previousClose    FLOAT64,
  regularMarketChangePercent   FLOAT64,
  marketCap        INT64,
  market_cap_cr    FLOAT64,     
  enterpriseValue  INT64,
  trailingPE       FLOAT64,
  forwardPE        FLOAT64,
  pegRatio         FLOAT64,
  priceToBook      FLOAT64,
  enterpriseToRevenue          FLOAT64,
  enterpriseToEbitda           FLOAT64,
  trailingEps      FLOAT64,
  forwardEps       FLOAT64,
  bookValue        FLOAT64,
  totalRevenue     INT64,
  revenue_cr       FLOAT64,    
  totalCash        INT64,
  totalDebt        INT64,
  net_cash_cr      FLOAT64,     
  ebitda           INT64,
  freeCashflow     INT64,
  operatingCashflowINT64,
  netIncomeToCommonINT64,
  profitMargins    FLOAT64,
  grossMargins     FLOAT64,
  operatingMargins FLOAT64,
  ebitdaMargins    FLOAT64,
  returnOnAssets   FLOAT64,
  returnOnEquity   FLOAT64,
  debtToEquity     FLOAT64,
  currentRatio     FLOAT64,
  quickRatio       FLOAT64,
  payoutRatio      FLOAT64,
  revenueGrowth    FLOAT64,
  earningsGrowth   FLOAT64,
  earningsQuarterlyGrowth      FLOAT64,
  dividendYield    FLOAT64,
  dividendRate     FLOAT64,
  fiveYearAvgDividendYield     FLOAT64,
  beta FLOAT64,
  fiftyTwoWeekLow  FLOAT64,
  fiftyTwoWeekHigh FLOAT64,
  fiftyDayAverage  FLOAT64,
  twoHundredDayAverage         FLOAT64,
  week52_change    FLOAT64,    
  sandp_52week_change          FLOAT64,
  recommendationMean           FLOAT64,
  numberOfAnalystOpinions      INT64,
  targetMeanPrice  FLOAT64,
  targetHighPrice  FLOAT64,
  targetLowPrice   FLOAT64,
  overallRisk      INT64,
  avg_governance_risk          FLOAT64,
  heldPercentInsiders          FLOAT64,
  heldPercentInstitutions      FLOAT64,
  pe_category      STRING,       
  roe_category     STRING,       
  revenue_growth_category      STRING,     
  week52_position_score        FLOAT64,      
  discount_from_52w_high_pct   FLOAT64,   
  above_200dma     BOOL,      
  analyst_upside_pct           FLOAT64,      
  is_strong_buy    BOOL,       
  earnings_yield   FLOAT64   
)
PARTITION BY ingestion_date
CLUSTER BY ticker_id;


-- Incremental Load (MERGE)
-- CAST(ingestion_date AS DATE) guards against string arriving from flat table
MERGE `your_project.your_dataset.quantitative_table` T
USING (
  SELECT * REPLACE (CAST(ingestion_date AS DATE) AS ingestion_date)
  FROM `your_project.your_dataset.yfinance_flat_table`
) S
ON  T.ticker_id      = S.ticker_id
AND T.ingestion_date = S.ingestion_date

WHEN MATCHED THEN
  UPDATE SET
    currentPrice = S.currentPrice,
    marketCap = S.marketCap,
    market_cap_cr = S.market_cap_cr,
    revenue_cr  = S.revenue_cr,
    net_cash_cr = S.net_cash_cr,
    trailingPE = S.trailingPE,
    forwardPE = S.forwardPE,
    revenueGrowth = S.revenueGrowth,
    earningsGrowth = S.earningsGrowth,
    profitMargins = S.profitMargins,
    returnOnEquity = S.returnOnEquity,
    pe_category = S.pe_category,
    roe_category = S.roe_category,
    revenue_growth_category = S.revenue_growth_category,
    week52_position_score = S.week52_position_score,
    discount_from_52w_high_pct = S.discount_from_52w_high_pct,
    above_200dma = S.above_200dma,
    analyst_upside_pct  = S.analyst_upside_pct,
    is_strong_buy  = S.is_strong_buy,
    earnings_yield = S.earnings_yield,
    avg_governance_risk  = S.avg_governance_risk

WHEN NOT MATCHED THEN
  INSERT ROW;
