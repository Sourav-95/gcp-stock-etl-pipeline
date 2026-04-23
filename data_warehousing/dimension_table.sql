-- Full refresh on each pipeline run. Dimension data changes rarely but is
-- not SCD-tracked here — last seen values win.

CREATE OR REPLACE TABLE `your_project.your_dataset.dimension_table` AS
SELECT DISTINCT
  ticker_id,
  symbol,
  longName,
  shortName,
  sector,
  sectorKey,
  industry,
  industryKey,
  country,
  city,
  currency,
  financialCurrency,
  exchange,
  fullExchangeName,
  quoteType,
  market,
  website
FROM `your_project.your_dataset.yfinance_flat_table`;
