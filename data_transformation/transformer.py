"""Applies all cleaning, renaming, and feature engineering to the raw Spark DataFrame.
Called by main.py after read_json().
"""
from pyspark.sql.functions import col, when, lit, round as spark_round
from pyspark.sql.types import StructType




# JSON keys that start with digits are illegal in BigQuery column names.
# Spark reads them fine (StructField name is just a string), but we must
# rename before writing to BQ.
_RENAME_MAP = {
    "52WeekChange":    "week52_change",
    "SandP52WeekChange": "sandp_52week_change",
}

# Step 1: Rename problematic columns before any transformations
def rename_problematic_columns(df):
    """Rename columns that are illegal or ambiguous in BigQuery."""
    for old, new in _RENAME_MAP.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
    return df


# Step 2: Flatten any struct columns (safety net if schema.yaml ever adds nested types)

def flatten_df(df):
    """
    Flatten any StructType columns one level deep.
    With explicit schema enforcement, yfinance JSON shouldn't produce struct
    columns, but this acts as a safety net if schema.yaml ever adds struct types.
    """
    flat_cols = []
    for field in df.schema.fields:
        if isinstance(field.dataType, StructType):
            for sub in field.dataType.fields:
                flat_cols.append(col(f"{field.name}.{sub.name}").alias(f"{field.name}_{sub.name}"))
        else:
            flat_cols.append(col(field.name))
    return df.select(flat_cols)


# Step 3: Add derived features for analytics and stock screening
def add_features(df):
    """
    Derived columns for analytics and stock screening.
    All use when(...).otherwise(lit(None)) guards to handle null inputs safely.
    """

    # Market cap in Indian crores (÷ 1 crore = 1e7)
    df = df.withColumn(
        "market_cap_cr",
        when(col("marketCap").isNotNull(), spark_round(col("marketCap") / 1e7, 2))
        .otherwise(lit(None))
    )

    # Cap outlier dividend yields (>100 is clearly a data error)
    df = df.withColumn(
        "dividendYield",
        when(col("dividendYield") > 100, lit(None)).otherwise(col("dividendYield"))
    )

    # PE bucket — null PE gets its own null bucket (not falsely labelled HIGH)
    df = df.withColumn(
        "pe_category",
        when(col("trailingPE").isNull(), lit(None))
        .when(col("trailingPE") < 15,   lit("LOW"))
        .when(col("trailingPE") < 25,   lit("MEDIUM"))
        .otherwise(lit("HIGH"))
    )

    # Where does current price sit in the 52-week range? (0 = at 52w low, 100 = at 52w high)
    # Useful for identifying stocks near highs vs beaten-down ones
    df = df.withColumn(
        "week52_position_score",
        when(
            (col("fiftyTwoWeekHigh") - col("fiftyTwoWeekLow")) > 0,
            spark_round(
                (col("currentPrice") - col("fiftyTwoWeekLow")) /
                (col("fiftyTwoWeekHigh") - col("fiftyTwoWeekLow")) * 100,
                2
            )
        ).otherwise(lit(None))
    )

    # % discount from 52-week high — lower = more discounted from peak
    df = df.withColumn(
        "discount_from_52w_high_pct",
        when(
            col("fiftyTwoWeekHigh") > 0,
            spark_round((col("fiftyTwoWeekHigh") - col("currentPrice")) / col("fiftyTwoWeekHigh") * 100, 2)
        ).otherwise(lit(None))
    )

    # Is price above its 200-day moving average? (trend signal)
    df = df.withColumn(
        "above_200dma",
        when(col("currentPrice").isNull() | col("twoHundredDayAverage").isNull(), lit(None))
        .when(col("currentPrice") > col("twoHundredDayAverage"), lit(True))
        .otherwise(lit(False))
    )

    # % upside to analyst mean target from current price
    df = df.withColumn(
        "analyst_upside_pct",
        when(
            col("targetMeanPrice").isNotNull() & (col("currentPrice") > 0),
            spark_round((col("targetMeanPrice") - col("currentPrice")) / col("currentPrice") * 100, 2)
        ).otherwise(lit(None))
    )

    # Strong buy flag: consensus rating ≤ 1.5 on 1–5 scale
    df = df.withColumn(
        "is_strong_buy",
        when(col("recommendationMean").isNull(), lit(None))
        .when(col("recommendationMean") <= 1.5, lit(True))
        .otherwise(lit(False))
    )

    # Net cash position in crores: positive = cash-rich, negative = net debt
    df = df.withColumn(
        "net_cash_cr",
        when(
            col("totalCash").isNotNull() & col("totalDebt").isNotNull(),
            spark_round((col("totalCash") - col("totalDebt")) / 1e7, 2)
        ).otherwise(lit(None))
    )

    # Revenue in crores (easier to read than raw INR)
    df = df.withColumn(
        "revenue_cr",
        when(col("totalRevenue").isNotNull(), spark_round(col("totalRevenue") / 1e7, 2))
        .otherwise(lit(None))
    )

    # Earnings yield = 1/PE — handy for comparing equity yield vs bond yield
    df = df.withColumn(
        "earnings_yield",
        when(
            col("trailingPE").isNotNull() & (col("trailingPE") != 0),
            spark_round(lit(1.0) / col("trailingPE"), 4)
        ).otherwise(lit(None))
    )

    # ROE bucket (return on equity quality tier)
    df = df.withColumn(
        "roe_category",
        when(col("returnOnEquity").isNull(), lit(None))
        .when(col("returnOnEquity") >= 0.25, lit("HIGH"))
        .when(col("returnOnEquity") >= 0.15, lit("MEDIUM"))
        .otherwise(lit("LOW"))
    )

    # Revenue growth quality tier
    df = df.withColumn(
        "revenue_growth_category",
        when(col("revenueGrowth").isNull(), lit(None))
        .when(col("revenueGrowth") >= 0.20, lit("HIGH"))
        .when(col("revenueGrowth") >= 0.10, lit("MEDIUM"))
        .otherwise(lit("LOW"))
    )

    # Composite governance risk = simple average of available sub-scores
    # Lower = better governed (ISS scale 1–10)
    df = df.withColumn(
        "avg_governance_risk",
        spark_round(
            (
                col("auditRisk").cast("double") +
                col("boardRisk").cast("double") +
                col("compensationRisk").cast("double") +
                col("shareHolderRightsRisk").cast("double")
            ) / lit(4.0),
            2
        )
    )

    return df


# Step 4: Master transform function called by main.py. 
# Order: rename → flatten → feature engineering.

def transform(df):
    """
    Master transform function called by main.py.
    Order: rename → flatten → feature engineering.
    """
    df = rename_problematic_columns(df)
    df = flatten_df(df)
    df = add_features(df)
    return df
