# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Forecasting Features
# MAGIC
# MAGIC **Layer:** Silver/Gold → Gold  
# MAGIC **Notebook:** `05_gold_forecasting_features`
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC Create a modeling-ready monthly feature table for economic forecasting.
# MAGIC
# MAGIC This notebook prepares time-series predictors from the existing Gold economic indicators while enforcing a strict no-leakage rule.
# MAGIC
# MAGIC ### Forecasting Design
# MAGIC
# MAGIC The feature table will support forecasting tasks such as:
# MAGIC
# MAGIC - Retail sales forecasting
# MAGIC - GDP forecasting
# MAGIC - Inflation forecasting
# MAGIC - Labour-market forecasting
# MAGIC
# MAGIC ### Target Grain
# MAGIC
# MAGIC One row per month.
# MAGIC
# MAGIC ### Core Rule: Prevent Data Leakage
# MAGIC
# MAGIC For a prediction made for month **t**, feature values must come only from information available at or before the permitted forecasting cutoff.
# MAGIC
# MAGIC Future values must never be used to construct predictors for earlier observations.
# MAGIC
# MAGIC #### Feature Types
# MAGIC
# MAGIC The forecasting table may include:
# MAGIC
# MAGIC - Lagged economic indicators
# MAGIC - Rolling historical averages
# MAGIC - Historical growth rates
# MAGIC - Monetary and exchange-rate indicators
# MAGIC - Calendar features
# MAGIC
# MAGIC ### Business Key
# MAGIC
# MAGIC `ref_date`
# MAGIC
# MAGIC The final output will preserve clear feature timing so it can later be split into training, validation, and forecasting periods safely.

# COMMAND ----------

# =========================================================
# CONFIGURE GOLD FORECASTING FEATURES
# =========================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

GOLD_FORECASTING_FEATURES_PATH = (
    "abfss://gold@stceidev01.dfs.core.windows.net/"
    "forecasting_features/monthly/"
)

GOLD_ECONOMIC_OVERVIEW_PATH = (
    "abfss://gold@stceidev01.dfs.core.windows.net/"
    "economic_overview/"
)

print("Gold Forecasting Features configuration loaded.")
print(f"Source: {GOLD_ECONOMIC_OVERVIEW_PATH}")
print(f"Output: {GOLD_FORECASTING_FEATURES_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load and Inspect the Gold Economic Overview
# MAGIC
# MAGIC The forecasting feature table is built from the validated monthly Economic Overview Gold mart.
# MAGIC
# MAGIC Before engineering forecasting features, inspect the source structure and coverage to confirm which indicators are available and where publication-lag nulls occur.
# MAGIC
# MAGIC ### Source Grain
# MAGIC
# MAGIC One row per month.
# MAGIC
# MAGIC ### Source Business Key
# MAGIC
# MAGIC `ref_date`
# MAGIC
# MAGIC ### Inspection Goals
# MAGIC
# MAGIC - Confirm the available economic indicators and derived measures
# MAGIC - Verify the monthly date range
# MAGIC - Confirm business-key uniqueness
# MAGIC - Identify nulls caused by different publication schedules
# MAGIC - Determine which variables are appropriate as forecasting targets and predictors
# MAGIC
# MAGIC No forecasting features are created in this step. The source is inspected first so feature engineering is based on validated data rather than assumptions.

# COMMAND ----------

# =========================================================
# LOAD AND INSPECT GOLD ECONOMIC OVERVIEW
# =========================================================

# Load the validated monthly Gold mart that will serve as the
# primary source for forecasting targets and predictor features.
gold_economic_overview = (
    spark.read
    .format("delta")
    .load(GOLD_ECONOMIC_OVERVIEW_PATH)
)

print("Gold Economic Overview loaded successfully.")
print(f"Rows: {gold_economic_overview.count():,}")


# ---------------------------------------------------------
# Validate monthly source coverage
# ---------------------------------------------------------

gold_economic_overview.agg(
    F.min("ref_date").alias("min_date"),
    F.max("ref_date").alias("max_date")
).show()


# ---------------------------------------------------------
# Validate the source business key
# ---------------------------------------------------------

# The Economic Overview should contain exactly one row per month.
overview_duplicate_months = (
    gold_economic_overview
    .groupBy("ref_date")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(
    f"Duplicate monthly business keys: "
    f"{overview_duplicate_months:,}"
)


# ---------------------------------------------------------
# Inspect available forecasting fields
# ---------------------------------------------------------

print("\nEconomic Overview schema:")
gold_economic_overview.printSchema()


# ---------------------------------------------------------
# Inspect source null patterns
# ---------------------------------------------------------

# Nulls are important for forecasting because the newest indicators
# may have different publication lags. They must not automatically
# be filled with future observations or zero values.
print("\nNull counts by column:")

gold_economic_overview.select(
    [
        F.sum(
            F.col(column_name).isNull().cast("int")
        ).alias(column_name)
        for column_name in gold_economic_overview.columns
    ]
).show(truncate=False)


# ---------------------------------------------------------
# Inspect the most recent monthly observations
# ---------------------------------------------------------

# Recent rows help identify publication-lag differences that must
# be considered when defining the forecasting cutoff later.
display(
    gold_economic_overview
    .orderBy(F.col("ref_date").desc())
    .limit(15)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Define Forecast Targets and Feature Timing Rules
# MAGIC
# MAGIC The forecasting dataset supports several economic forecasting targets while using a common monthly feature framework.
# MAGIC
# MAGIC ### Forecast Targets
# MAGIC
# MAGIC The initial modeling targets are:
# MAGIC
# MAGIC 1. **Retail Sales**
# MAGIC    - Target column: `retail_sales_dollars`
# MAGIC
# MAGIC 2. **Consumer Prices**
# MAGIC    - Target column: `cpi_all_items`
# MAGIC    - Inflation can also be modeled using `cpi_yoy_inflation_percent`
# MAGIC
# MAGIC 3. **Real GDP**
# MAGIC    - Target column: `real_gdp_chained_2017_millions`
# MAGIC
# MAGIC 4. **Employment**
# MAGIC    - Target column: `employment_thousands`
# MAGIC
# MAGIC These targets remain separate rather than being combined into a single artificial outcome.
# MAGIC
# MAGIC ### Forecast Horizon
# MAGIC
# MAGIC The initial feature table is designed for a **one-month-ahead forecast**.
# MAGIC
# MAGIC For target month **t**, predictor features must come from month **t-1 or earlier**.
# MAGIC
# MAGIC For example:
# MAGIC
# MAGIC - Target: Retail Sales for July 2026
# MAGIC - Latest permitted predictor month: June 2026
# MAGIC
# MAGIC ### Leakage-Prevention Rule
# MAGIC
# MAGIC Current-month predictor values are not used to predict that same month's target.
# MAGIC
# MAGIC All economic predictor variables will therefore be shifted backward by at least one month before they are exposed to a forecasting model.
# MAGIC
# MAGIC Rolling statistics will also be calculated only from historical observations available before the target month.
# MAGIC
# MAGIC ### Publication-Lag and Forecast-Cutoff Principle
# MAGIC
# MAGIC Economic indicators are released on different schedules.
# MAGIC
# MAGIC This feature table applies a strict **reference-period cutoff**: for target month **t**, predictor observations must come from reference month **t-1 or earlier**.
# MAGIC
# MAGIC The current source datasets do not contain historical publication-vintage timestamps for every indicator. Therefore, this notebook should not be interpreted as a fully vintage-aware real-time forecasting dataset.
# MAGIC
# MAGIC Legitimate source nulls near the newest months are preserved and are never filled using future observations.
# MAGIC
# MAGIC A production forecasting system could later add release-date or vintage metadata to enforce the exact information set available on each historical forecast date.
# MAGIC
# MAGIC ### Feature Table Grain
# MAGIC
# MAGIC One row per target month.
# MAGIC
# MAGIC ### Business Key
# MAGIC
# MAGIC `ref_date`
# MAGIC
# MAGIC Here, `ref_date` represents the month being forecast, while lagged feature columns represent information from earlier months.

# COMMAND ----------

# =========================================================
# VALIDATE FORECAST TARGET COVERAGE
# =========================================================

# Define the initial forecasting targets in one reusable structure.
# Keeping this list centralized makes it easier to add or remove
# targets later without rewriting validation logic.
FORECAST_TARGETS = [
    "retail_sales_dollars",
    "cpi_all_items",
    "real_gdp_chained_2017_millions",
    "employment_thousands"
]

print("Forecast targets:")
for target in FORECAST_TARGETS:
    print(f" - {target}")


# ---------------------------------------------------------
# Determine actual source coverage for each target
# ---------------------------------------------------------

# Each indicator has its own publication schedule. Finding the first
# and last non-null month prevents us from assuming that all targets
# are available through the same date.
target_coverage_expressions = []

for target in FORECAST_TARGETS:
    target_coverage_expressions.extend([
        F.min(
            F.when(
                F.col(target).isNotNull(),
                F.col("ref_date")
            )
        ).alias(f"{target}_first_available"),

        F.max(
            F.when(
                F.col(target).isNotNull(),
                F.col("ref_date")
            )
        ).alias(f"{target}_last_available")
    ])

print("\nForecast target coverage:")

gold_economic_overview.agg(
    *target_coverage_expressions
).show(truncate=False)


# ---------------------------------------------------------
# Inspect availability in the most recent months
# ---------------------------------------------------------

# This view is important for future model cutoffs because recent
# months may contain some targets while others are still unpublished.
display(
    gold_economic_overview
    .select(
        "ref_date",
        *FORECAST_TARGETS
    )
    .orderBy(F.col("ref_date").desc())
    .limit(15)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create Leakage-Safe Lagged Features
# MAGIC
# MAGIC Forecasting features must represent information available before the target month.
# MAGIC
# MAGIC For a one-month-ahead forecast of month **t**, the earliest predictor reference is therefore month **t-1**.
# MAGIC
# MAGIC ### Lag Strategy
# MAGIC
# MAGIC The feature table uses several historical horizons:
# MAGIC
# MAGIC - **Lag 1** → previous month
# MAGIC - **Lag 3** → three months earlier
# MAGIC - **Lag 6** → six months earlier
# MAGIC - **Lag 12** → same month in the previous year
# MAGIC
# MAGIC These lags help models capture:
# MAGIC
# MAGIC - Short-term momentum
# MAGIC - Medium-term economic changes
# MAGIC - Seasonal patterns
# MAGIC - Year-over-year relationships
# MAGIC
# MAGIC ### Predictor Timing
# MAGIC
# MAGIC All source indicators used as model predictors are shifted backward before being exposed to the model.
# MAGIC
# MAGIC The original target columns remain aligned to the target month and are **not** shifted.
# MAGIC
# MAGIC This separation ensures that a model predicting month **t** cannot directly observe month **t** economic indicators.

# COMMAND ----------

# =========================================================
# CREATE LEAKAGE-SAFE LAGGED FEATURES
# =========================================================

# A single chronological window is appropriate because the source
# contains exactly one validated observation per month.
forecast_window = (
    Window
    .partitionBy(F.lit(1))
    .orderBy("ref_date")
)


# ---------------------------------------------------------
# Define predictor variables
# ---------------------------------------------------------

# These indicators can provide useful historical information for
# multiple forecasting targets. Their current-month values will not
# be exposed to the model; only lagged versions will be used.
PREDICTOR_COLUMNS = [
    "retail_sales_dollars",
    "cpi_all_items",
    "employment_thousands",
    "unemployment_rate_percent",
    "real_gdp_chained_2017_millions",
    "population",
    "avg_monthly_fx_usd_cad",
    "policy_rate",
    "housing_starts_saar"
]

# Multiple lag horizons capture short-, medium-, and annual dynamics.
LAG_MONTHS = [1, 3, 6, 12]


# ---------------------------------------------------------
# Start with target-month identifiers and target values
# ---------------------------------------------------------

forecast_features = (
    gold_economic_overview
    .select(
        "ref_date",

        # Targets remain aligned with the month being predicted.
        "retail_sales_dollars",
        "cpi_all_items",
        "real_gdp_chained_2017_millions",
        "employment_thousands",

        # Predictor source columns are temporarily retained here so
        # lag features can be calculated in the following loop.
        "unemployment_rate_percent",
        "population",
        "avg_monthly_fx_usd_cad",
        "policy_rate",
        "housing_starts_saar"
    )
)


# ---------------------------------------------------------
# Generate historical lag features
# ---------------------------------------------------------

# Every generated predictor is shifted by at least one month.
# This is the core leakage-prevention rule for the feature table.
for column_name in PREDICTOR_COLUMNS:
    for lag_month in LAG_MONTHS:
        forecast_features = (
            forecast_features
            .withColumn(
                f"{column_name}_lag_{lag_month}m",
                F.lag(
                    F.col(column_name),
                    lag_month
                ).over(forecast_window)
            )
        )


# ---------------------------------------------------------
# Remove unshifted predictor-only columns
# ---------------------------------------------------------

# These variables were needed to generate their lagged versions but
# should not remain as same-month predictors in the modeling table.
forecast_features = (
    forecast_features
    .drop(
        "unemployment_rate_percent",
        "population",
        "avg_monthly_fx_usd_cad",
        "policy_rate",
        "housing_starts_saar"
    )
)


print(
    f"Forecast feature rows after lag creation: "
    f"{forecast_features.count():,}"
)

print(
    f"Forecast feature columns: "
    f"{len(forecast_features.columns):,}"
)

print("\nLag feature columns:")
for column_name in forecast_features.columns:
    if "_lag_" in column_name:
        print(f" - {column_name}")


# ---------------------------------------------------------
# Inspect recent target months and lagged information
# ---------------------------------------------------------

display(
    forecast_features
    .select(
        "ref_date",

        # Example targets
        "retail_sales_dollars",
        "cpi_all_items",

        # Example leakage-safe predictors
        "retail_sales_dollars_lag_1m",
        "retail_sales_dollars_lag_12m",
        "cpi_all_items_lag_1m",
        "cpi_all_items_lag_12m",
        "policy_rate_lag_1m",
        "avg_monthly_fx_usd_cad_lag_1m"
    )
    .orderBy(F.col("ref_date").desc())
    .limit(15)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create Leakage-Safe Rolling Features
# MAGIC
# MAGIC Rolling historical features summarize recent economic conditions without exposing information from the target month.
# MAGIC
# MAGIC ### Rolling Windows
# MAGIC
# MAGIC Two historical windows are created:
# MAGIC
# MAGIC - **3-month rolling average** → captures short-term economic conditions
# MAGIC - **6-month rolling average** → captures medium-term economic conditions
# MAGIC
# MAGIC ### Leakage-Prevention Rule
# MAGIC
# MAGIC For target month **t**, each rolling window ends at **t-1**.
# MAGIC
# MAGIC For example, a 3-month rolling average used to forecast July is calculated from:
# MAGIC
# MAGIC - April
# MAGIC - May
# MAGIC - June
# MAGIC
# MAGIC July itself is excluded.
# MAGIC
# MAGIC ### Minimum History
# MAGIC
# MAGIC Rolling averages are calculated only when the complete historical window is available.
# MAGIC
# MAGIC This prevents early rows with insufficient history from being interpreted as full 3-month or 6-month averages.
# MAGIC
# MAGIC ### Design Principle
# MAGIC
# MAGIC Rolling features supplement the explicit lag features. They do not replace the original forecasting targets and do not alter the published source values.

# COMMAND ----------

# =========================================================
# CREATE LEAKAGE-SAFE ROLLING FEATURES
# =========================================================

# The rolling calculations operate on lag_1m features.
#
# lag_1m on target month t already represents source month t-1.
# Therefore:
#   rowsBetween(-2, 0) = t-3 through t-1  -> 3 months
#   rowsBetween(-5, 0) = t-6 through t-1  -> 6 months
#
# This preserves the one-month-ahead leakage boundary while avoiding
# an accidental second shift of the historical information.
rolling_3m_window = (
    Window
    .partitionBy(F.lit(1))
    .orderBy("ref_date")
    .rowsBetween(-2, 0)
)

rolling_6m_window = (
    Window
    .partitionBy(F.lit(1))
    .orderBy("ref_date")
    .rowsBetween(-5, 0)
)


# ---------------------------------------------------------
# Define indicators used for rolling historical conditions
# ---------------------------------------------------------

ROLLING_FEATURE_COLUMNS = [
    "retail_sales_dollars",
    "cpi_all_items",
    "employment_thousands",
    "real_gdp_chained_2017_millions",
    "avg_monthly_fx_usd_cad",
    "policy_rate"
]


# ---------------------------------------------------------
# Add rolling historical averages
# ---------------------------------------------------------

for column_name in ROLLING_FEATURE_COLUMNS:

    lag_1m_column = f"{column_name}_lag_1m"

    # Short-term historical condition:
    # complete three-month window ending at t-1.
    forecast_features = (
        forecast_features
        .withColumn(
            f"{column_name}_rolling_avg_3m",
            F.when(
                F.count(
                    F.col(lag_1m_column)
                ).over(rolling_3m_window) == 3,
                F.avg(
                    F.col(lag_1m_column)
                ).over(rolling_3m_window)
            )
        )
    )

    # Medium-term historical condition:
    # complete six-month window ending at t-1.
    forecast_features = (
        forecast_features
        .withColumn(
            f"{column_name}_rolling_avg_6m",
            F.when(
                F.count(
                    F.col(lag_1m_column)
                ).over(rolling_6m_window) == 6,
                F.avg(
                    F.col(lag_1m_column)
                ).over(rolling_6m_window)
            )
        )
    )


print(
    f"Forecast feature rows: "
    f"{forecast_features.count():,}"
)

print(
    f"Forecast feature columns after rolling features: "
    f"{len(forecast_features.columns):,}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Make Forecast Targets Explicit
# MAGIC
# MAGIC The forecasting table contains several possible modeling outcomes.
# MAGIC
# MAGIC To clearly separate prediction targets from leakage-safe predictor features, all same-month outcome columns are prefixed with `target_`.
# MAGIC
# MAGIC ### Target Columns
# MAGIC
# MAGIC - `target_retail_sales_dollars`
# MAGIC - `target_cpi_all_items`
# MAGIC - `target_real_gdp_chained_2017_millions`
# MAGIC - `target_employment_thousands`
# MAGIC
# MAGIC ### Modeling Rule
# MAGIC
# MAGIC For a specific forecasting model, only the selected `target_` column is used as the outcome.
# MAGIC
# MAGIC Other same-month `target_` columns must not be used as predictor features.
# MAGIC
# MAGIC Predictor variables are identified through lagged, rolling, and other explicitly historical feature columns.

# COMMAND ----------

# =========================================================
# MAKE FORECAST TARGETS EXPLICIT
# =========================================================

# Prefix same-month outcome columns with "target_" so they cannot
# be confused with leakage-safe predictor features during modeling.
#
# Only the selected target_* column should be used as the outcome
# for a specific model. Other target_* columns must not be used
# as predictors.

forecast_features = (
    forecast_features
    .withColumnRenamed(
        "retail_sales_dollars",
        "target_retail_sales_dollars"
    )
    .withColumnRenamed(
        "cpi_all_items",
        "target_cpi_all_items"
    )
    .withColumnRenamed(
        "real_gdp_chained_2017_millions",
        "target_real_gdp_chained_2017_millions"
    )
    .withColumnRenamed(
        "employment_thousands",
        "target_employment_thousands"
    )
)


# ---------------------------------------------------------
# Validate explicit target columns
# ---------------------------------------------------------

target_columns = [
    column_name
    for column_name in forecast_features.columns
    if column_name.startswith("target_")
]

print("Forecast target columns made explicit:")

for column_name in target_columns:
    print(f" - {column_name}")

print(
    f"\nForecast feature rows: "
    f"{forecast_features.count():,}"
)

print(
    f"Forecast feature columns: "
    f"{len(forecast_features.columns):,}"
)


# Confirm that exactly four same-month target columns remain.
assert len(target_columns) == 4, (
    "Expected exactly four explicit forecast target columns."
)

print("\nForecast target validation passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Create Calendar and Seasonality Features
# MAGIC
# MAGIC Calendar features allow forecasting models to recognize recurring monthly and seasonal economic patterns.
# MAGIC
# MAGIC Unlike economic observations, calendar information for a target month is known in advance and therefore does not create data leakage.
# MAGIC
# MAGIC ### Calendar Features
# MAGIC
# MAGIC The forecasting table includes:
# MAGIC
# MAGIC - Target year
# MAGIC - Target month
# MAGIC - Target quarter
# MAGIC - Cyclical month representation
# MAGIC
# MAGIC ### Cyclical Seasonality
# MAGIC
# MAGIC Month numbers alone treat December (`12`) and January (`1`) as far apart numerically even though they are adjacent in the calendar.
# MAGIC
# MAGIC To represent this seasonal cycle more naturally, two additional features are created:
# MAGIC
# MAGIC - `target_month_sin`
# MAGIC - `target_month_cos`
# MAGIC
# MAGIC Together, these features allow forecasting models to recognize recurring annual seasonality.
# MAGIC
# MAGIC ### Timing Rule
# MAGIC
# MAGIC All calendar features describe the target month itself.
# MAGIC
# MAGIC This is permitted because calendar information is known before the forecast is generated.

# COMMAND ----------

# =========================================================
# CREATE CALENDAR AND SEASONALITY FEATURES
# =========================================================

# Calendar information for the target month is known in advance,
# so these features can safely describe month t without leakage.

forecast_features = (
    forecast_features

    # Basic calendar components
    .withColumn(
        "target_year",
        F.year("ref_date")
    )
    .withColumn(
        "target_month",
        F.month("ref_date")
    )
    .withColumn(
        "target_quarter",
        F.quarter("ref_date")
    )

    # Cyclical month encoding preserves the relationship between
    # December and January instead of treating 12 and 1 as distant.
    .withColumn(
        "target_month_sin",
        F.sin(
            2 * F.lit(3.141592653589793)
            * F.month("ref_date")
            / F.lit(12)
        )
    )
    .withColumn(
        "target_month_cos",
        F.cos(
            2 * F.lit(3.141592653589793)
            * F.month("ref_date")
            / F.lit(12)
        )
    )
)


# ---------------------------------------------------------
# Validate calendar features
# ---------------------------------------------------------

print(
    f"Forecast feature rows: "
    f"{forecast_features.count():,}"
)

print(
    f"Forecast feature columns after calendar features: "
    f"{len(forecast_features.columns):,}"
)

display(
    forecast_features
    .select(
        "ref_date",
        "target_year",
        "target_month",
        "target_quarter",
        "target_month_sin",
        "target_month_cos"
    )
    .orderBy(F.col("ref_date").desc())
    .limit(15)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Create Historical Growth and Momentum Features
# MAGIC
# MAGIC Growth features allow forecasting models to understand whether economic conditions were improving, weakening, or remaining stable before the target month.
# MAGIC
# MAGIC ### Growth Features
# MAGIC
# MAGIC The forecasting table includes historical measures such as:
# MAGIC
# MAGIC - Retail sales year-over-year growth
# MAGIC - CPI year-over-year inflation
# MAGIC - Employment month-over-month growth
# MAGIC - Real GDP month-over-month growth
# MAGIC - Real GDP year-over-year growth
# MAGIC
# MAGIC ### Leakage-Prevention Rule
# MAGIC
# MAGIC The Economic Overview already contains these derived growth indicators.
# MAGIC
# MAGIC However, the same-month values are not used directly as forecasting predictors.
# MAGIC
# MAGIC Instead, each growth measure is shifted backward so that the feature for target month **t** contains only growth information from **t-1 or earlier**.
# MAGIC
# MAGIC ### Modeling Purpose
# MAGIC
# MAGIC These features help forecasting models capture economic momentum without exposing the target month's outcome.

# COMMAND ----------

# =========================================================
# CREATE HISTORICAL GROWTH AND MOMENTUM FEATURES
# =========================================================

# Growth indicators already exist in the Economic Overview Gold mart.
# They are lagged here before being exposed as forecasting predictors
# so that same-month economic information is never used.

GROWTH_FEATURE_COLUMNS = [
    "retail_sales_mom_percent",
    "retail_sales_yoy_percent",
    "cpi_yoy_inflation_percent",
    "employment_mom_percent",
    "real_gdp_mom_percent",
    "real_gdp_yoy_percent"
]


# ---------------------------------------------------------
# Bring historical growth measures from the source mart
# ---------------------------------------------------------

growth_source = (
    gold_economic_overview
    .select(
        "ref_date",
        *GROWTH_FEATURE_COLUMNS
    )
)


# Join the source growth measures to the forecasting table.
forecast_features = (
    forecast_features
    .join(
        growth_source,
        on="ref_date",
        how="left"
    )
)


# ---------------------------------------------------------
# Create leakage-safe lagged growth features
# ---------------------------------------------------------

for column_name in GROWTH_FEATURE_COLUMNS:

    forecast_features = (
        forecast_features
        .withColumn(
            f"{column_name}_lag_1m",
            F.lag(
                F.col(column_name),
                1
            ).over(forecast_window)
        )
    )


# ---------------------------------------------------------
# Remove same-month growth measures
# ---------------------------------------------------------

# Same-month growth indicators are removed after creating their lagged
# versions so they cannot accidentally be used as predictors.
forecast_features = (
    forecast_features
    .drop(*GROWTH_FEATURE_COLUMNS)
)


# ---------------------------------------------------------
# Validate historical growth features
# ---------------------------------------------------------

print(
    f"Forecast feature rows: "
    f"{forecast_features.count():,}"
)

print(
    f"Forecast feature columns after growth features: "
    f"{len(forecast_features.columns):,}"
)

print("\nHistorical growth features:")

for column_name in forecast_features.columns:
    if (
        "_percent_lag_1m" in column_name
        and column_name not in [
            "unemployment_rate_percent_lag_1m"
        ]
    ):
        print(f" - {column_name}")


display(
    forecast_features
    .select(
        "ref_date",
        "retail_sales_mom_percent_lag_1m",
        "retail_sales_yoy_percent_lag_1m",
        "cpi_yoy_inflation_percent_lag_1m",
        "employment_mom_percent_lag_1m",
        "real_gdp_mom_percent_lag_1m",
        "real_gdp_yoy_percent_lag_1m"
    )
    .orderBy(F.col("ref_date").desc())
    .limit(15)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Create Model-Specific Feature Availability Flags
# MAGIC
# MAGIC Different forecasting targets depend on different historical predictors and have different publication schedules.
# MAGIC
# MAGIC Rather than dropping incomplete rows globally, the feature table keeps all months and adds model-specific availability flags.
# MAGIC
# MAGIC ### Availability Flags
# MAGIC
# MAGIC The flags indicate whether the minimum required historical information is available for a forecasting task.
# MAGIC
# MAGIC Initial flags are created for:
# MAGIC
# MAGIC - Retail Sales forecasting
# MAGIC - CPI forecasting
# MAGIC - Real GDP forecasting
# MAGIC - Employment forecasting
# MAGIC
# MAGIC ### Design Principle
# MAGIC
# MAGIC A row may be valid for one forecasting model but not for another.
# MAGIC
# MAGIC For example, CPI may already be available for a recent month while GDP is still unpublished.
# MAGIC
# MAGIC These flags allow downstream training notebooks to filter only the rows that are valid for the selected model.

# COMMAND ----------

# =========================================================
# CREATE MODEL-SPECIFIC FEATURE AVAILABILITY FLAGS
# =========================================================

# These flags do not remove rows.
# They identify whether the minimum target and historical predictors
# required for each forecasting task are available.

forecast_features = (
    forecast_features

    # Retail Sales model readiness
    .withColumn(
        "is_retail_model_ready",
        (
            F.col("target_retail_sales_dollars").isNotNull()
            & F.col("retail_sales_dollars_lag_1m").isNotNull()
            & F.col("retail_sales_dollars_lag_12m").isNotNull()
            & F.col("cpi_all_items_lag_1m").isNotNull()
            & F.col("employment_thousands_lag_1m").isNotNull()
        )
    )

    # CPI model readiness
    .withColumn(
        "is_cpi_model_ready",
        (
            F.col("target_cpi_all_items").isNotNull()
            & F.col("cpi_all_items_lag_1m").isNotNull()
            & F.col("cpi_all_items_lag_12m").isNotNull()
            & F.col("policy_rate_lag_1m").isNotNull()
            & F.col("avg_monthly_fx_usd_cad_lag_1m").isNotNull()
        )
    )

    # Real GDP model readiness
    .withColumn(
        "is_gdp_model_ready",
        (
            F.col(
                "target_real_gdp_chained_2017_millions"
            ).isNotNull()
            & F.col(
                "real_gdp_chained_2017_millions_lag_1m"
            ).isNotNull()
            & F.col(
                "real_gdp_chained_2017_millions_lag_12m"
            ).isNotNull()
            & F.col("employment_thousands_lag_1m").isNotNull()
            & F.col("policy_rate_lag_1m").isNotNull()
        )
    )

    # Employment model readiness
    .withColumn(
        "is_employment_model_ready",
        (
            F.col("target_employment_thousands").isNotNull()
            & F.col("employment_thousands_lag_1m").isNotNull()
            & F.col("employment_thousands_lag_12m").isNotNull()
            & F.col(
                "unemployment_rate_percent_lag_1m"
            ).isNotNull()
            & F.col("cpi_all_items_lag_1m").isNotNull()
        )
    )
)


# ---------------------------------------------------------
# Validate model readiness flags
# ---------------------------------------------------------

print(
    f"Forecast feature rows: "
    f"{forecast_features.count():,}"
)

print(
    f"Forecast feature columns after readiness flags: "
    f"{len(forecast_features.columns):,}"
)

print("\nModel-ready row counts:")

forecast_features.agg(
    F.sum(
        F.col("is_retail_model_ready").cast("int")
    ).alias("retail_ready_rows"),

    F.sum(
        F.col("is_cpi_model_ready").cast("int")
    ).alias("cpi_ready_rows"),

    F.sum(
        F.col("is_gdp_model_ready").cast("int")
    ).alias("gdp_ready_rows"),

    F.sum(
        F.col("is_employment_model_ready").cast("int")
    ).alias("employment_ready_rows")
).show()


# ---------------------------------------------------------
# Inspect latest model readiness
# ---------------------------------------------------------

display(
    forecast_features
    .select(
        "ref_date",
        "is_retail_model_ready",
        "is_cpi_model_ready",
        "is_gdp_model_ready",
        "is_employment_model_ready"
    )
    .orderBy(F.col("ref_date").desc())
    .limit(15)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Validate and Persist the Forecasting Feature Mart
# MAGIC
# MAGIC The final forecasting feature table is validated before being written to the Gold layer.
# MAGIC
# MAGIC ### Final Validation Checks
# MAGIC
# MAGIC The notebook confirms:
# MAGIC
# MAGIC - One row per `ref_date`
# MAGIC - No duplicate monthly business keys
# MAGIC - All four forecast targets are present
# MAGIC - Model-readiness flags are present
# MAGIC - Calendar, lag, rolling, and growth features exist
# MAGIC - The expected monthly date range is preserved
# MAGIC
# MAGIC ### Output
# MAGIC
# MAGIC The final Gold table is written as Delta format to:
# MAGIC
# MAGIC `gold/forecasting_features/monthly/`
# MAGIC
# MAGIC ### Business Key
# MAGIC
# MAGIC `ref_date`
# MAGIC
# MAGIC ### Modeling Use
# MAGIC
# MAGIC Downstream modeling notebooks can filter this table using the model-specific readiness flags and select one `target_` column as the prediction outcome.
# MAGIC
# MAGIC The other `target_` columns must not be used as predictors.

# COMMAND ----------

# =========================================================
# FINAL VALIDATION AND GOLD WRITE
# =========================================================

# ---------------------------------------------------------
# Validate final row count and date coverage
# ---------------------------------------------------------

final_row_count = forecast_features.count()

final_date_range = (
    forecast_features
    .agg(
        F.min("ref_date").alias("min_date"),
        F.max("ref_date").alias("max_date")
    )
    .collect()[0]
)

print(f"Final forecast feature rows: {final_row_count:,}")
print(f"Final forecast feature columns: {len(forecast_features.columns):,}")
print(
    f"Date range: "
    f"{final_date_range['min_date']} "
    f"to {final_date_range['max_date']}"
)


# ---------------------------------------------------------
# Validate monthly business key
# ---------------------------------------------------------

duplicate_months = (
    forecast_features
    .groupBy("ref_date")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(f"Duplicate monthly business keys: {duplicate_months:,}")

assert duplicate_months == 0, (
    "Forecast feature table contains duplicate ref_date values."
)


# ---------------------------------------------------------
# Validate required target columns
# ---------------------------------------------------------

REQUIRED_TARGET_COLUMNS = [
    "target_retail_sales_dollars",
    "target_cpi_all_items",
    "target_real_gdp_chained_2017_millions",
    "target_employment_thousands"
]

missing_target_columns = [
    column_name
    for column_name in REQUIRED_TARGET_COLUMNS
    if column_name not in forecast_features.columns
]

assert len(missing_target_columns) == 0, (
    f"Missing forecast target columns: {missing_target_columns}"
)

print("Required forecast target columns validated.")


# ---------------------------------------------------------
# Validate model-readiness flags
# ---------------------------------------------------------

REQUIRED_READINESS_COLUMNS = [
    "is_retail_model_ready",
    "is_cpi_model_ready",
    "is_gdp_model_ready",
    "is_employment_model_ready"
]

missing_readiness_columns = [
    column_name
    for column_name in REQUIRED_READINESS_COLUMNS
    if column_name not in forecast_features.columns
]

assert len(missing_readiness_columns) == 0, (
    f"Missing readiness columns: {missing_readiness_columns}"
)

print("Model-readiness columns validated.")


# ---------------------------------------------------------
# Validate final expected structure
# ---------------------------------------------------------

# Confirm that feature engineering preserves the monthly grain
# and does not add or remove rows from the Economic Overview source.
source_row_count = gold_economic_overview.count()

assert final_row_count == source_row_count, (
    f"Forecast feature row count ({final_row_count}) does not match "
    f"Economic Overview source row count ({source_row_count})."
)

assert len(forecast_features.columns) == 68, (
    f"Expected 68 columns, found {len(forecast_features.columns)}."
)

print("Final forecasting feature structure validated.")


# ---------------------------------------------------------
# Write forecasting feature mart to Gold
# ---------------------------------------------------------

# Overwrite is intentional because this notebook rebuilds the Gold
# forecasting mart deterministically from validated upstream data.
(
    forecast_features
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(GOLD_FORECASTING_FEATURES_PATH)
)

print("\nGold Forecasting Features written successfully.")
print(f"Path: {GOLD_FORECASTING_FEATURES_PATH}")


# ---------------------------------------------------------
# Read back persisted Delta table
# ---------------------------------------------------------

persisted_forecast_features = (
    spark.read
    .format("delta")
    .load(GOLD_FORECASTING_FEATURES_PATH)
)

persisted_row_count = persisted_forecast_features.count()

persisted_duplicate_months = (
    persisted_forecast_features
    .groupBy("ref_date")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

persisted_date_range = (
    persisted_forecast_features
    .agg(
        F.min("ref_date").alias("min_date"),
        F.max("ref_date").alias("max_date")
    )
    .collect()[0]
)

print("\nRead-back validation:")
print(f"Rows: {persisted_row_count:,}")
print(f"Columns: {len(persisted_forecast_features.columns):,}")
print(
    f"Date range: "
    f"{persisted_date_range['min_date']} "
    f"to {persisted_date_range['max_date']}"
)
print(
    f"Duplicate monthly business keys: "
    f"{persisted_duplicate_months:,}"
)


# ---------------------------------------------------------
# Final persistence assertions
# ---------------------------------------------------------

assert persisted_row_count == final_row_count, (
    "Persisted row count does not match the in-memory table."
)

assert len(persisted_forecast_features.columns) == 68, (
    "Persisted forecasting table does not contain 68 columns."
)

assert persisted_duplicate_months == 0, (
    "Persisted forecasting table contains duplicate ref_date values."
)

print(
    "\nAll persisted Gold forecasting feature validations passed."
)

# COMMAND ----------

# =========================================================
# FINAL GOLD DATA PREVIEW
# =========================================================

gold_forecasting_features_final = (
    spark.read
    .format("delta")
    .load(GOLD_FORECASTING_FEATURES_PATH)
)

display(
    gold_forecasting_features_final
    .orderBy(F.col("ref_date").desc())
    .limit(5)
)