# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Economic Overview
# MAGIC
# MAGIC **Layer:** Silver → Gold  
# MAGIC **Notebook:** `01_gold_economic_overview`
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC Create the main monthly national economic-intelligence mart for the Canadian Economic Intelligence Platform.
# MAGIC
# MAGIC This Gold dataset integrates selected national indicators from the Silver layer while preserving clear frequency and alignment rules.
# MAGIC
# MAGIC ### Target Grain
# MAGIC
# MAGIC One row per month.
# MAGIC
# MAGIC ### Source Alignment Rules
# MAGIC
# MAGIC - Retail Sales → monthly observation
# MAGIC - CPI → monthly observation
# MAGIC - Labour Force → monthly observation
# MAGIC - GDP → monthly observation
# MAGIC - Housing Starts → monthly SAAR observation
# MAGIC - USD/CAD → monthly average of daily observations
# MAGIC - Policy Rate → last available observation in each month
# MAGIC - Population → latest published quarterly observation carried forward until the next observation
# MAGIC
# MAGIC ### Gold Design Principles
# MAGIC
# MAGIC - Do not manufacture observations that were not published.
# MAGIC - Preserve source frequency and alignment metadata where appropriate.
# MAGIC - Use carry-forward rather than interpolation for population.
# MAGIC - Keep housing starts explicitly identified as SAAR.
# MAGIC - Derived and aligned values must remain distinguishable from directly observed values.

# COMMAND ----------

# =========================================================
# 1. CONFIGURATION
# =========================================================

GOLD_PATH = (
    "abfss://gold@stceidev01.dfs.core.windows.net/"
    "economic_overview/"
)

SILVER_RETAIL_PATH = (
    "abfss://silver@stceidev01.dfs.core.windows.net/"
    "statcan/retail_sales/"
)

SILVER_CPI_PATH = (
    "abfss://silver@stceidev01.dfs.core.windows.net/"
    "statcan/cpi/"
)

SILVER_LABOUR_PATH = (
    "abfss://silver@stceidev01.dfs.core.windows.net/"
    "statcan/labour_force/"
)

SILVER_GDP_PATH = (
    "abfss://silver@stceidev01.dfs.core.windows.net/"
    "statcan/gdp_industry/"
)

SILVER_POPULATION_PATH = (
    "abfss://silver@stceidev01.dfs.core.windows.net/"
    "statcan/population/"
)

SILVER_FX_PATH = (
    "abfss://silver@stceidev01.dfs.core.windows.net/"
    "bank_of_canada/exchange_rates/"
)

SILVER_POLICY_RATE_PATH = (
    "abfss://silver@stceidev01.dfs.core.windows.net/"
    "bank_of_canada/policy_rate/"
)

# Housing Starts is sourced from CMHC but distributed through
# Statistics Canada and stored under the StatCan Silver structure.
SILVER_HOUSING_PATH = (
    "abfss://silver@stceidev01.dfs.core.windows.net/"
    "statcan/housing_starts/"
)

print("Gold Economic Overview configuration loaded.")
print(f"Gold output: {GOLD_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Silver Source Tables
# MAGIC
# MAGIC Load the Silver Delta datasets required for the national economic overview.
# MAGIC
# MAGIC At this stage, the datasets are only loaded and inspected. No joins or frequency alignment are performed yet.
# MAGIC
# MAGIC The goal is to confirm:
# MAGIC
# MAGIC - available columns,
# MAGIC - data types,
# MAGIC - row counts,
# MAGIC - date ranges,
# MAGIC - candidate national measures required for the Gold mart.

# COMMAND ----------

# =========================================================
# 2. LOAD SILVER SOURCE TABLES
# =========================================================

silver_retail = (
    spark.read
    .format("delta")
    .load(SILVER_RETAIL_PATH)
)

silver_cpi = (
    spark.read
    .format("delta")
    .load(SILVER_CPI_PATH)
)

silver_labour = (
    spark.read
    .format("delta")
    .load(SILVER_LABOUR_PATH)
)

silver_gdp = (
    spark.read
    .format("delta")
    .load(SILVER_GDP_PATH)
)

silver_population = (
    spark.read
    .format("delta")
    .load(SILVER_POPULATION_PATH)
)

silver_fx = (
    spark.read
    .format("delta")
    .load(SILVER_FX_PATH)
)

silver_policy_rate = (
    spark.read
    .format("delta")
    .load(SILVER_POLICY_RATE_PATH)
)

silver_housing = (
    spark.read
    .format("delta")
    .load(SILVER_HOUSING_PATH)
)

print("All Silver source tables loaded successfully.")

# COMMAND ----------

# =========================================================
# INSPECT SILVER SOURCE SCHEMAS AND ROW COUNTS
# =========================================================

silver_sources = {
    "Retail Sales": silver_retail,
    "CPI": silver_cpi,
    "Labour Force": silver_labour,
    "GDP by Industry": silver_gdp,
    "Population": silver_population,
    "USD/CAD": silver_fx,
    "Policy Rate": silver_policy_rate,
    "Housing Starts": silver_housing
}

for source_name, df in silver_sources.items():

    print("\n" + "=" * 90)
    print(source_name)
    print("=" * 90)

    print(f"Rows: {df.count():,}")

    print("Columns:")
    for column_name in df.columns:
        print(f" - {column_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Select National Retail Sales Series
# MAGIC
# MAGIC The Retail Sales Silver table contains multiple geographies, industries, sales types and adjustment types.
# MAGIC
# MAGIC For the national Gold economic overview, identify the canonical Canada-level retail-sales series before any cross-source integration is performed.

# COMMAND ----------

# =========================================================
# INSPECT RETAIL SALES DIMENSIONS
# =========================================================

from pyspark.sql import functions as F

print("Retail geographies:")
silver_retail.select("geo").distinct().orderBy("geo").show(50, truncate=False)

print("\nRetail sales types:")
silver_retail.select("sales_type").distinct().orderBy("sales_type").show(50, truncate=False)

print("\nRetail adjustment types:")
silver_retail.select("adjustment_type").distinct().orderBy("adjustment_type").show(50, truncate=False)

print("\nRetail industries:")
silver_retail.select("industry").distinct().orderBy("industry").show(100, truncate=False)

# COMMAND ----------

# =========================================================
# SELECT NATIONAL RETAIL SALES SERIES
# =========================================================

gold_retail = (
    silver_retail
    .filter(F.col("geo") == "Canada")
    .filter(F.col("sales_type") == "Total retail sales")
    .filter(F.col("adjustment_type") == "Seasonally adjusted")
    .filter(F.col("industry") == "Retail trade [44-45]")
    .select(
        "ref_date",
        F.col("value_dollars").alias("retail_sales_dollars")
    )
    .orderBy("ref_date")
)

print(
    f"National retail observations: "
    f"{gold_retail.count():,}"
)

gold_retail.agg(
    F.min("ref_date").alias("min_date"),
    F.max("ref_date").alias("max_date")
).show()

print("Duplicate months:")

(
    gold_retail
    .groupBy("ref_date")
    .count()
    .filter(F.col("count") > 1)
    .show()
)

display(gold_retail)

# COMMAND ----------

# =========================================================
# INSPECT NATIONAL CPI SERIES
# =========================================================

print("Canada CPI product groups:")

(
    silver_cpi
    .filter(F.col("geo") == "Canada")
    .select(
        "product_group",
        "uom"
    )
    .distinct()
    .orderBy(
        "product_group",
        "uom"
    )
    .show(
        100,
        truncate=False
    )
)

# COMMAND ----------

# =========================================================
# SELECT AND VALIDATE NATIONAL ALL-ITEMS CPI SERIES
# =========================================================

gold_cpi = (
    silver_cpi
    # Canonical national CPI series for the economic overview
    .filter(F.col("geo") == "Canada")
    .filter(F.col("product_group") == "All-items")
    .filter(F.col("uom") == "2002=100")
    .select(
        "ref_date",
        F.col("value").alias("cpi_all_items")
    )
    .orderBy("ref_date")
)

print(
    f"National CPI observations: "
    f"{gold_cpi.count():,}"
)

gold_cpi.agg(
    F.min("ref_date").alias("min_date"),
    F.max("ref_date").alias("max_date")
).show()

print("Duplicate months:")

(
    gold_cpi
    .groupBy("ref_date")
    .count()
    .filter(F.col("count") > 1)
    .show()
)

display(gold_cpi)

# COMMAND ----------

# =========================================================
# INSPECT NATIONAL LABOUR FORCE DIMENSIONS
# =========================================================

print("Labour force characteristics:")

(
    silver_labour
    .filter(F.col("geo") == "Canada")
    .select("labour_force_characteristic")
    .distinct()
    .orderBy("labour_force_characteristic")
    .show(50, truncate=False)
)

print("\nGenders:")

(
    silver_labour
    .filter(F.col("geo") == "Canada")
    .select("gender")
    .distinct()
    .orderBy("gender")
    .show(20, truncate=False)
)

print("\nAge groups:")

(
    silver_labour
    .filter(F.col("geo") == "Canada")
    .select("age_group")
    .distinct()
    .orderBy("age_group")
    .show(50, truncate=False)
)

print("\nStatistics:")

(
    silver_labour
    .filter(F.col("geo") == "Canada")
    .select("statistics")
    .distinct()
    .orderBy("statistics")
    .show(20, truncate=False)
)

print("\nData types:")

(
    silver_labour
    .filter(F.col("geo") == "Canada")
    .select("data_type")
    .distinct()
    .orderBy("data_type")
    .show(20, truncate=False)
)

# COMMAND ----------

# =========================================================
# SELECT AND VALIDATE NATIONAL LABOUR MARKET SERIES
# =========================================================

gold_labour = (
    silver_labour
    # Canonical national headline labour-market series
    .filter(F.col("geo") == "Canada")
    .filter(F.col("gender") == "Total - Gender")
    .filter(F.col("age_group") == "15 years and over")
    .filter(F.col("statistics") == "Estimate")
    .filter(F.col("data_type") == "Seasonally adjusted")
    .filter(
        F.col("labour_force_characteristic").isin(
            "Employment",
            "Unemployment rate"
        )
    )
    .select(
        "ref_date",
        "labour_force_characteristic",
        "value"
    )
)

gold_labour = (
    gold_labour
    .groupBy("ref_date")
    .pivot(
        "labour_force_characteristic",
        [
            "Employment",
            "Unemployment rate"
        ]
    )
    .agg(F.first("value"))
    .withColumnRenamed(
        "Employment",
        "employment_thousands"
    )
    .withColumnRenamed(
        "Unemployment rate",
        "unemployment_rate_percent"
    )
    .orderBy("ref_date")
)

print(
    f"National labour observations: "
    f"{gold_labour.count():,}"
)

gold_labour.agg(
    F.min("ref_date").alias("min_date"),
    F.max("ref_date").alias("max_date")
).show()

print("Duplicate months:")

(
    gold_labour
    .groupBy("ref_date")
    .count()
    .filter(F.col("count") > 1)
    .show()
)

display(gold_labour)

# COMMAND ----------

# =========================================================
# INSPECT NATIONAL GDP DIMENSIONS
# =========================================================

print("Seasonal adjustment values:")

(
    silver_gdp
    .filter(F.col("geo") == "Canada")
    .select("seasonal_adjustment")
    .distinct()
    .orderBy("seasonal_adjustment")
    .show(20, truncate=False)
)

print("\nPrice concepts:")

(
    silver_gdp
    .filter(F.col("geo") == "Canada")
    .select("prices")
    .distinct()
    .orderBy("prices")
    .show(20, truncate=False)
)

print("\nCandidate GDP industries:")

(
    silver_gdp
    .filter(F.col("geo") == "Canada")
    .filter(
        F.col("industry").isin(
            "All industries [T001]",
            "Goods-producing industries [T002]",
            "Services-producing industries [T003]"
        )
    )
    .select("industry")
    .distinct()
    .orderBy("industry")
    .show(truncate=False)
)

# COMMAND ----------

# =========================================================
# SELECT AND VALIDATE NATIONAL GDP SERIES
# =========================================================

gold_gdp = (
    silver_gdp
    # Canonical headline real GDP series for the national overview
    .filter(F.col("geo") == "Canada")
    .filter(F.col("industry") == "All industries [T001]")
    .filter(
        F.col("seasonal_adjustment")
        == "Seasonally adjusted at annual rates"
    )
    .filter(F.col("prices") == "Chained (2017) dollars")
    .select(
        "ref_date",
        F.col("value").alias("real_gdp_chained_2017_millions")
    )
    .orderBy("ref_date")
)

print(
    f"National GDP observations: "
    f"{gold_gdp.count():,}"
)

gold_gdp.agg(
    F.min("ref_date").alias("min_date"),
    F.max("ref_date").alias("max_date")
).show()

print("Duplicate months:")

(
    gold_gdp
    .groupBy("ref_date")
    .count()
    .filter(F.col("count") > 1)
    .show()
)

display(gold_gdp)

# COMMAND ----------

# =========================================================
# INSPECT NATIONAL POPULATION SERIES
# =========================================================

gold_population_quarterly = (
    silver_population
    # Canonical national population series
    .filter(F.col("geo") == "Canada")
    .select(
        "ref_date",
        F.col("value").alias("population")
    )
    .orderBy("ref_date")
)

print(
    f"National population observations: "
    f"{gold_population_quarterly.count():,}"
)

gold_population_quarterly.agg(
    F.min("ref_date").alias("min_date"),
    F.max("ref_date").alias("max_date")
).show()

print("Duplicate dates:")

(
    gold_population_quarterly
    .groupBy("ref_date")
    .count()
    .filter(F.col("count") > 1)
    .show()
)

display(
    gold_population_quarterly
    .orderBy(F.col("ref_date").desc())
    .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Align Quarterly Population to Monthly Grain
# MAGIC
# MAGIC Population is published quarterly, while the Gold economic overview uses a monthly grain.
# MAGIC
# MAGIC The latest available population estimate is carried forward to subsequent months until a new quarterly observation becomes available.
# MAGIC
# MAGIC This is an as-of alignment, not interpolation. The Gold output preserves the original population source date and identifies whether the monthly value was carried forward.

# COMMAND ----------

# =========================================================
# ALIGN QUARTERLY POPULATION TO MONTHLY GRAIN
# =========================================================

# Build a monthly calendar covering the population series
population_months = (
    spark.sql("""
        SELECT explode(
            sequence(
                to_date('1946-01-01'),
                to_date('2026-07-01'),
                interval 1 month
            )
        ) AS ref_date
    """)
)

# As-of join: use the latest population observation
# available on or before each month
gold_population_monthly = (
    population_months.alias("m")
    .join(
        gold_population_quarterly.alias("p"),
        F.col("p.ref_date") <= F.col("m.ref_date"),
        "left"
    )
    .groupBy(
        F.col("m.ref_date").alias("ref_date")
    )
    .agg(
        F.max_by(
            F.struct(
                F.col("p.ref_date").alias("population_source_date"),
                F.col("p.population").alias("population")
            ),
            F.col("p.ref_date")
        ).alias("latest_population")
    )
    .select(
        "ref_date",
        F.col("latest_population.population")
        .alias("population"),
        F.col("latest_population.population_source_date")
        .alias("population_source_date")
    )
    .withColumn(
        "population_is_carried_forward",
        F.col("ref_date") != F.col("population_source_date")
    )
    .orderBy("ref_date")
)

print(
    f"Monthly population observations: "
    f"{gold_population_monthly.count():,}"
)

print("Latest aligned population rows:")

display(
    gold_population_monthly
    .orderBy(F.col("ref_date").desc())
    .limit(12)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Prepare Monthly USD/CAD Exchange Rate
# MAGIC
# MAGIC The Silver exchange-rate table already contains one monthly observation derived from daily Bank of Canada data.
# MAGIC
# MAGIC The Gold economic overview therefore uses the existing monthly average directly without further frequency conversion.

# COMMAND ----------

# =========================================================
# PREPARE MONTHLY USD/CAD EXCHANGE RATE
# =========================================================

gold_fx = (
    silver_fx
    .select(
        "ref_date",
        "avg_monthly_fx_usd_cad"
    )
    .orderBy("ref_date")
)

print(
    f"Monthly FX observations: "
    f"{gold_fx.count():,}"
)

gold_fx.agg(
    F.min("ref_date").alias("min_date"),
    F.max("ref_date").alias("max_date")
).show()

print("Duplicate months:")

(
    gold_fx
    .groupBy("ref_date")
    .count()
    .filter(F.col("count") > 1)
    .show()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Align Policy Rate to Monthly Grain
# MAGIC
# MAGIC The Bank of Canada policy-rate Silver table is stored at daily observation grain.
# MAGIC
# MAGIC For the monthly Gold economic overview, the policy rate is aligned using the **last available observation in each month**.
# MAGIC
# MAGIC This rule is deterministic and reproducible and avoids averaging a step-like policy variable.

# COMMAND ----------

# =========================================================
# ALIGN POLICY RATE TO MONTHLY GRAIN
# =========================================================

from pyspark.sql.window import Window

policy_monthly_base = (
    silver_policy_rate
    .withColumn(
        "month",
        F.trunc("ref_date", "month")
    )
)

policy_window = (
    Window
    .partitionBy("month")
    .orderBy(F.col("ref_date").desc())
)

gold_policy_rate = (
    policy_monthly_base
    .withColumn(
        "row_num",
        F.row_number().over(policy_window)
    )
    .filter(F.col("row_num") == 1)
    .select(
        F.col("month").alias("ref_date"),
        "policy_rate",
        F.col("ref_date").alias("policy_rate_source_date")
    )
    .orderBy("ref_date")
)

print(
    f"Monthly policy-rate observations: "
    f"{gold_policy_rate.count():,}"
)

gold_policy_rate.agg(
    F.min("ref_date").alias("min_date"),
    F.max("ref_date").alias("max_date")
).show()

print("Duplicate months:")

(
    gold_policy_rate
    .groupBy("ref_date")
    .count()
    .filter(F.col("count") > 1)
    .show()
)

display(
    gold_policy_rate
    .orderBy(F.col("ref_date").desc())
    .limit(12)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Prepare National Housing Starts
# MAGIC
# MAGIC The CMHC Silver table is already stored at monthly grain and contains seasonally adjusted annual rate (SAAR) observations by geography.
# MAGIC
# MAGIC For the national economic overview, use the `Canada Urban Centres` series and preserve the explicit SAAR measure name.

# COMMAND ----------

# =========================================================
# SELECT AND VALIDATE NATIONAL HOUSING STARTS SERIES
# =========================================================

# Select the national Housing Starts series from the standardized
# Statistics Canada / CMHC Silver dataset.
#
# housing_starts_saar is already standardized to actual SAAR units
# in Silver, so no additional unit conversion is required here.
gold_housing = (
    silver_housing
    .filter(F.col("geo") == "Canada")
    .select(
        "ref_date",
        "housing_starts_saar"
    )
    .orderBy("ref_date")
)

print(
    f"National housing observations: "
    f"{gold_housing.count():,}"
)

gold_housing.agg(
    F.min("ref_date").alias("min_date"),
    F.max("ref_date").alias("max_date")
).show()

print("Duplicate months:")

(
    gold_housing
    .groupBy("ref_date")
    .count()
    .filter(F.col("count") > 1)
    .show()
)

display(gold_housing)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Build the Monthly Economic Overview
# MAGIC
# MAGIC The prepared national indicators are integrated into a single monthly Gold mart.
# MAGIC
# MAGIC The Gold calendar is based on the available core economic time series rather than the shortest source history. Sources with shorter histories remain null before their first available observation.
# MAGIC
# MAGIC This preserves historical information while avoiding the creation of artificial observations.
# MAGIC
# MAGIC Population values use documented as-of carry-forward alignment, while policy-rate values represent the last available observation in each month.

# COMMAND ----------

# =========================================================
# BUILD MONTHLY GOLD ECONOMIC OVERVIEW
# =========================================================

# Retail provides the starting monthly calendar for the current
# economic overview because its history begins in January 2017.
gold_overview = (
    gold_retail

    .join(
        gold_cpi,
        on="ref_date",
        how="left"
    )

    .join(
        gold_labour,
        on="ref_date",
        how="left"
    )

    .join(
        gold_gdp,
        on="ref_date",
        how="left"
    )

    .join(
        gold_population_monthly,
        on="ref_date",
        how="left"
    )

    .join(
        gold_fx,
        on="ref_date",
        how="left"
    )

    .join(
        gold_policy_rate,
        on="ref_date",
        how="left"
    )

    .join(
        gold_housing,
        on="ref_date",
        how="left"
    )

    .orderBy("ref_date")
)

print(
    f"Gold overview rows: "
    f"{gold_overview.count():,}"
)

gold_overview.agg(
    F.min("ref_date").alias("min_date"),
    F.max("ref_date").alias("max_date")
).show()

print("Gold overview columns:")

for column_name in gold_overview.columns:
    print(f" - {column_name}")

display(
    gold_overview
    .orderBy(F.col("ref_date").desc())
    .limit(15)
)

# COMMAND ----------

# =========================================================
# VALIDATE GOLD OVERVIEW DATA COVERAGE
# =========================================================

from pyspark.sql import functions as F

value_columns = [
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

coverage_summary = gold_overview.select(
    *[
        F.sum(
            F.when(F.col(c).isNull(), 1).otherwise(0)
        ).alias(f"{c}_nulls")
        for c in value_columns
    ]
)

display(coverage_summary)

# COMMAND ----------

# =========================================================
# DISPLAY NULL COUNTS VERTICALLY
# =========================================================

for column_name in value_columns:
    null_count = (
        gold_overview
        .filter(F.col(column_name).isNull())
        .count()
    )

    print(f"{column_name}: {null_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Define the Final Monthly Calendar
# MAGIC
# MAGIC The final Gold economic overview uses a continuous monthly calendar beginning in January 2017.
# MAGIC
# MAGIC The calendar extends through the latest available month across the integrated indicators. Individual indicators may therefore contain null values in recent months when their publication schedules lag behind other sources.
# MAGIC
# MAGIC These nulls represent legitimate source availability differences and are not treated as data-quality failures.

# COMMAND ----------

# =========================================================
# CREATE FINAL GOLD MONTHLY CALENDAR
# =========================================================

# Determine the latest available reference month across the
# integrated Gold source series. This keeps the calendar dynamic
# as new source observations become available.
latest_gold_month = (
    gold_retail.select(F.max("ref_date").alias("max_date"))
    .union(gold_cpi.select(F.max("ref_date").alias("max_date")))
    .union(gold_labour.select(F.max("ref_date").alias("max_date")))
    .union(gold_gdp.select(F.max("ref_date").alias("max_date")))
    .union(gold_population_monthly.select(F.max("ref_date").alias("max_date")))
    .union(gold_fx.select(F.max("ref_date").alias("max_date")))
    .union(gold_policy_rate.select(F.max("ref_date").alias("max_date")))
    .union(gold_housing.select(F.max("ref_date").alias("max_date")))
    .agg(F.max("max_date").alias("latest_month"))
    .first()["latest_month"]
)

# Build a continuous monthly calendar from the start of the
# analytical period through the latest available source month.
gold_calendar = (
    spark.range(1)
    .select(
        F.explode(
            F.sequence(
                F.to_date(F.lit("2017-01-01")),
                F.to_date(F.lit(latest_gold_month)),
                F.expr("interval 1 month")
            )
        ).alias("ref_date")
    )
)

print(f"Latest available Gold month: {latest_gold_month}")

print(
    f"Gold calendar months: "
    f"{gold_calendar.count():,}"
)

gold_calendar.agg(
    F.min("ref_date").alias("min_date"),
    F.max("ref_date").alias("max_date")
).show()

# COMMAND ----------

# =========================================================
# REBUILD GOLD OVERVIEW USING FINAL MONTHLY CALENDAR
# =========================================================

gold_overview_final = (
    gold_calendar

    .join(gold_retail, on="ref_date", how="left")
    .join(gold_cpi, on="ref_date", how="left")
    .join(gold_labour, on="ref_date", how="left")
    .join(gold_gdp, on="ref_date", how="left")
    .join(gold_population_monthly, on="ref_date", how="left")
    .join(gold_fx, on="ref_date", how="left")
    .join(gold_policy_rate, on="ref_date", how="left")
    .join(gold_housing, on="ref_date", how="left")

    .orderBy("ref_date")
)

print(
    f"Final Gold overview rows: "
    f"{gold_overview_final.count():,}"
)

gold_overview_final.agg(
    F.min("ref_date").alias("min_date"),
    F.max("ref_date").alias("max_date")
).show()

display(
    gold_overview_final
    .orderBy(F.col("ref_date").desc())
    .limit(12)
)

# COMMAND ----------

# =========================================================
# VALIDATE FINAL GOLD OVERVIEW
# =========================================================

print("Duplicate months:")

(
    gold_overview_final
    .groupBy("ref_date")
    .count()
    .filter(F.col("count") > 1)
    .show()
)

print("\nNull counts:")

for column_name in value_columns:
    null_count = (
        gold_overview_final
        .filter(F.col(column_name).isNull())
        .count()
    )

    print(f"{column_name}: {null_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Create Derived Economic Indicators
# MAGIC
# MAGIC The Gold economic overview includes derived growth and inflation measures to support economic analysis.
# MAGIC
# MAGIC Derived indicators are calculated only from available observations. Missing source values remain null rather than being artificially filled.
# MAGIC
# MAGIC Key measures include:
# MAGIC
# MAGIC - Retail Sales month-over-month growth
# MAGIC - Retail Sales year-over-year growth
# MAGIC - CPI year-over-year inflation
# MAGIC - Employment month-over-month growth
# MAGIC - Real GDP month-over-month growth
# MAGIC - Real GDP year-over-year growth

# COMMAND ----------

# =========================================================
# CREATE DERIVED ECONOMIC INDICATORS
# =========================================================

from pyspark.sql.window import Window

# Single national monthly time series
monthly_window = (
    Window
    .partitionBy(F.lit(1))
    .orderBy("ref_date")
)

gold_overview_enriched = (
    gold_overview_final

    # Retail Sales growth
    .withColumn(
        "retail_sales_mom_percent",
        (
            (
                F.col("retail_sales_dollars") /
                F.lag("retail_sales_dollars", 1).over(monthly_window)
            ) - 1
        ) * 100
    )
    .withColumn(
        "retail_sales_yoy_percent",
        (
            (
                F.col("retail_sales_dollars") /
                F.lag("retail_sales_dollars", 12).over(monthly_window)
            ) - 1
        ) * 100
    )

    # CPI inflation
    .withColumn(
        "cpi_yoy_inflation_percent",
        (
            (
                F.col("cpi_all_items") /
                F.lag("cpi_all_items", 12).over(monthly_window)
            ) - 1
        ) * 100
    )

    # Employment growth
    .withColumn(
        "employment_mom_percent",
        (
            (
                F.col("employment_thousands") /
                F.lag("employment_thousands", 1).over(monthly_window)
            ) - 1
        ) * 100
    )

    # Real GDP growth
    .withColumn(
        "real_gdp_mom_percent",
        (
            (
                F.col("real_gdp_chained_2017_millions") /
                F.lag(
                    "real_gdp_chained_2017_millions",
                    1
                ).over(monthly_window)
            ) - 1
        ) * 100
    )
    .withColumn(
        "real_gdp_yoy_percent",
        (
            (
                F.col("real_gdp_chained_2017_millions") /
                F.lag(
                    "real_gdp_chained_2017_millions",
                    12
                ).over(monthly_window)
            ) - 1
        ) * 100
    )

    .orderBy("ref_date")
)

display(
    gold_overview_enriched
    .orderBy(F.col("ref_date").desc())
    .limit(15)
)

# COMMAND ----------

# =========================================================
# VALIDATE DERIVED ECONOMIC INDICATORS
# =========================================================

derived_columns = [
    "retail_sales_mom_percent",
    "retail_sales_yoy_percent",
    "cpi_yoy_inflation_percent",
    "employment_mom_percent",
    "real_gdp_mom_percent",
    "real_gdp_yoy_percent"
]

print("Derived indicator null counts:")

for column_name in derived_columns:
    null_count = (
        gold_overview_enriched
        .filter(F.col(column_name).isNull())
        .count()
    )

    print(f"{column_name}: {null_count}")

# COMMAND ----------

display(
    gold_overview_enriched
    .select(
        "ref_date",
        "retail_sales_mom_percent",
        "retail_sales_yoy_percent",
        "cpi_yoy_inflation_percent",
        "employment_mom_percent",
        "real_gdp_mom_percent",
        "real_gdp_yoy_percent"
    )
    .orderBy(F.col("ref_date").desc())
    .limit(15)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Write the Gold Economic Overview
# MAGIC
# MAGIC The final enriched monthly economic overview is written to the Gold layer in Delta format.
# MAGIC
# MAGIC The output preserves source-alignment fields, publication-lag nulls, and derived economic indicators.

# COMMAND ----------

# =========================================================
# WRITE GOLD ECONOMIC OVERVIEW TO DELTA
# =========================================================

(
    gold_overview_enriched
    .write
    .format("delta")
    .mode("overwrite")
    .save(GOLD_PATH)
)

print("Gold Economic Overview written successfully.")
print(f"Gold path: {GOLD_PATH}")

# COMMAND ----------

# =========================================================
# READ BACK AND VALIDATE GOLD ECONOMIC OVERVIEW
# =========================================================

gold_overview_check = (
    spark.read
    .format("delta")
    .load(GOLD_PATH)
)

print(
    f"Gold Delta rows: "
    f"{gold_overview_check.count():,}"
)

gold_overview_check.agg(
    F.min("ref_date").alias("min_date"),
    F.max("ref_date").alias("max_date")
).show()

print("Duplicate months:")

(
    gold_overview_check
    .groupBy("ref_date")
    .count()
    .filter(F.col("count") > 1)
    .show()
)

print("Gold Delta columns:")

for column_name in gold_overview_check.columns:
    print(f" - {column_name}")

display(
    gold_overview_check
    .orderBy(F.col("ref_date").desc())
    .limit(10)
)