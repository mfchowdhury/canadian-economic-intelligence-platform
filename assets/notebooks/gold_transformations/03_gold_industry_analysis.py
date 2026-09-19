# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Industry Analysis
# MAGIC
# MAGIC **Layer:** Silver → Gold  
# MAGIC **Notebook:** `03_gold_industry_analysis`
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC Create industry-level economic intelligence tables for analyzing changes in Canadian economic activity across industries.
# MAGIC
# MAGIC This notebook combines industry-focused measures while preserving their original analytical frequencies.
# MAGIC
# MAGIC ### Main Analytical Uses
# MAGIC
# MAGIC - Compare economic performance across industries
# MAGIC - Identify leading and lagging industries
# MAGIC - Analyze industry GDP trends and growth
# MAGIC - Analyze retail sales performance by retail industry
# MAGIC - Compare annual labour productivity across industries
# MAGIC - Support industry rankings and Power BI analysis
# MAGIC
# MAGIC ### Gold Output Strategy
# MAGIC
# MAGIC Because the source datasets have different frequencies and definitions, this notebook creates separate physical Gold tables rather than forcing all industry measures into one dataset:
# MAGIC
# MAGIC 1. **Monthly GDP by Industry**
# MAGIC 2. **Monthly Retail Sales by Industry**
# MAGIC 3. **Annual Labour Productivity by Industry**
# MAGIC
# MAGIC ### Grain Rules
# MAGIC
# MAGIC - GDP Industry → one row per month and industry
# MAGIC - Retail Industry → one row per month and retail industry
# MAGIC - Labour Productivity → one row per year and industry
# MAGIC
# MAGIC Each output preserves its native analytical grain and uses an explicit business key.

# COMMAND ----------

# =========================================================
# CONFIGURE GOLD INDUSTRY ANALYSIS
# =========================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Gold output paths are separated because the three
# industry datasets have different analytical grains.
GOLD_INDUSTRY_GDP_PATH = (
    "abfss://gold@stceidev01.dfs.core.windows.net/"
    "industry_analysis/gdp_monthly/"
)

GOLD_INDUSTRY_RETAIL_PATH = (
    "abfss://gold@stceidev01.dfs.core.windows.net/"
    "industry_analysis/retail_monthly/"
)

GOLD_INDUSTRY_PRODUCTIVITY_PATH = (
    "abfss://gold@stceidev01.dfs.core.windows.net/"
    "industry_analysis/productivity_annual/"
)

# Silver source paths
SILVER_GDP_PATH = (
    "abfss://silver@stceidev01.dfs.core.windows.net/"
    "statcan/gdp_industry/"
)

SILVER_RETAIL_PATH = (
    "abfss://silver@stceidev01.dfs.core.windows.net/"
    "statcan/retail_sales/"
)

SILVER_PRODUCTIVITY_PATH = (
    "abfss://silver@stceidev01.dfs.core.windows.net/"
    "statcan/labour_productivity/"
)

print("Gold Industry Analysis configuration loaded.")
print(f"GDP output: {GOLD_INDUSTRY_GDP_PATH}")
print(f"Retail output: {GOLD_INDUSTRY_RETAIL_PATH}")
print(f"Productivity output: {GOLD_INDUSTRY_PRODUCTIVITY_PATH}")

# COMMAND ----------

# =========================================================
# LOAD SILVER DATASETS FOR INDUSTRY ANALYSIS
# =========================================================

silver_gdp = (
    spark.read
    .format("delta")
    .load(SILVER_GDP_PATH)
)

silver_retail = (
    spark.read
    .format("delta")
    .load(SILVER_RETAIL_PATH)
)

silver_productivity = (
    spark.read
    .format("delta")
    .load(SILVER_PRODUCTIVITY_PATH)
)

print("Silver industry datasets loaded successfully.")
print(f"GDP rows: {silver_gdp.count():,}")
print(f"Retail rows: {silver_retail.count():,}")
print(f"Productivity rows: {silver_productivity.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Inspect Industry Dimensions
# MAGIC
# MAGIC Before selecting the canonical Gold series, inspect the available industry categories and supporting dimensions in each Silver dataset.
# MAGIC
# MAGIC This ensures that the Gold filters are based on the actual source values rather than assumptions.

# COMMAND ----------

# =========================================================
# INSPECT INDUSTRY DIMENSIONS
# =========================================================

print("GDP industries:")
silver_gdp.select("industry").distinct().orderBy("industry").show(100, truncate=False)

print("\nGDP seasonal adjustment values:")
silver_gdp.select("seasonal_adjustment").distinct().orderBy("seasonal_adjustment").show(truncate=False)

print("\nGDP price measures:")
silver_gdp.select("prices").distinct().orderBy("prices").show(truncate=False)

print("\nRetail industries:")
silver_retail.select("industry").distinct().orderBy("industry").show(100, truncate=False)

print("\nRetail sales types:")
silver_retail.select("sales_type").distinct().orderBy("sales_type").show(truncate=False)

print("\nRetail adjustment types:")
silver_retail.select("adjustment_type").distinct().orderBy("adjustment_type").show(truncate=False)

print("\nProductivity industries:")
silver_productivity.select("industry").distinct().orderBy("industry").show(100, truncate=False)

print("\nProductivity measures:")
silver_productivity.select("productivity_measure").distinct().orderBy("productivity_measure").show(truncate=False)

print("\nProductivity units:")
silver_productivity.select("uom").distinct().orderBy("uom").show(truncate=False)

print("\nProductivity scalar factors:")
silver_productivity.select("scalar_factor").distinct().orderBy("scalar_factor").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Prepare Monthly GDP by Industry
# MAGIC
# MAGIC Create a monthly industry-level GDP table for Canada using real GDP measured in chained 2017 dollars.
# MAGIC
# MAGIC ### Selection Rules
# MAGIC
# MAGIC - Geography → Canada
# MAGIC - Seasonal adjustment → Seasonally adjusted at annual rates
# MAGIC - Prices → Chained (2017) dollars
# MAGIC - Exclude aggregate `All industries [T001]` from industry comparisons
# MAGIC - Preserve the published industry classification from Statistics Canada
# MAGIC
# MAGIC ### Target Grain
# MAGIC
# MAGIC One row per month and industry.
# MAGIC
# MAGIC ### Business Key
# MAGIC
# MAGIC `ref_date + industry`
# MAGIC
# MAGIC GDP values remain in their published unit rather than being converted or rescaled in the Gold layer.

# COMMAND ----------

# =========================================================
# PREPARE MONTHLY GDP BY INDUSTRY
# =========================================================

gold_industry_gdp = (
    silver_gdp

    # Use the national series because this mart compares
    # industries rather than geographic regions.
    .filter(F.col("geo") == "Canada")

    # Select the canonical real GDP series used for
    # consistent month-to-month industry comparisons.
    .filter(
        (F.col("seasonal_adjustment") == "Seasonally adjusted at annual rates") &
        (F.col("prices") == "Chained (2017) dollars")
    )

    # All industries is an economy-wide aggregate rather than
    # an individual industry, so exclude it from this industry mart.
    .filter(F.col("industry") != "All industries [T001]")

    .select(
        "ref_date",
        "industry",
        F.col("value").alias("real_gdp_chained_2017_millions")
    )
)

# Validate the intended business key: ref_date + industry
gdp_duplicate_keys = (
    gold_industry_gdp
    .groupBy("ref_date", "industry")
    .count()
    .filter(F.col("count") > 1)
)

print(f"GDP industry rows: {gold_industry_gdp.count():,}")

gold_industry_gdp.agg(
    F.min("ref_date").alias("min_date"),
    F.max("ref_date").alias("max_date")
).show()

print("Duplicate business keys:")
gdp_duplicate_keys.show()

print("Number of industries:")
print(gold_industry_gdp.select("industry").distinct().count())

# COMMAND ----------

# =========================================================
# ADD GDP INDUSTRY GROWTH METRICS
# =========================================================

# Calculate growth separately within each industry so that
# lagged values never cross from one industry into another.
gdp_industry_window = (
    Window
    .partitionBy("industry")
    .orderBy("ref_date")
)

gold_industry_gdp_enriched = (
    gold_industry_gdp

    # Month-over-month real GDP growth
    .withColumn(
        "real_gdp_mom_percent",
        F.when(
            F.lag("real_gdp_chained_2017_millions", 1)
            .over(gdp_industry_window) != 0,
            (
                (
                    F.col("real_gdp_chained_2017_millions")
                    / F.lag("real_gdp_chained_2017_millions", 1)
                    .over(gdp_industry_window)
                ) - 1
            ) * 100
        )
    )

    # Year-over-year real GDP growth using the same month
    # from the previous year.
    .withColumn(
        "real_gdp_yoy_percent",
        F.when(
            F.lag("real_gdp_chained_2017_millions", 12)
            .over(gdp_industry_window) != 0,
            (
                (
                    F.col("real_gdp_chained_2017_millions")
                    / F.lag("real_gdp_chained_2017_millions", 12)
                    .over(gdp_industry_window)
                ) - 1
            ) * 100
        )
    )

    # Round analytical measures while keeping the underlying
    # published GDP value unchanged.
    .withColumn(
        "real_gdp_mom_percent",
        F.round(F.col("real_gdp_mom_percent"), 4)
    )
    .withColumn(
        "real_gdp_yoy_percent",
        F.round(F.col("real_gdp_yoy_percent"), 4)
    )
)

display(
    gold_industry_gdp_enriched
    .select(
        "ref_date",
        "industry",
        "real_gdp_chained_2017_millions",
        "real_gdp_mom_percent",
        "real_gdp_yoy_percent"
    )
    .orderBy(
        F.col("ref_date").desc(),
        "industry"
    )
    .limit(30)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Prepare Monthly Retail Sales by Industry
# MAGIC
# MAGIC Create a monthly industry-level retail sales table for Canada using total seasonally adjusted retail sales.
# MAGIC
# MAGIC ### Selection Rules
# MAGIC
# MAGIC - Geography → Canada
# MAGIC - Sales type → Total retail sales
# MAGIC - Adjustment type → Seasonally adjusted
# MAGIC - Exclude the aggregate `Retail trade [44-45]` from industry comparisons
# MAGIC - Preserve the published retail industry classification from Statistics Canada
# MAGIC
# MAGIC ### Target Grain
# MAGIC
# MAGIC One row per month and retail industry.
# MAGIC
# MAGIC ### Business Key
# MAGIC
# MAGIC `ref_date + industry`
# MAGIC
# MAGIC Retail sales remain in dollars as standardized in the Silver layer. Growth indicators are calculated independently within each retail industry.

# COMMAND ----------

# =========================================================
# PREPARE MONTHLY RETAIL SALES BY INDUSTRY
# =========================================================

gold_industry_retail = (
    silver_retail

    # Use Canada because this mart compares retail industries,
    # while geographic comparisons are handled in the regional mart.
    .filter(F.col("geo") == "Canada")

    # Use the headline seasonally adjusted total-sales series
    # for consistent month-to-month industry comparisons.
    .filter(
        (F.col("sales_type") == "Total retail sales") &
        (F.col("adjustment_type") == "Seasonally adjusted")
    )

    # Remove the overall retail-trade aggregate so this table
    # represents individual retail industries.
    .filter(F.col("industry") != "Retail trade [44-45]")

    .select(
        "ref_date",
        "industry",
        F.col("value_dollars").alias("retail_sales_dollars")
    )
)

# Validate the intended business key: ref_date + industry
retail_duplicate_keys = (
    gold_industry_retail
    .groupBy("ref_date", "industry")
    .count()
    .filter(F.col("count") > 1)
)

print(f"Retail industry rows: {gold_industry_retail.count():,}")

gold_industry_retail.agg(
    F.min("ref_date").alias("min_date"),
    F.max("ref_date").alias("max_date")
).show()

print("Duplicate business keys:")
retail_duplicate_keys.show()

print("Number of retail industries:")
print(
    gold_industry_retail
    .select("industry")
    .distinct()
    .count()
)

# COMMAND ----------

# =========================================================
# ADD RETAIL INDUSTRY GROWTH METRICS
# =========================================================

# Calculate growth independently within each retail industry
# so lagged values never cross between different industries.
retail_industry_window = (
    Window
    .partitionBy("industry")
    .orderBy("ref_date")
)

gold_industry_retail_enriched = (
    gold_industry_retail

    # Month-over-month retail sales growth
    .withColumn(
        "retail_sales_mom_percent",
        F.when(
            F.lag("retail_sales_dollars", 1)
            .over(retail_industry_window) != 0,
            (
                (
                    F.col("retail_sales_dollars")
                    / F.lag("retail_sales_dollars", 1)
                    .over(retail_industry_window)
                ) - 1
            ) * 100
        )
    )

    # Year-over-year retail sales growth using the same
    # month from the previous year.
    .withColumn(
        "retail_sales_yoy_percent",
        F.when(
            F.lag("retail_sales_dollars", 12)
            .over(retail_industry_window) != 0,
            (
                (
                    F.col("retail_sales_dollars")
                    / F.lag("retail_sales_dollars", 12)
                    .over(retail_industry_window)
                ) - 1
            ) * 100
        )
    )

    # Round derived analytical measures for clean Gold output.
    .withColumn(
        "retail_sales_mom_percent",
        F.round(F.col("retail_sales_mom_percent"), 4)
    )
    .withColumn(
        "retail_sales_yoy_percent",
        F.round(F.col("retail_sales_yoy_percent"), 4)
    )
)

display(
    gold_industry_retail_enriched
    .select(
        "ref_date",
        "industry",
        "retail_sales_dollars",
        "retail_sales_mom_percent",
        "retail_sales_yoy_percent"
    )
    .orderBy(
        F.col("ref_date").desc(),
        "industry"
    )
    .limit(30)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Prepare Annual Labour Productivity by Industry
# MAGIC
# MAGIC Create an annual industry-level labour productivity table for Canada.
# MAGIC
# MAGIC ### Selection Rules
# MAGIC
# MAGIC - Geography → Canada
# MAGIC - Productivity measure → Labour productivity
# MAGIC - Unit → Dollars
# MAGIC - Scalar factor → units
# MAGIC - Exclude `All industries [T001]` from industry comparisons
# MAGIC - Preserve the published industry classification from Statistics Canada
# MAGIC
# MAGIC ### Target Grain
# MAGIC
# MAGIC One row per year and industry.
# MAGIC
# MAGIC ### Business Key
# MAGIC
# MAGIC `ref_date + industry`
# MAGIC
# MAGIC Labour productivity remains at its native annual frequency and is not converted to monthly values.

# COMMAND ----------

# =========================================================
# PREPARE ANNUAL LABOUR PRODUCTIVITY BY INDUSTRY
# =========================================================

gold_industry_productivity = (
    silver_productivity

    # Use the national series because this mart compares
    # productivity across industries rather than regions.
    .filter(F.col("geo") == "Canada")

    # Select the canonical labour productivity measure.
    .filter(
        (F.col("productivity_measure") == "Labour productivity") &
        (F.col("uom") == "Dollars") &
        (F.col("scalar_factor") == "units")
    )

    # Remove the economy-wide aggregate so this table
    # represents individual industries.
    .filter(F.col("industry") != "All industries [T001]")

    .select(
        "ref_date",
        "industry",
        F.col("value").alias("labour_productivity_dollars")
    )
)

# Validate the intended business key: ref_date + industry
productivity_duplicate_keys = (
    gold_industry_productivity
    .groupBy("ref_date", "industry")
    .count()
    .filter(F.col("count") > 1)
)

print(f"Productivity industry rows: {gold_industry_productivity.count():,}")

gold_industry_productivity.agg(
    F.min("ref_date").alias("min_date"),
    F.max("ref_date").alias("max_date")
).show()

print("Duplicate business keys:")
productivity_duplicate_keys.show()

print("Number of productivity industries:")
print(
    gold_industry_productivity
    .select("industry")
    .distinct()
    .count()
)

# COMMAND ----------

# =========================================================
# ADD PRODUCTIVITY INDUSTRY GROWTH METRICS
# =========================================================

# Calculate year-over-year productivity growth independently
# within each industry.
productivity_industry_window = (
    Window
    .partitionBy("industry")
    .orderBy("ref_date")
)

gold_industry_productivity_enriched = (
    gold_industry_productivity

    # Annual productivity growth compared with the
    # previous published year for the same industry.
    .withColumn(
        "labour_productivity_yoy_percent",
        F.when(
            F.lag("labour_productivity_dollars", 1)
            .over(productivity_industry_window) != 0,
            (
                (
                    F.col("labour_productivity_dollars")
                    / F.lag("labour_productivity_dollars", 1)
                    .over(productivity_industry_window)
                ) - 1
            ) * 100
        )
    )

    # Round the derived growth metric for a cleaner Gold output.
    .withColumn(
        "labour_productivity_yoy_percent",
        F.round(
            F.col("labour_productivity_yoy_percent"),
            4
        )
    )
)

display(
    gold_industry_productivity_enriched
    .select(
        "ref_date",
        "industry",
        "labour_productivity_dollars",
        "labour_productivity_yoy_percent"
    )
    .orderBy(
        F.col("ref_date").desc(),
        "industry"
    )
    .limit(30)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validate Gold Industry Outputs
# MAGIC
# MAGIC Validate the three industry-level Gold tables before writing them to storage.
# MAGIC
# MAGIC ### Expected Business Keys
# MAGIC
# MAGIC - GDP Industry → `ref_date + industry`
# MAGIC - Retail Industry → `ref_date + industry`
# MAGIC - Productivity Industry → `ref_date + industry`
# MAGIC
# MAGIC The validation checks row counts, date ranges, and duplicate business keys for each output.

# COMMAND ----------

# =========================================================
# FINAL VALIDATION OF GOLD INDUSTRY OUTPUTS
# =========================================================

# ---------------------------------------------------------
# GDP INDUSTRY
# ---------------------------------------------------------

print("GDP industry rows:")
print(f"{gold_industry_gdp_enriched.count():,}")

gold_industry_gdp_enriched.agg(
    F.min("ref_date").alias("min_date"),
    F.max("ref_date").alias("max_date")
).show()

print("GDP duplicate business keys:")
(
    gold_industry_gdp_enriched
    .groupBy("ref_date", "industry")
    .count()
    .filter(F.col("count") > 1)
    .show()
)

# ---------------------------------------------------------
# RETAIL INDUSTRY
# ---------------------------------------------------------

print("\nRetail industry rows:")
print(f"{gold_industry_retail_enriched.count():,}")

gold_industry_retail_enriched.agg(
    F.min("ref_date").alias("min_date"),
    F.max("ref_date").alias("max_date")
).show()

print("Retail duplicate business keys:")
(
    gold_industry_retail_enriched
    .groupBy("ref_date", "industry")
    .count()
    .filter(F.col("count") > 1)
    .show()
)

# ---------------------------------------------------------
# PRODUCTIVITY INDUSTRY
# ---------------------------------------------------------

print("\nProductivity industry rows:")
print(f"{gold_industry_productivity_enriched.count():,}")

gold_industry_productivity_enriched.agg(
    F.min("ref_date").alias("min_date"),
    F.max("ref_date").alias("max_date")
).show()

print("Productivity duplicate business keys:")
(
    gold_industry_productivity_enriched
    .groupBy("ref_date", "industry")
    .count()
    .filter(F.col("count") > 1)
    .show()
)

# COMMAND ----------

# =========================================================
# WRITE GOLD INDUSTRY OUTPUTS TO DELTA
# =========================================================

# Write monthly GDP-by-industry analytical table.
(
    gold_industry_gdp_enriched
    .write
    .format("delta")
    .mode("overwrite")
    .save(GOLD_INDUSTRY_GDP_PATH)
)

print("GDP industry Gold table written successfully.")
print(f"Path: {GOLD_INDUSTRY_GDP_PATH}")


# Write monthly retail-sales-by-industry analytical table.
(
    gold_industry_retail_enriched
    .write
    .format("delta")
    .mode("overwrite")
    .save(GOLD_INDUSTRY_RETAIL_PATH)
)

print("\nRetail industry Gold table written successfully.")
print(f"Path: {GOLD_INDUSTRY_RETAIL_PATH}")


# Write annual labour-productivity-by-industry analytical table.
# Productivity stays annual rather than being artificially converted
# to monthly frequency.
(
    gold_industry_productivity_enriched
    .write
    .format("delta")
    .mode("overwrite")
    .save(GOLD_INDUSTRY_PRODUCTIVITY_PATH)
)

print("\nProductivity industry Gold table written successfully.")
print(f"Path: {GOLD_INDUSTRY_PRODUCTIVITY_PATH}")

# COMMAND ----------

# =========================================================
# READ BACK AND VALIDATE GOLD INDUSTRY OUTPUTS
# =========================================================

gdp_industry_check = (
    spark.read
    .format("delta")
    .load(GOLD_INDUSTRY_GDP_PATH)
)

retail_industry_check = (
    spark.read
    .format("delta")
    .load(GOLD_INDUSTRY_RETAIL_PATH)
)

productivity_industry_check = (
    spark.read
    .format("delta")
    .load(GOLD_INDUSTRY_PRODUCTIVITY_PATH)
)

# ---------------------------------------------------------
# GDP INDUSTRY VALIDATION
# ---------------------------------------------------------

print("GDP industry Gold rows:")
print(f"{gdp_industry_check.count():,}")

gdp_industry_check.agg(
    F.min("ref_date").alias("min_date"),
    F.max("ref_date").alias("max_date")
).show()

print("GDP duplicate business keys:")
(
    gdp_industry_check
    .groupBy("ref_date", "industry")
    .count()
    .filter(F.col("count") > 1)
    .show()
)

# ---------------------------------------------------------
# RETAIL INDUSTRY VALIDATION
# ---------------------------------------------------------

print("\nRetail industry Gold rows:")
print(f"{retail_industry_check.count():,}")

retail_industry_check.agg(
    F.min("ref_date").alias("min_date"),
    F.max("ref_date").alias("max_date")
).show()

print("Retail duplicate business keys:")
(
    retail_industry_check
    .groupBy("ref_date", "industry")
    .count()
    .filter(F.col("count") > 1)
    .show()
)

# ---------------------------------------------------------
# PRODUCTIVITY INDUSTRY VALIDATION
# ---------------------------------------------------------

print("\nProductivity industry Gold rows:")
print(f"{productivity_industry_check.count():,}")

productivity_industry_check.agg(
    F.min("ref_date").alias("min_date"),
    F.max("ref_date").alias("max_date")
).show()

print("Productivity duplicate business keys:")
(
    productivity_industry_check
    .groupBy("ref_date", "industry")
    .count()
    .filter(F.col("count") > 1)
    .show()
)

# COMMAND ----------

# =========================================================
# FINAL GOLD DATA PREVIEW
# =========================================================
# Purpose:
# Read the persisted industry Gold Delta tables after all
# processing and validation are complete.
#
# This cell is only for final visual verification and does
# not modify the datasets.

from pyspark.sql import functions as F

gdp_industry_final = (
    spark.read
    .format("delta")
    .load(
        "abfss://gold@stceidev01.dfs.core.windows.net/"
        "industry_analysis/gdp_monthly/"
    )
)

retail_industry_final = (
    spark.read
    .format("delta")
    .load(
        "abfss://gold@stceidev01.dfs.core.windows.net/"
        "industry_analysis/retail_monthly/"
    )
)

productivity_industry_final = (
    spark.read
    .format("delta")
    .load(
        "abfss://gold@stceidev01.dfs.core.windows.net/"
        "industry_analysis/productivity_annual/"
    )
)

print("GDP Industry Gold — Final Preview")
display(
    gdp_industry_final
    .orderBy(F.col("ref_date").desc(), "industry")
    .limit(5)
)

print("Retail Industry Gold — Final Preview")
display(
    retail_industry_final
    .orderBy(F.col("ref_date").desc(), "industry")
    .limit(5)
)

print("Productivity Industry Gold — Final Preview")
display(
    productivity_industry_final
    .orderBy(F.col("ref_date").desc(), "industry")
    .limit(5)
)