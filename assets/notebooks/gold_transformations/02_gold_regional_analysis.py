# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Regional Analysis
# MAGIC
# MAGIC **Layer:** Silver → Gold  
# MAGIC **Notebook:** `02_gold_regional_analysis`
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC Create regional economic-analysis tables for comparing Canada, Ontario, and other major provinces across key indicators.
# MAGIC
# MAGIC This notebook focuses on provincial and national comparisons rather than a single national time series.
# MAGIC
# MAGIC ### Main Analytical Uses
# MAGIC
# MAGIC - Compare Ontario with Canada and other major provinces
# MAGIC - Measure regional gaps and relative performance
# MAGIC - Rank provinces by selected indicators
# MAGIC - Analyze regional trends over time
# MAGIC - Support per-capita and regional benchmarking
# MAGIC
# MAGIC ### Grain Strategy
# MAGIC
# MAGIC This notebook may create more than one Gold table when source frequencies differ.
# MAGIC
# MAGIC Monthly indicators remain in monthly regional tables, while annual indicators such as labour productivity remain in separate annual tables.
# MAGIC
# MAGIC Different frequencies are not forced into one physical table.

# COMMAND ----------

# =========================================================
# CONFIGURE GOLD REGIONAL ANALYSIS PATHS
# =========================================================

GOLD_REGIONAL_MONTHLY_PATH = (
    "abfss://gold@stceidev01.dfs.core.windows.net/"
    "regional_analysis/monthly/"
)

GOLD_REGIONAL_PRODUCTIVITY_PATH = (
    "abfss://gold@stceidev01.dfs.core.windows.net/"
    "regional_analysis/productivity_annual/"
)

# Silver source paths used for regional analysis
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

SILVER_POPULATION_PATH = (
    "abfss://silver@stceidev01.dfs.core.windows.net/"
    "statcan/population/"
)

SILVER_PRODUCTIVITY_PATH = (
    "abfss://silver@stceidev01.dfs.core.windows.net/"
    "statcan/labour_productivity/"
)

print("Gold Regional Analysis configuration loaded.")
print(f"Monthly output: {GOLD_REGIONAL_MONTHLY_PATH}")
print(f"Annual productivity output: {GOLD_REGIONAL_PRODUCTIVITY_PATH}")

# COMMAND ----------

# =========================================================
# LOAD SILVER DATASETS FOR REGIONAL ANALYSIS
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

silver_population = (
    spark.read
    .format("delta")
    .load(SILVER_POPULATION_PATH)
)

silver_productivity = (
    spark.read
    .format("delta")
    .load(SILVER_PRODUCTIVITY_PATH)
)

print("Silver datasets loaded successfully.")
print(f"Retail rows: {silver_retail.count():,}")
print(f"CPI rows: {silver_cpi.count():,}")
print(f"Labour rows: {silver_labour.count():,}")
print(f"Population rows: {silver_population.count():,}")
print(f"Productivity rows: {silver_productivity.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Define Regional Scope
# MAGIC
# MAGIC Before creating regional Gold tables, inspect the geographic coverage available across the Silver datasets.
# MAGIC
# MAGIC The regional analysis will prioritize geographies that are consistently available across the required indicators. This ensures that comparisons between Canada, Ontario, and other provinces are based on compatible geographic definitions.

# COMMAND ----------

# =========================================================
# INSPECT AVAILABLE GEOGRAPHIES
# =========================================================

print("Retail geographies:")
silver_retail.select("geo").distinct().orderBy("geo").show(100, truncate=False)

print("CPI geographies:")
silver_cpi.select("geo").distinct().orderBy("geo").show(100, truncate=False)

print("Labour geographies:")
silver_labour.select("geo").distinct().orderBy("geo").show(100, truncate=False)

print("Population geographies:")
silver_population.select("geo").distinct().orderBy("geo").show(100, truncate=False)

print("Productivity geographies:")
silver_productivity.select("geo").distinct().orderBy("geo").show(100, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Select Consistent Regional Geographies
# MAGIC
# MAGIC The regional Gold analysis uses Canada and the ten provinces that are consistently available across the core Silver datasets.
# MAGIC
# MAGIC Territories and metropolitan areas are excluded from the main regional mart because their coverage is not consistent across all required indicators.
# MAGIC
# MAGIC ### Included Geographies
# MAGIC
# MAGIC - Canada
# MAGIC - Newfoundland and Labrador
# MAGIC - Prince Edward Island
# MAGIC - Nova Scotia
# MAGIC - New Brunswick
# MAGIC - Quebec
# MAGIC - Ontario
# MAGIC - Manitoba
# MAGIC - Saskatchewan
# MAGIC - Alberta
# MAGIC - British Columbia
# MAGIC
# MAGIC This creates a consistent geographic basis for cross-indicator regional comparisons.

# COMMAND ----------

# =========================================================
# DEFINE CONSISTENT REGIONAL GEOGRAPHIES
# =========================================================

REGIONAL_GEOS = [
    "Canada",
    "Newfoundland and Labrador",
    "Prince Edward Island",
    "Nova Scotia",
    "New Brunswick",
    "Quebec",
    "Ontario",
    "Manitoba",
    "Saskatchewan",
    "Alberta",
    "British Columbia"
]

print(f"Regional geographies selected: {len(REGIONAL_GEOS)}")

for geo in REGIONAL_GEOS:
    print(f" - {geo}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Prepare Regional Retail Sales
# MAGIC
# MAGIC Retail Sales is filtered to the consistent regional geography set and the headline retail measure.
# MAGIC
# MAGIC The regional Gold series uses:
# MAGIC
# MAGIC - Total retail sales
# MAGIC - Seasonally adjusted values
# MAGIC - Retail trade `[44-45]`
# MAGIC - Canada and the ten provinces
# MAGIC
# MAGIC The resulting grain is one row per month and geography.
# MAGIC
# MAGIC **Business Key:** `ref_date + geo`

# COMMAND ----------

# =========================================================
# PREPARE REGIONAL RETAIL SALES
# =========================================================
from pyspark.sql import functions as F

gold_regional_retail = (
    silver_retail

    # Keep only geographies included in the regional comparison scope
    .filter(F.col("geo").isin(REGIONAL_GEOS))

    # Use the headline retail series for consistent comparison
    .filter(F.col("sales_type") == "Total retail sales")
    .filter(F.col("adjustment_type") == "Seasonally adjusted")
    .filter(F.col("industry") == "Retail trade [44-45]")

    .select(
        "ref_date",
        "geo",
        F.col("value_dollars").alias("retail_sales_dollars")
    )

    .orderBy("ref_date", "geo")
)

print(
    f"Regional retail observations: "
    f"{gold_regional_retail.count():,}"
)

gold_regional_retail.agg(
    F.min("ref_date").alias("min_date"),
    F.max("ref_date").alias("max_date")
).show()

print("Duplicate business keys:")

(
    gold_regional_retail
    .groupBy("ref_date", "geo")
    .count()
    .filter(F.col("count") > 1)
    .show()
)

print("Geographies in regional retail series:")

(
    gold_regional_retail
    .select("geo")
    .distinct()
    .orderBy("geo")
    .show(20, truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Prepare Regional CPI
# MAGIC
# MAGIC CPI is filtered to the headline All-items index for Canada and the ten provinces.
# MAGIC
# MAGIC The Gold regional CPI series uses:
# MAGIC
# MAGIC - All-items CPI
# MAGIC - `2002=100`
# MAGIC - Canada and the ten provinces
# MAGIC
# MAGIC The resulting grain is one row per month and geography.
# MAGIC
# MAGIC **Business Key:** `ref_date + geo`

# COMMAND ----------

# =========================================================
# PREPARE REGIONAL CPI
# =========================================================

gold_regional_cpi = (
    silver_cpi

    # Keep only the consistent regional comparison geographies
    .filter(F.col("geo").isin(REGIONAL_GEOS))

    # Use the headline All-items CPI series
    .filter(F.col("product_group") == "All-items")
    .filter(F.col("uom") == "2002=100")

    .select(
        "ref_date",
        "geo",
        F.col("value").alias("cpi_all_items")
    )

    .orderBy("ref_date", "geo")
)

print(
    f"Regional CPI observations: "
    f"{gold_regional_cpi.count():,}"
)

gold_regional_cpi.agg(
    F.min("ref_date").alias("min_date"),
    F.max("ref_date").alias("max_date")
).show()

print("Duplicate business keys:")

(
    gold_regional_cpi
    .groupBy("ref_date", "geo")
    .count()
    .filter(F.col("count") > 1)
    .show()
)

print("Geographies in regional CPI series:")

(
    gold_regional_cpi
    .select("geo")
    .distinct()
    .orderBy("geo")
    .show(20, truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Prepare Regional Labour Market Indicators
# MAGIC
# MAGIC The Labour Force Survey is filtered to the standard headline labour-market measures for Canada and the ten provinces.
# MAGIC
# MAGIC The regional Gold labour series uses:
# MAGIC
# MAGIC - Total - Gender
# MAGIC - 15 years and over
# MAGIC - Estimate
# MAGIC - Seasonally adjusted
# MAGIC - Employment
# MAGIC - Unemployment rate
# MAGIC
# MAGIC The two labour measures are reshaped into one row per month and geography.
# MAGIC
# MAGIC **Business Key:** `ref_date + geo`

# COMMAND ----------

# =========================================================
# PREPARE REGIONAL LABOUR MARKET INDICATORS
# =========================================================

regional_labour_filtered = (
    silver_labour

    # Keep only geographies included in the regional analysis scope
    .filter(F.col("geo").isin(REGIONAL_GEOS))

    # Use the standard headline labour-market population
    .filter(F.col("gender") == "Total - Gender")
    .filter(F.col("age_group") == "15 years and over")
    .filter(F.col("statistics") == "Estimate")
    .filter(F.col("data_type") == "Seasonally adjusted")

    # Keep the two headline measures needed for regional comparison
    .filter(
        F.col("labour_force_characteristic").isin(
            "Employment",
            "Unemployment rate"
        )
    )
)

gold_regional_labour = (
    regional_labour_filtered

    # Pivot the two labour measures into analytical columns
    .groupBy("ref_date", "geo")
    .pivot(
        "labour_force_characteristic",
        ["Employment", "Unemployment rate"]
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

    .orderBy("ref_date", "geo")
)

print(
    f"Regional labour observations: "
    f"{gold_regional_labour.count():,}"
)

gold_regional_labour.agg(
    F.min("ref_date").alias("min_date"),
    F.max("ref_date").alias("max_date")
).show()

print("Duplicate business keys:")

(
    gold_regional_labour
    .groupBy("ref_date", "geo")
    .count()
    .filter(F.col("count") > 1)
    .show()
)

print("Geographies in regional labour series:")

(
    gold_regional_labour
    .select("geo")
    .distinct()
    .orderBy("geo")
    .show(20, truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Prepare Regional Population
# MAGIC
# MAGIC Population estimates are published less frequently than the monthly economic indicators used in this regional mart.
# MAGIC
# MAGIC For monthly regional analysis, the latest available official population estimate is carried forward until a new observation becomes available.
# MAGIC
# MAGIC This is an **as-of alignment**, not interpolation.
# MAGIC
# MAGIC The Gold output preserves the original population source date and identifies whether the monthly value was carried forward.
# MAGIC
# MAGIC **Monthly Business Key:** `ref_date + geo`

# COMMAND ----------

# =========================================================
# PREPARE REGIONAL POPULATION SERIES
# =========================================================

regional_population_base = (
    silver_population

    # Keep only the consistent regional comparison geographies
    .filter(F.col("geo").isin(REGIONAL_GEOS))

    .select(
        "ref_date",
        "geo",
        F.col("value").alias("population")
    )
)

print(
    f"Regional population source observations: "
    f"{regional_population_base.count():,}"
)

regional_population_base.agg(
    F.min("ref_date").alias("min_date"),
    F.max("ref_date").alias("max_date")
).show()

print("Duplicate source business keys:")

(
    regional_population_base
    .groupBy("ref_date", "geo")
    .count()
    .filter(F.col("count") > 1)
    .show()
)

# COMMAND ----------

# =========================================================
# ALIGN REGIONAL POPULATION TO MONTHLY GRAIN
# =========================================================

# Determine the latest available month across the monthly
# regional source series so the calendar extends automatically.
latest_regional_month = (
    gold_regional_retail.select(F.max("ref_date").alias("max_date"))
    .union(
        gold_regional_cpi.select(
            F.max("ref_date").alias("max_date")
        )
    )
    .union(
        gold_regional_labour.select(
            F.max("ref_date").alias("max_date")
        )
    )
    .agg(F.max("max_date").alias("latest_month"))
    .first()["latest_month"]
)

# Build a monthly calendar from the start of the analytical
# period through the latest available regional source month.
regional_months = (
    spark.range(1)
    .select(
        F.explode(
            F.sequence(
                F.to_date(F.lit("2017-01-01")),
                F.to_date(F.lit(latest_regional_month)),
                F.expr("interval 1 month")
            )
        ).alias("ref_date")
    )
)

# Create every month-geography combination required.
regional_calendar = (
    regional_months
    .crossJoin(
        spark.createDataFrame(
            [(geo,) for geo in REGIONAL_GEOS],
            ["geo"]
        )
    )
)

gold_regional_population = (
    regional_calendar.alias("c")
    .join(
        regional_population_base.alias("p"),
        (
            (F.col("c.geo") == F.col("p.geo")) &
            (F.col("p.ref_date") <= F.col("c.ref_date"))
        ),
        "left"
    )

    # For each month and geography, keep the latest
    # official population observation available at that time.
    .groupBy(
        F.col("c.ref_date").alias("ref_date"),
        F.col("c.geo").alias("geo")
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
        "geo",
        F.col("latest_population.population")
            .alias("population"),
        F.col("latest_population.population_source_date")
            .alias("population_source_date")
    )

    # Flag months that use a carried-forward population estimate.
    .withColumn(
        "population_is_carried_forward",
        F.col("ref_date") != F.col("population_source_date")
    )

    .orderBy("ref_date", "geo")
)

print(f"Latest available regional month: {latest_regional_month}")

print(
    f"Monthly regional population observations: "
    f"{gold_regional_population.count():,}"
)

print("Duplicate monthly business keys:")

(
    gold_regional_population
    .groupBy("ref_date", "geo")
    .count()
    .filter(F.col("count") > 1)
    .show()
)

display(
    gold_regional_population
    .orderBy(F.col("ref_date").desc(), "geo")
    .limit(25)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Prepare Annual Regional Labour Productivity
# MAGIC
# MAGIC Labour productivity is published annually and is therefore kept in a separate Gold table rather than being forced into the monthly regional mart.
# MAGIC
# MAGIC The regional productivity table supports annual comparisons across Canada and the ten provinces.
# MAGIC
# MAGIC The canonical productivity series will be selected using a consistent productivity measure and industry definition.
# MAGIC
# MAGIC **Target Grain:** one row per year and geography  
# MAGIC **Business Key:** `ref_date + geo`

# COMMAND ----------

# =========================================================
# INSPECT PRODUCTIVITY DIMENSIONS
# =========================================================

print("Productivity measures:")
(
    silver_productivity
    .select("productivity_measure")
    .distinct()
    .orderBy("productivity_measure")
    .show(100, truncate=False)
)

print("Industries:")
(
    silver_productivity
    .select("industry")
    .distinct()
    .orderBy("industry")
    .show(100, truncate=False)
)

print("Units of measure:")
(
    silver_productivity
    .select("uom")
    .distinct()
    .orderBy("uom")
    .show(100, truncate=False)
)

print("Scalar factors:")
(
    silver_productivity
    .select("scalar_factor")
    .distinct()
    .orderBy("scalar_factor")
    .show(50, truncate=False)
)

# COMMAND ----------

# =========================================================
# PREPARE ANNUAL REGIONAL LABOUR PRODUCTIVITY
# =========================================================

gold_regional_productivity = (
    silver_productivity

    # Keep only the geographies used in the regional analysis
    .filter(F.col("geo").isin(REGIONAL_GEOS))

    # Use the headline labour productivity measure for all industries
    .filter(F.col("productivity_measure") == "Labour productivity")
    .filter(F.col("industry") == "All industries [T001]")
    .filter(F.col("uom") == "Dollars")
    .filter(F.col("scalar_factor") == "units")

    .select(
        "ref_date",
        "geo",
        F.col("value").alias("labour_productivity_dollars")
    )

    .orderBy("ref_date", "geo")
)

print(
    f"Regional productivity observations: "
    f"{gold_regional_productivity.count():,}"
)

gold_regional_productivity.agg(
    F.min("ref_date").alias("min_date"),
    F.max("ref_date").alias("max_date")
).show()

print("Duplicate business keys:")

(
    gold_regional_productivity
    .groupBy("ref_date", "geo")
    .count()
    .filter(F.col("count") > 1)
    .show()
)

print("Geographies in productivity series:")

(
    gold_regional_productivity
    .select("geo")
    .distinct()
    .orderBy("geo")
    .show(20, truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Build the Monthly Regional Economic Mart
# MAGIC
# MAGIC The monthly regional mart integrates the core indicators that share a monthly analytical grain.
# MAGIC
# MAGIC Included measures:
# MAGIC
# MAGIC - Retail Sales
# MAGIC - CPI
# MAGIC - Employment
# MAGIC - Unemployment Rate
# MAGIC - Population
# MAGIC
# MAGIC The mart uses a complete `month × geography` calendar from January 2017 through August 2026.
# MAGIC
# MAGIC Shorter source histories or recent publication lags remain as null values rather than being artificially filled.
# MAGIC
# MAGIC **Target Grain:** one row per month and geography  
# MAGIC **Business Key:** `ref_date + geo`

# COMMAND ----------

# =========================================================
# BUILD MONTHLY REGIONAL ECONOMIC MART
# =========================================================

gold_regional_monthly = (
    regional_calendar

    # Join each prepared regional indicator onto the complete
    # month-by-geography calendar to preserve source availability gaps
    .join(
        gold_regional_retail,
        on=["ref_date", "geo"],
        how="left"
    )
    .join(
        gold_regional_cpi,
        on=["ref_date", "geo"],
        how="left"
    )
    .join(
        gold_regional_labour,
        on=["ref_date", "geo"],
        how="left"
    )
    .join(
        gold_regional_population,
        on=["ref_date", "geo"],
        how="left"
    )

    .orderBy("ref_date", "geo")
)

print(
    f"Monthly regional Gold rows: "
    f"{gold_regional_monthly.count():,}"
)

gold_regional_monthly.agg(
    F.min("ref_date").alias("min_date"),
    F.max("ref_date").alias("max_date")
).show()

print("Duplicate business keys:")

(
    gold_regional_monthly
    .groupBy("ref_date", "geo")
    .count()
    .filter(F.col("count") > 1)
    .show()
)

print("Gold regional monthly columns:")

for column_name in gold_regional_monthly.columns:
    print(f" - {column_name}")

# COMMAND ----------

# =========================================================
# VALIDATE MONTHLY REGIONAL DATA COVERAGE
# =========================================================

regional_value_columns = [
    "retail_sales_dollars",
    "cpi_all_items",
    "employment_thousands",
    "unemployment_rate_percent",
    "population"
]

print("Regional monthly null counts:")

for column_name in regional_value_columns:
    null_count = (
        gold_regional_monthly
        .filter(F.col(column_name).isNull())
        .count()
    )

    print(f"{column_name}: {null_count}")

# COMMAND ----------

# Review recent regional rows to confirm publication-lag patterns
display(
    gold_regional_monthly
    .orderBy(
        F.col("ref_date").desc(),
        F.col("geo")
    )
    .limit(33)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Create Regional Growth Indicators
# MAGIC
# MAGIC Derived regional indicators are calculated independently within each geography.
# MAGIC
# MAGIC The calculations preserve the monthly regional grain and support comparisons of economic momentum across provinces.
# MAGIC
# MAGIC Key derived measures include:
# MAGIC
# MAGIC - Retail Sales year-over-year growth
# MAGIC - CPI year-over-year inflation
# MAGIC - Employment year-over-year growth
# MAGIC - Retail Sales per capita
# MAGIC
# MAGIC Missing source observations remain null rather than being artificially filled.

# COMMAND ----------

# =========================================================
# CREATE REGIONAL GROWTH INDICATORS
# =========================================================

from pyspark.sql.window import Window

# Each geography must be compared only with its own historical values
regional_window = (
    Window
    .partitionBy("geo")
    .orderBy("ref_date")
)

gold_regional_enriched = (
    gold_regional_monthly

    # Retail Sales year-over-year growth
    .withColumn(
        "retail_sales_yoy_percent",
        (
            (
                F.col("retail_sales_dollars") /
                F.lag("retail_sales_dollars", 12).over(regional_window)
            ) - 1
        ) * 100
    )

    # CPI year-over-year inflation
    .withColumn(
        "cpi_yoy_inflation_percent",
        (
            (
                F.col("cpi_all_items") /
                F.lag("cpi_all_items", 12).over(regional_window)
            ) - 1
        ) * 100
    )

    # Employment year-over-year growth
    .withColumn(
        "employment_yoy_percent",
        (
            (
                F.col("employment_thousands") /
                F.lag("employment_thousands", 12).over(regional_window)
            ) - 1
        ) * 100
    )

    # Normalize retail activity by population for regional comparison
    .withColumn(
        "retail_sales_per_capita",
        F.when(
            F.col("population") > 0,
            F.col("retail_sales_dollars") / F.col("population")
        )
    )

    .orderBy("ref_date", "geo")
)

display(
    gold_regional_enriched
    .select(
        "ref_date",
        "geo",
        "retail_sales_yoy_percent",
        "cpi_yoy_inflation_percent",
        "employment_yoy_percent",
        "unemployment_rate_percent",
        "retail_sales_per_capita"
    )
    .orderBy(
        F.col("ref_date").desc(),
        "geo"
    )
    .limit(33)
)

# COMMAND ----------

# =========================================================
# CREATE PROVINCIAL GAPS VERSUS CANADA
# =========================================================

canada_benchmark = (
    gold_regional_enriched

    # Extract Canada values to use as the monthly national benchmark
    .filter(F.col("geo") == "Canada")

    .select(
        "ref_date",
        F.col("retail_sales_yoy_percent")
            .alias("canada_retail_sales_yoy_percent"),
        F.col("cpi_yoy_inflation_percent")
            .alias("canada_cpi_yoy_inflation_percent"),
        F.col("employment_yoy_percent")
            .alias("canada_employment_yoy_percent"),
        F.col("unemployment_rate_percent")
            .alias("canada_unemployment_rate_percent")
    )
)

gold_regional_compared = (
    gold_regional_enriched
    .join(
        canada_benchmark,
        on="ref_date",
        how="left"
    )

    # Positive values mean the region is above the Canada benchmark
    .withColumn(
        "retail_sales_yoy_gap_vs_canada",
        F.col("retail_sales_yoy_percent") -
        F.col("canada_retail_sales_yoy_percent")
    )

    .withColumn(
        "cpi_inflation_gap_vs_canada",
        F.col("cpi_yoy_inflation_percent") -
        F.col("canada_cpi_yoy_inflation_percent")
    )

    .withColumn(
        "employment_yoy_gap_vs_canada",
        F.col("employment_yoy_percent") -
        F.col("canada_employment_yoy_percent")
    )

    .withColumn(
        "unemployment_rate_gap_vs_canada",
        F.col("unemployment_rate_percent") -
        F.col("canada_unemployment_rate_percent")
    )

    .orderBy("ref_date", "geo")
)

display(
    gold_regional_compared
    .select(
        "ref_date",
        "geo",
        "retail_sales_yoy_gap_vs_canada",
        "cpi_inflation_gap_vs_canada",
        "employment_yoy_gap_vs_canada",
        "unemployment_rate_gap_vs_canada"
    )
    .orderBy(
        F.col("ref_date").desc(),
        "geo"
    )
    .limit(33)
)

# COMMAND ----------

# =========================================================
# CLEAN DERIVED REGIONAL METRICS
# =========================================================

regional_metric_columns = [
    "retail_sales_yoy_percent",
    "cpi_yoy_inflation_percent",
    "employment_yoy_percent",
    "retail_sales_per_capita",
    "retail_sales_yoy_gap_vs_canada",
    "cpi_inflation_gap_vs_canada",
    "employment_yoy_gap_vs_canada",
    "unemployment_rate_gap_vs_canada"
]

gold_regional_final = gold_regional_compared

# Round analytical measures for cleaner Gold outputs
for column_name in regional_metric_columns:
    gold_regional_final = gold_regional_final.withColumn(
        column_name,
        F.round(F.col(column_name), 4)
    )

gold_regional_final = gold_regional_final.orderBy(
    "ref_date",
    "geo"
)

display(
    gold_regional_final
    .select(
        "ref_date",
        "geo",
        "retail_sales_yoy_gap_vs_canada",
        "cpi_inflation_gap_vs_canada",
        "employment_yoy_gap_vs_canada",
        "unemployment_rate_gap_vs_canada"
    )
    .orderBy(
        F.col("ref_date").desc(),
        "geo"
    )
    .limit(33)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Validate and Write Regional Gold Outputs
# MAGIC
# MAGIC The regional analysis produces two Gold tables:
# MAGIC
# MAGIC 1. **Monthly Regional Economic Mart**  
# MAGIC    Combines monthly regional indicators and Canada-comparison metrics.
# MAGIC
# MAGIC 2. **Annual Regional Productivity Mart**  
# MAGIC    Preserves annual labour-productivity data at its native frequency.
# MAGIC
# MAGIC Both outputs are validated for business-key uniqueness before being written to Delta.

# COMMAND ----------

# =========================================================
# FINAL VALIDATION OF REGIONAL GOLD OUTPUTS
# =========================================================

print("Monthly regional rows:")
print(f"{gold_regional_final.count():,}")

print("\nMonthly regional duplicate business keys:")
(
    gold_regional_final
    .groupBy("ref_date", "geo")
    .count()
    .filter(F.col("count") > 1)
    .show()
)

print("\nAnnual productivity rows:")
print(f"{gold_regional_productivity.count():,}")

print("\nAnnual productivity duplicate business keys:")
(
    gold_regional_productivity
    .groupBy("ref_date", "geo")
    .count()
    .filter(F.col("count") > 1)
    .show()
)

# COMMAND ----------

# =========================================================
# WRITE REGIONAL GOLD OUTPUTS TO DELTA
# =========================================================

# Write the monthly regional economic mart
(
    gold_regional_final
    .write
    .format("delta")
    .mode("overwrite")
    .save(GOLD_REGIONAL_MONTHLY_PATH)
)

print("Monthly regional Gold table written successfully.")
print(f"Path: {GOLD_REGIONAL_MONTHLY_PATH}")

# Write the annual regional productivity mart
(
    gold_regional_productivity
    .write
    .format("delta")
    .mode("overwrite")
    .save(GOLD_REGIONAL_PRODUCTIVITY_PATH)
)

print("\nAnnual regional productivity Gold table written successfully.")
print(f"Path: {GOLD_REGIONAL_PRODUCTIVITY_PATH}")

# COMMAND ----------

# =========================================================
# READ BACK AND VALIDATE REGIONAL GOLD OUTPUTS
# =========================================================

monthly_regional_check = (
    spark.read
    .format("delta")
    .load(GOLD_REGIONAL_MONTHLY_PATH)
)

productivity_regional_check = (
    spark.read
    .format("delta")
    .load(GOLD_REGIONAL_PRODUCTIVITY_PATH)
)

print("Monthly regional Gold rows:")
print(f"{monthly_regional_check.count():,}")

monthly_regional_check.agg(
    F.min("ref_date").alias("min_date"),
    F.max("ref_date").alias("max_date")
).show()

print("Monthly regional duplicate business keys:")
(
    monthly_regional_check
    .groupBy("ref_date", "geo")
    .count()
    .filter(F.col("count") > 1)
    .show()
)

print("\nAnnual productivity Gold rows:")
print(f"{productivity_regional_check.count():,}")

productivity_regional_check.agg(
    F.min("ref_date").alias("min_date"),
    F.max("ref_date").alias("max_date")
).show()

print("Annual productivity duplicate business keys:")
(
    productivity_regional_check
    .groupBy("ref_date", "geo")
    .count()
    .filter(F.col("count") > 1)
    .show()
)

# COMMAND ----------

# =========================================================
# FINAL GOLD DATA PREVIEW
# =========================================================
# Purpose:
# Read the persisted regional Gold Delta tables after all
# processing and validation are complete.
#
# This cell is only for final visual verification and does
# not modify either dataset.

monthly_regional_final = (
    spark.read
    .format("delta")
    .load(GOLD_REGIONAL_MONTHLY_PATH)
)

productivity_regional_final = (
    spark.read
    .format("delta")
    .load(GOLD_REGIONAL_PRODUCTIVITY_PATH)
)

print("Monthly Regional Gold — Final Preview")
display(
    monthly_regional_final
    .orderBy(F.col("ref_date").desc(), "geo")
    .limit(11)
)

print("Annual Productivity Gold — Final Preview")
display(
    productivity_regional_final
    .orderBy(F.col("ref_date").desc(), "geo")
    .limit(11)
)