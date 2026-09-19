# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Fiscal and Affordability
# MAGIC
# MAGIC **Layer:** Silver → Gold  
# MAGIC **Notebook:** `04_gold_fiscal_and_affordability`
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC Create Gold analytical tables for fiscal conditions and affordability indicators in Canada and Ontario.
# MAGIC
# MAGIC This notebook combines fiscal-year government finance data with selected monthly affordability indicators while preserving their different source frequencies.
# MAGIC
# MAGIC ### Main Analytical Uses
# MAGIC
# MAGIC - Analyze federal and Ontario fiscal trends
# MAGIC - Compare revenues, expenses, deficits, and debt over time
# MAGIC - Track inflation and housing-market pressures
# MAGIC - Support affordability analysis using CPI and housing starts
# MAGIC - Enable fiscal and affordability reporting in Power BI
# MAGIC
# MAGIC ### Gold Output Strategy
# MAGIC
# MAGIC Because fiscal data is annual while affordability indicators are monthly, this notebook creates separate Gold tables rather than forcing all measures into one physical dataset:
# MAGIC
# MAGIC 1. **Federal Fiscal Annual**
# MAGIC 2. **Ontario Fiscal Annual**
# MAGIC 3. **Monthly Affordability Indicators**
# MAGIC
# MAGIC ### Grain Rules
# MAGIC
# MAGIC - Federal Fiscal → one row per fiscal year
# MAGIC - Ontario Fiscal → one row per fiscal year
# MAGIC - Affordability Indicators → one row per month
# MAGIC
# MAGIC Fiscal values remain at their published fiscal-year frequency. Monthly indicators are not artificially aggregated into fiscal-year values unless explicitly required for a later analytical use.
# MAGIC
# MAGIC ### Alignment Principle
# MAGIC
# MAGIC If fiscal and monthly indicators are compared later, the join must be explicitly documented as an analytical alignment rather than presented as if the sources had the same native frequency.

# COMMAND ----------

# =========================================================
# CONFIGURE GOLD FISCAL AND AFFORDABILITY ANALYSIS
# =========================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Separate Gold paths are used because fiscal and affordability
# datasets have different native frequencies and business keys.
GOLD_FEDERAL_FISCAL_PATH = (
    "abfss://gold@stceidev01.dfs.core.windows.net/"
    "fiscal_and_affordability/federal_fiscal_annual/"
)

GOLD_ONTARIO_FISCAL_PATH = (
    "abfss://gold@stceidev01.dfs.core.windows.net/"
    "fiscal_and_affordability/ontario_fiscal_annual/"
)

GOLD_AFFORDABILITY_MONTHLY_PATH = (
    "abfss://gold@stceidev01.dfs.core.windows.net/"
    "fiscal_and_affordability/affordability_monthly/"
)

# Silver source paths
SILVER_FEDERAL_FISCAL_PATH = (
    "abfss://silver@stceidev01.dfs.core.windows.net/"
    "finance_canada/fiscal/federal/"
)

SILVER_ONTARIO_FISCAL_PATH = (
    "abfss://silver@stceidev01.dfs.core.windows.net/"
    "finance_canada/fiscal/ontario/"
)

SILVER_CPI_PATH = (
    "abfss://silver@stceidev01.dfs.core.windows.net/"
    "statcan/cpi/"
)

SILVER_HOUSING_PATH = (
    "abfss://silver@stceidev01.dfs.core.windows.net/"
    "statcan/housing_starts/"
)

print("Gold Fiscal and Affordability configuration loaded.")
print(f"Federal fiscal output: {GOLD_FEDERAL_FISCAL_PATH}")
print(f"Ontario fiscal output: {GOLD_ONTARIO_FISCAL_PATH}")
print(f"Affordability output: {GOLD_AFFORDABILITY_MONTHLY_PATH}")

# COMMAND ----------

# =========================================================
# LOAD SILVER DATASETS FOR FISCAL AND AFFORDABILITY ANALYSIS
# =========================================================

silver_federal_fiscal = (
    spark.read
    .format("delta")
    .load(SILVER_FEDERAL_FISCAL_PATH)
)

silver_ontario_fiscal = (
    spark.read
    .format("delta")
    .load(SILVER_ONTARIO_FISCAL_PATH)
)

silver_cpi = (
    spark.read
    .format("delta")
    .load(SILVER_CPI_PATH)
)

silver_housing = (
    spark.read
    .format("delta")
    .load(SILVER_HOUSING_PATH)
)

print("Silver fiscal and affordability datasets loaded successfully.")
print(f"Federal fiscal rows: {silver_federal_fiscal.count():,}")
print(f"Ontario fiscal rows: {silver_ontario_fiscal.count():,}")
print(f"CPI rows: {silver_cpi.count():,}")
print(f"Housing starts rows: {silver_housing.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Inspect Fiscal and Affordability Source Structures
# MAGIC
# MAGIC Before creating the Gold tables, inspect the available fields and dimensions in each Silver dataset.
# MAGIC
# MAGIC This step confirms:
# MAGIC
# MAGIC - Which federal and Ontario fiscal measures are available
# MAGIC - The exact fiscal column names and data types
# MAGIC - The canonical CPI series required for affordability analysis
# MAGIC - The available CMHC housing-start geographies
# MAGIC
# MAGIC Gold transformations will be based on the validated Silver structure rather than assumed source fields.

# COMMAND ----------

# =========================================================
# INSPECT SILVER SOURCE STRUCTURES
# =========================================================

# Fiscal datasets are already small and curated, so inspect
# both their schemas and a few rows before selecting Gold measures.
print("FEDERAL FISCAL SCHEMA")
silver_federal_fiscal.printSchema()

print("\nFEDERAL FISCAL SAMPLE")
silver_federal_fiscal.show(10, truncate=False)


print("\nONTARIO FISCAL SCHEMA")
silver_ontario_fiscal.printSchema()

print("\nONTARIO FISCAL SAMPLE")
silver_ontario_fiscal.show(10, truncate=False)


# Inspect the dimensions needed to select the headline CPI series.
print("\nCPI geographies:")
silver_cpi.select("geo").distinct().orderBy("geo").show(30, truncate=False)

print("\nCPI product groups:")
silver_cpi.select("product_group").distinct().orderBy("product_group").show(30, truncate=False)

print("\nCPI units:")
silver_cpi.select("uom").distinct().orderBy("uom").show(truncate=False)


# Housing Silver is already a curated regional monthly dataset.
# Confirm which geographies are available for the affordability mart.
print("\nHousing-start geographies:")
silver_housing.select("geo").distinct().orderBy("geo").show(truncate=False)

print("\nHousing schema:")
silver_housing.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Prepare Federal Fiscal Annual Mart
# MAGIC
# MAGIC Create an annual federal fiscal table using the canonical Finance Canada fiscal measures preserved in the Silver layer.
# MAGIC
# MAGIC ### Main Measures
# MAGIC
# MAGIC - Revenues
# MAGIC - Program expenses
# MAGIC - Public debt charges
# MAGIC - Budgetary balance
# MAGIC - Accumulated deficit
# MAGIC
# MAGIC ### Target Grain
# MAGIC
# MAGIC One row per federal fiscal year.
# MAGIC
# MAGIC ### Business Key
# MAGIC
# MAGIC `fiscal_year`
# MAGIC
# MAGIC ### Fiscal Interpretation
# MAGIC
# MAGIC Monetary values remain in **millions of dollars**, consistent with the Finance Canada source.
# MAGIC
# MAGIC A positive budgetary balance represents a surplus, while a negative value represents a deficit.
# MAGIC
# MAGIC Historical null values are preserved when a measure was not reported for a particular fiscal year. Missing historical values are not replaced with zero.

# COMMAND ----------

# =========================================================
# PREPARE FEDERAL FISCAL ANNUAL MART
# =========================================================

gold_federal_fiscal = (
    silver_federal_fiscal
    .select(
        "fiscal_year",
        "revenues",
        "program_expenses_excluding_net_actuarial_losses",
        "public_debt_charges",
        "budgetary_balance",
        "accumulated_deficit"
    )

    # Create a simple analytical classification while preserving
    # the original published budgetary balance.
    .withColumn(
        "fiscal_position",
        F.when(
            F.col("budgetary_balance") > 0,
            F.lit("Surplus")
        )
        .when(
            F.col("budgetary_balance") < 0,
            F.lit("Deficit")
        )
        .otherwise(F.lit("Balanced"))
    )

    # Debt-service burden shows how much of federal revenue
    # is represented by public debt charges.
    .withColumn(
        "debt_charges_as_percent_of_revenue",
        F.when(
            F.col("revenues") > 0,
            (
                F.col("public_debt_charges")
                / F.col("revenues")
            ) * 100
        )
    )

    # Round derived analytical metrics while leaving the
    # published fiscal values unchanged.
    .withColumn(
        "debt_charges_as_percent_of_revenue",
        F.round(
            F.col("debt_charges_as_percent_of_revenue"),
            4
        )
    )
)

# Business key: one row per fiscal year.
federal_duplicate_keys = (
    gold_federal_fiscal
    .groupBy("fiscal_year")
    .count()
    .filter(F.col("count") > 1)
)

print(f"Federal fiscal Gold rows: {gold_federal_fiscal.count():,}")

gold_federal_fiscal.agg(
    F.min("fiscal_year").alias("min_fiscal_year"),
    F.max("fiscal_year").alias("max_fiscal_year")
).show()

print("Duplicate fiscal years:")
federal_duplicate_keys.show()

display(
    gold_federal_fiscal
    .orderBy(F.col("fiscal_year").desc())
    .limit(15)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Prepare Ontario Fiscal Annual Mart
# MAGIC
# MAGIC Create an annual Ontario fiscal table using the canonical provincial measures preserved in the Silver layer.
# MAGIC
# MAGIC ### Main Measures
# MAGIC
# MAGIC - Own-source revenues
# MAGIC - Federal transfers
# MAGIC - Total revenues
# MAGIC - Program expenditures
# MAGIC - Debt charges
# MAGIC - Total expenditures
# MAGIC - Deficit or surplus
# MAGIC - Net debt
# MAGIC
# MAGIC ### Target Grain
# MAGIC
# MAGIC One row per Ontario fiscal year.
# MAGIC
# MAGIC ### Business Key
# MAGIC
# MAGIC `fiscal_year`
# MAGIC
# MAGIC ### Fiscal Interpretation
# MAGIC
# MAGIC Monetary values remain in **millions of dollars**.
# MAGIC
# MAGIC A positive `deficit_or_surplus` represents a surplus, while a negative value represents a deficit.
# MAGIC
# MAGIC The Gold layer preserves the published fiscal measures and adds a small number of derived analytical indicators for comparison.

# COMMAND ----------

# =========================================================
# PREPARE ONTARIO FISCAL ANNUAL MART
# =========================================================

gold_ontario_fiscal = (
    silver_ontario_fiscal
    .select(
        "fiscal_year",
        "own_source_revenues",
        "federal_transfers",
        "total_revenues",
        "total_program_expenditures",
        "debt_charges",
        "total_expenditures",
        "deficit_or_surplus",
        "net_debt"
    )

    # Classify the published fiscal balance for easier
    # dashboard filtering and interpretation.
    .withColumn(
        "fiscal_position",
        F.when(
            F.col("deficit_or_surplus") > 0,
            F.lit("Surplus")
        )
        .when(
            F.col("deficit_or_surplus") < 0,
            F.lit("Deficit")
        )
        .otherwise(F.lit("Balanced"))
    )

    # Measure how dependent total provincial revenue is
    # on federal transfers.
    .withColumn(
        "federal_transfers_as_percent_of_revenue",
        F.when(
            F.col("total_revenues") > 0,
            (
                F.col("federal_transfers")
                / F.col("total_revenues")
            ) * 100
        )
    )

    # Measure debt-service burden relative to total revenue.
    .withColumn(
        "debt_charges_as_percent_of_revenue",
        F.when(
            F.col("total_revenues") > 0,
            (
                F.col("debt_charges")
                / F.col("total_revenues")
            ) * 100
        )
    )

    # Round only the derived analytical measures.
    .withColumn(
        "federal_transfers_as_percent_of_revenue",
        F.round(
            F.col("federal_transfers_as_percent_of_revenue"),
            4
        )
    )
    .withColumn(
        "debt_charges_as_percent_of_revenue",
        F.round(
            F.col("debt_charges_as_percent_of_revenue"),
            4
        )
    )
)

# Business key: one row per fiscal year.
ontario_duplicate_keys = (
    gold_ontario_fiscal
    .groupBy("fiscal_year")
    .count()
    .filter(F.col("count") > 1)
)

print(f"Ontario fiscal Gold rows: {gold_ontario_fiscal.count():,}")

gold_ontario_fiscal.agg(
    F.min("fiscal_year").alias("min_fiscal_year"),
    F.max("fiscal_year").alias("max_fiscal_year")
).show()

print("Duplicate fiscal years:")
ontario_duplicate_keys.show()

display(
    gold_ontario_fiscal
    .orderBy(F.col("fiscal_year").desc())
    .limit(15)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Prepare Monthly Affordability Mart
# MAGIC
# MAGIC Create a monthly affordability-focused Gold table using headline CPI and housing-start indicators for Canada and Ontario.
# MAGIC
# MAGIC ### Indicators
# MAGIC
# MAGIC - Canada All-items CPI
# MAGIC - Ontario All-items CPI
# MAGIC - Canada All-items year-over-year inflation
# MAGIC - Ontario All-items year-over-year inflation
# MAGIC - Canada housing starts (SAAR)
# MAGIC - Ontario housing starts (SAAR)
# MAGIC
# MAGIC ### Target Grain
# MAGIC
# MAGIC One row per month.
# MAGIC
# MAGIC ### Business Key
# MAGIC
# MAGIC `ref_date`
# MAGIC
# MAGIC ### Alignment Rules
# MAGIC
# MAGIC - CPI remains at its native monthly frequency.
# MAGIC - Housing starts remain at their native monthly frequency.
# MAGIC - Canada and Ontario housing-start series are preserved at their published SAAR values.
# MAGIC - Missing housing values before the available housing history are preserved as null and are not replaced with zero.
# MAGIC - Inflation is derived from the CPI index using the same month one year earlier.

# COMMAND ----------

# =========================================================
# PREPARE CANADA AND ONTARIO CPI SERIES
# =========================================================

# Select the canonical All-items CPI series used elsewhere
# in the project for consistent affordability analysis.
cpi_affordability_base = (
    silver_cpi
    .filter(
        (F.col("geo").isin("Canada", "Ontario")) &
        (F.col("product_group") == "All-items") &
        (F.col("uom") == "2002=100")
    )
    .select(
        "ref_date",
        "geo",
        F.col("value").alias("cpi_all_items")
    )
)

# Pivot Canada and Ontario into separate columns so the
# final affordability mart can retain one row per month.
cpi_affordability = (
    cpi_affordability_base
    .groupBy("ref_date")
    .pivot("geo", ["Canada", "Ontario"])
    .agg(F.first("cpi_all_items"))
    .withColumnRenamed("Canada", "cpi_canada_all_items")
    .withColumnRenamed("Ontario", "cpi_ontario_all_items")
    .orderBy("ref_date")
)

# Business key should be one row per month.
cpi_duplicate_months = (
    cpi_affordability
    .groupBy("ref_date")
    .count()
    .filter(F.col("count") > 1)
)

print(f"Affordability CPI rows: {cpi_affordability.count():,}")

cpi_affordability.agg(
    F.min("ref_date").alias("min_date"),
    F.max("ref_date").alias("max_date")
).show()

print("Duplicate CPI months:")
cpi_duplicate_months.show()

display(
    cpi_affordability
    .orderBy(F.col("ref_date").desc())
    .limit(12)
)

# COMMAND ----------

# =========================================================
# CALCULATE CANADA AND ONTARIO CPI INFLATION
# =========================================================

# The CPI table has one row per month, so a 12-row lag
# represents the same month in the previous year.
cpi_monthly_window = (
    Window
    .partitionBy(F.lit(1))
    .orderBy("ref_date")
)

cpi_affordability_enriched = (
    cpi_affordability

    # Canada year-over-year All-items CPI inflation
    .withColumn(
        "canada_cpi_yoy_inflation_percent",
        F.when(
            F.lag("cpi_canada_all_items", 12)
            .over(cpi_monthly_window) != 0,
            (
                (
                    F.col("cpi_canada_all_items")
                    / F.lag("cpi_canada_all_items", 12)
                    .over(cpi_monthly_window)
                ) - 1
            ) * 100
        )
    )

    # Ontario year-over-year All-items CPI inflation
    .withColumn(
        "ontario_cpi_yoy_inflation_percent",
        F.when(
            F.lag("cpi_ontario_all_items", 12)
            .over(cpi_monthly_window) != 0,
            (
                (
                    F.col("cpi_ontario_all_items")
                    / F.lag("cpi_ontario_all_items", 12)
                    .over(cpi_monthly_window)
                ) - 1
            ) * 100
        )
    )

    # Difference in inflation rates:
    # positive = Ontario inflation above Canada's rate.
    .withColumn(
        "ontario_inflation_gap_vs_canada",
        F.col("ontario_cpi_yoy_inflation_percent")
        - F.col("canada_cpi_yoy_inflation_percent")
    )

    # Round derived analytical measures for cleaner Gold output.
    .withColumn(
        "canada_cpi_yoy_inflation_percent",
        F.round("canada_cpi_yoy_inflation_percent", 4)
    )
    .withColumn(
        "ontario_cpi_yoy_inflation_percent",
        F.round("ontario_cpi_yoy_inflation_percent", 4)
    )
    .withColumn(
        "ontario_inflation_gap_vs_canada",
        F.round("ontario_inflation_gap_vs_canada", 4)
    )
)

display(
    cpi_affordability_enriched
    .select(
        "ref_date",
        "cpi_canada_all_items",
        "cpi_ontario_all_items",
        "canada_cpi_yoy_inflation_percent",
        "ontario_cpi_yoy_inflation_percent",
        "ontario_inflation_gap_vs_canada"
    )
    .orderBy(F.col("ref_date").desc())
    .limit(15)
)

# COMMAND ----------

# =========================================================
# PREPARE CANADA AND ONTARIO HOUSING STARTS SERIES
# =========================================================

# Select the Canada and Ontario housing-start series needed
# for the affordability mart.
housing_affordability_base = (
    silver_housing
    .filter(
        F.col("geo").isin(
            "Canada",
            "Ontario"
        )
    )
    .select(
        "ref_date",
        "geo",
        "housing_starts_saar"
    )
)

# Pivot the two geographies into separate columns so the
# final mart keeps one row per month.
housing_affordability = (
    housing_affordability_base
    .groupBy("ref_date")
    .pivot(
        "geo",
        ["Canada", "Ontario"]
    )
    .agg(F.first("housing_starts_saar"))
    .withColumnRenamed(
        "Canada",
        "canada_housing_starts_saar"
    )
    .withColumnRenamed(
        "Ontario",
        "ontario_housing_starts_saar"
    )
    .orderBy("ref_date")
)

housing_duplicate_months = (
    housing_affordability
    .groupBy("ref_date")
    .count()
    .filter(F.col("count") > 1)
)

print(
    f"Housing affordability rows: "
    f"{housing_affordability.count():,}"
)

housing_affordability.agg(
    F.min("ref_date").alias("min_date"),
    F.max("ref_date").alias("max_date")
).show()

print("Duplicate housing months:")
housing_duplicate_months.show()

display(
    housing_affordability
    .orderBy(F.col("ref_date").desc())
    .limit(15)
)

# COMMAND ----------

# =========================================================
# BUILD MONTHLY AFFORDABILITY GOLD MART
# =========================================================

gold_affordability_monthly = (
    cpi_affordability_enriched

    # CPI provides the long monthly calendar.
    # Housing begins in January 1990, so earlier housing
    # observations remain null rather than being manufactured.
    .join(
        housing_affordability,
        on="ref_date",
        how="left"
    )

    .select(
        "ref_date",
        "cpi_canada_all_items",
        "cpi_ontario_all_items",
        "canada_cpi_yoy_inflation_percent",
        "ontario_cpi_yoy_inflation_percent",
        "ontario_inflation_gap_vs_canada",
        "canada_housing_starts_saar",
        "ontario_housing_starts_saar"
    )
    .orderBy("ref_date")
)

# Validate the monthly business key.
affordability_duplicate_months = (
    gold_affordability_monthly
    .groupBy("ref_date")
    .count()
    .filter(F.col("count") > 1)
)

print(
    f"Monthly affordability Gold rows: "
    f"{gold_affordability_monthly.count():,}"
)

gold_affordability_monthly.agg(
    F.min("ref_date").alias("min_date"),
    F.max("ref_date").alias("max_date")
).show()

print("Duplicate months:")
affordability_duplicate_months.show()

print("Null counts:")
gold_affordability_monthly.select(
    [
        F.sum(
            F.col(c).isNull().cast("int")
        ).alias(c)
        for c in gold_affordability_monthly.columns
    ]
).show(truncate=False)

display(
    gold_affordability_monthly
    .orderBy(F.col("ref_date").desc())
    .limit(15)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validate Gold Fiscal and Affordability Outputs
# MAGIC
# MAGIC Perform final validation before persisting the Gold tables.
# MAGIC
# MAGIC ### Validation Checks
# MAGIC
# MAGIC - Confirm expected row counts and date ranges
# MAGIC - Confirm business-key uniqueness
# MAGIC - Confirm fiscal and monthly grains remain separate
# MAGIC - Preserve legitimate historical and source-availability nulls
# MAGIC - Confirm affordability indicators retain their published source coverage
# MAGIC
# MAGIC ### Expected Business Keys
# MAGIC
# MAGIC - Federal Fiscal Annual → `fiscal_year`
# MAGIC - Ontario Fiscal Annual → `fiscal_year`
# MAGIC - Monthly Affordability → `ref_date`
# MAGIC
# MAGIC Null values caused by historical source coverage or publication availability are valid and must not be replaced with zero.

# COMMAND ----------

# =========================================================
# FINAL VALIDATION BEFORE GOLD WRITE
# =========================================================

print("=== FEDERAL FISCAL ANNUAL ===")

print(
    f"Rows: {gold_federal_fiscal.count():,}"
)

gold_federal_fiscal.agg(
    F.min("fiscal_year").alias("min_fiscal_year"),
    F.max("fiscal_year").alias("max_fiscal_year")
).show()

federal_duplicates = (
    gold_federal_fiscal
    .groupBy("fiscal_year")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(f"Duplicate business keys: {federal_duplicates:,}")


print("\n=== ONTARIO FISCAL ANNUAL ===")

print(
    f"Rows: {gold_ontario_fiscal.count():,}"
)

gold_ontario_fiscal.agg(
    F.min("fiscal_year").alias("min_fiscal_year"),
    F.max("fiscal_year").alias("max_fiscal_year")
).show()

ontario_duplicates = (
    gold_ontario_fiscal
    .groupBy("fiscal_year")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(f"Duplicate business keys: {ontario_duplicates:,}")


print("\n=== MONTHLY AFFORDABILITY ===")

print(
    f"Rows: {gold_affordability_monthly.count():,}"
)

gold_affordability_monthly.agg(
    F.min("ref_date").alias("min_date"),
    F.max("ref_date").alias("max_date")
).show()

affordability_duplicates = (
    gold_affordability_monthly
    .groupBy("ref_date")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(f"Duplicate business keys: {affordability_duplicates:,}")


# Fail immediately if a Gold business key is not unique.
assert federal_duplicates == 0, (
    "Federal fiscal Gold contains duplicate fiscal years."
)

assert ontario_duplicates == 0, (
    "Ontario fiscal Gold contains duplicate fiscal years."
)

assert affordability_duplicates == 0, (
    "Monthly affordability Gold contains duplicate months."
)

print("\nAll Gold fiscal and affordability validation checks passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Write Gold Fiscal and Affordability Tables
# MAGIC
# MAGIC Persist the validated fiscal and affordability marts to the Gold layer as Delta tables.
# MAGIC
# MAGIC ### Gold Outputs
# MAGIC
# MAGIC 1. **Federal Fiscal Annual**
# MAGIC    - Grain: one row per fiscal year
# MAGIC    - Business key: `fiscal_year`
# MAGIC
# MAGIC 2. **Ontario Fiscal Annual**
# MAGIC    - Grain: one row per fiscal year
# MAGIC    - Business key: `fiscal_year`
# MAGIC
# MAGIC 3. **Monthly Affordability**
# MAGIC    - Grain: one row per month
# MAGIC    - Business key: `ref_date`
# MAGIC
# MAGIC Delta format is used to provide reliable, repeatable, and transactionally consistent Gold storage.

# COMMAND ----------

# =========================================================
# WRITE GOLD FISCAL AND AFFORDABILITY TABLES
# =========================================================

# Federal fiscal annual
(
    gold_federal_fiscal
    .write
    .format("delta")
    .mode("overwrite")
    .save(GOLD_FEDERAL_FISCAL_PATH)
)

print(
    "Federal fiscal Gold table written successfully:"
)
print(GOLD_FEDERAL_FISCAL_PATH)


# Ontario fiscal annual
(
    gold_ontario_fiscal
    .write
    .format("delta")
    .mode("overwrite")
    .save(GOLD_ONTARIO_FISCAL_PATH)
)

print(
    "\nOntario fiscal Gold table written successfully:"
)
print(GOLD_ONTARIO_FISCAL_PATH)


# Monthly affordability
(
    gold_affordability_monthly
    .write
    .format("delta")
    .mode("overwrite")
    .save(GOLD_AFFORDABILITY_MONTHLY_PATH)
)

print(
    "\nMonthly affordability Gold table written successfully:"
)
print(GOLD_AFFORDABILITY_MONTHLY_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Read-Back Validation
# MAGIC
# MAGIC Reload each persisted Gold Delta table from ADLS and validate the stored outputs independently of the in-memory DataFrames.
# MAGIC
# MAGIC This confirms that:
# MAGIC
# MAGIC - All three Gold tables were written successfully
# MAGIC - Persisted row counts match the validated DataFrames
# MAGIC - Business keys remain unique after persistence
# MAGIC - Fiscal-year and monthly date ranges remain correct
# MAGIC - The Gold Delta outputs are ready for downstream analytics and reporting

# COMMAND ----------

# =========================================================
# READ BACK AND VALIDATE PERSISTED GOLD TABLES
# =========================================================

# Reload directly from Gold storage.
federal_gold_check = (
    spark.read
    .format("delta")
    .load(GOLD_FEDERAL_FISCAL_PATH)
)

ontario_gold_check = (
    spark.read
    .format("delta")
    .load(GOLD_ONTARIO_FISCAL_PATH)
)

affordability_gold_check = (
    spark.read
    .format("delta")
    .load(GOLD_AFFORDABILITY_MONTHLY_PATH)
)


# =========================================================
# FEDERAL FISCAL VALIDATION
# =========================================================

federal_readback_rows = federal_gold_check.count()

federal_readback_duplicates = (
    federal_gold_check
    .groupBy("fiscal_year")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print("=== FEDERAL FISCAL READ-BACK ===")
print(f"Rows: {federal_readback_rows:,}")

federal_gold_check.agg(
    F.min("fiscal_year").alias("min_fiscal_year"),
    F.max("fiscal_year").alias("max_fiscal_year")
).show()

print(
    f"Duplicate business keys: "
    f"{federal_readback_duplicates:,}"
)


# =========================================================
# ONTARIO FISCAL VALIDATION
# =========================================================

ontario_readback_rows = ontario_gold_check.count()

ontario_readback_duplicates = (
    ontario_gold_check
    .groupBy("fiscal_year")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print("\n=== ONTARIO FISCAL READ-BACK ===")
print(f"Rows: {ontario_readback_rows:,}")

ontario_gold_check.agg(
    F.min("fiscal_year").alias("min_fiscal_year"),
    F.max("fiscal_year").alias("max_fiscal_year")
).show()

print(
    f"Duplicate business keys: "
    f"{ontario_readback_duplicates:,}"
)


# =========================================================
# AFFORDABILITY VALIDATION
# =========================================================

affordability_readback_rows = affordability_gold_check.count()

affordability_readback_duplicates = (
    affordability_gold_check
    .groupBy("ref_date")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print("\n=== MONTHLY AFFORDABILITY READ-BACK ===")
print(f"Rows: {affordability_readback_rows:,}")

affordability_gold_check.agg(
    F.min("ref_date").alias("min_date"),
    F.max("ref_date").alias("max_date")
).show()

print(
    f"Duplicate business keys: "
    f"{affordability_readback_duplicates:,}"
)


# =========================================================
# FINAL ASSERTIONS
# =========================================================

assert federal_readback_rows == 59
assert ontario_readback_rows == 34
assert affordability_readback_rows == 1351

assert federal_readback_duplicates == 0
assert ontario_readback_duplicates == 0
assert affordability_readback_duplicates == 0

print(
    "\nAll persisted Gold fiscal and affordability "
    "tables passed read-back validation."
)