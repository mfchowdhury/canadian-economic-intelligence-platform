# Databricks notebook source
# MAGIC %md
# MAGIC # Bank of Canada USD/CAD Exchange Rates — Bronze to Silver
# MAGIC
# MAGIC **Source:** Bank of Canada  
# MAGIC **Series:** FXUSDCAD  
# MAGIC **Dataset:** USD/CAD Exchange Rate  
# MAGIC **Pipeline Layer:** Bronze → Silver  
# MAGIC **Notebook:** `07_silver_boc_exchange_rates`
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC Transform the raw Bank of Canada daily USD/CAD exchange-rate data from the Bronze layer into a validated monthly Silver Delta dataset.
# MAGIC
# MAGIC Daily observations are preserved during validation and then aggregated to monthly averages for later integration with monthly Canadian economic indicators.
# MAGIC
# MAGIC ### Processing Flow
# MAGIC
# MAGIC `Bronze JSON → Read → Flatten Daily Observations → Validate → Monthly Average → Validate → Silver Delta`
# MAGIC
# MAGIC The raw Bronze source remains unchanged throughout the process.

# COMMAND ----------

# =========================================================
# CONFIGURATION
# =========================================================

SOURCE_NAME = "Bank of Canada"
SERIES_ID = "FXUSDCAD"
DATASET_NAME = "USD/CAD Exchange Rate"

BRONZE_FOLDER = (
    "abfss://bronze@stceidev01.dfs.core.windows.net/"
    "bank_of_canada/exchange_rates/"
)

SILVER_PATH = (
    "abfss://silver@stceidev01.dfs.core.windows.net/"
    "bank_of_canada/exchange_rates/"
)

# Find timestamped Bronze snapshots created by the ADF pipeline
bronze_files = dbutils.fs.ls(BRONZE_FOLDER)

snapshot_files = [
    file
    for file in bronze_files
    if file.name.startswith("fx_usd_cad_raw_")
    and file.name.endswith(".json")
]

assert snapshot_files, (
    f"No timestamped FX Bronze snapshot found in {BRONZE_FOLDER}"
)

# Select the most recently ingested snapshot
latest_bronze_file = max(
    snapshot_files,
    key=lambda file: file.modificationTime
)

BRONZE_FILE_NAME = latest_bronze_file.name
BRONZE_PATH = latest_bronze_file.path

print("Configuration loaded.")
print("Latest Bronze snapshot:", BRONZE_FILE_NAME)
print("Bronze:", BRONZE_PATH)
print("Silver:", SILVER_PATH)

# COMMAND ----------

# =========================================================
# VALIDATE BRONZE SOURCE
# =========================================================

bronze_files = dbutils.fs.ls(BRONZE_FOLDER)

available_files = [
    file.name
    for file in bronze_files
]

print("Files available in Bronze:")

for file_name in available_files:
    print(" -", file_name)

assert BRONZE_FILE_NAME in available_files, (
    f"Required Bronze file was not found: "
    f"{BRONZE_FILE_NAME}"
)

print("\nBronze source validation passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read and Inspect Raw Exchange-Rate Data
# MAGIC
# MAGIC Load the raw Bank of Canada USD/CAD JSON file from the Bronze layer and inspect its structure before transformation.
# MAGIC
# MAGIC The goal is to identify:
# MAGIC
# MAGIC - the daily observation array,
# MAGIC - the observation date,
# MAGIC - the USD/CAD exchange-rate value,
# MAGIC - available source metadata.

# COMMAND ----------

# =========================================================
# READ RAW BANK OF CANADA JSON
# =========================================================

boc_raw = (
    spark.read
    .option("multiline", "true")
    .json(BRONZE_PATH)
)

print(
    "Raw Bank of Canada JSON loaded successfully."
)

boc_raw.printSchema()

display(
    boc_raw
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Flatten Daily Exchange-Rate Observations
# MAGIC
# MAGIC The Bank of Canada JSON stores daily observations inside the `observations` array.
# MAGIC
# MAGIC This step expands the array into individual rows and extracts:
# MAGIC
# MAGIC - `ref_date`
# MAGIC - `usd_cad_rate`
# MAGIC
# MAGIC The daily observations are retained temporarily for validation before monthly aggregation.

# COMMAND ----------

# =========================================================
# FLATTEN DAILY OBSERVATIONS
# =========================================================

from pyspark.sql import functions as F

boc_daily = (
    boc_raw
    .select(
        F.explode("observations").alias("observation")
    )
    .select(
        F.to_date(
            F.col("observation.d")
        ).alias("ref_date"),

        F.col(
            "observation.FXUSDCAD.v"
        )
        .cast("double")
        .alias("usd_cad_rate")
    )
)

daily_row_count = (
    boc_daily.count()
)

print(
    f"Daily observations: "
    f"{daily_row_count:,}"
)

display(
    boc_daily
    .orderBy("ref_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validate Daily Exchange-Rate Data
# MAGIC
# MAGIC Validate the flattened daily observations before monthly aggregation.
# MAGIC
# MAGIC The checks confirm:
# MAGIC
# MAGIC - dates are present and valid,
# MAGIC - exchange-rate values are numeric,
# MAGIC - duplicate dates do not exist,
# MAGIC - the available reporting period is understood.

# COMMAND ----------

# =========================================================
# VALIDATE DAILY DATA
# =========================================================

missing_dates = (
    boc_daily
    .filter(
        F.col("ref_date").isNull()
    )
    .count()
)

missing_rates = (
    boc_daily
    .filter(
        F.col("usd_cad_rate").isNull()
    )
    .count()
)

duplicate_dates = (
    boc_daily
    .groupBy("ref_date")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

date_range = (
    boc_daily
    .agg(
        F.min("ref_date").alias("min_date"),
        F.max("ref_date").alias("max_date")
    )
    .collect()[0]
)

print(
    f"Missing dates: {missing_dates:,}"
)

print(
    f"Missing exchange rates: {missing_rates:,}"
)

print(
    f"Duplicate dates: {duplicate_dates:,}"
)

print(
    f"Date range: "
    f"{date_range['min_date']} "
    f"to "
    f"{date_range['max_date']}"
)

assert missing_dates == 0, (
    "Missing Bank of Canada observation dates detected."
)

assert missing_rates == 0, (
    "Missing USD/CAD exchange-rate values detected."
)

assert duplicate_dates == 0, (
    "Duplicate daily exchange-rate dates detected."
)

print(
    "\nDaily exchange-rate validation passed."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Aggregate Daily Exchange Rates to Monthly Averages
# MAGIC
# MAGIC The Bank of Canada source provides daily USD/CAD exchange-rate observations.
# MAGIC
# MAGIC For integration with monthly economic indicators, the daily observations are aggregated into one average exchange rate per month.
# MAGIC
# MAGIC The monthly reference date is standardized to the first day of each month.

# COMMAND ----------

# =========================================================
# AGGREGATE DAILY DATA TO MONTHLY AVERAGES
# =========================================================

boc_monthly = (
    boc_daily
    .withColumn(
        "ref_date",
        F.trunc(
            F.col("ref_date"),
            "month"
        )
    )
    .groupBy("ref_date")
    .agg(
        F.avg(
            "usd_cad_rate"
        ).alias(
            "avg_monthly_fx_usd_cad"
        ),

        F.count(
            "*"
        ).alias(
            "daily_observation_count"
        )
    )
    .orderBy("ref_date")
)

monthly_row_count = (
    boc_monthly.count()
)

print(
    f"Monthly observations: "
    f"{monthly_row_count:,}"
)

display(
    boc_monthly
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validate the Monthly Exchange-Rate Dataset
# MAGIC
# MAGIC Validate the monthly USD/CAD dataset before writing it to Silver.
# MAGIC
# MAGIC The checks confirm that:
# MAGIC
# MAGIC - each month has one observation,
# MAGIC - monthly exchange-rate values are present,
# MAGIC - duplicate months do not exist,
# MAGIC - the reporting-period range is correct,
# MAGIC - the number of daily observations contributing to each monthly average is retained.
# MAGIC
# MAGIC The daily observation count also helps identify a potentially partial latest month.

# COMMAND ----------

# =========================================================
# VALIDATE MONTHLY DATA
# =========================================================

missing_months = (
    boc_monthly
    .filter(
        F.col("ref_date").isNull()
    )
    .count()
)

missing_monthly_rates = (
    boc_monthly
    .filter(
        F.col("avg_monthly_fx_usd_cad").isNull()
    )
    .count()
)

duplicate_months = (
    boc_monthly
    .groupBy("ref_date")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

monthly_date_range = (
    boc_monthly
    .agg(
        F.min("ref_date").alias("min_date"),
        F.max("ref_date").alias("max_date")
    )
    .collect()[0]
)

print(
    f"Monthly observations: "
    f"{monthly_row_count:,}"
)

print(
    f"Missing months: "
    f"{missing_months:,}"
)

print(
    f"Missing monthly FX rates: "
    f"{missing_monthly_rates:,}"
)

print(
    f"Duplicate months: "
    f"{duplicate_months:,}"
)

print(
    f"Monthly date range: "
    f"{monthly_date_range['min_date']} "
    f"to "
    f"{monthly_date_range['max_date']}"
)

assert missing_months == 0, (
    "Missing monthly reference dates detected."
)

assert missing_monthly_rates == 0, (
    "Missing monthly exchange-rate values detected."
)

assert duplicate_months == 0, (
    "Duplicate monthly exchange-rate records detected."
)

print(
    "\nMonthly exchange-rate validation passed."
)

# COMMAND ----------

# =========================================================
# INSPECT LATEST MONTH COVERAGE
# =========================================================

latest_month = (
    boc_monthly
    .orderBy(
        F.col("ref_date").desc()
    )
    .limit(1)
)

print(
    "Latest monthly observation:"
)

display(
    latest_month
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validation Note: Partial Latest Month
# MAGIC
# MAGIC The latest available Bank of Canada daily observation is August 28, 2026.
# MAGIC
# MAGIC Therefore, the August 2026 monthly average is based on the available daily observations to date and represents a partial month.
# MAGIC
# MAGIC The `daily_observation_count` field is retained in Silver so monthly coverage can be evaluated during later analysis.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Write Exchange-Rate Data to Silver Delta
# MAGIC
# MAGIC Write the validated monthly USD/CAD dataset to the Silver layer using Delta format.
# MAGIC
# MAGIC The Silver business key is:
# MAGIC
# MAGIC `ref_date`
# MAGIC
# MAGIC Each monthly reporting period contains one USD/CAD average observation.

# COMMAND ----------

# =========================================================
# FINAL SILVER QUALITY VALIDATION
# =========================================================

silver_candidate_count = boc_monthly.count()

duplicate_business_keys = (
    boc_monthly
    .groupBy("ref_date")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

print(
    f"Silver candidate rows: "
    f"{silver_candidate_count:,}"
)

print(
    f"Duplicate business keys: "
    f"{duplicate_business_keys:,}"
)

assert silver_candidate_count > 0, (
    "No monthly exchange-rate records are available."
)

assert duplicate_business_keys == 0, (
    "Duplicate monthly business keys detected."
)

print(
    "\nSilver quality validation passed."
)

# COMMAND ----------

# =========================================================
# WRITE / MERGE SILVER DELTA TABLE
# =========================================================

from delta.tables import DeltaTable

if DeltaTable.isDeltaTable(
    spark,
    SILVER_PATH
):

    print(
        "Existing Silver Delta table found."
    )

    silver_delta = (
        DeltaTable.forPath(
            spark,
            SILVER_PATH
        )
    )

    (
        silver_delta.alias("target")
        .merge(
            boc_monthly.alias("source"),
            "target.ref_date = source.ref_date"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(
        "Silver Delta MERGE completed successfully."
    )

else:

    print(
        "No existing Silver Delta table found."
    )

    (
        boc_monthly
        .write
        .format("delta")
        .mode("overwrite")
        .save(SILVER_PATH)
    )

    print(
        "Initial Silver Delta table created successfully."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Post-Load Validation
# MAGIC
# MAGIC Read the Silver Delta table back from ADLS and confirm:
# MAGIC
# MAGIC - the expected monthly row count,
# MAGIC - reporting-period coverage,
# MAGIC - unique monthly business keys,
# MAGIC - successful Delta storage.

# COMMAND ----------

# =========================================================
# POST-LOAD VALIDATION
# =========================================================

silver_check = (
    spark.read
    .format("delta")
    .load(SILVER_PATH)
)

silver_row_count = (
    silver_check.count()
)

print(
    f"Silver Delta rows: "
    f"{silver_row_count:,}"
)

silver_check.selectExpr(
    "min(ref_date) AS min_ref_date",
    "max(ref_date) AS max_ref_date"
).show()

duplicate_silver_months = (
    silver_check
    .groupBy("ref_date")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

print(
    f"Duplicate Silver months: "
    f"{duplicate_silver_months:,}"
)

assert silver_row_count == monthly_row_count, (
    "Silver row count does not match "
    "the validated monthly dataset."
)

assert duplicate_silver_months == 0, (
    "Duplicate monthly records detected in Silver."
)

print(
    "\nPost-load validation passed."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Idempotency Test
# MAGIC
# MAGIC Re-run the same Delta MERGE using the validated monthly source dataset.
# MAGIC
# MAGIC The row count must remain unchanged, confirming that rerunning the notebook does not create duplicate monthly exchange-rate observations.

# COMMAND ----------

# =========================================================
# IDEMPOTENCY TEST
# =========================================================

silver_delta = (
    DeltaTable.forPath(
        spark,
        SILVER_PATH
    )
)

rows_before = (
    silver_delta
    .toDF()
    .count()
)

(
    silver_delta.alias("target")
    .merge(
        boc_monthly.alias("source"),
        "target.ref_date = source.ref_date"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

rows_after = (
    silver_delta
    .toDF()
    .count()
)

print(
    f"Rows before MERGE: "
    f"{rows_before:,}"
)

print(
    f"Rows after MERGE:  "
    f"{rows_after:,}"
)

assert rows_before == rows_after, (
    "Idempotency test failed: "
    "row count changed after rerun."
)

print(
    "\nIdempotency test passed."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Processing Summary
# MAGIC
# MAGIC The Bank of Canada USD/CAD Exchange Rate Bronze-to-Silver transformation is complete.
# MAGIC
# MAGIC ### Silver Output
# MAGIC
# MAGIC `abfss://silver@stceidev01.dfs.core.windows.net/bank_of_canada/exchange_rates/`
# MAGIC
# MAGIC ### Silver Business Key
# MAGIC
# MAGIC `ref_date`
# MAGIC
# MAGIC ### Silver Grain
# MAGIC
# MAGIC One row per month.
# MAGIC
# MAGIC ### Design Notes
# MAGIC
# MAGIC - Bronze JSON remains unchanged.
# MAGIC - Daily Bank of Canada observations are validated before aggregation.
# MAGIC - Daily rates are aggregated to monthly averages.
# MAGIC - `daily_observation_count` records how many daily observations contributed to each monthly average.
# MAGIC - The latest month may be partial until all daily observations for that month become available.
# MAGIC - Delta MERGE supports safe reruns without duplicate monthly observations.

# COMMAND ----------

# =========================================================
# FINAL SILVER DATA PREVIEW
# =========================================================
# Purpose:
# Read the persisted Silver Delta table after all processing
# and validation are complete.
#
# This cell is only for final visual verification and does
# not modify the dataset.

silver_final = (
    spark.read
    .format("delta")
    .load(SILVER_PATH)
)

display(silver_final.limit(5))