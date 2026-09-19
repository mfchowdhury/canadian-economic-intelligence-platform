# Databricks notebook source
# MAGIC %md
# MAGIC # Bank of Canada Policy Rate — Bronze to Silver
# MAGIC
# MAGIC **Source:** Bank of Canada  
# MAGIC **Series:** V39079  
# MAGIC **Dataset:** Policy Rate  
# MAGIC **Pipeline Layer:** Bronze → Silver  
# MAGIC **Notebook:** `08_silver_boc_policy_rate`
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC Transform the raw Bank of Canada policy-rate data from the Bronze layer into a cleaned, validated and standardized Silver Delta dataset.
# MAGIC
# MAGIC The source provides dated policy-rate observations that will later support monetary-policy analysis in the Gold layer.
# MAGIC
# MAGIC ### Processing Flow
# MAGIC
# MAGIC `Bronze JSON → Read → Flatten Observations → Validate → Standardize → Silver Delta`
# MAGIC
# MAGIC The raw Bronze source remains unchanged throughout the process.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration and Source Access
# MAGIC
# MAGIC Define the Bronze and Silver locations and confirm that the expected Bank of Canada policy-rate JSON file exists before transformation.

# COMMAND ----------

# =========================================================
# CONFIGURATION
# =========================================================

SOURCE_NAME = "Bank of Canada"
SERIES_ID = "V39079"
DATASET_NAME = "Policy Rate"

BRONZE_FOLDER = (
    "abfss://bronze@stceidev01.dfs.core.windows.net/"
    "bank_of_canada/policy_rate/"
)

BRONZE_FILE_NAME = "policy_rate_raw.json"

BRONZE_PATH = (
    f"{BRONZE_FOLDER}{BRONZE_FILE_NAME}"
)

SILVER_PATH = (
    "abfss://silver@stceidev01.dfs.core.windows.net/"
    "bank_of_canada/policy_rate/"
)

print("Configuration loaded.")
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
# MAGIC ## 2. Read and Inspect Raw Policy-Rate Data
# MAGIC
# MAGIC Load the raw Bank of Canada policy-rate JSON file from the Bronze layer and inspect its structure before transformation.
# MAGIC
# MAGIC The goal is to identify:
# MAGIC
# MAGIC - the observation array,
# MAGIC - the observation date,
# MAGIC - the policy-rate value,
# MAGIC - available source metadata.
# MAGIC

# COMMAND ----------

# =========================================================
# READ RAW BANK OF CANADA JSON
# =========================================================

boc_policy_raw = (
    spark.read
    .option("multiline", "true")
    .json(BRONZE_PATH)
)

print(
    "Raw Bank of Canada Policy Rate JSON loaded successfully."
)

boc_policy_raw.printSchema()

display(
    boc_policy_raw
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Flatten Policy-Rate Observations
# MAGIC
# MAGIC The Bank of Canada JSON stores policy-rate observations inside the `observations` array.
# MAGIC
# MAGIC This step expands the array into individual rows and extracts the observation date and published policy-rate value.

# COMMAND ----------

# =========================================================
# FLATTEN POLICY-RATE OBSERVATIONS
# =========================================================

from pyspark.sql import functions as F

boc_policy = (
    boc_policy_raw
    .select(
        F.explode("observations").alias("observation")
    )
    .select(
        F.to_date(
            F.col("observation.d")
        ).alias("ref_date"),

        F.col(
            "observation.V39079.v"
        )
        .cast("double")
        .alias("policy_rate")
    )
)

policy_row_count = (
    boc_policy.count()
)

print(
    f"Policy-rate observations: "
    f"{policy_row_count:,}"
)

display(
    boc_policy
    .orderBy("ref_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validate Policy-Rate Observations
# MAGIC
# MAGIC Validate the flattened Bank of Canada policy-rate records before defining the final Silver structure.
# MAGIC
# MAGIC The checks confirm:
# MAGIC
# MAGIC - valid observation dates,
# MAGIC - numeric policy-rate values,
# MAGIC - duplicate-date status,
# MAGIC - reporting-period coverage.

# COMMAND ----------

# =========================================================
# VALIDATE POLICY-RATE DATA
# =========================================================

missing_dates = (
    boc_policy
    .filter(
        F.col("ref_date").isNull()
    )
    .count()
)

missing_rates = (
    boc_policy
    .filter(
        F.col("policy_rate").isNull()
    )
    .count()
)

duplicate_dates = (
    boc_policy
    .groupBy("ref_date")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

date_range = (
    boc_policy
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
    f"Missing policy rates: {missing_rates:,}"
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Standardize Policy-Rate Data for Silver
# MAGIC
# MAGIC The validated Bank of Canada policy-rate observations are retained at their original daily grain.
# MAGIC
# MAGIC This preserves the source series without introducing analytical assumptions in the Silver layer.
# MAGIC
# MAGIC Monthly policy-rate features can be derived later in the Gold layer depending on the business question.

# COMMAND ----------

# =========================================================
# PREPARE SILVER POLICY-RATE DATASET
# =========================================================

boc_policy_silver = (
    boc_policy
    .select(
        "ref_date",
        "policy_rate"
    )
    .orderBy("ref_date")
)

silver_candidate_count = (
    boc_policy_silver.count()
)

print(
    f"Silver candidate rows: "
    f"{silver_candidate_count:,}"
)

display(
    boc_policy_silver
    .orderBy(
        F.col("ref_date").desc()
    )
    .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validate Silver-Ready Policy-Rate Data
# MAGIC
# MAGIC Before writing to Silver, confirm that:
# MAGIC
# MAGIC - observation dates are present,
# MAGIC - policy-rate values are present,
# MAGIC - one observation exists per date,
# MAGIC - the reporting period is valid.
# MAGIC
# MAGIC The Silver business key is:
# MAGIC
# MAGIC `ref_date`

# COMMAND ----------

# =========================================================
# FINAL SILVER QUALITY VALIDATION
# =========================================================

missing_ref_dates = (
    boc_policy_silver
    .filter(
        F.col("ref_date").isNull()
    )
    .count()
)

missing_policy_rates = (
    boc_policy_silver
    .filter(
        F.col("policy_rate").isNull()
    )
    .count()
)

duplicate_business_keys = (
    boc_policy_silver
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
    f"Missing ref_date values: "
    f"{missing_ref_dates:,}"
)

print(
    f"Missing policy_rate values: "
    f"{missing_policy_rates:,}"
)

print(
    f"Duplicate business keys: "
    f"{duplicate_business_keys:,}"
)

assert missing_ref_dates == 0, (
    "Missing policy-rate dates detected."
)

assert missing_policy_rates == 0, (
    "Missing policy-rate values detected."
)

assert duplicate_business_keys == 0, (
    "Duplicate policy-rate business keys detected."
)

print(
    "\nSilver quality validation passed."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Write Policy-Rate Data to Silver Delta
# MAGIC
# MAGIC Write the validated Bank of Canada policy-rate observations to the Silver layer using Delta format.
# MAGIC
# MAGIC The first run creates the Delta table. Subsequent runs use Delta MERGE so the notebook can be rerun safely as new Bank of Canada observations become available.
# MAGIC
# MAGIC ### Business Key
# MAGIC
# MAGIC `ref_date`
# MAGIC
# MAGIC ### Silver Grain
# MAGIC
# MAGIC One row per Bank of Canada observation date.

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
            boc_policy_silver.alias("source"),
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
        boc_policy_silver
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
# MAGIC - the expected row count,
# MAGIC - reporting-period coverage,
# MAGIC - unique daily business keys,
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

duplicate_silver_dates = (
    silver_check
    .groupBy("ref_date")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

print(
    f"Duplicate Silver dates: "
    f"{duplicate_silver_dates:,}"
)

assert silver_row_count == silver_candidate_count, (
    "Silver row count does not match "
    "the validated policy-rate dataset."
)

assert duplicate_silver_dates == 0, (
    "Duplicate policy-rate dates detected in Silver."
)

print(
    "\nPost-load validation passed."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Idempotency Test
# MAGIC
# MAGIC Re-run the same Delta MERGE using the validated Bank of Canada policy-rate dataset.
# MAGIC
# MAGIC The row count must remain unchanged, confirming that rerunning the notebook does not create duplicate observations.

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
        boc_policy_silver.alias("source"),
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
# MAGIC The Bank of Canada Policy Rate Bronze-to-Silver transformation is complete.
# MAGIC
# MAGIC ### Silver Output
# MAGIC
# MAGIC `abfss://silver@stceidev01.dfs.core.windows.net/bank_of_canada/policy_rate/`
# MAGIC
# MAGIC ### Silver Business Key
# MAGIC
# MAGIC `ref_date`
# MAGIC
# MAGIC ### Silver Grain
# MAGIC
# MAGIC One row per Bank of Canada observation date.
# MAGIC
# MAGIC ### Design Notes
# MAGIC
# MAGIC - Bronze JSON remains unchanged.
# MAGIC - Policy-rate observations are preserved at their original daily grain.
# MAGIC - No monthly aggregation is applied in Silver.
# MAGIC - Monthly or end-of-month monetary-policy features can be created later in Gold.
# MAGIC - Delta MERGE supports safe reruns as new observations become available.

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