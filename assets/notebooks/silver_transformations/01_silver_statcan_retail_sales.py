# Databricks notebook source
# MAGIC %md
# MAGIC # Statistics Canada Retail Sales — Bronze to Silver
# MAGIC
# MAGIC **Source:** Statistics Canada  
# MAGIC **Table:** 20-10-0056-01  
# MAGIC **Dataset:** Monthly Retail Trade Sales  
# MAGIC **Pipeline Layer:** Bronze → Silver  
# MAGIC **Notebook:** `01_silver_statcan_retail_sales`
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC This notebook transforms the raw Statistics Canada Retail Sales dataset from the Bronze layer into a validated and standardized Silver Delta dataset.
# MAGIC
# MAGIC The notebook performs:
# MAGIC
# MAGIC 1. Source configuration
# MAGIC 2. Bronze file validation
# MAGIC 3. ZIP extraction
# MAGIC 4. Raw-data inspection
# MAGIC 5. Data-quality profiling
# MAGIC 6. Data cleaning and standardization
# MAGIC 7. Valid / quarantine classification
# MAGIC 8. Silver quality validation
# MAGIC 9. Business-key validation
# MAGIC 10. Delta Lake loading
# MAGIC 11. Incremental MERGE
# MAGIC 12. Post-load validation
# MAGIC 13. Idempotency testing
# MAGIC
# MAGIC ## Medallion Design
# MAGIC
# MAGIC **Bronze**  
# MAGIC Preserves the raw source exactly as ingested.
# MAGIC
# MAGIC **Silver**  
# MAGIC Contains cleaned, standardized and validated observations suitable for downstream processing.
# MAGIC
# MAGIC **Quarantine**  
# MAGIC Contains observations that cannot meet Silver analytical requirements, together with the reason they were excluded.
# MAGIC
# MAGIC **Gold**  
# MAGIC Uses trusted Silver datasets to create economic indicators, integrations, aggregations and analytical outputs.
# MAGIC
# MAGIC > Economic calculations and cross-dataset analysis are intentionally kept out of this notebook and belong in the Gold layer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration
# MAGIC
# MAGIC Define source identifiers, storage locations and temporary processing paths in one place.
# MAGIC
# MAGIC Centralizing configuration makes the notebook easier to maintain and reuse.

# COMMAND ----------

# =========================================================
# 1. CONFIGURATION
# =========================================================

TABLE_ID = "20100056"
SOURCE_NAME = "Statistics Canada"
DATASET_NAME = "Retail Sales"

BRONZE_FOLDER = (
    "abfss://bronze@stceidev01.dfs.core.windows.net/"
    "statcan/retail_sales/"
)

SILVER_PATH = (
    "abfss://silver@stceidev01.dfs.core.windows.net/"
    "statcan/retail_sales/"
)

QUARANTINE_PATH = (
    "abfss://silver@stceidev01.dfs.core.windows.net/"
    "quarantine/statcan/retail_sales/"
)

# Find timestamped Bronze snapshots created by the automated ADF pipeline
bronze_files = dbutils.fs.ls(BRONZE_FOLDER)

snapshot_files = [
    file
    for file in bronze_files
    if file.name.startswith(f"{TABLE_ID}_raw_")
    and file.name.endswith(".zip")
]

assert snapshot_files, (
    f"No timestamped Bronze snapshot found in {BRONZE_FOLDER}"
)

# Process the most recently ingested Bronze snapshot
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
print("Quarantine:", QUARANTINE_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Validate Bronze Source
# MAGIC
# MAGIC Confirm that the expected Retail Sales ZIP file exists before transformation begins.
# MAGIC
# MAGIC The notebook fails immediately if the required Bronze source is unavailable rather than continuing with incomplete input.

# COMMAND ----------

# =========================================================
# 2. VALIDATE BRONZE SOURCE
# =========================================================

bronze_files = dbutils.fs.ls(BRONZE_FOLDER)

available_files = [file.name for file in bronze_files]

print("Files available in Bronze:")
for file_name in available_files:
    print(" -", file_name)

assert BRONZE_FILE_NAME in available_files, (
    f"Required Bronze file was not found: {BRONZE_FILE_NAME}"
)

print("\nBronze source validation passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Read and Extract Statistics Canada Source
# MAGIC
# MAGIC Statistics Canada distributes the complete table as a ZIP archive.
# MAGIC
# MAGIC This step:
# MAGIC
# MAGIC - reads the Bronze ZIP directly from ADLS as binary data,
# MAGIC - opens the ZIP archive in memory,
# MAGIC - identifies the main Statistics Canada CSV,
# MAGIC - extracts the primary data file for processing.
# MAGIC
# MAGIC This approach avoids dependency on restricted local filesystem paths in Databricks serverless/shared compute.
# MAGIC
# MAGIC The original Bronze file remains unchanged.

# COMMAND ----------

# DBTITLE 1,Cell 7
# =========================================================
# 3. READ AND EXTRACT SOURCE ZIP
# =========================================================

import io
import zipfile

# Read the ZIP file directly from Bronze as binary data.
# This avoids using restricted local filesystem paths
# in Databricks serverless/shared compute.

binary_df = (
    spark.read
    .format("binaryFile")
    .load(BRONZE_PATH)
)

binary_rows = binary_df.collect()

assert len(binary_rows) == 1, (
    f"Expected one Bronze ZIP file, but found {len(binary_rows)}."
)

zip_bytes = binary_rows[0]["content"]

print(
    f"Bronze ZIP loaded successfully: "
    f"{len(zip_bytes):,} bytes"
)

# Open the ZIP archive directly from memory
zip_buffer = io.BytesIO(zip_bytes)

with zipfile.ZipFile(zip_buffer, "r") as zip_ref:

    extracted_files = zip_ref.namelist()

    print("\nFiles inside ZIP:")
    for file_name in extracted_files:
        print(" -", file_name)

    # Expected main Statistics Canada data CSV
    MAIN_CSV_NAME = f"{TABLE_ID}.csv"

    assert MAIN_CSV_NAME in extracted_files, (
        f"Expected data file not found inside ZIP: "
        f"{MAIN_CSV_NAME}"
    )

    # Read the main CSV directly into memory
    csv_bytes = zip_ref.read(MAIN_CSV_NAME)

print("\nSource ZIP validation and extraction completed successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Load Raw Retail Sales Data
# MAGIC
# MAGIC Load the main Statistics Canada CSV into a pandas DataFrame.
# MAGIC
# MAGIC This is still treated as raw source data. No transformations are applied in this step.

# COMMAND ----------

# =========================================================
# 4. LOAD RAW DATA
# =========================================================

import pandas as pd

# Read the main Statistics Canada CSV directly from memory
statcan_raw = pd.read_csv(
    io.BytesIO(csv_bytes),
    low_memory=False
)

print(f"Raw rows: {len(statcan_raw):,}")
print(f"Raw columns: {len(statcan_raw.columns)}")

display(statcan_raw.head())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Profile Raw Data Quality
# MAGIC
# MAGIC Inspect the source before transformation.
# MAGIC
# MAGIC The profile checks:
# MAGIC
# MAGIC - schema
# MAGIC - missing values
# MAGIC - duplicate rows
# MAGIC - reporting-period coverage
# MAGIC - Statistics Canada status information
# MAGIC
# MAGIC This establishes a baseline that can be compared with the final Silver output.

# COMMAND ----------

# =========================================================
# 5. RAW DATA QUALITY PROFILE
# =========================================================

print("Column names:")
print(statcan_raw.columns.tolist())

print("\nMissing values:")
missing_summary = (
    statcan_raw
    .isnull()
    .sum()
    .reset_index()
)

missing_summary.columns = [
    "column_name",
    "missing_count"
]

display(
    missing_summary[
        missing_summary["missing_count"] > 0
    ]
)

raw_duplicate_count = statcan_raw.duplicated().sum()

print(f"\nDuplicate rows: {raw_duplicate_count:,}")

print(
    "\nRaw reporting period:",
    statcan_raw["REF_DATE"].min(),
    "to",
    statcan_raw["REF_DATE"].max()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Investigate Missing Retail Sales Values
# MAGIC
# MAGIC Missing Statistics Canada values must not automatically be interpreted as zero.
# MAGIC
# MAGIC Source `STATUS` codes may identify observations that are suppressed, unavailable or otherwise unsuitable for numerical analysis.
# MAGIC
# MAGIC These observations are inspected before the dataset is divided into valid Silver records and quarantine records.

# COMMAND ----------

# =========================================================
# 6. INVESTIGATE MISSING VALUES
# =========================================================

missing_value_rows = statcan_raw[
    statcan_raw["VALUE"].isna()
].copy()

print(
    f"Rows with missing VALUE: "
    f"{len(missing_value_rows):,}"
)

print("\nSTATUS distribution:")
print(
    missing_value_rows["STATUS"]
    .fillna("NULL")
    .value_counts(dropna=False)
)

display(
    missing_value_rows[
        [
            "REF_DATE",
            "GEO",
            "North American Industry Classification System (NAICS)",
            "Sales",
            "Adjustments",
            "VALUE",
            "STATUS"
        ]
    ].head(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Clean and Standardize
# MAGIC
# MAGIC Create a standardized working representation of the source.
# MAGIC
# MAGIC Transformations include:
# MAGIC
# MAGIC - removing unused source fields,
# MAGIC - standardizing important column names,
# MAGIC - converting the reporting period to a date,
# MAGIC - converting the observation value to numeric,
# MAGIC - preserving source quality-status information.
# MAGIC
# MAGIC No Gold-layer economic calculations are performed here.

# COMMAND ----------

# =========================================================
# 7. CLEAN AND STANDARDIZE
# =========================================================

statcan_clean = statcan_raw.copy()

# Remove source fields that do not contain useful information
columns_to_drop = [
    "TERMINATED",
    "SYMBOL"
]

statcan_clean = statcan_clean.drop(
    columns=columns_to_drop,
    errors="ignore"
)

# Standardize important analytical column names
statcan_clean = statcan_clean.rename(
    columns={
        "REF_DATE": "ref_date",
        "GEO": "geo",
        "North American Industry Classification System (NAICS)": "industry",
        "Sales": "sales_type",
        "Adjustments": "adjustment_type",
        "UOM": "unit",
        "SCALAR_FACTOR": "scalar_factor",
        "VALUE": "value",
        "STATUS": "status"
    }
)

print(
    f"Working dataset: "
    f"{len(statcan_clean):,} rows × "
    f"{len(statcan_clean.columns)} columns"
)

# COMMAND ----------

# Standardize data types

statcan_clean["ref_date"] = pd.to_datetime(
    statcan_clean["ref_date"],
    format="%Y-%m",
    errors="coerce"
)

statcan_clean["value"] = pd.to_numeric(
    statcan_clean["value"],
    errors="coerce"
)

print("Key data types:")
print(
    statcan_clean[
        ["ref_date", "value"]
    ].dtypes
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Classify Valid and Quarantine Records
# MAGIC
# MAGIC Records are classified before Silver is written.
# MAGIC
# MAGIC ### Valid Silver Record
# MAGIC
# MAGIC A Retail Sales observation must have:
# MAGIC
# MAGIC - a valid reporting date,
# MAGIC - geography,
# MAGIC - industry,
# MAGIC - sales type,
# MAGIC - adjustment type,
# MAGIC - numeric retail-sales value.
# MAGIC
# MAGIC ### Quarantine Record
# MAGIC
# MAGIC An observation is quarantined when it cannot satisfy the minimum Silver analytical requirements.
# MAGIC
# MAGIC Quarantine does **not** mean that Statistics Canada published incorrect data. For example, a value may intentionally be suppressed or unavailable.
# MAGIC
# MAGIC The original source always remains preserved in Bronze.

# COMMAND ----------

# =========================================================
# 8. CLASSIFY VALID AND INVALID RECORDS
# =========================================================

required_dimension_columns = [
    "ref_date",
    "geo",
    "industry",
    "sales_type",
    "adjustment_type"
]

invalid_mask = (
    statcan_clean["value"].isna()
    | statcan_clean["ref_date"].isna()
    | statcan_clean[required_dimension_columns].isna().any(axis=1)
)

valid_rows = statcan_clean[
    ~invalid_mask
].copy()

quarantine_rows = statcan_clean[
    invalid_mask
].copy()

print(f"Total records:      {len(statcan_clean):,}")
print(f"Valid records:      {len(valid_rows):,}")
print(f"Quarantine records: {len(quarantine_rows):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Add Quarantine Metadata
# MAGIC
# MAGIC Add operational metadata explaining why an observation did not qualify for Silver.
# MAGIC
# MAGIC This makes rejected records traceable and easier to investigate later.

# COMMAND ----------

# =========================================================
# 9. ADD QUARANTINE METADATA
# =========================================================

def determine_quarantine_reason(row):

    reasons = []

    if pd.isna(row["ref_date"]):
        reasons.append("Invalid or missing reporting date")

    if pd.isna(row["geo"]):
        reasons.append("Missing geography")

    if pd.isna(row["industry"]):
        reasons.append("Missing industry")

    if pd.isna(row["sales_type"]):
        reasons.append("Missing sales type")

    if pd.isna(row["adjustment_type"]):
        reasons.append("Missing adjustment type")

    if pd.isna(row["value"]):
        reasons.append("Missing or non-publishable VALUE")

    return "; ".join(reasons)


if len(quarantine_rows) > 0:

    quarantine_rows["quarantine_reason"] = (
        quarantine_rows.apply(
            determine_quarantine_reason,
            axis=1
        )
    )

    quarantine_rows["source_name"] = SOURCE_NAME
    quarantine_rows["source_table_id"] = TABLE_ID
    quarantine_rows["source_file"] = BRONZE_FILE_NAME
    quarantine_rows["quarantine_timestamp_utc"] = pd.Timestamp.utcnow()

print(
    f"Quarantine metadata prepared for "
    f"{len(quarantine_rows):,} records."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Write Quarantine Dataset
# MAGIC
# MAGIC Write records that failed Silver eligibility checks to a dedicated Delta location.
# MAGIC
# MAGIC This preserves visibility into excluded observations without mixing them with trusted analytical records.

# COMMAND ----------

# =========================================================
# 10. WRITE QUARANTINE DATA
# =========================================================

if len(quarantine_rows) > 0:

    quarantine_spark_df = spark.createDataFrame(
        quarantine_rows
    )

    (
        quarantine_spark_df
        .write
        .format("delta")
        .mode("overwrite")
        .save(QUARANTINE_PATH)
    )

    print(
        f"{len(quarantine_rows):,} records "
        "written to quarantine."
    )

else:

    print(
        "No quarantine records detected."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Standardize Monetary Values
# MAGIC
# MAGIC Statistics Canada may publish monetary observations using scalar factors such as units, thousands or millions.
# MAGIC
# MAGIC Create `value_dollars` to provide a consistent full-dollar measure for downstream Gold analysis.
# MAGIC
# MAGIC The original `value` and `scalar_factor` fields are retained for traceability.

# COMMAND ----------

# =========================================================
# 11. STANDARDIZE MONETARY VALUES
# =========================================================

scalar_multiplier = {
    "units": 1,
    "thousands": 1_000,
    "millions": 1_000_000
}

valid_rows["value_dollars"] = (
    valid_rows["value"]
    * valid_rows["scalar_factor"]
        .astype(str)
        .str.lower()
        .map(scalar_multiplier)
)

display(
    valid_rows[
        [
            "ref_date",
            "geo",
            "industry",
            "value",
            "scalar_factor",
            "value_dollars"
        ]
    ].head(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Validate Silver Candidate
# MAGIC
# MAGIC Before writing to Silver, confirm that the cleaned dataset satisfies the required quality rules.
# MAGIC
# MAGIC Validation includes:
# MAGIC
# MAGIC - required columns exist,
# MAGIC - key analytical fields contain no null values,
# MAGIC - monetary values were standardized successfully,
# MAGIC - duplicate records are not present,
# MAGIC - reporting dates are valid.
# MAGIC
# MAGIC A failed validation stops processing rather than silently loading questionable data.

# COMMAND ----------

# =========================================================
# 12. SILVER QUALITY VALIDATION
# =========================================================

required_columns = [
    "ref_date",
    "geo",
    "industry",
    "sales_type",
    "adjustment_type",
    "value",
    "value_dollars"
]

# Confirm required columns exist
missing_columns = [
    column
    for column in required_columns
    if column not in valid_rows.columns
]

assert not missing_columns, (
    f"Required Silver columns missing: {missing_columns}"
)

# Confirm required analytical fields contain no nulls
null_summary = (
    valid_rows[required_columns]
    .isnull()
    .sum()
)

print("Missing values in required Silver fields:")
print(null_summary)

assert null_summary.sum() == 0, (
    "Silver validation failed: "
    "required fields contain null values."
)

# Check complete duplicate rows
duplicate_count = valid_rows.duplicated().sum()

print(f"\nDuplicate rows: {duplicate_count:,}")

assert duplicate_count == 0, (
    "Silver validation failed: "
    "duplicate rows detected."
)

print(
    "\nSilver date range:",
    valid_rows["ref_date"].min(),
    "to",
    valid_rows["ref_date"].max()
)

print("\nSilver quality validation passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Validate Business Key
# MAGIC
# MAGIC The business key identifies one unique retail-sales observation.
# MAGIC
# MAGIC For this dataset:
# MAGIC
# MAGIC `ref_date + geo + industry + sales_type + adjustment_type`
# MAGIC
# MAGIC This key will be used by Delta Lake MERGE to determine whether an incoming observation should update an existing record or create a new record.

# COMMAND ----------

# =========================================================
# 13. BUSINESS KEY VALIDATION
# =========================================================

BUSINESS_KEY = [
    "ref_date",
    "geo",
    "industry",
    "sales_type",
    "adjustment_type"
]

duplicate_business_keys = (
    valid_rows
    .duplicated(
        subset=BUSINESS_KEY,
        keep=False
    )
    .sum()
)

print(
    "Rows with duplicate business keys:",
    f"{duplicate_business_keys:,}"
)

assert duplicate_business_keys == 0, (
    "Business-key validation failed."
)

print("Business key validation passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Prepare Spark Silver DataFrame
# MAGIC
# MAGIC Convert the validated pandas dataset to a Spark DataFrame before loading it into Delta Lake.

# COMMAND ----------

# =========================================================
# 14. CONVERT TO SPARK
# =========================================================

silver_df = spark.createDataFrame(
    valid_rows
)

print(
    f"Silver candidate rows: "
    f"{silver_df.count():,}"
)

print("\nSilver schema:")
silver_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Load Silver Delta Dataset
# MAGIC
# MAGIC Use Delta Lake for the Silver layer.
# MAGIC
# MAGIC ### First Run
# MAGIC
# MAGIC If the Silver Delta dataset does not yet exist, create it.
# MAGIC
# MAGIC ### Subsequent Runs
# MAGIC
# MAGIC If the dataset already exists, use Delta `MERGE` to:
# MAGIC
# MAGIC - update matching observations,
# MAGIC - insert newly published observations,
# MAGIC - preserve previously loaded records.
# MAGIC
# MAGIC This design supports recurring Statistics Canada updates and revisions.

# COMMAND ----------

# =========================================================
# 15. DELTA INITIAL LOAD / INCREMENTAL MERGE
# =========================================================

from delta.tables import DeltaTable

MERGE_CONDITION = """
    target.ref_date = source.ref_date
    AND target.geo = source.geo
    AND target.industry = source.industry
    AND target.sales_type = source.sales_type
    AND target.adjustment_type = source.adjustment_type
"""

# First load
if not DeltaTable.isDeltaTable(
    spark,
    SILVER_PATH
):

    (
        silver_df
        .write
        .format("delta")
        .mode("overwrite")
        .save(SILVER_PATH)
    )

    print(
        "Initial Silver Delta dataset created successfully."
    )

# Incremental load
else:

    silver_delta = DeltaTable.forPath(
        spark,
        SILVER_PATH
    )

    (
        silver_delta.alias("target")
        .merge(
            silver_df.alias("source"),
            MERGE_CONDITION
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(
        "Incremental Silver Delta MERGE "
        "completed successfully."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 16. Validate Silver Output
# MAGIC
# MAGIC Read the persisted Delta dataset and verify:
# MAGIC
# MAGIC - the table can be opened,
# MAGIC - records were written,
# MAGIC - the schema is available,
# MAGIC - the expected reporting range is present.
# MAGIC
# MAGIC This is the final production validation for the Bronze-to-Silver transformation.

# COMMAND ----------

# =========================================================
# 16. POST-LOAD VALIDATION
# =========================================================

silver_check = (
    spark.read
    .format("delta")
    .load(SILVER_PATH)
)

silver_row_count = silver_check.count()

print(
    f"Silver Delta rows: "
    f"{silver_row_count:,}"
)

print("\nSilver schema:")
silver_check.printSchema()

print("\nSilver date range:")

silver_check.selectExpr(
    "MIN(ref_date) AS min_date",
    "MAX(ref_date) AS max_date"
).show()

display(
    silver_check.limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 17. Development Test — Validate Idempotency
# MAGIC
# MAGIC An idempotent pipeline produces the same final state when the same source is processed more than once.
# MAGIC
# MAGIC Re-running the same source through the Delta MERGE should therefore **not create duplicate records**.
# MAGIC
# MAGIC This test is useful during development and acceptance testing. It does not need to perform a second duplicate MERGE during every normal production pipeline run.

# COMMAND ----------

# =========================================================
# 17. IDEMPOTENCY TEST
# Development / acceptance testing only
# =========================================================

silver_delta = DeltaTable.forPath(
    spark,
    SILVER_PATH
)

rows_before_rerun = (
    silver_delta
    .toDF()
    .count()
)

# Process the same source again
(
    silver_delta.alias("target")
    .merge(
        silver_df.alias("source"),
        MERGE_CONDITION
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

rows_after_rerun = (
    silver_delta
    .toDF()
    .count()
)

print(
    "Rows before re-running MERGE:",
    f"{rows_before_rerun:,}"
)

print(
    "Rows after re-running MERGE:",
    f"{rows_after_rerun:,}"
)

assert rows_before_rerun == rows_after_rerun, (
    "Idempotency test failed: "
    "the row count changed after processing "
    "the same source."
)

print(
    "\nIdempotency test passed — "
    "no duplicate records were created."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 18. Processing Summary
# MAGIC
# MAGIC The Bronze-to-Silver Retail Sales transformation is complete.
# MAGIC
# MAGIC ### Source
# MAGIC
# MAGIC **Statistics Canada Table 20-10-0056-01**
# MAGIC
# MAGIC ### Processing Controls Completed
# MAGIC
# MAGIC - Bronze source validated
# MAGIC - Source ZIP extracted
# MAGIC - Raw data profiled
# MAGIC - Missing values investigated
# MAGIC - Source columns standardized
# MAGIC - Data types standardized
# MAGIC - Valid and non-Silver observations classified
# MAGIC - Quarantine records preserved
# MAGIC - Monetary values standardized
# MAGIC - Required-field validation passed
# MAGIC - Duplicate validation passed
# MAGIC - Business key validated
# MAGIC - Silver Delta dataset created or incrementally updated
# MAGIC - Post-load validation completed
# MAGIC - Idempotency tested
# MAGIC
# MAGIC ### Silver Output
# MAGIC
# MAGIC `silver/statcan/retail_sales/`
# MAGIC
# MAGIC ### Quarantine Output
# MAGIC
# MAGIC `silver/quarantine/statcan/retail_sales/`
# MAGIC
# MAGIC ### Business Key
# MAGIC
# MAGIC `ref_date + geo + industry + sales_type + adjustment_type`
# MAGIC
# MAGIC ### Downstream Use
# MAGIC
# MAGIC This Silver dataset can now support Gold-layer analysis such as:
# MAGIC
# MAGIC - monthly retail-sales growth,
# MAGIC - year-over-year retail-sales growth,
# MAGIC - Ontario versus Canada comparisons,
# MAGIC - provincial retail-performance comparisons,
# MAGIC - industry-level retail trends,
# MAGIC - inflation-adjusted retail analysis using CPI,
# MAGIC - integration with population, labour-market and monetary-policy datasets.