# Databricks notebook source
# MAGIC %md
# MAGIC # Statistics Canada Housing Starts — Bronze to Silver
# MAGIC
# MAGIC **Source:** Statistics Canada  
# MAGIC **Underlying Data Provider:** Canada Mortgage and Housing Corporation (CMHC)  
# MAGIC **Table:** 34-10-0158-01  
# MAGIC **Dataset:** Housing Starts  
# MAGIC **Frequency:** Monthly  
# MAGIC **Pipeline Layer:** Bronze → Silver  
# MAGIC **Notebook:** `10_silver_statcan_housing_starts`
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC Transform the raw Statistics Canada Housing Starts data from the Bronze layer into a cleaned, standardized, and validated Silver Delta dataset.
# MAGIC
# MAGIC The dataset contains monthly housing starts for Canada and provinces, reported as seasonally adjusted annual rates (SAAR).
# MAGIC
# MAGIC ### Processing Flow
# MAGIC
# MAGIC `Bronze ZIP → Extract CSV → Clean → Standardize → Validate → Silver Delta`
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 1. Configuration and Source Access
# MAGIC
# MAGIC Define the Bronze, Silver, and Quarantine locations used by the Housing Starts transformation.
# MAGIC
# MAGIC Identify the latest Statistics Canada Housing Starts source file available in the Bronze layer.

# COMMAND ----------

# =========================================================
# CONFIGURATION AND SOURCE ACCESS
# =========================================================

from pyspark.sql import functions as F

STORAGE_ACCOUNT = "stceidev01"

BRONZE_FOLDER = (
    f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
    "statcan/housing_starts/"
)

SILVER_PATH = (
    f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
    "statcan/housing_starts/"
)

QUARANTINE_PATH = (
    f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
    "quarantine/statcan/housing_starts/"
)

TABLE_ID = "34100158"
SOURCE_NAME = "Statistics Canada"
DATASET_NAME = "Housing Starts"

print("Configuration loaded.")
print("Bronze folder:", BRONZE_FOLDER)
print("Silver path:", SILVER_PATH)
print("Quarantine path:", QUARANTINE_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Identify Latest Bronze Source
# MAGIC
# MAGIC Identify the most recent Statistics Canada Housing Starts ZIP file available in Bronze.
# MAGIC
# MAGIC The latest snapshot is selected using the file modification timestamp.

# COMMAND ----------

# =========================================================
# IDENTIFY LATEST BRONZE SOURCE
# =========================================================

bronze_files = (
    spark.read
    .format("binaryFile")
    .load(BRONZE_FOLDER)
    .select(
        "path",
        "modificationTime",
        "length"
    )
)

housing_source_files = (
    bronze_files
    .filter(
        F.col("path").contains(f"{TABLE_ID}_raw_")
    )
    .orderBy(
        F.col("modificationTime").desc()
    )
)

display(housing_source_files)

latest_source = housing_source_files.first()

assert latest_source is not None, (
    "No Statistics Canada Housing Starts Bronze ZIP was found."
)

BRONZE_FILE_PATH = latest_source["path"]
BRONZE_FILE_NAME = BRONZE_FILE_PATH.split("/")[-1]

print("Latest Bronze source:", BRONZE_FILE_NAME)
print("Source path:", BRONZE_FILE_PATH)
print("File size:", latest_source["length"], "bytes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Load and Inspect the Statistics Canada ZIP
# MAGIC
# MAGIC Read the selected Housing Starts ZIP directly from Bronze storage and inspect the files contained inside it.
# MAGIC
# MAGIC The primary Statistics Canada CSV will be extracted in memory for transformation.

# COMMAND ----------

# =========================================================
# LOAD AND INSPECT ZIP CONTENTS
# =========================================================

import io
import zipfile

binary_row = (
    spark.read
    .format("binaryFile")
    .load(BRONZE_FILE_PATH)
    .select("content")
    .first()
)

zip_bytes = binary_row["content"]

with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zip_ref:
    zip_files = zip_ref.namelist()

print("Files inside ZIP:")

for file_name in zip_files:
    print(" -", file_name)

MAIN_CSV_NAME = f"{TABLE_ID}.csv"

assert MAIN_CSV_NAME in zip_files, (
    f"Expected source CSV not found: {MAIN_CSV_NAME}"
)

print("\nStatistics Canada Housing Starts ZIP loaded successfully.")

# COMMAND ----------

# =========================================================
# LOAD RAW HOUSING STARTS DATA
# =========================================================

import pandas as pd

with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zip_ref:
    with zip_ref.open(MAIN_CSV_NAME) as csv_file:
        housing_raw = pd.read_csv(csv_file)

print(
    f"Raw rows: {len(housing_raw):,}"
)

print(
    f"Raw columns: {len(housing_raw.columns)}"
)

print("\nColumn names:")

for column in housing_raw.columns:
    print(" -", column)

display(housing_raw.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Profile Raw Housing Starts Data
# MAGIC
# MAGIC Inspect the raw Statistics Canada Housing Starts dataset before transformation.
# MAGIC
# MAGIC The profile checks:
# MAGIC
# MAGIC - reporting-period coverage,
# MAGIC - available geographies,
# MAGIC - published unit and scalar factor,
# MAGIC - missing values,
# MAGIC - duplicate records,
# MAGIC - duplicate month-geography business keys,
# MAGIC - source status values.
# MAGIC
# MAGIC These checks confirm the raw source structure and expected monthly grain before the Silver transformation.

# COMMAND ----------

# =========================================================
# PROFILE RAW HOUSING STARTS DATA
# =========================================================

profile_ref_date = pd.to_datetime(
    housing_raw["REF_DATE"],
    format="%Y-%m",
    errors="coerce"
)

print(
    "Raw reporting period:",
    profile_ref_date.min(),
    "to",
    profile_ref_date.max()
)

print(
    "Unparsed REF_DATE values:",
    profile_ref_date.isna().sum()
)

print("\nGeographies:")
print(
    housing_raw["GEO"]
    .value_counts()
)

print("\nUnits:")
print(
    housing_raw["UOM"]
    .value_counts(dropna=False)
)

print("\nScalar factors:")
print(
    housing_raw["SCALAR_FACTOR"]
    .value_counts(dropna=False)
)

missing_summary = housing_raw.isna().sum()
missing_summary = missing_summary[missing_summary > 0]

print("\nColumns containing missing values:")
print(missing_summary)

duplicate_rows = housing_raw.duplicated().sum()

print(
    f"\nDuplicate rows: {duplicate_rows:,}"
)

duplicate_keys = (
    housing_raw
    .duplicated(
        subset=["REF_DATE", "GEO"]
    )
    .sum()
)

print(
    f"Duplicate REF_DATE + GEO keys: {duplicate_keys:,}"
)

print("\nSTATUS values:")
print(
    housing_raw["STATUS"]
    .fillna("NULL")
    .value_counts(dropna=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Clean and Standardize Housing Starts Data
# MAGIC
# MAGIC Standardize the raw Statistics Canada Housing Starts fields into a consistent Silver-ready structure.
# MAGIC
# MAGIC This step:
# MAGIC
# MAGIC - renames source columns to standardized field names,
# MAGIC - converts the monthly reference period to a date,
# MAGIC - converts housing-start values to numeric,
# MAGIC - preserves geography and Statistics Canada source identifiers,
# MAGIC - removes empty source fields that do not add analytical value.

# COMMAND ----------

# =========================================================
# 5. CLEAN AND STANDARDIZE HOUSING STARTS DATA
# =========================================================

housing_clean = housing_raw.copy()

housing_clean = housing_clean.rename(
    columns={
        "REF_DATE": "ref_date",
        "GEO": "geo",
        "DGUID": "dguid",
        "UOM": "uom",
        "UOM_ID": "uom_id",
        "SCALAR_FACTOR": "scalar_factor",
        "SCALAR_ID": "scalar_id",
        "VECTOR": "vector",
        "COORDINATE": "coordinate",
        "VALUE": "source_value_thousands",
        "STATUS": "status",
        "DECIMALS": "decimals"
    }
)

housing_clean["ref_date"] = pd.to_datetime(
    housing_clean["ref_date"],
    format="%Y-%m",
    errors="coerce"
)

housing_clean["source_value_thousands"] = pd.to_numeric(
    housing_clean["source_value_thousands"],
    errors="coerce"
)

# Statistics Canada publishes the series in thousands of SAAR units.
housing_clean["housing_starts_saar"] = (
    housing_clean["source_value_thousands"] * 1000
)

columns_to_drop = [
    "SYMBOL",
    "TERMINATED",
    "status"
]

existing_columns_to_drop = [
    column
    for column in columns_to_drop
    if column in housing_clean.columns
]

housing_clean = housing_clean.drop(
    columns=existing_columns_to_drop
)

print(f"Cleaned rows: {len(housing_clean):,}")
print(f"Cleaned columns: {len(housing_clean.columns)}")

print(
    "Date range:",
    housing_clean["ref_date"].min(),
    "to",
    housing_clean["ref_date"].max()
)

display(housing_clean.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validate Records and Define the Silver Business Key
# MAGIC
# MAGIC Validate the fields required for a usable Housing Starts observation and confirm the Silver business key.
# MAGIC
# MAGIC A valid record requires:
# MAGIC
# MAGIC - a reporting date,
# MAGIC - a geography,
# MAGIC - a Statistics Canada vector identifier,
# MAGIC - a numeric source value,
# MAGIC - a numeric housing-starts SAAR value.
# MAGIC
# MAGIC The candidate Silver business key is:
# MAGIC
# MAGIC `ref_date + geo`

# COMMAND ----------

# =========================================================
# VALIDATE RECORDS AND DEFINE SILVER BUSINESS KEY
# =========================================================

required_fields = [
    "ref_date",
    "geo",
    "vector",
    "source_value_thousands",
    "housing_starts_saar"
]

invalid_mask = (
    housing_clean[required_fields]
    .isna()
    .any(axis=1)
)

housing_valid = (
    housing_clean[~invalid_mask]
    .copy()
)

housing_quarantine = (
    housing_clean[invalid_mask]
    .copy()
)

print(f"Total records:      {len(housing_clean):,}")
print(f"Valid records:      {len(housing_valid):,}")
print(f"Quarantine records: {len(housing_quarantine):,}")

candidate_business_key = [
    "ref_date",
    "geo"
]

duplicate_business_keys = (
    housing_valid
    .duplicated(
        subset=candidate_business_key,
        keep=False
    )
)

duplicate_business_key_count = (
    duplicate_business_keys.sum()
)

print(
    "\nCandidate business key:",
    " + ".join(candidate_business_key)
)

print(
    "Rows involved in duplicate business keys:",
    f"{duplicate_business_key_count:,}"
)

assert duplicate_business_key_count == 0, (
    "Duplicate Housing Starts business keys detected."
)

print("\nHousing Starts business key validation passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validate Silver-Ready Housing Starts Data
# MAGIC
# MAGIC Confirm that the validated Housing Starts dataset meets the expected Silver quality rules before writing.
# MAGIC
# MAGIC This step checks:
# MAGIC
# MAGIC - required fields are complete,
# MAGIC - the business key remains unique,
# MAGIC - reporting dates are valid,
# MAGIC - housing-start values are numeric,
# MAGIC - the validated row count is preserved.

# COMMAND ----------

# =========================================================
# 7. SILVER QUALITY VALIDATION
# =========================================================

required_null_counts = (
    housing_valid[required_fields]
    .isna()
    .sum()
)

print("Required-field null counts:")
print(required_null_counts)

duplicate_rows = (
    housing_valid
    .duplicated()
    .sum()
)

print(
    f"\nDuplicate rows: {duplicate_rows:,}"
)

business_key_duplicates = (
    housing_valid
    .duplicated(
        subset=candidate_business_key
    )
    .sum()
)

print(
    f"Duplicate business keys: "
    f"{business_key_duplicates:,}"
)

print(
    "\nSilver reporting period:",
    housing_valid["ref_date"].min(),
    "to",
    housing_valid["ref_date"].max()
)

assert required_null_counts.sum() == 0, (
    "Required Housing Starts fields contain null values."
)

assert duplicate_rows == 0, (
    "Duplicate Housing Starts rows detected."
)

assert business_key_duplicates == 0, (
    "Duplicate Housing Starts business keys detected."
)

print(
    "\nSilver quality validation passed."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Prepare the Silver Spark DataFrame
# MAGIC
# MAGIC Convert the validated Housing Starts dataset to a Spark DataFrame before writing it to the Silver Delta layer.
# MAGIC
# MAGIC The Silver dataset preserves the original Statistics Canada source value and the standardized housing-starts SAAR value in actual units.

# COMMAND ----------

# =========================================================
# PREPARE SILVER SPARK DATAFRAME
# =========================================================

from pyspark.sql import functions as F

# Convert the validated Housing Starts dataset to Spark for Delta processing
housing_silver_spark = spark.createDataFrame(housing_valid)

# COORDINATE is a Statistics Canada source identifier rather than
# an analytical numeric measure, so preserve it as a string.
housing_silver_spark = (
    housing_silver_spark
    .withColumn(
        "coordinate",
        F.col("coordinate").cast("string")
    )
)

print(f"Silver candidate rows: {housing_silver_spark.count():,}")

print("\nSilver schema:")
housing_silver_spark.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Write Housing Starts Data to Silver Delta
# MAGIC
# MAGIC Write the validated Housing Starts dataset to the Silver layer using Delta format.
# MAGIC
# MAGIC The first run creates the Delta table. Subsequent runs use a MERGE based on the validated Silver business key:
# MAGIC
# MAGIC `ref_date + geo`
# MAGIC
# MAGIC This allows the notebook to be rerun without creating duplicate observations.

# COMMAND ----------

# =========================================================
# 9. WRITE / MERGE SILVER DELTA TABLE
# =========================================================

from delta.tables import DeltaTable

if DeltaTable.isDeltaTable(
    spark,
    SILVER_PATH
):

    print("Existing Silver Delta table found.")

    silver_delta = DeltaTable.forPath(
        spark,
        SILVER_PATH
    )

    merge_condition = """
        target.ref_date = source.ref_date
        AND target.geo = source.geo
    """

    (
        silver_delta.alias("target")
        .merge(
            housing_silver_spark.alias("source"),
            merge_condition
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(
        "Silver Delta MERGE completed successfully."
    )

else:

    print("No existing Silver Delta table found.")

    (
        housing_silver_spark
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
# MAGIC ## 10. Post-Load Validation
# MAGIC
# MAGIC Validate the Silver Delta table after writing.
# MAGIC
# MAGIC This confirms:
# MAGIC
# MAGIC - the expected number of rows were written,
# MAGIC - the reporting-period range is correct,
# MAGIC - the Silver schema can be read successfully from ADLS,
# MAGIC - the business key remains unique.

# COMMAND ----------

# =========================================================
# 10. POST-LOAD SILVER VALIDATION
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

print("\nSilver reporting period:")

silver_check.selectExpr(
    "min(ref_date) AS min_ref_date",
    "max(ref_date) AS max_ref_date"
).show()

print("\nSilver schema:")
silver_check.printSchema()

assert silver_row_count == len(housing_valid), (
    "Silver row count does not match "
    "the validated Housing Starts row count."
)

print(
    "\nPost-load validation passed."
)

# COMMAND ----------

# =========================================================
# VALIDATE BUSINESS KEY IN SILVER
# =========================================================

duplicate_key_check = (
    silver_check
    .groupBy(
        "ref_date",
        "geo"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
)

duplicate_key_count = (
    duplicate_key_check.count()
)

print(
    f"Duplicate Silver business keys: "
    f"{duplicate_key_count:,}"
)

assert duplicate_key_count == 0, (
    "Duplicate business keys detected "
    "in the Housing Starts Silver table."
)

print(
    "Silver business key validation passed."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Idempotency Validation
# MAGIC
# MAGIC Confirm that rerunning the Silver Delta merge does not create duplicate records or change the expected row count.

# COMMAND ----------

# =========================================================
# 12. IDEMPOTENCY TEST
# =========================================================

silver_before_count = (
    spark.read
    .format("delta")
    .load(SILVER_PATH)
    .count()
)

silver_delta = DeltaTable.forPath(
    spark,
    SILVER_PATH
)

merge_condition = """
    target.ref_date = source.ref_date
    AND target.geo = source.geo
"""

(
    silver_delta.alias("target")
    .merge(
        housing_silver_spark.alias("source"),
        merge_condition
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

silver_after_count = (
    spark.read
    .format("delta")
    .load(SILVER_PATH)
    .count()
)

print(
    f"Rows before rerun: {silver_before_count:,}"
)

print(
    f"Rows after rerun:  {silver_after_count:,}"
)

assert silver_before_count == silver_after_count, (
    "Idempotency validation failed: "
    "row count changed after rerunning the MERGE."
)

print(
    "\nIdempotency validation passed."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Processing Summary
# MAGIC
# MAGIC The Statistics Canada Housing Starts Bronze-to-Silver transformation is complete.
# MAGIC
# MAGIC ### Silver Output
# MAGIC
# MAGIC `abfss://silver@stceidev01.dfs.core.windows.net/statcan/housing_starts/`
# MAGIC
# MAGIC ### Source Information
# MAGIC
# MAGIC - **Source:** Statistics Canada
# MAGIC - **Underlying Data Provider:** Canada Mortgage and Housing Corporation (CMHC)
# MAGIC - **Table:** 34-10-0158-01
# MAGIC - **Frequency:** Monthly
# MAGIC - **Coverage:** January 1990 to July 2026
# MAGIC
# MAGIC ### Silver Dataset
# MAGIC
# MAGIC - **Records:** 5,707
# MAGIC - **Business Key:** `ref_date + geo`
# MAGIC - **Housing Starts Measure:** Seasonally adjusted annual rate (SAAR)
# MAGIC - **Source Unit:** Thousands of units
# MAGIC - **Standardized Unit:** Actual SAAR units
# MAGIC - **Silver Format:** Delta
# MAGIC
# MAGIC ### Data Quality
# MAGIC
# MAGIC - Required analytical fields are complete.
# MAGIC - Duplicate business keys are not present.
# MAGIC - Post-load validation passed.
# MAGIC - Idempotency validation passed.
# MAGIC - No source records required quarantine in the current snapshot.
# MAGIC
# MAGIC ### Design Notes
# MAGIC
# MAGIC - Bronze source data remains unchanged.
# MAGIC - Housing Starts are sourced from CMHC and distributed through Statistics Canada Table 34-10-0158-01.
# MAGIC - Source values are preserved in `source_value_thousands`.
# MAGIC - `housing_starts_saar` standardizes the published values from thousands to actual SAAR units.
# MAGIC - The original monthly source grain is preserved in Silver.
# MAGIC - `ref_date + geo` is the validated Silver business key.
# MAGIC - Statistics Canada source identifiers are preserved for traceability.
# MAGIC - Delta MERGE supports idempotent reruns and source revisions without creating duplicate observations.
# MAGIC
# MAGIC The Silver dataset is ready for downstream Gold-layer integration and analytical use.

# COMMAND ----------

# =========================================================
# FINAL SILVER DATA PREVIEW
# =========================================================
# Purpose:
# Read the persisted Silver Delta table after all processing,
# validation, and MERGE operations are complete.
#
# This cell is only for final visual verification of the
# stored Silver output and does not modify the dataset.

silver_final = (
    spark.read
    .format("delta")
    .load(SILVER_PATH)
)

display(silver_final.limit(5))