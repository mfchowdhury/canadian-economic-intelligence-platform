# Databricks notebook source
# MAGIC %md
# MAGIC # Statistics Canada CPI — Bronze to Silver
# MAGIC
# MAGIC **Source:** Statistics Canada  
# MAGIC **Table:** 18-10-0004-01  
# MAGIC **Dataset:** Consumer Price Index, monthly, not seasonally adjusted  
# MAGIC **Pipeline Layer:** Bronze → Silver  
# MAGIC **Notebook:** `02_silver_statcan_cpi`
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC Transform the raw Statistics Canada Consumer Price Index dataset from the Bronze layer into a cleaned, standardized and validated Silver Delta dataset.
# MAGIC
# MAGIC This notebook follows the same Bronze-to-Silver framework used for Retail Sales, while keeping CPI-specific transformations and validation rules separate.
# MAGIC
# MAGIC ### Processing Flow
# MAGIC
# MAGIC `Bronze → Read Source → Inspect → Clean → Validate → Quarantine / Valid → Silver Delta`
# MAGIC
# MAGIC The raw Bronze source remains unchanged throughout the process.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration and Source Access
# MAGIC
# MAGIC Define the source and target locations, confirm that the expected Bronze file exists, and read the Statistics Canada ZIP directly from ADLS.
# MAGIC
# MAGIC The ZIP is processed in memory to avoid dependency on restricted local filesystem paths in Databricks serverless/shared compute.

# COMMAND ----------

# =========================================================
# CONFIGURATION
# =========================================================

TABLE_ID = "18100004"
SOURCE_NAME = "Statistics Canada"
DATASET_NAME = "Consumer Price Index"

BRONZE_FOLDER = (
    "abfss://bronze@stceidev01.dfs.core.windows.net/"
    "statcan/cpi/"
)

BRONZE_FILE_NAME = "18100004_raw.zip"

BRONZE_PATH = (
    f"{BRONZE_FOLDER}{BRONZE_FILE_NAME}"
)

SILVER_PATH = (
    "abfss://silver@stceidev01.dfs.core.windows.net/"
    "statcan/cpi/"
)

QUARANTINE_PATH = (
    "abfss://silver@stceidev01.dfs.core.windows.net/"
    "quarantine/statcan/cpi/"
)

print("Configuration loaded.")
print("Bronze:", BRONZE_PATH)
print("Silver:", SILVER_PATH)
print("Quarantine:", QUARANTINE_PATH)

# COMMAND ----------

# =========================================================
# VALIDATE BRONZE SOURCE
# =========================================================

# List files in the CPI Bronze folder
bronze_files = dbutils.fs.ls(BRONZE_FOLDER)

available_files = [
    file.name
    for file in bronze_files
]

print("Files available in Bronze:")
for file_name in available_files:
    print(" -", file_name)

# Fail early if the expected source file is missing
assert BRONZE_FILE_NAME in available_files, (
    f"Required Bronze file was not found: "
    f"{BRONZE_FILE_NAME}"
)

print("\nBronze source validation passed.")

# COMMAND ----------

# =========================================================
# READ AND EXTRACT SOURCE ZIP
# =========================================================

import io
import zipfile

# Read the ZIP directly from ADLS as binary data
binary_df = (
    spark.read
    .format("binaryFile")
    .load(BRONZE_PATH)
)

binary_rows = binary_df.collect()

# We expect exactly one ZIP file at this path
assert len(binary_rows) == 1, (
    f"Expected one Bronze ZIP file, "
    f"but found {len(binary_rows)}."
)

zip_bytes = binary_rows[0]["content"]

print(
    f"Bronze ZIP loaded successfully: "
    f"{len(zip_bytes):,} bytes"
)

# Open the ZIP archive directly in memory
zip_buffer = io.BytesIO(zip_bytes)

with zipfile.ZipFile(zip_buffer, "r") as zip_ref:

    files_inside_zip = zip_ref.namelist()

    print("\nFiles inside ZIP:")
    for file_name in files_inside_zip:
        print(" -", file_name)

    MAIN_CSV_NAME = f"{TABLE_ID}.csv"

    # Confirm that the main StatCan data file exists
    assert MAIN_CSV_NAME in files_inside_zip, (
        f"Expected data file not found inside ZIP: "
        f"{MAIN_CSV_NAME}"
    )

    # Read the main CSV directly into memory
    csv_bytes = zip_ref.read(MAIN_CSV_NAME)

print(
    "\nStatistics Canada CPI source "
    "loaded and extracted successfully."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load and Inspect Raw CPI Data
# MAGIC
# MAGIC Load the primary Statistics Canada CPI CSV and inspect its structure before defining transformation rules.
# MAGIC
# MAGIC This step establishes:
# MAGIC
# MAGIC - source row and column counts,
# MAGIC - available dimensions and measures,
# MAGIC - sample records,
# MAGIC - reporting-period coverage.
# MAGIC
# MAGIC No source values are changed at this stage.

# COMMAND ----------

# =========================================================
# LOAD RAW CPI DATA
# =========================================================

import pandas as pd

# Load the main CPI CSV directly from memory
cpi_raw = pd.read_csv(
    io.BytesIO(csv_bytes),
    low_memory=False
)

print(
    f"Raw rows: {len(cpi_raw):,}"
)

print(
    f"Raw columns: {len(cpi_raw.columns)}"
)

print("\nColumn names:")

for column in cpi_raw.columns:
    print(" -", column)

# Display a small sample for manual inspection
display(
    cpi_raw.head(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Profile Raw Data Quality
# MAGIC
# MAGIC Inspect the CPI source before cleaning.
# MAGIC
# MAGIC The profile checks:
# MAGIC
# MAGIC - missing values,
# MAGIC - complete duplicate rows,
# MAGIC - reporting-period coverage,
# MAGIC - source status information where available.
# MAGIC
# MAGIC These results will be used to define the CPI-specific Silver validation and quarantine rules.

# COMMAND ----------

# =========================================================
# RAW DATA QUALITY PROFILE
# =========================================================

# Count missing values in each source column
missing_summary = (
    cpi_raw
    .isnull()
    .sum()
    .reset_index()
)

missing_summary.columns = [
    "column_name",
    "missing_count"
]

print("Columns containing missing values:")

display(
    missing_summary[
        missing_summary["missing_count"] > 0
    ]
)

# Check complete duplicate rows
duplicate_count = (
    cpi_raw
    .duplicated()
    .sum()
)

print(
    f"Duplicate rows: {duplicate_count:,}"
)

# Confirm reporting-period coverage
print(
    "\nRaw reporting period:",
    cpi_raw["REF_DATE"].min(),
    "to",
    cpi_raw["REF_DATE"].max()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Inspect CPI Dimensions and Measures
# MAGIC
# MAGIC Before defining the Silver schema, inspect the main CPI dimensions and measurement fields.
# MAGIC
# MAGIC This helps determine:
# MAGIC
# MAGIC - which CPI categories are present,
# MAGIC - which geographies are included,
# MAGIC - how units and scalar factors are represented,
# MAGIC - whether status flags require special handling,
# MAGIC - what combination of fields uniquely identifies an observation.
# MAGIC
# MAGIC The results from this section will determine the correct Silver business key and validation rules.

# COMMAND ----------

# =========================================================
# INSPECT KEY CPI FIELDS
# =========================================================

# Show distinct values for common StatCan dimensions
# only when those columns exist in the source.

columns_to_inspect = [
    "GEO",
    "Products and product groups",
    "UOM",
    "SCALAR_FACTOR",
    "STATUS"
]

for column in columns_to_inspect:

    if column in cpi_raw.columns:

        print(
            f"\n--- {column} ---"
        )

        print(
            cpi_raw[column]
            .fillna("NULL")
            .value_counts(dropna=False)
            .head(25)
        )

# COMMAND ----------

# =========================================================
# INSPECT MISSING CPI VALUES
# =========================================================

# VALUE is the main CPI observation field.
# Check whether any observations are missing before
# defining Silver validation and quarantine rules.

if "VALUE" in cpi_raw.columns:

    missing_value_rows = cpi_raw[
        cpi_raw["VALUE"].isna()
    ].copy()

    missing_value_count = len(missing_value_rows)

    print(
        f"Rows with missing VALUE: "
        f"{missing_value_count:,}"
    )

    if missing_value_count > 0:

        # Review StatCan status flags for missing observations
        if "STATUS" in cpi_raw.columns:

            print(
                "\nSTATUS distribution "
                "for missing VALUE rows:"
            )

            print(
                missing_value_rows["STATUS"]
                .fillna("NULL")
                .value_counts(dropna=False)
            )

        display(
            missing_value_rows.head(20)
        )

    else:
        print(
            "\nNo missing VALUE observations were found."
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Clean and Standardize CPI Data
# MAGIC
# MAGIC Standardize the raw Statistics Canada CPI fields into a cleaner Silver-ready structure.
# MAGIC
# MAGIC This step:
# MAGIC
# MAGIC - renames source columns to consistent lowercase names,
# MAGIC - converts the reporting period to a date,
# MAGIC - converts the CPI observation to numeric,
# MAGIC - preserves geography, product group, unit/index base and StatCan identifiers,
# MAGIC - removes source columns that do not add analytical value to the Silver layer.
# MAGIC
# MAGIC CPI values remain index values and are not converted into monetary amounts.

# COMMAND ----------

# =========================================================
# CLEAN AND STANDARDIZE CPI DATA
# =========================================================

# Work on a copy so the raw source DataFrame remains unchanged
cpi_clean = cpi_raw.copy()

# Rename source columns to consistent Silver-friendly names
cpi_clean = cpi_clean.rename(
    columns={
        "REF_DATE": "ref_date",
        "GEO": "geo",
        "DGUID": "dguid",
        "Products and product groups": "product_group",
        "UOM": "uom",
        "UOM_ID": "uom_id",
        "SCALAR_FACTOR": "scalar_factor",
        "SCALAR_ID": "scalar_id",
        "VECTOR": "vector",
        "COORDINATE": "coordinate",
        "VALUE": "value",
        "STATUS": "status",
        "DECIMALS": "decimals"
    }
)

# Convert monthly reference period to a proper date.
# StatCan provides REF_DATE as YYYY-MM.
cpi_clean["ref_date"] = pd.to_datetime(
    cpi_clean["ref_date"],
    format="%Y-%m",
    errors="coerce"
)

# Ensure CPI observations are numeric
cpi_clean["value"] = pd.to_numeric(
    cpi_clean["value"],
    errors="coerce"
)

print(
    f"Cleaned rows: {len(cpi_clean):,}"
)

print(
    f"Cleaned columns: {len(cpi_clean.columns)}"
)

print("\nData types:")
print(
    cpi_clean[
        [
            "ref_date",
            "geo",
            "product_group",
            "uom",
            "value",
            "status"
        ]
    ].dtypes
)

display(
    cpi_clean.head(10)
)

# COMMAND ----------

# =========================================================
# REMOVE NON-ANALYTICAL SOURCE COLUMNS
# =========================================================

# SYMBOL and TERMINATED are completely empty in many
# Statistics Canada tables and are not required for
# the canonical CPI Silver dataset.

columns_to_drop = [
    "SYMBOL",
    "TERMINATED"
]

existing_columns_to_drop = [
    column
    for column in columns_to_drop
    if column in cpi_clean.columns
]

cpi_clean = cpi_clean.drop(
    columns=existing_columns_to_drop
)

print(
    "Removed columns:",
    existing_columns_to_drop
)

print(
    f"Remaining columns: {len(cpi_clean.columns)}"
)

# COMMAND ----------

# =========================================================
# VALIDATE CLEANING RESULTS
# =========================================================

print(
    "Rows after cleaning:",
    f"{len(cpi_clean):,}"
)

print(
    "Invalid ref_date values:",
    f"{cpi_clean['ref_date'].isna().sum():,}"
)

print(
    "Missing CPI values:",
    f"{cpi_clean['value'].isna().sum():,}"
)

print(
    "Date range:",
    cpi_clean["ref_date"].min(),
    "to",
    cpi_clean["ref_date"].max()
)

print(
    "\nCPI index bases:"
)

print(
    cpi_clean["uom"]
    .value_counts()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validate CPI Records and Define the Silver Business Key
# MAGIC
# MAGIC Validate the fields required for a usable CPI observation.
# MAGIC
# MAGIC A record is eligible for Silver when it has:
# MAGIC
# MAGIC - a valid reporting date,
# MAGIC - a geography,
# MAGIC - a CPI product group,
# MAGIC - an index base/unit,
# MAGIC - a numeric CPI value,
# MAGIC - a Statistics Canada vector identifier.
# MAGIC
# MAGIC Source status flags are preserved for transparency and are not automatically treated as invalid.
# MAGIC
# MAGIC Records that fail the required-field validation are separated into quarantine rather than silently removed.

# COMMAND ----------

# =========================================================
# CLASSIFY VALID AND QUARANTINE RECORDS
# =========================================================

# Required fields for a usable CPI observation
required_fields = [
    "ref_date",
    "geo",
    "product_group",
    "uom",
    "value",
    "vector"
]

# A record is invalid if any required field is missing
invalid_mask = (
    cpi_clean[required_fields]
    .isna()
    .any(axis=1)
)

cpi_valid = (
    cpi_clean[~invalid_mask]
    .copy()
)

cpi_quarantine = (
    cpi_clean[invalid_mask]
    .copy()
)

print(
    f"Total records:      {len(cpi_clean):,}"
)

print(
    f"Valid records:      {len(cpi_valid):,}"
)

print(
    f"Quarantine records: {len(cpi_quarantine):,}"
)

# COMMAND ----------

# =========================================================
# PREPARE QUARANTINE METADATA
# =========================================================

from datetime import datetime, timezone

if len(cpi_quarantine) > 0:

    # Record why the observation was rejected
    cpi_quarantine["quarantine_reason"] = (
        "Missing required CPI field"
    )

    cpi_quarantine["source_name"] = SOURCE_NAME
    cpi_quarantine["table_id"] = TABLE_ID
    cpi_quarantine["source_file"] = BRONZE_FILE_NAME

    cpi_quarantine["quarantine_timestamp_utc"] = (
        datetime.now(timezone.utc)
    )

    print(
        f"Quarantine metadata prepared for "
        f"{len(cpi_quarantine):,} records."
    )

else:

    print(
        "No records require quarantine."
    )

# COMMAND ----------

# =========================================================
# TEST CPI BUSINESS KEY
# =========================================================

# Candidate business key:
# one CPI observation for a reporting period,
# geography, product group and index base.

candidate_business_key = [
    "ref_date",
    "geo",
    "product_group",
    "uom"
]

duplicate_business_keys = (
    cpi_valid
    .duplicated(
        subset=candidate_business_key,
        keep=False
    )
)

duplicate_business_key_count = (
    duplicate_business_keys.sum()
)

print(
    "Candidate business key:"
)

print(
    " + ".join(candidate_business_key)
)

print(
    f"\nRows involved in duplicate business keys: "
    f"{duplicate_business_key_count:,}"
)

if duplicate_business_key_count > 0:

    print(
        "\nDuplicate key examples:"
    )

    display(
        cpi_valid.loc[
            duplicate_business_keys,
            candidate_business_key
            + [
                "vector",
                "coordinate",
                "value",
                "status"
            ]
        ]
        .sort_values(candidate_business_key)
        .head(50)
    )

else:

    print(
        "\nCandidate business key is unique."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validate Silver-Ready CPI Data
# MAGIC
# MAGIC Before writing to the Silver layer, confirm that the validated CPI dataset meets the expected quality rules.
# MAGIC
# MAGIC This step checks:
# MAGIC
# MAGIC - required fields are complete,
# MAGIC - the business key remains unique,
# MAGIC - reporting dates are valid,
# MAGIC - CPI values are numeric,
# MAGIC - source row counts are preserved after validation.
# MAGIC
# MAGIC Only records that pass these checks are written to the Silver Delta table.

# COMMAND ----------

# =========================================================
# SILVER QUALITY VALIDATION
# =========================================================

# Required fields must be complete
required_null_counts = (
    cpi_valid[required_fields]
    .isna()
    .sum()
)

print("Required-field null counts:")
print(required_null_counts)

# Full-row duplicate check
duplicate_rows = (
    cpi_valid
    .duplicated()
    .sum()
)

print(
    f"\nDuplicate rows: {duplicate_rows:,}"
)

# Business-key duplicate check
business_key_duplicates = (
    cpi_valid
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
    cpi_valid["ref_date"].min(),
    "to",
    cpi_valid["ref_date"].max()
)

# Fail the notebook if a core quality rule is violated
assert required_null_counts.sum() == 0, (
    "Required CPI fields contain null values."
)

assert duplicate_rows == 0, (
    "Duplicate CPI rows detected."
)

assert business_key_duplicates == 0, (
    "Duplicate CPI business keys detected."
)

print(
    "\nSilver quality validation passed."
)

# COMMAND ----------

from pyspark.sql import functions as F

# Convert the validated CPI dataset to Spark for Delta processing
cpi_silver_spark = spark.createDataFrame(cpi_valid)

# COORDINATE is a Statistics Canada source identifier rather than
# an analytical numeric measure, so preserve it as a string.
cpi_silver_spark = (
    cpi_silver_spark
    .withColumn(
        "coordinate",
        F.col("coordinate").cast("string")
    )
)

print(f"Silver candidate rows: {cpi_silver_spark.count():,}")

print("\nSilver schema:")
cpi_silver_spark.printSchema()
# Convert the validated CPI dataset to Spark for Delta processing
cpi_silver_spark = spark.createDataFrame(cpi_valid)

# COORDINATE is a Statistics Canada source identifier rather than
# an analytical numeric measure, so preserve it as a string.
cpi_silver_spark = (
    cpi_silver_spark
    .withColumn(
        "coordinate",
        F.col("coordinate").cast("string")
    )
)

print(f"Silver candidate rows: {cpi_silver_spark.count():,}")

print("\nSilver schema:")
cpi_silver_spark.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Write CPI Data to Silver Delta
# MAGIC
# MAGIC Write the validated CPI dataset to the Silver layer using Delta format.
# MAGIC
# MAGIC The first run creates the Delta table. Subsequent runs use a MERGE based on the CPI business key so the notebook can be rerun without creating duplicate observations.
# MAGIC
# MAGIC The merge key is:
# MAGIC
# MAGIC `ref_date + geo + product_group + uom`

# COMMAND ----------

# =========================================================
# WRITE / MERGE SILVER DELTA TABLE
# =========================================================

from delta.tables import DeltaTable

# Determine whether a Delta table already exists
# at the Silver destination.

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

    # Match one CPI observation using the validated
    # business key.
    merge_condition = """
        target.ref_date = source.ref_date
        AND target.geo = source.geo
        AND target.product_group = source.product_group
        AND target.uom = source.uom
    """

    (
        silver_delta.alias("target")
        .merge(
            cpi_silver_spark.alias("source"),
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

    print(
        "No existing Silver Delta table found."
    )

    (
        cpi_silver_spark
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
# MAGIC ## 9. Post-Load Validation
# MAGIC
# MAGIC Validate the Silver Delta table after writing.
# MAGIC
# MAGIC This confirms:
# MAGIC
# MAGIC - the expected number of rows were written,
# MAGIC - the reporting-period range is correct,
# MAGIC - the business key remains unique,
# MAGIC - the Delta table can be read successfully from ADLS.

# COMMAND ----------

# =========================================================
# POST-LOAD SILVER VALIDATION
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

# Confirm the written row count matches
# the validated Silver candidate count.
assert silver_row_count == len(cpi_valid), (
    "Silver row count does not match "
    "the validated CPI row count."
)

print(
    "\nPost-load validation passed."
)

# COMMAND ----------

# =========================================================
# VALIDATE BUSINESS KEY IN SILVER
# =========================================================

from pyspark.sql import functions as F

duplicate_key_check = (
    silver_check
    .groupBy(
        "ref_date",
        "geo",
        "product_group",
        "uom"
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
    "in the Silver Delta table."
)

print(
    "Silver business key validation passed."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Idempotency Test
# MAGIC
# MAGIC Confirm that rerunning the Silver MERGE does not create duplicate CPI observations.
# MAGIC
# MAGIC A successful idempotency test means the row count remains unchanged after the same validated source data is merged again.

# COMMAND ----------

# =========================================================
# IDEMPOTENCY TEST
# =========================================================

from delta.tables import DeltaTable

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

merge_condition = """
    target.ref_date = source.ref_date
    AND target.geo = source.geo
    AND target.product_group = source.product_group
    AND target.uom = source.uom
"""

(
    silver_delta.alias("target")
    .merge(
        cpi_silver_spark.alias("source"),
        merge_condition
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
# MAGIC ## 11. Processing Summary
# MAGIC
# MAGIC The Statistics Canada CPI Bronze-to-Silver transformation is complete.
# MAGIC
# MAGIC ### Output
# MAGIC
# MAGIC **Silver Delta path**
# MAGIC
# MAGIC `abfss://silver@stceidev01.dfs.core.windows.net/statcan/cpi/`
# MAGIC
# MAGIC ### Silver Business Key
# MAGIC
# MAGIC `ref_date + geo + product_group + uom`
# MAGIC
# MAGIC ### Validation Rules
# MAGIC
# MAGIC Silver records require:
# MAGIC
# MAGIC - valid reporting date,
# MAGIC - geography,
# MAGIC - CPI product group,
# MAGIC - CPI index base/unit,
# MAGIC - numeric CPI value,
# MAGIC - Statistics Canada vector identifier.
# MAGIC
# MAGIC Source status flags are preserved for transparency rather than automatically rejected.
# MAGIC
# MAGIC ### Design Notes
# MAGIC
# MAGIC - Bronze data remains unchanged.
# MAGIC - CPI values remain index values.
# MAGIC - Multiple historical CPI index bases are preserved.
# MAGIC - Invalid required-field records are designed to flow to quarantine.
# MAGIC - Delta MERGE allows safe notebook reruns without creating duplicate observations.

# COMMAND ----------

# MAGIC %md
# MAGIC