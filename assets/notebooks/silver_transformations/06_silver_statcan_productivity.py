# Databricks notebook source
# MAGIC %md
# MAGIC # Statistics Canada Labour Productivity — Bronze to Silver
# MAGIC
# MAGIC **Source:** Statistics Canada  
# MAGIC **Table:** 36-10-0713-01  
# MAGIC **Dataset:** Labour productivity  
# MAGIC **Pipeline Layer:** Bronze → Silver  
# MAGIC **Notebook:** `06_silver_statcan_productivity`
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC Transform the raw Statistics Canada Labour Productivity dataset from the Bronze layer into a cleaned, standardized and validated Silver Delta dataset.
# MAGIC
# MAGIC This notebook follows the reusable Bronze-to-Silver framework while keeping productivity-specific transformations and validation rules separate.
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
# MAGIC Define the Bronze and Silver locations, confirm that the expected source file exists, and read the Statistics Canada ZIP directly from ADLS.
# MAGIC
# MAGIC The ZIP is opened in memory so the notebook works reliably with Databricks serverless/shared compute.

# COMMAND ----------

# =========================================================
# CONFIGURATION
# =========================================================

TABLE_ID = "36100713"
SOURCE_NAME = "Statistics Canada"
DATASET_NAME = "Labour Productivity"

BRONZE_FOLDER = (
    "abfss://bronze@stceidev01.dfs.core.windows.net/"
    "statcan/labour_productivity/"
)

BRONZE_FILE_NAME = "36100713_raw.zip"

BRONZE_PATH = (
    f"{BRONZE_FOLDER}{BRONZE_FILE_NAME}"
)

SILVER_PATH = (
    "abfss://silver@stceidev01.dfs.core.windows.net/"
    "statcan/labour_productivity/"
)

QUARANTINE_PATH = (
    "abfss://silver@stceidev01.dfs.core.windows.net/"
    "quarantine/statcan/labour_productivity/"
)

print("Configuration loaded.")
print("Bronze:", BRONZE_PATH)
print("Silver:", SILVER_PATH)
print("Quarantine:", QUARANTINE_PATH)

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

# =========================================================
# READ AND EXTRACT SOURCE ZIP
# =========================================================

import io
import zipfile

binary_df = (
    spark.read
    .format("binaryFile")
    .load(BRONZE_PATH)
)

binary_rows = binary_df.collect()

assert len(binary_rows) == 1, (
    f"Expected one Bronze ZIP file, "
    f"but found {len(binary_rows)}."
)

zip_bytes = binary_rows[0]["content"]

print(
    f"Bronze ZIP loaded successfully: "
    f"{len(zip_bytes):,} bytes"
)

zip_buffer = io.BytesIO(zip_bytes)

with zipfile.ZipFile(zip_buffer, "r") as zip_ref:

    files_inside_zip = zip_ref.namelist()

    print("\nFiles inside ZIP:")

    for file_name in files_inside_zip:
        print(" -", file_name)

    MAIN_CSV_NAME = f"{TABLE_ID}.csv"

    assert MAIN_CSV_NAME in files_inside_zip, (
        f"Expected data file not found inside ZIP: "
        f"{MAIN_CSV_NAME}"
    )

    csv_bytes = zip_ref.read(MAIN_CSV_NAME)

print(
    "\nStatistics Canada Productivity source "
    "loaded and extracted successfully."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load and Inspect Raw Productivity Data
# MAGIC
# MAGIC Load the primary Statistics Canada Productivity CSV and inspect its structure before defining Silver transformations.
# MAGIC
# MAGIC This step identifies:
# MAGIC
# MAGIC - source row and column counts,
# MAGIC - geography coverage,
# MAGIC - productivity measures,
# MAGIC - industry dimensions,
# MAGIC - measurement units,
# MAGIC - reporting-period coverage,
# MAGIC - source quality fields.
# MAGIC
# MAGIC No source values are changed during this stage.

# COMMAND ----------

# =========================================================
# LOAD RAW PRODUCTIVITY DATA
# =========================================================

import pandas as pd

productivity_raw = pd.read_csv(
    io.BytesIO(csv_bytes),
    low_memory=False
)

print(
    f"Raw rows: {len(productivity_raw):,}"
)

print(
    f"Raw columns: {len(productivity_raw.columns)}"
)

print("\nColumn names:")

for column in productivity_raw.columns:
    print(" -", column)

display(
    productivity_raw.head(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Profile Raw Data Quality
# MAGIC
# MAGIC Inspect the raw Productivity dataset before cleaning.
# MAGIC
# MAGIC The profile checks:
# MAGIC
# MAGIC - missing values,
# MAGIC - complete duplicate rows,
# MAGIC - reporting-period coverage,
# MAGIC - missing productivity observations,
# MAGIC - Statistics Canada status information.
# MAGIC
# MAGIC These results will determine the Productivity-specific Silver validation and quarantine rules.

# COMMAND ----------

# =========================================================
# RAW DATA QUALITY PROFILE
# =========================================================

missing_summary = (
    productivity_raw
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

duplicate_count = (
    productivity_raw
    .duplicated()
    .sum()
)

print(
    f"Duplicate rows: {duplicate_count:,}"
)

print(
    "\nRaw reporting period:",
    productivity_raw["REF_DATE"].min(),
    "to",
    productivity_raw["REF_DATE"].max()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Inspect Productivity Dimensions and Measures
# MAGIC
# MAGIC Inspect the major dimensions and measurement fields before defining the canonical Silver schema.
# MAGIC
# MAGIC This helps determine:
# MAGIC
# MAGIC - available productivity measures,
# MAGIC - industry classifications,
# MAGIC - geography coverage,
# MAGIC - units and scalar factors,
# MAGIC - source status flags,
# MAGIC - the fields required to uniquely identify one productivity observation.

# COMMAND ----------

# =========================================================
# INSPECT PRODUCTIVITY DIMENSIONS
# =========================================================

possible_dimensions = [
    "GEO",
    "North American Industry Classification System (NAICS)",
    "Labour productivity and related measures",
    "Characteristics",
    "Statistics",
    "UOM",
    "SCALAR_FACTOR",
    "STATUS"
]

for column in possible_dimensions:

    if column in productivity_raw.columns:

        print(
            f"\n--- {column} ---"
        )

        print(
            productivity_raw[column]
            .fillna("NULL")
            .value_counts(dropna=False)
            .head(30)
        )

# COMMAND ----------

# =========================================================
# INSPECT MISSING PRODUCTIVITY VALUES
# =========================================================

if "VALUE" in productivity_raw.columns:

    missing_value_rows = (
        productivity_raw[
            productivity_raw["VALUE"].isna()
        ]
        .copy()
    )

    missing_value_count = (
        len(missing_value_rows)
    )

    print(
        f"Rows with missing VALUE: "
        f"{missing_value_count:,}"
    )

    if missing_value_count > 0:

        if "STATUS" in productivity_raw.columns:

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
# MAGIC ## 5. Clean and Standardize Labour Productivity Data
# MAGIC
# MAGIC Standardize the Statistics Canada Labour Productivity source into a Silver-ready structure.
# MAGIC
# MAGIC This step:
# MAGIC
# MAGIC - renames source fields using consistent lowercase names,
# MAGIC - converts the annual reporting period to a proper date,
# MAGIC - converts published observations to numeric values,
# MAGIC - preserves productivity measure, industry, geography, unit and scalar-factor dimensions,
# MAGIC - preserves Statistics Canada source identifiers and status information.
# MAGIC
# MAGIC Published values remain in their original Statistics Canada units and scalar factors.

# COMMAND ----------

# =========================================================
# CLEAN AND STANDARDIZE PRODUCTIVITY DATA
# =========================================================

# Work on a copy so the Bronze source remains unchanged
productivity_clean = productivity_raw.copy()

# Rename StatCan fields to Silver-friendly names
productivity_clean = productivity_clean.rename(
    columns={
        "REF_DATE": "ref_date",
        "GEO": "geo",
        "DGUID": "dguid",
        "Labour productivity and related measures": "productivity_measure",
        "North American Industry Classification System (NAICS)": "industry",
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

# REF_DATE is annual for this table.
# Convert the year to January 1 of that year.
productivity_clean["ref_date"] = pd.to_datetime(
    productivity_clean["ref_date"].astype(str),
    format="%Y",
    errors="coerce"
)

# Ensure published observations are numeric
productivity_clean["value"] = pd.to_numeric(
    productivity_clean["value"],
    errors="coerce"
)

print(
    f"Cleaned rows: {len(productivity_clean):,}"
)

print(
    f"Cleaned columns: {len(productivity_clean.columns)}"
)

print("\nSelected data types:")

print(
    productivity_clean[
        [
            "ref_date",
            "geo",
            "productivity_measure",
            "industry",
            "uom",
            "scalar_factor",
            "value",
            "status"
        ]
    ].dtypes
)

# COMMAND ----------

# =========================================================
# REMOVE NON-ANALYTICAL SOURCE COLUMNS
# =========================================================

columns_to_drop = [
    "SYMBOL",
    "TERMINATED"
]

existing_columns_to_drop = [
    column
    for column in columns_to_drop
    if column in productivity_clean.columns
]

productivity_clean = productivity_clean.drop(
    columns=existing_columns_to_drop
)

print(
    "Removed columns:",
    existing_columns_to_drop
)

print(
    f"Remaining columns: {len(productivity_clean.columns)}"
)

# COMMAND ----------

# =========================================================
# VALIDATE CLEANING RESULTS
# =========================================================

print(
    f"Rows after cleaning: "
    f"{len(productivity_clean):,}"
)

print(
    f"Invalid ref_date values: "
    f"{productivity_clean['ref_date'].isna().sum():,}"
)

print(
    f"Missing productivity values: "
    f"{productivity_clean['value'].isna().sum():,}"
)

print(
    "Date range:",
    productivity_clean["ref_date"].min(),
    "to",
    productivity_clean["ref_date"].max()
)

print("\nProductivity measures:")

print(
    productivity_clean["productivity_measure"]
    .value_counts(dropna=False)
)

print("\nUnits:")

print(
    productivity_clean["uom"]
    .value_counts(dropna=False)
)

print("\nScalar factors:")

print(
    productivity_clean["scalar_factor"]
    .value_counts(dropna=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validate Records and Separate Quarantine
# MAGIC
# MAGIC A Labour Productivity observation is eligible for Silver when the required analytical dimensions are present and Statistics Canada provides a numeric published value.
# MAGIC
# MAGIC Rows marked unavailable by Statistics Canada are not interpreted as zero.
# MAGIC
# MAGIC Those observations are preserved in quarantine with their source status information.

# COMMAND ----------

# =========================================================
# CLASSIFY VALID AND QUARANTINE RECORDS
# =========================================================

required_fields = [
    "ref_date",
    "geo",
    "productivity_measure",
    "industry",
    "uom",
    "scalar_factor",
    "vector",
    "value"
]

invalid_mask = (
    productivity_clean[required_fields]
    .isna()
    .any(axis=1)
)

productivity_valid = (
    productivity_clean[~invalid_mask]
    .copy()
)

productivity_quarantine = (
    productivity_clean[invalid_mask]
    .copy()
)

print(
    f"Total records:      {len(productivity_clean):,}"
)

print(
    f"Valid records:      {len(productivity_valid):,}"
)

print(
    f"Quarantine records: {len(productivity_quarantine):,}"
)

print("\nQuarantine STATUS distribution:")

print(
    productivity_quarantine["status"]
    .fillna("NULL")
    .value_counts(dropna=False)
)

# COMMAND ----------

# =========================================================
# PREPARE QUARANTINE METADATA
# =========================================================

from datetime import datetime, timezone

if len(productivity_quarantine) > 0:

    productivity_quarantine["quarantine_reason"] = (
        "Missing required field or non-publishable productivity observation"
    )

    productivity_quarantine["source_name"] = SOURCE_NAME
    productivity_quarantine["table_id"] = TABLE_ID
    productivity_quarantine["source_file"] = BRONZE_FILE_NAME

    productivity_quarantine["quarantine_timestamp_utc"] = (
        datetime.now(timezone.utc)
    )

    print(
        f"Quarantine metadata prepared for "
        f"{len(productivity_quarantine):,} records."
    )

else:

    print(
        "No records require quarantine."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Define and Validate the Productivity Business Key
# MAGIC
# MAGIC One productivity observation is expected to be identified by:
# MAGIC
# MAGIC `ref_date + geo + productivity_measure + industry + uom + scalar_factor`
# MAGIC
# MAGIC The candidate key is tested against the validated records before it is used for Delta MERGE operations.

# COMMAND ----------

# =========================================================
# TEST PRODUCTIVITY BUSINESS KEY
# =========================================================

candidate_business_key = [
    "ref_date",
    "geo",
    "productivity_measure",
    "industry",
    "uom",
    "scalar_factor"
]

duplicate_business_keys = (
    productivity_valid
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
        productivity_valid.loc[
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

# =========================================================
# WRITE QUARANTINE DATA
# =========================================================

if len(productivity_quarantine) > 0:

    productivity_quarantine_spark = (
        spark.createDataFrame(productivity_quarantine)
    )

    (
        productivity_quarantine_spark
        .write
        .format("delta")
        .mode("overwrite")
        .save(QUARANTINE_PATH)
    )

    print(
        f"Quarantine Delta written successfully: "
        f"{len(productivity_quarantine):,} records."
    )

else:

    print(
        "No quarantine records to write."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Validate Silver-Ready Labour Productivity Data
# MAGIC
# MAGIC Before writing to Silver, confirm that:
# MAGIC
# MAGIC - required analytical fields are complete,
# MAGIC - complete duplicate rows do not exist,
# MAGIC - the validated business key remains unique,
# MAGIC - productivity values are numeric,
# MAGIC - reporting dates are valid.
# MAGIC
# MAGIC Only validated published observations are written to the Silver Delta table.

# COMMAND ----------

# =========================================================
# SILVER QUALITY VALIDATION
# =========================================================

required_null_counts = (
    productivity_valid[required_fields]
    .isna()
    .sum()
)

print("Required-field null counts:")
print(required_null_counts)

duplicate_rows = (
    productivity_valid
    .duplicated()
    .sum()
)

print(
    f"\nDuplicate rows: {duplicate_rows:,}"
)

business_key_duplicates = (
    productivity_valid
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
    productivity_valid["ref_date"].min(),
    "to",
    productivity_valid["ref_date"].max()
)

assert required_null_counts.sum() == 0, (
    "Required Productivity fields contain null values."
)

assert duplicate_rows == 0, (
    "Duplicate Productivity rows detected."
)

assert business_key_duplicates == 0, (
    "Duplicate Productivity business keys detected."
)

print(
    "\nSilver quality validation passed."
)

# COMMAND ----------

# =========================================================
# PREPARE SILVER SPARK DATAFRAME
# =========================================================

from pyspark.sql import functions as F

# Convert the validated Labour Productivity dataset to Spark for Delta processing
productivity_silver_spark = spark.createDataFrame(productivity_valid)

# STATUS is normally null for valid published observations.
# Cast explicitly to string so Delta does not persist it as void.
productivity_silver_spark = (
    productivity_silver_spark
    .withColumn("status", F.col("status").cast("string"))
)

print(f"Silver candidate rows: {productivity_silver_spark.count():,}")

print("\nSilver schema:")
productivity_silver_spark.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Write Labour Productivity Data to Silver Delta
# MAGIC
# MAGIC Write the validated Labour Productivity dataset to the Silver layer using Delta format.
# MAGIC
# MAGIC The first run creates the Delta table. Subsequent runs use Delta MERGE so the notebook can be rerun safely without creating duplicate observations.
# MAGIC
# MAGIC ### Business Key
# MAGIC
# MAGIC `ref_date + geo + productivity_measure + industry + uom + scalar_factor`

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

    merge_condition = """
        target.ref_date = source.ref_date
        AND target.geo = source.geo
        AND target.productivity_measure = source.productivity_measure
        AND target.industry = source.industry
        AND target.uom = source.uom
        AND target.scalar_factor = source.scalar_factor
    """

    (
        silver_delta.alias("target")
        .merge(
            productivity_silver_spark.alias("source"),
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
        productivity_silver_spark
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
# MAGIC - the expected row count was written,
# MAGIC - the reporting-period range is correct,
# MAGIC - the table can be read successfully,
# MAGIC - the business key remains unique.

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

print("\nSilver reporting period:")

silver_check.selectExpr(
    "min(ref_date) AS min_ref_date",
    "max(ref_date) AS max_ref_date"
).show()

assert silver_row_count == len(productivity_valid), (
    "Silver row count does not match "
    "the validated Productivity row count."
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
        "productivity_measure",
        "industry",
        "uom",
        "scalar_factor"
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
# MAGIC ## 11. Idempotency Test
# MAGIC
# MAGIC Confirm that rerunning the same Labour Productivity MERGE does not create duplicate records.
# MAGIC
# MAGIC A successful test means the row count remains unchanged after the same validated source dataset is merged again.

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

merge_condition = """
    target.ref_date = source.ref_date
    AND target.geo = source.geo
    AND target.productivity_measure = source.productivity_measure
    AND target.industry = source.industry
    AND target.uom = source.uom
    AND target.scalar_factor = source.scalar_factor
"""

(
    silver_delta.alias("target")
    .merge(
        productivity_silver_spark.alias("source"),
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
# MAGIC ## 12. Processing Summary
# MAGIC
# MAGIC The Statistics Canada Labour Productivity Bronze-to-Silver transformation is complete.
# MAGIC
# MAGIC ### Silver Output
# MAGIC
# MAGIC `abfss://silver@stceidev01.dfs.core.windows.net/statcan/labour_productivity/`
# MAGIC
# MAGIC ### Quarantine Output
# MAGIC
# MAGIC `abfss://silver@stceidev01.dfs.core.windows.net/quarantine/statcan/labour_productivity/`
# MAGIC
# MAGIC ### Silver Business Key
# MAGIC
# MAGIC `ref_date + geo + productivity_measure + industry + uom + scalar_factor`
# MAGIC
# MAGIC ### Design Notes
# MAGIC
# MAGIC - Bronze data remains unchanged.
# MAGIC - Published values remain in their original Statistics Canada units and scalar factors.
# MAGIC - Geography, industry and productivity measure dimensions are preserved.
# MAGIC - Missing or non-publishable observations are not treated as zero.
# MAGIC - Those records are preserved in quarantine.
# MAGIC - Delta MERGE supports safe reruns without duplicate observations.

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