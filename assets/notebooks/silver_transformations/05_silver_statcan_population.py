# Databricks notebook source
# MAGIC %md
# MAGIC # Statistics Canada Population Estimates — Bronze to Silver
# MAGIC
# MAGIC **Source:** Statistics Canada  
# MAGIC **Table:** 17-10-0009-01  
# MAGIC **Dataset:** Population estimates, quarterly  
# MAGIC **Pipeline Layer:** Bronze → Silver  
# MAGIC **Notebook:** `05_silver_statcan_population`
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC Transform the raw Statistics Canada Population Estimates dataset from the Bronze layer into a cleaned, standardized and validated Silver Delta dataset.
# MAGIC
# MAGIC This notebook follows the reusable Bronze-to-Silver framework while keeping population-specific transformations and validation rules separate.
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

TABLE_ID = "17100009"
SOURCE_NAME = "Statistics Canada"
DATASET_NAME = "Population Estimates"

BRONZE_FOLDER = (
    "abfss://bronze@stceidev01.dfs.core.windows.net/"
    "statcan/population/"
)

BRONZE_FILE_NAME = "17100009_raw.zip"

BRONZE_PATH = (
    f"{BRONZE_FOLDER}{BRONZE_FILE_NAME}"
)

SILVER_PATH = (
    "abfss://silver@stceidev01.dfs.core.windows.net/"
    "statcan/population/"
)

QUARANTINE_PATH = (
    "abfss://silver@stceidev01.dfs.core.windows.net/"
    "quarantine/statcan/population/"
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

# Read the StatCan ZIP directly from ADLS
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

# Open the ZIP archive directly in memory
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
    "\nStatistics Canada Population source "
    "loaded and extracted successfully."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load and Inspect Raw Population Data
# MAGIC
# MAGIC Load the primary Statistics Canada Population CSV and inspect the source before defining Silver transformations.
# MAGIC
# MAGIC This step identifies:
# MAGIC
# MAGIC - source row and column counts,
# MAGIC - geography coverage,
# MAGIC - population measures,
# MAGIC - demographic dimensions,
# MAGIC - reporting-period coverage,
# MAGIC - source quality fields.
# MAGIC
# MAGIC No source values are changed during this stage.

# COMMAND ----------

# =========================================================
# LOAD RAW POPULATION DATA
# =========================================================

import pandas as pd

population_raw = pd.read_csv(
    io.BytesIO(csv_bytes),
    low_memory=False
)

print(
    f"Raw rows: {len(population_raw):,}"
)

print(
    f"Raw columns: {len(population_raw.columns)}"
)

print("\nColumn names:")

for column in population_raw.columns:
    print(" -", column)

display(
    population_raw.head(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Profile Raw Data Quality
# MAGIC
# MAGIC Inspect the raw Population dataset before cleaning.
# MAGIC
# MAGIC The profile checks:
# MAGIC
# MAGIC - missing values,
# MAGIC - complete duplicate rows,
# MAGIC - reporting-period coverage,
# MAGIC - missing population observations,
# MAGIC - Statistics Canada status information.
# MAGIC
# MAGIC These results will determine the Population-specific Silver validation and quarantine rules.

# COMMAND ----------

# =========================================================
# RAW DATA QUALITY PROFILE
# =========================================================

missing_summary = (
    population_raw
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
    population_raw
    .duplicated()
    .sum()
)

print(
    f"Duplicate rows: {duplicate_count:,}"
)

print(
    "\nRaw reporting period:",
    population_raw["REF_DATE"].min(),
    "to",
    population_raw["REF_DATE"].max()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Inspect Population Dimensions and Measures
# MAGIC
# MAGIC Inspect the main population dimensions before defining the canonical Silver schema.
# MAGIC
# MAGIC This helps determine:
# MAGIC
# MAGIC - available geography levels,
# MAGIC - demographic categories,
# MAGIC - population measures,
# MAGIC - units and scalar factors,
# MAGIC - source status flags,
# MAGIC - the fields required to uniquely identify one population observation.

# COMMAND ----------

# =========================================================
# INSPECT POPULATION DIMENSIONS
# =========================================================

# Inspect likely Population dimensions only when
# they actually exist in the source table.

possible_dimensions = [
    "GEO",
    "Age group",
    "Gender",
    "Sex",
    "Characteristics",
    "Population and demographic factors",
    "UOM",
    "SCALAR_FACTOR",
    "STATUS"
]

for column in possible_dimensions:

    if column in population_raw.columns:

        print(
            f"\n--- {column} ---"
        )

        print(
            population_raw[column]
            .fillna("NULL")
            .value_counts(dropna=False)
            .head(30)
        )

# COMMAND ----------

# =========================================================
# INSPECT MISSING POPULATION VALUES
# =========================================================

if "VALUE" in population_raw.columns:

    missing_value_rows = (
        population_raw[
            population_raw["VALUE"].isna()
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

        if "STATUS" in population_raw.columns:

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
# MAGIC ## 5. Clean and Standardize Population Data
# MAGIC
# MAGIC Standardize the Statistics Canada Population source into a Silver-ready structure.
# MAGIC
# MAGIC This step:
# MAGIC
# MAGIC - renames source fields using consistent lowercase names,
# MAGIC - converts the reporting period to a proper date,
# MAGIC - converts population observations to numeric values,
# MAGIC - preserves geography and Statistics Canada source identifiers,
# MAGIC - removes non-analytical source-control fields.
# MAGIC
# MAGIC Population values remain in persons.

# COMMAND ----------

# =========================================================
# CLEAN AND STANDARDIZE POPULATION DATA
# =========================================================

# Work on a copy so the Bronze source remains unchanged
population_clean = population_raw.copy()

# Rename StatCan fields to Silver-friendly names
population_clean = population_clean.rename(
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
        "VALUE": "value",
        "STATUS": "status",
        "DECIMALS": "decimals"
    }
)

# Convert the quarterly reference period to a proper date.
# StatCan provides the period in YYYY-MM format.
population_clean["ref_date"] = pd.to_datetime(
    population_clean["ref_date"],
    format="%Y-%m",
    errors="coerce"
)

# Ensure population observations are numeric
population_clean["value"] = pd.to_numeric(
    population_clean["value"],
    errors="coerce"
)

print(
    f"Cleaned rows: {len(population_clean):,}"
)

print(
    f"Cleaned columns: {len(population_clean.columns)}"
)

print("\nSelected data types:")

print(
    population_clean[
        [
            "ref_date",
            "geo",
            "uom",
            "value",
            "status"
        ]
    ].dtypes
)

# COMMAND ----------

# =========================================================
# REMOVE NON-ANALYTICAL SOURCE COLUMNS
# =========================================================

# SYMBOL and TERMINATED are not required in the
# canonical Population Silver dataset.

columns_to_drop = [
    "SYMBOL",
    "TERMINATED"
]

existing_columns_to_drop = [
    column
    for column in columns_to_drop
    if column in population_clean.columns
]

population_clean = population_clean.drop(
    columns=existing_columns_to_drop
)

print(
    "Removed columns:",
    existing_columns_to_drop
)

print(
    f"Remaining columns: {len(population_clean.columns)}"
)

# COMMAND ----------

# =========================================================
# VALIDATE CLEANING RESULTS
# =========================================================

print(
    f"Rows after cleaning: "
    f"{len(population_clean):,}"
)

print(
    f"Invalid ref_date values: "
    f"{population_clean['ref_date'].isna().sum():,}"
)

print(
    f"Missing population values: "
    f"{population_clean['value'].isna().sum():,}"
)

print(
    "Date range:",
    population_clean["ref_date"].min(),
    "to",
    population_clean["ref_date"].max()
)

print("\nUnits:")

print(
    population_clean["uom"]
    .value_counts(dropna=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validate Records and Separate Quarantine
# MAGIC
# MAGIC A Population observation is eligible for Silver when it contains:
# MAGIC
# MAGIC - a valid reporting date,
# MAGIC - a geography,
# MAGIC - a population unit,
# MAGIC - a Statistics Canada vector identifier,
# MAGIC - a numeric population value.
# MAGIC
# MAGIC Records that fail these requirements are preserved in quarantine rather than silently removed.

# COMMAND ----------

# =========================================================
# CLASSIFY VALID AND QUARANTINE RECORDS
# =========================================================

required_fields = [
    "ref_date",
    "geo",
    "uom",
    "vector",
    "value"
]

invalid_mask = (
    population_clean[required_fields]
    .isna()
    .any(axis=1)
)

population_valid = (
    population_clean[~invalid_mask]
    .copy()
)

population_quarantine = (
    population_clean[invalid_mask]
    .copy()
)

print(
    f"Total records:      {len(population_clean):,}"
)

print(
    f"Valid records:      {len(population_valid):,}"
)

print(
    f"Quarantine records: {len(population_quarantine):,}"
)

# COMMAND ----------

# =========================================================
# PREPARE QUARANTINE METADATA
# =========================================================

from datetime import datetime, timezone

if len(population_quarantine) > 0:

    population_quarantine["quarantine_reason"] = (
        "Missing required Population field"
    )

    population_quarantine["source_name"] = SOURCE_NAME
    population_quarantine["table_id"] = TABLE_ID
    population_quarantine["source_file"] = BRONZE_FILE_NAME

    population_quarantine["quarantine_timestamp_utc"] = (
        datetime.now(timezone.utc)
    )

    print(
        f"Quarantine metadata prepared for "
        f"{len(population_quarantine):,} records."
    )

else:

    print(
        "No records require quarantine."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Define and Validate the Population Business Key
# MAGIC
# MAGIC One population estimate is identified by its reporting period and geography.
# MAGIC
# MAGIC The candidate Silver business key is:
# MAGIC
# MAGIC `ref_date + geo + uom`
# MAGIC
# MAGIC The key is tested before it is used for Delta MERGE operations.

# COMMAND ----------

# =========================================================
# TEST POPULATION BUSINESS KEY
# =========================================================

candidate_business_key = [
    "ref_date",
    "geo",
    "uom"
]

duplicate_business_keys = (
    population_valid
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
        population_valid.loc[
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

if len(population_quarantine) > 0:

    population_quarantine_spark = (
        spark.createDataFrame(population_quarantine)
    )

    (
        population_quarantine_spark
        .write
        .format("delta")
        .mode("overwrite")
        .save(QUARANTINE_PATH)
    )

    print(
        f"Quarantine Delta written successfully: "
        f"{len(population_quarantine):,} records."
    )

else:

    print(
        "No quarantine records to write."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Validate Silver-Ready Population Data
# MAGIC
# MAGIC Before writing to Silver, confirm that:
# MAGIC
# MAGIC - required analytical fields are complete,
# MAGIC - complete duplicate rows do not exist,
# MAGIC - the validated business key remains unique,
# MAGIC - population values are numeric,
# MAGIC - reporting dates are valid.
# MAGIC
# MAGIC Only validated population observations are written to the Silver Delta table.

# COMMAND ----------

# =========================================================
# SILVER QUALITY VALIDATION
# =========================================================

required_null_counts = (
    population_valid[required_fields]
    .isna()
    .sum()
)

print("Required-field null counts:")
print(required_null_counts)

duplicate_rows = (
    population_valid
    .duplicated()
    .sum()
)

print(
    f"\nDuplicate rows: {duplicate_rows:,}"
)

business_key_duplicates = (
    population_valid
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
    population_valid["ref_date"].min(),
    "to",
    population_valid["ref_date"].max()
)

assert required_null_counts.sum() == 0, (
    "Required Population fields contain null values."
)

assert duplicate_rows == 0, (
    "Duplicate Population rows detected."
)

assert business_key_duplicates == 0, (
    "Duplicate Population business keys detected."
)

print(
    "\nSilver quality validation passed."
)

# COMMAND ----------

# =========================================================
# PREPARE SILVER SPARK DATAFRAME
# =========================================================
from pyspark.sql import functions as F

# Convert the validated Population dataset to Spark for Delta processing
population_silver_spark = spark.createDataFrame(population_valid)

# Preserve Statistics Canada identifier/status fields with stable data types
population_silver_spark = (
    population_silver_spark
    .withColumn("coordinate", F.col("coordinate").cast("string"))
    .withColumn("status", F.col("status").cast("string"))
)

print(f"Silver candidate rows: {population_silver_spark.count():,}")

print("\nSilver schema:")
population_silver_spark.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Write Population Data to Silver Delta
# MAGIC
# MAGIC Write the validated Population dataset to the Silver layer using Delta format.
# MAGIC
# MAGIC The first run creates the Delta table. Subsequent runs use Delta MERGE so the notebook can be rerun safely without creating duplicate observations.
# MAGIC
# MAGIC ### Business Key
# MAGIC
# MAGIC `ref_date + geo + uom`

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
        AND target.uom = source.uom
    """

    (
        silver_delta.alias("target")
        .merge(
            population_silver_spark.alias("source"),
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
        population_silver_spark
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

assert silver_row_count == len(population_valid), (
    "Silver row count does not match "
    "the validated Population row count."
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
# MAGIC ## 11. Idempotency Test
# MAGIC
# MAGIC Confirm that rerunning the same Population MERGE does not create duplicate records.
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
    AND target.uom = source.uom
"""

(
    silver_delta.alias("target")
    .merge(
        population_silver_spark.alias("source"),
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
# MAGIC The Statistics Canada Population Estimates Bronze-to-Silver transformation is complete.
# MAGIC
# MAGIC ### Silver Output
# MAGIC
# MAGIC `abfss://silver@stceidev01.dfs.core.windows.net/statcan/population/`
# MAGIC
# MAGIC ### Quarantine Output
# MAGIC
# MAGIC `abfss://silver@stceidev01.dfs.core.windows.net/quarantine/statcan/population/`
# MAGIC
# MAGIC ### Silver Business Key
# MAGIC
# MAGIC `ref_date + geo + uom`
# MAGIC
# MAGIC ### Design Notes
# MAGIC
# MAGIC - Bronze data remains unchanged.
# MAGIC - Population observations remain in persons.
# MAGIC - Geography is preserved as the primary analytical dimension.
# MAGIC - Invalid observations are not silently removed.
# MAGIC - Records failing Silver validation are preserved in quarantine.
# MAGIC - Delta MERGE supports safe reruns without duplicate observations.

# COMMAND ----------

# =========================================================
# FINAL SILVER DATA PREVIEW
# =========================================================
# Purpose:
# Read the persisted Silver Delta table after all processing,
# validation, MERGE, and idempotency checks are complete.
#
# This cell is only for a quick visual verification of the
# final stored output and does not modify the dataset.

silver_final = (
    spark.read
    .format("delta")
    .load(SILVER_PATH)
)

display(silver_final.limit(5))