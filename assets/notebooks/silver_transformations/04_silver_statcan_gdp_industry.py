# Databricks notebook source
# MAGIC %md
# MAGIC # Statistics Canada GDP by Industry — Bronze to Silver
# MAGIC
# MAGIC **Source:** Statistics Canada  
# MAGIC **Table:** 36-10-0434-01  
# MAGIC **Dataset:** Gross domestic product (GDP) at basic prices, by industry, monthly  
# MAGIC **Pipeline Layer:** Bronze → Silver  
# MAGIC **Notebook:** `04_silver_statcan_gdp_industry`
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC Transform the raw Statistics Canada GDP by Industry dataset from the Bronze layer into a cleaned, standardized and validated Silver Delta dataset.
# MAGIC
# MAGIC This notebook follows the reusable Bronze-to-Silver framework while keeping GDP-specific transformations and validation rules separate.
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

TABLE_ID = "36100434"
SOURCE_NAME = "Statistics Canada"
DATASET_NAME = "GDP by Industry"

BRONZE_FOLDER = (
    "abfss://bronze@stceidev01.dfs.core.windows.net/"
    "statcan/gdp_industry/"
)

BRONZE_FILE_NAME = "36100434_raw.zip"

BRONZE_PATH = (
    f"{BRONZE_FOLDER}{BRONZE_FILE_NAME}"
)

SILVER_PATH = (
    "abfss://silver@stceidev01.dfs.core.windows.net/"
    "statcan/gdp_industry/"
)

QUARANTINE_PATH = (
    "abfss://silver@stceidev01.dfs.core.windows.net/"
    "quarantine/statcan/gdp_industry/"
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
    "\nStatistics Canada GDP source "
    "loaded and extracted successfully."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load and Inspect Raw GDP Data
# MAGIC
# MAGIC Load the primary Statistics Canada GDP CSV and inspect its structure before defining Silver transformations.
# MAGIC
# MAGIC This step identifies:
# MAGIC
# MAGIC - source row and column counts,
# MAGIC - available GDP and industry dimensions,
# MAGIC - measurement units,
# MAGIC - reporting-period coverage,
# MAGIC - source quality fields.
# MAGIC
# MAGIC No source values are changed during this stage.

# COMMAND ----------

# =========================================================
# LOAD RAW GDP DATA
# =========================================================

import pandas as pd

gdp_raw = pd.read_csv(
    io.BytesIO(csv_bytes),
    low_memory=False
)

print(
    f"Raw rows: {len(gdp_raw):,}"
)

print(
    f"Raw columns: {len(gdp_raw.columns)}"
)

print("\nColumn names:")

for column in gdp_raw.columns:
    print(" -", column)

display(
    gdp_raw.head(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Profile Raw Data Quality
# MAGIC
# MAGIC Inspect the raw GDP dataset before cleaning.
# MAGIC
# MAGIC The profile checks:
# MAGIC
# MAGIC - missing values,
# MAGIC - complete duplicate rows,
# MAGIC - reporting-period coverage,
# MAGIC - missing GDP observations,
# MAGIC - Statistics Canada status information.
# MAGIC
# MAGIC These results will determine the GDP-specific Silver validation and quarantine rules.

# COMMAND ----------

# =========================================================
# RAW DATA QUALITY PROFILE
# =========================================================

missing_summary = (
    gdp_raw
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

# Check for complete duplicate source rows
duplicate_count = (
    gdp_raw
    .duplicated()
    .sum()
)

print(
    f"Duplicate rows: {duplicate_count:,}"
)

# Confirm the available historical period
print(
    "\nRaw reporting period:",
    gdp_raw["REF_DATE"].min(),
    "to",
    gdp_raw["REF_DATE"].max()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Inspect GDP Dimensions and Measures
# MAGIC
# MAGIC Inspect the major dimensions and measurement fields before defining the canonical Silver schema.
# MAGIC
# MAGIC This helps determine:
# MAGIC
# MAGIC - industry classifications,
# MAGIC - GDP measures,
# MAGIC - price representation,
# MAGIC - units and scalar factors,
# MAGIC - source status flags,
# MAGIC - the fields required to uniquely identify one GDP observation.

# COMMAND ----------

# =========================================================
# INSPECT GDP DIMENSIONS
# =========================================================

# Inspect likely GDP dimensions only when they
# actually exist in the downloaded StatCan table.

possible_dimensions = [
    "GEO",
    "North American Industry Classification System (NAICS)",
    "Prices",
    "Seasonal adjustment",
    "Estimates",
    "UOM",
    "SCALAR_FACTOR",
    "STATUS"
]

for column in possible_dimensions:

    if column in gdp_raw.columns:

        print(
            f"\n--- {column} ---"
        )

        print(
            gdp_raw[column]
            .fillna("NULL")
            .value_counts(dropna=False)
            .head(30)
        )

# COMMAND ----------

# =========================================================
# INSPECT MISSING GDP VALUES
# =========================================================

# Missing GDP observations must be inspected before
# deciding whether they belong in Silver or quarantine.

if "VALUE" in gdp_raw.columns:

    missing_value_rows = (
        gdp_raw[
            gdp_raw["VALUE"].isna()
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

        if "STATUS" in gdp_raw.columns:

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
# MAGIC ## 5. Clean and Standardize GDP Data
# MAGIC
# MAGIC Standardize the Statistics Canada GDP source into a Silver-ready structure.
# MAGIC
# MAGIC This step:
# MAGIC
# MAGIC - renames source fields using consistent lowercase names,
# MAGIC - converts the reporting period to a proper date,
# MAGIC - converts GDP observations to numeric values,
# MAGIC - preserves industry, price and seasonal-adjustment dimensions,
# MAGIC - preserves Statistics Canada source identifiers and status information.
# MAGIC
# MAGIC GDP values remain in their published unit of millions of dollars.

# COMMAND ----------

# =========================================================
# CLEAN AND STANDARDIZE GDP DATA
# =========================================================

# Work on a copy so the raw Bronze source remains unchanged
gdp_clean = gdp_raw.copy()

# Rename StatCan fields to Silver-friendly names
gdp_clean = gdp_clean.rename(
    columns={
        "REF_DATE": "ref_date",
        "GEO": "geo",
        "DGUID": "dguid",
        "Seasonal adjustment": "seasonal_adjustment",
        "Prices": "prices",
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

# Convert monthly REF_DATE from YYYY-MM to a date
gdp_clean["ref_date"] = pd.to_datetime(
    gdp_clean["ref_date"],
    format="%Y-%m",
    errors="coerce"
)

# Convert published GDP observations to numeric
gdp_clean["value"] = pd.to_numeric(
    gdp_clean["value"],
    errors="coerce"
)

print(f"Cleaned rows: {len(gdp_clean):,}")
print(f"Cleaned columns: {len(gdp_clean.columns)}")

print("\nSelected data types:")

print(
    gdp_clean[
        [
            "ref_date",
            "geo",
            "seasonal_adjustment",
            "prices",
            "industry",
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
# canonical analytical Silver dataset.
# STATUS is retained for source transparency.

columns_to_drop = [
    "SYMBOL",
    "TERMINATED"
]

existing_columns_to_drop = [
    column
    for column in columns_to_drop
    if column in gdp_clean.columns
]

gdp_clean = gdp_clean.drop(
    columns=existing_columns_to_drop
)

print(
    "Removed columns:",
    existing_columns_to_drop
)

print(
    f"Remaining columns: {len(gdp_clean.columns)}"
)

# COMMAND ----------

# =========================================================
# VALIDATE CLEANING RESULTS
# =========================================================

print(
    f"Rows after cleaning: "
    f"{len(gdp_clean):,}"
)

print(
    f"Invalid ref_date values: "
    f"{gdp_clean['ref_date'].isna().sum():,}"
)

print(
    f"Missing GDP values: "
    f"{gdp_clean['value'].isna().sum():,}"
)

print(
    "Date range:",
    gdp_clean["ref_date"].min(),
    "to",
    gdp_clean["ref_date"].max()
)

print("\nPrice types:")

print(
    gdp_clean["prices"]
    .value_counts(dropna=False)
)

print("\nSeasonal adjustment types:")

print(
    gdp_clean["seasonal_adjustment"]
    .value_counts(dropna=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validate Records and Separate Quarantine
# MAGIC
# MAGIC A GDP observation is eligible for Silver when the required analytical dimensions are present and Statistics Canada provides a numeric published value.
# MAGIC
# MAGIC Rows with unavailable GDP observations are not interpreted as zero.
# MAGIC
# MAGIC Instead, those records are preserved in quarantine together with their Statistics Canada status information.

# COMMAND ----------

# =========================================================
# CLASSIFY VALID AND QUARANTINE RECORDS
# =========================================================

required_fields = [
    "ref_date",
    "geo",
    "seasonal_adjustment",
    "prices",
    "industry",
    "uom",
    "vector",
    "value"
]

# Any missing required analytical field makes
# the row unsuitable for canonical Silver.
invalid_mask = (
    gdp_clean[required_fields]
    .isna()
    .any(axis=1)
)

gdp_valid = (
    gdp_clean[~invalid_mask]
    .copy()
)

gdp_quarantine = (
    gdp_clean[invalid_mask]
    .copy()
)

print(
    f"Total records:      {len(gdp_clean):,}"
)

print(
    f"Valid records:      {len(gdp_valid):,}"
)

print(
    f"Quarantine records: {len(gdp_quarantine):,}"
)

print("\nQuarantine STATUS distribution:")

print(
    gdp_quarantine["status"]
    .fillna("NULL")
    .value_counts(dropna=False)
)

# COMMAND ----------

# =========================================================
# PREPARE QUARANTINE METADATA
# =========================================================

from datetime import datetime, timezone

if len(gdp_quarantine) > 0:

    gdp_quarantine["quarantine_reason"] = (
        "Missing required field or non-publishable GDP observation"
    )

    gdp_quarantine["source_name"] = SOURCE_NAME
    gdp_quarantine["table_id"] = TABLE_ID
    gdp_quarantine["source_file"] = BRONZE_FILE_NAME

    gdp_quarantine["quarantine_timestamp_utc"] = (
        datetime.now(timezone.utc)
    )

    print(
        f"Quarantine metadata prepared for "
        f"{len(gdp_quarantine):,} records."
    )

else:

    print(
        "No records require quarantine."
    )

# COMMAND ----------

# =========================================================
# PREPARE QUARANTINE METADATA
# =========================================================

from datetime import datetime, timezone

if len(gdp_quarantine) > 0:

    gdp_quarantine["quarantine_reason"] = (
        "Missing required field or non-publishable GDP observation"
    )

    gdp_quarantine["source_name"] = SOURCE_NAME
    gdp_quarantine["table_id"] = TABLE_ID
    gdp_quarantine["source_file"] = BRONZE_FILE_NAME

    gdp_quarantine["quarantine_timestamp_utc"] = (
        datetime.now(timezone.utc)
    )

    print(
        f"Quarantine metadata prepared for "
        f"{len(gdp_quarantine):,} records."
    )

else:

    print(
        "No records require quarantine."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Define and Validate the GDP Business Key
# MAGIC
# MAGIC One GDP observation is identified by its reporting period and analytical dimensions.
# MAGIC
# MAGIC The candidate Silver business key is:
# MAGIC
# MAGIC `ref_date + geo + seasonal_adjustment + prices + industry + uom`
# MAGIC
# MAGIC The key is tested before it is used for Delta MERGE operations.

# COMMAND ----------

# =========================================================
# TEST GDP BUSINESS KEY
# =========================================================

candidate_business_key = [
    "ref_date",
    "geo",
    "seasonal_adjustment",
    "prices",
    "industry",
    "uom"
]

duplicate_business_keys = (
    gdp_valid
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
        gdp_valid.loc[
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

if len(gdp_quarantine) > 0:

    gdp_quarantine_spark = (
        spark.createDataFrame(gdp_quarantine)
    )

    (
        gdp_quarantine_spark
        .write
        .format("delta")
        .mode("overwrite")
        .save(QUARANTINE_PATH)
    )

    print(
        f"Quarantine Delta written successfully: "
        f"{len(gdp_quarantine):,} records."
    )

else:

    print(
        "No quarantine records to write."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Validate Silver-Ready GDP Data
# MAGIC
# MAGIC Before writing to Silver, confirm that:
# MAGIC
# MAGIC - required analytical fields are complete,
# MAGIC - complete duplicate rows do not exist,
# MAGIC - the validated business key remains unique,
# MAGIC - GDP values are numeric,
# MAGIC - reporting dates are valid.
# MAGIC
# MAGIC Only validated GDP observations are written to the Silver Delta table.

# COMMAND ----------

# =========================================================
# SILVER QUALITY VALIDATION
# =========================================================

required_null_counts = (
    gdp_valid[required_fields]
    .isna()
    .sum()
)

print("Required-field null counts:")
print(required_null_counts)

duplicate_rows = (
    gdp_valid
    .duplicated()
    .sum()
)

print(
    f"\nDuplicate rows: {duplicate_rows:,}"
)

business_key_duplicates = (
    gdp_valid
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
    gdp_valid["ref_date"].min(),
    "to",
    gdp_valid["ref_date"].max()
)

assert required_null_counts.sum() == 0, (
    "Required GDP fields contain null values."
)

assert duplicate_rows == 0, (
    "Duplicate GDP rows detected."
)

assert business_key_duplicates == 0, (
    "Duplicate GDP business keys detected."
)

print(
    "\nSilver quality validation passed."
)

# COMMAND ----------

# =========================================================
# PREPARE SILVER SPARK DATAFRAME
# =========================================================

from pyspark.sql import functions as F

gdp_silver_spark = spark.createDataFrame(gdp_valid)

# STATUS is normally null for valid published observations.
# Cast explicitly to string so Delta does not persist it as void.
gdp_silver_spark = (
    gdp_silver_spark
    .withColumn("status", F.col("status").cast("string"))
)

print(f"Silver candidate rows: {gdp_silver_spark.count():,}")

print("\nSilver schema:")
gdp_silver_spark.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Write GDP Data to Silver Delta
# MAGIC
# MAGIC Write the validated GDP dataset to the Silver layer using Delta format.
# MAGIC
# MAGIC The first run creates the Delta table. Subsequent runs use Delta MERGE so the notebook can be rerun safely without creating duplicate observations.
# MAGIC
# MAGIC ### Business Key
# MAGIC
# MAGIC `ref_date + geo + seasonal_adjustment + prices + industry + uom`

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
        AND target.seasonal_adjustment = source.seasonal_adjustment
        AND target.prices = source.prices
        AND target.industry = source.industry
        AND target.uom = source.uom
    """

    (
        silver_delta.alias("target")
        .merge(
            gdp_silver_spark.alias("source"),
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
        gdp_silver_spark
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

assert silver_row_count == len(gdp_valid), (
    "Silver row count does not match "
    "the validated GDP row count."
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
        "seasonal_adjustment",
        "prices",
        "industry",
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
# MAGIC Confirm that rerunning the same GDP MERGE does not create duplicate records.
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
    AND target.seasonal_adjustment = source.seasonal_adjustment
    AND target.prices = source.prices
    AND target.industry = source.industry
    AND target.uom = source.uom
"""

(
    silver_delta.alias("target")
    .merge(
        gdp_silver_spark.alias("source"),
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
# MAGIC The Statistics Canada GDP by Industry Bronze-to-Silver transformation is complete.
# MAGIC
# MAGIC ### Silver Output
# MAGIC
# MAGIC `abfss://silver@stceidev01.dfs.core.windows.net/statcan/gdp_industry/`
# MAGIC
# MAGIC ### Quarantine Output
# MAGIC
# MAGIC `abfss://silver@stceidev01.dfs.core.windows.net/quarantine/statcan/gdp_industry/`
# MAGIC
# MAGIC ### Silver Business Key
# MAGIC
# MAGIC `ref_date + geo + seasonal_adjustment + prices + industry + uom`
# MAGIC
# MAGIC ### Design Notes
# MAGIC
# MAGIC - Bronze data remains unchanged.
# MAGIC - GDP observations remain in the published unit of millions of dollars.
# MAGIC - Price and seasonal-adjustment dimensions are preserved.
# MAGIC - Missing or non-publishable observations are not treated as zero.
# MAGIC - Those records are preserved in quarantine with Statistics Canada status information.
# MAGIC - Delta MERGE supports safe reruns without duplicate observations.