# Databricks notebook source
# MAGIC %md
# MAGIC # Statistics Canada Labour Force — Bronze to Silver
# MAGIC
# MAGIC **Source:** Statistics Canada  
# MAGIC **Table:** 14-10-0287-03  
# MAGIC **Dataset:** Labour force characteristics by province, monthly, seasonally adjusted  
# MAGIC **Pipeline Layer:** Bronze → Silver  
# MAGIC **Notebook:** `03_silver_statcan_labour_force`
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC Transform the raw Statistics Canada Labour Force dataset from the Bronze layer into a cleaned, standardized, and validated Silver Delta dataset.
# MAGIC
# MAGIC The notebook preserves the analytical dimensions required for labour-market analysis while separating unavailable or suppressed observations into quarantine.
# MAGIC
# MAGIC ### Processing Flow
# MAGIC
# MAGIC `Bronze ZIP → Inspect → Clean → Validate → Quarantine / Valid → Silver Delta`
# MAGIC
# MAGIC The original Bronze source remains unchanged.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration and Source Access
# MAGIC
# MAGIC Define the Bronze and Silver locations and confirm that the expected Statistics Canada source ZIP is available.
# MAGIC
# MAGIC The ZIP is read directly from ADLS as binary content and opened in memory so the notebook remains compatible with Databricks serverless/shared compute.

# COMMAND ----------

# =========================================================
# 1. CONFIGURATION AND SOURCE ACCESS
# =========================================================

import io
import zipfile
import pandas as pd

from delta.tables import DeltaTable
from pyspark.sql import functions as F

BRONZE_PATH = (
    "abfss://bronze@stceidev01.dfs.core.windows.net/"
    "statcan/labour_force/14100287_raw.zip"
)

SILVER_PATH = (
    "abfss://silver@stceidev01.dfs.core.windows.net/"
    "statcan/labour_force/"
)

QUARANTINE_PATH = (
    "abfss://silver@stceidev01.dfs.core.windows.net/"
    "quarantine/statcan/labour_force/"
)

TABLE_ID = "14100287"
MAIN_CSV = f"{TABLE_ID}.csv"
METADATA_CSV = f"{TABLE_ID}_MetaData.csv"

print("Configuration loaded.")
print("Bronze:", BRONZE_PATH)
print("Silver:", SILVER_PATH)
print("Quarantine:", QUARANTINE_PATH)

# COMMAND ----------

# Confirm the expected Bronze source exists
bronze_files = (
    spark.read.format("binaryFile")
    .load(
        "abfss://bronze@stceidev01.dfs.core.windows.net/"
        "statcan/labour_force/"
    )
    .select("path", "modificationTime", "length")
)

display(bronze_files)

source_exists = (
    bronze_files
    .filter(F.col("path") == BRONZE_PATH)
    .count()
    > 0
)

assert source_exists, f"Bronze source not found: {BRONZE_PATH}"

print("Bronze source validation passed.")

# COMMAND ----------

# Read the Statistics Canada ZIP directly from ADLS
binary_row = (
    spark.read.format("binaryFile")
    .load(BRONZE_PATH)
    .select("content")
    .first()
)

assert binary_row is not None, "Unable to read Bronze ZIP."

zip_bytes = bytes(binary_row["content"])

with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zip_ref:
    zip_files = zip_ref.namelist()

print(f"Bronze ZIP loaded successfully: {len(zip_bytes):,} bytes")
print("Files inside ZIP:")

for file_name in zip_files:
    print(" -", file_name)

assert MAIN_CSV in zip_files, (
    f"Expected Statistics Canada CSV not found: {MAIN_CSV}"
)

print("\nStatistics Canada Labour Force source loaded successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load and Inspect Raw Labour Force Data
# MAGIC
# MAGIC Load the main Statistics Canada CSV and inspect the raw source before defining Silver transformations.
# MAGIC
# MAGIC The inspection confirms:
# MAGIC
# MAGIC - row and column counts,
# MAGIC - source dimensions,
# MAGIC - geography coverage,
# MAGIC - reporting-period coverage,
# MAGIC - statistical measures,
# MAGIC - published units,
# MAGIC - source-quality fields.
# MAGIC
# MAGIC No source values are changed during this stage.

# COMMAND ----------

# =========================================================
# 2. LOAD AND INSPECT RAW LABOUR FORCE DATA
# =========================================================

with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zip_ref:
    with zip_ref.open(MAIN_CSV) as csv_file:
        labour_raw = pd.read_csv(csv_file, low_memory=False)

print(f"Raw rows: {len(labour_raw):,}")
print(f"Raw columns: {len(labour_raw.columns):,}")

print("\nColumn names:")
for column_name in labour_raw.columns:
    print(" -", column_name)

display(labour_raw.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Profile Raw Data Quality
# MAGIC
# MAGIC Inspect the raw Labour Force dataset before cleaning.
# MAGIC
# MAGIC The profile checks:
# MAGIC
# MAGIC - missing values,
# MAGIC - complete duplicate rows,
# MAGIC - reporting-period coverage,
# MAGIC - missing observations,
# MAGIC - source status flags.
# MAGIC
# MAGIC These results will determine the Labour Force-specific Silver validation and quarantine rules.

# COMMAND ----------

# =========================================================
# 3. PROFILE RAW DATA QUALITY
# =========================================================

missing_summary = (
    labour_raw.isna()
    .sum()
    .reset_index()
)

missing_summary.columns = [
    "column_name",
    "missing_count"
]

missing_summary = missing_summary[
    missing_summary["missing_count"] > 0
].sort_values(
    "missing_count",
    ascending=False
)

print("Columns containing missing values:")
display(missing_summary)

duplicate_rows = labour_raw.duplicated().sum()

print(f"Duplicate rows: {duplicate_rows:,}")

raw_dates = pd.to_datetime(
    labour_raw["REF_DATE"],
    format="%Y-%m",
    errors="coerce"
)

print(
    "Raw reporting period:",
    raw_dates.min().strftime("%Y-%m"),
    "to",
    raw_dates.max().strftime("%Y-%m")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Inspect Labour Force Dimensions and Measures
# MAGIC
# MAGIC Inspect the main analytical dimensions before defining the canonical Silver schema.
# MAGIC
# MAGIC Important dimensions include:
# MAGIC
# MAGIC - geography,
# MAGIC - labour force characteristic,
# MAGIC - gender,
# MAGIC - age group,
# MAGIC - statistics,
# MAGIC - seasonal-adjustment/data type,
# MAGIC - unit,
# MAGIC - scalar factor.
# MAGIC
# MAGIC These dimensions are required to distinguish one published Statistics Canada observation from another.

# COMMAND ----------

# =========================================================
# 4. INSPECT LABOUR FORCE DIMENSIONS AND MEASURES
# =========================================================

dimension_columns = [
    "GEO",
    "Labour force characteristics",
    "Gender",
    "Age group",
    "Statistics",
    "Data type",
    "UOM",
    "SCALAR_FACTOR",
    "STATUS"
]

for column_name in dimension_columns:
    print(f"\n--- {column_name} ---")

    print(
        labour_raw[column_name]
        .fillna("NULL")
        .value_counts()
        .head(20)
    )

# COMMAND ----------

# =========================================================
# INSPECT MISSING LABOUR FORCE VALUES
# =========================================================

missing_value_rows = labour_raw[
    labour_raw["VALUE"].isna()
].copy()

print(
    f"Rows with missing VALUE: "
    f"{len(missing_value_rows):,}"
)

print("\nSTATUS distribution for missing VALUE rows:")

print(
    missing_value_rows["STATUS"]
    .fillna("NULL")
    .value_counts()
)

display(missing_value_rows.head(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Clean and Standardize Labour Force Data
# MAGIC
# MAGIC Standardize the Statistics Canada Labour Force source into a consistent Silver-ready structure.
# MAGIC
# MAGIC This step:
# MAGIC
# MAGIC - renames source columns using consistent lowercase names,
# MAGIC - converts the reporting period to a proper date,
# MAGIC - converts published observations to numeric values,
# MAGIC - preserves labour-market dimensions such as gender, age group, statistics and data type,
# MAGIC - preserves Statistics Canada source identifiers and status information.
# MAGIC
# MAGIC Published values are retained in their original statistical units. Unit normalization can be derived later where required for Gold-layer analysis.

# COMMAND ----------

# =========================================================
# 5. CLEAN AND STANDARDIZE LABOUR FORCE DATA
# =========================================================

labour_clean = labour_raw.rename(
    columns={
        "REF_DATE": "ref_date",
        "GEO": "geo",
        "DGUID": "dguid",
        "Labour force characteristics":
            "labour_force_characteristic",
        "Gender": "gender",
        "Age group": "age_group",
        "Statistics": "statistics",
        "Data type": "data_type",
        "UOM": "uom",
        "UOM_ID": "uom_id",
        "SCALAR_FACTOR": "scalar_factor",
        "SCALAR_ID": "scalar_id",
        "VECTOR": "vector",
        "COORDINATE": "coordinate",
        "VALUE": "value",
        "STATUS": "status",
        "SYMBOL": "symbol",
        "TERMINATED": "terminated",
        "DECIMALS": "decimals"
    }
).copy()

# Standardize reporting period
labour_clean["ref_date"] = pd.to_datetime(
    labour_clean["ref_date"],
    format="%Y-%m",
    errors="coerce"
)

# Ensure published observations are numeric
labour_clean["value"] = pd.to_numeric(
    labour_clean["value"],
    errors="coerce"
)

print(f"Cleaned rows: {len(labour_clean):,}")
print(f"Cleaned columns: {len(labour_clean.columns):,}")

print("\nSelected data types:")
print(
    labour_clean[
        [
            "ref_date",
            "geo",
            "labour_force_characteristic",
            "gender",
            "age_group",
            "statistics",
            "data_type",
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

# SYMBOL and TERMINATED are not required in the canonical Silver output.
columns_to_remove = [
    column_name
    for column_name in ["symbol", "terminated"]
    if column_name in labour_clean.columns
]

labour_clean = labour_clean.drop(
    columns=columns_to_remove
)

print("Removed columns:", columns_to_remove)
print(
    "Remaining columns:",
    len(labour_clean.columns)
)

# COMMAND ----------

# =========================================================
# INSPECT ADDITIONAL LABOUR FORCE DIMENSIONS
# =========================================================

for column_name in [
    "gender",
    "statistics",
    "data_type"
]:
    print(f"\n--- {column_name} ---")

    print(
        labour_clean[column_name]
        .fillna("NULL")
        .value_counts()
    )

print(
    "\nRows after cleaning:",
    f"{len(labour_clean):,}"
)

print(
    "Invalid ref_date values:",
    f"{labour_clean['ref_date'].isna().sum():,}"
)

print(
    "Missing VALUE observations:",
    f"{labour_clean['value'].isna().sum():,}"
)

print(
    "Date range:",
    labour_clean["ref_date"].min(),
    "to",
    labour_clean["ref_date"].max()
)

print("\nPublished units:")
print(
    labour_clean["uom"]
    .value_counts()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validate Records and Separate Quarantine
# MAGIC
# MAGIC A Labour Force observation is eligible for Silver when all analytical dimensions are present and Statistics Canada provides a numeric published value.
# MAGIC
# MAGIC Rows with unavailable or suppressed observations are not interpreted as zero.
# MAGIC
# MAGIC Instead, those records are preserved in quarantine together with their Statistics Canada status codes so that the original source information remains traceable.

# COMMAND ----------

# =========================================================
# 6. VALIDATE RECORDS AND SEPARATE QUARANTINE
# =========================================================

required_dimensions = [
    "ref_date",
    "geo",
    "labour_force_characteristic",
    "gender",
    "age_group",
    "statistics",
    "data_type",
    "uom",
    "vector"
]

valid_mask = (
    labour_clean[required_dimensions]
    .notna()
    .all(axis=1)
    &
    labour_clean["value"].notna()
)

labour_valid = (
    labour_clean[valid_mask]
    .copy()
)

labour_quarantine = (
    labour_clean[~valid_mask]
    .copy()
)

print(
    f"Total records:      "
    f"{len(labour_clean):,}"
)

print(
    f"Valid records:      "
    f"{len(labour_valid):,}"
)

print(
    f"Quarantine records: "
    f"{len(labour_quarantine):,}"
)

print("\nQuarantine STATUS distribution:")

print(
    labour_quarantine["status"]
    .fillna("NULL")
    .value_counts()
)

# COMMAND ----------

# Preserve rejected-record context for traceability
labour_quarantine["quarantine_reason"] = (
    "Missing required dimension or published VALUE"
)

labour_quarantine["quarantine_timestamp"] = (
    pd.Timestamp.utcnow()
)

print(
    "Quarantine metadata prepared for",
    f"{len(labour_quarantine):,}",
    "records."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Define and Validate the Labour Force Business Key
# MAGIC
# MAGIC One Labour Force observation is distinguished by its reporting period and analytical dimensions.
# MAGIC
# MAGIC ### Silver Business Key
# MAGIC
# MAGIC `ref_date + geo + labour_force_characteristic + gender + age_group + statistics + data_type + uom`
# MAGIC
# MAGIC The candidate key is validated before being used in Delta MERGE operations.

# COMMAND ----------

# =========================================================
# 7. DEFINE AND VALIDATE BUSINESS KEY
# =========================================================

BUSINESS_KEY = [
    "ref_date",
    "geo",
    "labour_force_characteristic",
    "gender",
    "age_group",
    "statistics",
    "data_type",
    "uom"
]

duplicate_business_keys = (
    labour_valid
    .duplicated(
        subset=BUSINESS_KEY,
        keep=False
    )
    .sum()
)

print(
    "Candidate business key:",
    " + ".join(BUSINESS_KEY)
)

print(
    "Rows involved in duplicate business keys:",
    f"{duplicate_business_keys:,}"
)

assert duplicate_business_keys == 0, (
    "Duplicate Labour Force business keys detected."
)

print("Candidate business key is unique.")

# COMMAND ----------

# Convert quarantine records to Spark
labour_quarantine_spark = (
    spark.createDataFrame(labour_quarantine)
    .withColumn(
        "status",
        F.col("status").cast("string")
    )
)

(
    labour_quarantine_spark
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(QUARANTINE_PATH)
)

print(
    "Quarantine Delta written successfully:",
    f"{labour_quarantine_spark.count():,}",
    "records."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Validate Silver-Ready Labour Force Data
# MAGIC
# MAGIC Before writing the validated observations to Silver, confirm that:
# MAGIC
# MAGIC - required analytical fields are complete,
# MAGIC - complete duplicate rows do not exist,
# MAGIC - the business key remains unique,
# MAGIC - published values are numeric,
# MAGIC - reporting dates are valid.
# MAGIC
# MAGIC The `status` field is explicitly cast to `string` before the Delta write so that an all-null valid-source column does not become a Spark `void` type.

# COMMAND ----------

# =========================================================
# 8. VALIDATE SILVER-READY DATA
# =========================================================

required_silver_fields = [
    "ref_date",
    "geo",
    "labour_force_characteristic",
    "gender",
    "age_group",
    "statistics",
    "data_type",
    "uom",
    "vector",
    "value"
]

required_null_counts = (
    labour_valid[
        required_silver_fields
    ]
    .isna()
    .sum()
)

print("Required-field null counts:")
print(required_null_counts)

duplicate_rows = (
    labour_valid
    .duplicated()
    .sum()
)

duplicate_business_keys = (
    labour_valid
    .duplicated(
        subset=BUSINESS_KEY
    )
    .sum()
)

print(
    "\nDuplicate rows:",
    f"{duplicate_rows:,}"
)

print(
    "Duplicate business keys:",
    f"{duplicate_business_keys:,}"
)

print(
    "\nSilver reporting period:",
    labour_valid["ref_date"].min(),
    "to",
    labour_valid["ref_date"].max()
)

assert required_null_counts.sum() == 0
assert duplicate_rows == 0
assert duplicate_business_keys == 0

print("\nSilver quality validation passed.")

# COMMAND ----------

# Convert validated pandas records to Spark
labour_silver_spark = spark.createDataFrame(
    labour_valid
)

# Explicit schema-quality handling:
# STATUS is normally null for valid published observations.
# Cast it to string so Delta does not persist it as a void type.
labour_silver_spark = (
    labour_silver_spark
    .withColumn(
        "status",
        F.col("status").cast("string")
    )
    .withColumn(
        "coordinate",
        F.col("coordinate").cast("string")
    )
)

silver_candidate_count = (
    labour_silver_spark.count()
)

print(
    "Silver candidate rows:",
    f"{silver_candidate_count:,}"
)

print("\nSilver schema:")
labour_silver_spark.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Write Labour Force Data to Silver Delta
# MAGIC
# MAGIC Write the validated Labour Force observations to the Silver layer using Delta format.
# MAGIC
# MAGIC The first run creates the Delta table. Subsequent runs use Delta MERGE so the notebook can be rerun safely and can incorporate revised Statistics Canada observations without creating duplicate business keys.
# MAGIC
# MAGIC ### Business Key
# MAGIC
# MAGIC `ref_date + geo + labour_force_characteristic + gender + age_group + statistics + data_type + uom`

# COMMAND ----------

# =========================================================
# 9. WRITE LABOUR FORCE DATA TO SILVER DELTA
# =========================================================

merge_condition = """
target.ref_date = source.ref_date
AND target.geo = source.geo
AND target.labour_force_characteristic =
    source.labour_force_characteristic
AND target.gender = source.gender
AND target.age_group = source.age_group
AND target.statistics = source.statistics
AND target.data_type = source.data_type
AND target.uom = source.uom
"""

if DeltaTable.isDeltaTable(
    spark,
    SILVER_PATH
):

    silver_delta = DeltaTable.forPath(
        spark,
        SILVER_PATH
    )

    (
        silver_delta.alias("target")
        .merge(
            labour_silver_spark.alias("source"),
            merge_condition
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(
        "Existing Silver Delta table found."
    )

    print(
        "MERGE completed successfully."
    )

else:

    (
        labour_silver_spark
        .write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true"
        )
        .save(SILVER_PATH)
    )

    print(
        "No existing Silver Delta table found."
    )

    print(
        "Initial Silver Delta table "
        "created successfully."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Post-Load Validation
# MAGIC
# MAGIC Validate the persisted Silver Delta table after writing.
# MAGIC
# MAGIC The checks confirm:
# MAGIC
# MAGIC - expected records were written,
# MAGIC - reporting-period coverage is correct,
# MAGIC - the Delta table can be read successfully,
# MAGIC - the business key remains unique,
# MAGIC - the persisted schema contains stable data types.

# COMMAND ----------

# =========================================================
# 10. POST-LOAD VALIDATION
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
    "Silver Delta rows:",
    f"{silver_row_count:,}"
)

print("\nSilver reporting period:")

(
    silver_check
    .agg(
        F.min("ref_date").alias(
            "min_ref_date"
        ),
        F.max("ref_date").alias(
            "max_ref_date"
        )
    )
    .show()
)

assert silver_row_count == silver_candidate_count

print("\nPersisted Silver schema:")
silver_check.printSchema()

assert (
    dict(silver_check.dtypes)["status"]
    == "string"
), "STATUS must be persisted as string."

assert (
    dict(silver_check.dtypes)["coordinate"]
    == "string"
), "COORDINATE must be persisted as string."

print("\nPost-load validation passed.")

# COMMAND ----------

# =========================================================
# VALIDATE BUSINESS KEY IN SILVER
# =========================================================

duplicate_key_check = (
    silver_check
    .groupBy(*BUSINESS_KEY)
    .count()
    .filter(
        F.col("count") > 1
    )
)

duplicate_key_count = (
    duplicate_key_check.count()
)

print(
    "Duplicate Silver business keys:",
    duplicate_key_count
)

assert duplicate_key_count == 0

print(
    "Silver business key validation passed."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Idempotency Test
# MAGIC
# MAGIC Confirm that rerunning the same Labour Force MERGE does not create duplicate observations.
# MAGIC
# MAGIC A successful test means the Silver row count remains unchanged when the same validated source observations are merged again.

# COMMAND ----------

# =========================================================
# 11. IDEMPOTENCY TEST
# =========================================================

rows_before_merge = (
    spark.read
    .format("delta")
    .load(SILVER_PATH)
    .count()
)

silver_delta = DeltaTable.forPath(
    spark,
    SILVER_PATH
)

(
    silver_delta.alias("target")
    .merge(
        labour_silver_spark.alias("source"),
        merge_condition
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

rows_after_merge = (
    spark.read
    .format("delta")
    .load(SILVER_PATH)
    .count()
)

print(
    "Rows before MERGE:",
    f"{rows_before_merge:,}"
)

print(
    "Rows after MERGE: ",
    f"{rows_after_merge:,}"
)

assert (
    rows_before_merge
    == rows_after_merge
), "Idempotency validation failed."

print("\nIdempotency test passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Processing Summary
# MAGIC
# MAGIC The Statistics Canada Labour Force Bronze-to-Silver transformation is complete.
# MAGIC
# MAGIC ### Silver Output
# MAGIC
# MAGIC `abfss://silver@stceidev01.dfs.core.windows.net/statcan/labour_force/`
# MAGIC
# MAGIC ### Quarantine Output
# MAGIC
# MAGIC `abfss://silver@stceidev01.dfs.core.windows.net/quarantine/statcan/labour_force/`
# MAGIC
# MAGIC ### Silver Business Key
# MAGIC
# MAGIC `ref_date + geo + labour_force_characteristic + gender + age_group + statistics + data_type + uom`
# MAGIC
# MAGIC ### Design Notes
# MAGIC
# MAGIC - Bronze data remains unchanged.
# MAGIC - Published Statistics Canada labour-market values remain in their original statistical units.
# MAGIC - Missing or non-publishable observations are not interpreted as zero.
# MAGIC - Unavailable or suppressed observations are preserved in quarantine with their source status information.
# MAGIC - `status` is explicitly persisted as a string to maintain a stable Delta schema.
# MAGIC - `coordinate` is preserved as a string because it functions as a source identifier rather than a numeric analytical measure.
# MAGIC - Delta MERGE supports idempotent reruns and source revisions without creating duplicate business keys.

# COMMAND ----------

# Display five rows from the persisted Silver Delta dataset
silver_final = (
    spark.read
    .format("delta")
    .load(SILVER_PATH)
)

display(silver_final.limit(5))