# Databricks notebook source
# MAGIC %md
# MAGIC # Finance Canada Fiscal Reference Tables — Bronze to Silver
# MAGIC
# MAGIC **Source:** Department of Finance Canada  
# MAGIC **Source File:** Fiscal Reference Tables 2025  
# MAGIC **Dataset:** Federal and Ontario Fiscal Indicators  
# MAGIC **Pipeline Layer:** Bronze → Silver  
# MAGIC **Notebook:** `09_silver_finance_canada_fiscal`
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC Transform selected fiscal data from the Department of Finance Canada's Fiscal Reference Tables workbook from the Bronze layer into cleaned, standardized and validated Silver Delta datasets.
# MAGIC
# MAGIC The source is an Excel workbook containing multiple fiscal tables. This notebook first inspects the workbook structure before extracting the specific federal and Ontario tables required for the Canadian Economic Intelligence Platform.
# MAGIC
# MAGIC ### Target Fiscal Tables
# MAGIC
# MAGIC - **Federal:** Table 1
# MAGIC - **Ontario:** Table 23
# MAGIC
# MAGIC ### Processing Flow
# MAGIC
# MAGIC `Bronze Excel → Inspect Workbook → Identify Tables → Extract → Clean → Validate → Silver Delta`
# MAGIC
# MAGIC The original Finance Canada workbook remains unchanged in the Bronze layer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration and Source Access
# MAGIC
# MAGIC Define the Bronze and Silver locations and confirm that the Finance Canada Fiscal Reference Tables workbook exists in ADLS.
# MAGIC
# MAGIC Because the source is an Excel workbook, the file is first loaded from ADLS as binary content and opened in memory for inspection.
# MAGIC
# MAGIC No fiscal values are transformed at this stage.

# COMMAND ----------

# =========================================================
# CONFIGURATION
# =========================================================

SOURCE_NAME = "Department of Finance Canada"
DATASET_NAME = "Fiscal Reference Tables 2025"

BRONZE_FOLDER = (
    "abfss://bronze@stceidev01.dfs.core.windows.net/"
    "finance_canada/fiscal/"
)

BRONZE_FILE_NAME = "frt-trf-25-eng.xlsx"

BRONZE_PATH = (
    f"{BRONZE_FOLDER}{BRONZE_FILE_NAME}"
)

SILVER_FEDERAL_PATH = (
    "abfss://silver@stceidev01.dfs.core.windows.net/"
    "finance_canada/fiscal/federal/"
)

SILVER_ONTARIO_PATH = (
    "abfss://silver@stceidev01.dfs.core.windows.net/"
    "finance_canada/fiscal/ontario/"
)

print("Configuration loaded.")
print("Bronze:", BRONZE_PATH)
print("Federal Silver:", SILVER_FEDERAL_PATH)
print("Ontario Silver:", SILVER_ONTARIO_PATH)

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
# MAGIC ## 2. Load and Inspect the Fiscal Reference Tables Workbook
# MAGIC
# MAGIC Load the Finance Canada Excel workbook directly from Bronze storage and inspect its worksheet structure.
# MAGIC
# MAGIC This step identifies:
# MAGIC
# MAGIC - available worksheets,
# MAGIC - worksheet names used for the federal and provincial fiscal tables,
# MAGIC - workbook organization,
# MAGIC - the exact location of Federal Table 1 and Ontario Table 23.
# MAGIC
# MAGIC The workbook is inspected before defining any extraction or cleaning logic.

# COMMAND ----------

# Install the Excel dependency required by pandas to read .xlsx files.
# Databricks may restart the Python environment after installation.
%pip install openpyxl

# COMMAND ----------

# =========================================================
# LOAD FINANCE CANADA WORKBOOK IN MEMORY
# =========================================================

import io
import pandas as pd

binary_df = (
    spark.read
    .format("binaryFile")
    .load(BRONZE_PATH)
)

binary_rows = binary_df.collect()

assert len(binary_rows) == 1, (
    f"Expected one Finance Canada workbook, "
    f"but found {len(binary_rows)}."
)

excel_bytes = binary_rows[0]["content"]

print(
    f"Finance Canada workbook loaded successfully: "
    f"{len(excel_bytes):,} bytes"
)

excel_buffer = io.BytesIO(excel_bytes)

workbook = pd.ExcelFile(
    excel_buffer,
    engine="openpyxl"
)

print("\nWorkbook sheets:")

for sheet_name in workbook.sheet_names:
    print(" -", sheet_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Inspect Target Fiscal Tables
# MAGIC
# MAGIC Inspect the two fiscal worksheets required for the Silver layer before applying any transformation logic.
# MAGIC
# MAGIC ### Target Worksheets
# MAGIC
# MAGIC - Federal fiscal transactions: `1 - Fiscal transactions`
# MAGIC - Ontario fiscal data: `23-Ont`
# MAGIC
# MAGIC The goal is to identify:
# MAGIC
# MAGIC - title and header rows,
# MAGIC - fiscal-year columns,
# MAGIC - fiscal indicator rows,
# MAGIC - blank or note rows,
# MAGIC - the correct structure for standardized Silver datasets.

# COMMAND ----------

# =========================================================
# INSPECT FEDERAL TABLE 1
# =========================================================

FEDERAL_SHEET = "1 - Fiscal transactions"

federal_raw = pd.read_excel(
    io.BytesIO(excel_bytes),
    sheet_name=FEDERAL_SHEET,
    header=None,
    engine="openpyxl"
)

print(
    f"Federal raw shape: "
    f"{federal_raw.shape[0]:,} rows × "
    f"{federal_raw.shape[1]:,} columns"
)

display(
    spark.createDataFrame(
        federal_raw.head(40).fillna("").astype(str)
    )
)

# COMMAND ----------

# =========================================================
# INSPECT ONTARIO TABLE 23
# =========================================================

ONTARIO_SHEET = "23-Ont"

ontario_raw = pd.read_excel(
    io.BytesIO(excel_bytes),
    sheet_name=ONTARIO_SHEET,
    header=None,
    engine="openpyxl"
)

print(
    f"Ontario raw shape: "
    f"{ontario_raw.shape[0]:,} rows × "
    f"{ontario_raw.shape[1]:,} columns"
)

display(
    spark.createDataFrame(
        ontario_raw.head(40).fillna("").astype(str)
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Examine Header Structure and Reporting Coverage
# MAGIC
# MAGIC The target fiscal worksheets use multi-row Excel headers rather than a standard single header row.
# MAGIC
# MAGIC Before constructing the Silver schema, inspect the complete header text and the most recent fiscal-year observations.
# MAGIC
# MAGIC This ensures that:
# MAGIC
# MAGIC - fiscal indicators receive accurate standardized names,
# MAGIC - truncated display labels are not misinterpreted,
# MAGIC - the full reporting-period coverage is preserved.

# COMMAND ----------

# =========================================================
# INSPECT FEDERAL HEADER AND LATEST ROWS
# =========================================================

print("FEDERAL TABLE 1 — HEADER STRUCTURE")
print("=" * 80)

for row_index in range(0, 10):
    print(
        f"\nRow {row_index + 1}:",
        federal_raw.iloc[row_index].tolist()
    )

print("\n\nFEDERAL TABLE 1 — LAST 10 ROWS")
print("=" * 80)

print(
    federal_raw.tail(10).to_string(
        index=False,
        header=False
    )
)

# COMMAND ----------

# =========================================================
# INSPECT ONTARIO HEADER AND LATEST ROWS
# =========================================================

print("ONTARIO TABLE 23 — HEADER STRUCTURE")
print("=" * 80)

for row_index in range(0, 7):
    print(
        f"\nRow {row_index + 1}:",
        ontario_raw.iloc[row_index].tolist()
    )

print("\n\nONTARIO TABLE 23 — LAST 10 ROWS")
print("=" * 80)

print(
    ontario_raw.tail(10).to_string(
        index=False,
        header=False
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Extract and Standardize Fiscal Observations
# MAGIC
# MAGIC The Finance Canada worksheets contain titles, multi-row headers, fiscal observations and explanatory footnotes.
# MAGIC
# MAGIC Only rows representing valid fiscal years are retained for the canonical Silver datasets.
# MAGIC
# MAGIC The source fiscal-year label is preserved, while standardized column names are applied to each table.
# MAGIC
# MAGIC ### Federal Table 1 Grain
# MAGIC
# MAGIC One row per federal fiscal year.
# MAGIC
# MAGIC ### Ontario Table 23 Grain
# MAGIC
# MAGIC One row per Ontario fiscal year.
# MAGIC
# MAGIC All monetary values remain in the source unit:
# MAGIC
# MAGIC **millions of dollars**

# COMMAND ----------

# =========================================================
# CLEAN FEDERAL TABLE 1
# =========================================================

import re
import numpy as np

federal_columns = [
    "fiscal_year",
    "revenues",
    "program_expenses_excluding_net_actuarial_losses",
    "public_debt_charges",
    "budgetary_balance_excluding_net_actuarial_losses",
    "net_actuarial_losses",
    "budgetary_balance",
    "net_remeasurement_gains_losses",
    "adjustments_to_accumulated_deficit",
    "accumulated_deficit",
    "non_budgetary_transactions",
    "financial_requirement_or_source"
]

# Keep only rows whose first column contains a fiscal year.
federal_clean = federal_raw[
    federal_raw[0]
    .astype(str)
    .str.match(r"^\d{4}-\d{2}$", na=False)
].copy()

federal_clean.columns = federal_columns

# Convert fiscal measures to numeric values.
federal_numeric_columns = [
    column
    for column in federal_columns
    if column != "fiscal_year"
]

for column in federal_numeric_columns:
    federal_clean[column] = pd.to_numeric(
        federal_clean[column],
        errors="coerce"
    )

federal_clean = federal_clean.reset_index(drop=True)

print(
    f"Federal fiscal observations: "
    f"{len(federal_clean):,}"
)

print(
    f"Federal fiscal-year range: "
    f"{federal_clean['fiscal_year'].iloc[0]} "
    f"to "
    f"{federal_clean['fiscal_year'].iloc[-1]}"
)

display(
    spark.createDataFrame(
        federal_clean
    )
)

# COMMAND ----------

# =========================================================
# CLEAN ONTARIO TABLE 23
# =========================================================

ontario_columns = [
    "fiscal_year",
    "own_source_revenues",
    "federal_transfers",
    "total_revenues",
    "total_program_expenditures",
    "debt_charges",
    "total_expenditures",
    "deficit_or_surplus",
    "net_debt"
]

# Keep only rows whose first column contains a fiscal year.
ontario_clean = ontario_raw[
    ontario_raw[0]
    .astype(str)
    .str.match(r"^\d{4}-\d{2}$", na=False)
].copy()

ontario_clean.columns = ontario_columns

# Convert fiscal measures to numeric values.
ontario_numeric_columns = [
    column
    for column in ontario_columns
    if column != "fiscal_year"
]

for column in ontario_numeric_columns:
    ontario_clean[column] = pd.to_numeric(
        ontario_clean[column],
        errors="coerce"
    )

ontario_clean = ontario_clean.reset_index(drop=True)

print(
    f"Ontario fiscal observations: "
    f"{len(ontario_clean):,}"
)

print(
    f"Ontario fiscal-year range: "
    f"{ontario_clean['fiscal_year'].iloc[0]} "
    f"to "
    f"{ontario_clean['fiscal_year'].iloc[-1]}"
)

display(
    spark.createDataFrame(
        ontario_clean
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validate Standardized Fiscal Data
# MAGIC
# MAGIC Validate the extracted fiscal observations before writing them to Silver.
# MAGIC
# MAGIC The checks confirm:
# MAGIC
# MAGIC - valid fiscal-year labels,
# MAGIC - unique fiscal-year business keys,
# MAGIC - expected reporting coverage,
# MAGIC - missing fiscal measures.
# MAGIC
# MAGIC Missing values are not automatically replaced with zero because blank historical cells can represent unavailable or non-applicable fiscal information.

# COMMAND ----------

# =========================================================
# PROFILE STANDARDIZED FEDERAL DATA
# =========================================================

print("FEDERAL TABLE 1 — VALIDATION")
print("=" * 60)

print(
    f"Rows: "
    f"{len(federal_clean):,}"
)

print(
    f"Duplicate fiscal years: "
    f"{federal_clean['fiscal_year'].duplicated().sum():,}"
)

print("\nMissing values by column:")

print(
    federal_clean
    .isna()
    .sum()
    .to_string()
)

# COMMAND ----------

# =========================================================
# PROFILE STANDARDIZED ONTARIO DATA
# =========================================================

print("ONTARIO TABLE 23 — VALIDATION")
print("=" * 60)

print(
    f"Rows: "
    f"{len(ontario_clean):,}"
)

print(
    f"Duplicate fiscal years: "
    f"{ontario_clean['fiscal_year'].duplicated().sum():,}"
)

print("\nMissing values by column:")

print(
    ontario_clean
    .isna()
    .sum()
    .to_string()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Apply Silver Validation Rules
# MAGIC
# MAGIC The standardized fiscal tables are validated before writing to Silver.
# MAGIC
# MAGIC ### Federal Table
# MAGIC
# MAGIC Some specialized fiscal measures contain historical null values because those accounting categories were introduced in later reporting periods.
# MAGIC
# MAGIC These values are preserved as null rather than replaced with zero because a blank historical observation does not necessarily represent a fiscal value of zero.
# MAGIC
# MAGIC The core fiscal fields required for each federal observation are:
# MAGIC
# MAGIC - fiscal year,
# MAGIC - revenues,
# MAGIC - program expenses,
# MAGIC - public debt charges,
# MAGIC - budgetary balance,
# MAGIC - accumulated deficit,
# MAGIC - non-budgetary transactions,
# MAGIC - financial requirement or source.
# MAGIC
# MAGIC ### Ontario Table
# MAGIC
# MAGIC All selected Ontario fiscal measures are complete.
# MAGIC
# MAGIC ### Business Key
# MAGIC
# MAGIC For both datasets:
# MAGIC
# MAGIC `fiscal_year`
# MAGIC
# MAGIC ### Units
# MAGIC
# MAGIC All monetary values remain in **millions of dollars** as published by Finance Canada.

# COMMAND ----------

# =========================================================
# CREATE SILVER-READY FEDERAL DATASET
# =========================================================

federal_required_columns = [
    "fiscal_year",
    "revenues",
    "program_expenses_excluding_net_actuarial_losses",
    "public_debt_charges",
    "budgetary_balance",
    "accumulated_deficit",
    "non_budgetary_transactions",
    "financial_requirement_or_source"
]

federal_invalid_mask = (
    federal_clean[
        federal_required_columns
    ]
    .isna()
    .any(axis=1)
)

federal_valid = (
    federal_clean[
        ~federal_invalid_mask
    ]
    .copy()
)

federal_quarantine = (
    federal_clean[
        federal_invalid_mask
    ]
    .copy()
)

print(
    f"Federal source rows: "
    f"{len(federal_clean):,}"
)

print(
    f"Federal valid rows: "
    f"{len(federal_valid):,}"
)

print(
    f"Federal quarantine rows: "
    f"{len(federal_quarantine):,}"
)

assert len(federal_valid) == len(federal_clean), (
    "Federal rows failed required-field validation."
)

assert (
    federal_valid["fiscal_year"]
    .duplicated()
    .sum()
) == 0, (
    "Duplicate federal fiscal-year keys detected."
)

print(
    "\nFederal Silver validation passed."
)

# COMMAND ----------

# =========================================================
# CREATE SILVER-READY ONTARIO DATASET
# =========================================================

ontario_required_columns = [
    "fiscal_year",
    "own_source_revenues",
    "federal_transfers",
    "total_revenues",
    "total_program_expenditures",
    "debt_charges",
    "total_expenditures",
    "deficit_or_surplus",
    "net_debt"
]

ontario_invalid_mask = (
    ontario_clean[
        ontario_required_columns
    ]
    .isna()
    .any(axis=1)
)

ontario_valid = (
    ontario_clean[
        ~ontario_invalid_mask
    ]
    .copy()
)

ontario_quarantine = (
    ontario_clean[
        ontario_invalid_mask
    ]
    .copy()
)

print(
    f"Ontario source rows: "
    f"{len(ontario_clean):,}"
)

print(
    f"Ontario valid rows: "
    f"{len(ontario_valid):,}"
)

print(
    f"Ontario quarantine rows: "
    f"{len(ontario_quarantine):,}"
)

assert len(ontario_valid) == len(ontario_clean), (
    "Ontario rows failed required-field validation."
)

assert (
    ontario_valid["fiscal_year"]
    .duplicated()
    .sum()
) == 0, (
    "Duplicate Ontario fiscal-year keys detected."
)

print(
    "\nOntario Silver validation passed."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Convert Fiscal Tables to Spark DataFrames
# MAGIC
# MAGIC Convert the validated pandas fiscal datasets into Spark DataFrames before writing them to Delta.
# MAGIC
# MAGIC The Silver datasets preserve the published fiscal-year grain and original monetary units.

# COMMAND ----------

# =========================================================
# CONVERT TO SPARK DATAFRAMES
# =========================================================

federal_silver = (
    spark.createDataFrame(
        federal_valid
    )
)

ontario_silver = (
    spark.createDataFrame(
        ontario_valid
    )
)

print(
    f"Federal Silver candidate rows: "
    f"{federal_silver.count():,}"
)

print(
    f"Ontario Silver candidate rows: "
    f"{ontario_silver.count():,}"
)

print("\nFederal schema:")
federal_silver.printSchema()

print("\nOntario schema:")
ontario_silver.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Write Fiscal Data to Silver Delta
# MAGIC
# MAGIC Write the validated Federal and Ontario fiscal datasets to separate Silver Delta locations.
# MAGIC
# MAGIC The first run creates each Delta table. Subsequent runs use Delta MERGE so the notebook can be rerun safely without creating duplicate fiscal-year records.
# MAGIC
# MAGIC ### Federal Silver Path
# MAGIC
# MAGIC `finance_canada/fiscal/federal/`
# MAGIC
# MAGIC ### Ontario Silver Path
# MAGIC
# MAGIC `finance_canada/fiscal/ontario/`
# MAGIC
# MAGIC ### Business Key
# MAGIC
# MAGIC `fiscal_year`

# COMMAND ----------

# =========================================================
# WRITE / MERGE FEDERAL SILVER DELTA
# =========================================================

from delta.tables import DeltaTable

if DeltaTable.isDeltaTable(
    spark,
    SILVER_FEDERAL_PATH
):

    print("Existing Federal Silver Delta table found.")

    federal_delta = (
        DeltaTable.forPath(
            spark,
            SILVER_FEDERAL_PATH
        )
    )

    (
        federal_delta.alias("target")
        .merge(
            federal_silver.alias("source"),
            "target.fiscal_year = source.fiscal_year"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(
        "Federal Silver Delta MERGE completed successfully."
    )

else:

    print("No existing Federal Silver Delta table found.")

    (
        federal_silver
        .write
        .format("delta")
        .mode("overwrite")
        .save(SILVER_FEDERAL_PATH)
    )

    print(
        "Initial Federal Silver Delta table created successfully."
    )

# COMMAND ----------

# =========================================================
# WRITE / MERGE ONTARIO SILVER DELTA
# =========================================================

if DeltaTable.isDeltaTable(
    spark,
    SILVER_ONTARIO_PATH
):

    print("Existing Ontario Silver Delta table found.")

    ontario_delta = (
        DeltaTable.forPath(
            spark,
            SILVER_ONTARIO_PATH
        )
    )

    (
        ontario_delta.alias("target")
        .merge(
            ontario_silver.alias("source"),
            "target.fiscal_year = source.fiscal_year"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(
        "Ontario Silver Delta MERGE completed successfully."
    )

else:

    print("No existing Ontario Silver Delta table found.")

    (
        ontario_silver
        .write
        .format("delta")
        .mode("overwrite")
        .save(SILVER_ONTARIO_PATH)
    )

    print(
        "Initial Ontario Silver Delta table created successfully."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Post-Load Validation
# MAGIC
# MAGIC Read both Silver Delta datasets back from ADLS and verify:
# MAGIC
# MAGIC - expected row counts,
# MAGIC - fiscal-year coverage,
# MAGIC - unique business keys,
# MAGIC - successful Delta storage.

# COMMAND ----------

# =========================================================
# POST-LOAD VALIDATION
# =========================================================

federal_check = (
    spark.read
    .format("delta")
    .load(SILVER_FEDERAL_PATH)
)

ontario_check = (
    spark.read
    .format("delta")
    .load(SILVER_ONTARIO_PATH)
)

federal_count = federal_check.count()
ontario_count = ontario_check.count()

print(
    f"Federal Silver rows: "
    f"{federal_count:,}"
)

print(
    f"Ontario Silver rows: "
    f"{ontario_count:,}"
)

print("\nFederal fiscal-year range:")

federal_check.selectExpr(
    "min(fiscal_year) AS min_fiscal_year",
    "max(fiscal_year) AS max_fiscal_year"
).show()

print("Ontario fiscal-year range:")

ontario_check.selectExpr(
    "min(fiscal_year) AS min_fiscal_year",
    "max(fiscal_year) AS max_fiscal_year"
).show()

federal_duplicate_keys = (
    federal_check
    .groupBy("fiscal_year")
    .count()
    .filter("count > 1")
    .count()
)

ontario_duplicate_keys = (
    ontario_check
    .groupBy("fiscal_year")
    .count()
    .filter("count > 1")
    .count()
)

print(
    f"Federal duplicate fiscal years: "
    f"{federal_duplicate_keys:,}"
)

print(
    f"Ontario duplicate fiscal years: "
    f"{ontario_duplicate_keys:,}"
)

assert federal_count == len(federal_valid)
assert ontario_count == len(ontario_valid)

assert federal_duplicate_keys == 0
assert ontario_duplicate_keys == 0

print(
    "\nPost-load validation passed."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Idempotency Test
# MAGIC
# MAGIC Re-run the same Delta MERGE operations using the validated Federal and Ontario fiscal datasets.
# MAGIC
# MAGIC The row counts must remain unchanged, confirming that rerunning the notebook does not create duplicate fiscal-year observations.

# COMMAND ----------

# =========================================================
# IDEMPOTENCY TEST
# =========================================================

federal_delta = (
    DeltaTable.forPath(
        spark,
        SILVER_FEDERAL_PATH
    )
)

ontario_delta = (
    DeltaTable.forPath(
        spark,
        SILVER_ONTARIO_PATH
    )
)

federal_before = federal_delta.toDF().count()
ontario_before = ontario_delta.toDF().count()

(
    federal_delta.alias("target")
    .merge(
        federal_silver.alias("source"),
        "target.fiscal_year = source.fiscal_year"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

(
    ontario_delta.alias("target")
    .merge(
        ontario_silver.alias("source"),
        "target.fiscal_year = source.fiscal_year"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

federal_after = federal_delta.toDF().count()
ontario_after = ontario_delta.toDF().count()

print(
    f"Federal rows before MERGE: "
    f"{federal_before:,}"
)

print(
    f"Federal rows after MERGE:  "
    f"{federal_after:,}"
)

print(
    f"Ontario rows before MERGE: "
    f"{ontario_before:,}"
)

print(
    f"Ontario rows after MERGE:  "
    f"{ontario_after:,}"
)

assert federal_before == federal_after, (
    "Federal idempotency test failed."
)

assert ontario_before == ontario_after, (
    "Ontario idempotency test failed."
)

print(
    "\nIdempotency test passed."
)

# COMMAND ----------

# =========================================================
# FINAL SILVER DATA PREVIEW
# =========================================================
# Purpose:
# Read the persisted Federal and Ontario Silver Delta tables
# after all processing and validation are complete.
#
# This cell is only for final visual verification and does
# not modify either dataset.

federal_final = (
    spark.read
    .format("delta")
    .load(
        "abfss://silver@stceidev01.dfs.core.windows.net/"
        "finance_canada/fiscal/federal/"
    )
)

ontario_final = (
    spark.read
    .format("delta")
    .load(
        "abfss://silver@stceidev01.dfs.core.windows.net/"
        "finance_canada/fiscal/ontario/"
    )
)

print("Federal Fiscal — Final Silver Preview")
display(federal_final.limit(5))

print("Ontario Fiscal — Final Silver Preview")
display(ontario_final.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC