# Databricks notebook source
# MAGIC %md
# MAGIC # Azure SQL Reporting Publication
# MAGIC
# MAGIC **Notebook:** `01_publish_gold_to_azure_sql`
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC Publish validated Gold-layer datasets from Azure Data Lake Storage to Azure SQL Database for Power BI reporting.
# MAGIC
# MAGIC ## Architecture
# MAGIC
# MAGIC `Gold Delta → Azure SQL → Power BI`
# MAGIC
# MAGIC ## Authentication
# MAGIC
# MAGIC Azure SQL access uses Microsoft Entra authentication through a Databricks service credential backed by managed identity.
# MAGIC
# MAGIC No SQL username, password, client secret, or access token is stored in notebook code.

# COMMAND ----------

# =========================================================
# AZURE SQL CONFIGURATION
# =========================================================

SQL_SERVER = (
    "sql-canadian-economic-intelligence-dev.database.windows.net"
)

SQL_DATABASE = "sqldb-economic-intelligence-dev"

SERVICE_CREDENTIAL_NAME = "svc_cei_sql_serving"

print("Azure SQL configuration loaded.")

# COMMAND ----------

# MAGIC %pip install -q mssql-python

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Azure SQL Connection
# MAGIC
# MAGIC Create an encrypted Azure SQL connection using the Databricks service credential and Microsoft Entra authentication.

# COMMAND ----------

# =========================================================
# AZURE SQL CONNECTION
# =========================================================

import struct
import mssql_python

SQL_COPT_SS_ACCESS_TOKEN = 1256


def get_sql_connection():
    """
    Create an Azure SQL connection using the Databricks
    service credential backed by managed identity.
    """

    credential_provider = (
        dbutils.credentials
        .getServiceCredentialsProvider(
            SERVICE_CREDENTIAL_NAME
        )
    )

    access_token = credential_provider.get_token(
        "https://database.windows.net/.default"
    )

    token_bytes = access_token.token.encode("utf-16le")

    token_struct = struct.pack(
        f"<I{len(token_bytes)}s",
        len(token_bytes),
        token_bytes
    )

    connection_string = (
        f"Server={SQL_SERVER};"
        f"Database={SQL_DATABASE};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
    )

    return mssql_python.connect(
        connection_string,
        attrs_before={
            SQL_COPT_SS_ACCESS_TOKEN: token_struct
        }
    )


print("Azure SQL connection configured.")

# COMMAND ----------

# =========================================================
# VALIDATE AZURE SQL CONNECTION
# =========================================================

conn = get_sql_connection()
cursor = conn.cursor()

cursor.execute("SELECT 1")
connection_test = cursor.fetchone()[0]

cursor.execute("""
SELECT
    SUSER_SNAME(),
    USER_NAME();
""")

identity = cursor.fetchone()

assert connection_test == 1

print("Connection test:", connection_test)
print("Database user:", identity[1])

cursor.close()
conn.close()

print("Azure SQL connection validated successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Shared Publication Utilities
# MAGIC
# MAGIC Define reusable functions for validating and publishing Gold datasets to Azure SQL.

# COMMAND ----------

# =========================================================
# SHARED PUBLICATION UTILITIES
# =========================================================

import numpy as np
import pandas as pd


def validate_columns(spark_df, required_columns, dataset_name):
    """
    Confirm that required reporting columns exist.
    """

    missing_columns = [
        column
        for column in required_columns
        if column not in spark_df.columns
    ]

    assert not missing_columns, (
        f"{dataset_name}: missing columns {missing_columns}"
    )


def to_sql_value(value):
    """
    Convert pandas and NumPy values to SQL-compatible values.
    """

    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()

    if isinstance(value, np.generic):
        return value.item()

    return value


def prepare_sql_rows(spark_df, columns):
    """
    Convert selected Spark columns to Python tuples.
    """

    pdf = (
        spark_df
        .select(*columns)
        .toPandas()
    )

    return [
        tuple(
            to_sql_value(value)
            for value in row
        )
        for row in pdf.itertuples(
            index=False,
            name=None
        )
    ]


def publish_sql_table(
    table_name,
    spark_df,
    columns,
    create_table_sql,
    business_key_columns=None
):
    """
    Publish one Gold dataset to Azure SQL using a full refresh.
    """

    validate_columns(
        spark_df,
        columns,
        table_name
    )

    source_row_count = spark_df.count()

    assert source_row_count > 0, (
        f"{table_name}: source dataset contains no rows."
    )

    duplicate_count = 0

    if business_key_columns:
        duplicate_count = (
            spark_df
            .groupBy(*business_key_columns)
            .count()
            .filter("count > 1")
            .count()
        )

        assert duplicate_count == 0, (
            f"{table_name}: duplicate business keys detected."
        )

    rows = prepare_sql_rows(
        spark_df,
        columns
    )

    column_list = ", ".join(
        f"[{column}]"
        for column in columns
    )

    placeholders = ", ".join(
        ["?"] * len(columns)
    )

    insert_sql = (
        f"INSERT INTO {table_name} "
        f"({column_list}) "
        f"VALUES ({placeholders})"
    )

    conn = get_sql_connection()
    cursor = conn.cursor()

    try:
        # Reporting tables use a full-refresh publication strategy.
        cursor.execute(
            f"""
            IF OBJECT_ID('{table_name}', 'U') IS NOT NULL
                DROP TABLE {table_name};
            """
        )

        cursor.execute(create_table_sql)

        if rows:
            cursor.executemany(
                insert_sql,
                rows
            )

        conn.commit()

        cursor.execute(
            f"SELECT COUNT(*) FROM {table_name};"
        )

        sql_row_count = cursor.fetchone()[0]

        assert sql_row_count == source_row_count, (
            f"{table_name}: Gold row count "
            f"{source_row_count:,} does not match SQL "
            f"row count {sql_row_count:,}."
        )

        print(
            f"{table_name}: {sql_row_count:,} rows published"
        )

        if business_key_columns:
            print(
                f"{table_name}: duplicate keys = "
                f"{duplicate_count}"
            )

    except Exception:
        conn.rollback()
        raise

    finally:
        cursor.close()
        conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Economic Overview
# MAGIC
# MAGIC Publish the monthly national economic overview dataset.

# COMMAND ----------

# =========================================================
# ECONOMIC OVERVIEW
# =========================================================

GOLD_ECONOMIC_OVERVIEW_PATH = (
    "abfss://gold@stceidev01.dfs.core.windows.net/"
    "economic_overview/"
)

economic_overview_df = (
    spark.read
    .format("delta")
    .load(GOLD_ECONOMIC_OVERVIEW_PATH)
)

ECONOMIC_OVERVIEW_COLUMNS = [
    "ref_date",
    "retail_sales_dollars",
    "cpi_all_items",
    "employment_thousands",
    "unemployment_rate_percent",
    "real_gdp_chained_2017_millions",
    "population",
    "population_source_date",
    "population_is_carried_forward",
    "avg_monthly_fx_usd_cad",
    "policy_rate",
    "policy_rate_source_date",
    "housing_starts_saar",
    "retail_sales_mom_percent",
    "retail_sales_yoy_percent",
    "cpi_yoy_inflation_percent",
    "employment_mom_percent",
    "real_gdp_mom_percent",
    "real_gdp_yoy_percent"
]

validate_columns(
    economic_overview_df,
    ECONOMIC_OVERVIEW_COLUMNS,
    "economic_overview"
)

print("Gold rows:", economic_overview_df.count())

display(
    economic_overview_df
    .orderBy("ref_date", ascending=False)
    .limit(5)
)

# COMMAND ----------

# =========================================================
# PUBLISH ECONOMIC OVERVIEW
# =========================================================

ECONOMIC_OVERVIEW_DDL = """
CREATE TABLE dbo.economic_overview (
    ref_date DATE NOT NULL,
    retail_sales_dollars FLOAT NULL,
    cpi_all_items FLOAT NULL,
    employment_thousands FLOAT NULL,
    unemployment_rate_percent FLOAT NULL,
    real_gdp_chained_2017_millions FLOAT NULL,
    population BIGINT NULL,
    population_source_date DATE NULL,
    population_is_carried_forward BIT NULL,
    avg_monthly_fx_usd_cad FLOAT NULL,
    policy_rate FLOAT NULL,
    policy_rate_source_date DATE NULL,
    housing_starts_saar FLOAT NULL,
    retail_sales_mom_percent FLOAT NULL,
    retail_sales_yoy_percent FLOAT NULL,
    cpi_yoy_inflation_percent FLOAT NULL,
    employment_mom_percent FLOAT NULL,
    real_gdp_mom_percent FLOAT NULL,
    real_gdp_yoy_percent FLOAT NULL
);
"""

publish_sql_table(
    table_name="dbo.economic_overview",
    spark_df=economic_overview_df,
    columns=ECONOMIC_OVERVIEW_COLUMNS,
    create_table_sql=ECONOMIC_OVERVIEW_DDL,
    business_key_columns=["ref_date"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Regional Analysis
# MAGIC
# MAGIC Publish monthly provincial and territorial economic indicators.

# COMMAND ----------

# =========================================================
# REGIONAL ANALYSIS
# =========================================================

GOLD_REGIONAL_PATH = (
    "abfss://gold@stceidev01.dfs.core.windows.net/"
    "regional_analysis/monthly/"
)

regional_df = (
    spark.read
    .format("delta")
    .load(GOLD_REGIONAL_PATH)
)

REGIONAL_COLUMNS = [
    "ref_date",
    "geo",
    "retail_sales_dollars",
    "cpi_all_items",
    "employment_thousands",
    "unemployment_rate_percent",
    "population",
    "population_source_date",
    "population_is_carried_forward",
    "retail_sales_yoy_percent",
    "cpi_yoy_inflation_percent",
    "employment_yoy_percent",
    "retail_sales_per_capita",
    "canada_retail_sales_yoy_percent",
    "canada_cpi_yoy_inflation_percent",
    "canada_employment_yoy_percent",
    "canada_unemployment_rate_percent",
    "retail_sales_yoy_gap_vs_canada",
    "cpi_inflation_gap_vs_canada",
    "employment_yoy_gap_vs_canada",
    "unemployment_rate_gap_vs_canada"
]

validate_columns(
    regional_df,
    REGIONAL_COLUMNS,
    "regional_analysis"
)

print("Gold rows:", regional_df.count())

display(
    regional_df
    .orderBy("ref_date", ascending=False)
    .limit(5)
)

# COMMAND ----------

# =========================================================
# PUBLISH REGIONAL ANALYSIS
# =========================================================

REGIONAL_DDL = """
CREATE TABLE dbo.regional_analysis_monthly (
    ref_date DATE NOT NULL,
    geo NVARCHAR(100) NOT NULL,
    retail_sales_dollars FLOAT NULL,
    cpi_all_items FLOAT NULL,
    employment_thousands FLOAT NULL,
    unemployment_rate_percent FLOAT NULL,
    population BIGINT NULL,
    population_source_date DATE NULL,
    population_is_carried_forward BIT NULL,
    retail_sales_yoy_percent FLOAT NULL,
    cpi_yoy_inflation_percent FLOAT NULL,
    employment_yoy_percent FLOAT NULL,
    retail_sales_per_capita FLOAT NULL,
    canada_retail_sales_yoy_percent FLOAT NULL,
    canada_cpi_yoy_inflation_percent FLOAT NULL,
    canada_employment_yoy_percent FLOAT NULL,
    canada_unemployment_rate_percent FLOAT NULL,
    retail_sales_yoy_gap_vs_canada FLOAT NULL,
    cpi_inflation_gap_vs_canada FLOAT NULL,
    employment_yoy_gap_vs_canada FLOAT NULL,
    unemployment_rate_gap_vs_canada FLOAT NULL
);
"""

publish_sql_table(
    table_name="dbo.regional_analysis_monthly",
    spark_df=regional_df,
    columns=REGIONAL_COLUMNS,
    create_table_sql=REGIONAL_DDL,
    business_key_columns=[
        "ref_date",
        "geo"
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Industry Analysis
# MAGIC
# MAGIC Publish monthly industry GDP and retail sales indicators and annual labour productivity measures.

# COMMAND ----------

# =========================================================
# INDUSTRY GDP
# =========================================================

GOLD_INDUSTRY_GDP_PATH = (
    "abfss://gold@stceidev01.dfs.core.windows.net/"
    "industry_analysis/gdp_monthly/"
)

industry_gdp_df = (
    spark.read
    .format("delta")
    .load(GOLD_INDUSTRY_GDP_PATH)
)

INDUSTRY_GDP_COLUMNS = [
    "ref_date",
    "industry",
    "real_gdp_chained_2017_millions",
    "real_gdp_mom_percent",
    "real_gdp_yoy_percent"
]

validate_columns(
    industry_gdp_df,
    INDUSTRY_GDP_COLUMNS,
    "industry_gdp"
)

print("Gold rows:", industry_gdp_df.count())

# COMMAND ----------

# =========================================================
# PUBLISH INDUSTRY GDP
# =========================================================

INDUSTRY_GDP_DDL = """
CREATE TABLE dbo.industry_gdp_monthly (
    ref_date DATE NOT NULL,
    industry NVARCHAR(300) NOT NULL,
    real_gdp_chained_2017_millions FLOAT NULL,
    real_gdp_mom_percent FLOAT NULL,
    real_gdp_yoy_percent FLOAT NULL
);
"""

publish_sql_table(
    table_name="dbo.industry_gdp_monthly",
    spark_df=industry_gdp_df,
    columns=INDUSTRY_GDP_COLUMNS,
    create_table_sql=INDUSTRY_GDP_DDL,
    business_key_columns=[
        "ref_date",
        "industry"
    ]
)

# COMMAND ----------

# =========================================================
# INDUSTRY RETAIL SALES
# =========================================================

GOLD_INDUSTRY_RETAIL_PATH = (
    "abfss://gold@stceidev01.dfs.core.windows.net/"
    "industry_analysis/retail_monthly/"
)

industry_retail_df = (
    spark.read
    .format("delta")
    .load(GOLD_INDUSTRY_RETAIL_PATH)
)

INDUSTRY_RETAIL_COLUMNS = [
    "ref_date",
    "industry",
    "retail_sales_dollars",
    "retail_sales_mom_percent",
    "retail_sales_yoy_percent"
]

validate_columns(
    industry_retail_df,
    INDUSTRY_RETAIL_COLUMNS,
    "industry_retail"
)

print("Gold rows:", industry_retail_df.count())

# COMMAND ----------

# =========================================================
# PUBLISH INDUSTRY RETAIL SALES
# =========================================================

INDUSTRY_RETAIL_DDL = """
CREATE TABLE dbo.industry_retail_monthly (
    ref_date DATE NOT NULL,
    industry NVARCHAR(300) NOT NULL,
    retail_sales_dollars FLOAT NULL,
    retail_sales_mom_percent FLOAT NULL,
    retail_sales_yoy_percent FLOAT NULL
);
"""

publish_sql_table(
    table_name="dbo.industry_retail_monthly",
    spark_df=industry_retail_df,
    columns=INDUSTRY_RETAIL_COLUMNS,
    create_table_sql=INDUSTRY_RETAIL_DDL,
    business_key_columns=[
        "ref_date",
        "industry"
    ]
)

# COMMAND ----------

# =========================================================
# INDUSTRY LABOUR PRODUCTIVITY
# =========================================================

GOLD_PRODUCTIVITY_PATH = (
    "abfss://gold@stceidev01.dfs.core.windows.net/"
    "industry_analysis/productivity_annual/"
)

industry_productivity_df = (
    spark.read
    .format("delta")
    .load(GOLD_PRODUCTIVITY_PATH)
)

PRODUCTIVITY_COLUMNS = [
    "ref_date",
    "industry",
    "labour_productivity_dollars",
    "labour_productivity_yoy_percent"
]

validate_columns(
    industry_productivity_df,
    PRODUCTIVITY_COLUMNS,
    "industry_productivity"
)

print("Gold rows:", industry_productivity_df.count())

# COMMAND ----------

# =========================================================
# PUBLISH INDUSTRY LABOUR PRODUCTIVITY
# =========================================================

PRODUCTIVITY_DDL = """
CREATE TABLE dbo.industry_productivity_annual (
    ref_date DATE NOT NULL,
    industry NVARCHAR(300) NOT NULL,
    labour_productivity_dollars FLOAT NULL,
    labour_productivity_yoy_percent FLOAT NULL
);
"""

publish_sql_table(
    table_name="dbo.industry_productivity_annual",
    spark_df=industry_productivity_df,
    columns=PRODUCTIVITY_COLUMNS,
    create_table_sql=PRODUCTIVITY_DDL,
    business_key_columns=[
        "ref_date",
        "industry"
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Fiscal and Affordability
# MAGIC
# MAGIC Publish monthly affordability indicators and annual federal and Ontario fiscal datasets.

# COMMAND ----------

# =========================================================
# AFFORDABILITY
# =========================================================

GOLD_AFFORDABILITY_PATH = (
    "abfss://gold@stceidev01.dfs.core.windows.net/"
    "fiscal_and_affordability/affordability_monthly/"
)

affordability_df = (
    spark.read
    .format("delta")
    .load(GOLD_AFFORDABILITY_PATH)
)

AFFORDABILITY_COLUMNS = [
    "ref_date",
    "cpi_canada_all_items",
    "cpi_ontario_all_items",
    "canada_cpi_yoy_inflation_percent",
    "ontario_cpi_yoy_inflation_percent",
    "ontario_inflation_gap_vs_canada",
    "canada_housing_starts_saar",
    "ontario_housing_starts_saar"
]

validate_columns(
    affordability_df,
    AFFORDABILITY_COLUMNS,
    "affordability"
)

print("Gold rows:", affordability_df.count())

display(
    affordability_df
    .select(
        "ref_date",
        "canada_housing_starts_saar",
        "ontario_housing_starts_saar"
    )
    .orderBy("ref_date", ascending=False)
    .limit(5)
)

# COMMAND ----------

# =========================================================
# PUBLISH AFFORDABILITY
# =========================================================

AFFORDABILITY_DDL = """
CREATE TABLE dbo.affordability_monthly (
    ref_date DATE NOT NULL,
    cpi_canada_all_items FLOAT NULL,
    cpi_ontario_all_items FLOAT NULL,
    canada_cpi_yoy_inflation_percent FLOAT NULL,
    ontario_cpi_yoy_inflation_percent FLOAT NULL,
    ontario_inflation_gap_vs_canada FLOAT NULL,
    canada_housing_starts_saar FLOAT NULL,
    ontario_housing_starts_saar FLOAT NULL
);
"""

publish_sql_table(
    table_name="dbo.affordability_monthly",
    spark_df=affordability_df,
    columns=AFFORDABILITY_COLUMNS,
    create_table_sql=AFFORDABILITY_DDL,
    business_key_columns=["ref_date"]
)

# COMMAND ----------

# =========================================================
# FEDERAL FISCAL
# =========================================================

GOLD_FEDERAL_FISCAL_PATH = (
    "abfss://gold@stceidev01.dfs.core.windows.net/"
    "fiscal_and_affordability/federal_fiscal_annual/"
)

federal_fiscal_df = (
    spark.read
    .format("delta")
    .load(GOLD_FEDERAL_FISCAL_PATH)
)

FEDERAL_FISCAL_COLUMNS = [
    "fiscal_year",
    "revenues",
    "program_expenses_excluding_net_actuarial_losses",
    "public_debt_charges",
    "budgetary_balance",
    "accumulated_deficit",
    "fiscal_position",
    "debt_charges_as_percent_of_revenue"
]

validate_columns(
    federal_fiscal_df,
    FEDERAL_FISCAL_COLUMNS,
    "federal_fiscal"
)

print("Gold rows:", federal_fiscal_df.count())

# COMMAND ----------

# =========================================================
# PUBLISH FEDERAL FISCAL
# =========================================================

FEDERAL_FISCAL_DDL = """
CREATE TABLE dbo.federal_fiscal_annual (
    fiscal_year NVARCHAR(20) NOT NULL,
    revenues FLOAT NULL,
    program_expenses_excluding_net_actuarial_losses FLOAT NULL,
    public_debt_charges FLOAT NULL,
    budgetary_balance FLOAT NULL,
    accumulated_deficit FLOAT NULL,
    fiscal_position NVARCHAR(50) NULL,
    debt_charges_as_percent_of_revenue FLOAT NULL
);
"""

publish_sql_table(
    table_name="dbo.federal_fiscal_annual",
    spark_df=federal_fiscal_df,
    columns=FEDERAL_FISCAL_COLUMNS,
    create_table_sql=FEDERAL_FISCAL_DDL,
    business_key_columns=["fiscal_year"]
)

# COMMAND ----------

# =========================================================
# ONTARIO FISCAL
# =========================================================

GOLD_ONTARIO_FISCAL_PATH = (
    "abfss://gold@stceidev01.dfs.core.windows.net/"
    "fiscal_and_affordability/ontario_fiscal_annual/"
)

ontario_fiscal_df = (
    spark.read
    .format("delta")
    .load(GOLD_ONTARIO_FISCAL_PATH)
)

ONTARIO_FISCAL_COLUMNS = [
    "fiscal_year",
    "own_source_revenues",
    "federal_transfers",
    "total_revenues",
    "total_program_expenditures",
    "debt_charges",
    "total_expenditures",
    "deficit_or_surplus",
    "net_debt",
    "fiscal_position",
    "federal_transfers_as_percent_of_revenue",
    "debt_charges_as_percent_of_revenue"
]

validate_columns(
    ontario_fiscal_df,
    ONTARIO_FISCAL_COLUMNS,
    "ontario_fiscal"
)

print("Gold rows:", ontario_fiscal_df.count())

# COMMAND ----------

# =========================================================
# PUBLISH ONTARIO FISCAL
# =========================================================

ONTARIO_FISCAL_DDL = """
CREATE TABLE dbo.ontario_fiscal_annual (
    fiscal_year NVARCHAR(20) NOT NULL,
    own_source_revenues FLOAT NULL,
    federal_transfers FLOAT NULL,
    total_revenues FLOAT NULL,
    total_program_expenditures FLOAT NULL,
    debt_charges FLOAT NULL,
    total_expenditures FLOAT NULL,
    deficit_or_surplus FLOAT NULL,
    net_debt FLOAT NULL,
    fiscal_position NVARCHAR(50) NULL,
    federal_transfers_as_percent_of_revenue FLOAT NULL,
    debt_charges_as_percent_of_revenue FLOAT NULL
);
"""

publish_sql_table(
    table_name="dbo.ontario_fiscal_annual",
    spark_df=ontario_fiscal_df,
    columns=ONTARIO_FISCAL_COLUMNS,
    create_table_sql=ONTARIO_FISCAL_DDL,
    business_key_columns=["fiscal_year"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Retail Sales Forecasting
# MAGIC
# MAGIC Publish model evaluation, holdout predictions, model metadata, and the final retail sales forecast.

# COMMAND ----------

# =========================================================
# RETAIL MODEL METRICS
# =========================================================

GOLD_RETAIL_MODEL_METRICS_PATH = (
    "abfss://gold@stceidev01.dfs.core.windows.net/"
    "forecasting/retail_sales/model_metrics/"
)

retail_model_metrics_df = (
    spark.read
    .format("delta")
    .load(GOLD_RETAIL_MODEL_METRICS_PATH)
)

RETAIL_MODEL_METRICS_COLUMNS = [
    "model",
    "mae",
    "rmse",
    "target",
    "forecast_horizon_months",
    "evaluation_period"
]

validate_columns(
    retail_model_metrics_df,
    RETAIL_MODEL_METRICS_COLUMNS,
    "retail_model_metrics"
)

model_metrics_count = retail_model_metrics_df.count()

assert model_metrics_count == 4, (
    f"Expected 4 model records, found {model_metrics_count}."
)

display(
    retail_model_metrics_df
    .orderBy("rmse")
)

# COMMAND ----------

# =========================================================
# PUBLISH RETAIL MODEL METRICS
# =========================================================

RETAIL_MODEL_METRICS_DDL = """
CREATE TABLE dbo.retail_model_metrics (
    model NVARCHAR(100) NOT NULL,
    mae FLOAT NULL,
    rmse FLOAT NULL,
    target NVARCHAR(100) NULL,
    forecast_horizon_months INT NULL,
    evaluation_period NVARCHAR(100) NULL
);
"""

publish_sql_table(
    table_name="dbo.retail_model_metrics",
    spark_df=retail_model_metrics_df,
    columns=RETAIL_MODEL_METRICS_COLUMNS,
    create_table_sql=RETAIL_MODEL_METRICS_DDL,
    business_key_columns=["model"]
)

# COMMAND ----------

# =========================================================
# RETAIL HOLDOUT PREDICTIONS
# =========================================================

GOLD_RETAIL_HOLDOUT_PATH = (
    "abfss://gold@stceidev01.dfs.core.windows.net/"
    "forecasting/retail_sales/holdout_predictions/"
)

retail_holdout_df = (
    spark.read
    .format("delta")
    .load(GOLD_RETAIL_HOLDOUT_PATH)
)

RETAIL_HOLDOUT_COLUMNS = [
    "ref_date",
    "actual_retail_sales",
    "predicted_retail_sales",
    "forecast_error",
    "model",
    "forecast_horizon_months"
]

validate_columns(
    retail_holdout_df,
    RETAIL_HOLDOUT_COLUMNS,
    "retail_holdout_predictions"
)

holdout_count = retail_holdout_df.count()

assert holdout_count == 12, (
    f"Expected 12 holdout records, found {holdout_count}."
)

print("Gold holdout rows:", holdout_count)

# COMMAND ----------

# =========================================================
# PUBLISH RETAIL HOLDOUT PREDICTIONS
# =========================================================

RETAIL_HOLDOUT_DDL = """
CREATE TABLE dbo.retail_holdout_predictions (
    ref_date DATE NOT NULL,
    actual_retail_sales FLOAT NULL,
    predicted_retail_sales FLOAT NULL,
    forecast_error FLOAT NULL,
    model NVARCHAR(100) NULL,
    forecast_horizon_months INT NULL
);
"""

publish_sql_table(
    table_name="dbo.retail_holdout_predictions",
    spark_df=retail_holdout_df,
    columns=RETAIL_HOLDOUT_COLUMNS,
    create_table_sql=RETAIL_HOLDOUT_DDL,
    business_key_columns=["ref_date"]
)

# COMMAND ----------

# =========================================================
# RETAIL MODEL METADATA
# =========================================================

GOLD_RETAIL_MODEL_METADATA_PATH = (
    "abfss://gold@stceidev01.dfs.core.windows.net/"
    "forecasting/retail_sales/model_metadata/"
)

retail_model_metadata_df = (
    spark.read
    .format("delta")
    .load(GOLD_RETAIL_MODEL_METADATA_PATH)
)

RETAIL_MODEL_METADATA_COLUMNS = [
    "forecast_horizon_months",
    "holdout_mae",
    "holdout_rmse",
    "selected_model",
    "selection_metric",
    "target",
    "validation_mae",
    "validation_rmse"
]

validate_columns(
    retail_model_metadata_df,
    RETAIL_MODEL_METADATA_COLUMNS,
    "retail_model_metadata"
)

metadata_count = retail_model_metadata_df.count()

assert metadata_count == 1, (
    f"Expected 1 metadata record, found {metadata_count}."
)

display(retail_model_metadata_df)

# COMMAND ----------

# =========================================================
# PUBLISH RETAIL MODEL METADATA
# =========================================================

RETAIL_MODEL_METADATA_DDL = """
CREATE TABLE dbo.retail_model_metadata (
    forecast_horizon_months INT NULL,
    holdout_mae FLOAT NULL,
    holdout_rmse FLOAT NULL,
    selected_model NVARCHAR(100) NOT NULL,
    selection_metric NVARCHAR(50) NULL,
    target NVARCHAR(100) NULL,
    validation_mae FLOAT NULL,
    validation_rmse FLOAT NULL
);
"""

publish_sql_table(
    table_name="dbo.retail_model_metadata",
    spark_df=retail_model_metadata_df,
    columns=RETAIL_MODEL_METADATA_COLUMNS,
    create_table_sql=RETAIL_MODEL_METADATA_DDL,
    business_key_columns=["selected_model"]
)

# COMMAND ----------

# =========================================================
# FINAL RETAIL SALES FORECAST
# =========================================================

GOLD_RETAIL_FORECAST_PATH = (
    "abfss://gold@stceidev01.dfs.core.windows.net/"
    "forecasting/retail_sales/forecasts/"
)

retail_forecast_df = (
    spark.read
    .format("delta")
    .load(GOLD_RETAIL_FORECAST_PATH)
)

RETAIL_FORECAST_COLUMNS = [
    "forecast_horizon_months",
    "forecast_retail_sales_dollars",
    "model",
    "ref_date"
]

validate_columns(
    retail_forecast_df,
    RETAIL_FORECAST_COLUMNS,
    "retail_forecast"
)

forecast_count = retail_forecast_df.count()

assert forecast_count == 1, (
    f"Expected 1 forecast record, found {forecast_count}."
)

display(retail_forecast_df)

# COMMAND ----------

# =========================================================
# PUBLISH FINAL RETAIL SALES FORECAST
# =========================================================

RETAIL_FORECAST_DDL = """
CREATE TABLE dbo.retail_forecasts (
    forecast_horizon_months INT NULL,
    forecast_retail_sales_dollars FLOAT NULL,
    model NVARCHAR(100) NOT NULL,
    ref_date DATE NOT NULL
);
"""

publish_sql_table(
    table_name="dbo.retail_forecasts",
    spark_df=retail_forecast_df,
    columns=RETAIL_FORECAST_COLUMNS,
    create_table_sql=RETAIL_FORECAST_DDL,
    business_key_columns=["ref_date"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Publication Validation
# MAGIC
# MAGIC Validate that the required reporting tables were successfully published to Azure SQL.

# COMMAND ----------

# =========================================================
# FINAL AZURE SQL PUBLICATION VALIDATION
# =========================================================

EXPECTED_TABLES = [
    "economic_overview",
    "regional_analysis_monthly",
    "industry_gdp_monthly",
    "industry_retail_monthly",
    "industry_productivity_annual",
    "affordability_monthly",
    "federal_fiscal_annual",
    "ontario_fiscal_annual",
    "retail_model_metrics",
    "retail_holdout_predictions",
    "retail_model_metadata",
    "retail_forecasts"
]

conn = get_sql_connection()
cursor = conn.cursor()

cursor.execute("""
SELECT TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'dbo'
  AND TABLE_TYPE = 'BASE TABLE';
""")

existing_tables = {
    row[0]
    for row in cursor.fetchall()
}

missing_tables = [
    table
    for table in EXPECTED_TABLES
    if table not in existing_tables
]

assert not missing_tables, (
    f"Missing Azure SQL reporting tables: {missing_tables}"
)

print("All expected Azure SQL reporting tables exist.")
print("\nReporting table row counts:")

for table in EXPECTED_TABLES:

    cursor.execute(
        f"SELECT COUNT(*) FROM dbo.{table};"
    )

    row_count = cursor.fetchone()[0]

    assert row_count > 0, (
        f"dbo.{table} contains no rows."
    )

    print(
        f"dbo.{table}: {row_count:,}"
    )

cursor.close()
conn.close()

print(
    "\nAzure SQL publication validated successfully."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Publication Complete
# MAGIC
# MAGIC Gold reporting datasets are published to Azure SQL for Power BI.