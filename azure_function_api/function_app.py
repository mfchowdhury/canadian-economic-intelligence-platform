import json
import logging
import os
import struct
import time
from datetime import date, datetime

import azure.functions as func
import mssql_python
from azure.identity import DefaultAzureCredential


# =========================================================
# AZURE SQL CONNECTION
# =========================================================

SQL_COPT_SS_ACCESS_TOKEN = 1256


def get_sql_connection(max_attempts=2, retry_delay_seconds=2):
    """Create an Azure SQL connection using Microsoft Entra authentication."""

    sql_server = os.environ["SQL_SERVER"]
    sql_database = os.environ["SQL_DATABASE"]

    credential = DefaultAzureCredential()

    access_token = credential.get_token(
        "https://database.windows.net/.default"
    ).token

    token_bytes = access_token.encode("utf-16-le")

    token_struct = struct.pack(
        f"<I{len(token_bytes)}s",
        len(token_bytes),
        token_bytes,
    )

    connection_string = (
        f"Server={sql_server};"
        f"Database={sql_database};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
    )

    for attempt in range(1, max_attempts + 1):
        try:
            return mssql_python.connect(
                connection_string,
                attrs_before={
                    SQL_COPT_SS_ACCESS_TOKEN: token_struct
                },
            )

        except Exception:
            if attempt == max_attempts:
                logging.exception(
                    "Azure SQL connection failed after %s attempts.",
                    max_attempts,
                )
                raise

            logging.warning(
                "Azure SQL connection attempt %s failed. "
                "Retrying in %s seconds.",
                attempt,
                retry_delay_seconds,
            )

            time.sleep(retry_delay_seconds)

# =========================================================
# RESPONSE HELPERS
# =========================================================

def json_default(value):
    """Convert SQL date values into JSON-compatible strings."""

    if isinstance(value, (date, datetime)):
        return value.isoformat()

    raise TypeError(
        f"Object of type {type(value).__name__} is not JSON serializable."
    )


def json_response(payload, status_code=200):
    """Create a consistent JSON HTTP response."""

    return func.HttpResponse(
        json.dumps(
            payload,
            default=json_default,
        ),
        status_code=status_code,
        mimetype="application/json",
    )


def rows_to_dicts(cursor, rows):
    """Convert SQL result rows into dictionaries using column names."""

    columns = [
        column[0]
        for column in cursor.description
    ]

    return [
        dict(zip(columns, row))
        for row in rows
    ]


# =========================================================
# FUNCTION APP
# =========================================================

app = func.FunctionApp(
    http_auth_level=func.AuthLevel.FUNCTION
)


# =========================================================
# HEALTH CHECK
# =========================================================

@app.route(
    route="health",
    methods=["GET"],
)
def health(req: func.HttpRequest) -> func.HttpResponse:
    """Return the API health status."""

    logging.info(
        "Economic Intelligence API health check requested."
    )

    return json_response(
        {
            "status": "ok",
            "service": "cei-assistant-api",
        }
    )


# =========================================================
# SQL HEALTH CHECK
# =========================================================

@app.route(
    route="health/sql",
    methods=["GET"],
)
def sql_health(req: func.HttpRequest) -> func.HttpResponse:
    """Verify managed-identity connectivity to Azure SQL."""

    connection = None

    try:
        connection = get_sql_connection()
        cursor = connection.cursor()

        cursor.execute(
            """
            SELECT
                DB_NAME() AS database_name,
                USER_NAME() AS database_user;
            """
        )

        row = cursor.fetchone()

        return json_response(
            {
                "status": "ok",
                "database": row[0],
                "user": row[1],
            }
        )

    except Exception:
        logging.exception(
            "Azure SQL health check failed."
        )

        return json_response(
            {
                "status": "error",
                "message": "Azure SQL connection failed.",
            },
            status_code=500,
        )

    finally:
        if connection is not None:
            connection.close()


# =========================================================
# NATIONAL ECONOMIC INDICATORS
# =========================================================

@app.route(
    route="national/latest",
    methods=["GET"],
)
def national_latest(req: func.HttpRequest) -> func.HttpResponse:
    """Return the latest available Canadian economic indicators."""

    connection = None

    try:
        connection = get_sql_connection()
        cursor = connection.cursor()

        query = """
        SELECT
            indicator,
            value,
            ref_date
        FROM (
            SELECT
                'retail_sales_dollars' AS indicator,
                retail_sales_dollars AS value,
                ref_date
            FROM economic_overview
            WHERE retail_sales_dollars IS NOT NULL

            UNION ALL

            SELECT
                'cpi_yoy_inflation_percent',
                cpi_yoy_inflation_percent,
                ref_date
            FROM economic_overview
            WHERE cpi_yoy_inflation_percent IS NOT NULL

            UNION ALL

            SELECT
                'employment_thousands',
                employment_thousands,
                ref_date
            FROM economic_overview
            WHERE employment_thousands IS NOT NULL

            UNION ALL

            SELECT
                'unemployment_rate_percent',
                unemployment_rate_percent,
                ref_date
            FROM economic_overview
            WHERE unemployment_rate_percent IS NOT NULL

            UNION ALL

            SELECT
                'real_gdp_chained_2017_millions',
                real_gdp_chained_2017_millions,
                ref_date
            FROM economic_overview
            WHERE real_gdp_chained_2017_millions IS NOT NULL

            UNION ALL

            SELECT
                'real_gdp_mom_percent',
                real_gdp_mom_percent,
                ref_date
            FROM economic_overview
            WHERE real_gdp_mom_percent IS NOT NULL

            UNION ALL

            SELECT
                'real_gdp_yoy_percent',
                real_gdp_yoy_percent,
                ref_date
            FROM economic_overview
            WHERE real_gdp_yoy_percent IS NOT NULL

            UNION ALL

            SELECT
                'population',
                CAST(population AS FLOAT),
                ref_date
            FROM economic_overview
            WHERE population IS NOT NULL

            UNION ALL

            SELECT
                'avg_monthly_fx_usd_cad',
                avg_monthly_fx_usd_cad,
                ref_date
            FROM economic_overview
            WHERE avg_monthly_fx_usd_cad IS NOT NULL

            UNION ALL

            SELECT
                'policy_rate',
                policy_rate,
                ref_date
            FROM economic_overview
            WHERE policy_rate IS NOT NULL

            UNION ALL

            SELECT
                'housing_starts_saar',
                housing_starts_saar,
                ref_date
            FROM economic_overview
            WHERE housing_starts_saar IS NOT NULL

            UNION ALL

            SELECT
                'retail_sales_mom_percent',
                retail_sales_mom_percent,
                ref_date
            FROM economic_overview
            WHERE retail_sales_mom_percent IS NOT NULL

            UNION ALL

            SELECT
                'retail_sales_yoy_percent',
                retail_sales_yoy_percent,
                ref_date
            FROM economic_overview
            WHERE retail_sales_yoy_percent IS NOT NULL
        ) AS indicators
        WHERE ref_date = (
            SELECT MAX(e2.ref_date)
            FROM economic_overview AS e2
            WHERE
                CASE indicator
                    WHEN 'retail_sales_dollars'
                        THEN e2.retail_sales_dollars
                    WHEN 'cpi_yoy_inflation_percent'
                        THEN e2.cpi_yoy_inflation_percent
                    WHEN 'employment_thousands'
                        THEN e2.employment_thousands
                    WHEN 'unemployment_rate_percent'
                        THEN e2.unemployment_rate_percent
                    WHEN 'real_gdp_chained_2017_millions'
                        THEN e2.real_gdp_chained_2017_millions
                    WHEN 'real_gdp_mom_percent'
                        THEN e2.real_gdp_mom_percent
                    WHEN 'real_gdp_yoy_percent'
                        THEN e2.real_gdp_yoy_percent
                    WHEN 'population'
                        THEN CAST(e2.population AS FLOAT)
                    WHEN 'avg_monthly_fx_usd_cad'
                        THEN e2.avg_monthly_fx_usd_cad
                    WHEN 'policy_rate'
                        THEN e2.policy_rate
                    WHEN 'housing_starts_saar'
                        THEN e2.housing_starts_saar
                    WHEN 'retail_sales_mom_percent'
                        THEN e2.retail_sales_mom_percent
                    WHEN 'retail_sales_yoy_percent'
                        THEN e2.retail_sales_yoy_percent
                END IS NOT NULL
        );
        """

        cursor.execute(query)

        indicators = {}

        for row in cursor.fetchall():
            indicators[row[0]] = {
                "value": row[1],
                "reference_period": row[2],
            }

        return json_response(
            {
                "status": "ok",
                "scope": "Canada",
                "indicators": indicators,
            }
        )

    except Exception:
        logging.exception(
            "National indicator retrieval failed."
        )

        return json_response(
            {
                "status": "error",
                "message": "Unable to retrieve national indicators.",
            },
            status_code=500,
        )

    finally:
        if connection is not None:
            connection.close()


# =========================================================
# REGIONAL ECONOMIC INDICATORS
# =========================================================

@app.route(
    route="regional",
    methods=["GET"],
)
def regional(req: func.HttpRequest) -> func.HttpResponse:
    """Return the latest available indicators for a requested geography."""

    geo = req.params.get("geo")

    if not geo:
        return json_response(
            {
                "status": "error",
                "message": "The geo parameter is required.",
            },
            status_code=400,
        )

    connection = None

    try:
        connection = get_sql_connection()
        cursor = connection.cursor()

        query = """
        SELECT
            indicator,
            value,
            ref_date
        FROM (
            SELECT
                'retail_sales_dollars' AS indicator,
                retail_sales_dollars AS value,
                ref_date
            FROM regional_analysis_monthly
            WHERE geo = ?
              AND retail_sales_dollars IS NOT NULL

            UNION ALL

            SELECT
                'cpi_yoy_inflation_percent',
                cpi_yoy_inflation_percent,
                ref_date
            FROM regional_analysis_monthly
            WHERE geo = ?
              AND cpi_yoy_inflation_percent IS NOT NULL

            UNION ALL

            SELECT
                'employment_thousands',
                employment_thousands,
                ref_date
            FROM regional_analysis_monthly
            WHERE geo = ?
              AND employment_thousands IS NOT NULL

            UNION ALL

            SELECT
                'unemployment_rate_percent',
                unemployment_rate_percent,
                ref_date
            FROM regional_analysis_monthly
            WHERE geo = ?
              AND unemployment_rate_percent IS NOT NULL

            UNION ALL

            SELECT
                'population',
                CAST(population AS FLOAT),
                ref_date
            FROM regional_analysis_monthly
            WHERE geo = ?
              AND population IS NOT NULL

            UNION ALL

            SELECT
                'retail_sales_yoy_percent',
                retail_sales_yoy_percent,
                ref_date
            FROM regional_analysis_monthly
            WHERE geo = ?
              AND retail_sales_yoy_percent IS NOT NULL

            UNION ALL

            SELECT
                'employment_yoy_percent',
                employment_yoy_percent,
                ref_date
            FROM regional_analysis_monthly
            WHERE geo = ?
              AND employment_yoy_percent IS NOT NULL

            UNION ALL

            SELECT
                'retail_sales_per_capita',
                retail_sales_per_capita,
                ref_date
            FROM regional_analysis_monthly
            WHERE geo = ?
              AND retail_sales_per_capita IS NOT NULL

            UNION ALL

            SELECT
                'retail_sales_yoy_gap_vs_canada',
                retail_sales_yoy_gap_vs_canada,
                ref_date
            FROM regional_analysis_monthly
            WHERE geo = ?
              AND retail_sales_yoy_gap_vs_canada IS NOT NULL

            UNION ALL

            SELECT
                'cpi_inflation_gap_vs_canada',
                cpi_inflation_gap_vs_canada,
                ref_date
            FROM regional_analysis_monthly
            WHERE geo = ?
              AND cpi_inflation_gap_vs_canada IS NOT NULL

            UNION ALL

            SELECT
                'employment_yoy_gap_vs_canada',
                employment_yoy_gap_vs_canada,
                ref_date
            FROM regional_analysis_monthly
            WHERE geo = ?
              AND employment_yoy_gap_vs_canada IS NOT NULL

            UNION ALL

            SELECT
                'unemployment_rate_gap_vs_canada',
                unemployment_rate_gap_vs_canada,
                ref_date
            FROM regional_analysis_monthly
            WHERE geo = ?
              AND unemployment_rate_gap_vs_canada IS NOT NULL
        ) AS indicators
        WHERE ref_date = (
            SELECT MAX(r2.ref_date)
            FROM regional_analysis_monthly AS r2
            WHERE r2.geo = ?
              AND
                CASE indicator
                    WHEN 'retail_sales_dollars'
                        THEN r2.retail_sales_dollars
                    WHEN 'cpi_yoy_inflation_percent'
                        THEN r2.cpi_yoy_inflation_percent
                    WHEN 'employment_thousands'
                        THEN r2.employment_thousands
                    WHEN 'unemployment_rate_percent'
                        THEN r2.unemployment_rate_percent
                    WHEN 'population'
                        THEN CAST(r2.population AS FLOAT)
                    WHEN 'retail_sales_yoy_percent'
                        THEN r2.retail_sales_yoy_percent
                    WHEN 'employment_yoy_percent'
                        THEN r2.employment_yoy_percent
                    WHEN 'retail_sales_per_capita'
                        THEN r2.retail_sales_per_capita
                    WHEN 'retail_sales_yoy_gap_vs_canada'
                        THEN r2.retail_sales_yoy_gap_vs_canada
                    WHEN 'cpi_inflation_gap_vs_canada'
                        THEN r2.cpi_inflation_gap_vs_canada
                    WHEN 'employment_yoy_gap_vs_canada'
                        THEN r2.employment_yoy_gap_vs_canada
                    WHEN 'unemployment_rate_gap_vs_canada'
                        THEN r2.unemployment_rate_gap_vs_canada
                END IS NOT NULL
        );
        """

        parameters = [geo] * 13

        cursor.execute(
            query,
            tuple(parameters),
        )

        indicators = {}

        for row in cursor.fetchall():
            indicators[row[0]] = {
                "value": row[1],
                "reference_period": row[2],
            }

        if not indicators:
            return json_response(
                {
                    "status": "not_found",
                    "message": f"No regional data found for '{geo}'.",
                },
                status_code=404,
            )

        return json_response(
            {
                "status": "ok",
                "scope": geo,
                "indicators": indicators,
            }
        )

    except Exception:
        logging.exception(
            "Regional indicator retrieval failed."
        )

        return json_response(
            {
                "status": "error",
                "message": "Unable to retrieve regional indicators.",
            },
            status_code=500,
        )

    finally:
        if connection is not None:
            connection.close()


# =========================================================
# INDUSTRY GDP
# =========================================================

@app.route(
    route="industry/gdp",
    methods=["GET"],
)
def industry_gdp(req: func.HttpRequest) -> func.HttpResponse:
    """Return the latest GDP indicators for a requested industry."""

    industry = req.params.get("industry")

    if not industry:
        return json_response(
            {
                "status": "error",
                "message": "The industry parameter is required.",
            },
            status_code=400,
        )

    connection = None

    try:
        connection = get_sql_connection()
        cursor = connection.cursor()

        query = """
        SELECT TOP 1
            ref_date,
            industry,
            real_gdp_chained_2017_millions,
            real_gdp_mom_percent,
            real_gdp_yoy_percent
        FROM industry_gdp_monthly
        WHERE industry = ?
          AND real_gdp_chained_2017_millions IS NOT NULL
        ORDER BY ref_date DESC;
        """

        cursor.execute(
            query,
            (industry,),
        )

        row = cursor.fetchone()

        if row is None:
            return json_response(
                {
                    "status": "not_found",
                    "message": f"No GDP data found for '{industry}'.",
                },
                status_code=404,
            )

        return json_response(
            {
                "status": "ok",
                "scope": "industry",
                "industry": row[1],
                "reference_period": row[0],
                "indicators": {
                    "real_gdp_chained_2017_millions": row[2],
                    "real_gdp_mom_percent": row[3],
                    "real_gdp_yoy_percent": row[4],
                },
            }
        )

    except Exception:
        logging.exception(
            "Industry GDP retrieval failed."
        )

        return json_response(
            {
                "status": "error",
                "message": "Unable to retrieve industry GDP data.",
            },
            status_code=500,
        )

    finally:
        if connection is not None:
            connection.close()


# =========================================================
# INDUSTRY RETAIL SALES
# =========================================================

@app.route(
    route="industry/retail",
    methods=["GET"],
)
def industry_retail(req: func.HttpRequest) -> func.HttpResponse:
    """Return the latest retail indicators for a requested industry."""

    industry = req.params.get("industry")

    if not industry:
        return json_response(
            {
                "status": "error",
                "message": "The industry parameter is required.",
            },
            status_code=400,
        )

    connection = None

    try:
        connection = get_sql_connection()
        cursor = connection.cursor()

        query = """
        SELECT TOP 1
            ref_date,
            industry,
            retail_sales_dollars,
            retail_sales_mom_percent,
            retail_sales_yoy_percent
        FROM industry_retail_monthly
        WHERE industry = ?
          AND retail_sales_dollars IS NOT NULL
        ORDER BY ref_date DESC;
        """

        cursor.execute(
            query,
            (industry,),
        )

        row = cursor.fetchone()

        if row is None:
            return json_response(
                {
                    "status": "not_found",
                    "message": f"No retail data found for '{industry}'.",
                },
                status_code=404,
            )

        return json_response(
            {
                "status": "ok",
                "scope": "industry",
                "industry": row[1],
                "reference_period": row[0],
                "indicators": {
                    "retail_sales_dollars": row[2],
                    "retail_sales_mom_percent": row[3],
                    "retail_sales_yoy_percent": row[4],
                },
            }
        )

    except Exception:
        logging.exception(
            "Industry retail retrieval failed."
        )

        return json_response(
            {
                "status": "error",
                "message": "Unable to retrieve industry retail data.",
            },
            status_code=500,
        )

    finally:
        if connection is not None:
            connection.close()


# =========================================================
# INDUSTRY PRODUCTIVITY
# =========================================================

@app.route(
    route="industry/productivity",
    methods=["GET"],
)
def industry_productivity(
    req: func.HttpRequest,
) -> func.HttpResponse:
    """Return the latest productivity data for a requested industry."""

    industry = req.params.get("industry")

    if not industry:
        return json_response(
            {
                "status": "error",
                "message": "The industry parameter is required.",
            },
            status_code=400,
        )

    connection = None

    try:
        connection = get_sql_connection()
        cursor = connection.cursor()

        query = """
        SELECT TOP 1
            ref_date,
            industry,
            labour_productivity_dollars,
            labour_productivity_yoy_percent
        FROM industry_productivity_annual
        WHERE industry = ?
          AND labour_productivity_dollars IS NOT NULL
        ORDER BY ref_date DESC;
        """

        cursor.execute(
            query,
            (industry,),
        )

        row = cursor.fetchone()

        if row is None:
            return json_response(
                {
                    "status": "not_found",
                    "message": (
                        f"No productivity data found for '{industry}'."
                    ),
                },
                status_code=404,
            )

        return json_response(
            {
                "status": "ok",
                "scope": "industry",
                "industry": row[1],
                "reference_period": row[0],
                "indicators": {
                    "labour_productivity_dollars": row[2],
                    "labour_productivity_yoy_percent": row[3],
                },
            }
        )

    except Exception:
        logging.exception(
            "Industry productivity retrieval failed."
        )

        return json_response(
            {
                "status": "error",
                "message": "Unable to retrieve productivity data.",
            },
            status_code=500,
        )

    finally:
        if connection is not None:
            connection.close()


# =========================================================
# AFFORDABILITY AND HOUSING
# =========================================================

@app.route(
    route="affordability/latest",
    methods=["GET"],
)
def affordability_latest(
    req: func.HttpRequest,
) -> func.HttpResponse:
    """Return the latest available affordability and housing indicators."""

    connection = None

    try:
        connection = get_sql_connection()
        cursor = connection.cursor()

        # Retrieve the latest non-null observation independently
        # because CPI and housing data may have different release periods.
        query = """
        SELECT
            indicator,
            value,
            ref_date
        FROM (
            SELECT
                'cpi_canada_all_items' AS indicator,
                cpi_canada_all_items AS value,
                ref_date
            FROM affordability_monthly
            WHERE cpi_canada_all_items IS NOT NULL

            UNION ALL

            SELECT
                'cpi_ontario_all_items',
                cpi_ontario_all_items,
                ref_date
            FROM affordability_monthly
            WHERE cpi_ontario_all_items IS NOT NULL

            UNION ALL

            SELECT
                'canada_cpi_yoy_inflation_percent',
                canada_cpi_yoy_inflation_percent,
                ref_date
            FROM affordability_monthly
            WHERE canada_cpi_yoy_inflation_percent IS NOT NULL

            UNION ALL

            SELECT
                'ontario_cpi_yoy_inflation_percent',
                ontario_cpi_yoy_inflation_percent,
                ref_date
            FROM affordability_monthly
            WHERE ontario_cpi_yoy_inflation_percent IS NOT NULL

            UNION ALL

            SELECT
                'ontario_inflation_gap_vs_canada',
                ontario_inflation_gap_vs_canada,
                ref_date
            FROM affordability_monthly
            WHERE ontario_inflation_gap_vs_canada IS NOT NULL

            UNION ALL

            SELECT
                'canada_housing_starts_saar',
                canada_housing_starts_saar,
                ref_date
            FROM affordability_monthly
            WHERE canada_housing_starts_saar IS NOT NULL

            UNION ALL

            SELECT
                'ontario_housing_starts_saar',
                ontario_housing_starts_saar,
                ref_date
            FROM affordability_monthly
            WHERE ontario_housing_starts_saar IS NOT NULL
        ) AS indicators
        WHERE ref_date = (
            SELECT MAX(a2.ref_date)
            FROM affordability_monthly AS a2
            WHERE
                CASE indicator
                    WHEN 'cpi_canada_all_items'
                        THEN a2.cpi_canada_all_items
                    WHEN 'cpi_ontario_all_items'
                        THEN a2.cpi_ontario_all_items
                    WHEN 'canada_cpi_yoy_inflation_percent'
                        THEN a2.canada_cpi_yoy_inflation_percent
                    WHEN 'ontario_cpi_yoy_inflation_percent'
                        THEN a2.ontario_cpi_yoy_inflation_percent
                    WHEN 'ontario_inflation_gap_vs_canada'
                        THEN a2.ontario_inflation_gap_vs_canada
                    WHEN 'canada_housing_starts_saar'
                        THEN a2.canada_housing_starts_saar
                    WHEN 'ontario_housing_starts_saar'
                        THEN a2.ontario_housing_starts_saar
                END IS NOT NULL
        );
        """

        cursor.execute(query)

        indicators = {}

        for row in cursor.fetchall():
            indicators[row[0]] = {
                "value": row[1],
                "reference_period": row[2],
            }

        if not indicators:
            return json_response(
                {
                    "status": "not_found",
                    "message": "No affordability data is available.",
                },
                status_code=404,
            )

        return json_response(
            {
                "status": "ok",
                "scope": "Canada and Ontario",
                "indicators": indicators,
            }
        )

    except Exception:
        logging.exception(
            "Affordability retrieval failed."
        )

        return json_response(
            {
                "status": "error",
                "message": "Unable to retrieve affordability data.",
            },
            status_code=500,
        )

    finally:
        if connection is not None:
            connection.close()

# =========================================================
# FEDERAL FISCAL DATA
# =========================================================

@app.route(
    route="fiscal/federal/latest",
    methods=["GET"],
)
def federal_fiscal_latest(
    req: func.HttpRequest,
) -> func.HttpResponse:
    """Return the latest federal fiscal indicators."""

    connection = None

    try:
        connection = get_sql_connection()
        cursor = connection.cursor()

        query = """
        SELECT TOP 1
            fiscal_year,
            revenues,
            program_expenses_excluding_net_actuarial_losses,
            public_debt_charges,
            budgetary_balance,
            accumulated_deficit,
            fiscal_position,
            debt_charges_as_percent_of_revenue
        FROM federal_fiscal_annual
        ORDER BY fiscal_year DESC;
        """

        cursor.execute(query)
        row = cursor.fetchone()

        if row is None:
            return json_response(
                {
                    "status": "not_found",
                    "message": "No federal fiscal data is available.",
                },
                status_code=404,
            )

        return json_response(
            {
                "status": "ok",
                "scope": "Federal",
                "fiscal_year": row[0],
                "indicators": {
                    "revenues": row[1],
                    "program_expenses_excluding_net_actuarial_losses": row[2],
                    "public_debt_charges": row[3],
                    "budgetary_balance": row[4],
                    "accumulated_deficit": row[5],
                    "fiscal_position": row[6],
                    "debt_charges_as_percent_of_revenue": row[7],
                },
            }
        )

    except Exception:
        logging.exception(
            "Federal fiscal retrieval failed."
        )

        return json_response(
            {
                "status": "error",
                "message": "Unable to retrieve federal fiscal data.",
            },
            status_code=500,
        )

    finally:
        if connection is not None:
            connection.close()


# =========================================================
# ONTARIO FISCAL DATA
# =========================================================

@app.route(
    route="fiscal/ontario/latest",
    methods=["GET"],
)
def ontario_fiscal_latest(
    req: func.HttpRequest,
) -> func.HttpResponse:
    """Return the latest Ontario fiscal indicators."""

    connection = None

    try:
        connection = get_sql_connection()
        cursor = connection.cursor()

        query = """
        SELECT TOP 1
            fiscal_year,
            own_source_revenues,
            federal_transfers,
            total_revenues,
            total_program_expenditures,
            debt_charges,
            total_expenditures,
            deficit_or_surplus,
            net_debt,
            fiscal_position,
            federal_transfers_as_percent_of_revenue,
            debt_charges_as_percent_of_revenue
        FROM ontario_fiscal_annual
        ORDER BY fiscal_year DESC;
        """

        cursor.execute(query)
        row = cursor.fetchone()

        if row is None:
            return json_response(
                {
                    "status": "not_found",
                    "message": "No Ontario fiscal data is available.",
                },
                status_code=404,
            )

        return json_response(
            {
                "status": "ok",
                "scope": "Ontario",
                "fiscal_year": row[0],
                "indicators": {
                    "own_source_revenues": row[1],
                    "federal_transfers": row[2],
                    "total_revenues": row[3],
                    "total_program_expenditures": row[4],
                    "debt_charges": row[5],
                    "total_expenditures": row[6],
                    "deficit_or_surplus": row[7],
                    "net_debt": row[8],
                    "fiscal_position": row[9],
                    "federal_transfers_as_percent_of_revenue": row[10],
                    "debt_charges_as_percent_of_revenue": row[11],
                },
            }
        )

    except Exception:
        logging.exception(
            "Ontario fiscal retrieval failed."
        )

        return json_response(
            {
                "status": "error",
                "message": "Unable to retrieve Ontario fiscal data.",
            },
            status_code=500,
        )

    finally:
        if connection is not None:
            connection.close()


# =========================================================
# RETAIL SALES FORECAST
# =========================================================

@app.route(
    route="forecast/latest",
    methods=["GET"],
)
def forecast_latest(req: func.HttpRequest) -> func.HttpResponse:
    """Return the latest retail forecast and selected model evidence."""

    connection = None

    try:
        connection = get_sql_connection()
        cursor = connection.cursor()

        forecast_query = """
        SELECT TOP 1
            forecast_horizon_months,
            forecast_retail_sales_dollars,
            model,
            ref_date
        FROM retail_forecasts
        ORDER BY ref_date DESC;
        """

        cursor.execute(forecast_query)
        forecast_row = cursor.fetchone()

        metadata_query = """
        SELECT TOP 1
            forecast_horizon_months,
            holdout_mae,
            holdout_rmse,
            selected_model,
            selection_metric,
            target,
            validation_mae,
            validation_rmse
        FROM retail_model_metadata;
        """

        cursor.execute(metadata_query)
        metadata_row = cursor.fetchone()

        metrics_query = """
        SELECT
            model,
            mae,
            rmse,
            target,
            forecast_horizon_months,
            evaluation_period
        FROM retail_model_metrics
        ORDER BY rmse ASC;
        """

        cursor.execute(metrics_query)

        metrics = rows_to_dicts(
            cursor,
            cursor.fetchall(),
        )

        if forecast_row is None:
            return json_response(
                {
                    "status": "not_found",
                    "message": "No retail forecast is available.",
                },
                status_code=404,
            )

        metadata = None

        if metadata_row is not None:
            metadata = {
                "forecast_horizon_months": metadata_row[0],
                "holdout_mae": metadata_row[1],
                "holdout_rmse": metadata_row[2],
                "selected_model": metadata_row[3],
                "selection_metric": metadata_row[4],
                "target": metadata_row[5],
                "validation_mae": metadata_row[6],
                "validation_rmse": metadata_row[7],
            }

        return json_response(
            {
                "status": "ok",
                "scope": "Canada retail sales",
                "forecast": {
                    "reference_period": forecast_row[3],
                    "forecast_retail_sales_dollars": forecast_row[1],
                    "model": forecast_row[2],
                    "forecast_horizon_months": forecast_row[0],
                },
                "model_metadata": metadata,
                "model_metrics": metrics,
            }
        )

    except Exception:
        logging.exception(
            "Retail forecast retrieval failed."
        )

        return json_response(
            {
                "status": "error",
                "message": "Unable to retrieve retail forecast data.",
            },
            status_code=500,
        )

    finally:
        if connection is not None:
            connection.close()