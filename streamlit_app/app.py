import requests
import streamlit as st
from openai import OpenAI
from azure.identity import ClientSecretCredential
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

# =========================================================
# PAGE CONFIGURATION
# =========================================================

st.set_page_config(
    page_title="Canadian Economic Intelligence | Live Demo",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


# =========================================================
# PROJECT LINKS
# =========================================================

PORTFOLIO_URL = (
    "https://mfchowdhury.github.io/"
    "canadian-economic-intelligence-platform/"
)

GITHUB_URL = (
    "https://github.com/mfchowdhury/"
    "canadian-economic-intelligence-platform"
)


# =========================================================
# AZURE AUTHENTICATION
# =========================================================

def get_azure_credential():
    """Create the Azure credential used for ADF management requests."""

    return ClientSecretCredential(
        tenant_id=st.secrets["azure"]["tenant_id"],
        client_id=st.secrets["azure"]["client_id"],
        client_secret=st.secrets["azure"]["client_secret"],
    )


def get_management_token():
    """Acquire an Azure Management API access token."""

    credential = get_azure_credential()

    return credential.get_token(
        "https://management.azure.com/.default"
    ).token



# =========================================================
# ECONOMIC INTELLIGENCE API
# =========================================================

def get_api_config():
    """Return the Economic Intelligence API configuration."""

    return {
        "base_url": st.secrets["function_api"]["base_url"].rstrip("/"),
        "function_key": st.secrets["function_api"]["function_key"],
    }


def call_economic_api(route, params=None):
    """Call a controlled Azure Function retrieval endpoint."""

    config = get_api_config()
    url = f"{config['base_url']}/api/{route.lstrip('/')}"

    response = requests.get(
        url,
        params=dict(params or {}),
        headers={
            "x-functions-key": config["function_key"]
        },
        timeout=30,
    )

    if response.status_code == 404:
        try:
            return response.json()
        except ValueError:
            return {
                "status": "not_found",
                "message": "No matching project data was found.",
            }

    response.raise_for_status()
    return response.json()

# =========================================================
# AZURE DATA FACTORY CONFIGURATION
# =========================================================

def get_adf_config():
    """Return the Azure Data Factory configuration from Streamlit Secrets."""

    return {
        "subscription_id": st.secrets["azure"]["subscription_id"],
        "resource_group": st.secrets["azure"]["resource_group"],
        "data_factory": st.secrets["azure"]["data_factory"],
        "pipeline_name": st.secrets["azure"]["pipeline_name"],
    }


def get_adf_headers():
    """Build authenticated headers for Azure Management API requests."""

    token = get_management_token()

    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


# =========================================================
# AZURE DATA FACTORY ACCESS TEST
# =========================================================

def test_adf_access():
    """Verify that the service principal can access the Data Factory."""

    config = get_adf_config()

    url = (
        f"https://management.azure.com/subscriptions/"
        f"{config['subscription_id']}"
        f"/resourceGroups/{config['resource_group']}"
        f"/providers/Microsoft.DataFactory/factories/"
        f"{config['data_factory']}"
        f"?api-version=2018-06-01"
    )

    response = requests.get(
        url,
        headers=get_adf_headers(),
        timeout=30,
    )

    response.raise_for_status()

    return response.json()


# =========================================================
# AZURE DATA FACTORY PIPELINE TRIGGER
# =========================================================

def trigger_adf_pipeline():
    """Start the configured Azure Data Factory master pipeline."""

    config = get_adf_config()

    url = (
        f"https://management.azure.com/subscriptions/"
        f"{config['subscription_id']}"
        f"/resourceGroups/{config['resource_group']}"
        f"/providers/Microsoft.DataFactory/factories/"
        f"{config['data_factory']}"
        f"/pipelines/{config['pipeline_name']}/createRun"
        f"?api-version=2018-06-01"
    )

    response = requests.post(
        url,
        headers=get_adf_headers(),
        json={},
        timeout=30,
    )

    response.raise_for_status()

    return response.json()["runId"]


# =========================================================
# AZURE DATA FACTORY PIPELINE RUN STATUS
# =========================================================

def get_adf_run_status(run_id):
    """Retrieve status and timestamps for a specific ADF pipeline run."""

    config = get_adf_config()

    url = (
        f"https://management.azure.com/subscriptions/"
        f"{config['subscription_id']}"
        f"/resourceGroups/{config['resource_group']}"
        f"/providers/Microsoft.DataFactory/factories/"
        f"{config['data_factory']}"
        f"/pipelineruns/{run_id}"
        f"?api-version=2018-06-01"
    )

    response = requests.get(
        url,
        headers=get_adf_headers(),
        timeout=30,
    )

    response.raise_for_status()

    return response.json()


# =========================================================
# AZURE DATA FACTORY LATEST PIPELINE RUN
# =========================================================

def get_latest_adf_pipeline_run():
    """Retrieve the most recent execution of the configured master pipeline."""

    config = get_adf_config()

    url = (
        f"https://management.azure.com/subscriptions/"
        f"{config['subscription_id']}"
        f"/resourceGroups/{config['resource_group']}"
        f"/providers/Microsoft.DataFactory/factories/"
        f"{config['data_factory']}"
        f"/queryPipelineRuns"
        f"?api-version=2018-06-01"
    )

    # Keep ADF timestamps in UTC. Timezone conversion is presentation-only.
    now_utc = datetime.now(timezone.utc)

    request_body = {
        "lastUpdatedAfter": (
            now_utc - timedelta(days=90)
        ).isoformat().replace("+00:00", "Z"),
        "lastUpdatedBefore": (
            now_utc + timedelta(minutes=5)
        ).isoformat().replace("+00:00", "Z"),
        "filters": [
            {
                "operand": "PipelineName",
                "operator": "Equals",
                "values": [config["pipeline_name"]],
            }
        ],
        "orderBy": [
            {
                "orderBy": "RunStart",
                "order": "DESC",
            }
        ],
    }

    response = requests.post(
        url,
        headers=get_adf_headers(),
        json=request_body,
        timeout=30,
    )

    response.raise_for_status()

    runs = response.json().get("value", [])
    return runs[0] if runs else None


# =========================================================
# AZURE DATA FACTORY ACTIVITY RUNS
# =========================================================

def get_adf_activity_runs(run_id):
    """Retrieve activity-level execution details for an ADF pipeline run."""

    config = get_adf_config()

    url = (
        f"https://management.azure.com/subscriptions/"
        f"{config['subscription_id']}"
        f"/resourceGroups/{config['resource_group']}"
        f"/providers/Microsoft.DataFactory/factories/"
        f"{config['data_factory']}"
        f"/pipelineruns/{run_id}/queryActivityruns"
        f"?api-version=2018-06-01"
    )

    # ADF requires a time window when querying activity runs.
    request_body = {
        "lastUpdatedAfter": "2000-01-01T00:00:00Z",
        "lastUpdatedBefore": "2100-01-01T00:00:00Z",
    }

    response = requests.post(
        url,
        headers=get_adf_headers(),
        json=request_body,
        timeout=30,
    )

    response.raise_for_status()

    return response.json().get("value", [])


# =========================================================
# DATE, STATUS, AND DURATION FORMATTING
# =========================================================

def format_adf_datetime(timestamp):
    """Format an ADF UTC timestamp for presentation in Toronto time."""

    if not timestamp:
        return "—"

    try:
        parsed_time = datetime.fromisoformat(
            timestamp.replace("Z", "+00:00")
        )

        if parsed_time.tzinfo is None:
            parsed_time = parsed_time.replace(tzinfo=timezone.utc)

        toronto_time = parsed_time.astimezone(
            ZoneInfo("America/Toronto")
        )

        return toronto_time.strftime(
            "%b %d, %Y, %I:%M %p %Z"
        )

    except (ValueError, TypeError):
        return timestamp


def format_pipeline_status(status):
    """Convert an ADF status value into a user-friendly label."""

    status_labels = {
        "Queued": "Starting",
        "InProgress": "In Progress",
        "Succeeded": "Succeeded",
        "Failed": "Failed",
        "Cancelled": "Cancelled",
        "Canceling": "Cancelling",
    }

    return status_labels.get(status, status or "Ready")


def format_activity_status(status):
    """Return a concise display label for an ADF activity status."""

    status_labels = {
        "Queued": "Starting",
        "InProgress": "In Progress",
        "Succeeded": "✓ Succeeded",
        "Failed": "✕ Failed",
        "Cancelled": "Cancelled",
        "Canceling": "Cancelling",
    }

    return status_labels.get(status, status or "Waiting")


def format_duration(duration_ms):
    """Convert an ADF activity duration from milliseconds to a readable value."""

    if duration_ms is None:
        return "—"

    try:
        total_seconds = max(0, int(duration_ms) // 1000)

        minutes, seconds = divmod(total_seconds, 60)
        hours, minutes = divmod(minutes, 60)

        if hours:
            return f"{hours}h {minutes}m {seconds}s"

        if minutes:
            return f"{minutes}m {seconds}s"

        return f"{seconds}s"

    except (TypeError, ValueError):
        return "—"


# =========================================================
# REPRESENTATIVE ACTIVITY CONFIGURATION
# =========================================================

# Only the six primary workflow activities are shown in the public demo.
# Failure-handling activities remain part of ADF but are not displayed
# as normal processing stages.

ACTIVITY_DISPLAY_NAMES = {
    "run_retail_sales_ingestion": "Retail Sales Ingestion",
    "run_fx_ingestion": "USD/CAD Ingestion",
    "run_retail_silver": "Retail Sales Silver",
    "run_fx_silver": "USD/CAD Silver",
    "run_gold_economic_overview": "Gold Economic Overview",
    "run_gold_forecasting_features": "Gold Forecasting Features",
}

ACTIVITY_ORDER = list(ACTIVITY_DISPLAY_NAMES.keys())


def prepare_activity_rows(activity_runs):
    """Prepare the six representative ADF activities for display."""

    activity_lookup = {
        activity.get("activityName"): activity
        for activity in activity_runs
        if activity.get("activityName") in ACTIVITY_DISPLAY_NAMES
    }

    rows = []

    for activity_name in ACTIVITY_ORDER:
        activity = activity_lookup.get(activity_name)

        if activity:
            rows.append(
                {
                    "Activity": ACTIVITY_DISPLAY_NAMES[activity_name],
                    "Status": format_activity_status(
                        activity.get("status")
                    ),
                    "Run Start": format_adf_datetime(
                        activity.get("activityRunStart")
                    ),
                    "Duration": format_duration(
                        activity.get("durationInMs")
                    ),
                }
            )

        else:
            rows.append(
                {
                    "Activity": ACTIVITY_DISPLAY_NAMES[activity_name],
                    "Status": "Waiting",
                    "Run Start": "—",
                    "Duration": "—",
                }
            )

    return rows


# =========================================================
# AUTOMATION COOLDOWN
# =========================================================

AUTOMATION_COOLDOWN_HOURS = 24


def parse_adf_datetime(timestamp):
    """Parse an ADF timestamp as a timezone-aware UTC datetime."""

    if not timestamp:
        return None

    try:
        parsed_time = datetime.fromisoformat(
            timestamp.replace("Z", "+00:00")
        )

        if parsed_time.tzinfo is None:
            parsed_time = parsed_time.replace(tzinfo=timezone.utc)

        return parsed_time.astimezone(timezone.utc)

    except (ValueError, TypeError):
        return None


def get_automation_cooldown(latest_run):
    """
    Return global cooldown information from the latest real ADF run.

    ADF is the shared source of truth, so the cooldown is shared across
    visitors and does not depend on a browser or Streamlit session.
    """

    if not latest_run:
        return {
            "active": False,
            "next_eligible_utc": None,
            "remaining": timedelta(0),
        }

    run_started = parse_adf_datetime(
        latest_run.get("runStart")
    )

    if not run_started:
        return {
            "active": False,
            "next_eligible_utc": None,
            "remaining": timedelta(0),
        }

    next_eligible_utc = run_started + timedelta(
        hours=AUTOMATION_COOLDOWN_HOURS
    )
    remaining = next_eligible_utc - datetime.now(timezone.utc)

    return {
        "active": remaining.total_seconds() > 0,
        "next_eligible_utc": next_eligible_utc,
        "remaining": max(remaining, timedelta(0)),
    }


def format_local_datetime(dt_value):
    """Format a timezone-aware datetime in Toronto/ET time."""

    if not dt_value:
        return "—"

    toronto_time = dt_value.astimezone(
        ZoneInfo("America/Toronto")
    )

    return toronto_time.strftime(
        "%b %d, %Y, %I:%M %p %Z"
    )


def format_remaining_time(delta_value):
    """Format a remaining cooldown duration."""

    total_seconds = max(
        0,
        int(delta_value.total_seconds())
    )

    hours, remainder = divmod(total_seconds, 3600)
    minutes, _ = divmod(remainder, 60)

    if hours:
        return f"{hours}h {minutes}m"

    return f"{minutes}m"


# =========================================================
# OPENAI CONFIGURATION
# =========================================================

def get_openai_client():
    """Create the OpenAI client from private Streamlit secrets."""

    return OpenAI(
        api_key=st.secrets["openai"]["api_key"]
    )


def get_openai_model():
    """Return the configured OpenAI model."""

    return st.secrets["openai"]["model"]


# =========================================================
# ECONOMIC ASSISTANT SCOPE AND ROUTING
# =========================================================

SUPPORTED_GEOS = [
    "Canada",
    "Newfoundland and Labrador",
    "Prince Edward Island",
    "Nova Scotia",
    "New Brunswick",
    "Quebec",
    "Ontario",
    "Manitoba",
    "Saskatchewan",
    "Alberta",
    "British Columbia",
    "Yukon",
    "Northwest Territories",
    "Nunavut",
]

GEO_ALIASES = {
    "canada": "Canada",
    "newfoundland and labrador": "Newfoundland and Labrador",
    "newfoundland": "Newfoundland and Labrador",
    "pei": "Prince Edward Island",
    "prince edward island": "Prince Edward Island",
    "nova scotia": "Nova Scotia",
    "new brunswick": "New Brunswick",
    "quebec": "Quebec",
    "ontario": "Ontario",
    "manitoba": "Manitoba",
    "saskatchewan": "Saskatchewan",
    "alberta": "Alberta",
    "british columbia": "British Columbia",
    "bc": "British Columbia",
    "yukon": "Yukon",
    "northwest territories": "Northwest Territories",
    "nwt": "Northwest Territories",
    "nunavut": "Nunavut",
}

ECONOMIC_SCOPE_TERMS = {
    "economy", "economic", "gdp", "growth", "inflation", "cpi",
    "employment", "unemployment", "labour", "labor", "jobs",
    "retail", "sales", "population", "exchange rate", "usd/cad",
    "cad", "policy rate", "interest rate", "housing", "affordability",
    "fiscal", "budget", "deficit", "surplus", "debt", "revenue",
    "expenditure", "productivity", "industry", "forecast",
    "manufacturing", "wholesale", "consumer", "prices",
}

SUPPORTED_SCOPE_TERMS = {
    "gdp", "growth", "inflation", "cpi", "employment",
    "unemployment", "labour", "labor", "jobs", "retail",
    "sales", "population", "exchange rate", "usd/cad", "cad",
    "policy rate", "interest rate", "housing", "affordability",
    "fiscal", "budget", "deficit", "surplus", "debt", "revenue",
    "expenditure", "productivity", "industry", "forecast",
    "economy", "economic",
}

SUGGESTED_QUESTIONS = [
    "How is Canada's economy performing?",
    "What is Ontario's unemployment rate?",
    "What is the latest inflation rate in Canada?",
    "What is Ontario's latest fiscal position?",
    "What is the latest retail sales forecast?",
]


def normalize_question(question):
    """Normalize a question for lightweight routing."""

    return " ".join(
        question.lower().strip().split()
    )


def contains_any(text, terms):
    """Return True when any routing term occurs in the text."""

    return any(term in text for term in terms)


def detect_geography(question):
    """Detect a supported Canadian geography from a question."""

    normalized = normalize_question(question)

    # Longer aliases are checked first to avoid partial matches.
    for alias in sorted(
        GEO_ALIASES,
        key=len,
        reverse=True,
    ):
        if alias in normalized:
            return GEO_ALIASES[alias]

    return None


def extract_industry_phrase(question):
    """
    Extract a simple industry phrase for controlled industry endpoints.

    The API remains authoritative. If the phrase does not match a reporting
    table label, the app returns a data-availability message rather than
    guessing another industry.
    """

    normalized = normalize_question(question)

    known_industries = [
        "manufacturing",
        "retail trade",
        "wholesale trade",
        "construction",
        "mining",
        "utilities",
        "transportation and warehousing",
        "finance and insurance",
        "real estate",
        "professional services",
        "accommodation and food services",
    ]

    for industry in known_industries:
        if industry in normalized:
            return industry.title()

    return None


def route_economic_question(question):
    """
    Classify a question into:
    - supported
    - related_unsupported
    - out_of_scope

    Supported questions are mapped only to controlled Azure Function routes.
    """

    normalized = normalize_question(question)

    if not normalized:
        return {
            "status": "empty",
            "message": "Enter a question first.",
        }

    economic_related = contains_any(
        normalized,
        ECONOMIC_SCOPE_TERMS,
    )

    if not economic_related:
        return {
            "status": "out_of_scope",
            "message": (
                "This question is outside the scope of the Economic "
                "Intelligence Assistant. Ask about Canadian economic "
                "indicators available in this platform, such as GDP, "
                "inflation, employment, retail sales, housing, fiscal "
                "indicators, industries, regional conditions, or forecasts."
            ),
        }

    geo = detect_geography(normalized)

    # Forecast questions are intentionally limited to the project's
    # retail-sales forecasting mart.
    if "forecast" in normalized:
        if contains_any(
            normalized,
            {"retail", "sales", "forecast", "canada", "economic"},
        ):
            return {
                "status": "supported",
                "domain": "Retail Sales Forecast",
                "route": "forecast/latest",
                "params": {},
            }

    # Fiscal routing.
    if contains_any(
        normalized,
        {
            "fiscal", "budget", "deficit", "surplus",
            "debt", "revenue", "expenditure",
        },
    ):
        if "ontario" in normalized:
            return {
                "status": "supported",
                "domain": "Ontario Fiscal",
                "route": "fiscal/ontario/latest",
                "params": {},
            }

        if contains_any(
            normalized,
            {"federal", "canada", "canadian", "government"},
        ):
            return {
                "status": "supported",
                "domain": "Federal Fiscal",
                "route": "fiscal/federal/latest",
                "params": {},
            }

        return {
            "status": "related_unsupported",
            "message": (
                "This is a Canadian economic question, but the fiscal "
                "scope must be federal or Ontario for the validated "
                "fiscal datasets currently available in this platform."
            ),
        }

    # Industry productivity.
    if "productivity" in normalized:
        industry = extract_industry_phrase(normalized)

        if industry:
            return {
                "status": "supported",
                "domain": "Industry Productivity",
                "route": "industry/productivity",
                "params": {"industry": industry},
            }

        return {
            "status": "related_unsupported",
            "message": (
                "This topic is related to Canadian economics, but an "
                "industry is required for the productivity dataset "
                "available in this platform."
            ),
        }

    # Industry GDP.
    if "gdp" in normalized and "industry" in normalized:
        industry = extract_industry_phrase(normalized)

        if industry:
            return {
                "status": "supported",
                "domain": "Industry GDP",
                "route": "industry/gdp",
                "params": {"industry": industry},
            }

        return {
            "status": "related_unsupported",
            "message": (
                "This is an industry-GDP question, but the requested "
                "industry could not be matched safely to a controlled "
                "reporting-table label."
            ),
        }

    # Industry retail sales.
    if (
        "retail" in normalized
        and "industry" in normalized
    ):
        industry = extract_industry_phrase(normalized)

        if industry:
            return {
                "status": "supported",
                "domain": "Industry Retail Sales",
                "route": "industry/retail",
                "params": {"industry": industry},
            }

        return {
            "status": "related_unsupported",
            "message": (
                "This is an industry retail-sales question, but the "
                "requested industry could not be matched safely to a "
                "controlled reporting-table label."
            ),
        }

    # Affordability and housing questions use the dedicated mart.
    if contains_any(
        normalized,
        {"housing", "affordability"},
    ):
        return {
            "status": "supported",
            "domain": "Affordability & Housing",
            "route": "affordability/latest",
            "params": {},
        }

    # Province/territory questions use the regional endpoint.
    if geo and geo != "Canada":
        return {
            "status": "supported",
            "domain": "Regional Analysis",
            "route": "regional",
            "params": {"geo": geo},
        }

    # General supported Canadian macro questions use the national endpoint.
    if contains_any(
        normalized,
        SUPPORTED_SCOPE_TERMS,
    ):
        return {
            "status": "supported",
            "domain": "National Economy",
            "route": "national/latest",
            "params": {},
        }

    return {
        "status": "related_unsupported",
        "message": (
            "This topic is related to Canadian economics, but the "
            "indicator needed to answer it is not currently available "
            "through this platform's validated datasets."
        ),
    }


# =========================================================
# ASSISTANT EVIDENCE FORMATTING
# =========================================================

INDICATOR_LABELS = {
    "retail_sales_dollars": "Retail Sales",
    "retail_sales_mom_percent": "Retail Sales MoM",
    "retail_sales_yoy_percent": "Retail Sales YoY",
    "cpi_yoy_inflation_percent": "Inflation",
    "employment_thousands": "Employment",
    "employment_yoy_percent": "Employment YoY",
    "unemployment_rate_percent": "Unemployment Rate",
    "real_gdp_chained_2017_millions": "Real GDP",
    "real_gdp_mom_percent": "Real GDP MoM",
    "real_gdp_yoy_percent": "Real GDP YoY",
    "population": "Population",
    "avg_monthly_fx_usd_cad": "Average USD/CAD",
    "policy_rate": "Policy Rate",
    "housing_starts_saar": "Housing Starts SAAR",
    "retail_sales_per_capita": "Retail Sales per Capita",
    "retail_sales_yoy_gap_vs_canada": "Retail Sales YoY Gap vs Canada",
    "cpi_inflation_gap_vs_canada": "Inflation Gap vs Canada",
    "employment_yoy_gap_vs_canada": "Employment YoY Gap vs Canada",
    "unemployment_rate_gap_vs_canada": "Unemployment Gap vs Canada",
    "deficit_or_surplus": "Deficit / Surplus",
    "net_debt": "Net Debt",
    "total_revenues": "Total Revenues",
    "total_expenditures": "Total Expenditures",
    "own_source_revenues": "Own-Source Revenues",
    "federal_transfers": "Federal Transfers",
    "total_program_expenditures": "Program Expenditures",
    "debt_charges": "Debt Charges",
    "federal_transfers_as_percent_of_revenue": "Federal Transfers / Revenue",
    "debt_charges_as_percent_of_revenue": "Debt Charges / Revenue",
    "fiscal_position": "Fiscal Position",
}


def humanize_indicator_name(name):
    """Convert a technical field name to a readable label."""

    if name in INDICATOR_LABELS:
        return INDICATOR_LABELS[name]

    return name.replace("_", " ").title()


def format_evidence_value(key, value):
    """Apply concise display formatting to common economic indicators."""

    if value is None:
        return "—"

    if isinstance(value, str):
        return value

    key_lower = key.lower()

    if "percent" in key_lower or key_lower.endswith("_rate"):
        return f"{value:,.2f}%"

    if key_lower == "policy_rate":
        return f"{value:,.2f}%"

    if key_lower == "avg_monthly_fx_usd_cad":
        return f"{value:,.4f}"

    if "population" in key_lower:
        return f"{value:,.0f}"

    if "employment_thousands" in key_lower:
        return f"{value:,.1f} thousand"

    if "retail_sales_dollars" in key_lower:
        return f"${value / 1_000_000_000:,.2f}B"

    if "real_gdp_chained_2017_millions" in key_lower:
        return f"${value / 1_000:,.2f}B"

    if isinstance(value, float):
        return f"{value:,.2f}"

    if isinstance(value, int):
        return f"{value:,}"

    return str(value)


def build_evidence_rows(api_result):
    """
    Convert a controlled API response into compact supporting-evidence rows.
    """

    rows = []
    indicators = api_result.get("indicators", {})

    for key, item in indicators.items():
        if isinstance(item, dict):
            value = item.get("value")
            period = item.get(
                "reference_period",
                api_result.get("reference_period", "—"),
            )
        else:
            value = item
            period = (
                api_result.get("reference_period")
                or api_result.get("fiscal_year")
                or "—"
            )

        rows.append(
            {
                "Indicator": humanize_indicator_name(key),
                "Value": format_evidence_value(key, value),
                "Reference Period": period or "—",
            }
        )

    # Some endpoints may expose useful scalar fields outside "indicators".
    if not rows:
        excluded = {
            "status",
            "scope",
            "message",
            "reference_period",
            "fiscal_year",
        }

        for key, value in api_result.items():
            if key in excluded:
                continue

            if isinstance(value, (dict, list)):
                continue

            rows.append(
                {
                    "Indicator": humanize_indicator_name(key),
                    "Value": format_evidence_value(key, value),
                    "Reference Period": (
                        api_result.get("reference_period")
                        or api_result.get("fiscal_year")
                        or "—"
                    ),
                }
            )

    return rows


def build_grounding_text(api_result, evidence_rows):
    """Build a compact evidence block for the language model."""

    scope = api_result.get("scope", "Canadian economy")
    period = (
        api_result.get("reference_period")
        or api_result.get("fiscal_year")
        or "varies by indicator"
    )

    lines = [
        f"Scope: {scope}",
        f"Overall reference period: {period}",
    ]

    for row in evidence_rows:
        lines.append(
            f"- {row['Indicator']}: {row['Value']} "
            f"(reference period: {row['Reference Period']})"
        )

    return "\n".join(lines)


# =========================================================
# GROUNDED LANGUAGE-MODEL RESPONSE
# =========================================================

def generate_grounded_answer(question, api_result, evidence_rows):
    """
    Generate an explanation using only evidence retrieved from the platform.
    """

    evidence_text = build_grounding_text(
        api_result,
        evidence_rows,
    )

    instructions = """
You are the Economic Intelligence Assistant for a portfolio project
focused on Canadian economic data.

Answer only from the validated evidence supplied below.

Rules:
- Do not invent, estimate, or import facts that are not in the evidence.
- Do not use outside knowledge to fill missing values.
- Preserve the reference period when it matters.
- If the evidence is insufficient for part of the question, say so clearly.
- Distinguish observed historical indicators from forecasts.
- Do not provide investment, legal, or policy advice.
- Keep the answer concise and analytical, usually 1 to 3 short paragraphs.
- Use plain language while retaining important economic terminology.
"""

    prompt = f"""
USER QUESTION:
{question}

VALIDATED PROJECT EVIDENCE:
{evidence_text}

Provide a grounded answer to the user's question.
"""

    client = get_openai_client()

    response = client.responses.create(
        model=get_openai_model(),
        instructions=instructions,
        input=prompt,
    )

    return response.output_text.strip()


def answer_economic_question(question):
    """
    Complete controlled assistant flow:
    route -> retrieve -> format evidence -> grounded LLM explanation.
    """

    route_result = route_economic_question(question)

    if route_result.get("status") != "supported":
        return {
            "status": route_result.get("status"),
            "message": route_result.get("message"),
        }

    api_result = call_economic_api(
        route_result["route"],
        params=route_result.get("params", {}),
    )

    if api_result.get("status") == "not_found":
        return {
            "status": "related_unsupported",
            "message": (
                api_result.get("message")
                or "The topic is related to this project, but no matching "
                   "validated data was found."
            ),
        }

    if api_result.get("status") != "ok":
        return {
            "status": "api_error",
            "message": (
                api_result.get("message")
                or "The Economic Intelligence API returned an error."
            ),
        }

    evidence_rows = build_evidence_rows(api_result)

    if not evidence_rows:
        return {
            "status": "related_unsupported",
            "message": (
                "Validated data was retrieved, but there was not enough "
                "evidence to construct a grounded answer."
            ),
        }

    answer = generate_grounded_answer(
        question,
        api_result,
        evidence_rows,
    )

    return {
        "status": "ok",
        "answer": answer,
        "evidence_rows": evidence_rows,
        "domain": route_result.get("domain"),
        "scope": api_result.get("scope"),
    }


# =========================================================
# STREAMLIT SESSION STATE
# =========================================================

if "adf_run_id" not in st.session_state:
    st.session_state.adf_run_id = None

if "adf_run_data" not in st.session_state:
    st.session_state.adf_run_data = None

if "adf_activity_runs" not in st.session_state:
    st.session_state.adf_activity_runs = []

if "adf_trigger_message" not in st.session_state:
    st.session_state.adf_trigger_message = None


if "assistant_result" not in st.session_state:
    st.session_state.assistant_result = None

if "assistant_question" not in st.session_state:
    st.session_state.assistant_question = ""


# Load the latest real ADF execution when a new Streamlit session starts.
# ADF remains the source of truth, so the latest result survives browser
# refreshes and remains visible until a newer pipeline run exists.
if st.session_state.adf_run_id is None:
    try:
        latest_run = get_latest_adf_pipeline_run()

        if latest_run:
            st.session_state.adf_run_id = latest_run.get("runId")
            st.session_state.adf_run_data = latest_run

            if st.session_state.adf_run_id:
                st.session_state.adf_activity_runs = get_adf_activity_runs(
                    st.session_state.adf_run_id
                )

    except Exception:
        pass


# =========================================================
# GLOBAL STYLES
# =========================================================

st.markdown(
    """
    <style>
        .block-container {
            max-width: 1200px;
            padding-top: 2rem;
            padding-bottom: 3rem;
        }

        .main-title {
            font-size: 2.6rem;
            font-weight: 700;
            color: #16324F;
            margin-bottom: 0.35rem;
        }

        .main-subtitle {
            font-size: 1.05rem;
            color: #607D8B;
            margin-bottom: 2rem;
            line-height: 1.6;
        }

        .section-title {
            font-size: 1.7rem;
            font-weight: 700;
            color: #16324F;
            margin-top: 0.5rem;
            margin-bottom: 0.5rem;
        }

        .section-description {
            font-size: 1rem;
            color: #607D8B;
            margin-bottom: 1.3rem;
            line-height: 1.6;
        }

        .pipeline-box {
            border: 1px solid #D9E1E8;
            border-radius: 12px;
            padding: 1.35rem;
            background-color: #F8FAFC;
            text-align: center;
            font-weight: 600;
            color: #16324F;
        }

        .note-text {
            color: #607D8B;
            font-size: 0.9rem;
            line-height: 1.5;
        }

        .sidebar-label {
            color: #607D8B;
            font-size: 0.78rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            margin-bottom: 0.5rem;
        }

        div[data-testid="stSidebar"] {
            background-color: #F5F7FA;
        }
    </style>
    """,
    unsafe_allow_html=True,
)


# =========================================================
# SIDEBAR
# =========================================================

with st.sidebar:

    st.markdown(
        '<div class="sidebar-label">LIVE DEMO</div>',
        unsafe_allow_html=True,
    )

    st.markdown("## Canadian Economic Intelligence")

    st.caption(
        "Interactive demonstrations of the platform's "
        "automation and analytical capabilities."
    )

    page = st.radio(
        "Navigation",
        [
            "Automation Demo",
            "Economic Intelligence Assistant",
        ],
        label_visibility="collapsed",
    )

    st.divider()

    st.markdown(
        f'<a href="{PORTFOLIO_URL}" target="_blank">'
        '← Back to Portfolio</a>',
        unsafe_allow_html=True,
    )

    st.markdown(
        f'<a href="{GITHUB_URL}" target="_blank">'
        'View GitHub Repository ↗</a>',
        unsafe_allow_html=True,
    )


# =========================================================
# PAGE: AUTOMATION DEMO
# =========================================================

if page == "Automation Demo":

    st.markdown(
        '<div class="main-title">Automation Demo</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="main-subtitle">
            Trigger and monitor a representative Azure Data Factory workflow
            that ingests official Canadian economic data, executes Databricks
            transformations, and refreshes analytical Gold datasets.
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ---------------------------------------------------------
    # REPRESENTATIVE WORKFLOW
    # ---------------------------------------------------------

    st.markdown(
        '<div class="section-title">Representative Workflow</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="section-description">
            The live automation proof of concept uses Statistics Canada
            Retail Sales and Bank of Canada USD/CAD data to demonstrate
            the platform's end-to-end orchestration pattern.
        </div>
        """,
        unsafe_allow_html=True,
    )

    col1, col2 = st.columns(2)

    with col1:
        st.markdown(
            """
            <div class="pipeline-box">
                Statistics Canada<br>
                Retail Sales
            </div>
            """,
            unsafe_allow_html=True,
        )

    with col2:
        st.markdown(
            """
            <div class="pipeline-box">
                Bank of Canada<br>
                USD/CAD
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown(
        """
        <div style="text-align:center; font-size:1.8rem; padding:0.6rem;">
            ↓
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="pipeline-box">
            Silver Transformations
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div style="text-align:center; font-size:1.8rem; padding:0.6rem;">
            ↓
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="pipeline-box">
            Gold Economic Overview
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div style="text-align:center; font-size:1.8rem; padding:0.6rem;">
            ↓
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="pipeline-box">
            Gold Forecasting Features
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.write("")

    # ---------------------------------------------------------
    # LIVE ADF RUN STATUS
    # ---------------------------------------------------------

    st.markdown(
        '<div class="section-title">Latest Automation Run</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="section-description">
            Pipeline and activity status are retrieved directly from
            Azure Data Factory for the latest master-pipeline execution.
        </div>
        """,
        unsafe_allow_html=True,
    )

    pipeline_status = "Ready"
    run_started = "—"

    # Refresh the latest persisted ADF execution from Azure Data Factory.
    if st.session_state.adf_run_id:

        try:
            run_data = get_adf_run_status(
                st.session_state.adf_run_id
            )

            st.session_state.adf_run_data = run_data

            pipeline_status = format_pipeline_status(
                run_data.get("status")
            )

            run_started = format_adf_datetime(
                run_data.get("runStart")
            )

        except Exception:
            pipeline_status = "Status Unavailable"

    status_col1, status_col2, status_col3 = st.columns(
        [1.7, 1, 1.5]
    )

    with status_col1:
        st.metric(
            label="Pipeline",
            value="Master Economic Intelligence",
        )

    with status_col2:
        st.metric(
            label="Status",
            value=pipeline_status,
        )

    with status_col3:
        st.metric(
            label="Run Started",
            value=run_started,
        )

    if st.session_state.adf_run_id:
        st.caption(
            f"ADF Run ID: {st.session_state.adf_run_id}"
        )

    # Display the most recent pipeline trigger confirmation.
    if st.session_state.adf_trigger_message:
        st.success(st.session_state.adf_trigger_message)
        st.session_state.adf_trigger_message = None

    st.write("")

    # ---------------------------------------------------------
    # ACTIVITY EXECUTION STATUS
    # ---------------------------------------------------------

    st.markdown(
        '<div class="section-title">Activity Execution</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="section-description">
            The table shows the execution state of the six primary
            activities in the representative automation workflow.
        </div>
        """,
        unsafe_allow_html=True,
    )

    if st.session_state.adf_run_id:

        try:
            activity_runs = get_adf_activity_runs(
                st.session_state.adf_run_id
            )

            st.session_state.adf_activity_runs = activity_runs

        except Exception:
            activity_runs = st.session_state.adf_activity_runs

        activity_rows = prepare_activity_rows(
            activity_runs
        )

        st.dataframe(
            activity_rows,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Activity": st.column_config.TextColumn(
                    "Activity",
                    width="large",
                ),
                "Status": st.column_config.TextColumn(
                    "Status",
                    width="medium",
                ),
                "Run Start": st.column_config.TextColumn(
                    "Run Start",
                    width="large",
                ),
                "Duration": st.column_config.TextColumn(
                    "Duration",
                    width="small",
                ),
            },
        )

    else:
        waiting_rows = prepare_activity_rows([])

        st.dataframe(
            waiting_rows,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Activity": st.column_config.TextColumn(
                    "Activity",
                    width="large",
                ),
                "Status": st.column_config.TextColumn(
                    "Status",
                    width="medium",
                ),
                "Run Start": st.column_config.TextColumn(
                    "Run Start",
                    width="large",
                ),
                "Duration": st.column_config.TextColumn(
                    "Duration",
                    width="small",
                ),
            },
        )

    st.markdown(
        """
        <div class="note-text">
            This public demonstration runs the representative Retail Sales
            and USD/CAD workflow rather than the platform's complete
            ten-dataset registry.
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.write("")
    st.divider()

    # ---------------------------------------------------------
    # AUTOMATION CONTROLS
    # ---------------------------------------------------------

    st.markdown(
        '<div class="section-title">Automation Controls</div>',
        unsafe_allow_html=True,
    )

    current_adf_status = None

    if st.session_state.adf_run_data:
        current_adf_status = st.session_state.adf_run_data.get(
            "status"
        )

    run_is_active = current_adf_status in {
        "Queued",
        "InProgress",
        "Canceling",
    }

    # Use the latest real ADF run as shared state for the global cooldown.
    # This means every visitor sees the same eligibility window.
    latest_shared_run = st.session_state.adf_run_data

    try:
        refreshed_latest_run = get_latest_adf_pipeline_run()

        if refreshed_latest_run:
            latest_shared_run = refreshed_latest_run

            latest_shared_run_id = refreshed_latest_run.get("runId")

            # If Azure reports a newer run, make it the displayed result.
            if (
                latest_shared_run_id
                and latest_shared_run_id != st.session_state.adf_run_id
            ):
                st.session_state.adf_run_id = latest_shared_run_id
                st.session_state.adf_run_data = refreshed_latest_run
                st.session_state.adf_activity_runs = (
                    get_adf_activity_runs(latest_shared_run_id)
                )

    except Exception:
        # Keep the last successfully loaded result visible if Azure status
        # refresh is temporarily unavailable.
        pass

    cooldown = get_automation_cooldown(
        latest_shared_run
    )

    cooldown_active = cooldown["active"]

    run_disabled = (
        run_is_active
        or cooldown_active
    )

    if cooldown_active:
        st.info(
            "This public automation demo can be triggered once every "
            f"{AUTOMATION_COOLDOWN_HOURS} hours across all visitors. "
            "The latest run result remains visible until a newer run is "
            "started."
        )

        cooldown_col1, cooldown_col2 = st.columns(2)

        with cooldown_col1:
            st.metric(
                "Next Eligible Run",
                format_local_datetime(
                    cooldown["next_eligible_utc"]
                ),
            )

        with cooldown_col2:
            st.metric(
                "Cooldown Remaining",
                format_remaining_time(
                    cooldown["remaining"]
                ),
            )

    button_col1, button_col2 = st.columns(2)

    with button_col1:

        if st.button(
            "Run Automation Demo",
            type="primary",
            disabled=run_disabled,
            use_container_width=True,
        ):
            try:
                # Re-check the shared ADF state immediately before triggering
                # so a stale browser session cannot bypass the global cooldown.
                latest_before_trigger = get_latest_adf_pipeline_run()
                latest_cooldown = get_automation_cooldown(
                    latest_before_trigger
                )

                latest_status = (
                    latest_before_trigger.get("status")
                    if latest_before_trigger
                    else None
                )

                if latest_status in {
                    "Queued",
                    "InProgress",
                    "Canceling",
                }:
                    st.warning(
                        "A pipeline run is already active. Refresh the run "
                        "status before trying again."
                    )

                elif latest_cooldown["active"]:
                    st.warning(
                        "The 24-hour global cooldown is still active. "
                        "The next eligible run is "
                        f"{format_local_datetime(latest_cooldown['next_eligible_utc'])}."
                    )

                else:
                    run_id = trigger_adf_pipeline()

                    st.session_state.adf_run_id = run_id
                    st.session_state.adf_run_data = None
                    st.session_state.adf_activity_runs = []
                    st.session_state.adf_trigger_message = (
                        "ADF pipeline started successfully."
                    )

                    st.rerun()

            except Exception as e:
                st.error(
                    f"Unable to start ADF pipeline: {e}"
                )

    with button_col2:

        refresh_disabled = (
            st.session_state.adf_run_id is None
        )

        if st.button(
            "Refresh Run Status",
            disabled=refresh_disabled,
            use_container_width=True,
        ):
            try:
                run_data = get_adf_run_status(
                    st.session_state.adf_run_id
                )

                activity_runs = get_adf_activity_runs(
                    st.session_state.adf_run_id
                )

                st.session_state.adf_run_data = run_data
                st.session_state.adf_activity_runs = activity_runs

                st.rerun()

            except Exception as e:
                st.error(
                    f"Unable to retrieve ADF run status: {e}"
                )

    if run_is_active:
        st.info(
            "The Azure Data Factory pipeline is currently processing. "
            "Use Refresh Run Status to retrieve the latest pipeline "
            "and activity execution states."
        )

    elif pipeline_status == "Succeeded":
        st.success(
            "The latest automation run completed successfully."
        )

    elif pipeline_status == "Failed":
        st.error(
            "The latest automation run failed. Review the activity "
            "execution table to identify the failed stage."
        )

    elif pipeline_status == "Cancelled":
        st.warning(
            "The latest automation run was cancelled."
        )

    st.caption(
        "The latest ADF run and its activity results remain visible until "
        "a newer automation run is started. The 24-hour trigger cooldown "
        "is shared across visitors. Displayed timestamps use Toronto (ET) time."
    )


# =========================================================
# PAGE: ECONOMIC INTELLIGENCE ASSISTANT
# =========================================================

elif page == "Economic Intelligence Assistant":

    st.markdown(
        '<div class="main-title">Economic Intelligence Assistant</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="main-subtitle">
            Ask natural-language questions about validated Canadian economic
            indicators available through this platform. Answers are grounded
            in the project's controlled Azure Function API and analytical
            reporting layer.
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ---------------------------------------------------------
    # ASK THE ASSISTANT
    # ---------------------------------------------------------

    st.markdown(
        '<div class="section-title">Ask the Assistant</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="section-description">
            The assistant routes supported questions to controlled project
            endpoints, retrieves validated evidence, and uses GPT-5.6 Luna
            only to explain that evidence. It does not have unrestricted
            database access.
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.caption(
        "Try a question such as: "
        + "  •  ".join(SUGGESTED_QUESTIONS[:3])
    )

    question = st.text_area(
        "Economic question",
        value=st.session_state.assistant_question,
        placeholder=(
            "Example: How is Canada's economy performing?"
        ),
        height=110,
    )

    ask_button = st.button(
        "Ask the Assistant",
        type="primary",
        use_container_width=True,
    )

    if ask_button:

        if not question.strip():
            st.warning(
                "Enter an economic question first."
            )

        else:
            st.session_state.assistant_question = question.strip()

            try:
                with st.spinner(
                    "Retrieving validated evidence and preparing a grounded answer..."
                ):
                    assistant_result = answer_economic_question(
                        question.strip()
                    )

                st.session_state.assistant_result = assistant_result

            except requests.RequestException as e:
                st.session_state.assistant_result = {
                    "status": "api_error",
                    "message": (
                        "Unable to reach the Economic Intelligence API: "
                        f"{e}"
                    ),
                }

            except Exception as e:
                st.session_state.assistant_result = {
                    "status": "model_error",
                    "message": (
                        "Unable to generate the economic explanation: "
                        f"{e}"
                    ),
                }

    assistant_result = st.session_state.assistant_result

    if assistant_result:

        result_status = assistant_result.get("status")

        if result_status == "ok":
            st.write("")

            st.markdown(
                '<div class="section-title">Economic Intelligence</div>',
                unsafe_allow_html=True,
            )

            if assistant_result.get("domain"):
                st.caption(
                    f"Routed domain: {assistant_result['domain']}"
                )

            st.write(
                assistant_result.get(
                    "answer",
                    "No answer was generated.",
                )
            )

            st.markdown(
                '<div class="section-title">Supporting Evidence</div>',
                unsafe_allow_html=True,
            )

            st.markdown(
                """
                <div class="section-description">
                    These are the validated project indicators supplied to
                    the language model for this answer.
                </div>
                """,
                unsafe_allow_html=True,
            )

            st.dataframe(
                assistant_result.get(
                    "evidence_rows",
                    [],
                ),
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Indicator": st.column_config.TextColumn(
                        "Indicator",
                        width="large",
                    ),
                    "Value": st.column_config.TextColumn(
                        "Value",
                        width="medium",
                    ),
                    "Reference Period": st.column_config.TextColumn(
                        "Reference Period",
                        width="medium",
                    ),
                },
            )

        elif result_status == "out_of_scope":
            st.info(
                assistant_result.get(
                    "message",
                    "This question is outside the assistant's scope.",
                )
            )

        elif result_status == "related_unsupported":
            st.warning(
                assistant_result.get(
                    "message",
                    "This economic topic is not currently supported "
                    "by the platform's validated datasets.",
                )
            )

        elif result_status == "empty":
            st.warning(
                assistant_result.get(
                    "message",
                    "Enter a question first.",
                )
            )

        else:
            st.error(
                assistant_result.get(
                    "message",
                    "The assistant could not complete the request.",
                )
            )

    st.write("")
    st.divider()

    # ---------------------------------------------------------
    # VALIDATED DATA EXPLORER
    # ---------------------------------------------------------

    st.markdown(
        '<div class="section-title">Validated Data Explorer</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="section-description">
            Inspect the controlled retrieval layer directly. This technical
            view retrieves current project evidence from Azure Function
            endpoints rather than connecting Streamlit directly to Azure SQL.
        </div>
        """,
        unsafe_allow_html=True,
    )

    domain = st.selectbox(
        "Analytical domain",
        [
            "National Economy",
            "Regional Analysis",
            "Industry GDP",
            "Industry Retail Sales",
            "Industry Productivity",
            "Affordability & Housing",
            "Federal Fiscal",
            "Ontario Fiscal",
            "Retail Sales Forecast",
        ],
    )

    route = None
    params = {}

    if domain == "National Economy":
        route = "national/latest"

    elif domain == "Regional Analysis":
        geo = st.text_input(
            "Province or territory",
            value="Ontario",
        )
        route = "regional"
        params = {"geo": geo.strip()}

    elif domain == "Industry GDP":
        industry = st.text_input(
            "Industry",
            placeholder=(
                "Enter the exact industry name used in the reporting table"
            ),
        )
        route = "industry/gdp"
        params = {"industry": industry.strip()}

    elif domain == "Industry Retail Sales":
        industry = st.text_input(
            "Industry",
            placeholder=(
                "Enter the exact industry name used in the reporting table"
            ),
        )
        route = "industry/retail"
        params = {"industry": industry.strip()}

    elif domain == "Industry Productivity":
        industry = st.text_input(
            "Industry",
            placeholder=(
                "Enter the exact industry name used in the reporting table"
            ),
        )
        route = "industry/productivity"
        params = {"industry": industry.strip()}

    elif domain == "Affordability & Housing":
        route = "affordability/latest"

    elif domain == "Federal Fiscal":
        route = "fiscal/federal/latest"

    elif domain == "Ontario Fiscal":
        route = "fiscal/ontario/latest"

    elif domain == "Retail Sales Forecast":
        route = "forecast/latest"

    retrieve_button = st.button(
        "Retrieve Validated Evidence",
    )

    if retrieve_button:

        missing_parameter = (
            domain == "Regional Analysis"
            and not params.get("geo")
        ) or (
            domain in {
                "Industry GDP",
                "Industry Retail Sales",
                "Industry Productivity",
            }
            and not params.get("industry")
        )

        if missing_parameter:
            st.warning(
                "Enter the required geography or industry first."
            )

        else:
            try:
                with st.spinner(
                    "Retrieving validated project data..."
                ):
                    result = call_economic_api(
                        route,
                        params=params,
                    )

                if result.get("status") == "not_found":
                    st.warning(
                        result.get(
                            "message",
                            "No matching project data was found.",
                        )
                    )

                elif result.get("status") != "ok":
                    st.error(
                        result.get(
                            "message",
                            "The Economic Intelligence API returned an error.",
                        )
                    )

                else:
                    st.success(
                        "Validated evidence retrieved successfully."
                    )

                    evidence_rows = build_evidence_rows(
                        result
                    )

                    if evidence_rows:
                        st.dataframe(
                            evidence_rows,
                            use_container_width=True,
                            hide_index=True,
                        )

                    with st.expander(
                        "View API response"
                    ):
                        st.json(result)

            except requests.RequestException as e:
                st.error(
                    "Unable to reach the Economic Intelligence API: "
                    f"{e}"
                )

            except Exception as e:
                st.error(
                    "Unable to retrieve economic evidence: "
                    f"{e}"
                )

    st.write("")
    st.caption(
        "AI-generated responses summarize curated project data and should "
        "not be interpreted as official forecasts, investment advice, or "
        "policy advice."
    )
