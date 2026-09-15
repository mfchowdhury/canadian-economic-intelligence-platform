import struct

import mssql_python
import requests
import streamlit as st
from azure.identity import ClientSecretCredential
from datetime import datetime

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
# AZURE SQL CONNECTION
# =========================================================

SQL_COPT_SS_ACCESS_TOKEN = 1256


def get_sql_config():
    """Return the Azure SQL configuration from Streamlit Secrets."""

    return {
        "server": st.secrets["sql"]["server"],
        "database": st.secrets["sql"]["database"],
    }


def get_sql_connection():
    """Create an Azure SQL connection using Microsoft Entra authentication."""

    config = get_sql_config()
    credential = get_azure_credential()

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
        f"Server={config['server']};"
        f"Database={config['database']};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
    )

    return mssql_python.connect(
        connection_string,
        attrs_before={
            SQL_COPT_SS_ACCESS_TOKEN: token_struct
        },
    )



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
    """Convert an ADF timestamp to a readable display value."""

    if not timestamp:
        return "—"

    try:
        parsed_time = datetime.fromisoformat(
            timestamp.replace("Z", "+00:00")
        )

        # Convert UTC to the environment's local display timezone.
        local_time = parsed_time.astimezone()

        return local_time.strftime("%b %d, %Y, %I:%M %p")

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
            Azure Data Factory for the currently tracked execution.
        </div>
        """,
        unsafe_allow_html=True,
    )

    pipeline_status = "Ready"
    run_started = "—"

    # Retrieve the latest state for the ADF run tracked by
    # the current Streamlit browser session.
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

    button_col1, button_col2 = st.columns(2)

    with button_col1:

        # Session-level protection prevents another trigger from the
        # same browser while the currently tracked run remains active.
        if st.button(
            "Run Automation Demo",
            type="primary",
            disabled=run_is_active,
            use_container_width=True,
        ):
            try:
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

        # Refresh both the master pipeline and its activity-level
        # execution details without starting another ADF run.
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
        "Public-run frequency controls will limit how often the live "
        "Azure workflow can be triggered."
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
            Ask questions about curated Canadian economic data and receive
            plain-language analytical explanations grounded in the
            platform's validated datasets.
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        '<div class="section-title">Ask the Assistant</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="section-description">
            The assistant retrieves relevant validated indicators from
            the analytical serving layer before generating an explanation.
        </div>
        """,
        unsafe_allow_html=True,
    )

    question = st.text_area(
        "Economic question",
        placeholder=(
            "Example: How are Canadian economic conditions changing?"
        ),
        height=120,
    )

    ask_button = st.button(
        "Ask",
        type="primary",
    )

    if ask_button:

        if not question.strip():
            st.warning(
                "Enter an economic question first."
            )

        else:
            st.info(
                "The Economic Intelligence Assistant is not connected yet. "
                "Azure SQL retrieval and the LLM connection will be added next."
            )

    st.write("")
    st.divider()

    st.markdown(
        '<div class="section-title">Response Evidence</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="section-description">
            Each generated answer will show the indicators, values, and
            reference periods used so the analysis remains traceable
            to project data.
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        | Indicator | Value | Reference Period |
        |---|---:|---|
        | Inflation | — | — |
        | Unemployment | — | — |
        | Retail Sales | — | — |
        """
    )

    st.caption(
        "AI-generated responses provide analytical summaries of curated "
        "project data and should not be interpreted as official forecasts "
        "or policy advice."
    )