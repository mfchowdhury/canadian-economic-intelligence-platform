import requests
import streamlit as st
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
        "base_url": st.secrets["api"]["base_url"].rstrip("/"),
        "function_key": st.secrets["api"]["function_key"],
    }


def call_economic_api(route, params=None):
    """Call a controlled Azure Function retrieval endpoint."""

    config = get_api_config()
    url = f"{config['base_url']}/api/{route.lstrip('/')}"

    request_params = dict(params or {})
    request_params["code"] = config["function_key"]

    response = requests.get(
        url,
        params=request_params,
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

    button_col1, button_col2 = st.columns(2)

    with button_col1:

        # Prevent another trigger while the currently tracked ADF run is active.
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
        "The latest ADF run remains visible until a newer automation "
        "run is started. Displayed timestamps use Toronto (ET) time."
    )


# =========================================================
# PAGE: ECONOMIC INTELLIGENCE ASSISTANT
# =========================================================

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
            Explore validated Canadian economic indicators through the
            platform's controlled Azure Function API.
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ---------------------------------------------------------
    # TEMPORARY API CONNECTION TESTS
    # ---------------------------------------------------------

    test_col1, test_col2 = st.columns(2)

    # Test the controlled Azure Function retrieval layer.
    with test_col1:
        if st.button(
            "Test Function API",
            use_container_width=True,
        ):
            try:
                base_url = st.secrets["function_api"]["base_url"].rstrip("/")
                function_key = st.secrets["function_api"]["function_key"]

                response = requests.get(
                    f"{base_url}/api/national/latest",
                    headers={
                        "x-functions-key": function_key
                    },
                    timeout=30,
                )

                response.raise_for_status()

                st.success(
                    "Azure Function API connection succeeded."
                )
                st.json(response.json())

            except Exception as e:
                st.error(
                    f"Azure Function API connection failed: {e}"
                )

    # Test the OpenAI API connection using the private Streamlit secret.
    with test_col2:
        if st.button(
            "Test OpenAI API",
            use_container_width=True,
        ):
            try:
                client = OpenAI(
                    api_key=st.secrets["openai"]["api_key"]
                )

                response = client.responses.create(
                    model=st.secrets["openai"]["model"],
                    input=(
                        "Reply with exactly: "
                        "OpenAI API connection succeeded."
                    ),
                )

                st.success(response.output_text)

            except Exception as e:
                st.error(
                    f"OpenAI API connection failed: {e}"
                )

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
            Select an analytical domain to retrieve current project evidence.
            Streamlit calls controlled API endpoints rather than connecting
            directly to Azure SQL.
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
        type="primary",
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

                    st.markdown(
                        '<div class="section-title">'
                        'Response Evidence'
                        '</div>',
                        unsafe_allow_html=True,
                    )

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
    st.divider()

    # ---------------------------------------------------------
    # AI ASSISTANT
    # ---------------------------------------------------------

    st.markdown(
        '<div class="section-title">AI Assistant</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="section-description">
            The controlled retrieval layer is now connected. Question routing,
            evidence formatting, and grounded language-model interpretation
            will be added on top of these validated API responses.
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.caption(
        "AI-generated responses will provide analytical summaries of curated "
        "project data and should not be interpreted as official forecasts "
        "or policy advice."
    )