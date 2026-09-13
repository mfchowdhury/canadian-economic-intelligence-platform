import requests
import streamlit as st
from azure.identity import ClientSecretCredential
from datetime import datetime, timezone


# =========================================================
# PAGE CONFIGURATION
# =========================================================

st.set_page_config(
    page_title="Canadian Economic Intelligence Platform",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
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
    """Verify that the Streamlit service principal can access the Data Factory."""

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
# DATE AND STATUS FORMATTING
# =========================================================

def format_adf_datetime(timestamp):
    """Convert an ADF UTC timestamp to a readable local display value."""

    if not timestamp:
        return "—"

    try:
        parsed_time = datetime.fromisoformat(
            timestamp.replace("Z", "+00:00")
        )

        # Convert UTC to the user's project display timezone.
        local_time = parsed_time.astimezone()

        return local_time.strftime("%b %d, %Y, %I:%M %p")

    except (ValueError, TypeError):
        return timestamp


def format_pipeline_status(status):
    """Convert the ADF status value into a user-friendly label."""

    status_labels = {
        "Queued": "Starting",
        "InProgress": "In Progress",
        "Succeeded": "Succeeded",
        "Failed": "Failed",
        "Cancelled": "Cancelled",
        "Canceling": "Cancelling",
    }

    return status_labels.get(status, status or "Ready")


# =========================================================
# STREAMLIT SESSION STATE
# =========================================================

if "adf_run_id" not in st.session_state:
    st.session_state.adf_run_id = None

if "adf_run_data" not in st.session_state:
    st.session_state.adf_run_data = None

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

        .info-card {
            border: 1px solid #D9E1E8;
            border-radius: 12px;
            padding: 1.15rem 1.25rem;
            background-color: #FFFFFF;
            height: 100%;
        }

        .info-label {
            color: #607D8B;
            font-size: 0.85rem;
            margin-bottom: 0.3rem;
        }

        .info-value {
            color: #16324F;
            font-size: 1.15rem;
            font-weight: 700;
        }

        .note-text {
            color: #607D8B;
            font-size: 0.9rem;
            line-height: 1.5;
        }

        .status-success {
            color: #2E7D32;
            font-weight: 700;
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
    st.markdown("## Canadian Economic Intelligence")
    st.caption("Interactive project demonstration")

    page = st.radio(
        "Navigation",
        [
            "Overview",
            "Automation Demo",
            "Economic Intelligence Assistant",
        ],
        label_visibility="collapsed",
    )

    st.divider()

    st.caption(
        "Built with Azure Data Factory, ADLS Gen2, Azure Databricks, "
        "Azure SQL, Power BI, Python, and Streamlit."
    )


# =========================================================
# PAGE: OVERVIEW
# =========================================================

if page == "Overview":

    st.markdown(
        '<div class="main-title">Canadian Economic Intelligence Platform</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="main-subtitle">
            An interactive companion to an end-to-end Canadian economic
            intelligence platform, demonstrating automated economic-data
            processing and AI-assisted economic analysis.
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        '<div class="section-title">Platform Overview</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="section-description">
            The platform integrates official Canadian economic data,
            transforms it through a medallion architecture, and produces
            analysis-ready datasets for reporting, forecasting, and
            decision-oriented economic analysis.
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="pipeline-box">
            Official Data Sources
            &nbsp; → &nbsp;
            Azure Data Factory
            &nbsp; → &nbsp;
            ADLS Gen2
            &nbsp; → &nbsp;
            Azure Databricks
            &nbsp; → &nbsp;
            Gold
            &nbsp; → &nbsp;
            Azure SQL
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.write("")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown(
            """
            <div class="info-card">
                <div class="info-label">Data Engineering</div>
                <div class="info-value">Azure Lakehouse Pipeline</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with col2:
        st.markdown(
            """
            <div class="info-card">
                <div class="info-label">Economic Analysis</div>
                <div class="info-value">Canadian Macro Data</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with col3:
        st.markdown(
            """
            <div class="info-card">
                <div class="info-label">Advanced Analytics</div>
                <div class="info-value">Forecasting + AI</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.write("")

    st.info(
        "Use the navigation panel to explore the representative automation "
        "workflow and the Economic Intelligence Assistant."
    )


# =========================================================
# PAGE: AUTOMATION DEMO
# =========================================================

elif page == "Automation Demo":

    st.markdown(
        '<div class="main-title">Automation Demo</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="main-subtitle">
            A representative automated workflow demonstrating ingestion,
            transformation, validation, and Gold-layer integration using
            Azure Data Factory and Azure Databricks.
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
            The automation proof of concept uses representative datasets
            from Statistics Canada and the Bank of Canada.
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
            Start the live Azure Data Factory master pipeline and monitor
            its actual execution status directly from Azure.
        </div>
        """,
        unsafe_allow_html=True,
    )

    pipeline_status = "Ready"
    last_run = "—"

    # Retrieve the latest state for the run currently tracked
    # by this Streamlit browser session.
    if st.session_state.adf_run_id:

        try:
            run_data = get_adf_run_status(
                st.session_state.adf_run_id
            )

            st.session_state.adf_run_data = run_data

            pipeline_status = format_pipeline_status(
                run_data.get("status")
            )

            last_run = format_adf_datetime(
                run_data.get("runStart")
            )

        except Exception as e:
            pipeline_status = "Status Unavailable"

    col1, col2 = st.columns(2)

    with col1:
        st.metric(
            label="Pipeline Status",
            value=pipeline_status,
        )

    with col2:
        st.metric(
            label="Last Run",
            value=last_run,
        )

    st.write("")

    # Display the ADF run identifier for traceability.
    if st.session_state.adf_run_id:
        st.caption(
            f"ADF Run ID: {st.session_state.adf_run_id}"
        )

    # Display the most recent trigger confirmation.
    if st.session_state.adf_trigger_message:
        st.success(st.session_state.adf_trigger_message)
        st.session_state.adf_trigger_message = None

    st.markdown(
        """
        <div class="note-text">
            This demonstration represents the automated workflow for
            Retail Sales and USD/CAD rather than the complete dataset registry.
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.write("")
    st.divider()

    # ---------------------------------------------------------
    # AUTOMATION CONTROLS
    # ---------------------------------------------------------

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

        # Prevent another run from being started from the same
        # session while the tracked pipeline is still active.
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
                st.session_state.adf_trigger_message = (
                    "ADF pipeline started successfully."
                )

                st.rerun()

            except Exception as e:
                st.error(
                    f"Unable to start ADF pipeline: {e}"
                )

    with button_col2:

        # Refresh the current ADF run without starting
        # another pipeline execution.
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

                st.session_state.adf_run_data = run_data

                st.rerun()

            except Exception as e:
                st.error(
                    f"Unable to retrieve ADF run status: {e}"
                )

    if run_is_active:
        st.info(
            "The Azure Data Factory pipeline is currently running. "
            "Use Refresh Run Status to retrieve the latest execution state."
        )

    elif pipeline_status == "Succeeded":
        st.success(
            "The latest automation run completed successfully."
        )

    elif pipeline_status == "Failed":
        st.error(
            "The latest automation run failed. Review the Azure Data Factory "
            "monitoring details for the failed activity."
        )

    elif pipeline_status == "Cancelled":
        st.warning(
            "The latest automation run was cancelled."
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
            plain-language analytical explanations grounded in project data.
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
            The assistant will retrieve relevant validated indicators before
            generating a response.
        </div>
        """,
        unsafe_allow_html=True,
    )

    question = st.text_area(
        "Economic question",
        placeholder="Example: How are Canadian economic conditions changing?",
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
            Each generated answer will show the indicators and reference
            periods used so the analysis remains traceable to project data.
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