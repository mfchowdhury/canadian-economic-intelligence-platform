import streamlit as st
from azure.identity import ClientSecretCredential


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

def test_azure_authentication():
    credential = ClientSecretCredential(
        tenant_id=st.secrets["azure"]["tenant_id"],
        client_id=st.secrets["azure"]["client_id"],
        client_secret=st.secrets["azure"]["client_secret"],
    )

    credential.get_token("https://management.azure.com/.default")

    return True


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
            A representative automated workflow demonstrating scheduled
            ingestion, transformation, validation, and Gold-layer integration.
        </div>
        """,
        unsafe_allow_html=True,
    )

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

    st.write("")

    st.markdown(
        '<div class="section-title">Latest Automation Run</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="section-description">
            Live operational metadata will be connected to the Azure environment.
        </div>
        """,
        unsafe_allow_html=True,
    )

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            label="Pipeline Status",
            value="Not Connected",
        )

    with col2:
        st.metric(
            label="Last Run",
            value="—",
        )

    with col3:
        st.metric(
            label="Data Through",
            value="—",
        )

    st.write("")

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

    # Temporary authentication test.
    # This requests an Azure Management API token but does not run ADF.
    if st.button("Test Azure Authentication"):
        try:
            test_azure_authentication()
            st.success("Azure authentication succeeded.")
        except Exception as e:
            st.error(f"Azure authentication failed: {e}")


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

            st.warning("Enter an economic question first.")

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
