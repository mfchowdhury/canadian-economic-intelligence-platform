/* =========================================================
   Architecture interaction
   ========================================================= */

const architectureStages = document.querySelectorAll(".architecture-stage");

const detailTitle = document.getElementById("architecture-detail-title");
const detailDescription = document.getElementById(
    "architecture-detail-description"
);
const detailTechnology = document.getElementById(
    "architecture-detail-technology"
);
const detailInput = document.getElementById("architecture-detail-input");
const detailOutput = document.getElementById("architecture-detail-output");
const detailRole = document.getElementById("architecture-detail-role");


const architectureDetails = {
    sources: {
        title: "Official Data Sources",
        description:
            "The platform begins with public Canadian economic data published by Statistics Canada, the Bank of Canada, Finance Canada, and CMHC.",
        technology: "Public APIs and official datasets",
        input: "Published economic indicators",
        output: "Source-aligned raw datasets",
        role: "Authoritative economic data"
    },

    ingestion: {
        title: "Automated Data Ingestion",
        description:
            "Azure Data Factory orchestrates representative ingestion pipelines, schedules recurring execution, manages dependencies, and supports operational monitoring.",
        technology: "Azure Data Factory",
        input: "Official source data and API responses",
        output: "Bronze-layer raw datasets",
        role: "Orchestration and ingestion"
    },

    lakehouse: {
        title: "Lakehouse Processing",
        description:
            "ADLS Gen2 and Azure Databricks implement the Bronze, Silver, and Gold architecture used to preserve raw history, validate canonical datasets, and create reusable analytical models.",
        technology: "ADLS Gen2 and Azure Databricks",
        input: "Bronze raw datasets",
        output: "Validated Silver and analytical Gold datasets",
        role: "Transformation, validation, and integration"
    },

    serving: {
        title: "Analytical Serving Layer",
        description:
            "Azure SQL provides a stable reporting layer between analytical processing and downstream applications. Curated tables are designed for efficient reporting and reusable consumption.",
        technology: "Azure SQL Database",
        input: "Gold analytical outputs",
        output: "12 curated reporting tables",
        role: "Reporting and analytical serving"
    },

    delivery: {
        title: "Decision and Application Layer",
        description:
            "Curated analytical outputs are delivered through Power BI, Streamlit, and forecasting workflows so economic information can be explored and communicated through multiple interfaces.",
        technology: "Power BI, Streamlit, Python",
        input: "Curated analytical and forecasting data",
        output: "Dashboards, forecasts, and interactive analysis",
        role: "Decision-focused delivery"
    }
};


/* ---------- Update selected architecture stage ---------- */

function updateArchitectureStage(stageButton) {
    const stageKey = stageButton.dataset.stage;
    const stageDetails = architectureDetails[stageKey];

    if (!stageDetails) {
        return;
    }

    architectureStages.forEach((stage) => {
        stage.classList.remove("active");
        stage.setAttribute("aria-pressed", "false");
    });

    stageButton.classList.add("active");
    stageButton.setAttribute("aria-pressed", "true");

    detailTitle.textContent = stageDetails.title;
    detailDescription.textContent = stageDetails.description;
    detailTechnology.textContent = stageDetails.technology;
    detailInput.textContent = stageDetails.input;
    detailOutput.textContent = stageDetails.output;
    detailRole.textContent = stageDetails.role;
}


/* ---------- Architecture stage events ---------- */

architectureStages.forEach((stageButton) => {
    stageButton.addEventListener("click", () => {
        updateArchitectureStage(stageButton);
    });
});