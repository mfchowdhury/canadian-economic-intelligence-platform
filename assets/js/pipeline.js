/* =========================================================
   PAGE: Pipeline
   Interactive pipeline stage selector
   ========================================================= */

const pipelineStages = document.querySelectorAll(".pipeline-stage");

const pipelineDetailTitle = document.getElementById(
    "pipeline-detail-title"
);

const pipelineDetailDescription = document.getElementById(
    "pipeline-detail-description"
);

const pipelineDetailTechnology = document.getElementById(
    "pipeline-detail-technology"
);

const pipelineDetailInput = document.getElementById(
    "pipeline-detail-input"
);

const pipelineDetailOutput = document.getElementById(
    "pipeline-detail-output"
);

const pipelineDetailControl = document.getElementById(
    "pipeline-detail-control"
);


/* =========================================================
   PAGE: Pipeline
   Pipeline stage content
   ========================================================= */

const pipelineDetails = {
    source: {
        title: "Official Data Sources",
        description:
            "The pipeline begins with official Canadian economic datasets published by Statistics Canada, the Bank of Canada, Finance Canada, and CMHC.",
        technology: "Public APIs and official datasets",
        input: "Published economic indicators",
        output: "Source-ready ingestion payloads",
        control: "Source availability and schema checks"
    },

    ingest: {
        title: "Automated Data Ingestion",
        description:
            "Azure Data Factory coordinates representative ingestion workflows and moves source data into the Bronze layer while preserving source-aligned structure.",
        technology: "Azure Data Factory",
        input: "API responses, files, and official source data",
        output: "Bronze raw datasets in ADLS Gen2",
        control: "Activity status, dependency checks, and failure handling"
    },

    bronze: {
        title: "Bronze Raw Data Layer",
        description:
            "The Bronze layer preserves immutable source history so datasets can be traced, audited, and reprocessed without reacquiring historical source data.",
        technology: "Azure Data Lake Storage Gen2",
        input: "Raw ingested source data",
        output: "Source-aligned historical datasets",
        control: "File availability, path structure, and ingestion completeness"
    },

    silver: {
        title: "Silver Validation Layer",
        description:
            "Azure Databricks transforms raw source data into canonical datasets by applying validation, cleaning, filtering, standardization, and data-quality rules.",
        technology: "Azure Databricks and PySpark",
        input: "Bronze raw datasets",
        output: "Validated canonical datasets",
        control: "Schema validation, business rules, and quarantine handling"
    },

    gold: {
        title: "Gold Analytical Layer",
        description:
            "Validated datasets are aligned by economic purpose and transformed into reusable analytical models for economic overview, regional analysis, industry analysis, affordability, and forecasting.",
        technology: "Azure Databricks",
        input: "Validated Silver datasets",
        output: "Integrated analytical and forecasting datasets",
        control: "Grain alignment, business keys, frequency alignment, and derived metrics"
    },

    serve: {
        title: "Reporting and Analytical Delivery",
        description:
            "Gold outputs are served through Azure SQL and consumed by Power BI, forecasting workflows, and interactive applications for decision-focused analysis.",
        technology: "Azure SQL, Power BI, Streamlit, Python",
        input: "Gold analytical outputs",
        output: "Reporting tables, dashboards, forecasts, and interactive analysis",
        control: "Curated table structure and downstream consumption readiness"
    }
};


/* =========================================================
   PAGE: Pipeline
   Update selected pipeline stage
   ========================================================= */

function updatePipelineStage(stageButton) {
    const stageKey = stageButton.dataset.pipelineStage;
    const stageDetails = pipelineDetails[stageKey];

    if (!stageDetails) {
        return;
    }

    pipelineStages.forEach((stage) => {
        stage.classList.remove("active");
        stage.setAttribute("aria-pressed", "false");
    });

    stageButton.classList.add("active");
    stageButton.setAttribute("aria-pressed", "true");

    pipelineDetailTitle.textContent = stageDetails.title;

    pipelineDetailDescription.textContent =
        stageDetails.description;

    pipelineDetailTechnology.textContent =
        stageDetails.technology;

    pipelineDetailInput.textContent =
        stageDetails.input;

    pipelineDetailOutput.textContent =
        stageDetails.output;

    pipelineDetailControl.textContent =
        stageDetails.control;
}


/* =========================================================
   PAGE: Pipeline
   Stage interaction events
   ========================================================= */

pipelineStages.forEach((stageButton) => {
    stageButton.addEventListener("click", () => {
        updatePipelineStage(stageButton);
    });
});