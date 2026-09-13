/* =========================================================
   PAGE: Pipeline
   SECTION: Interactive pipeline stage selector
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
   SECTION: Pipeline stage content
   ========================================================= */

const pipelineDetails = {

    source: {
        title: "Official Data Sources",

        description:
            "The platform integrates official Canadian economic data published by Statistics Canada, the Bank of Canada, Finance Canada, and Ontario Finance. Housing starts are sourced through Statistics Canada using data supplied by CMHC.",

        technology:
            "Official APIs, downloadable datasets, and public data tables",

        input:
            "Published Canadian economic and fiscal indicators",

        output:
            "Source data prepared for ingestion",

        control:
            "Source availability, expected structure, and dataset metadata"
    },


    ingest: {
        title: "Automated Data Ingestion",

        description:
            "Azure Data Factory orchestrates representative automated ingestion workflows for Statistics Canada Retail Sales and Bank of Canada USD/CAD data. The broader platform contains ten official economic datasets.",

        technology:
            "Azure Data Factory",

        input:
            "Official API responses and downloadable source files",

        output:
            "Immutable raw data stored in the Bronze layer",

        control:
            "Activity dependencies, run status, failure paths, and monitoring"
    },


    bronze: {
        title: "Bronze Raw Data Layer",

        description:
            "The Bronze layer preserves source-aligned raw history before analytical transformation. This supports traceability, reproducibility, auditing, and reprocessing without unnecessarily reacquiring historical source data.",

        technology:
            "Azure Data Lake Storage Gen2",

        input:
            "Raw ingested source data",

        output:
            "Immutable source-aligned historical datasets",

        control:
            "File availability, storage paths, ingestion completeness, and source traceability"
    },


    silver: {
        title: "Silver Validation Layer",

        description:
            "Azure Databricks converts raw source data into validated canonical datasets by applying schema checks, cleaning, standardization, business rules, and dataset-specific transformations. Invalid records are directed to quarantine rather than silently removed.",

        technology:
            "Azure Databricks · PySpark · Delta Lake",

        input:
            "Bronze raw datasets",

        output:
            "Validated canonical Silver datasets",

        control:
            "Schema validation, business keys, duplicate checks, data-quality rules, and quarantine handling"
    },


    gold: {
        title: "Gold Analytical Layer",

        description:
            "Validated Silver datasets are aligned according to analytical purpose and transformed into five reusable Gold models covering economic overview, regional analysis, industry analysis, fiscal and affordability analysis, and forecasting features.",

        technology:
            "Azure Databricks · PySpark · Delta Lake",

        input:
            "Validated Silver datasets",

        output:
            "Integrated economic indicators, derived metrics, analytical marts, and forecasting features",

        control:
            "Business keys, analytical grain, frequency alignment, source-date tracking, and derived-metric validation"
    },


    serve: {
        title: "Serving and Analytical Delivery",

        description:
            "Curated Gold outputs are published to Azure SQL for downstream consumption. Power BI provides five-page economic reporting, while the Streamlit application provides a live automation demonstration and an interactive economic intelligence interface.",

        technology:
            "Azure SQL · Power BI · Streamlit",

        input:
            "Validated Gold analytical outputs",

        output:
            "SQL reporting tables, Power BI dashboards, forecasts, and interactive applications",

        control:
            "Curated reporting structures, downstream readiness, and separation of analytical processing from presentation"
    }
};


/* =========================================================
   PAGE: Pipeline
   SECTION: Update selected stage
   ========================================================= */

function updatePipelineStage(stageButton) {

    const stageKey = stageButton.dataset.pipelineStage;
    const stageDetails = pipelineDetails[stageKey];

    // Ignore a stage if no matching detail configuration exists.
    if (!stageDetails) {
        return;
    }

    // Reset the visual and accessibility state of all stage buttons.
    pipelineStages.forEach((stage) => {
        stage.classList.remove("active");
        stage.setAttribute("aria-pressed", "false");
    });

    // Mark the selected stage as active.
    stageButton.classList.add("active");
    stageButton.setAttribute("aria-pressed", "true");

    // Populate the detail panel with the selected stage information.
    pipelineDetailTitle.textContent =
        stageDetails.title;

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
   SECTION: Stage interaction events
   ========================================================= */

pipelineStages.forEach((stageButton) => {

    stageButton.addEventListener("click", () => {
        updatePipelineStage(stageButton);
    });

});