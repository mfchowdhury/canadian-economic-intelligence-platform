# Canadian Economic Intelligence Platform

An end-to-end Azure data and analytics platform that transforms official
Canadian economic data into validated analytical marts, forecasts,
interactive dashboards, and grounded economic intelligence.

The project combines **data engineering, economic analysis, forecasting,
business intelligence, cloud automation, and application development**
in one portfolio platform.

## Live Project

-   **Portfolio Website:**
    https://mfchowdhury.github.io/canadian-economic-intelligence-platform/
-   **Interactive Application:**
    https://canadian-economic-intelligence-platform.streamlit.app/
-   **GitHub Repository:**
    https://github.com/mfchowdhury/canadian-economic-intelligence-platform
-   **Power BI Dashboard:** available through the Power BI page of the
    portfolio website

## Featured Result

  Result                                          Value
  ------------------------------------- ---------------
  Retail Sales Forecast --- July 2026      **\$73.77B**
  Selected Model                             **SARIMA**
  Holdout RMSE                            **≈ \$0.93B**
  Forecast Horizon                          **1 month**

The forecasting case study compares four candidate approaches under a
common time-series validation framework, selects the best-performing
model using validation results, evaluates it on a separate 12-month
holdout period, and produces a final one-month-ahead forecast.

## Project Objective

Canadian economic indicators are published across multiple official
sources, frequencies, structures, and release schedules. This makes
integrated analysis more difficult than working with a single prepared
dataset.

The platform brings these sources into a common analytical environment
to support:

-   integrated economic monitoring;
-   regional comparison;
-   industry analysis;
-   fiscal and affordability analysis;
-   retail sales forecasting;
-   interactive business intelligence;
-   cloud pipeline monitoring; and
-   grounded natural-language economic intelligence.

## Platform at a Glance

  Component                                                 Scope
  -------------------------------------- ------------------------
  Economic datasets                                            10
  Gold analytical marts                                         5
  Azure SQL reporting tables                                   12
  Power BI report pages                                         5
  Forecast models evaluated                                     4
  Automated ingestion proof of concept     Retail Sales + USD/CAD

The platform uses official data published by **Statistics Canada, the
Bank of Canada, and Finance Canada**. Housing-start data supplied by
**CMHC** is accessed through Statistics Canada.

## Architecture

``` text
Official Canadian Economic Data
              |
              v
      Azure Data Factory
              |
              v
       ADLS Gen2 — Bronze
              |
              v
 Azure Databricks — Silver
              |
              v
   Gold Analytical Marts
              |
              v
    Azure SQL Reporting Layer
        |               |
        |               +--------------------> Power BI
        |
        +--------------------> Streamlit Application
                                  |
                                  |-- Automation Demo
                                  |-- Validated Data Explorer
                                  |
                                  +-- Economic Intelligence Assistant
                                           |
                                           v
                                    Azure Function API
                                           |
                                           v
                                    Validated SQL Evidence
                                           |
                                           v
                                       OpenAI API
                                           |
                                           v
                                  Grounded Economic Answer
```

The architecture separates ingestion, transformation, analytical
modeling, serving, and presentation. Power BI consumes curated reporting
outputs, while the Streamlit application provides automation monitoring,
validated-data exploration, and a grounded economic assistant. For
assistant questions, controlled Azure Function endpoints retrieve
validated SQL evidence before the OpenAI model generates the response.

## Data Sources

The project integrates ten economic datasets covering consumer demand,
prices, labour markets, production, population, monetary conditions,
housing, fiscal conditions, and productivity.

  -----------------------------------------------------------------------
  \#                Dataset           Source            Frequency / Use
  ----------------- ----------------- ----------------- -----------------
  1                 Retail Sales      Statistics Canada Monthly

  2                 Consumer Price    Statistics Canada Monthly
                    Index                               

  3                 Labour Force      Statistics Canada Monthly
                    Survey                              

  4                 GDP by Industry   Statistics Canada Monthly

  5                 Population        Statistics Canada Quarterly
                    Estimates                           

  6                 USD/CAD Exchange  Bank of Canada    Daily → monthly
                    Rate                                average

  7                 Policy Interest   Bank of Canada    Daily → month-end
                    Rate                                

  8                 Government Fiscal Finance Canada /  Annual
                    Data              Statistics Canada 

  9                 Housing Starts    Statistics        Monthly SAAR
                                      Canada, using     
                                      CMHC-supplied     
                                      data              

  10                Labour            Statistics Canada Analytical
                    Productivity                        industry measure
  -----------------------------------------------------------------------

Examples of Statistics Canada tables used in the project include retail
sales **20-10-0056-01**, CPI **18-10-0004-01**, labour force
**14-10-0287-03**, GDP by industry **36-10-0434-01**, population
**17-10-0009-01**, and housing starts **34-10-0158-01**.

## Bronze, Silver, and Gold Design

### Bronze --- Raw History

Bronze preserves source-aligned data after ingestion. The goal is to
retain the original source structure and provide a reproducible starting
point for downstream processing.

### Silver --- Validated Canonical Data

Azure Databricks notebooks clean and standardize the ingested datasets.
Processing includes schema checks, required-field validation,
source-specific transformations, and preparation of consistent
analytical structures.

### Gold --- Analytical Marts

Validated Silver datasets are aligned according to analytical purpose
and transformed into five reusable Gold analytical marts:

1.  **Economic Overview** --- integrated monthly national indicators
2.  **Regional Analysis** --- provincial and national comparisons
3.  **Industry Analysis** --- industry GDP, retail performance, and
    productivity
4.  **Fiscal & Affordability** --- inflation, housing, and fiscal
    indicators
5.  **Forecasting Features** --- model-ready retail-sales forecasting
    features

Frequency alignment is handled according to analytical purpose. Examples
include monthly averages for USD/CAD, month-end policy rates, monthly
SAAR housing starts, and carry-forward alignment for quarterly
population estimates.

## Automated Pipeline

Azure Data Factory provides the orchestration layer.

To keep the portfolio implementation cost-conscious, the automated proof
of concept demonstrates representative end-to-end ingestion for:

-   **Statistics Canada Retail Sales**
-   **Bank of Canada USD/CAD**

The implemented **master pipeline** coordinates two ingestion workflows,
Silver validation, Gold analytical processing, dependency-based
execution, and explicit failure paths. Downstream processing begins only
after required upstream activities succeed.

The Streamlit Automation Demo exposes the latest master pipeline run,
activity execution, and controlled pipeline triggering. A global 24-hour
cooldown limits public triggering while the latest execution result
remains visible to visitors.

## Azure SQL Reporting Layer

Gold analytical outputs are served through Azure SQL to separate
analytical processing from downstream consumption.

The reporting layer contains 12 curated tables:

``` text
economic_overview
regional_analysis_monthly
industry_gdp_monthly
industry_retail_monthly
industry_productivity_annual
affordability_monthly
federal_fiscal_annual
ontario_fiscal_annual
retail_model_metrics
retail_holdout_predictions
retail_model_metadata
retail_forecasts
```

These tables provide stable inputs for Power BI, the Azure Function API,
and the interactive application.

## Analytical Outputs

The analytical layer organizes the source datasets into five practical
areas.

### Integrated Economic Conditions

Combines retail sales, inflation, unemployment, GDP growth, USD/CAD, and
the policy rate to monitor Canadian economic conditions.

### Regional Comparison

Compares retail growth, inflation, unemployment, and employment across
provinces and against national conditions.

### Industry Performance

Analyzes GDP growth, retail performance, and labour productivity across
industries and over time.

### Fiscal & Affordability

Connects inflation, housing, and fiscal indicators to examine
affordability alongside federal and provincial fiscal trends.

### Retail Sales Forecasting

Evaluates alternative forecasting approaches and produces a
one-month-ahead Canadian retail sales forecast.

## Forecasting Methodology

The candidate set compares a **seasonal benchmark, exponential
smoothing, a feature-based regression model, and a seasonal
autoregressive model** to evaluate different forecasting approaches on
the same retail-sales series.

Four forecasting approaches were evaluated using **expanding-window
one-step-ahead validation**.

  Model                Validation RMSE
  ------------------ -----------------
  **SARIMA**              **\$0.973B**
  Ridge Regression            \$1.392B
  ETS                         \$1.974B
  Seasonal Naive              \$2.549B

SARIMA achieved the lowest validation RMSE under the common evaluation
framework and was selected for final evaluation.

The selected model was then tested on a separate **12-month holdout
period**:

-   **Holdout MAE:** approximately **\$0.80B**
-   **Holdout RMSE:** approximately **\$0.93B**

The final model produced a one-month-ahead Canadian retail sales
forecast of:

> **\$73.77B for July 2026**

The forecasting workflow keeps model selection and final holdout
evaluation separate to reduce the risk of selecting a model based on
final-test performance.

## Power BI Dashboard

The Power BI report contains five pages aligned with the analytical
marts:

1.  **Overview**
2.  **Regional**
3.  **Industry**
4.  **Fiscal & Affordability**
5.  **Forecasting & Outlook**

The dashboard communicates validated analytical outputs rather than
recreating transformation logic inside the reporting layer.

Examples include national economic indicators, provincial labour-market
comparisons, industry performance, Canada/Ontario affordability
indicators, fiscal trends, model-performance comparison, holdout
predictions, and the final retail-sales forecast.

## Interactive Application

The Streamlit application provides three user-facing capabilities:

### Automation Demo

Visitors can inspect:

-   the latest Azure Data Factory master pipeline run;
-   individual activity execution;
-   pipeline status and duration; and
-   controlled public pipeline triggering.

### Economic Intelligence Assistant

The assistant answers supported economic questions using validated
project data rather than allowing unrestricted model-generated data
retrieval.

The assistant workflow is:

``` text
User Question
     |
     v
Scope and Domain Routing
     |
     v
Azure Function API
     |
     v
Azure SQL Reporting Tables
     |
     v
Validated Evidence Builder
     |
     v
OpenAI Model
     |
     v
Grounded Economic Answer
     |
     v
Supporting Evidence
```

The application distinguishes between supported questions, economically
relevant questions for which the required data is unavailable, and
out-of-scope requests.

The language model does **not** receive arbitrary SQL access. Controlled
API endpoints retrieve validated reporting data first, and the model
receives the user's question together with that evidence.

### Validated Data Explorer

The Validated Data Explorer provides direct access to curated project
outputs so users can inspect the underlying validated data used by the
analytical and application layers.

This separates direct data inspection from natural-language
interpretation and provides an additional way to verify the information
exposed through the platform.

## API Layer

Azure Functions provides controlled access between the Streamlit
application and Azure SQL.

Implemented routes include:

``` text
health
health/sql
national/latest
regional
industry/gdp
industry/retail
industry/productivity
affordability/latest
fiscal/federal/latest
fiscal/ontario/latest
forecast/latest
```

This design keeps database access and retrieval logic behind a defined
application interface.

## Technology Stack

  Area                    Technologies
  ----------------------- -----------------------------------------------
  Orchestration           Azure Data Factory
  Data Lake               Azure Data Lake Storage Gen2
  Processing              Azure Databricks, PySpark
  Storage Format          Delta Lake
  Serving                 Azure SQL Database
  API                     Azure Functions
  Analytics               Python, SQL
  Forecasting             SARIMA, ETS, Ridge Regression, Seasonal Naive
  Business Intelligence   Power BI
  Application             Streamlit
  Grounded Assistant      OpenAI API
  Front End / Portfolio   HTML, CSS, JavaScript
  Version Control         Git, GitHub

## Engineering and Security Design

The project applies several implemented controls to make the public
portfolio more reliable and to limit unnecessary access:

-   Azure Managed Identity is used where supported for
    service-to-service authentication.
-   Application secrets are stored outside source code.
-   Database retrieval is exposed through defined Azure Function
    endpoints rather than direct arbitrary SQL access by the language
    model.
-   Economic answers are grounded in validated reporting evidence
    retrieved before model generation.
-   Application routing distinguishes supported economic questions,
    unsupported-data questions, and out-of-scope requests.
-   Tested prompt-handling logic limits attempts to redirect the
    assistant away from its defined economic-intelligence role.
-   ADF execution uses explicit dependencies and failure paths.
-   Public automation triggering is restricted through a shared
    cooldown.

These controls are part of the portfolio implementation and are not
presented as a comprehensive security assessment or certification.

No credentials, API keys, Function keys, connection secrets, or local
configuration values should be committed to this repository.

## Key Design Decisions

### Representative Automation Instead of Automating Every Source

The analytical platform integrates ten datasets, but the public ADF
proof of concept automates Retail Sales and USD/CAD end to end. This
demonstrates orchestration, dependency management, validation, and Gold
processing while controlling cloud cost.

### Separate Processing From Reporting

Transformations are completed upstream in Databricks and Gold analytical
marts. Azure SQL and Power BI consume curated outputs rather than
reproducing core transformation logic.

### Preserve Economic Meaning During Frequency Alignment

Datasets are not forced into a common frequency without considering
their economic interpretation. Monthly averages, month-end values, SAAR
measures, and carry-forward alignment are applied according to the
indicator.

### Separate Model Selection From Holdout Evaluation

Candidate models are compared using time-series validation. The selected
model is subsequently evaluated on a separate holdout period.

### Ground the Assistant in Validated Data

The assistant retrieves project evidence through controlled API
endpoints before generating an answer. This keeps the language-model
layer separate from direct database access.

## Project Scope and Limitations

This is a portfolio implementation rather than a production government
forecasting system.

Important scope boundaries include:

-   two representative ingestion workflows are automated end to end;
-   remaining datasets are prepared for the analytical platform without
    claiming equivalent ADF automation;
-   forecasting results are demonstrations based on the available
    historical sample and selected methodology;
-   the assistant answers only within the validated data exposed by the
    application;
-   public automation is intentionally restricted to control cloud cost
    and misuse; and
-   the platform is designed for analytical demonstration and economic
    intelligence, not automated policy decisions.

## Author

**Mahin Chowdhury**

This project was designed and implemented across cloud architecture,
data engineering, analytical modeling, forecasting, business
intelligence, API integration, interactive application development, and
portfolio presentation.

-   **GitHub:** https://github.com/mfchowdhury
-   **Portfolio:**
    https://mfchowdhury.github.io/canadian-economic-intelligence-platform/
