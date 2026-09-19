# Databricks notebook source
# MAGIC %md
# MAGIC # Retail Sales Forecasting
# MAGIC
# MAGIC **Notebook:** `01_retail_sales_forecasting`
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC Build, compare, and evaluate statistically appropriate models for one-month-ahead Canadian retail sales forecasting using the Gold forecasting feature mart.
# MAGIC
# MAGIC ## Forecasting Objective
# MAGIC
# MAGIC - **Target:** `target_retail_sales_dollars`
# MAGIC - **Frequency:** Monthly
# MAGIC - **Forecast horizon:** 1 month ahead
# MAGIC - **Model-ready filter:** `is_retail_model_ready = true`
# MAGIC
# MAGIC ## Candidate Models
# MAGIC
# MAGIC The candidate models are selected to match the size and structure of the monthly dataset:
# MAGIC
# MAGIC 1. **Seasonal Naive** — annual-seasonality benchmark
# MAGIC 2. **ETS** — parsimonious level, trend, and seasonal model
# MAGIC 3. **SARIMA** — autoregressive and seasonal time-series model
# MAGIC 4. **Ridge Regression** — regularized multivariate model using lagged economic predictors
# MAGIC
# MAGIC ## Validation Strategy
# MAGIC
# MAGIC Model selection uses expanding-window, one-step-ahead validation.
# MAGIC
# MAGIC - **RMSE** is the primary model-selection metric.
# MAGIC - **MAE** is retained as a complementary measure.
# MAGIC - The final 12 eligible months are reserved for final out-of-sample evaluation.
# MAGIC
# MAGIC ## Leakage Controls
# MAGIC
# MAGIC - Same-month target values are excluded from predictors.
# MAGIC - Regression predictors use only historical lagged, rolling, growth, or known calendar information.
# MAGIC - Ridge scaling and alpha selection are performed within the development period.
# MAGIC - SARIMA specification selection is performed within the development period.
# MAGIC - Final holdout performance is not used for model or hyperparameter selection.
# MAGIC
# MAGIC > **Limitation:** The feature mart applies a reference-period cutoff rather than a fully publication-vintage-aware cutoff because historical release timestamps are not available for every source.
# MAGIC
# MAGIC ## Outputs
# MAGIC
# MAGIC - Validation model-comparison metrics
# MAGIC - Final holdout predictions
# MAGIC - Selected-model metadata
# MAGIC - One-month-ahead retail sales forecast

# COMMAND ----------

# Install the statistical modeling library required for ETS and SARIMA
%pip install -q statsmodels

# COMMAND ----------

# Restart Python so the newly installed package is available
dbutils.library.restartPython()

# COMMAND ----------

# =========================================================
# CONFIGURATION AND IMPORTS
# =========================================================

import warnings
import numpy as np
import pandas as pd

from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from statsmodels.tsa.holtwinters import ExponentialSmoothing
from statsmodels.tsa.statespace.sarimax import SARIMAX
from statsmodels.tools.sm_exceptions import ConvergenceWarning

from pyspark.sql import functions as F


# Gold feature mart used for model development and forecasting
GOLD_FORECASTING_FEATURES_PATH = (
    "abfss://gold@stceidev01.dfs.core.windows.net/"
    "forecasting_features/monthly/"
)

# Gold output locations consumed downstream by SQL and Power BI
RETAIL_FORECAST_BASE_PATH = (
    "abfss://gold@stceidev01.dfs.core.windows.net/"
    "forecasting/retail_sales"
)

METRICS_PATH = f"{RETAIL_FORECAST_BASE_PATH}/model_metrics"
HOLDOUT_PATH = f"{RETAIL_FORECAST_BASE_PATH}/holdout_predictions"
METADATA_PATH = f"{RETAIL_FORECAST_BASE_PATH}/model_metadata"
FORECAST_PATH = f"{RETAIL_FORECAST_BASE_PATH}/forecasts"

TARGET_COLUMN = "target_retail_sales_dollars"
READY_COLUMN = "is_retail_model_ready"

HOLDOUT_MONTHS = 12
VALIDATION_MONTHS = 18
SEASONAL_PERIOD = 12

print("Retail sales forecasting configuration loaded.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load and Validate the Forecasting Dataset
# MAGIC
# MAGIC Load the Gold forecasting feature mart, retain retail model-ready observations, and validate the monthly business key and target coverage.

# COMMAND ----------

forecast_df = (
    spark.read
    .format("delta")
    .load(GOLD_FORECASTING_FEATURES_PATH)
)

required_columns = [
    "ref_date",
    TARGET_COLUMN,
    READY_COLUMN
]

missing_required = [
    column
    for column in required_columns
    if column not in forecast_df.columns
]

assert not missing_required, (
    f"Missing required columns: {missing_required}"
)

# Keep only observations that are eligible for retail model development
# and have an observed target value.
retail_df = (
    forecast_df
    .filter(F.col(READY_COLUMN) == True)
    .filter(F.col(TARGET_COLUMN).isNotNull())
    .orderBy("ref_date")
)

retail_count = retail_df.count()

duplicate_months = (
    retail_df
    .groupBy("ref_date")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

date_range = (
    retail_df
    .agg(
        F.min("ref_date").alias("min_date"),
        F.max("ref_date").alias("max_date")
    )
    .first()
)

assert retail_count > HOLDOUT_MONTHS
assert duplicate_months == 0

print(f"Retail model-ready rows: {retail_count}")
print(
    f"Date range: {date_range['min_date']} "
    f"to {date_range['max_date']}"
)
print(f"Duplicate months: {duplicate_months}")

# COMMAND ----------

# Review the most recent model-ready observations and key retail lags
display(
    retail_df
    .select(
        "ref_date",
        TARGET_COLUMN,
        "retail_sales_dollars_lag_1m",
        "retail_sales_dollars_lag_3m",
        "retail_sales_dollars_lag_12m"
    )
    .orderBy(F.col("ref_date").desc())
    .limit(12)
)

# COMMAND ----------

# Inspect the first model-ready observations to confirm where usable
# lagged history begins.
display(
    retail_df
    .select(
        "ref_date",
        TARGET_COLUMN,
        "retail_sales_dollars_lag_1m",
        "retail_sales_dollars_lag_12m"
    )
    .orderBy("ref_date")
    .limit(12)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Select Regression Features and Prepare the Modeling Sample
# MAGIC
# MAGIC Ridge Regression uses a focused set of historical retail, macroeconomic, and calendar predictors. The time-series models use the retail sales target history directly.
# MAGIC
# MAGIC Rows with unavailable selected predictors are excluded when missingness reflects insufficient historical lag coverage rather than an observed economic value.

# COMMAND ----------

FEATURE_COLUMNS = [
    # Retail history and momentum
    "retail_sales_dollars_lag_1m",
    "retail_sales_dollars_lag_3m",
    "retail_sales_dollars_lag_12m",
    "retail_sales_dollars_rolling_avg_3m",
    "retail_sales_dollars_rolling_avg_6m",
    "retail_sales_yoy_percent_lag_1m",

    # Lagged macroeconomic conditions
    "cpi_yoy_inflation_percent_lag_1m",
    "employment_mom_percent_lag_1m",
    "unemployment_rate_percent_lag_1m",
    "real_gdp_yoy_percent_lag_1m",
    "avg_monthly_fx_usd_cad_lag_1m",
    "policy_rate_lag_1m",

    # Calendar seasonality known before the forecast month
    "target_month_sin",
    "target_month_cos"
]

missing_features = [
    column
    for column in FEATURE_COLUMNS
    if column not in retail_df.columns
]

assert not missing_features, (
    f"Missing selected features: {missing_features}"
)

print(f"Selected Ridge features: {len(FEATURE_COLUMNS)}")
print(f"Missing feature columns: {missing_features}")

# COMMAND ----------

# Count missing observations by predictor before defining the final
# regression modeling sample.
feature_null_counts = retail_df.select([
    F.sum(F.col(column).isNull().cast("int")).alias(column)
    for column in FEATURE_COLUMNS
])

display(feature_null_counts)

# COMMAND ----------

# Retain only observations with a complete predictor set and observed target.
# The first model-ready month is excluded because several lagged YoY
# predictors require additional historical growth-rate coverage.
model_df = (
    retail_df
    .dropna(subset=FEATURE_COLUMNS + [TARGET_COLUMN])
    .orderBy("ref_date")
)

model_count = model_df.count()

model_date_range = (
    model_df
    .agg(
        F.min("ref_date").alias("min_date"),
        F.max("ref_date").alias("max_date")
    )
    .first()
)

print(f"Usable modeling rows: {model_count}")
print(
    f"Modeling date range: {model_date_range['min_date']} "
    f"to {model_date_range['max_date']}"
)

# COMMAND ----------

# Confirm which model-ready observations were excluded because one or
# more selected predictors were unavailable.
incomplete_predictor_rows = (
    retail_df
    .filter(
        F.expr(
            " OR ".join(
                [f"`{column}` IS NULL" for column in FEATURE_COLUMNS]
            )
        )
    )
    .select(
        "ref_date",
        TARGET_COLUMN,
        *[
            column
            for column in FEATURE_COLUMNS
            if column in [
                "retail_sales_yoy_percent_lag_1m",
                "cpi_yoy_inflation_percent_lag_1m",
                "real_gdp_yoy_percent_lag_1m"
            ]
        ]
    )
    .orderBy("ref_date")
)

display(incomplete_predictor_rows)

# COMMAND ----------

# Convert the small monthly modeling sample to pandas for statsmodels
# and scikit-learn forecasting.
model_pd = (
    model_df
    .select(
        "ref_date",
        TARGET_COLUMN,
        *FEATURE_COLUMNS
    )
    .toPandas()
    .sort_values("ref_date")
    .reset_index(drop=True)
)

model_pd["ref_date"] = pd.to_datetime(model_pd["ref_date"])

# Reserve the final 12 observed months for out-of-sample evaluation.
# All earlier observations form the development period used for
# model tuning and expanding-window validation.
development_pd = (
    model_pd
    .iloc[:-HOLDOUT_MONTHS]
    .copy()
)

holdout_pd = (
    model_pd
    .iloc[-HOLDOUT_MONTHS:]
    .copy()
)

MIN_TRAIN_MONTHS = (
    len(development_pd) - VALIDATION_MONTHS
)

assert MIN_TRAIN_MONTHS >= 36

print(f"Total usable observations: {len(model_pd)}")
print(f"Development observations: {len(development_pd)}")
print(f"Initial training observations: {MIN_TRAIN_MONTHS}")
print(f"Validation observations: {VALIDATION_MONTHS}")
print(f"Holdout observations: {len(holdout_pd)}")

print(
    "Development period: "
    f"{development_pd['ref_date'].min().date()} "
    f"to {development_pd['ref_date'].max().date()}"
)

print(
    "Holdout period: "
    f"{holdout_pd['ref_date'].min().date()} "
    f"to {holdout_pd['ref_date'].max().date()}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Expanding-Window Validation
# MAGIC
# MAGIC Within the development sample, the final 18 months are used as validation targets.
# MAGIC
# MAGIC Each validation month is forecast one step ahead using all observations available before that month. After each forecast, the estimation window expands.
# MAGIC
# MAGIC This approach reflects the operational objective of producing a new one-month-ahead forecast as additional monthly observations become available.

# COMMAND ----------

validation_dates = (
    development_pd
    .iloc[MIN_TRAIN_MONTHS:]["ref_date"]
    .tolist()
)

assert len(validation_dates) == VALIDATION_MONTHS

print(f"Validation forecasts: {len(validation_dates)}")
print(
    "Validation period: "
    f"{validation_dates[0].date()} "
    f"to {validation_dates[-1].date()}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Forecasting Utilities
# MAGIC
# MAGIC Candidate models are refit at each forecast origin so that every one-month-ahead prediction uses only information available before the target month.
# MAGIC
# MAGIC SARIMA uses a deliberately small specification set to limit specification overfitting, while Ridge Regression standardizes predictors and uses regularization to manage correlated economic features.

# COMMAND ----------

def regression_metrics(actual, predicted):
    """Calculate MAE and RMSE for model evaluation."""
    actual = np.asarray(actual, dtype=float)
    predicted = np.asarray(predicted, dtype=float)

    return {
        "mae": float(
            mean_absolute_error(actual, predicted)
        ),
        "rmse": float(
            np.sqrt(
                mean_squared_error(actual, predicted)
            )
        )
    }


def seasonal_naive_one_step(history):
    """Forecast using the observed value from the same month one year earlier."""
    if len(history) < SEASONAL_PERIOD:
        raise ValueError(
            "Seasonal Naive requires at least "
            f"{SEASONAL_PERIOD} prior observations."
        )

    return float(
        history.iloc[-SEASONAL_PERIOD]
    )


def ets_one_step(history):
    """Fit additive ETS and generate a one-month-ahead forecast."""

    # Suppress only the expected ETS optimization convergence warning.
    with warnings.catch_warnings():
        warnings.simplefilter(
            "ignore",
            ConvergenceWarning
        )

        model = ExponentialSmoothing(
            history.astype(float),
            trend="add",
            seasonal="add",
            seasonal_periods=SEASONAL_PERIOD,
            initialization_method="estimated"
        ).fit(
            optimized=True
        )

    return float(
        model.forecast(1).iloc[0]
    )


def sarima_one_step(
    history,
    order,
    seasonal_order
):
    """Fit the specified SARIMA model and forecast one month ahead."""
    model = SARIMAX(
        history.astype(float),
        order=order,
        seasonal_order=seasonal_order,
        enforce_stationarity=False,
        enforce_invertibility=False
    ).fit(
        disp=False
    )

    return float(
        model.forecast(1).iloc[0]
    )


def ridge_one_step(
    train_frame,
    target_row,
    alpha
):
    """Fit standardized Ridge Regression and forecast the target month."""

    # Scaling is required because the economic predictors use substantially
    # different units and magnitudes.
    model = Pipeline([
        (
            "scaler",
            StandardScaler()
        ),
        (
            "ridge",
            Ridge(alpha=alpha)
        )
    ])

    model.fit(
        train_frame[FEATURE_COLUMNS],
        train_frame[TARGET_COLUMN]
    )

    prediction = model.predict(
        target_row[
            FEATURE_COLUMNS
        ].to_frame().T
    )[0]

    return float(prediction)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Tune Candidate Specifications
# MAGIC
# MAGIC Ridge Regression and SARIMA require a small amount of specification tuning before model comparison.
# MAGIC
# MAGIC Ridge alpha values and SARIMA orders are evaluated using the expanding-window development-validation period only. The final holdout remains excluded from all tuning decisions.

# COMMAND ----------

RIDGE_ALPHAS = [
    0.01,
    0.1,
    1.0,
    10.0,
    100.0
]

ridge_tuning_rows = []

for alpha in RIDGE_ALPHAS:
    actuals = []
    predictions = []

    for target_idx in range(
        MIN_TRAIN_MONTHS,
        len(development_pd)
    ):
        train_fold = development_pd.iloc[:target_idx]
        target_row = development_pd.iloc[target_idx]

        prediction = ridge_one_step(
            train_fold,
            target_row,
            alpha
        )

        actuals.append(
            float(target_row[TARGET_COLUMN])
        )

        predictions.append(
            prediction
        )

    metrics = regression_metrics(
        actuals,
        predictions
    )

    ridge_tuning_rows.append({
        "alpha": alpha,
        **metrics
    })

ridge_tuning_df = (
    pd.DataFrame(ridge_tuning_rows)
    .sort_values(["rmse", "mae"])
    .reset_index(drop=True)
)

BEST_RIDGE_ALPHA = float(
    ridge_tuning_df.iloc[0]["alpha"]
)

display(ridge_tuning_df)

print(
    f"Selected Ridge alpha: "
    f"{BEST_RIDGE_ALPHA}"
)

# COMMAND ----------

# Keep the candidate set deliberately small because the monthly history
# is limited and a large parameter search could overfit.
SARIMA_CANDIDATES = [
    (
        (0, 1, 1),
        (0, 1, 1, 12)
    ),
    (
        (1, 1, 0),
        (0, 1, 1, 12)
    ),
    (
        (1, 1, 1),
        (0, 1, 1, 12)
    )
]

sarima_tuning_rows = []

for order, seasonal_order in SARIMA_CANDIDATES:
    actuals = []
    predictions = []

    for target_idx in range(
        MIN_TRAIN_MONTHS,
        len(development_pd)
    ):
        history = (
            development_pd
            .iloc[:target_idx][TARGET_COLUMN]
        )

        target_row = development_pd.iloc[target_idx]

        prediction = sarima_one_step(
            history,
            order,
            seasonal_order
        )

        actuals.append(
            float(target_row[TARGET_COLUMN])
        )

        predictions.append(
            prediction
        )

    metrics = regression_metrics(
        actuals,
        predictions
    )

    sarima_tuning_rows.append({
        "order": order,
        "seasonal_order": seasonal_order,
        **metrics
    })

sarima_tuning_df = (
    pd.DataFrame(sarima_tuning_rows)
    .sort_values(["rmse", "mae"])
    .reset_index(drop=True)
)

BEST_SARIMA_ORDER = (
    sarima_tuning_df.iloc[0]["order"]
)

BEST_SARIMA_SEASONAL_ORDER = (
    sarima_tuning_df.iloc[0]["seasonal_order"]
)

display(sarima_tuning_df)

print(
    "Selected SARIMA:",
    BEST_SARIMA_ORDER,
    "x",
    BEST_SARIMA_SEASONAL_ORDER
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Compare Candidate Models
# MAGIC
# MAGIC The four candidate approaches are evaluated over the same expanding-window validation period:
# MAGIC
# MAGIC - Seasonal Naive
# MAGIC - ETS
# MAGIC - SARIMA
# MAGIC - Ridge Regression
# MAGIC
# MAGIC RMSE is used as the primary model-selection metric because it places greater weight on larger forecast errors. MAE is retained as a complementary measure of typical absolute forecast error.

# COMMAND ----------

validation_prediction_rows = []

for target_idx in range(
    MIN_TRAIN_MONTHS,
    len(development_pd)
):
    train_fold = (
        development_pd
        .iloc[:target_idx]
    )

    target_row = (
        development_pd
        .iloc[target_idx]
    )

    history = (
        train_fold[TARGET_COLUMN]
    )

    actual = float(
        target_row[TARGET_COLUMN]
    )

    # Generate one-month-ahead forecasts from each candidate model
    # using the same expanding training window.
    predictions = {
        "Seasonal Naive":
            seasonal_naive_one_step(
                history
            ),

        "ETS":
            ets_one_step(
                history
            ),

        "SARIMA":
            sarima_one_step(
                history,
                BEST_SARIMA_ORDER,
                BEST_SARIMA_SEASONAL_ORDER
            ),

        "Ridge Regression":
            ridge_one_step(
                train_fold,
                target_row,
                BEST_RIDGE_ALPHA
            )
    }

    for model_name, prediction in predictions.items():
        validation_prediction_rows.append({
            "ref_date":
                target_row["ref_date"],

            "model":
                model_name,

            "actual_retail_sales":
                actual,

            "predicted_retail_sales":
                float(prediction)
        })

validation_predictions_df = pd.DataFrame(
    validation_prediction_rows
)

validation_results = []

for model_name, group in (
    validation_predictions_df
    .groupby("model")
):
    metrics = regression_metrics(
        group["actual_retail_sales"],
        group["predicted_retail_sales"]
    )

    validation_results.append({
        "model": model_name,
        **metrics
    })

validation_results_df = (
    pd.DataFrame(validation_results)
    .sort_values(
        ["rmse", "mae"]
    )
    .reset_index(drop=True)
)

display(validation_results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Select the Leading Model
# MAGIC
# MAGIC The candidate with the lowest expanding-window validation RMSE is selected for final holdout evaluation.
# MAGIC
# MAGIC The final holdout period has not contributed to model selection or hyperparameter tuning.

# COMMAND ----------

best_model_row = (
    validation_results_df
    .iloc[0]
)

selected_model = str(
    best_model_row["model"]
)

print(
    f"Selected model: "
    f"{selected_model}"
)

print(
    f"Validation MAE:  "
    f"${best_model_row['mae'] / 1e9:,.2f}B"
)

print(
    f"Validation RMSE: "
    f"${best_model_row['rmse'] / 1e9:,.2f}B"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Final Holdout Evaluation
# MAGIC
# MAGIC The selected model specification is fixed before holdout evaluation.
# MAGIC
# MAGIC Each of the final 12 months is forecast one step ahead using only observations available before that month. Once an earlier holdout month becomes observed, it may enter the estimation history for the following month's forecast.
# MAGIC
# MAGIC This reproduces an operational monthly forecasting process while keeping holdout performance completely separate from model selection.

# COMMAND ----------

def selected_model_one_step(
    train_frame,
    target_row
):
    """Generate a one-month-ahead forecast using the selected model."""
    history = (
        train_frame[TARGET_COLUMN]
    )

    if selected_model == "Seasonal Naive":
        return seasonal_naive_one_step(
            history
        )

    if selected_model == "ETS":
        return ets_one_step(
            history
        )

    if selected_model == "SARIMA":
        return sarima_one_step(
            history,
            BEST_SARIMA_ORDER,
            BEST_SARIMA_SEASONAL_ORDER
        )

    if selected_model == "Ridge Regression":
        return ridge_one_step(
            train_frame,
            target_row,
            BEST_RIDGE_ALPHA
        )

    raise ValueError(
        f"Unsupported selected model: {selected_model}"
    )

# COMMAND ----------

holdout_rows = []

# Simulate operational one-month-ahead forecasting across the final
# 12 months. Each realized month becomes available for the next forecast.
for target_idx in range(
    len(development_pd),
    len(model_pd)
):
    train_fold = (
        model_pd
        .iloc[:target_idx]
    )

    target_row = (
        model_pd
        .iloc[target_idx]
    )

    prediction = selected_model_one_step(
        train_fold,
        target_row
    )

    actual = float(
        target_row[TARGET_COLUMN]
    )

    holdout_rows.append({
        "ref_date":
            target_row["ref_date"],
        "actual_retail_sales":
            actual,
        "predicted_retail_sales":
            float(prediction),
        "forecast_error":
            actual - float(prediction)
    })

holdout_results_df = pd.DataFrame(
    holdout_rows
)

holdout_metrics = regression_metrics(
    holdout_results_df["actual_retail_sales"],
    holdout_results_df["predicted_retail_sales"]
)

holdout_mae = holdout_metrics["mae"]
holdout_rmse = holdout_metrics["rmse"]

print(
    f"Holdout MAE:  "
    f"${holdout_mae / 1e9:,.2f}B"
)

print(
    f"Holdout RMSE: "
    f"${holdout_rmse / 1e9:,.2f}B"
)

display(holdout_results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Generate the Next One-Month-Ahead Forecast
# MAGIC
# MAGIC After final evaluation, the selected model is refit using all available usable retail sales history.
# MAGIC
# MAGIC The resulting forecast represents the next monthly retail sales observation beyond the latest available actual value.

# COMMAND ----------

latest_actual_date = (
    model_pd["ref_date"]
    .max()
)

forecast_month = (
    latest_actual_date
    + pd.offsets.MonthBegin(1)
)

full_history = (
    model_pd[TARGET_COLUMN]
)

# Refit the selected model using all available actual retail sales history
# before producing the next one-month-ahead forecast.
if selected_model == "Seasonal Naive":

    retail_sales_forecast = (
        seasonal_naive_one_step(
            full_history
        )
    )

elif selected_model == "ETS":

    retail_sales_forecast = (
        ets_one_step(
            full_history
        )
    )

elif selected_model == "SARIMA":

    retail_sales_forecast = (
        sarima_one_step(
            full_history,
            BEST_SARIMA_ORDER,
            BEST_SARIMA_SEASONAL_ORDER
        )
    )

elif selected_model == "Ridge Regression":

    next_feature_pd = (
        forecast_df
        .filter(
            F.col("ref_date")
            == F.lit(
                forecast_month.date()
            )
        )
        .select(
            "ref_date",
            *FEATURE_COLUMNS
        )
        .toPandas()
    )

    assert len(next_feature_pd) == 1, (
        f"Forecast features unavailable "
        f"for {forecast_month.date()}."
    )

    assert not (
        next_feature_pd[
            FEATURE_COLUMNS
        ]
        .isnull()
        .any(axis=None)
    ), (
        "Required Ridge predictors are "
        f"incomplete for {forecast_month.date()}."
    )

    # Standardize the final predictor set using the full modeling history.
    final_ridge = Pipeline([
        (
            "scaler",
            StandardScaler()
        ),
        (
            "ridge",
            Ridge(
                alpha=BEST_RIDGE_ALPHA
            )
        )
    ])

    final_ridge.fit(
        model_pd[FEATURE_COLUMNS],
        model_pd[TARGET_COLUMN]
    )

    retail_sales_forecast = float(
        final_ridge.predict(
            next_feature_pd[
                FEATURE_COLUMNS
            ]
        )[0]
    )

else:

    raise ValueError(
        f"Unsupported selected model: "
        f"{selected_model}"
    )

print(
    "Latest observed retail sales month:",
    latest_actual_date.date()
)

print(
    "Forecast month:",
    forecast_month.date()
)

print(
    "Selected model:",
    selected_model
)

print(
    f"Retail sales forecast: "
    f"${retail_sales_forecast / 1e9:,.2f}B"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Persist Forecasting Outputs
# MAGIC
# MAGIC Persist the validation model-comparison metrics, final holdout predictions, selected-model metadata, and next one-month-ahead forecast.
# MAGIC
# MAGIC These outputs provide the reporting layer used by Azure SQL, Power BI, and the portfolio website.

# COMMAND ----------

# Persist one validation-performance record for each candidate model.
metrics_spark_df = (
    spark.createDataFrame(
        validation_results_df
    )
    .withColumn(
        "target",
        F.lit(TARGET_COLUMN)
    )
    .withColumn(
        "forecast_horizon_months",
        F.lit(1)
    )
    .withColumn(
        "evaluation_period",
        F.lit("validation")
    )
)

(
    metrics_spark_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(METRICS_PATH)
)

print("Validation model metrics saved.")

# COMMAND ----------

holdout_output_pd = (
    holdout_results_df
    .copy()
)

holdout_output_pd["ref_date"] = (
    holdout_output_pd["ref_date"]
    .dt.date
)

# Persist the operational one-step-ahead predictions for the
# final 12-month out-of-sample evaluation period.
holdout_spark_df = (
    spark.createDataFrame(
        holdout_output_pd
    )
    .withColumn(
        "model",
        F.lit(selected_model)
    )
    .withColumn(
        "forecast_horizon_months",
        F.lit(1)
    )
)

(
    holdout_spark_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(HOLDOUT_PATH)
)

print("Holdout predictions saved.")

# COMMAND ----------

# Store the selected model and its final validation and holdout performance.
model_metadata = [{
    "target": TARGET_COLUMN,
    "selected_model": selected_model,
    "selection_metric": "RMSE",

    "validation_mae": float(
        best_model_row["mae"]
    ),
    "validation_rmse": float(
        best_model_row["rmse"]
    ),

    "holdout_mae": float(
        holdout_mae
    ),
    "holdout_rmse": float(
        holdout_rmse
    ),

    "forecast_horizon_months": 1,

    "sarima_order": (
        str(BEST_SARIMA_ORDER)
        if selected_model == "SARIMA"
        else None
    ),

    "sarima_seasonal_order": (
        str(BEST_SARIMA_SEASONAL_ORDER)
        if selected_model == "SARIMA"
        else None
    )
}]

metadata_spark_df = (
    spark.createDataFrame(
        model_metadata
    )
)

(
    metadata_spark_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(METADATA_PATH)
)

print("Selected-model metadata saved.")

# COMMAND ----------

# Persist the single next-month forecast for downstream reporting.
forecast_output = [{
    "ref_date":
        forecast_month.date(),

    "forecast_retail_sales_dollars":
        float(retail_sales_forecast),

    "model":
        selected_model,

    "forecast_horizon_months":
        1
}]

forecast_output_df = (
    spark.createDataFrame(
        forecast_output
    )
)

(
    forecast_output_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(FORECAST_PATH)
)

display(
    forecast_output_df
)

print("Retail sales forecast saved.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Validate Persisted Outputs
# MAGIC
# MAGIC Read the forecasting outputs back from the Gold layer and verify expected record counts, holdout uniqueness, selected-model metadata, and the final forecast record.

# COMMAND ----------

saved_metrics_df = (
    spark.read
    .format("delta")
    .load(METRICS_PATH)
)

saved_holdout_df = (
    spark.read
    .format("delta")
    .load(HOLDOUT_PATH)
)

saved_metadata_df = (
    spark.read
    .format("delta")
    .load(METADATA_PATH)
)

saved_forecast_df = (
    spark.read
    .format("delta")
    .load(FORECAST_PATH)
)

assert saved_metrics_df.count() == 4, (
    "Expected four candidate model rows."
)

assert saved_holdout_df.count() == HOLDOUT_MONTHS, (
    "Unexpected holdout row count."
)

assert saved_metadata_df.count() == 1, (
    "Expected one model metadata row."
)

assert saved_forecast_df.count() == 1, (
    "Expected one forecast row."
)

# Confirm one forecast per holdout month.
holdout_duplicate_months = (
    saved_holdout_df
    .groupBy("ref_date")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

assert holdout_duplicate_months == 0, (
    "Duplicate holdout forecast months detected."
)

print(
    "Saved model metrics rows:",
    saved_metrics_df.count()
)

print(
    "Saved holdout rows:",
    saved_holdout_df.count()
)

print(
    "Saved metadata rows:",
    saved_metadata_df.count()
)

print(
    "Saved forecast rows:",
    saved_forecast_df.count()
)

print(
    "Holdout duplicate months:",
    holdout_duplicate_months
)

print(
    "Retail sales forecasting outputs "
    "validated successfully."
)

# COMMAND ----------

# Final preview of the persisted forecasting outputs.
display(
    saved_metrics_df
    .orderBy("rmse")
)

display(
    saved_metadata_df
)

display(
    saved_forecast_df
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Processing Summary
# MAGIC
# MAGIC The retail sales forecasting workflow completed successfully.
# MAGIC
# MAGIC ### Final Model
# MAGIC
# MAGIC - **Selected model:** SARIMA
# MAGIC - **Specification:** SARIMA (0, 1, 1) × (0, 1, 1, 12)
# MAGIC - **Selection metric:** RMSE
# MAGIC - **Forecast horizon:** 1 month ahead
# MAGIC
# MAGIC ### Model Performance
# MAGIC
# MAGIC - **Validation MAE:** $0.74B
# MAGIC - **Validation RMSE:** $0.97B
# MAGIC - **Holdout MAE:** $0.80B
# MAGIC - **Holdout RMSE:** $0.93B
# MAGIC
# MAGIC ### Forecast
# MAGIC
# MAGIC - **Latest observed month:** June 2026
# MAGIC - **Forecast month:** July 2026
# MAGIC - **Forecast retail sales:** $73.77B
# MAGIC
# MAGIC ### Outputs
# MAGIC
# MAGIC The workflow persists four Gold reporting datasets:
# MAGIC
# MAGIC - Model-comparison metrics
# MAGIC - Holdout predictions
# MAGIC - Selected-model metadata
# MAGIC - One-month-ahead forecast
# MAGIC
# MAGIC These outputs are prepared for downstream Azure SQL, Power BI, and portfolio reporting.

# COMMAND ----------

# MAGIC %md
# MAGIC