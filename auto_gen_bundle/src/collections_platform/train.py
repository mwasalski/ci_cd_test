"""Propensity-to-pay training with MLflow + Unity Catalog model registry.

The DE-relevant parts are not the model. They are:
  * the training set is built by a *versioned* function against a *parameterised*
    as_of_date, so the run is reproducible;
  * the Delta table version is logged, so "which data did this model see" has an
    answer;
  * the model is registered in UC (three-level name), so lineage from table ->
    feature -> model is queryable.
"""

from __future__ import annotations

from datetime import date

import mlflow
from pyspark.sql import DataFrame, SparkSession

from .observability import log_event


def log_source_data_version(spark: SparkSession, table: str) -> None:
    """Record the exact Delta version used. Time travel makes the run replayable:
    `SELECT * FROM tbl VERSION AS OF <v>`. Without this, "reproducible" is a claim,
    not a fact."""
    version = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").collect()[0]["version"]
    mlflow.log_param(f"src_version__{table.replace('.', '__')}", version)
    log_event("mlflow.source_version", table=table, version=version)


def train_propensity(
    spark: SparkSession,
    training_set: DataFrame,
    as_of: date,
    model_name: str,
    experiment_path: str,
) -> str:
    """Returns the registered model URI."""
    import mlflow.spark
    from pyspark.ml import Pipeline
    from pyspark.ml.classification import GBTClassifier
    from pyspark.ml.evaluation import BinaryClassificationEvaluator
    from pyspark.ml.feature import VectorAssembler

    mlflow.set_registry_uri("databricks-uc")  # register into Unity Catalog, not workspace registry
    mlflow.set_experiment(experiment_path)

    feature_cols = [
        "payments_count_lifetime",
        "amount_paid_lifetime",
        "amount_paid_30d",
        "amount_paid_90d",
        "distinct_months_paid",
        "days_since_last_payment",
        "avg_days_between_payments",
    ]

    # Time-based split, never random. A random split lets the model see the same
    # debtor on both sides and inflates every metric.
    train_df = training_set.filter(training_set.as_of_date < as_of)
    test_df = training_set.filter(training_set.as_of_date >= as_of)

    with mlflow.start_run(run_name=f"propensity_{as_of.isoformat()}") as run:
        mlflow.log_params({"as_of_date": as_of.isoformat(), "label_horizon_days": 90})
        mlflow.log_param("feature_cols", ",".join(feature_cols))

        pipeline = Pipeline(
            stages=[
                VectorAssembler(
                    inputCols=feature_cols, outputCol="features", handleInvalid="keep"
                ),
                GBTClassifier(labelCol="label_paid_90d", featuresCol="features", maxIter=50),
            ]
        )
        model = pipeline.fit(train_df)

        auc = BinaryClassificationEvaluator(
            labelCol="label_paid_90d", metricName="areaUnderROC"
        ).evaluate(model.transform(test_df))
        mlflow.log_metric("test_auc", auc)

        # A sanity gate, not a vanity metric. AUC above ~0.95 on a 90-day
        # repayment label almost always means leakage, not a great model.
        if auc > 0.95:
            log_event("train.suspicious_auc", auc=auc, hint="check feature point-in-time bounds")

        mlflow.spark.log_model(
            model, artifact_path="model", registered_model_name=model_name
        )
        log_event("train.done", run_id=run.info.run_id, auc=auc, model=model_name)
        return f"models:/{model_name}@champion"
