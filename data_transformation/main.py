# data_transformation/main.py
# PySpark job submitted to Dataproc via transformer_dag.py
# Flow: Read JSON (explicit schema) → Transform → Write BQ → Archive → Log status

import os
import argparse
import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType, DateType,
    IntegerType, FloatType, BooleanType
)
from transformer import transform
from google.cloud import storage, bigquery


# Step 1: Config Loader
def load_config(path=None):
    """Load YAML config. Path resolved from env var or default."""
    if path is None:
        path = os.environ.get("CONFIG_PATH", "config/config.yaml")
    with open(path) as f:
        return yaml.safe_load(f)


# Step 2: Schema Loader
# Maps schema.yaml type names to PySpark types
_TYPE_MAP = {
    "string":  StringType(),
    "double":  DoubleType(),
    "float":   FloatType(),
    "long":    LongType(),
    "integer": IntegerType(),
    "boolean": BooleanType(),
    "date":    DateType(),
}

def load_spark_schema(schema_path=None):
    """
    Build a PySpark StructType from schema.yaml.
    This enforces column types at read time so Spark never infers them.
    Fields absent in a JSON file become null — expected for missing keys.
    Extra JSON fields not in the schema are silently ignored.
    """
    if schema_path is None:
        schema_path = os.environ.get("SCHEMA_PATH", "config/schema.yaml")
    with open(schema_path) as f:
        cfg = yaml.safe_load(f)

    fields = [
        StructField(col["name"], _TYPE_MAP[col["type"]], nullable=True)
        for col in cfg["columns"]
    ]
    return StructType(fields)


# Step 3: Spark Session Builder
def get_spark():
    # .getOrCreate() reuses existing session on Dataproc (already configured)
    return SparkSession.builder.appName("yfinance_transformation").getOrCreate()


# Step 4: Read with Schema & Extract Metadata
def read_json(spark, base_path, process_date, schema):
    """
    Read all ticker JSONs for a single date partition.
    schema= is critical: without it Spark infers types per-file, causing
    inconsistent types (e.g. marketCap as string in one run, long in another).
    """
    path = f"{base_path}date={process_date}/*.json"
    df   = spark.read.option("multiline", "true").schema(schema).json(path)

    # ticker_id extracted from filename
    df = df.withColumn(
        "ticker_id",
        F.regexp_extract(F.input_file_name(), r"/([^/]+)\.json$", 1)
    )

    # Adding ingestion_date for partitioning in BQ.
    df = df.withColumn("ingestion_date", F.to_date(F.lit(process_date)))

    return df


# Step 5: Write to BigQuery via Spark Connector
def write_bq(df, config):
    """Write Spark DataFrame to BigQuery flat table via the BQ Spark connector."""
    df.write \
      .format("bigquery") \
      .option(
          "table",
          f"{config['gcp']['project_id']}:{config['bq']['dataset']}.{config['bq']['table']}"
      ) \
      .option("temporaryGcsBucket", config["gcp"]["bucket"]) \
      .mode(config["bq"]["write_mode"]) \
      .save()


# Step 6: Archive Processed Raw Files
def archive_folder(bucket_name, source_prefix, dest_prefix):
    """
    Move processed date folder from raw/ → archive/ after successful BQ write.
    Prevents reprocessing on retry and keeps raw/ clean.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs  = list(bucket.list_blobs(prefix=source_prefix))

    for blob in blobs:
        new_name = blob.name.replace(source_prefix, dest_prefix, 1)
        bucket.copy_blob(blob, bucket, new_name)
        blob.delete()

    print(f"[INFO] Archived {len(blobs)} files: {source_prefix} → {dest_prefix}")


# Step 7: Update Control Table in BigQuery
def update_control(project_id, dataset, table, dag_id, status):
    """Log pipeline run status to BQ control table for audit and monitoring."""
    client = bigquery.Client(project=project_id)
    query  = f"""
        INSERT INTO `{project_id}.{dataset}.{table}`
        (dag_id, status, updated_ts)
        VALUES ('{dag_id}', '{status}', CURRENT_TIMESTAMP())
    """
    client.query(query).result()


# Step 8: CLI Argument Parsing
def parse_args():
    """Accept --date from Airflow so process_date isn't hardcoded in config."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None, help="Process date YYYY-MM-DD")
    return parser.parse_args()

# Step 9: Orchestration & Control Flow
def run():
    args         = parse_args()
    config       = load_config()
    schema       = load_spark_schema()
    spark        = get_spark()

    # CLI arg takes precedence over config.yaml runtime.process_date
    process_date = args.date or config["runtime"]["process_date"]
    print(f"[INFO] Processing date: {process_date}")

    try:
        df = read_json(spark, config["paths"]["raw_base"], process_date, schema)
        df = transform(df)
        write_bq(df, config)
        archive_folder(
            config["gcp"]["bucket"],
            f"raw-yfinance/date={process_date}",
            f"archive-yfinance/date={process_date}"
        )
        update_control(
            config["gcp"]["project_id"], config["bq"]["dataset"],
            config["bq"]["control_table"], "yfinance_transformation", "SUCCESS"
        )
        print("[INFO] Pipeline completed successfully.")

    except Exception as e:
        update_control(
            config["gcp"]["project_id"], config["bq"]["dataset"],
            config["bq"]["control_table"], "yfinance_transformation", "FAILED"
        )
        raise


if __name__ == "__main__":
    run()
