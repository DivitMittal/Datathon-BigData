import sys
from typing import Optional

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
import pyspark.sql.functions as F

from config import (
    S3_INPUT_PATH,
    S3_OUTPUT_PATH,
    EVENT_TIMESTAMP_START,
    EVENT_TIMESTAMP_END,
    JOB_NAME,
    COLUMN_EVENT_ACTION,
    COLUMN_CREATED_AT,
    COLUMN_SERVER_CREATED_AT,
    COLUMN_EVENT_TIMESTAMP,
    COLUMN_EVENT_NAME,
    COLUMN_EVENT_DATE,
    COLUMN_PROCESS_DATE,
    COLUMN_DATA,
    COLUMN_REQUEST_ID,
    COLUMN_DATA_EVENT_PARAMS,
    DATA_FIELD_PREFIX,
)


def init_spark():
    sc = SparkContext.getOrCreate()
    glue_ctx = GlueContext(sc)
    spark = glue_ctx.spark_session
    job = Job(glue_ctx)
    job.init(JOB_NAME)
    return spark, glue_ctx, job


def load_data(spark, path: str):
    return spark.read.json(path)


def add_ts(df):
    return df.withColumn(
        COLUMN_EVENT_TIMESTAMP,
        F.unix_timestamp(COLUMN_CREATED_AT, "yyyy-MM-dd HH:mm:ss"),
    )


def filter_by_ts(df, start: int, end: int):
    return df.filter(
        (F.col(COLUMN_EVENT_TIMESTAMP) >= start)
        & (F.col(COLUMN_EVENT_TIMESTAMP) <= end)
    )


def add_dates(df):
    df = df.withColumn(
        COLUMN_EVENT_DATE, F.to_date(COLUMN_CREATED_AT, "yyyy-MM-dd HH:mm:ss")
    )
    df = df.withColumn(
        COLUMN_PROCESS_DATE, F.to_date(COLUMN_SERVER_CREATED_AT, "yyyy-MM-dd HH:mm:ss")
    )
    return df


def rename_columns(df):
    return df.withColumn(COLUMN_EVENT_NAME, F.col(COLUMN_EVENT_ACTION).cast("string"))


def drop_columns(df):
    for col in [
        COLUMN_CREATED_AT,
        COLUMN_SERVER_CREATED_AT,
        COLUMN_EVENT_ACTION,
        COLUMN_REQUEST_ID,
    ]:
        if col in df.columns:
            df = df.drop(col)
    return df


def expand_data(df):
    df_expanded = df.select(
        F.col("device_id"),
        F.col("advertisingId"),
        F.col("app_id"),
        F.col("app_version"),
        F.col("sdk_version"),
        F.col("ip"),
        F.col(COLUMN_EVENT_TIMESTAMP),
        F.col(COLUMN_EVENT_NAME),
        F.col(COLUMN_PROCESS_DATE),
        F.col(COLUMN_EVENT_DATE),
        F.col("event_type"),
        F.col("screen_at"),
        F.col(COLUMN_DATA).alias(COLUMN_DATA_EVENT_PARAMS),
    )

    data_fields = df_expanded.select(f"{COLUMN_DATA_EVENT_PARAMS}.*").columns
    for field in data_fields:
        df_expanded = df_expanded.withColumn(
            f"{DATA_FIELD_PREFIX}{field}", F.col(f"{COLUMN_DATA_EVENT_PARAMS}.{field}")
        )

    return df_expanded.drop(COLUMN_DATA_EVENT_PARAMS)


def write_parquet(df, path: str):
    df.coalesce(1).write.mode("overwrite").parquet(path)


def run(spark, input_path: Optional[str] = None, output_path: Optional[str] = None):
    input_path = input_path or S3_INPUT_PATH
    output_path = output_path or S3_OUTPUT_PATH

    print(f"Loading: {input_path}")
    df = load_data(spark, input_path)
    print(f"Loaded {df.count()} rows, {len(df.columns)} cols")

    df = df.drop(COLUMN_REQUEST_ID)
    df = add_ts(df)
    df = filter_by_ts(df, EVENT_TIMESTAMP_START, EVENT_TIMESTAMP_END)
    print(f"Filtered: {df.count()} rows")

    df = add_dates(df)
    df = rename_columns(df)
    df = drop_columns(df)
    print(f"Columns: {len(df.columns)}")

    df = expand_data(df)
    print(f"Expanded: {len(df.columns)} columns")

    print(f"Writing: {output_path}")
    write_parquet(df, output_path)


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "input_path", "output_path"])
    input_path = args.get("input_path", S3_INPUT_PATH)
    output_path = args.get("output_path", S3_OUTPUT_PATH)

    spark, _, _ = init_spark()
    run(spark, input_path, output_path)


if __name__ == "__main__":
    main()
