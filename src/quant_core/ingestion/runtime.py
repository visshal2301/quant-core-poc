"""Runtime entry points for Bronze ingestion."""

from pyspark.sql import functions as F

from quant_core.ingestion.bronze import ALL_BRONZE_SPECS
from quant_core.ingestion.bronze import build_replace_where
from quant_core.ingestion.bronze import resolve_dimension_path, resolve_monthly_path

OPEN_ENDED_TS = "9999-12-31 23:59:59"


def table_exists(spark, table_name):
    return spark.catalog.tableExists(table_name)


def align_to_existing_bronze_schema(spark, df, target_table_name):
    if not table_exists(spark, target_table_name):
        return df

    target_df = spark.table(target_table_name)
    target_columns = target_df.columns
    source_columns = df.columns

    for column_name in target_columns:
        if column_name not in source_columns:
            target_type = target_df.schema[column_name].dataType
            df = df.withColumn(column_name, F.lit(None).cast(target_type))

    return df.select(*sorted(df.columns))


def ingest_csv_to_bronze(spark, source_path, target_table_name, expected_grain, target_yyyymm):
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .load(source_path)
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("source_file_path", F.col("_metadata.file_path"))
        .withColumn("source_file_name", F.element_at(F.split(F.col("_metadata.file_path"), "/"), -1))
        .withColumn("load_id", F.date_format(F.current_timestamp(), "yyyyMMddHHmmss"))
        .withColumn("record_hash", F.sha2(F.to_json(F.struct("*")), 256))
        .withColumn("system_from_ts", F.current_timestamp())
        .withColumn("system_to_ts", F.to_timestamp(F.lit(OPEN_ENDED_TS)))
        .withColumn("record_version", F.lit(1))
        .withColumn("change_type", F.lit("INSERT"))
        .withColumn("is_current_system", F.lit(True))
        .withColumn("source_yyyymm", F.lit(target_yyyymm))
        .withColumn("expected_grain", F.lit(expected_grain))
        .withColumn("schema_drift_captured_ts", F.current_timestamp())
    )

    if "_corrupt_record" in df.columns:
        df = df.withColumn("is_corrupt_record", F.col("_corrupt_record").isNotNull())
    else:
        df = df.withColumn("is_corrupt_record", F.lit(False))

    drift_columns = [
        column_name
        for column_name in df.columns
        if column_name
        not in {
            "_corrupt_record",
            "ingestion_ts",
            "source_file_path",
            "source_file_name",
            "load_id",
            "record_hash",
            "system_from_ts",
            "system_to_ts",
            "record_version",
            "change_type",
            "is_current_system",
            "source_yyyymm",
            "expected_grain",
            "schema_drift_captured_ts",
            "is_corrupt_record",
        }
    ]
    df = df.withColumn("source_column_list", F.array_sort(F.array(*[F.lit(column_name) for column_name in drift_columns])))
    df = align_to_existing_bronze_schema(spark, df, target_table_name)

    (
        df.write.format("delta")
        .option("mergeSchema", "true")
        .mode("overwrite")
        .saveAsTable(target_table_name)
    )


def run_bronze_ingestion(spark, target_yyyymm, catalog="quant_core", landing_base="/Volumes/quant_core/landing/mock_data"):
    bronze_schema = "{0}.bronze".format(catalog)
    for spec in ALL_BRONZE_SPECS:
        if spec.is_dimension:
            source_path = resolve_dimension_path(landing_base, spec.dataset, target_yyyymm)
        else:
            source_path = resolve_monthly_path(landing_base, spec.dataset, target_yyyymm)
        ingest_csv_to_bronze(
            spark=spark,
            source_path=source_path,
            target_table_name="{0}.{1}".format(bronze_schema, spec.table_name),
            expected_grain=spec.expected_grain,
            target_yyyymm=target_yyyymm,
        )

