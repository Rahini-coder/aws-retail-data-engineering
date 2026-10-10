import sys
import uuid

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window


BUSINESS_COLUMNS = [
    "customer_id", "order_date", "product_id", "quantity",
    "unit_price", "discount_pct", "order_amount",
    "order_status", "payment_method"
]


def record_hash(df):
    values = [
        F.coalesce(F.col(c).cast("string"), F.lit("<NULL>"))
        for c in BUSINESS_COLUMNS
    ]
    return df.withColumn(
        "record_hash",
        F.sha2(F.concat_ws("||", *values), 256)
    )


def normalize(df, bootstrap_timestamp, run_id):
    return (
        df
        .withColumn("order_id", F.col("order_id").cast("string"))
        .withColumn("customer_id", F.col("customer_id").cast("string"))
        .withColumn("order_date", F.to_date("order_date"))
        .withColumn("product_id", F.col("product_id").cast("string"))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("decimal(18,2)"))
        .withColumn("discount_pct", F.col("discount_pct").cast("decimal(7,4)"))
        .withColumn("order_amount", F.col("order_amount").cast("decimal(18,2)"))
        .withColumn("order_status", F.col("order_status").cast("string"))
        .withColumn("payment_method", F.col("payment_method").cast("string"))
        .withColumn("source_updated_at", F.to_timestamp(F.lit(bootstrap_timestamp)))
        .withColumn("pipeline_run_id", F.lit(run_id))
        .withColumn("source_file", F.input_file_name())
    )


def add_reference_flags(orders, customers, products):
    customers_ref = (
        customers.select(F.col("customer_id").cast("string").alias("customer_id"))
        .where(F.col("customer_id").isNotNull())
        .dropDuplicates(["customer_id"])
        .withColumn("_customer_exists", F.lit(True))
    )
    products_ref = (
        products.select(F.col("product_id").cast("string").alias("product_id"))
        .where(F.col("product_id").isNotNull())
        .dropDuplicates(["product_id"])
        .withColumn("_product_exists", F.lit(True))
    )
    return (
        orders
        .join(F.broadcast(customers_ref), "customer_id", "left")
        .join(F.broadcast(products_ref), "product_id", "left")
    )


def apply_bootstrap_dq(df):
    valid_statuses = ["Completed", "Cancelled", "Returned", "Pending"]

    rules = [
        ("DQ_001", F.col("order_id").isNull()),
        ("DQ_002", F.col("customer_id").isNull()),
        ("DQ_003", F.col("product_id").isNull()),
        ("DQ_004", F.col("order_date").isNull()),
        ("DQ_005", F.col("order_date") > F.current_date()),
        ("DQ_006", F.col("quantity").isNull() | (F.col("quantity") <= 0)),
        ("DQ_007", F.col("unit_price").isNull() | (F.col("unit_price") < 0)),
        ("DQ_008", F.col("discount_pct").isNull()
                   | (F.col("discount_pct") < 0)
                   | (F.col("discount_pct") > 100)),
        ("DQ_009", F.col("order_amount").isNull() | (F.col("order_amount") <= 0)),
        ("DQ_010", F.col("order_status").isNull()
                   | (~F.col("order_status").isin(valid_statuses))),
        ("DQ_011", F.col("_customer_exists").isNull()),
        ("DQ_012", F.col("_product_exists").isNull()),
    ]

    ids = [F.when(condition, F.lit(rule_id)) for rule_id, condition in rules]

    return (
        df.withColumn("dq_rule_ids", F.array_compact(F.array(*ids)))
          .withColumn("dq_is_valid", F.size("dq_rule_ids") == 0)
    )


def resolve_bootstrap_duplicates(valid_df):
    # Detect business-key conflicts first.
    key_summary = (
        valid_df.groupBy("order_id")
        .agg(
            F.count("*").alias("_key_row_count"),
            F.countDistinct("record_hash").alias("_key_hash_count")
        )
    )

    enriched = valid_df.join(key_summary, "order_id", "left")

    conflicts = enriched.filter(F.col("_key_hash_count") > 1)

    non_conflicting = enriched.filter(F.col("_key_hash_count") == 1)

    # For exact duplicate copies, keep exactly one deterministic canonical row.
    w = Window.partitionBy("order_id").orderBy(
        F.col("record_hash").asc(),
        F.col("source_file").asc()
    )

    ranked = non_conflicting.withColumn(
        "_duplicate_rank", F.row_number().over(w)
    )

    canonical = ranked.filter(F.col("_duplicate_rank") == 1)
    duplicate_copies = ranked.filter(F.col("_duplicate_rank") > 1)

    cleanup = ["_key_row_count", "_key_hash_count", "_duplicate_rank"]

    canonical = canonical.drop(*cleanup)
    duplicate_copies = duplicate_copies.drop(*cleanup)
    conflicts = conflicts.drop("_key_row_count", "_key_hash_count")

    return canonical, duplicate_copies, conflicts


def ensure_empty_bootstrap_target(spark, table_fqn, location):
    spark.sql(f'''
        CREATE TABLE IF NOT EXISTS {table_fqn} (
            order_id STRING NOT NULL,
            customer_id STRING,
            order_date DATE,
            product_id STRING,
            quantity INT,
            unit_price DECIMAL(18,2),
            discount_pct DECIMAL(7,4),
            order_amount DECIMAL(18,2),
            order_status STRING,
            payment_method STRING,
            source_updated_at TIMESTAMP,
            record_hash STRING,
            source_file STRING,
            first_ingested_at TIMESTAMP,
            last_updated_at TIMESTAMP,
            pipeline_run_id STRING
        )
        USING iceberg
        PARTITIONED BY (months(order_date))
        LOCATION '{location}'
        TBLPROPERTIES ('format-version'='2')
    ''')

    existing = spark.table(table_fqn).limit(1).count()
    if existing:
        raise RuntimeError(
            f"Bootstrap target {table_fqn} is not empty. "
            "Refusing to overwrite an established Silver table."
        )


def write_bootstrap_silver(canonical, table_fqn):
    (
        canonical.select(
            "order_id", "customer_id", "order_date", "product_id",
            "quantity", "unit_price", "discount_pct", "order_amount",
            "order_status", "payment_method", "source_updated_at",
            "record_hash", "source_file", "pipeline_run_id"
        )
        .withColumn("first_ingested_at", F.current_timestamp())
        .withColumn("last_updated_at", F.current_timestamp())
        .writeTo(table_fqn)
        .append()
    )


def write_quarantine(df, path, category):
    (
        df.withColumn("quarantine_category", F.lit(category))
          .withColumn("quarantine_timestamp", F.current_timestamp())
          .withColumn("processing_date", F.current_date())
          .write.mode("append")
          .partitionBy("processing_date", "quarantine_category")
          .parquet(path)
    )


def main():
    arg_names = [
        "JOB_NAME", "database", "orders_table", "customers_table",
        "products_table", "quarantine_path", "audit_path",
        "silver_table_fqn", "silver_table_location",
        "bootstrap_version_timestamp"
    ]
    args = getResolvedOptions(sys.argv, arg_names)

    sc = SparkContext.getOrCreate()
    glue = GlueContext(sc)
    spark = glue.spark_session
    job = Job(glue)
    job.init(args["JOB_NAME"], args)

    run_id = str(uuid.uuid4())

    raw_orders = glue.create_dynamic_frame.from_catalog(
        database=args["database"],
        table_name=args["orders_table"]
    ).toDF()

    customers = glue.create_dynamic_frame.from_catalog(
        database=args["database"],
        table_name=args["customers_table"]
    ).toDF()

    products = glue.create_dynamic_frame.from_catalog(
        database=args["database"],
        table_name=args["products_table"]
    ).toDF()

    orders = normalize(
        raw_orders,
        args["bootstrap_version_timestamp"],
        run_id
    )
    orders = add_reference_flags(orders, customers, products)
    orders = record_hash(orders)
    validated = apply_bootstrap_dq(orders).persist()

    input_count = validated.count()

    dq_bad = validated.filter(~F.col("dq_is_valid"))
    dq_good = validated.filter(F.col("dq_is_valid"))

    dq_rejected_count = dq_bad.count()
    if dq_rejected_count:
        write_quarantine(
            dq_bad.drop("_customer_exists", "_product_exists"),
            args["quarantine_path"],
            "BOOTSTRAP_DATA_QUALITY"
        )

    canonical, duplicates, conflicts = resolve_bootstrap_duplicates(dq_good)

    canonical = canonical.persist()
    duplicates = duplicates.persist()
    conflicts = conflicts.persist()

    silver_insert_count = canonical.count()
    duplicate_copy_count = duplicates.count()
    conflict_count = conflicts.count()

    if duplicate_copy_count:
        write_quarantine(
            duplicates.drop("_customer_exists", "_product_exists"),
            args["quarantine_path"],
            "BOOTSTRAP_DUPLICATE_COPY"
        )

    if conflict_count:
        write_quarantine(
            conflicts.drop("_customer_exists", "_product_exists"),
            args["quarantine_path"],
            "BOOTSTRAP_KEY_CONFLICT"
        )

    reconciled = (
        dq_rejected_count
        + duplicate_copy_count
        + conflict_count
        + silver_insert_count
    )

    if reconciled != input_count:
        raise RuntimeError(
            f"Bootstrap reconciliation failed: input={input_count}, "
            f"accounted={reconciled}"
        )

    ensure_empty_bootstrap_target(
        spark,
        args["silver_table_fqn"],
        args["silver_table_location"]
    )

    write_bootstrap_silver(canonical, args["silver_table_fqn"])

    actual_silver_count = spark.table(
        args["silver_table_fqn"]
    ).count()

    if actual_silver_count != silver_insert_count:
        raise RuntimeError(
            f"Silver reconciliation failed: expected={silver_insert_count}, "
            f"actual={actual_silver_count}"
        )

    audit = spark.createDataFrame([{
        "pipeline_run_id": run_id,
        "pipeline_mode": "BOOTSTRAP",
        "bootstrap_version_timestamp": args["bootstrap_version_timestamp"],
        "input_record_count": input_count,
        "dq_rejected_count": dq_rejected_count,
        "duplicate_copy_count": duplicate_copy_count,
        "conflict_count": conflict_count,
        "silver_insert_count": silver_insert_count,
        "silver_actual_count": actual_silver_count,
        "reconciliation_status": "PASSED",
        "status": "SUCCEEDED",
    }]).withColumn("audit_timestamp", F.current_timestamp())

    audit.write.mode("append").parquet(args["audit_path"])

    canonical.unpersist()
    duplicates.unpersist()
    conflicts.unpersist()
    validated.unpersist()

    job.commit()


if __name__ == "__main__":
    main()

