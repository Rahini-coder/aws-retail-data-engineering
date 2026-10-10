import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

REQUIRED_ARGS = [
    "JOB_NAME",
    "source_database",
    "customers_table",
    "silver_database",
    "silver_orders_table",
    "gold_database",
    "gold_customer_360_table",
    "gold_customer_360_location",
    "gold_audit_path",
]

def main():
    a = getResolvedOptions(sys.argv, REQUIRED_ARGS)

    sc = SparkContext()
    glue = GlueContext(sc)
    spark = glue.spark_session
    job = Job(glue)
    job.init(a["JOB_NAME"], a)

    catalog = "glue_catalog"
    silver_fqn = f"{catalog}.{a['silver_database']}.{a['silver_orders_table']}"
    gold_fqn = f"{catalog}.{a['gold_database']}.{a['gold_customer_360_table']}"

    # ----------------------------
    # Customer master contract
    # ----------------------------
    # customers is a crawler-created Glue Data Catalog external table,
    # not an Iceberg table. Read it through Glue's catalog source API.
    customers = (
        glue.create_dynamic_frame.from_catalog(
            database=a["source_database"],
            table_name=a["customers_table"],
            transformation_ctx="customers_snapshot_source"
        ).toDF()
        .select(
            F.trim(F.col("customer_id")).alias("customer_id"),
            F.col("first_name"),
            F.col("last_name"),
            F.col("age").cast("long").alias("age"),
            F.col("gender"),
            F.col("country"),
            F.col("state"),
            F.col("city"),
            F.to_date("signup_date").alias("signup_date"),
            F.col("subscription_type"),
            F.col("monthly_revenue")
             .cast(DecimalType(18, 2))
             .alias("monthly_subscription_revenue"),
            F.col("preferred_device"),
            F.col("acquisition_channel"),
        )
        .cache()
    )

    customer_count = customers.count()
    null_customer_ids = customers.filter(F.col("customer_id").isNull() | (F.col("customer_id") == "")).count()
    duplicate_customer_ids = (
        customers.groupBy("customer_id").count()
        .filter(F.col("customer_id").isNotNull() & (F.col("count") > 1))
        .count()
    )

    if null_customer_ids or duplicate_customer_ids:
        raise RuntimeError(
            "CUSTOMER_MASTER_CONTRACT_FAILURE: "
            f"null_or_blank_customer_ids={null_customer_ids}, "
            f"duplicate_customer_ids={duplicate_customer_ids}"
        )

    # ----------------------------
    # Completed-order behavior
    # ----------------------------
    completed = (
        spark.table(silver_fqn)
        .filter(F.col("order_status") == F.lit("Completed"))
        .select(
            F.col("customer_id"),
            F.col("order_id"),
            F.to_date("order_date").alias("order_date"),
            F.col("quantity").cast("long").alias("quantity"),
            F.col("unit_price").cast(DecimalType(18, 2)).alias("unit_price"),
            F.col("order_amount").cast(DecimalType(18, 2)).alias("order_amount"),
        )
        .cache()
    )

    bad_order_dates = completed.filter(F.col("order_date").isNull()).count()
    if bad_order_dates:
        raise RuntimeError(
            f"GOLD_CONTRACT_FAILURE: {bad_order_dates} Completed orders have invalid order_date"
        )

    behavior = (
        completed
        .withColumn(
            "gross_line_amount",
            (F.col("quantity") * F.col("unit_price")).cast(DecimalType(38, 2))
        )
        .groupBy("customer_id")
        .agg(
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
            F.countDistinct("order_id").cast("long").alias("lifetime_orders"),
            F.sum("quantity").cast("long").alias("lifetime_units"),
            F.sum("gross_line_amount").cast(DecimalType(38, 2)).alias("lifetime_gross_sales"),
            F.sum("order_amount").cast(DecimalType(38, 2)).alias("lifetime_net_sales"),
        )
        .withColumn(
            "average_order_value",
            (F.col("lifetime_net_sales") / F.col("lifetime_orders")).cast(DecimalType(38, 2))
        )
    )

    zero_money = F.lit(0).cast(DecimalType(38, 2))

    gold = (
        customers.alias("c")
        .join(behavior.alias("b"), on="customer_id", how="left")
        .select(
            "customer_id",
            "first_name",
            "last_name",
            "age",
            "gender",
            "country",
            "state",
            "city",
            "signup_date",
            "subscription_type",
            "monthly_subscription_revenue",
            "preferred_device",
            "acquisition_channel",
            "first_order_date",
            "last_order_date",
            F.coalesce(F.col("lifetime_orders"), F.lit(0)).cast("long").alias("lifetime_orders"),
            F.coalesce(F.col("lifetime_units"), F.lit(0)).cast("long").alias("lifetime_units"),
            F.coalesce(F.col("lifetime_gross_sales"), zero_money)
             .cast(DecimalType(38, 2)).alias("lifetime_gross_sales"),
            F.coalesce(F.col("lifetime_net_sales"), zero_money)
             .cast(DecimalType(38, 2)).alias("lifetime_net_sales"),
            F.coalesce(F.col("average_order_value"), zero_money)
             .cast(DecimalType(38, 2)).alias("average_order_value"),
        )
        .withColumn(
            "days_since_last_order",
            F.when(
                F.col("last_order_date").isNotNull(),
                F.datediff(F.current_date(), F.col("last_order_date"))
            ).cast("long")
        )
        .withColumn(
            "customer_purchase_status",
            F.when(F.col("lifetime_orders") > 0, F.lit("PURCHASED"))
             .otherwise(F.lit("NEVER_PURCHASED"))
        )
        .withColumn("last_refreshed_at", F.current_timestamp())
    )

    # ----------------------------
    # Iceberg Gold table
    # ----------------------------
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {gold_fqn} (
            customer_id STRING,
            first_name STRING,
            last_name STRING,
            age BIGINT,
            gender STRING,
            country STRING,
            state STRING,
            city STRING,
            signup_date DATE,
            subscription_type STRING,
            monthly_subscription_revenue DECIMAL(18,2),
            preferred_device STRING,
            acquisition_channel STRING,
            first_order_date DATE,
            last_order_date DATE,
            lifetime_orders BIGINT,
            lifetime_units BIGINT,
            lifetime_gross_sales DECIMAL(38,2),
            lifetime_net_sales DECIMAL(38,2),
            average_order_value DECIMAL(38,2),
            days_since_last_order BIGINT,
            customer_purchase_status STRING,
            last_refreshed_at TIMESTAMP
        )
        USING iceberg
        LOCATION '{a["gold_customer_360_location"]}'
        TBLPROPERTIES (
            'format-version'='2',
            'write.parquet.compression-codec'='snappy'
        )
    """)

    # V1 is a deterministic current-state full refresh.
    gold.createOrReplaceTempView("gold_customer_360_stage")
    spark.sql(f"""
        INSERT OVERWRITE {gold_fqn}
        SELECT
            customer_id,
            first_name,
            last_name,
            age,
            gender,
            country,
            state,
            city,
            signup_date,
            subscription_type,
            monthly_subscription_revenue,
            preferred_device,
            acquisition_channel,
            first_order_date,
            last_order_date,
            lifetime_orders,
            lifetime_units,
            lifetime_gross_sales,
            lifetime_net_sales,
            average_order_value,
            days_since_last_order,
            customer_purchase_status,
            last_refreshed_at
        FROM gold_customer_360_stage
    """)

    # ----------------------------
    # Reconciliation
    # ----------------------------
    silver_recon = completed.agg(
        F.countDistinct("order_id").alias("orders"),
        F.sum("order_amount").cast(DecimalType(38, 2)).alias("net_sales")
    ).first()

    published = spark.table(gold_fqn).cache()
    gold_recon = published.agg(
        F.count("*").alias("customers"),
        F.countDistinct("customer_id").alias("distinct_customers"),
        F.sum("lifetime_orders").alias("orders"),
        F.sum("lifetime_net_sales").cast(DecimalType(38, 2)).alias("net_sales"),
    ).first()

    if gold_recon["customers"] != customer_count:
        raise RuntimeError(
            f"GOLD_CUSTOMER_COUNT_FAILURE: master={customer_count}, gold={gold_recon['customers']}"
        )

    if gold_recon["customers"] != gold_recon["distinct_customers"]:
        raise RuntimeError(
            "GOLD_CUSTOMER_UNIQUENESS_FAILURE: "
            f"rows={gold_recon['customers']}, distinct_customer_id={gold_recon['distinct_customers']}"
        )

    if (silver_recon["orders"] != gold_recon["orders"] or
            silver_recon["net_sales"] != gold_recon["net_sales"]):
        raise RuntimeError(
            "GOLD_RECONCILIATION_FAILURE: "
            f"Silver orders={silver_recon['orders']}, Gold orders={gold_recon['orders']}; "
            f"Silver net_sales={silver_recon['net_sales']}, Gold net_sales={gold_recon['net_sales']}"
        )

    purchase_status = {
        r["customer_purchase_status"]: int(r["count"])
        for r in published.groupBy("customer_purchase_status").count().collect()
    }

    audit = spark.createDataFrame([{
        "pipeline_name": "gold_customer_360",
        "refresh_mode": "FULL",
        "business_rule": "financial behavior uses order_status=Completed; preserve all customer master rows",
        "master_customer_count": int(customer_count),
        "gold_customer_count": int(gold_recon["customers"]),
        "gold_distinct_customer_count": int(gold_recon["distinct_customers"]),
        "silver_completed_orders": int(silver_recon["orders"]),
        "gold_lifetime_orders": int(gold_recon["orders"]),
        "silver_net_sales": str(silver_recon["net_sales"]),
        "gold_lifetime_net_sales": str(gold_recon["net_sales"]),
        "purchased_customers": purchase_status.get("PURCHASED", 0),
        "never_purchased_customers": purchase_status.get("NEVER_PURCHASED", 0),
        "reconciliation_status": "PASSED",
        "status": "SUCCEEDED",
    }]).withColumn("audit_timestamp", F.current_timestamp())

    audit.write.mode("append").parquet(a["gold_audit_path"])

    published.unpersist()
    completed.unpersist()
    customers.unpersist()
    job.commit()

if __name__ == "__main__":
    main()
