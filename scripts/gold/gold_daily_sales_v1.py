import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

REQUIRED_ARGS = [
    "JOB_NAME", "silver_database", "silver_orders_table",
    "gold_database", "gold_daily_sales_table",
    "gold_daily_sales_location", "gold_audit_path"
]

def main():
    a = getResolvedOptions(sys.argv, REQUIRED_ARGS)
    sc = SparkContext()
    glue = GlueContext(sc)
    spark = glue.spark_session
    job = Job(glue)
    job.init(a["JOB_NAME"], a)

    silver_fqn = f"glue_catalog.{a['silver_database']}.{a['silver_orders_table']}"
    gold_fqn = f"glue_catalog.{a['gold_database']}.{a['gold_daily_sales_table']}"

    completed = (
        spark.table(silver_fqn)
        .filter(F.col("order_status") == "Completed")
        .select(
            F.to_date("order_date").alias("order_date"),
            "order_id", "customer_id",
            F.col("quantity").cast("long").alias("quantity"),
            F.col("unit_price").cast(DecimalType(18,2)).alias("unit_price"),
            F.col("order_amount").cast(DecimalType(18,2)).alias("order_amount")
        ).cache()
    )

    bad_dates = completed.filter(F.col("order_date").isNull()).count()
    if bad_dates:
        raise RuntimeError(f"GOLD_CONTRACT_FAILURE: {bad_dates} invalid order_date values")

    gold = (
        completed
        .withColumn("gross_line_amount",
                    (F.col("quantity") * F.col("unit_price")).cast(DecimalType(38,2)))
        .groupBy("order_date")
        .agg(
            F.countDistinct("order_id").cast("long").alias("total_orders"),
            F.countDistinct("customer_id").cast("long").alias("unique_customers"),
            F.sum("quantity").cast("long").alias("units_sold"),
            F.sum("gross_line_amount").cast(DecimalType(38,2)).alias("gross_sales"),
            F.sum("order_amount").cast(DecimalType(38,2)).alias("net_sales")
        )
        .withColumn("discount_amount",
                    (F.col("gross_sales") - F.col("net_sales")).cast(DecimalType(38,2)))
        .withColumn("average_order_value",
                    (F.col("net_sales") / F.col("total_orders")).cast(DecimalType(38,2)))
        .withColumn("last_refreshed_at", F.current_timestamp())
        .select("order_date","total_orders","unique_customers","units_sold",
                "gross_sales","discount_amount","net_sales",
                "average_order_value","last_refreshed_at")
    )

    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {gold_fqn} (
        order_date DATE,
        total_orders BIGINT,
        unique_customers BIGINT,
        units_sold BIGINT,
        gross_sales DECIMAL(38,2),
        discount_amount DECIMAL(38,2),
        net_sales DECIMAL(38,2),
        average_order_value DECIMAL(38,2),
        last_refreshed_at TIMESTAMP
      )
      USING iceberg
      LOCATION '{a["gold_daily_sales_location"]}'
      TBLPROPERTIES ('format-version'='2',
                     'write.parquet.compression-codec'='snappy')
    """)

    # Full-refresh V1: reruns replace Gold rather than append duplicates.
    gold.createOrReplaceTempView("gold_daily_sales_stage")
    spark.sql(f"""
      INSERT OVERWRITE {gold_fqn}
      SELECT order_date,total_orders,unique_customers,units_sold,
             gross_sales,discount_amount,net_sales,
             average_order_value,last_refreshed_at
      FROM gold_daily_sales_stage
    """)

    silver_recon = completed.agg(
        F.countDistinct("order_id").alias("orders"),
        F.sum("order_amount").cast(DecimalType(38,2)).alias("net_sales")
    ).first()
    gold_recon = spark.table(gold_fqn).agg(
        F.sum("total_orders").alias("orders"),
        F.sum("net_sales").cast(DecimalType(38,2)).alias("net_sales")
    ).first()

    if (silver_recon["orders"] != gold_recon["orders"] or
        silver_recon["net_sales"] != gold_recon["net_sales"]):
        raise RuntimeError(
            "GOLD_RECONCILIATION_FAILURE: "
            f"Silver orders={silver_recon['orders']}, Gold orders={gold_recon['orders']}; "
            f"Silver net_sales={silver_recon['net_sales']}, Gold net_sales={gold_recon['net_sales']}"
        )

    audit = spark.createDataFrame([{
        "pipeline_name": "gold_daily_sales",
        "refresh_mode": "FULL",
        "business_rule": "order_status=Completed",
        "silver_completed_orders": int(silver_recon["orders"]),
        "gold_total_orders": int(gold_recon["orders"]),
        "silver_net_sales": str(silver_recon["net_sales"]),
        "gold_net_sales": str(gold_recon["net_sales"]),
        "reconciliation_status": "PASSED",
        "status": "SUCCEEDED"
    }]).withColumn("audit_timestamp", F.current_timestamp())
    audit.write.mode("append").parquet(a["gold_audit_path"])

    completed.unpersist()
    job.commit()

if __name__ == "__main__":
    main()
