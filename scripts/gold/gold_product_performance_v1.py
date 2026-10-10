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
    "products_table",
    "silver_database",
    "silver_orders_table",
    "gold_database",
    "gold_product_performance_table",
    "gold_product_performance_location",
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
    gold_fqn = f"{catalog}.{a['gold_database']}.{a['gold_product_performance_table']}"

    # Product master is a crawler-created Glue external table, not Iceberg.
    products = (
        glue.create_dynamic_frame.from_catalog(
            database=a["source_database"],
            table_name=a["products_table"],
            transformation_ctx="products_snapshot_source"
        ).toDF()
        .select(
            F.trim(F.col("product_id")).alias("product_id"),
            F.col("product_name"),
            F.col("category"),
            F.col("subcategory"),
            F.col("unit_price").cast(DecimalType(18,2)).alias("catalog_unit_price"),
            F.col("cost").cast(DecimalType(18,2)).alias("unit_cost"),
            F.to_date("launch_date").alias("launch_date"),
        )
        .cache()
    )

    product_count = products.count()
    null_product_ids = products.filter(
        F.col("product_id").isNull() | (F.col("product_id") == "")
    ).count()
    duplicate_product_ids = (
        products.groupBy("product_id").count()
        .filter(F.col("product_id").isNotNull() & (F.col("count") > 1))
        .count()
    )

    if null_product_ids or duplicate_product_ids:
        raise RuntimeError(
            "PRODUCT_MASTER_CONTRACT_FAILURE: "
            f"null_or_blank_product_ids={null_product_ids}, "
            f"duplicate_product_ids={duplicate_product_ids}"
        )

    completed = (
        spark.table(silver_fqn)
        .filter(F.col("order_status") == F.lit("Completed"))
        .select(
            "product_id", "order_id", "customer_id",
            F.to_date("order_date").alias("order_date"),
            F.col("quantity").cast("long").alias("quantity"),
            F.col("unit_price").cast(DecimalType(18,2)).alias("order_unit_price"),
            F.col("order_amount").cast(DecimalType(18,2)).alias("order_amount"),
        )
        .cache()
    )

    bad_order_dates = completed.filter(F.col("order_date").isNull()).count()
    if bad_order_dates:
        raise RuntimeError(
            f"GOLD_CONTRACT_FAILURE: {bad_order_dates} Completed orders have invalid order_date"
        )

    performance = (
        completed
        .withColumn(
            "gross_line_amount",
            (F.col("quantity") * F.col("order_unit_price")).cast(DecimalType(38,2))
        )
        .groupBy("product_id")
        .agg(
            F.min("order_date").alias("first_sale_date"),
            F.max("order_date").alias("last_sale_date"),
            F.countDistinct("order_id").cast("long").alias("lifetime_orders"),
            F.countDistinct("customer_id").cast("long").alias("unique_customers"),
            F.sum("quantity").cast("long").alias("units_sold"),
            F.sum("gross_line_amount").cast(DecimalType(38,2)).alias("gross_sales"),
            F.sum("order_amount").cast(DecimalType(38,2)).alias("net_sales"),
        )
        .withColumn(
            "discount_amount",
            (F.col("gross_sales") - F.col("net_sales")).cast(DecimalType(38,2))
        )
        .withColumn(
            "average_selling_price",
            F.when(
                F.col("units_sold") > 0,
                (F.col("net_sales") / F.col("units_sold")).cast(DecimalType(38,2))
            )
        )
    )

    zero_money = F.lit(0).cast(DecimalType(38,2))

    gold = (
        products.alias("p")
        .join(performance.alias("s"), on="product_id", how="left")
        .select(
            "product_id",
            "product_name",
            "category",
            "subcategory",
            "catalog_unit_price",
            "unit_cost",
            "launch_date",
            "first_sale_date",
            "last_sale_date",
            F.coalesce(F.col("lifetime_orders"), F.lit(0)).cast("long").alias("lifetime_orders"),
            F.coalesce(F.col("unique_customers"), F.lit(0)).cast("long").alias("unique_customers"),
            F.coalesce(F.col("units_sold"), F.lit(0)).cast("long").alias("units_sold"),
            F.coalesce(F.col("gross_sales"), zero_money).cast(DecimalType(38,2)).alias("gross_sales"),
            F.coalesce(F.col("discount_amount"), zero_money).cast(DecimalType(38,2)).alias("discount_amount"),
            F.coalesce(F.col("net_sales"), zero_money).cast(DecimalType(38,2)).alias("net_sales"),
            F.coalesce(F.col("average_selling_price"), zero_money)
             .cast(DecimalType(38,2)).alias("average_selling_price"),
        )
        .withColumn(
            "estimated_cogs",
            (F.col("units_sold") * F.col("unit_cost")).cast(DecimalType(38,2))
        )
        .withColumn(
            "estimated_gross_margin",
            (F.col("net_sales") - F.col("estimated_cogs")).cast(DecimalType(38,2))
        )
        .withColumn(
            "estimated_gross_margin_pct",
            F.when(
                F.col("net_sales") > 0,
                ((F.col("estimated_gross_margin") / F.col("net_sales")) * F.lit(100))
                .cast(DecimalType(18,2))
            ).otherwise(F.lit(0).cast(DecimalType(18,2)))
        )
        .withColumn(
            "product_sales_status",
            F.when(F.col("lifetime_orders") > 0, F.lit("SOLD"))
             .otherwise(F.lit("NEVER_SOLD"))
        )
        .withColumn("last_refreshed_at", F.current_timestamp())
    )

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {gold_fqn} (
            product_id STRING,
            product_name STRING,
            category STRING,
            subcategory STRING,
            catalog_unit_price DECIMAL(18,2),
            unit_cost DECIMAL(18,2),
            launch_date DATE,
            first_sale_date DATE,
            last_sale_date DATE,
            lifetime_orders BIGINT,
            unique_customers BIGINT,
            units_sold BIGINT,
            gross_sales DECIMAL(38,2),
            discount_amount DECIMAL(38,2),
            net_sales DECIMAL(38,2),
            average_selling_price DECIMAL(38,2),
            estimated_cogs DECIMAL(38,2),
            estimated_gross_margin DECIMAL(38,2),
            estimated_gross_margin_pct DECIMAL(18,2),
            product_sales_status STRING,
            last_refreshed_at TIMESTAMP
        )
        USING iceberg
        LOCATION '{a["gold_product_performance_location"]}'
        TBLPROPERTIES (
            'format-version'='2',
            'write.parquet.compression-codec'='snappy'
        )
    """)

    # Current-state Gold V1: deterministic full refresh, not append.
    gold.createOrReplaceTempView("gold_product_performance_stage")
    spark.sql(f"""
        INSERT OVERWRITE {gold_fqn}
        SELECT
            product_id, product_name, category, subcategory,
            catalog_unit_price, unit_cost, launch_date,
            first_sale_date, last_sale_date,
            lifetime_orders, unique_customers, units_sold,
            gross_sales, discount_amount, net_sales,
            average_selling_price, estimated_cogs,
            estimated_gross_margin, estimated_gross_margin_pct,
            product_sales_status, last_refreshed_at
        FROM gold_product_performance_stage
    """)

    silver_recon = completed.agg(
        F.countDistinct("order_id").alias("orders"),
        F.sum("order_amount").cast(DecimalType(38,2)).alias("net_sales")
    ).first()

    published = spark.table(gold_fqn).cache()
    gold_recon = published.agg(
        F.count("*").alias("products"),
        F.countDistinct("product_id").alias("distinct_products"),
        F.sum("lifetime_orders").alias("orders"),
        F.sum("net_sales").cast(DecimalType(38,2)).alias("net_sales"),
    ).first()

    if gold_recon["products"] != product_count:
        raise RuntimeError(
            f"GOLD_PRODUCT_COUNT_FAILURE: master={product_count}, gold={gold_recon['products']}"
        )

    if gold_recon["products"] != gold_recon["distinct_products"]:
        raise RuntimeError(
            "GOLD_PRODUCT_UNIQUENESS_FAILURE: "
            f"rows={gold_recon['products']}, distinct_product_id={gold_recon['distinct_products']}"
        )

    if (silver_recon["orders"] != gold_recon["orders"] or
            silver_recon["net_sales"] != gold_recon["net_sales"]):
        raise RuntimeError(
            "GOLD_RECONCILIATION_FAILURE: "
            f"Silver orders={silver_recon['orders']}, Gold orders={gold_recon['orders']}; "
            f"Silver net_sales={silver_recon['net_sales']}, Gold net_sales={gold_recon['net_sales']}"
        )

    sales_status = {
        r["product_sales_status"]: int(r["count"])
        for r in published.groupBy("product_sales_status").count().collect()
    }

    audit = spark.createDataFrame([{
        "pipeline_name": "gold_product_performance",
        "refresh_mode": "FULL",
        "business_rule": "financial performance uses order_status=Completed; estimated margin uses current product master cost",
        "master_product_count": int(product_count),
        "gold_product_count": int(gold_recon["products"]),
        "gold_distinct_product_count": int(gold_recon["distinct_products"]),
        "silver_completed_orders": int(silver_recon["orders"]),
        "gold_lifetime_orders": int(gold_recon["orders"]),
        "silver_net_sales": str(silver_recon["net_sales"]),
        "gold_net_sales": str(gold_recon["net_sales"]),
        "sold_products": sales_status.get("SOLD", 0),
        "never_sold_products": sales_status.get("NEVER_SOLD", 0),
        "reconciliation_status": "PASSED",
        "status": "SUCCEEDED",
    }]).withColumn("audit_timestamp", F.current_timestamp())

    audit.write.mode("append").parquet(a["gold_audit_path"])

    published.unpersist()
    completed.unpersist()
    products.unpersist()
    job.commit()

if __name__ == "__main__":
    main()
