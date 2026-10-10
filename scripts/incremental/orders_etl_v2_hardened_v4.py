import sys
import uuid
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

ORDERS_BOOKMARK_CTX = "orders_incremental_source"
BUSINESS_COLUMNS = [
    "customer_id","order_date","product_id","quantity","unit_price",
    "discount_pct","order_amount","order_status","payment_method"
]
VALID_ORDER_STATUSES = ["Completed","Cancelled","Returned","Pending"]

def args():
    a = getResolvedOptions(sys.argv, [
        "JOB_NAME","environment","database","orders_table","customers_table",
        "products_table","iceberg_catalog","iceberg_warehouse","silver_database",
        "silver_orders_table","quarantine_database","quarantine_orders_table",
        "incremental_audit_path"
    ])

    # Optional, controlled resilience-test switch.
    # Default behavior is normal production processing.
    a["failure_injection"] = "NONE"
    if "--failure_injection" in sys.argv:
        i = sys.argv.index("--failure_injection")
        if i + 1 >= len(sys.argv):
            raise ValueError("--failure_injection requires a value")
        a["failure_injection"] = sys.argv[i + 1].strip().upper()

    allowed = {"NONE", "AFTER_SILVER_MERGE"}
    if a["failure_injection"] not in allowed:
        raise ValueError(
            f"Unsupported --failure_injection={a['failure_injection']}. "
            f"Allowed values: {sorted(allowed)}"
        )
    return a

def normalize(df, run_id):
    return (df
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
        .withColumn("source_updated_at", F.to_timestamp("source_updated_at"))
        .withColumn("source_file", F.lit("incremental_orders_catalog"))
        .withColumn("pipeline_run_id", F.lit(run_id)))

def refs(orders, customers, products):
    c=(customers.select(F.col("customer_id").cast("string").alias("customer_id"))
       .where(F.col("customer_id").isNotNull()).dropDuplicates(["customer_id"])
       .withColumn("_customer_exists",F.lit(True)))
    p=(products.select(F.col("product_id").cast("string").alias("product_id"))
       .where(F.col("product_id").isNotNull()).dropDuplicates(["product_id"])
       .withColumn("_product_exists",F.lit(True)))
    return orders.join(F.broadcast(c),"customer_id","left").join(F.broadcast(p),"product_id","left")

def hash_business(df):
    vals=[F.coalesce(F.col(c).cast("string"),F.lit("<NULL>")) for c in BUSINESS_COLUMNS]
    return df.withColumn("record_hash",F.sha2(F.concat_ws("||",*vals),256))

def dq(df):
    rules=[
      ("DQ_001","order_id_not_null",F.col("order_id").isNull()),
      ("DQ_002","customer_id_not_null",F.col("customer_id").isNull()),
      ("DQ_003","product_id_not_null",F.col("product_id").isNull()),
      ("DQ_004","order_date_valid",F.col("order_date").isNull()),
      ("DQ_005","order_date_not_future",F.col("order_date")>F.current_date()),
      ("DQ_006","quantity_positive",F.col("quantity").isNull()|(F.col("quantity")<=0)),
      ("DQ_007","unit_price_non_negative",F.col("unit_price").isNull()|(F.col("unit_price")<0)),
      ("DQ_008","discount_percentage_valid",F.col("discount_pct").isNull()|(F.col("discount_pct")<0)|(F.col("discount_pct")>100)),
      ("DQ_009","order_amount_positive",F.col("order_amount").isNull()|(F.col("order_amount")<=0)),
      ("DQ_010","order_status_valid",F.col("order_status").isNull()|(~F.col("order_status").isin(VALID_ORDER_STATUSES))),
      ("DQ_011","customer_reference_exists",F.col("_customer_exists").isNull()),
      ("DQ_012","product_reference_exists",F.col("_product_exists").isNull()),
      ("DQ_013","source_updated_at_present",F.col("source_updated_at").isNull())
    ]
    ids=[F.when(cond,F.lit(i)) for i,_,cond in rules]
    reasons=[F.when(cond,F.lit(n)) for _,n,cond in rules]
    return (df.withColumn("dq_rule_ids",F.array_compact(F.array(*ids)))
              .withColumn("dq_error_reasons",F.array_compact(F.array(*reasons)))
              .withColumn("dq_is_valid",F.size("dq_rule_ids")==0))

def resolve_batch(df):
    vc=(df.groupBy("order_id","source_updated_at")
          .agg(F.countDistinct("record_hash").alias("_hashes")))
    x=df.join(vc,["order_id","source_updated_at"],"left")
    conflicts=x.filter(F.col("_hashes")>1).drop("_hashes")
    candidates=x.filter(F.col("_hashes")==1)
    w=Window.partitionBy("order_id").orderBy(F.col("source_updated_at").desc(),F.col("record_hash").desc())
    ranked=candidates.withColumn("_rank",F.row_number().over(w))
    winners=ranked.filter(F.col("_rank")==1).drop("_rank","_hashes")
    superseded=ranked.filter(F.col("_rank")>1).drop("_rank","_hashes")
    return winners,conflicts,superseded

def classify(incoming,silver):
    t=silver.select(F.col("order_id").alias("_tid"),
                    F.col("source_updated_at").alias("_tver"),
                    F.col("record_hash").alias("_thash"))
    return (incoming.join(t,incoming.order_id==t._tid,"left")
      .withColumn("change_type",
        F.when(F.col("_tid").isNull(),"INSERT")
         .when((F.col("source_updated_at")==F.col("_tver"))&(F.col("record_hash")==F.col("_thash")),"DUPLICATE")
         .when(F.col("source_updated_at")>F.col("_tver"),"UPDATE")
         .when(F.col("source_updated_at")<F.col("_tver"),"STALE")
         .otherwise("CONFLICT")))

def ensure_quarantine(spark,fqn,location):
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {fqn} (
      quarantine_key STRING NOT NULL, order_id STRING, customer_id STRING, order_date DATE,
      product_id STRING, quantity INT, unit_price DECIMAL(18,2), discount_pct DECIMAL(7,4),
      order_amount DECIMAL(18,2), order_status STRING, payment_method STRING,
      source_updated_at TIMESTAMP, record_hash STRING, source_file STRING,
      quarantine_category STRING, dq_rule_ids ARRAY<STRING>, dq_error_reasons ARRAY<STRING>,
      first_quarantined_at TIMESTAMP, last_seen_at TIMESTAMP,
      first_pipeline_run_id STRING, last_pipeline_run_id STRING)
      USING iceberg PARTITIONED BY (months(order_date)) LOCATION '{location}'
      TBLPROPERTIES ('format-version'='2')""")

def qprep(df,category):
    # source_file is intentionally excluded from the key: retry/replay identity is
    # business-version-content-reason, not execution attempt.
    return (df.withColumn("quarantine_category",F.lit(category))
      .withColumn("quarantine_key",F.sha2(F.concat_ws("||",
        F.coalesce(F.col("order_id"),F.lit("<NULL>")),
        F.coalesce(F.col("source_updated_at").cast("string"),F.lit("<NULL>")),
        F.coalesce(F.col("record_hash"),F.lit("<NULL>")),
        F.lit(category),
        F.coalesce(F.concat_ws(",",F.sort_array(F.col("dq_rule_ids"))),F.lit(""))
      ),256)))

def qmerge(spark,fqn,df):
    cols=["quarantine_key","order_id","customer_id","order_date","product_id","quantity",
          "unit_price","discount_pct","order_amount","order_status","payment_method",
          "source_updated_at","record_hash","source_file","quarantine_category",
          "dq_rule_ids","dq_error_reasons","pipeline_run_id"]
    df.select(*cols).createOrReplaceTempView("incoming_quarantine")
    spark.sql(f"""MERGE INTO {fqn} t USING incoming_quarantine s
      ON t.quarantine_key=s.quarantine_key
      WHEN MATCHED THEN UPDATE SET last_seen_at=current_timestamp(),
        last_pipeline_run_id=s.pipeline_run_id
      WHEN NOT MATCHED THEN INSERT (
        quarantine_key,order_id,customer_id,order_date,product_id,quantity,unit_price,
        discount_pct,order_amount,order_status,payment_method,source_updated_at,record_hash,
        source_file,quarantine_category,dq_rule_ids,dq_error_reasons,first_quarantined_at,
        last_seen_at,first_pipeline_run_id,last_pipeline_run_id)
      VALUES (s.quarantine_key,s.order_id,s.customer_id,s.order_date,s.product_id,s.quantity,
        s.unit_price,s.discount_pct,s.order_amount,s.order_status,s.payment_method,
        s.source_updated_at,s.record_hash,s.source_file,s.quarantine_category,s.dq_rule_ids,
        s.dq_error_reasons,current_timestamp(),current_timestamp(),s.pipeline_run_id,s.pipeline_run_id)""")

def silver_merge(spark,fqn,df):
    cols=["order_id","customer_id","order_date","product_id","quantity","unit_price",
          "discount_pct","order_amount","order_status","payment_method","source_updated_at",
          "record_hash","source_file","pipeline_run_id"]
    df.filter(F.col("change_type").isin("INSERT","UPDATE")).select(*cols).createOrReplaceTempView("incoming_silver")
    spark.sql(f"""MERGE INTO {fqn} t USING incoming_silver s ON t.order_id=s.order_id
      WHEN MATCHED AND s.source_updated_at>t.source_updated_at AND s.record_hash<>t.record_hash
      THEN UPDATE SET customer_id=s.customer_id,order_date=s.order_date,product_id=s.product_id,
        quantity=s.quantity,unit_price=s.unit_price,discount_pct=s.discount_pct,
        order_amount=s.order_amount,order_status=s.order_status,payment_method=s.payment_method,
        source_updated_at=s.source_updated_at,record_hash=s.record_hash,source_file=s.source_file,
        last_updated_at=current_timestamp(),pipeline_run_id=s.pipeline_run_id
      WHEN NOT MATCHED THEN INSERT (order_id,customer_id,order_date,product_id,quantity,
        unit_price,discount_pct,order_amount,order_status,payment_method,source_updated_at,
        record_hash,source_file,first_ingested_at,last_updated_at,pipeline_run_id)
      VALUES (s.order_id,s.customer_id,s.order_date,s.product_id,s.quantity,s.unit_price,
        s.discount_pct,s.order_amount,s.order_status,s.payment_method,s.source_updated_at,
        s.record_hash,s.source_file,current_timestamp(),current_timestamp(),s.pipeline_run_id)""")

def main():
    a=args()
    sc=SparkContext.getOrCreate(); glue=GlueContext(sc); spark=glue.spark_session
    job=Job(glue); job.init(a["JOB_NAME"],a)
    run_id=str(uuid.uuid4())

    silver_fqn=f'{a["iceberg_catalog"]}.{a["silver_database"]}.{a["silver_orders_table"]}'
    q_fqn=f'{a["iceberg_catalog"]}.{a["quarantine_database"]}.{a["quarantine_orders_table"]}'
    q_location=f'{a["iceberg_warehouse"].rstrip("/")}/{a["quarantine_database"]}/{a["quarantine_orders_table"]}/'

    orders_dyf=glue.create_dynamic_frame.from_catalog(
        database=a["database"],table_name=a["orders_table"],
        transformation_ctx=ORDERS_BOOKMARK_CTX)

    # A zero-row bookmarked read is a normal operational state. Exit cleanly
    # before downstream transformations reference source columns.
    if orders_dyf.count() == 0:
        audit=spark.createDataFrame([{
          "pipeline_run_id":run_id,"pipeline_mode":"INCREMENTAL",
          "input_record_count":0,"dq_rejected_count":0,
          "batch_conflict_count":0,"superseded_in_batch_count":0,
          "insert_count":0,"update_count":0,"duplicate_count":0,
          "stale_count":0,"target_conflict_count":0,"status":"NO_NEW_DATA"
        }]).withColumn("audit_timestamp",F.current_timestamp())
        audit.write.mode("append").parquet(a["incremental_audit_path"])
        job.commit()
        return

    orders=orders_dyf.toDF()
    customers=glue.create_dynamic_frame.from_catalog(database=a["database"],table_name=a["customers_table"]).toDF()
    products=glue.create_dynamic_frame.from_catalog(database=a["database"],table_name=a["products_table"]).toDF()

    validated=dq(hash_business(refs(normalize(orders,run_id),customers,products))).persist()
    input_count=validated.count()
    bad=validated.filter(~F.col("dq_is_valid")); good=validated.filter(F.col("dq_is_valid"))
    ensure_quarantine(spark,q_fqn,q_location)

    dq_count=bad.count()
    if dq_count: qmerge(spark,q_fqn,qprep(bad,"DATA_QUALITY"))

    winners,batch_conflicts,superseded=resolve_batch(good)
    bc=batch_conflicts.count()
    if bc: qmerge(spark,q_fqn,qprep(batch_conflicts,"VERSION_CONFLICT"))

    classified=classify(winners,spark.table(silver_fqn)).persist()
    tc=classified.filter(F.col("change_type")=="CONFLICT")
    tc_count=tc.count()
    if tc_count: qmerge(spark,q_fqn,qprep(tc,"TARGET_VERSION_CONFLICT"))

    counts={r["change_type"]:r["count"] for r in classified.groupBy("change_type").count().collect()}
    silver_merge(spark,silver_fqn,classified)

    # Resilience test boundary:
    # Silver Iceberg transaction is committed, while Glue job.commit() has not
    # executed yet. A forced failure here leaves the source bookmark unadvanced.
    if a["failure_injection"] == "AFTER_SILVER_MERGE":
        raise RuntimeError(
            "CONTROLLED_FAILURE_AFTER_SILVER_MERGE: "
            "intentional test failure before Glue job.commit()."
        )

    audit=spark.createDataFrame([{
      "pipeline_run_id":run_id,"pipeline_mode":"INCREMENTAL",
      "input_record_count":input_count,"dq_rejected_count":dq_count,
      "batch_conflict_count":bc,"superseded_in_batch_count":superseded.count(),
      "insert_count":counts.get("INSERT",0),"update_count":counts.get("UPDATE",0),
      "duplicate_count":counts.get("DUPLICATE",0),"stale_count":counts.get("STALE",0),
      "target_conflict_count":counts.get("CONFLICT",0),"status":"SUCCEEDED"
    }]).withColumn("audit_timestamp",F.current_timestamp())
    audit.write.mode("append").parquet(a["incremental_audit_path"])

    classified.unpersist(); validated.unpersist()
    job.commit()

if __name__=="__main__":
    main()
