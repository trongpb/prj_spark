import os

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, StructType, StructField, LongType, ArrayType, MapType
import util.config as conf
from util.logger import Log4j
# =====================
# FOREACH BATCH
# =====================

def upsert_dim_table(
    df,
    table_name,
    staging_table,
    key_columns,
    insert_columns,
    spark,
    pg_url,
    pg_props
):

    # 1. Deduplicate theo key
    df = df.dropDuplicates(key_columns)

    # 2. Write vào staging
    df.coalesce(4).write \
        .mode("append") \
        .jdbc(pg_url, staging_table, properties=pg_props)

    # 3. Build SQL dynamic
    cols = ",".join(insert_columns)
    keys = ",".join(key_columns)

    merge_sql = f"""
        INSERT INTO {table_name} ({cols})
        SELECT DISTINCT {cols} FROM {staging_table}
        ON CONFLICT ({keys}) DO NOTHING
    """

    truncate_sql = f"TRUNCATE {staging_table}"

    # 4. Execute SQL
    conn = spark._jvm.java.sql.DriverManager.getConnection(
        pg_url,
        pg_props["user"],
        pg_props["password"]
    )
    stmt = conn.createStatement()

    stmt.execute(merge_sql)
    stmt.execute(truncate_sql)

    stmt.close()
    conn.close()

def write_star_schema(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    batch_df = batch_df.withColumn("batch_id", F.lit(batch_id))

    batch_df = batch_df.withColumn(
        "event_time",
        F.to_timestamp("local_time", "yyyy-MM-dd HH:mm:ss")
    )

    # ========= DIM TIME =========
    dim_time = batch_df.select(
        F.date_format("event_time", "yyyyMMddHH").cast("string").alias("event_id"),
        "time_stamp",
        F.col("event_time").alias("local_time"),
        F.to_date("event_time").alias("short_date"),
        F.year("event_time").alias("year"),
        F.month("event_time").alias("month"),
        F.dayofmonth("event_time").alias("day"),
        F.hour("event_time").alias("hour")
    )

    upsert_dim_table(
        dim_time,
        table_name="dim_time",
        staging_table="dim_time_staging",
        key_columns=["event_id"],
        insert_columns=["event_id","local_time","time_stamp","short_date","day","month","year","hour"],
        spark=spark,
        pg_url=pg_url,
        pg_props=pg_props
    )

    # ========= DIM USER =========
    dim_user = batch_df.select("ip", "email", "user_agent")

    upsert_dim_table(
        dim_user,
        "dim_user",
        "dim_user_staging",
        ["ip"],
        ["ip","email","user_agent"],
        spark, pg_url, pg_props
    )

    # ========= DIM DEVICE =========
    dim_device = batch_df.select("device_id")

    upsert_dim_table(
        dim_device,
        "dim_device",
        "dim_device_staging",
        ["device_id"],
        ["device_id"],
        spark, pg_url, pg_props
    )

    # ========= DIM PRODUCT =========
    dim_product = batch_df.select("product_id") \
        .filter(F.col("product_id").isNotNull())

    upsert_dim_table(
        dim_product,
        "dim_product",
        "dim_product_staging",
        ["product_id"],
        ["product_id"],
        spark, pg_url, pg_props
    )

    # ========= DIM STORE =========
    dim_store = batch_df.select("store_id")

    upsert_dim_table(
        dim_store,
        "dim_store",
        "dim_store_staging",
        ["store_id"],
        ["store_id"],
        spark, pg_url, pg_props
    )

    # ========= DIM URL =========
    dim_url = batch_df.select(
        F.hash("current_url").alias("url_id"),
        "current_url",
        "referrer_url"
    )

    upsert_dim_table(
        dim_url,
        "dim_url",
        "dim_url_staging",
        ["url_id"],
        ["url_id","current_url","referrer_url"],
        spark, pg_url, pg_props
    )

    # ========= FACT =========
    fact = batch_df.select(
        F.col("_id").alias("log_id"),
        F.date_format("event_time", "yyyyMMddHH").cast("string").alias("time_id"),
        "product_id",
        "device_id",
        "store_id",
        F.hash("current_url").alias("url_id"),
        F.col("ip").alias("user_id"),
        "collection"
    ).filter(F.col("product_id").isNotNull())

    fact.write \
        .mode("append") \
        .jdbc(pg_url, "fact_view", properties=pg_props)


if __name__ == '__main__':
    base_dir = os.path.dirname(os.path.abspath(__file__))
    print(f"base_dir: {base_dir}")

    conf_path_file = base_dir + "/spark.conf"

    conf = conf.Config(conf_path_file)

    spark_conf = conf.spark_conf
    kaka_conf = conf.kafka_conf

    spark = SparkSession.builder \
        .config(conf=spark_conf) \
        .getOrCreate()

    log = Log4j(spark)

    log.info(f"spark_conf: {spark_conf.getAll()}")
    log.info(f"kafka_conf: {kaka_conf.items()}")

    # =====================
    # POSTGRES CONFIG
    # =====================
    pg_url = "jdbc:postgresql://192.168.1.5:5432/postgres"
    pg_props = {
        "user": "postgres",
        "password": "UnigapPostgres@123",
        "driver": "org.postgresql.Driver",
        "batchsize": "5000",
        "numPartitions": "4"
    }

    # =====================
    # JSON SCHEMA
    # =====================
    json_schema = StructType([
        StructField("_id", StringType()),
        StructField("collection", StringType()),
        StructField("device_id", StringType()),
        StructField("product_id", StringType()),
        StructField("option", StringType()),
        StructField("store_id", StringType()),
        StructField("current_url", StringType()),
        StructField("referrer_url", StringType()),
        StructField("email", StringType()),
        StructField("ip", StringType()),
        StructField("user_agent", StringType()),
        StructField("local_time", StringType()),
        StructField("time_stamp", StringType())
    ])
    df = spark.readStream \
        .format("kafka") \
        .options(**kaka_conf) \
        .load()

    df.printSchema()
    parsed_df = df.select(
        F.from_json(F.col("value").cast("string"), json_schema).alias("v")
    ).select("v.*")

    # =====================
    # START STREAM
    # =====================
    query = parsed_df.writeStream \
        .foreachBatch(write_star_schema) \
        .option("checkpointLocation", "/user/spark/checkpoints/product_view") \
        .trigger(processingTime="30 seconds") \
        .start()

    query.awaitTermination()
