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
# Khi nào batch_id được tạo?
# Spark gom dữ liệu trong 30 giây
# Tạo 1 micro-batch
# Gán cho batch đó 1 batch_id duy nhất
# Gọi hàm foreachBatch
# 📌 Mỗi batch chỉ có 1 batch_id
# 🔹 batch_id dùng để làm gì?
# ✅ 1️⃣ Idempotent (tránh ghi trùng dữ liệu)
# Ví dụ:
# if batch_id <= last_batch_id:
#     return
# 👉 Khi job restart:
# Spark có thể chạy lại batch cũ
# Dùng batch_id để bỏ qua batch đã xử lý
def write_star_schema(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    batch_df = batch_df.withColumn("batch_id", F.lit(batch_id))

    batch_df = batch_df.withColumn(
        "event_time",
        F.to_timestamp("local_time", "yyyy-MM-dd HH:mm:ss")
    )

    # ========= 1-DIM TIME =========
    dim_time = batch_df.select(
        F.date_format("event_time", "yyyyMMddHHmmss").cast("string").alias("event_id"),
        F.col("time_stamp"),
        F.to_timestamp("local_time", "yyyy-MM-dd HH:mm:ss").alias("local_time"),
        F.to_date("event_time").alias("short_date"),
        F.year("event_time").alias("year"),
        F.month("event_time").alias("month"),
        F.dayofmonth("event_time").alias("day"),
        F.hour("event_time").alias("hour")
    ).dropDuplicates(["event_id"])

    dim_time.write \
        .mode("append") \
        .jdbc(pg_url, "dim_time_staging", properties=pg_props)
    conn = spark._jvm.java.sql.DriverManager.getConnection(
        pg_url,
        pg_props["user"],
        pg_props["password"]
    )
    stmt = conn.createStatement()
    stmt.execute("""
               INSERT INTO dim_time (event_id,local_time,time_stamp,short_date,day,month,year,hour)
               SELECT DISTINCT event_id,local_time,time_stamp,short_date,day,month,year,hour FROM dim_time_staging
               ON CONFLICT (event_id) DO NOTHING
               """)

    stmt.execute("TRUNCATE dim_time_staging")
    stmt.close()
    conn.close()
    # ========= 2DIM USER =========
    dim_user = batch_df.select(
        F.col("ip"),
        "email",
        "user_agent"
    ).dropDuplicates(["ip"])

    dim_user.write \
        .mode("append") \
        .jdbc(pg_url, "dim_user_staging", properties=pg_props)
    conn = spark._jvm.java.sql.DriverManager.getConnection(
        pg_url,
        pg_props["user"],
        pg_props["password"]
    )
    stmt = conn.createStatement()
    stmt.execute("""
                   INSERT INTO dim_user (ip,email,user_agent)
                   SELECT DISTINCT ip,email,user_agent FROM dim_user_staging
                   ON CONFLICT (ip) DO NOTHING
                   """)

    stmt.execute("TRUNCATE dim_user_staging")
    stmt.close()
    conn.close()
    # ========= 3DIM DEVICE =========
    dim_device = batch_df.select(
        F.col("device_id"),
    ).dropDuplicates(["device_id"])

    dim_device.write \
        .mode("append") \
        .jdbc(pg_url, "dim_device_staging", properties=pg_props)
    conn = spark._jvm.java.sql.DriverManager.getConnection(
        pg_url,
        pg_props["user"],
        pg_props["password"]
    )
    stmt = conn.createStatement()
    stmt.execute("""
           INSERT INTO dim_device (device_id)
           SELECT DISTINCT device_id FROM dim_device_staging
           ON CONFLICT (device_id) DO NOTHING
           """)

    stmt.execute("TRUNCATE dim_device_staging")
    stmt.close()
    conn.close()

    # ========= 4 DIM PRODUCT =========
    dim_product = batch_df.select(
        F.col("product_id")
       ).dropDuplicates(["product_id"])
    dim_product_clean = dim_product.filter(col("product_id").isNotNull())
    dim_product_clean.write \
        .mode("append") \
        .jdbc(pg_url, "dim_product_staging", properties=pg_props)

    conn = spark._jvm.java.sql.DriverManager.getConnection(
        pg_url,
        pg_props["user"],
        pg_props["password"]
    )
    stmt = conn.createStatement()
    stmt.execute("""
           INSERT INTO dim_product (product_id)
           SELECT DISTINCT product_id FROM dim_product_staging
           ON CONFLICT (product_id) DO NOTHING
           """)

    stmt.execute("TRUNCATE dim_product_staging")
    stmt.close()
    conn.close()

    # ========= 5 DIM STORE =========
    dim_store = batch_df.select("store_id").dropDuplicates(["store_id"])

    dim_store.write \
        .mode("append") \
        .jdbc(pg_url, "dim_store_staging", properties=pg_props)

    conn = spark._jvm.java.sql.DriverManager.getConnection(
        pg_url,
        pg_props["user"],
        pg_props["password"]
    )
    stmt = conn.createStatement()
    stmt.execute("""
       INSERT INTO dim_store (store_id)
       SELECT DISTINCT store_id FROM dim_store_staging
       ON CONFLICT (store_id) DO NOTHING
       """)

    stmt.execute("TRUNCATE dim_store_staging")
    stmt.close()
    conn.close()

    # ========= 6 DIM url =========
    dim_page = batch_df.select(
        F.hash("current_url").alias("url_id"),
        "current_url",
        "referrer_url"
    ).dropDuplicates(["current_url"])

    dim_page.write \
        .mode("append") \
        .jdbc(pg_url, "dim_url_staging", properties=pg_props)
    conn = spark._jvm.java.sql.DriverManager.getConnection(
        pg_url,
        pg_props["user"],
        pg_props["password"]
    )
    stmt = conn.createStatement()
    stmt.execute("""
           INSERT INTO dim_url (url_id,current_url,referrer_url)
           SELECT DISTINCT url_id,current_url,referrer_url FROM dim_url_staging
           ON CONFLICT (url_id) DO NOTHING
           """)

    stmt.execute("TRUNCATE dim_url_staging")
    stmt.close()
    conn.close()

    # ========= 8 FACT =========
    fact = batch_df.select(
        F.col("_id").alias("log_id"),
        F.date_format("local_time", "yyyyMMddHHmmss").cast("string").alias("time_id"),
        F.col("product_id").alias("product_id"),
        F.col("device_id"),
        F.col("store_id").alias("store_id"),
        F.hash("current_url").alias("url_id"),
        F.col("ip").alias("user_id"),
        F.col("collection")
    )
    fact_view_clean = fact.filter(col("product_id").isNotNull())
    fact_view_clean.write \
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
    pg_url = "jdbc:postgresql://192.168.1.26:5432/postgres"
    pg_props = {
        "user": "postgres",
        "password": "UnigapPostgres@123",
        "driver": "org.postgresql.Driver"
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
        .trigger(processingTime="30 seconds") \
        .start()

    query.awaitTermination()