import os

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"

os.environ["SPARK_LOCAL_DIRS"] = r"C:\spark-temp"

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, IntegerType, StringType, FloatType


spark = SparkSession.builder \
    .appName("OrderStreamingToPostgres") \
    .master("local[*]") \
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
            "org.postgresql:postgresql:42.7.3"
        ])
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType() \
    .add("order_id", IntegerType()) \
    .add("user_id", IntegerType()) \
    .add("product", StringType()) \
    .add("quantity", IntegerType()) \
    .add("price", FloatType()) \
    .add("timestamp", StringType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders") \
    .load()

parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")


def write_to_postgres(batch_df, batch_id):

    print("=" * 50)
    print("BATCH:", batch_id)
    print("ROWS :", batch_df.count())

    batch_df.show(truncate=False)

    batch_df.write \
        .format("jdbc") \
        .option(
            "url",
            "jdbc:postgresql://localhost:5432/order_stream"
        ) \
        .option("dbtable", "orders") \
        .option("user", "admin") \
        .option("password", "admin123") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    print("Saved to PostgreSQL")


query = parsed.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .option(
        "checkpointLocation",
        "C:/spark-checkpoints/orders"
    ) \
    .start()

query.awaitTermination()