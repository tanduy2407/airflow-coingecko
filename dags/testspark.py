from pyspark.sql import SparkSession
import os

spark = (
    SparkSession.builder.appName("airflow-coingecko").getOrCreate()
)

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/coingecko") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "historical_data") \
    .option("user", "root") \
    .option("password", "tanduy2407") \
    .load()

df.show()