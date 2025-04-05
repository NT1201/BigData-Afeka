import os
from pyspark.sql import SparkSession

# -----------------------------
# 🔧 Ensure Hadoop is set (needed on Windows)
# -----------------------------
os.environ["HADOOP_HOME"] = "C:/hadoop"

# -----------------------------
# ✅ Start Spark Session with JDBC driver
# -----------------------------
spark = SparkSession.builder \
    .appName("PostgresConnectionTest") \
    .config("spark.jars", "file:///C:/Bigdata/final/drivers/postgresql-42.6.0.jar") \
    .getOrCreate()

print("[OK] Spark Session started!")

# -----------------------------
# 📦 JDBC connection properties
# -----------------------------
url = "jdbc:postgresql://localhost:5432/bookrec"
properties = {
    "user": "postgres",
    "password": "",
    "driver": "org.postgresql.Driver"
}

# -----------------------------
# 📥 Read from 'books' table
# -----------------------------
df = spark.read.jdbc(url=url, table="books", properties=properties)

print("[OK] Data loaded from PostgreSQL!")
df.show(5)

spark.stop()
