from pyspark.sql import SparkSession

# Update path to where you downloaded the driver
JDBC_JAR = "C:/Bigdata/drivers/postgresql-42.6.0.jar"

spark = SparkSession.builder \
    .appName("PostgresConnectionTest") \
    .config("spark.jars", JDBC_JAR) \
    .getOrCreate()

print("[✓] Spark Session started!")

# PostgreSQL connection properties
url = "jdbc:postgresql://localhost:5432/bookrec"
properties = {
    "user": "postgres",
    "password": "",  # No password
    "driver": "org.postgresql.Driver"
}

# Try reading a sample table, e.g., "books"
df = spark.read.jdbc(url=url, table="books", properties=properties)

df.show(5)  # Just show a few rows

spark.stop()
