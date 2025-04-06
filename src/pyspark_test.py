# pyspark_test.py
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TestApp").getOrCreate()
print("[✓] Spark Session started successfully!")

# Create sample data
data = [("Alice", 25), ("Bob", 30), ("Cathy", 27)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Show data
df.show()

spark.stop()
