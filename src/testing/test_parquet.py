from pyspark.sql import SparkSession
import os

# Set HADOOP_HOME before Spark starts
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = os.environ["PATH"] + ";C:\\hadoop\\bin"

# Create Spark session
spark = SparkSession.builder \
    .appName("TestParquet") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem") \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .config("spark.driver.extraJavaOptions", "-Dhadoop.home.dir=C:\\hadoop") \
    .config("spark.executor.extraJavaOptions", "-Dhadoop.home.dir=C:\\hadoop") \
    .getOrCreate()
print("✅ Spark started")
spark.sparkContext.setLogLevel("INFO")  # More logs for debugging

# Path to test
user_factors_path = "C:\\als_model_Bigdata\\userFactors"

# Load Parquet
try:
    print(f"Attempting to load: {user_factors_path}")
    user_df = spark.read.parquet(user_factors_path)
    print("✅ User factors loaded")
    user_df.show(5)
except Exception as e:
    print(f"❌ Error: {str(e)}")
finally:
    spark.stop()