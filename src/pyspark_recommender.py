import os
import shutil
import time
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

# Set HADOOP_HOME and PATH explicitly
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += ";C:\\hadoop\\bin"

# PostgreSQL connection
url = "jdbc:postgresql://localhost:5432/bookrec"
properties = {
    "user": "postgres",
    "password": "1234",
    "driver": "org.postgresql.Driver"
}

jar_path = "file:///C:/Bigdata/final/drivers/postgresql-42.7.1.jar"

# Create Spark session
spark = SparkSession.builder \
    .appName("BookRecommender") \
    .master("local[*]") \
    .config("spark.jars", jar_path) \
    .config("spark.hadoop.hadoop.home.dir", "C:\\hadoop") \
    .config("spark.local.dir", "C:\\Bigdata\\spark-tmp") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
    .config("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true") \
    .config("spark.filesystem.implementation", "org.apache.hadoop.fs.LocalFileSystem") \
    .config("spark.sql.parquet.fs.optimized.committer.optimization-enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("DEBUG")
print(f" Spark version: {spark.version}")
print("Reading ratings from PostgreSQL...")

# Read ratings table
ratings_df = spark.read.jdbc(url=url, table="ratings", properties=properties)

# Train/test split
(training, test) = ratings_df.randomSplit([0.8, 0.2])

# ALS model
als = ALS(
    maxIter=10,
    regParam=0.1,
    userCol="user_id",
    itemCol="book_id",
    ratingCol="rating",
    coldStartStrategy="drop",
    nonnegative=True
)

model = als.fit(training)

# Make predictions
predictions = model.transform(test)

# Evaluate RMSE
evaluator = RegressionEvaluator(
    metricName="rmse",
    labelCol="rating",
    predictionCol="prediction"
)
rmse = evaluator.evaluate(predictions)
print(f" RMSE: {rmse}")

# Show predictions
predictions.select("book_id", "user_id", "rating", "prediction").show(20)

# Save predictions to PostgreSQL
print("Writing recommendations to PostgreSQL...")
predictions.write.jdbc(
    url=url,
    table="recommendations",
    mode="overwrite",
    properties=properties
)

# Save model as CSVs
model_path = "C:/Bigdata/final/models/als_model"
try:
    if os.path.exists(model_path):
        shutil.rmtree(model_path)
        print(f"\ud83d\udca5 Old model directory removed: {model_path}")
    os.makedirs(model_path, exist_ok=True)
    print(f"\u2705 Directory created: {model_path}")

    user_factors_path = os.path.join(model_path, "user_factors.csv")
    item_factors_path = os.path.join(model_path, "item_factors.csv")

    print(f"Writing user factors to: {user_factors_path}")
    model.userFactors.write.option("header", "true").csv(user_factors_path, mode="overwrite")

    print(f" Writing item factors to: {item_factors_path}")
    model.itemFactors.write.option("header", "true").csv(item_factors_path, mode="overwrite")

    print("Model factors saved as CSV successfully!")
except Exception as e:
    print(f"\u274c Error saving CSVs: {str(e)}")

# Finalize
try:
    print(" Done. Recommendations and model saved!")
finally:
    spark.sparkContext.stop()
    time.sleep(10)
    spark.stop()
    temp_dir = "C:\\Bigdata\\spark-tmp"
    if os.path.exists(temp_dir):
        try:
            shutil.rmtree(temp_dir)
            print(f"Cleaned temp directory: {temp_dir}")
        except Exception as e:
            print(f"Couldn't clean temp directory: {e}")
