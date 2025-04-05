import os
import shutil
import time
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

# Paths
ratings_path = "data/ratings.csv/ratings.csv"
model_save_path = "model/als_model"

# Clean old model directory
if os.path.exists(model_save_path):
    shutil.rmtree(model_save_path)
    print(f"💥 Old model directory removed: {model_save_path}")

# Create Spark session
spark = SparkSession.builder \
    .appName("LocalBookRecommender") \
    .master("local[*]") \
    .config("spark.local.dir", "spark-tmp") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print(f"✅ Spark version: {spark.version}")
print("📥 Reading ratings from CSV...")

# Load ratings data from CSV
ratings_df = spark.read.option("header", "true").option("inferSchema", "true").csv(ratings_path)

# Split data
(training, test) = ratings_df.randomSplit([0.8, 0.2], seed=42)

# ALS Model
als = ALS(
    maxIter=10,
    regParam=0.1,
    userCol="user_id",
    itemCol="book_id",
    ratingCol="rating",
    coldStartStrategy="drop",
    nonnegative=True
)

print("🧠 Training ALS model...")
model = als.fit(training)

# Make predictions
predictions = model.transform(test)

# Evaluate model
evaluator = RegressionEvaluator(
    metricName="rmse",
    labelCol="rating",
    predictionCol="prediction"
)
rmse = evaluator.evaluate(predictions)
print(f"📉 RMSE: {rmse:.4f}")

# Show predictions
print("🔍 Sample predictions:")
predictions.select("book_id", "user_id", "rating", "prediction").show(10)

# Save model
try:
    print(f"💾 Saving ALS model to: {model_save_path}")
    model.save(model_save_path)
    print("✅ Model saved successfully!")
except Exception as e:
    print(f"❌ Error saving model: {e}")

# Cleanup
try:
    print("🏁 Done. ALS model created and saved.")
finally:
    spark.stop()
    time.sleep(5)
    temp_dir = "spark-tmp"
    if os.path.exists(temp_dir):
        try:
            shutil.rmtree(temp_dir)
            print(f"🧹 Cleaned temp directory: {temp_dir}")
        except Exception as e:
            print(f"⚠️ Couldn't clean temp directory: {e}")
