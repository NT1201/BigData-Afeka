import os
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel

# Path to the saved model
model_path = "model/als_model"

# Initialize Spark session
spark = SparkSession.builder \
    .appName("LoadALSModel") \
    .master("local[*]") \
    .getOrCreate()

# Load saved ALS model
print(f"📦 Loading ALS model from: {model_path}")
model = ALSModel.load(model_path)
print("✅ Model loaded!")

# Example: Create new data for prediction
# Replace this with real user_id and book_id pairs as needed
new_data = spark.createDataFrame([
    (1, 101),   # user_id, book_id
    (2, 102),
    (3, 103),
], ["user_id", "book_id"])

# Make predictions
predictions = model.transform(new_data)

print("🔮 Predictions:")
predictions.show()

# Stop Spark
spark.stop()