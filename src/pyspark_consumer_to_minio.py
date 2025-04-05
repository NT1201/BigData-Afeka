from pyspark.sql import SparkSession
from kafka import KafkaConsumer
from minio import Minio
import json
import numpy as np
import uuid
import psycopg2
import os

# Start Spark
spark = SparkSession.builder \
    .appName("KafkaToMinIOAndPostgres") \
    .getOrCreate()
print("✅ Spark started")

# Load ALS factor files
print("📤 Loading ALS factors...")
user_factors_path = "file:///C:/Bigdata/final/models/als_model/user_factors.csv"
item_factors_path = "file:///C:/Bigdata/final/models/als_model/item_factors.csv"

user_df = spark.read.option("header", "true").csv(user_factors_path)
item_df = spark.read.option("header", "true").csv(item_factors_path)

# Convert string features column to numpy array
def parse_features(feature_str):
    return np.array(json.loads(feature_str))


user_factors = {
    int(row["id"]): parse_features(row["features"])
    for row in user_df.collect()
}
item_factors = {
    int(row["id"]): parse_features(row["features"])
    for row in item_df.collect()
}

# Kafka consumer
consumer = KafkaConsumer(
    'ratings',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
print("📥 Listening to Kafka topic 'ratings'...")

# MinIO setup
minio_client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)
bucket_name = "book-data"

if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)

# PostgreSQL setup
conn = psycopg2.connect(
    dbname="bookrec",
    user="postgres",
    password="1234",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS predictions (
    id SERIAL PRIMARY KEY,
    user_id INT,
    book_id INT,
    prediction FLOAT
)
""")
conn.commit()

# Start consuming and predicting
for msg in consumer:
    data = msg.value
    user_id = int(data["user_id"])
    book_id = int(data["book_id"])

    print(f"🔍 Predicting for user {user_id}, book {book_id}...")

    if user_id in user_factors and book_id in item_factors:
        prediction = float(np.dot(user_factors[user_id], item_factors[book_id]))
        result = {
            "user_id": user_id,
            "book_id": book_id,
            "prediction": round(prediction, 3)
        }

        # Save to MinIO
        filename = f"{uuid.uuid4()}.json"
        filepath = f"/tmp/{filename}"
        with open(filepath, "w") as f:
            json.dump(result, f)

        minio_client.fput_object(bucket_name, filename, filepath)
        print(f"✅ Prediction saved to MinIO as {filename}")

        # Save to PostgreSQL
        cursor.execute(
            "INSERT INTO predictions (user_id, book_id, prediction) VALUES (%s, %s, %s)",
            (user_id, book_id, prediction)
        )
        conn.commit()
        print(f"🗃️ Saved prediction to PostgreSQL for user {user_id}, book {book_id}")
        
        # Clean up the temporary file
        os.remove(filepath)
    else:
        print(f"⚠️ Missing factors for user {user_id} or book {book_id}")
