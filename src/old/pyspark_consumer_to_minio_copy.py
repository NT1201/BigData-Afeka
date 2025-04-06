from pyspark.sql import SparkSession
from minio import Minio
from kafka import KafkaConsumer
import numpy as np
import json
import uuid
import psycopg2

# 🧠 Helper: Parse feature vector from Spark array
def vector_to_numpy(spark_row):
    return np.array(spark_row["features"])

# 🟢 Spark Session
spark = SparkSession.builder \
    .appName("KafkaToMinIO") \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .getOrCreate()
print("✅ Spark started")
print("📤 Loading ALS factors...")

# 🗂 Paths
user_factors_path = "C:\\als_model_Bigdata\\userFactors"
item_factors_path = "C:\\als_model_Bigdata\\itemFactors"

# 🧠 Load Parquet user/item factors
try:
    user_df = spark.read.parquet(user_factors_path)
    item_df = spark.read.parquet(item_factors_path)
    print("✅ Successfully loaded ALS factors")
except Exception as e:
    print(f"❌ Error loading Parquet files: {str(e)}")
    spark.stop()
    raise

user_factors = {
    int(row["id"]): vector_to_numpy(row)
    for row in user_df.collect()
}
item_factors = {
    int(row["id"]): vector_to_numpy(row)
    for row in item_df.collect()
}

# 📨 Kafka Consumer
consumer = KafkaConsumer(
    'ratings',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
print("📡 Listening to Kafka topic 'ratings'...")

# 🗃️ MinIO Client
minio_client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)
bucket_name = "book-data"
if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)

# 🗄️ PostgreSQL Connection
conn = psycopg2.connect(
    host="localhost",
    database="bookrec",
    user="postgres",
    password="1234"
)
cursor = conn.cursor()

# 🧪 Predict and Save
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
        with open(f"/tmp/{filename}", "w") as f:
            json.dump(result, f)
        minio_client.fput_object(bucket_name, filename, f"/tmp/{filename}")
        print(f"✅ Saved to MinIO: {filename}")

        # Save to PostgreSQL
        cursor.execute("""
            INSERT INTO predictions (user_id, book_id, prediction)
            VALUES (%s, %s, %s)
        """, (user_id, book_id, prediction))
        conn.commit()
        print(f"🗃️ Saved prediction to PostgreSQL for user {user_id}, book {book_id}")
    else:
        print(f"⚠️ Missing factor for user {user_id} or book {book_id}")
