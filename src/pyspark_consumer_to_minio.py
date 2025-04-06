import os
import json
import uuid
import numpy as np
import time
from kafka import KafkaConsumer
from minio import Minio
import psycopg2

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, row_number, udf, desc
from pyspark.sql.types import DoubleType, StringType
from pyspark.ml.feature import Tokenizer, HashingTF, IDF, Normalizer
from pyspark.sql.window import Window

# Spark session
spark = SparkSession.builder \
    .appName("KafkaToMinIOWithCosine") \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .getOrCreate()
print("✅ Spark started")

# Load ALS factors
user_df = spark.read.parquet("C:/als_model_Bigdata/userFactors")
item_df = spark.read.parquet("C:/als_model_Bigdata/itemFactors")
user_factors = {int(row["id"]): np.array(row["features"]) for row in user_df.collect()}
item_factors = {int(row["id"]): np.array(row["features"]) for row in item_df.collect()}
print("✅ ALS factors loaded")

# Load books metadata and ratings
books_df = spark.read.option("header", True).option("inferSchema", True).csv("C:\\Bigdata\\final\\data\\books.csv\\books.csv")
ratings_df = spark.read.option("header", True).option("inferSchema", True).csv("C:\\Bigdata\\final\\data\\ratings.csv\\ratings.csv")

# Content-based filtering: TF-IDF + cosine
books_df = books_df.withColumn("metadata_text", concat_ws(" ", col("title"), col("authors")))
tokenizer = Tokenizer(inputCol="metadata_text", outputCol="words")
wordsData = tokenizer.transform(books_df)
hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=10000)
featurizedData = hashingTF.transform(wordsData)
idf = IDF(inputCol="rawFeatures", outputCol="features").fit(featurizedData)
tfidf_data = idf.transform(featurizedData)
normalizer = Normalizer(inputCol="features", outputCol="normFeatures")
books_features = normalizer.transform(tfidf_data).select("book_id", "title", "normFeatures")

books_features_list = books_features.collect()
book_features_map = {row["book_id"]: row["normFeatures"] for row in books_features_list}
book_titles_map = {row["book_id"]: row["title"] for row in books_features_list}

# Get favorite book for each user
windowSpec = Window.partitionBy("user_id").orderBy(desc("rating"))
favorite_books_df = ratings_df.withColumn("rank", row_number().over(windowSpec)) \
    .filter(col("rank") == 1) \
    .select("user_id", "book_id")
favorite_books = {row["user_id"]: row["book_id"] for row in favorite_books_df.collect()}
favorite_features = {uid: book_features_map.get(bid) for uid, bid in favorite_books.items()}

# Kafka
consumer = KafkaConsumer(
    'ratings',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
print("📡 Listening to Kafka...")

# MinIO
minio_client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)
bucket_name = "book-data"
if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)

# PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    database="bookrec",
    user="postgres",
    password="1234"
)
cursor = conn.cursor()

# Cosine similarity
def cosine(v1, v2):
    if v1 is None or v2 is None:
        return 0.0
    return float(np.dot(v1.toArray(), v2.toArray()))

# Hybrid score
def hybrid(als_score, cosine_score, alpha=0.7):
    return alpha * als_score + (1 - alpha) * cosine_score

# Loop for predictions
for msg in consumer:
    data = msg.value
    user_id = int(data["user_id"])
    book_id = int(data["book_id"])

    print(f"🔍 Predicting for user {user_id}, book {book_id}...")

    if user_id in user_factors and book_id in item_factors:
        als_score = float(np.dot(user_factors[user_id], item_factors[book_id]))
        cosine_score = cosine(favorite_features.get(user_id), book_features_map.get(book_id))
        final_score = hybrid(als_score, cosine_score)

        explanation = (
            f"We recommend '{book_titles_map.get(book_id, 'Unknown')}' because it's similar to "
            f"your favorite book '{book_titles_map.get(favorite_books.get(user_id), 'Unknown')}'. "
            f"ALS: {als_score:.2f}, Cosine: {cosine_score:.2f}, Hybrid: {final_score:.2f}"
        )

        result = {
            "user_id": user_id,
            "book_id": book_id,
            "als_score": round(als_score, 3),
            "cosine_score": round(cosine_score, 3),
            "hybrid_score": round(final_score, 3),
            "explanation": explanation
        }

        # Save to MinIO
        filename = f"{uuid.uuid4()}.json"
        with open(f"/tmp/{filename}", "w") as f:
            json.dump(result, f)
        minio_client.fput_object(bucket_name, filename, f"/tmp/{filename}")
        print(f"✅ Saved to MinIO: {filename}")

        # Save to PostgreSQL
        cursor.execute("""
            INSERT INTO predictions (user_id, book_id, prediction, explanation)
            VALUES (%s, %s, %s, %s)
        """, (user_id, book_id, final_score, explanation))
        conn.commit()
        print(f"🗃️ Saved prediction to PostgreSQL for user {user_id}, book {book_id}")

    else:
        print(f"⚠️ Missing ALS factors for user {user_id} or book {book_id}")

