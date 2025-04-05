import os
import shutil
import time
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import col, udf, explode, row_number, concat_ws
from pyspark.sql.types import DoubleType, StringType
from pyspark.ml.feature import Tokenizer, HashingTF, IDF, Normalizer
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import numpy as np

# Paths
ratings_path = "data/ratings.csv/ratings.csv"
books_path = "data/books.csv/books.csv"
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
books_df = spark.read.option("header", "true").option("inferSchema", "true").csv(books_path)


##############################COSINE#######################################
books_df = books_df.withColumn(
    "metadata_text", 
    concat_ws(" ", col("title"), col("authors"))
)
books_df.select("book_id", "title", "authors")
# Tokenize the combined metadata text.
tokenizer = Tokenizer(inputCol="metadata_text", outputCol="words")
wordsData = tokenizer.transform(books_df)

# Build term frequency vectors.
hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=10000)
featurizedData = hashingTF.transform(wordsData)

# Compute TF‑IDF features.
idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurizedData)
rescaledData = idfModel.transform(featurizedData)

# Normalize vectors (so that dot product equals cosine similarity).
normalizer = Normalizer(inputCol="features", outputCol="normFeatures")
books_features = normalizer.transform(rescaledData).select("book_id", "title", "normFeatures")
books_features.show(5, truncate=False)

# UDF to compute cosine similarity.
def cosine_similarity(v1, v2):
    if v1 is None or v2 is None:
        return float(0.0)
    return float(np.dot(v1.toArray(), v2.toArray()))

cosine_similarity_udf = udf(cosine_similarity, DoubleType())

##############################windowSpec#######################################
windowSpec = Window.partitionBy("user_id").orderBy(F.desc("rating"))
favorite_books_df = ratings_df.withColumn("rank", row_number().over(windowSpec)) \
                              .filter(col("rank") == 1) \
                              .select("user_id", col("book_id").alias("favorite_book_id"))

# Join to obtain the favorite book's title.
favorite_books_df = favorite_books_df.join(
    books_df.select("book_id", "title"),
    favorite_books_df.favorite_book_id == books_df.book_id,
    how="left"
).select("user_id", col("title").alias("favorite_book_title"), col("book_id").alias("favorite_book_id"))
favorite_books_df.show(5, truncate=False)

# Join favorite info with books_features to get the favorite book's normalized vector.
favorite_features_df = favorite_books_df.join(
    books_features.withColumnRenamed("normFeatures", "fav_normFeatures"),
    favorite_books_df.favorite_book_id == books_features.book_id,
    how="left"
).select("user_id", "favorite_book_title", "fav_normFeatures")

##############################ALS#######################################
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
all_user_recs = model.recommendForAllUsers(10)
all_user_recs = all_user_recs.select("user_id", explode("recommendations").alias("rec"))
all_user_recs = all_user_recs.select("user_id", col("rec.book_id").alias("rec_book_id"), col("rec.rating").alias("als_score"))
all_user_recs.show(5, truncate=False)

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


##############################Hybrid Score#######################################
def hybrid_score(als_score, content_score, alpha=0.7):
    return alpha * als_score + (1 - alpha) * content_score

hybrid_score_udf = udf(lambda a, c: hybrid_score(a, c), DoubleType())

def generate_explanation(als_score, content_score, fav_title, rec_title, similar_books):
    return (f"We recommend '{rec_title}' because it is similar to your favorite book '{fav_title}' "
            f"(content similarity: {content_score:.2f}) and our collaborative model predicted a rating of {als_score:.2f}. "
            f"Top similar books: {similar_books}")

explanation_udf = udf(generate_explanation, StringType())


###############################RECOMAMENDER####################################
# Collect and broadcast the books_features list.
books_features_list = books_features.select("book_id", "title", "normFeatures").collect()
broadcast_books_features = spark.sparkContext.broadcast(books_features_list)

def top_n_similar_books(fav_vec, n=3):
    if fav_vec is None:
        return "No similar books found (missing favorite book features)"
        
    all_books = broadcast_books_features.value
    sim_list = []
    for row in all_books:
        if row['normFeatures'] is not None:
            score = float(np.dot(fav_vec.toArray(), row['normFeatures'].toArray()))
            sim_list.append((row['book_id'], row['title'], score))
    if len(sim_list) == 0:
        return "No valid book vectors for similarity"
    sim_list = sorted(sim_list, key=lambda x: x[2], reverse=True)
    top_n = sim_list[1:n+1] if len(sim_list) > 1 else sim_list[:n]
    return ", ".join([f"{title} ({score:.2f})" for _, title, score in top_n])

top_n_similar_books_udf = udf(top_n_similar_books, StringType())
# Join ALS recommendations with books_features to get candidate content features.
candidate_recs = all_user_recs.join(
    books_features.select(col("book_id").alias("rec_book_id"), "title", "normFeatures"),
    on="rec_book_id",
    how="inner"
)

# Join with favorite features per user.
candidate_recs = candidate_recs.join(favorite_features_df, on="user_id", how="left")

# Compute content similarity using the favorite book's vector.
candidate_recs = candidate_recs.withColumn("content_score", cosine_similarity_udf(col("fav_normFeatures"), col("normFeatures")))

# Compute the final hybrid score.
candidate_recs = candidate_recs.withColumn("hybrid_score", hybrid_score_udf(col("als_score"), col("content_score")))

# Compute top 3 similar books string using the favorite book's vector.
candidate_recs = candidate_recs.withColumn("similar_books", top_n_similar_books_udf(col("fav_normFeatures")))

# Generate the explanation.
candidate_recs = candidate_recs.withColumn(
    "explanation", 
    explanation_udf(col("als_score"), col("content_score"), col("favorite_book_title"), col("title"), col("similar_books"))
)
# Order final recommendations by hybrid score.
final_recs = candidate_recs.orderBy(col("hybrid_score").desc())
final_recs.select("user_id", "rec_book_id", "hybrid_score", "explanation").show(10, truncate=False)

##############################Cleanup#######################################
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