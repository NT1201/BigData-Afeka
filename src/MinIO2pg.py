from minio import Minio
import pandas as pd
from io import BytesIO
from sqlalchemy import create_engine

print("🚀 Starting script...")

# 1. Connect to MinIO
print("🔗 Connecting to MinIO...")

minio_client = Minio(
    "127.0.0.1:9000",             # Use IP explicitly instead of "localhost"
    access_key="minioadmin",     # Must match what your terminal says
    secret_key="minioadmin",
    secure=False
)



# 2. Read CSVs from MinIO
bucket_name = "book-data"

def read_csv_from_minio(filename):
    print(f"📦 Downloading {filename} from MinIO...")
    obj = minio_client.get_object(bucket_name, filename)
    return pd.read_csv(BytesIO(obj.read()))

books_df = read_csv_from_minio("books.csv")
print("✅ books.csv loaded:", books_df.shape)

ratings_df = read_csv_from_minio("ratings.csv")
print("✅ ratings.csv loaded:", ratings_df.shape)

# 3. Connect to PostgreSQL (trust mode)
print("🔗 Connecting to PostgreSQL...")
engine = create_engine("postgresql://postgres@localhost:5432/bookrec")

# 4. Upload to PostgreSQL
print("⬆️ Uploading books...")
books_df.to_sql("books", engine, index=False, if_exists="replace")

print("⬆️ Uploading ratings...")
ratings_df.to_sql("ratings", engine, index=False, if_exists="replace")

print("🎉 All done! Data is now in PostgreSQL.")
