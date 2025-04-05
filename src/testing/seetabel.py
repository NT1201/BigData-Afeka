from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CheckColumns").getOrCreate()
df = spark.read.option("header", "true").csv("file:///C:/Bigdata/final/models/als_model/user_factors.csv", inferSchema=True)
df.printSchema()
df.show(5)
