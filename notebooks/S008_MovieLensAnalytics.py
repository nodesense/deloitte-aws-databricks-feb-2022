# Databricks notebook source
from pyspark.sql.types import StructType, LongType,StringType, IntegerType, DoubleType


# create dataframe schema manually, best practices
 
movieSchema = StructType()\
         .add("movieId", IntegerType(), True)\
         .add("title", StringType(), True)\
         .add("genres", StringType(), True)\
 
 
ratingSchema = StructType()\
         .add("userId", IntegerType(), True)\
         .add("movieId", IntegerType(), True)\
         .add("rating", DoubleType(), True)\
         .add("timestamp", StringType(), True)


# COMMAND ----------

# inferschema, spark scan records and build schema with data types, expensive
# movieDf = spark.read.format("csv")\
#           .option("header", True)\
#           .option("inferSchema", True)\
#           .load("/FileStore/tables/feb14-02-2022/movies.csv")
# movieDf.printSchema()

# COMMAND ----------

# if we use directory instead of single file with extention, spark will load all the files in directory
movieDf = spark.read.format("csv")\
          .option("header", True)\
          .schema(movieSchema)\
          .load("/mnt/aws/raw/movies")
 
ratingDf = spark.read.format("csv")\
          .option("header", True)\
          .schema(ratingSchema)\
          .load("/mnt/aws/raw/ratings")

# COMMAND ----------

print(movieDf.count())
print(ratingDf.count())

# COMMAND ----------



# COMMAND ----------

# column values are displayed in full
movieDf.show(truncate = False) # 20 records

# COMMAND ----------

ratingDf.show(2)

# COMMAND ----------

# to get all columns
print("Columns", ratingDf.columns)
# to get schema associated with dataframe
print("Schema ", ratingDf.schema)

# COMMAND ----------

# add new columns/drive new columns from existing data
df3 = ratingDf.where("rating < 2").withColumn("rating_adjusted", ratingDf.rating + .2  )
df3.printSchema()
df3.show(2)
print("derived ", df3.count())


# COMMAND ----------

df2 = ratingDf.withColumnRenamed("rating", "ratings")
df2.printSchema()
df2.show(2)

# COMMAND ----------

df2 = ratingDf.select(ratingDf.userId, 
                     (ratingDf.rating + 0.2).alias("rating_adjusted") )
df2.show(1)

# COMMAND ----------

# filter with and conditions
df2 = ratingDf.filter( (ratingDf.rating >=3) & (ratingDf.rating <=4))
df2.show(4)


# COMMAND ----------

from pyspark.sql.functions import col, asc, desc
# sort data by ascending order/ default
df2 = ratingDf.sort("rating")
df2.show(5)
# sort data by ascending by explitly
df2 = ratingDf.sort(asc("rating"))
df2.show(5)
# sort data by descending order
df2 = ratingDf.sort(desc("rating"))
df2.show(5)

# COMMAND ----------

# aggregation count
from pyspark.sql.functions import col, desc, avg, count
# count, groupBy
# a movie, rated by more users, dones't count avg rating
# filter, ensure that total_ratings >= 100 users
mostPopularDf = ratingDf\
                .groupBy("movieId")\
                .agg(count("userId"))\
                .withColumnRenamed("count(userId)", "total_ratings")\
                .sort(desc("total_ratings"))\
                .filter(col("total_ratings") >= 100)\
                
mostPopularDf.createOrReplaceTempView("popular_movies")
# temporary result, which may be reusable in many places in same application
mostPopularDf.cache()
  
# logical, physical plan
mostPopularDf.explain(extended = True)
print ("--------")
# print phisical plans
mostPopularDf.explain()
#mostPopularDf.show(200)

# COMMAND ----------

df2 = spark.sql("select movieId, total_ratings from popular_movies where total_ratings > 100")
df2.show(2)
 
df2.explain(extended = True)

# COMMAND ----------

mostPopularDf = mostPopularDf.coalesce(1)
mostPopularDf.cache()

mostPopularDf.write.mode('overwrite')\
                         .parquet("/mnt/aws/silver/popular-movies-parquet")

# COMMAND ----------

mostPopularDf.write.mode('overwrite')\
                         .orc("/mnt/aws/silver/popular-movies-orc")

# COMMAND ----------

mostPopularDf.write.mode('overwrite')\
                    .csv("/mnt/aws/silver/popular-movies-csv")

# COMMAND ----------

mostPopularDf.write.mode('overwrite')\
                    .json("/mnt/aws/silver/popular-movies-json")

# COMMAND ----------

# write your dataframe as table/parquet
mostPopularDf.write.mode('overwrite')\
                   .saveAsTable("moviedb.popular_today")

# COMMAND ----------

# store data in parquet format in aws
movieDf.coalesce(1).write.mode('overwrite')\
                         .parquet("/mnt/aws/silver/movies")

ratingDf.coalesce(1).write.mode('overwrite')\
                         .parquet("/mnt/aws/silver/ratings")


# COMMAND ----------

movieDfPar = spark.read.format("parquet")\
             .load("/mnt/aws/silver/movies")
 
ratingDfPar = spark.read.format("parquet")\
              .load("/mnt/aws/silver/ratings")

print(movieDfPar.count())
print(ratingDfPar.count())


# COMMAND ----------

