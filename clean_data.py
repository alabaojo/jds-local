#%% sparkSession

import os
# import pyspark #if findspark is not used 
import findspark
findspark.init()
from pyspark.sql import SparkSession
# for yarn-client
# spark = SparkSession.builder.appName("Saple")
#     .getOrCreate()

#for local 
spark = SparkSession.builder \
    .master("local").appName("JDS Top Ten1") \
    .config("spark.executor.memory", "512mb") \
    .getOrCreate()

from pyspark.sql.types import *


#%% Secrets 
ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

#%% Constants
AWS_BUCKET_NAME = "jdscurema"
MOUNT_NAME = "rawdata"
APPS_S3 = "s3n://jdscurema/20190628/google-playstore-apps.csv"
#REVIEWS_S3 = "s3n://jdscurema/20190628/google-playstore-user-reviews.csv"
APPS = "/home/alaba/sparkProjects/jds/googleplaystore.csv"
REVIEWS = "/home/alaba/sparkProjects/jds/googleplaystore_user_reviews.csv"


#%% schema
appFields = [StructField("App", StringType(), True), \
                 StructField("Category", StringType(), True), \
                 StructField("Rating", DoubleType(), True), \
                 StructField("Review", StringType(), True), \
                 StructField("Installs", StringType(), True), \
                 StructField("Price", StringType(), True), \
                 StructField("Content", StringType(), True), \
                 StructField("Rating", StringType(), True), \
                 StructField("Genres", DoubleType(), True), \
                 StructField("Last_Updated", DoubleType(), True), \
                 StructField("Current_Version", DoubleType(), True), \
                 StructField("Android_Version", DoubleType(), True)]

appSchema = StructType(appFields)

reviewFields = [StructField("App", StringType(), True),
                 StructField("Translated_Review", StringType(), True),
                 StructField("Review", StringType(), True),
                 StructField("Sentiment", StringType(), True),
                 StructField("Sentiment_Polarity", DoubleType(), True),
                 StructField("Sentiment_Subjectivity", StringType(), True)]

reviewSchema = StructType(reviewFields)


#%% Load Data
rawAppDf = spark.read.format("csv") \
   .option("header", "true") \
   .option("inferSchema", "true") \
   .load(APPS_LOCAL)

reviewDf = spark.read.format("csv" ) \
  .option("header", "true")  \
  .option("inferSchema", "true") \
  .load(REVIEWS)

#%% Clean Data
from pyspark.sql.functions import udf,col
import re
cleanNum = udf(lambda x : re.sub("[^0-9]", "",x))
cleanAlpha = udf(lambda x: re.sub(r'\W+', ' ', x))

appDf1 = rawAppDf.dropna(how ='any', subset=['Rating', 'App', 'Category'])
        
appDf = rawAppDf.withColumn('App', cleanAlpha('App')) \
        .withColumn('Install_new', cleanNum("Installs")).drop("Installs") \
        .selectExpr("App", "Category", "Rating", "Reviews","Install_new as Installs","Type as App_Type")  \
        .filter('Installs > 50000') \
        .distinct() \
        .withColumn('Installs', col('Installs').cast(IntegerType())) \
        .dropna(how ='any', subset=['Rating', 'App', 'Category']) 

appDf.show(123)
#data = data.dropna() # drop rows with missing value
#data.cache() # Cache data for faster reuses



#%% Top Ten
from pyspark.sql.window import Window
from pyspark.sql.functions import *
windowSpec = Window.partitionBy('Category') \
    .orderBy(col('Rating').desc(), col('Reviews').desc())

toptenDf = appDf.withColumn("row_number", row_number().over(windowSpec)) \
    .filter('row_number <= 10')

toptenDf.show(12)    

#%% Join apps and reviews

sentimentsDf = toptenDf.join(reviewDf, "app") \
    .select("category", "app", "translated_review", "sentiment")  \
    .withColumn('positive', when(col('sentiment').like('%ositive'), 1).otherwise(0)) \
    .withColumn('negative', when(col('sentiment').like('%egative'), 1).otherwise(0)) \
    .drop('sentiment'). drop('translated_review')               .groupBy("app").sum()                 .withColumnRenamed('sum(positive)', 'positive_sentiment')                .withColumnRenamed('sum(negative)', 'negative_sentiment')

#%%
linesDf = reviewDf.select('Translated_Review')
wordsDf = linesDf.select(explode(split(col("Translated_Review"), "\s+")) \
    .alias("word")) \
    .filter(length('word') > 5) \
    .groupBy("word") .count() .sort(desc("count"))

sentimentsDf.show(12)

#%% save in proper format
toptenDf.write.mode("overwrite").partitionBy("category").parquet("output/topten.parquet")
wordsDf.write.mode("overwrite").parquet("output/wordcount.parquet")
sentimentsDf.write.mode("overwrite").parquet("output/sentiments.parquet")
#%% 
toptenDf.show(122) 