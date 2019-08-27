#%% sparkSession
import os
# import pyspark #if findspark is not used 
import findspark
findspark.init()
import pyspark 
from pyspark import SparkContext, SparkConf


# for yarn-client
# spark = SparkSession.builder.appName("Saple")
#     .getOrCreate()

#%% Secrets 
ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

#%%

conf = pyspark.SparkConf()
sc = pyspark.SparkContext('local[4]', conf=conf)
sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "ACCESS_KEY")
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "SECRET_KEY")
sql = pyspark.SQLContext(sc)

#%%
t_object ="s3n://jdscurema.s3.amazonaws.com/playstore/output/topten.csv"
 part-00000-6550666b-53a8-4319-b839-5df4a5eac540-c000.csv""
df = sql.read.parquet(t_object)
#%% Constants

topten_in = "s3a://jdscurema/playstore/output/topten.parquet"
dummy = "s3a://das5v-web1/index.html"
topten_out = "s3a://jdscurema/playstore/output/topten.csv"
#%%
toptenDf = sql.read.format("text" ) \
  .load(dummy)

#%% save in proper format
#toptenDf.write.mode("overwrite").mode("overwrite").csv(topten_out)
#%% 
toptenDf.show(12) 