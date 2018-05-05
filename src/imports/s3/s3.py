import time
import findspark
findspark.init()
import pyspark
from pyspark.sql import SQLContext

# Initializing
sc = pyspark.SparkContext(appName = "app_name")
sqlContext = SQLContext(sc)


sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "")
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "")

sql = pyspark.SQLContext(sc)
df = sql.read.parquet('s3n://bucket-name/mydata.parquet')
df.write.json("/tmp/output/json_output_" + timestr)

