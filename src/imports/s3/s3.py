import time
import findspark
findspark.init()
import pyspark
from pyspark.sql import SQLContext
import ConfigParser # pip

# time stamp
timestr = time.strftime("%Y%m%d-%H%M%S")

# Parsing Configs
config = ConfigParser.ConfigParser()
config.read('s3.conf')
conf_dict = dict(config.items('config'))

# Initializing
sc = pyspark.SparkContext(appName = "app_name")
sqlContext = SQLContext(sc)


sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", conf_dict['s3_access_key'])
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", conf_dict['s3_secret_key'])

sql = pyspark.SQLContext(sc)
df = sql.read.json('s3n:' + conf_dict['s3_bucket_name'])
df.write.json("/import/" + conf_dict['s3_hdfs_destination_dir'] +"_"+ timestr)