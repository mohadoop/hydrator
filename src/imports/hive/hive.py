import time
import findspark
findspark.init()
import pyspark
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
import ConfigParser # pip

# time stamp
timestr = time.strftime("%Y%m%d-%H%M%S")


# Parsing Configs
config = ConfigParser.ConfigParser()
config.read('hive.conf')
conf_dict = dict(config.items('config'))


# Initializing
#sc = pyspark.SparkContext(appName = "app_name")
#sqlContext = SQLContext(sc)
#hc = HiveContext(sc)


hive_meta_host = conf_dict['hive_meta_host']
hive_meta_port = conf_dict['hive_meta_port']
hive_meta_uri = "thrift://" + hive_meta_host +":"+ hive_meta_port
#hive_meta_host = conf_dict['hive_meta_host']
#hive_meta_host = conf_dict['hive_meta_host']


spark = SparkSession.builder\
    .appName("interfacing spark sql to hive metastore without configuration file")\
    .config("hive.metastore.uris", hive_meta_uri)\
    .enableHiveSupport()\
    .getOrCreate()


df = spark.table("foodmart.customer")
df.write.format("orc").save("/import/" + conf_dict['hive_hdfs_destination'] +"_"+ timestr )