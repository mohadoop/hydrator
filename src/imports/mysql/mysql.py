import time
import ConfigParser # pip
import findspark
findspark.init()
import pyspark
from pyspark.sql import SQLContext
from pyspark.sql.session import SparkSession
import ConfigParser # pip

# time stamp
timestr = time.strftime("%Y%m%d-%H%M%S")

# Parsing Configs
config = ConfigParser.ConfigParser()
config.read('mysql.conf')
conf_dict = dict(config.items('config'))

# Initializing
sc = pyspark.SparkContext(appName = "app_name")
sqlContext = SQLContext(sc)
spark = SparkSession(sc)

#sc.setSystemProperty("spark.driver.extraClassPath", "/usr/share/java/mysql-connector-java-5.1.37.jar")
#spark.jars "mysql-connector-java-5.1.37.jar"


df = spark.read.format("jdbc").options(
    url ="jdbc:mysql://" + conf_dict['mysql_host'] +":"+ conf_dict['mysql_port'] +"/"+ conf_dict['mysql_database'],
    driver="com.mysql.jdbc.Driver",
    dbtable=conf_dict['mysql_table'],
    user=conf_dict['mysql_user'],
    password=conf_dict['mysql_password']
).load()


df.write.format("orc").save("/import/" + conf_dict['mysql_hdfs_destination'] +"_"+ timestr )