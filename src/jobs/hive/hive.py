import findspark
findspark.init()
import pyspark
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext

# Initializing
sc = pyspark.SparkContext(appName = "app_name")
sqlContext = SQLContext(sc)
hc = HiveContext(sc)

spark = SparkSession
        .builder()
        .appName("interfacing spark sql to hive metastore without configuration file")
        .config("hive.metastore.uris", "thrift://localhost:9083") // replace with your hivemetastore service's thrift url
        .enableHiveSupport() // don't forget to enable hive support
        .getOrCreate()

fdmrt_cstmr = hc.table("foodmart.customer")
fdmrt_cstmr.show()
fdmrt_cstmr.registerTempTable("fdmrt_temp")
hc.sql("select * from fdmrt_temp limit 5").show()
fdmrt.write.format("orc").save("/imported/hive/data_from_hive_orc")
fdmrt.write.format("csv").save("/imported/hive/data_from_hive_csv")
