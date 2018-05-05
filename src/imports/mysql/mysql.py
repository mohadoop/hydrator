import time
import findspark
findspark.init()
import pyspark
from pyspark.sql import SQLContext

# Initializing
sc = pyspark.SparkContext(appName = "app_name")
sqlContext = SQLContext(sc)

df_customers = spark.read.format("jdbc").options(
    url ="jdbc:mysql://bmathew-test.czrx1al336uo.ap-northeast-1.rds.amazonaws.com:3306/test",
    driver="com.mysql.jdbc.Driver",
    dbtable="customer",
    user="root",
    password="password"
).load()


df_customers.write.format("orc").save("/tmp/customer_orders")
