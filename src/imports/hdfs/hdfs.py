import findspark
findspark.init()
from pyspark import SparkConf,SparkContext

conf = SparkConf().setAppName('from remote hdfs').setMaster('spark://sandbox-hdp.hortonworks.com:7077')
sc = SparkContext(conf=conf)
