import happybase
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
config.read('hbase.conf')
conf_dict = dict(config.items('config'))

# Vars from configs
hbase_host = conf_dict['hbase_meta_host']
hbase_port = conf_dict['hbase_meta_port']
hbase_uri = "thrift://" + hbase_meta_host +":"+ hbase_meta_port




connection = happybase.Connection('hostname')
table = connection.table('table-name')

table.put(b'row-key', {b'family:qual1': b'value1',
                       b'family:qual2': b'value2'})

row = table.row(b'row-key')
print(row[b'family:qual1'])  # prints 'value1'

for key, data in table.rows([b'row-key-1', b'row-key-2']):
    print(key, data)  # prints row key and data for each row

for key, data in table.scan(row_prefix=b'row'):
    print(key, data)  # prints 'value1' and 'value2'

row = table.delete(b'row-key')
