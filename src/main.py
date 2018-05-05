from jobs import *


test.testing()

try:
	hive.import()
	hbase.import()
	mysql.import()
	s3.import()
except Exception as ex:
        print(ex)
	


