from imports import *
from jobs import *

test.testing()

# Importing
print("Importing data from all the defined data sources")
try:
	hive.import()
	hbase.import()
	mysql.import()
	s3.import()
except Exception as ex:
        print(ex)
	
# Job execution
print("Executing jobs")
try:
	join.master_join
except Exception as ex:
        print(ex)


