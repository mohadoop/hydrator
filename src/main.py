from imports import *
from jobs import *

test.testing()

# Importing
print("Importing data from all the defined data sources")
try:
	hive.import()
	hbase.import()
	mysql.import() # spark-submit --driver-class-path /usr/share/java/mysql-connector-java-5.1.37.jar import_mysql.py
	s3.import()
except Exception as ex:
        print(ex)
	
# Job execution
print("Executing jobs")
try:
	join.master_join
except Exception as ex:
        print(ex)


