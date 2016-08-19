
from exporter import Exporter
from importer import Importer
from param import param
import psycopg2.extras
import threading
import psycopg2
import importer
import datetime
import time
import sys
import os



print("started at: " + str(datetime.datetime.now()))
host = sys.argv[1]

param.dbconn(host)


# perform no_op load from postgres
psycopg2.extras.register_default_json(loads=lambda x: x)
psycopg2.extras.register_default_jsonb(loads=lambda x: x)

DEC2FLOAT = psycopg2.extensions.new_type(
    psycopg2.extensions.DECIMAL.values,
    'DEC2FLOAT',
    lambda value, curs: int(value) if value is not None else None)
psycopg2.extensions.register_type(DEC2FLOAT)




if not os.path.exists(param.newpath):
    os.makedirs(param.newpath)


filter_row = " where updated_at >='" + str(param.start_date.strftime('%Y-%m-%d')) + "' and updated_at<'" + str(param.end_date.strftime('%Y-%m-%d')) + "'"
#filter_row = " limit 1000"

if (host == "nwsl") :
	param.counter = 2
	for i in param.tbl_nwsl:
	    print('extraction of ' + i +' started')
	    runner = Exporter("select * from "+ i + filter_row, i+"_nwsl")
	    runner.start()

	runner_employee = Exporter("select * from newsletter_customers",'newsletter_customers')
	runner_employee.start()


elif (host == "core"):
	param.counter = 17
	for i in param.tbl_core:
		print('extraction of ' + i +' started')
		runner = Exporter("select * from "+ i + filter_row, i)
		runner.start()

	runner_employee = Exporter("select * from employees",'employees')
	runner_employee.start()


elif (host == "msg"):
	param.counter = 6
	for i in param.tbl_msg:
		print('extraction of ' + i +' started')
		runner = Exporter("select * from "+ i + filter_row, i)
		runner.start()

	runner_employee = Exporter("select * from conversation_senders",'conversation_senders')
	runner_employee.start()



while param.counter != 0:
	importer.import_data()


print("finished at: " + str(datetime.datetime.now()))














