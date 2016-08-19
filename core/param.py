from datetime import date, timedelta
import pandas as pd
import datetime
import pprint
import time




class param:
	
	# db connection parameters
	connection = ""
	start_date = date.today() - timedelta(91)
	end_date = date.today() - timedelta(61)

	# data source file path
	newpath = 'data/'+start_date.strftime('%Y-%m-%d')+'/'

	# source database connection
	conn_nwsl = 
	conn_core = 
	conn_msg = "host='xyz' dbname='xyz' user='xyz' password='xyz'"
	
	# BI database
	conn_bi = "postgres://xyz:@xyz/db"

	tbl_core = ['table1','table2']
	tbl_nwsl = ['table1','table2']
	tbl_msg = ['table1','table2']

	# tables to import into BI warehouse
	tbl_except = ['table3','table4']

	tbl_bi = tbl_core + tbl_msg  + tbl_except

	# files ready to be extracted
	exported_file = dict((el,0) for el in tbl_bi)

	counter = 0


	truncate_tbl = ['table1','table2']

	truncate = ''

	# allocation of db connection 
	@classmethod
	def dbconn(self,host):
		if(host == "server1"):
			param.connection = self.conn_nwsl
		elif(host == "server2"):
			param.connection = self.conn_msg
		elif(host == "server3"):
			param.connection = self.conn_core
		elif(host == "server4"):
			param.connection = self.conn_bi
		else:
			print("Invalid Host given : Please enter the details of the host in param.py file ")



