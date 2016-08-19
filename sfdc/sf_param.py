import time
import pprint
import datetime
import dateutil
import pandas as pd
from datetime import date, timedelta





class param:
	
	st_dt = date.today() - timedelta(4)
	e_dt = date.today()
	start_date= str(dateutil.parser.parse(st_dt.strftime('%Y-%m-%d')).isoformat())+".000Z"
	end_date = str(dateutil.parser.parse(e_dt.strftime('%Y-%m-%d')).isoformat())+".000Z"

	rows = " where LastModifiedDate >= "+start_date+" and LastModifiedDate < "+end_date
	#rows = " where CreatedDate ="+end_date
	#rows = " "
	# data source file path
	newpath = 'sf_data/'+st_dt.strftime('%Y-%m-%d')+'/'

	# source database connection
	user_name = "xyz@hotmail.com" 
	password = "passtoken"
	
	# BI database
	conn_bi = "postgres://user:@server/db"


	# files ready to be extracted
	tbl_bi = ['table1','table2','table3','table4','table5']

	exported_file = dict((el,0) for el in tbl_bi)

	counter = 0

	exported_table = [] 

	sf_tables = 'sf_tables.csv'




