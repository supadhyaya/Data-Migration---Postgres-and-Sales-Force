
from datetime import date, timedelta
from threading import Thread, Lock
from sf_importer import Importer
from sf_param import param
import sqlalchemy as sqa
import psycopg2.extras
import pandas as pd
import psycopg2
import datetime
import beatbox
import logging
import logging
import pprint
import time
import sys
import os





class Exporter(Thread):
    __lock = Lock()


    def __init__(self, query, table_name,col):
        Thread.__init__(self)
        self.table_name = table_name
        self.query = query
        self.col = col


    def run(self):
        result = None
        logging.info("Connecting to sales force...")
    

        try:
            service = beatbox.PythonClient()  
            service.login(param.user_name, param.password)  
            query_result = service.query(self.query)
            records = query_result['records']  
            total_records = query_result['size']  
            query_locator = query_result['queryLocator']  

            
            while query_result['done'] is False and len(records) < total_records:
                query_result = service.queryMore(query_locator)
                query_locator = query_result['queryLocator'] 
                records = records + query_result['records']  


            table = pd.DataFrame(records)

            

            # record the extraction time
            start_time = datetime.datetime.now()
            table.to_csv(param.newpath+self.table_name+'.csv', index=False , cols = self.col)

            # for tacking the invalid query locator error from salesforce
            param.exported_table.append(self.table_name)
            # record the extraction end time
            finish_time = datetime.datetime.now()

            # cacluate the extraction time for each table
            diff = finish_time - start_time
                 
            print('Extracted '+self.table_name + ", Time Taken: " + str(diff))

            # list tables which are ready to be imported
            param.exported_file[self.table_name] = 1

        except Exception as e:
            logging.error("Unable to access sales force %s" % str(e))






