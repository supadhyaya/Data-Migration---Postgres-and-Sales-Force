from sqlalchemy import create_engine
from datetime import date, timedelta
from threading import Thread, Lock
from param import param
import pandas as pd
import threading
import datetime
import psycopg2
import logging
import logging
import pprint
import time
import glob
import sys
import os




class Importer(Thread):
    __lock = Lock()

    def __init__(self, file_name):
        Thread.__init__(self)
        self.file_name = file_name

    def run(self):
        result = None
        logging.info("Connecting to database...")

        try:

            conn_string = param.conn_bi
            conn = psycopg2.connect(conn_string)
            curs = conn.cursor()

            if(self.file_name[:-4] == 'table1' or self.file_name[:-4] == 'table2' or self.file_name[:-4] == 'table3'):
                print("truncating tablle "+ self.file_name[:-4])
                curs.execute("truncate table " + self.file_name[:-4])
                conn.commit()
            
            if (os.stat(param.newpath+self.file_name).st_size > 4):
                file = open(param.newpath+self.file_name)
                curs.copy_expert(sql = """ COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ',' """ % self.file_name[:-4], file=file)
                conn.commit()
                conn.close()
                curs.close
                print("import for " +self.file_name[:-4] + " completed !!!")
            else:
                print("Empty file for: "+self.file_name[:-4])

        except Exception as e:
            logging.error("Unable to access database %s" % str(e))

            
def import_data():
    for key, values in param.exported_file.iteritems():
        if(param.exported_file[key] == 1):
            print("importing data into table: " + key)
            runner = Importer(key + '.csv')
            param.exported_file[key] = 0
            param.counter = param.counter - 1
            runner.start()
            













