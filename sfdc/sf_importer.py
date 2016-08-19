from sqlalchemy import create_engine
from datetime import date, timedelta
from threading import Thread, Lock
from sf_param import param
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

            if (os.stat(param.newpath+self.file_name).st_size > 4):
                file = open(param.newpath+self.file_name)
                curs.copy_expert(sql = """ COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ',' """ % str(self.file_name[:-4]), file=file)
                conn.commit()
                conn.close()
                curs.close
                print("Import for: "+ self.file_name[:-4] + " completed !!!")

            else:
                print("Empty file for: "+ self.file_name[:-4])


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
            













