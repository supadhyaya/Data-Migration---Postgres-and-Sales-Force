from datetime import date, timedelta
from threading import Thread, Lock
from importer import Importer
import sqlalchemy as sqa
from param import param
import psycopg2.extras
import pandas as pd
import datetime
import psycopg2
import logging
import pprint
import time
import sys
import os




class Exporter(Thread):
    __lock = Lock()


    def __init__(self, query, file_name):
        Thread.__init__(self)
        self.file_name = file_name
        self.query = query


    def run(self):
        result = None
        logging.info("Connecting to database...")
    
        try:
            conn_string = param.connection
            conn = psycopg2.connect(conn_string)
            curs = conn.cursor()
            outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(self.query)

            with open(param.newpath+self.file_name+".csv", 'w+') as f:
                curs.copy_expert(outputquery, f)

            conn.close()
            curs.close()
            conn.close()
            param.exported_file[self.file_name] = 1

        except Exception as e:
            logging.error("Unable to access database %s" % str(e))




