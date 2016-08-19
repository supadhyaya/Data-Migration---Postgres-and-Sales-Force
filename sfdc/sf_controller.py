from datetime import date, timedelta
from threading import Thread, Lock
from sf_importer import Importer
import sqlalchemy as sqa
from sf_param import param
import psycopg2.extras
import pandas as pd
import datetime
import psycopg2
import logging
import pprint
import time
import sys
import csv
import os


def run_cntrl_src(query):
    logging.info("Connecting to database...")


    try:
        conn_string = param.conn_bi
        conn = psycopg2.connect(conn_string)
        curs = conn.cursor()
        curs.execute(query)
        result = curs.fetchall()
        table = pd.DataFrame(result)

        with open('control_table.csv', 'a') as f:
            table.to_csv(f, header=False, index=False)

        conn.commit()
        curs.close()
        conn.close()

    except Exception as e:
        logging.error("Unable to access database %s" % str(e))




def run_cntrl_files(file_name):

    try:
        row_count = 0

        with open(param.newpath+file_name+".csv","r") as f:
            reader = csv.reader(f,delimiter = ",")
            data = list(reader)
            row_count = len(data)-1

        rows = [date.today(),file_name,row_count,"Extracts"]

        tables = pd.DataFrame(rows)
        table = tables.transpose()

        with open('control_table.csv', 'a') as f:
            table.to_csv(f, header=False, index=False)

    except Exception as e:
        logging.error("Unable to get file %s" % str(e))



csv.field_size_limit(sys.maxsize)


for i in param.tbl_bi:
    run_cntrl_src("select now()::date, '" +i +"' as Table, count(1) as Counts from "+ '"'+i+'"' +"  where "+ '"'+i+'"."' +"LastModifiedDate"+ '"' +" "">='" +str(param.st_dt.strftime('%Y-%m-%d'))+"'"+' and "'+i+'"."' +"LastModifiedDate"+ '"' +" ""<'"+str(param.e_dt.strftime('%Y-%m-%d'))+"'")
    run_cntrl_files(i)


















    
