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
import csv
import os


def run_cntrl_src(query):
    logging.info("Connecting to database...")


    try:
        conn_string = param.connection
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

row_count = 0 

host = sys.argv[1]

param.dbconn(host)

filter_row = """
                ' as Table, count(1) as Counts from "+ i +"  
                where updated_at >='"+str(param.start_date.strftime('%Y-%m-%d'))+"'
                and updated_at<'"+str(param.end_date.strftime('%Y-%m-%d'))+"'
            """
            

if host == 'core':
    for i in param.tbl_core:
        run_cntrl_src("select now()::date, '" +i +)
        run_cntrl_files(i)

if host == 'msg':
    for i in param.tbl_msg:
        run_cntrl_src("select now()::date, '" +i + filter_row)
        run_cntrl_files(i)

if host == 'nwsl':
    for i in param.tbl_nwsl:
        run_cntrl_src("select now()::date, '" +i  filter_row)
        run_cntrl_files(i)

if host == 'bi':
    for i in param.tbl_core+param.tbl_msg+param.tbl_nwsl:
        run_cntrl_src("select now()::date, '" +i + filter_row)
        run_cntrl_files(i)
