import psycopg2
import psycopg2.extras
import sys
import os
import time
from sf_exporter import Exporter
from sf_param import param
from sf_importer import Importer
import sf_importer
import threading
import queries



# perform no_op load from postgres
psycopg2.extras.register_default_json(loads=lambda x: x)
psycopg2.extras.register_default_jsonb(loads=lambda x: x)


if not os.path.exists(param.newpath):
    os.makedirs(param.newpath)


param.counter = 5


runner12 = Exporter(queries.Account+param.rows,'table1', col= queries.Account.split(','))
runner12.start()

runner = Exporter(queries.onb2__invoice__c+param.rows,'table2',col = queries.onb2__invoice__c.split(','))
runner.start()

runner2 = Exporter(queries.onb2__item__c+param.rows,'table3',col = queries.onb2__item__c.split(','))
runner2.start()

runner3 = Exporter(queries.onb2__subscription__c+param.rows,'table4',col = queries.onb2__subscription__c.split(','))
runner3.start()

runner4 = Exporter(queries.onb2__dunning__c+param.rows,'table5',col = queries.onb2__dunning__c.split(','))
runner4.start()



while param.counter != 0:
	sf_importer.import_data()
















