
import os
import psycopg2
from psycopg2.extensions import AsIs
import csv
import sys

import h5py
import numpy as np

user = os.environ["USER"]
print (user)

conn = psycopg2.connect(database="postgres", user=user)
cur = conn.cursor()
f = h5py.File('Data.mat', 'r')

def load_data(table):
    
    attrs = f[table]
    row_len = len(attrs)
    
    new_name = table + "_ex"
    sql = "CREATE TABLE " + new_name 
    
    count = 0
    types = "(" + ", ".join('{0} TEXT'.format(attr) for attr in attrs) + ")"
    sql += types
    
    cur.execute(sql)
    conn.commit()
    
    row_count = len(f[table].values()[0])
    
    for row in range(1, row_count):
        
        sql = "INSERT INTO {0} VALUES ".format(new_name)
        
        count = 0
        values = []
        for attr in attrs:
            data = f[table][attr][row]
            values.append(str(data))
        
        sql = sql + "(" + ", ".join(map(lambda x: "'"+x+"'", values)) + ")"
        cur.execute(sql)
        
        if not row % 1000:
            conn.commit()
    
conn.commit()



load_data('TRANS_CO2')
print ('load TRANS_CO2 successful ******************************************')
load_data('ELEC_CO2')
print ('load ELEC_CO2 successful ******************************************')
load_data('ELEC_MKWH')
#print ('load ELEC_MKWH successful ******************************************')
#load_data('HHV2PUB')
#print ('load HHV2PUB successful ******************************************')
#load_data('VEHV2PUB')
#print ('load VEHV2PUB successful ******************************************')
#load_data('PERV2PUB')
#print ('load PERV2PUB successful ******************************************')
#load_data('DAYV2PUB')
#print ('load DAYV2PUB successful ******************************************')

cur.close()
conn.close()
