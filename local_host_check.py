from os import environ
from time import time  , sleep, strftime , gmtime
from sqlalchemy import create_engine , text as sql_text
from sqlalchemy.exc import OperationalError
import pandas as pd
from geopy.distance import distance , lonlat
import json

print('Waiting for the data generator...')
#sleep(20)
print('ETL Starting...')

mysql_engine = create_engine("mysql+pymysql://nonroot:nonroot@localhost:3306/analytics", pool_pre_ping=True, pool_size=10)
#sleep(30)
psql_engine = create_engine("postgresql+psycopg2://postgres:password@localhost:5432/main", pool_pre_ping=True, pool_size=10)



my_table    = pd.read_sql_query(con=psql_engine.connect(), sql=sql_text('select * from devices '))
print ('my_table',my_table)


con = mysql_engine.connect()  # may need to add some other options to connect



with mysql_engine.connect() as conn:
   check_tbl = conn.execute(sql_text("SELECT * FROM aggregated_table")).fetchall()

print ('check_tbl',check_tbl)






