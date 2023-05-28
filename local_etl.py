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
print ('hhh')


my_table    = pd.read_sql_query(con=psql_engine.connect(), sql=sql_text('select * from devices '))

my_table['hour'] = pd.to_datetime(my_table['time'],unit='s').dt.round("H")


my_table['location_tupple'] = my_table['location'].apply(lambda x: tuple(json.loads(x).values()))

#from pandas.io.json import json_normalize
# lat_lon_split = pd.DataFrame(my_table['location'].values.tolist())
# my_table = pd.concat([my_table, lat_lon_split],axis=1)



my_table['location_lagged'] = (my_table.sort_values(by=['time'], ascending=True)
                       .groupby(['device_id'])['location_tupple'].shift(1))

my_table['location_lagged'] = my_table['location_lagged'].fillna(0)
def fxy(x, y):
     if x == 0 or y == 0 : return None
     else :
          try :
            return distance(lonlat(float(x[1]),float(x[0])), lonlat(float(y[1]),float(y[0]))).km
          except Exception as error:
            print('Caught this error: ' + repr(error)) #for nan values  -- Caught this error: TypeError("'float' object is not subscriptable")
            return None
        
           
          
     

my_table['distance'] = my_table.apply(lambda x: fxy(x['location_tupple'], x['location_lagged']), axis=1)



df2 = my_table.groupby(['device_id','hour']).aggregate({'distance':  "sum", "temperature":  "max","time": "count"}).reset_index().rename(columns={'distance':'distance_sum','temperature' : 'max_temperature','time' : 'count_data_points'})

# df2['distance_sum'] = df2
# df2.distance.distance_sum.drop
# df2.drop()

#my_table    = pd.read_sql('select * from devices', psql_engine)
#another_attempt= psql.read_sql("SELECT * FROM devices", psql_engine)
print("lll",my_table)
print(tuple(json.loads(my_table['location'][1]).values()))
print(type(my_table['distance'][5]))
print(df2)









from pandas.io import sql




con = mysql_engine.connect()  # may need to add some other options to connect


df2.to_sql('aggregated_table',con=con, 
                if_exists='replace')
con.commit()

con.close

with mysql_engine.connect() as conn:
   pp = conn.execute(sql_text("SELECT * FROM aggregated_table")).fetchall()

print ('pp',pp)






#df2.to_csv("./df3.csv")
#print(pd.to_datetime(my_table['time'],unit='s').dt.round("H"))

# utc = gmtime(int(str(int(time()))))
# print(strftime("%Y-%m-%d %H", utc))




# Write the solution here


# while True:
#     try:
#         #sleep(30)
#         psql_engine = create_engine("postgresql+psycopg2://postgres:password@psql_db:5432/main", pool_pre_ping=True, pool_size=10)
#         print ('hhh')
#         my_table    = pd.read_sql_query(con=psql_engine.connect(), sql=sql_text('select * from devices'))
#         #my_table    = pd.read_sql('select * from devices', psql_engine)
#         #another_attempt= psql.read_sql("SELECT * FROM devices", psql_engine)
#         print("lll",my_table)

# # OR
#         #print(another_attempt)

#         break
#     except OperationalError:
#         sleep(0.1)
# print('Connection abcd to PostgresSQL successful.')




# # Write the solution here
