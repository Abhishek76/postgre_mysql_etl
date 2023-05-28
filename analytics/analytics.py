from os import environ
from time import sleep
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
import pandas as pd
import asyncio

print('Waiting for the data generator...')
sleep(20)
print('ETL Starting...')

while True:
    try:
        sleep(30)
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        
        mysql_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, pool_size=10)

        break
    except Exception as error:
        print('Caught this opperational analytics in setting up connction error: ' + repr(error))

print('Connection abcd to PostgresSQL successful.')


async def read_data():
    
    while True:
        try:
            sleep(3)
            psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
            
            my_table    = pd.read_sql('select * from devices', psql_engine)
            
            my_table['time'] = my_table['time'].astype(int)
            my_table['hour'] = pd.to_datetime(my_table['time'],unit='s').dt.round("H") # converting unix time stamp to hours for later group by


            my_table['location_tupple'] = my_table['location'].apply(lambda x: tuple(json.loads(x).values())) # converting a json lat long values to a python tupple

            my_table['location_lagged'] = (my_table.sort_values(by=['time'], ascending=True).groupby(['device_id'])['location_tupple'].shift(1)) # shifting a lagging lat long values creating a new col so that we cant compare the to find out distance

            #a python function to find out distance between 2 lat long point
            def fxy(x, y):
                if x is None or y is None : return None
                else :
                    try :
                        return distance(lonlat(float(x[1]),float(x[0])), lonlat(float(y[1]),float(y[0]))).km
                    except Exception as error:
                        print('Caught this error: ' + repr(error)) #for nan values  -- Caught this error: TypeError("'float' object is not subscriptable")
                        return None
                    
                    
                    
                
            # calculating the distance
            my_table['distance'] = my_table.apply(lambda x: fxy(x['location_tupple'], x['location_lagged']), axis=1)

            #creation of the aggreagate table

            df2 = my_table.groupby(['device_id','hour']).aggregate({'distance':  "sum", "temperature":  "max","time": "count"}).reset_index().rename(columns={'distance':'distance_sum','temperature' : 'max_temperature','time' : 'count_data_points'})

            #oppning up the mysql port
            con = mysql_engine.connect()  # may need to add some other options to connect

            #writing the table and the commiting
            df2.to_sql('aggregated_table',con=con, if_exists='replace')
            con.commit()

            con.close

            with mysql_engine.connect() as conn:
                to_check = conn.execute(sql_text("SELECT * FROM aggregated_table")).fetchall()

            print ('pp',to_check)

            


            
        except Exception as error:
            print('Caught this opperational analytics in setting up connction error: ' + repr(error))
            sleep(0.1)
            await asyncio.sleep(1.0)


loop = asyncio.get_event_loop()

asyncio.ensure_future(
    read_data()
    )

loop.run_forever()


#some more improvements can be done is by making the table incremental where i add a cluse based on certain time range  read the data and  drop some of the data
# on the edge after aggregation  --  but ya in real life an icreamental table will be implemented

#some changes are also done in the main file as there was no cnn.commit and in docker file for analytics added some more packages and also 
# mounted the ports in docker compose so that one can check mannualy the data bases have provided the code along with read me

