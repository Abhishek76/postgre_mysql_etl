## Running the docker

To get started run ``` docker-compose up ``` in root directory.
It will create the PostgresSQL database and start generating the data.
It will create an empty MySQL database.
It will launch the analytics.py script. 

Your task will be to write the ETL script inside the analytics/analytics.py file.

some more improvements can be done is by making the table incremental where i add a cluse based on certain time range  read the data and  drop some of the data
 on the edge after aggregation  --  but ya in real life an icreamental table will be implemented

some changes are also done in the main file as there was no cnn.commit and in docker file for analytics added some more packages and also 
 mounted the ports in docker compose so that one can check mannualy the data bases have provided the code along with read me

also had to change the version of postgre as docker wasnt unable to get that specefic version updated to latest version

and ya sucess in writing the agreagated table to mysql (analytics) database
<img width="1162" alt="Screenshot 2023-05-28 at 23 58 56" src="https://github.com/Abhishek76/postgre_mysql_etl/assets/26035155/8b6d2462-af14-49f5-914e-e7bfd58bd7eb">
