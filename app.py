import mysql.connector

import pymongo
 
delete_existing_documents = True

mysql_schema = "myschema"

mongodb_host = "mongodb://localhost:27017/"

mongodb_dbname = "mymongodb"

mysqldb = mysql.connector.connect(
    host='localhost',
    user='myuser',
    password='********',
    database='mydatabase',
)

cursor = mysqldb.cursor(dictionary=True)

# retrieve all tables

tables = cursor.execute('SHOW TABLES')

for table in tables:
    print(f'Table ====> {table}')


cursor.execute("SELECT * from categories;")

myresult = cursor.fetchall()

myclient = pymongo.MongoClient(mongodb_host)

mydb = myclient[mongodb_dbname]

mycol = mydb["categories"]

if len(myresult) > 0:
       x = mycol.insert_many(myresult) #myresult comes from mysql cursor
       print(len(x.inserted_ids))
