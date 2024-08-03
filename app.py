import mysql.connector
 
import pymongo
 
delete_existing_documents = True
 
mysql_host="localhost"
 
mysql_database="mydatabase"
 
mysql_schema = "myschema"
 
mysql_user="myuser"
 
mysql_password="********"
 
mongodb_host = "mongodb://localhost:27017/"
 
mongodb_dbname = "mymongodb"
 
mysqldb = mysql.connector.connect(
 
   host=mysql_host,
 
   database=mysql_database,
 
   user=mysql_user,
 
   password=mysql_password
 
)

mycursor = mysqldb.cursor(dictionary=True)
 
mycursor.execute("SELECT * from categories;")
 
myresult = mycursor.fetchall()
 
myclient = pymongo.MongoClient(mongodb_host)
 
mydb = myclient[mongodb_dbname]
 
mycol = mydb["categories"]
 
if len(myresult) > 0:
 
       x = mycol.insert_many(myresult) #myresult comes from mysql cursor
 
       print(len(x.inserted_ids))
