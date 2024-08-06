import argparse
import sys

import mysql.connector

from mysql.connector import errorcode
from pymongo import MongoClient
 
delete_existing_documents = True

mysql_schema = "myschema"

mongodb_host = "mongodb://localhost:27017/"

mongodb_dbname = "mymongodb"

myclient = MongoClient(mongodb_host)

mydb = myclient[mongodb_dbname]

mycol = mydb["categories"]

# mysqldb = mysql.connector.connect(
#         host='localhost',
#         user='myuser',
#         password='********',
#         database='mydatabase',
#     )

def db_migrate(mysqldb_host: str, mysqldb_user: str, mysqldb_password: str, mysqldb_database: str, mongodb_connection_uri: str):
    # connect to MySQL DB
    try:
        mysqldb = mysql.connector.connect(
            host=mysqldb_host,
            user=mysqldb_user,
            password=mysqldb_password,
            database=mysqldb_database
        )
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print('Invalid username or password')
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print('Database does not exist')
        else:
            print(err)
        sys.exit(1)

    # connect to MongoDB
    mongodb_client = MongoClient(mongodb_connection_uri)

    # get or create database in mongodb
    mongodb_client_db = mongodb_client[mysqldb_database]

    # instantiate MySQL DB cursor
    cursor = mysqldb.cursor(dictionary=True)

    # retrieve all tables
    tables = cursor.execute('SHOW TABLES')

    for table_name in tables:
        print(f'Processing table ====> {table_name}')
        # create and populate the collections
        collection = mongodb_client_db[table_name]

        results = cursor.execute(f"SELECT * FROM {table_name}")
        if results:
            paginated_results = cursor.fetchmany(100)

            # bulk insert data into mongodb
            inserted_data = collection.insert_many(paginated_results)
            print(len(inserted_data.inserted_ids))


def run_db_migration():
    parser = argparse.ArgumentParser(description="ETL Script to migrate data from MySQL to MongoDB")
    parser.add_argument('mysqldb_connection_string', help="MySQL Database connection string")
    parser.add_argument('mongodb_connection_string', help="MongoDB connection string")

    args = parser.parse_args()


if __name__ == '__main__':
    run_db_migration()
