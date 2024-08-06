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

def db_migrate():
    mysqldb_host: str = input('Enter MySQL DB Host:')
    mysqldb_user: str = input('Enter MySQL DB User:')
    mysqldb_password: str = input('Enter MySQL DB Password:')
    mysqldb_database: str = input('Enter MySQL DB Database name:')
    mongodb_connection_uri: str = input('Enter MongoDB Connection URI:')

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
        print(f'Processing table ====> {table_name}\n')
        # create and populate the collections
        collection = mongodb_client_db[table_name]

        results = cursor.execute(f"SELECT * FROM {table_name}")
        if results:
            paginated_results = cursor.fetchmany(100)

            # bulk insert data into mongodb
            inserted_data = collection.insert_many(paginated_results)
        total_documents = collection.count_documents({})
        print(f'Total documents in the collection {table_name}: {total_documents}\n')
        print(f'Data fully migrated for {table_name}...\n')
    print('Database completely migrated. Rate our ETL script from 1 to 5.\n')
    rating: str = input('Ratings:')
    try:
        if int(rating) < 4:
            print('Uh oh!')
        else:
            print('Amazing...')
    except ValueError:
        print('Enter a valid number from 1 through 5!')


def run_db_migration():
    parser = argparse.ArgumentParser(description="ETL Script to migrate data from MySQL to MongoDB")
    parser.add_argument('mysqldb_connection_string', help="MySQL Database connection string")
    parser.add_argument('mongodb_connection_string', help="MongoDB connection string")

    args = parser.parse_args()


if __name__ == '__main__':
    # run_db_migration()
    db_migrate()
