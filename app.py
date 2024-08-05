import argparse

import mysql.connector

import pymongo
 
delete_existing_documents = True

mysql_schema = "myschema"

mongodb_host = "mongodb://localhost:27017/"

mongodb_dbname = "mymongodb"

myclient = pymongo.MongoClient(mongodb_host)

mydb = myclient[mongodb_dbname]

mycol = mydb["categories"]

# mysqldb = mysql.connector.connect(
#         host='localhost',
#         user='myuser',
#         password='********',
#         database='mydatabase',
#     )

def db_migrate(mysqldb_host: str, mysqldb_user: str, mysqldb_password: str, mysqldb_database: str, mongodb_conn_string: str):
    mysqldb = mysql.connector.connect(
        host=mysqldb_host,
        user=mysqldb_user,
        password=mysqldb_password,
        database=mysqldb_database
    )

    cursor = mysqldb.cursor(dictionary=True)

    # retrieve all tables
    tables = cursor.execute('SHOW TABLES')

    for table in tables:
        print(f'Processing table ====> {table}')

    results = cursor.fetchall()

    if results:
        x = mycol.insert_many(results)  # results comes from mysql cursor
        print(len(x.inserted_ids))


def run_db_migration():
    parser = argparse.ArgumentParser(description="ETL Script to migrate data from MySQL to MongoDB")
    parser.add_argument('mysqldb_connection_string', help="MySQL Database connection string")
    parser.add_argument('mongodb_connection_string', help="MongoDB connection string")

    args = parser.parse_args()


if __name__ == '__main__':
    run_db_migration()
