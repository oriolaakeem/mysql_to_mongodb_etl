import argparse
import sys

import mysql.connector

from faker import Faker
from mysql.connector import errorcode
from pymongo import MongoClient


MIGRATION_FILE_NAME: str = 'migrated.txt'


def db_migrate():
    mysqldb_host: str = input('Enter MySQL DB Host:')
    mysqldb_port: str = input('Enter MySQL DB Port:')
    mysqldb_user: str = input('Enter MySQL DB User:')
    mysqldb_password: str = input('Enter MySQL DB Password:')
    mysqldb_database: str = input('Enter MySQL DB Database name:')
    mongodb_connection_uri: str = input('Enter MongoDB Connection URI:')

    # connect to MySQL DB
    try:
        mysqldb = mysql.connector.connect(
            host=mysqldb_host,
            port=mysqldb_port,
            user=mysqldb_user,
            password=mysqldb_password,
            database=mysqldb_database
        )
        print('Connection to MySQL DB successful...')
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print('Invalid username or password')
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print('Database does not exist')
        else:
            print(err)
        sys.exit(1)

    # generate_fake_data(mysqldb_host, mysqldb_port, mysqldb_user, mysqldb_password, mysqldb_database)

    # connect to MongoDB
    mongodb_client = MongoClient(mongodb_connection_uri)

    # get or create database in mongodb
    mongodb_client_db = mongodb_client[mysqldb_database]

    # instantiate MySQL DB cursor
    cursor = mysqldb.cursor()

    # execute query
    cursor.execute('SHOW TABLES')

    # fetch all tables
    tables = cursor.fetchall()
    print(f'All Tables ====> {tables}')

    if tables:
        for table_name in tables:
            table_name = table_name[0]
            print(f'Processing table ====> {table_name}\n')

            # SQL query to count the total number of rows
            count_query = f"SELECT COUNT(*) FROM {table_name}"

            cursor.execute(count_query)
            # Fetch the count result
            total_rows = cursor.fetchone()[0]
            print(f"{total_rows} records found in the {table_name} table")

            try:
                with open(f'{MIGRATION_FILE_NAME}') as f:
                    file_read = f.read()
                    if table_name in file_read:
                        print(f'{table_name} already migrated successfully!')
                        continue
            except FileNotFoundError:
                pass

            # create and populate the collections
            collection = mongodb_client_db[table_name]

            cursor2 = mysqldb.cursor(dictionary=True)

            offset: int = 0
            page_size: int = 100

            if not total_rows:
                continue

            while offset < total_rows:
                cursor2.execute(f"SELECT * FROM {table_name} LIMIT {page_size} OFFSET {offset}")
                results = cursor2.fetchall()

                print(f'{offset} - {offset + page_size} of {total_rows} migrated...')
                offset += page_size

                # bulk insert data into mongodb
                collection.insert_many(results)

            print(f'Data fully migrated for {table_name}...\n')
            total_documents = collection.count_documents({})
            print(f'Total documents in the collection {table_name}: {total_documents}\n')

            with open(f'{MIGRATION_FILE_NAME}', 'a') as f:
                f.write(f'{table_name},')

            # reset the connection
            cursor.reset(free=True)
            cursor2.reset(free=True)
    else:
        print('No tables found. \n Exiting...')

    print('Database completely migrated. Rate our ETL script from 1 to 5.\n')
    rating: str = input('Ratings:')
    try:
        if int(rating) < 4:
            print('Uh oh!')
        else:
            print('Amazing...')
    except ValueError:
        print('Enter a valid number from 1 through 5!')


def generate_fake_data(mysqldb_host: str, mysqldb_port: str, mysqldb_user: str, mysqldb_password: str, mysqldb_database: str):
    """
    Generating fake data
    """
    mysqldb = mysql.connector.connect(
        host=mysqldb_host,
        port=mysqldb_port,
        user=mysqldb_user,
        password=mysqldb_password,
        database=mysqldb_database
    )
    print('Fake Data Generation...')

    fake = Faker()

    books_isbn_data: list = []
    users_data: list = []

    for _ in range(100):
        users_data.append((fake.user_name(), fake.email(), fake.phone_number()))
        books_isbn_data.append((fake.isbn10(), fake.isbn13()))

    if mysqldb.is_connected():
        print('generating data...')
        cursor = mysqldb.cursor()

        create_user_table_query = f"""
                    CREATE TABLE IF NOT EXISTS users (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        username VARCHAR(100) NOT NULL,
                        email VARCHAR(100) NOT NULL,
                        phone_number VARCHAR(30) NOT NULL
                    );
                    """

        cursor.execute(create_user_table_query)
        print("Users table created successfully")

        insert_users_query = """
                INSERT INTO users (username, email, phone_number)
                VALUES (%s, %s, %s)
                """

        create_book_table_query = f"""
                                    CREATE TABLE IF NOT EXISTS books (
                                        id INT AUTO_INCREMENT PRIMARY KEY,
                                        isbn10 VARCHAR(100) NOT NULL,
                                        isbn13 VARCHAR(100) NOT NULL
                                    );
                                    """

        cursor.execute(create_book_table_query)
        print("Books table created successfully")

        insert_books_query = """
                INSERT INTO books (isbn10, isbn13)
                VALUES (%s, %s)
                """

        # insert_query = f"""
        #         INSERT INTO defaultdb (username, email, phone_number)
        #         VALUES {(users_data[0], users_data[1], users_data[2])}
        #         """

        # print(f'users_data ====> {users_data}')

        # Execute the query with multiple data rows
        cursor.executemany(insert_users_query, users_data)
        cursor.executemany(insert_books_query, books_isbn_data)

        # commit
        mysqldb.commit()

        print(f"{cursor.rowcount} records inserted successfully into users table")

    print('Successfully generated data...')


# def insert_users(users_data):
#     try:
#         connection = mysql.connector.connect(
#             host='your_host',
#             database='your_database',
#             user='your_username',
#             password='your_password'
#         )
#
#         if connection.is_connected():
#             cursor = connection.cursor()
#
#             insert_query = """
#             INSERT INTO users (username, email, phone_number)
#             VALUES (%s, %s, %s)
#             """
#
#             # Execute the query with multiple data rows
#             cursor.executemany(insert_query, users_data)
#
#             connection.commit()
#
#             print(f"{cursor.rowcount} records inserted successfully into users table")
#
#     except Exception as e:
#         print(f"Error while connecting to MySQL: {e}")

    # finally:
    #     if connection.is_connected():
    #         cursor.close()
    #         connection.close()
    #         print("MySQL connection is closed")


def run_db_migration():
    parser = argparse.ArgumentParser(description="ETL Script to migrate data from MySQL to MongoDB")
    parser.add_argument('mysqldb_connection_string', help="MySQL Database connection string")
    parser.add_argument('mongodb_connection_string', help="MongoDB connection string")

    args = parser.parse_args()


if __name__ == '__main__':
    # run_db_migration()
    db_migrate()
