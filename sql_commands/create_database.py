import psycopg2
import os
from dotenv import load_dotenv
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

#load .env to get postgres user login info
load_dotenv()

# Replace these with your PostgreSQL server and authentication details
db_host = 'localhost'
db_port = '5432'  # Default PostgreSQL port
db_user = os.getenv('psql_username')
db_password = os.getenv('psql_password')

# Connect to the PostgreSQL server
try:
    conn = psycopg2.connect(
        dbname='postgres',  # Connect to the default 'postgres' database
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )

    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

except psycopg2.Error as e:
    print('Error: Unable to connect to the database server.')
    print(e)
    exit(1)

# Create a cursor object
cur = conn.cursor()

# Create the database 'team_flow'
try:
    db_name = 'team_flow'
    create_db_query = 'CREATE DATABASE {db};'.format(db=db_name) #sql.SQL('CREATE DATABASE {}').format(sql.Identifier(db_name))
    cur.execute(create_db_query)
    conn.commit()
    print(f"Database '{db_name}' created successfully.")
except psycopg2.Error as e:
    print(f"Error: Unable to create the database '{db_name}'.")
    print(e)
finally:
    cur.close()
    conn.close()
