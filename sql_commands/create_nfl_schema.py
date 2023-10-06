import psycopg2
import os
from dotenv import load_dotenv

# Load .env to get PostgreSQL user login info
load_dotenv()

# Replace these with your PostgreSQL server and authentication details
db_host = 'localhost'
db_port = '5432'  # Default PostgreSQL port
db_user = os.getenv('psql_username')
db_password = os.getenv('psql_password')
db_name = 'team_flow' 

# Connect to the PostgreSQL server
try:
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )

except psycopg2.Error as e:
    print('Error: Unable to connect to the database server.')
    print(e)
    exit(1)

# Create a cursor object
cur = conn.cursor()

# Create the 'nfl' schema
try:
    schema_name = 'nfl'
    create_schema_query = f'CREATE SCHEMA IF NOT EXISTS {schema_name};'
    cur.execute(create_schema_query)
    conn.commit()
    print(f"Schema '{schema_name}' created successfully in database '{db_name}'.")
except psycopg2.Error as e:
    print(f"Error: Unable to create the schema '{schema_name}'.")
    print(e)
finally:
    cur.close()
    conn.close()
