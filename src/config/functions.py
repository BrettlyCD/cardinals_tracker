### File to store functions for importing to other files and using

###################################################
#              IMPORTS   
###################################################

import pandas as pd
import psycopg2
from datetime import datetime, time


###################################################
#              FUNCTIONS   
###################################################

def pandas_game_time_calc(row):
    """function for pandas dataframe to calculate how much time has passed in the total game using the score time and the period elapsed time"""
    if row['score_period'] == 'OT':
        return (15 * 4 * 60) + row['period_elapsed_time']
    else:
        #try to grab quarter from both 'Q1' and '1Q' format - API changes mid year
        try:
            quarter = int(str(row['score_period'])[0])
        except ValueError:
            quarter = int(str(row['score_period'])[1])
            
        return ((quarter - 1) * 15 * 60) + row['period_elapsed_time']
    
def get_unique_ids(db_parameters, schema_name='nfl', table_name='dim_game', column_name='game_id'):   
    """
    Pull down a set of unique values from any field in a postgres database. Use it to validate foreign key values.
    """
    try:
        #setup connection and cursor
        conn = psycopg2.connect(**db_parameters)
        cursor = conn.cursor() 

        # Define the SQL query to retrieve unique game IDs
        query = f"SELECT DISTINCT {column_name} FROM {schema_name}.{table_name};"
    
        # Execute the query
        cursor.execute(query)
        
        # Fetch all the unique game IDs and store them in a list
        unique_game_ids = {row[0] for row in cursor.fetchall()}

    except (Exception, psycopg2.Error) as error:
        print(f"Error: {error}")
    
    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    
    return unique_game_ids

def get_dimension_dict(db_parameters, schema_name, table_name, column1_name, column2_name):
    """
    Pull down distinct values from two columns in a postgres database. Use the output to build a dictionary of unique id-values combos to use in python functions.
    """
    try:
        #setup connection and cursor
        conn = psycopg2.connect(**db_parameters)
        cursor = conn.cursor() 

        # Define the SQL query to retrieve unique game IDs
        query = f"SELECT DISTINCT {column1_name}, {column2_name} FROM {schema_name}.{table_name};"
    
        # Execute the query
        cursor.execute(query)
        
        # Fetch all rows and convert them into a list of dictionaries
        result_dict = {}
        for row in cursor.fetchall():
            field1, field2 = row
            result_dict[field1] = field2
        

    except (Exception, psycopg2.Error) as error:
        print(f"Error: {error}")
    
    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    
    return result_dict

def load_to_postgres(dataframe_to_load, target_schema, target_table, db_parameters):
    """
    Take dataframe created in transform step and load the data into the target_table in a PostgreSQL database.
    """

    #save column names to a list
    column_names = dataframe_to_load.columns.tolist()

    #create empty list to store insert statements
    insert_statements = []

    #iterate through dataframe to create a list of INSERT SQL statements to run
    for index, row in dataframe_to_load.iterrows():
        values = ', '.join([f"'{val}'" if isinstance(val, (str, datetime, time)) and not pd.isna(val) else 'NULL' if pd.isna(val) else str(val) for val in row]) #have some adjustments here to get into the correct format based on values
        insert_statement = f"INSERT INTO {target_schema}.{target_table} ({', '.join(column_names)}) VALUES ({values});"
        insert_statements.append(insert_statement)
    
    #make load to database
    try:
        #setup connection and cursor
        conn = psycopg2.connect(**db_parameters)
        cursor = conn.cursor()

        #insert data
        for sql in insert_statements:
            cursor.execute(sql)

        #commit changes
        conn.commit()

        count = len(insert_statements)
        print(count, "records inserted successfully into {schema_table} table".format(schema_table=target_table))

    except (Exception, psycopg2.Error) as error:
        print(f"Error: {error}")

    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == '__main__':
    pandas_game_time_calc()
    get_unique_ids()
    get_dimension_dict()
    load_to_postgres()