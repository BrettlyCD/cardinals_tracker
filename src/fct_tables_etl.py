###################################################
#              IMPORTS   
###################################################

import pandas as pd
import json
import os
import requests
import datetime
from datetime import datetime

from dotenv import load_dotenv

#load in dtype mappings and game list to load
#from config.dim_tables_static import sportsbook_dict
from config.mappings import betting_dtype_mapping, record_dtype_mapping
from config.api_variables import game_sample, game_sample_23

#load in functions
from config.functions import get_unique_ids, get_dimension_dict, load_to_postgres

###################################################
#              SETUP VARIABLES   
###################################################

#get API Key and Host
load_dotenv()
api_token = os.getenv('nfl_api_key')
api_host = os.getenv('rapid_api_host')

#set API headers
headers = {
	'X-RapidAPI-Key': '{key}'.format(key=api_token),
	'X-RapidAPI-Host': '{host}'.format(host=api_host)
}

#set API endpoints
betting_endpoint = 'https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLBettingOdds'
record_endpoint = 'https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLTeams'

#set static querystring
record_querystring = {'rosters':'false','schedules':'false','topPerformers':'false','teamStats':'false'}

#set postgres access variables
db_host = 'localhost'
db_user = os.getenv('psql_username')
db_password = os.getenv('psql_password')
db_name = 'team_flow'

#put connection details into params variable
db_params = {
    'host': '{host}'.format(host=db_host),
    'database': '{database}'.format(database=db_name),  
    'user': '{user}'.format(user=db_user),
    'password': '{password}'.format(password=db_password)
}

###################################################
#              ETL Functions  
###################################################

def get_betting_data(game_list):
    """Loop through list of games and get data from Rapid API"""

    #set empty list to save data from each API call
    betting_extract_list = []

    #loop through list and call API
    for game_id in game_list:

        #check for incorrect gameID
        #pull all game_ids in database into a set
        unique_game_ids = get_unique_ids(db_parameters=db_params, schema_name='nfl', table_name='dim_game', column_name='game_id')

        #check if it exists in the database before loading
        if game_id not in unique_game_ids:
            continue

        #set querystring
        betting_querystring = {'gameID':'{game_id}'.format(game_id=game_id)}

        #call API
        betting_response = requests.get(betting_endpoint, headers=headers, params=betting_querystring)

        #drill to body and game details
        game_odds = betting_response.json()['body'][game_id]

        #append to list a duo of the game_id and odds data
        betting_extract_list.append([game_id, game_odds])
    
    return betting_extract_list

def transform_betting_data(betting_extract_list):
    """Take in extract from get_betting_data function and our sportsbook ID mapping dictionary and build useable dataframe"""

    #set empty list to save betting data
    betting_data_list = []

    #load in sportsbook ids and names to iterate through
    sportsbook_mapping = get_dimension_dict(db_params, 'nfl', 'dim_sportsbook', 'sportsbook_id', 'sportsbook_name')

    #loop through extract of game odds
    for odds in betting_extract_list:

        #check for incorrect gameID
        #pull all game_ids in database into a set
        unique_game_ids = get_unique_ids(db_parameters=db_params, schema_name='nfl', table_name='dim_game', column_name='game_id')

        #save gameid and updated time as a variable and check if it exists
        game_id = odds[0] #the first index is the gameID
        if game_id not in unique_game_ids:
            continue

        #save the updated time as a variable
        updated_time = odds[1]['last_updated_e_time'] #the second index holds the odds data

        #now loop through all of our known sportsbook in the mapping dictionary. If there is a match - create a record, if not - pass
        for key, value in sportsbook_mapping.items():
            try: 
                #drill into sportsbook
                sportsbook_data = odds[1]['{sportsbook}'.format(sportsbook=value)] #key = sportsbook name

                #create dictionary to append to list
                betting_odds = {
                    'game_id': game_id,
                    'sportsbook_id': key, #value is the sportsbook ID
                    'last_update': updated_time,
                    'total_over_under': sportsbook_data['totalOver'],
                    'over_odds': sportsbook_data['totalOverOdds'],
                    'under_odds': sportsbook_data['totalUnderOdds'],
                    'home_spread': sportsbook_data['homeTeamSpread'],
                    'home_odds': sportsbook_data['homeTeamSpreadOdds'],
                    'away_spread': sportsbook_data['awayTeamSpread'],
                    'away_odds': sportsbook_data['awayTeamSpreadOdds'],
                    'home_ml_odds': sportsbook_data['homeTeamMLOdds'],
                    'away_ml_odds': sportsbook_data['awayTeamMLOdds']
                }

                betting_data_list.append(betting_odds)
            
            except KeyError:
                pass

    #convert list into dataframe
    betting_df = pd.DataFrame(betting_data_list)

    #change datatypes
    betting_df = betting_df.astype(betting_dtype_mapping)

    #update the last update to datetime
    betting_df['last_update'] = pd.to_datetime(betting_df['last_update'], unit='s')

    return betting_df

def get_record_data():
    """Pull dimensional team data from Rapid API that contains team and record data"""

    ### Team Data
    record_response = requests.get(record_endpoint, headers=headers, params=record_querystring)

    #drill into body of response
    records = record_response.json()['body']

    return records

def transform_record_data(records_json, season):
    """Take exported json from get_record_data and the current season and transform into usebale dataframe"""

    #set empty list to save record data
    record_data_list = []

    #set current timestamp as variable
    refresh_timestamp = datetime.now()

    #loop through each record to save data for each team
    for team in records_json:
        record_dict = {
            'team_id': team['teamID'],
            'updated_datetime': refresh_timestamp,
            'season': season,
            'wins': team['wins'],
            'loses': team['loss'],
            'ties': team['tie'],
            'points_for': team['pf'],
            'points_against': team['pa']
        }

        record_data_list.append(record_dict)
    
    #convert list into dataframe
    record_df = pd.DataFrame(record_data_list)

    #change datatypes
    record_df = record_df.astype(record_dtype_mapping)

    #update updated_datetime to datetime format
    record_df['updated_datetime'] = pd.to_datetime(record_df['updated_datetime'], format='%Y-%m-%d %H:%M:%S.%f')

    return record_df

#load betting data into PostgreSQL
# betting_extract = get_betting_data(game_sample_23)
# betting_df = transform_betting_data(betting_extract)
# load_to_postgres(betting_df, 'nfl', 'fct_betting', db_params)

#load record data into PostgreSQL
# records_response = get_record_data()
# records_df = transform_record_data(records_response, 2023) #can I find a way to auto calculate season?
# load_to_postgres(records_df, 'nfl', 'fct_record', db_params)