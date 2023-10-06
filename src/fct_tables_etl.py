import pandas as pd
import json
import os
import requests
from datetime import datetime

from dotenv import load_dotenv
from config.dim_tables_static import sportsbook_dict
from config.mappings import betting_dtype_mapping, record_dtype_mapping
from config.api_variables import game_sample_23

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

def get_betting_data(game_list):
    """Loop through list of games and get data from Rapid API"""

    #set empty list to save data from each API call
    betting_extract_list = []

    #loop through list and call API
    for game_id in game_list:

        #set querystring
        betting_querystring = {'gameID':'{game_id}'.format(game_id=game_id)}

        #call API
        betting_response = requests.get(betting_endpoint, headers=headers, params=betting_querystring)

        #drill to body and game details
        game_odds = betting_response.json()['body'][game_id]

        #append to list a duo of the game_id and odds data
        betting_extract_list.append([game_id, game_odds])
    
    return betting_extract_list

def transform_betting_data(betting_extract_list, sportsbook_mapping):
    """Take in extract from get_betting_data function and our sportsbook ID mapping dictionary and build useable dataframe"""

    #set empty list to save betting data
    betting_data_list = []

    #loop through extract of game odds
    for odds in betting_extract_list:

        #save gameid and updated time as a variable
        game_id = odds[0] #the first index is the gameID
        updated_time = odds[1]['last_updated_e_time'] #the second index holds the odds data

        #now loop through all of our known sportsbook in the mapping dictionary. If there is a match - create a record, if not - pass
        for key, value in sportsbook_mapping.items():
            try: 
                #drill into sportsbook
                sportsbook_data = odds[1]['{sportsbook}'.format(sportsbook=key)] #key = sportsbook name

                #create dictionary to append to list
                betting_odds = {
                    'game_id': game_id,
                    'last_update': updated_time,
                    'sportsbook_id': value, #value is the sportsbook ID
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

#test betting data
# betting_extract = get_betting_data(game_sample_23)
# betting_df = transform_betting_data(betting_extract, sportsbook_dict)
# betting_df.to_csv('../data/Exports/fct_betting.csv')

#test record data
records_response = get_record_data()
records_df = transform_record_data(records_response, 2023) #can I find a way to auto calculate season?
records_df.to_csv('../data/Exports/fct_record.csv')