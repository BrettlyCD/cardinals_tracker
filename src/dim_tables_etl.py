###################################################
#              IMPORTS   
###################################################

import pandas as pd
import json
import os
import requests
import datetime
import psycopg2

from dotenv import load_dotenv
from config.dim_tables_static import score_type_dict, sportsbook_dict, game_type_dict #import static dimension data for database update
from config.mappings import team_dtype_mapping, schedule_dtype_mapping, game_dtype_mapping

#import team sample to use for tests
from config.api_variables import team_sample, game_list, game_sample_23

#import functions
from config.functions import get_unique_ids, load_to_postgres

###################################################
#              SETUP VARIABLES   
###################################################

#get API Key and Host
load_dotenv()
api_token = os.getenv('nfl_api_key')
api_host = os.getenv('rapid_api_host')

#set API headers
headers = {
	"X-RapidAPI-Key": "{key}".format(key=api_token),
	"X-RapidAPI-Host": "{host}".format(host=api_host)
}

#set API endpoints
team_endpoint = 'https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLTeams'
schedule_endpoint = 'https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLTeamSchedule'
game_endpoint = 'https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLGameInfo'

#set static API querystrings - I'll set dynamic endpoints in the functions themselves
team_querystring = {'rosters':'false','schedules':'false','topPerformers':'false','teamStats':'false'}

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
#              ETL FUNCTIONS   
###################################################

def create_dim_dataframe(dim_dict, label_column_name, id_column_name):
    """Convert input dictionary for static table into a pandas dataframe.
    Enter column names as strings. 'label_column_name example' = 'game_type_name'
    'id_column_name' example = 'game_type_id'
    """

    #Convert imported dictionary into a pandas dataframe
    df = pd.DataFrame(dim_dict.items(), columns=[label_column_name, id_column_name])
    #Reorder to put ID first
    df = df[[id_column_name, label_column_name]]

    df = df.astype({
        id_column_name: 'int64',
        label_column_name: 'object'
    })

    return df

def get_team_data():
    """Pull dimensional team data from Rapid API"""

    ### Team Data
    team_response = requests.get(team_endpoint, headers=headers, params=team_querystring)

    #drill into body of response
    teams = team_response.json()['body']

    return teams

def transform_team_data(teams_json):
    """Transform Teams API response into a useable dataframe"""

    #setup empty list for saving team records
    team_data_list = []

    #loop through each te am and save data
    for team in teams_json:
        team_info = {
            'team_id': team['teamID'],
            'team_name_location': team['teamCity'],
            'team_name': team['teamName'],
            'team_abrv': team['teamAbv'],
            'team_logo_link': team['nflComLogo1']
        }

        team_data_list.append(team_info)

    #convert list into dataframe
    teams_df = pd.DataFrame(team_data_list)

    #change datatypes
    teams_df = teams_df.astype(team_dtype_mapping)

    return teams_df

def create_dim_date_dataframe(start_date_str, end_date_str):
    """Take a start and end date in YYYY-MM-DD format and create a dataframe of date dimension for each day in that range."""

    #calculate days in input range
    start_date = pd.to_datetime(start_date_str)
    end_date = pd.to_datetime(end_date_str)
    days_count = (end_date-start_date).days+1
    
    date_range = pd.date_range(start_date, periods=days_count, freq='D')

    #create dataframe
    date_df = pd.DataFrame({'date_id': date_range.strftime('%Y%m%d'),
                        'full_date': date_range,
                        'day_of_week': date_range.strftime('%A'),
                        'day_of_week_num': (date_range.weekday + 1) % 7 +1,
                        'day_of_month': date_range.day,
                        'month': date_range.month,
                        'quarter': date_range.quarter,
                        'year': date_range.year})
    
    return date_df

def get_game_data(game_list):
    """Take a list of game_ids as input and pull the game data from the Rapid API"""

    #setup empty list to store API responses
    game_extract_list = []

    #loop through game list and call API
    for game_id in game_list:

        #set querystring
        game_querystring = {"gameID":"{game_id}".format(game_id=game_id)}

        #call API
        game_response = requests.get(game_endpoint, headers=headers, params=game_querystring)

        #drill into body of response
        game_data = game_response.json()['body']

        #add data to extract list
        game_extract_list.append(game_data)

    return game_extract_list

def transform_game_data(game_extract_list):
    """Take the output of our get_game_data function and transform into useable dataframe."""

    #setup empty list to store data
    game_data_list = []

    #read in game location data from persist_variables.json
    with open('../data/persist_variables.json', 'r') as f:
        location_data = json.load(f)

    #loop through extracted game list
    for game in game_extract_list:

        #flag if the game is on a neutral site
        if game['neutralSite'] == 'True':
            neutral_site_flag = 1
        else:
            neutral_site_flag = 0

        #map game type - from our game_type_dict
        game_type_id = game_type_dict.get(game['seasonType'], game['seasonType'])

        #map location data from read in json file if possible
        for i in location_data['location_data']: #iterate through each record in json
            if i['game_id'] == game['gameID']: #check if any records match the game we're on in the extract list
                location = i['game_location']
                arena = i['game_arena']
                break
            else: #else save as blank variables we'll have to fill in later.
                location = ''
                arena = ''

        #create dictionary for team data
        game_info = {
            "game_id": game['gameID'],
            "game_date_id": game['gameDate'],
            "game_type_id": game_type_id,
            "home_team_id": game['teamIDHome'],
            "away_team_id": game['teamIDAway'],
            "game_start_time": game['gameTime'],
            "game_location": location,
            "game_arena": arena,
            "is_neutral_site_flag": neutral_site_flag,
            "espn_link": game['espnLink'],
            "cbs_link": game['cbsLink']
        }
            
        game_data_list.append(game_info)

    #convert list into dataframe
    game_df = pd.DataFrame(game_data_list)

    #change datatypes
    game_df = game_df.astype(game_dtype_mapping)

    return game_df

def get_schedule_data(team_list, season):
    """Pull Schedule data from Rapid API, using dynamic team list to pull for multiple teams."""
    
    #setup empty lsit for saving schedule data
    schedule_extract_list = []

    #loop through each team and save data
    for team in team_list:
        #set querystring
        schedule_querystring = {"teamID":"{team_id}".format(team_id=team),"season":"{season}".format(season=season)}

        #call API
        schedule_response = requests.get(schedule_endpoint, headers=headers, params=schedule_querystring)

        #drill to team and schedule 
        team = schedule_response.json()['body']['team'] #use this flag if team is home team
        schedule = schedule_response.json()['body']['schedule']

        #combine these variables and append to our extract list
        schedule_extract_list.append([team, season, schedule])

    return schedule_extract_list

def transform_schedule_data(schedule_extract_list):
    """Take the output of our get_schedule_data function and transform into a useable dataframe"""   

    #setup empty list to store schedule data
    schedule_data_list = []

    for item in schedule_extract_list:
        #set team and season value
        team = item[0] #take the first item in the sublist
        season = item[1] #take the second item in the sublist
        #now loop through each game
        for game in item[2]: #take the third item in the sublist

            #check for incorrect gameID
            #pull all game_ids in database into a set
            unique_game_ids = get_unique_ids(db_parameters=db_params, schema_name='nfl', table_name='dim_game', column_name='game_id')

            #check if game is in set, if it is continue, if not - skip to the next iteration
            game_id = game['gameID']
            if game_id not in unique_game_ids:
                continue

            #create dictionary for team data
            if game['home'] == team:
                schedule_info = {
                    "game_id": game_id,
                    "team_id": game['teamIDHome'],
                    "game_type_id": game_type_dict.get(game['seasonType'], ""),
                    "season": season, #can I find a way to automate this?
                    "game_week": game['gameWeek'],
                    "is_home_team_flag": 1,
                    "is_complete_flag": 1
                }
            else:
                schedule_info = {
                    "game_id": game_id,
                    "team_id": game['teamIDAway'],
                    "game_type_id": game_type_dict.get(game['seasonType'], ""),
                    "season": season, #can I find a way to automate this?
                    "game_week": game['gameWeek'],
                    "is_home_team_flag": 0,
                    "is_complete_flag": 1
                }
        
            schedule_data_list.append(schedule_info)

    #convert list into dataframe
    schedule_df = pd.DataFrame(schedule_data_list)

    #change datatypes
    schedule_df = schedule_df.astype(schedule_dtype_mapping)

    return schedule_df

#load manually created tables into PostgreSQL
# score_type_df = create_dim_dataframe(score_type_dict, 'score_type', 'score_type_id')
# sportsbook_df = create_dim_dataframe(sportsbook_dict, 'sportsbook_name', 'sportsbook_id')
# game_type_df = create_dim_dataframe(game_type_dict, 'game_type', 'game_type_id')

# load_to_postgres(score_type_df, 'nfl', 'dim_score_type', db_params)
# load_to_postgres(sportsbook_df, 'nfl', 'dim_sportsbook', db_params)
# load_to_postgres(game_type_df, 'nfl', 'dim_game_type', db_params)

# #load team data into PostgreSQL
# teams_json = get_team_data()
# teams_df = transform_team_data(teams_json)
# load_to_postgres(teams_df, 'nfl', 'dim_team', db_params)

# #load date data into PostgreSQL
# date_df = create_dim_date_dataframe('2022-01-01', '2023-12-31')
# load_to_postgres(date_df, 'nfl', 'dim_date', db_params)

# #load game data into PostgreSQL
# game_extract = get_game_data(game_sample_23)
# game_df = transform_game_data(game_extract)
# load_to_postgres(game_df, 'nfl', 'dim_game', db_params)

# # #load schedule data into PostgreSQL
# schedule_extract = get_schedule_data(team_sample, 2023)
# schedule_df = transform_schedule_data(schedule_extract)
# load_to_postgres(schedule_df, 'nfl', 'dim_schedule', db_params)

##maybe need to change this to an upsert?