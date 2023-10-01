import pandas as pd
import json
import os
import requests

from dotenv import load_dotenv
from config.dim_tables_static import score_type_dict, sportsbook_dict, game_type_dict #import static dimension data for database update
from config.mappings import static_dim_dtype_mapping, team_dtype_mapping

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

#set API querystrings
team_querystring = {'rosters':'false','schedules':'false','topPerformers':'false','teamStats':'false'}

def create_dim_dataframe(dim_dict):
    """Convert input dictionary for static table into a pandas dataframe"""
    #Convert imported dictionary into a pandas dataframe
    df = pd.DataFrame(dim_dict.items(), columns=[['label', 'ID']])
    #Reorder to put ID first
    df = df[['ID','label']]

    df = df.astype(static_dim_dtype_mapping)

    return df

def get_team_data():
    """Pull dimensional team data from Rapid API"""
    ### Team Data
    team_response = requests.get(team_endpoint, headers=headers, params=team_querystring)

    #drill into body of response
    teams = team_response.json()['body']

    return teams

#def transform_team_data(teams_json):


    #setup empty list for saving team records
    team_list = []

    #loop through each te am and save data
    for team in teams:
        team_info = {
            'team_id': team['teamID'],
            'team_name_location': team['teamCity'],
            'team_name': team['teamName'],
            'team_abrv': team['teamAbv'],
            'team_logo_link': team['nflComLogo1']
        }

        team_list.append(team_info)

    #convert list into dataframe
    teams_df = pd.DataFrame(team_list)

    #change datatypes
    teams_df = teams_df.astype(team_dtype_mapping)

    return teams_df

###export dataframes for storage
# score_type_df = create_dim_dataframe(score_type_dict)
# sportsbook_df = create_dim_dataframe(sportsbook_dict)
# game_type_df = create_dim_dataframe(game_type_dict)

# score_type_df.to_csv('../data/Exports/dim_score_type.csv')
# sportsbook_df.to_csv('../data/Exports/dim_sportsbook.csv')
# game_type_df.to_csv('../data/Exports/dim_game_type.csv')

### Team Data
team_response = requests.get(team_endpoint, headers=headers, params=team_querystring)

#drill into body of response
teams = team_response.json()['body']

#setup empty list for saving team records
team_list = []

#loop through each te am and save data
for team in teams:
    team_info = {
        'team_id': team['teamID'],
        'team_name_location': team['teamCity'],
        'team_name': team['teamName'],
        'team_abrv': team['teamAbv'],
        'team_logo_link': team['nflComLogo1']
    }

    team_list.append(team_info)

#convert list into dataframe
teams_df = pd.DataFrame(team_list)

#change datatypes
teams_df = teams_df.astype(team_dtype_mapping)