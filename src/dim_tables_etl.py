import pandas as pd
import json
import os
import requests

from dotenv import load_dotenv
from config.dim_tables_static import score_type_dict, sportsbook_dict, game_type_dict #import static dimension data for database update
from config.mappings import static_dim_dtype_mapping, team_dtype_mapping, schedule_dtype_mapping

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

#set static API querystrings - I'll set dynamic endpoints in the functions themselves
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

def get_transform_schedule_data(team_list, season):
    """Pull Schedule data from Rapid API, using dynamic team list to pull for multiple teams.
    Going to combine extract and transform here for simplicity do to the nesting of the data. At least for now."""
    #setup empty lsit for saving schedule data
    schedule_data_list = []

    #loop through each team and save data
    for team in team_list:
        #set querystring
        schedule_querystring = {"teamID":"{team_id}".format(team_id=team),"season":"{season}".format(season=season)}

        #call API
        schedule_response = requests.get(schedule_endpoint, headers=headers, params=schedule_querystring)

        #drill to team and schedule 
        team = schedule_response['team'] #use this flag if team is home team
        schedule = schedule_response['schedule']

        #create dictionary for for each game in the schedule
        for game in schedule:
            if game['home'] == team:
                schedule_info = {
                    'game_id': game['gameID'],
                    'team_id': game['teamIDHome'],
                    'game_type_id': game_type_dict.get(game['seasonType'], ""),
                    'season': season, 
                    'game_week': game['gameWeek'],
                    'is_home_team_flag': 1,
                    'is_complete_flag': 1
                }
            else:
                schedule_info = {
                    'game_id': game['gameID'],
                    'team_id': game['teamIDHome'],
                    'game_type_id': game_type_dict.get(game['seasonType'], ""),
                    'season': season, 
                    'game_week': game['gameWeek'],
                    'is_home_team_flag': 0,
                    'is_complete_flag': 1
                }
            
            schedule_data_list.append(schedule_info)

        #convert list into dataframe
        schedule_df = pd.DataFrame(schedule_data_list)

        #change datatypes
        schedule_df = schedule_df.astype(schedule_dtype_mapping)

        return schedule_df



###export dataframes for storage
# score_type_df = create_dim_dataframe(score_type_dict)
# sportsbook_df = create_dim_dataframe(sportsbook_dict)
# game_type_df = create_dim_dataframe(game_type_dict)

# score_type_df.to_csv('../data/Exports/dim_score_type.csv')
# sportsbook_df.to_csv('../data/Exports/dim_sportsbook.csv')
# game_type_df.to_csv('../data/Exports/dim_game_type.csv')

