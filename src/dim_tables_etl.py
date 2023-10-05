import pandas as pd
import json
import os
import requests

from dotenv import load_dotenv
from config.dim_tables_static import score_type_dict, sportsbook_dict, game_type_dict #import static dimension data for database update
from config.mappings import static_dim_dtype_mapping, team_dtype_mapping, schedule_dtype_mapping, game_dtype_mapping

#import team sample to use for tests
from config.api_variables import team_sample, game_sample

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
            #create dictionary for team data
            if game['home'] == team:
                schedule_info = {
                    "game_id": game['gameID'],
                    "team_id": game['teamIDHome'],
                    "game_type_id": game_type_dict.get(game['seasonType'], ""),
                    "season": season, #can I find a way to automate this?
                    "game_week": game['gameWeek'],
                    "is_home_team_flag": 1,
                    "is_complete_flag": 1
                }
            else:
                schedule_info = {
                    "game_id": game['gameID'],
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
            "is_neautral_site_flag": neutral_site_flag,
            "espn_link": game['espnLink'],
            "cbs_link": game['cbsLink']
        }
            
        game_data_list.append(game_info)

    #convert list into dataframe
    game_df = pd.DataFrame(game_data_list)

    #change datatypes
    game_df = game_df.astype(game_dtype_mapping)

    return game_df

###export dataframes for storage
# score_type_df = create_dim_dataframe(score_type_dict)
# sportsbook_df = create_dim_dataframe(sportsbook_dict)
# game_type_df = create_dim_dataframe(game_type_dict)

# score_type_df.to_csv('../data/Exports/dim_score_type.csv')
# sportsbook_df.to_csv('../data/Exports/dim_sportsbook.csv')
# game_type_df.to_csv('../data/Exports/dim_game_type.csv')

#test team data ETL to simple csv
# teams_json = get_team_data()
# teams_df = transform_team_data(teams_json)
# teams_df.to_csv('../data/Exports/dim_team.csv')

#test schedule data ETL to simple csv
# schedule_extract = get_schedule_data(team_sample, 2022)
# schedule_df = transform_schedule_data(schedule_extract)
# schedule_df.to_csv('../data/Exports/dim_schedule.csv')

# #test game data now that we have some location data saved
# game_extract = get_game_data(game_sample)
# game_df = transform_game_data(game_extract)
# game_df.to_csv('../data/Exports/dim_game.csv')