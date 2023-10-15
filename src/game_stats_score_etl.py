###################################################
#              IMPORTS   
###################################################

import pandas as pd
import numpy as np
import os
import json
import requests
from datetime import datetime
import s3fs

from dotenv import load_dotenv

#import mappings for naming and datatypes
from config.mappings import period_mapping, boxscore_dtype_mapping, scoring_dtype_mapping

#import game list and lookup variables
from config.api_variables import game_sample
from config.dim_tables_static import score_type_dict

#import functions
from config.functions import pandas_game_time_calc, get_unique_ids, load_to_postgres

###################################################
#              SETUP VARIABLES   
###################################################

#get API Key and Host
load_dotenv()
api_token = os.getenv('nfl_api_key')
api_host = os.getenv('rapid_api_host')

#set API endpoint
url = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLBoxScore"

#set header
headers = {
	"X-RapidAPI-Key": "{key}".format(key=api_token),
	"X-RapidAPI-Host": "{host}".format(host=api_host)
}

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
#              SETUP VARIABLES   
###################################################

def get_game_data(game_list):
    """Pull data from NFL Rapid API and save to lists for transformation"""
    #setup empty lists to store json data
    boxscore_responses = []
    scoring_responses = []

    #loop through game list and pull down game data
    for game_id in game_list:

        #check for incorrect gameID
        #pull all game_ids in database into a set
        unique_game_ids = get_unique_ids(db_parameters=db_params, schema_name='nfl', table_name='dim_game', column_name='game_id')

        #check if it exists in the database before loading
        if game_id not in unique_game_ids:
            continue

        #setup querystring for API endpoint
        querystring = {"gameID":"{game_id}".format(game_id=game_id),"fantasyPoints":"false"}
        
        #call using predefined headers and querystring
        response = requests.get(url, headers=headers, params=querystring)

        #drill into json to get to boxscore data
        boxscore = response.json()['body']

        #append body of response to boxscore list
        boxscore_responses.append(boxscore)

        #loop through scoring events in each game
        for score in boxscore['scoringPlays']:
            #create a sub_list with the gameID and scoring details
            score_sub_list = [game_id, score]
            #append score detail to scoring list
            scoring_responses.append(score_sub_list)

    return boxscore_responses, scoring_responses

def save_game_location(boxscore_responses, json_path):
    """Take list from boxscore api response and save the game location data to persist_variables.json"""

    #read in json
    with open(json_path, 'r') as f:
        data = json.load(f)

    #create a set of existing game IDs for faster lookup
    existing_game_ids = set(item['game_id'] for item in data['location_data'])

    #for each boxscore record, check if the record exisits and if not, add it
    for boxscore in boxscore_responses:
        game_id = boxscore['gameID']

        #check if game ID already exisits, and if not prep the variables
        if game_id not in existing_game_ids:
            game_location = {
                "game_id": boxscore['gameID'],
                "game_location": boxscore['gameLocation'],
                "game_arena": '' #leave as blank and use try except later as this variable isn't in all the games
                #"game_arena": boxscore['arena']
                }
            
            #try to fill in the arena if it exists, if not, leave it blank
            try:
                game_location['game_arena'] = boxscore['arena']
            except KeyError:
                game_location['game_arena']=''
            
            #append to list
            data['location_data'].append(game_location)

            #add the game id to our set
            existing_game_ids.add(game_id)

    #re-save json file
    with open(json_path, 'w') as f:
        json.dump(data, f)

def transform_boxscore_data(boxscore_responses):
    """Take lists from boxscore api responses and process the data into a useable dataframe"""
    #setup empty lists to store game data rows
    boxscore_data_list = []

    #loop through API responses to populate data
    for boxscore in boxscore_responses:

        #check for incorrect gameID
        #pull all game_ids in database into a set
        unique_game_ids = get_unique_ids(db_parameters=db_params, schema_name='nfl', table_name='dim_game', column_name='game_id')

        #save gameid and updated time as a variable and check if it exists
        game_id = boxscore['gameID']
        if game_id not in unique_game_ids:
            continue

        #set home team win flag value
        if boxscore['homeResult'] == 'W':
            home_win_flag = 1
        else:
            home_win_flag = 0

        game_summary = {
            "game_id": game_id,
            "home_team_id": boxscore['lineScore']['home']['teamID'],
            "away_team_id": boxscore['lineScore']['away']['teamID'],
            "home_q1_score": boxscore['lineScore']['home']['Q1'],
            "home_q2_score": boxscore['lineScore']['home']['Q2'],
            "home_q3_score": boxscore['lineScore']['home']['Q3'],
            "home_q4_score": boxscore['lineScore']['home']['Q4'],
            "home_ot_score": '', #leave as blank for now to fill in the next step
            "home_total_score": boxscore['lineScore']['home']['totalPts'],
            "home_total_plays": boxscore['teamStats']['home']['totalPlays'],
            "home_total_yards": boxscore['teamStats']['home']['totalYards'],
            "home_passing_yards": boxscore['teamStats']['home']['passingYards'],
            "home_rushing_yards": boxscore['teamStats']['home']['rushingYards'],
            "home_turnovers": boxscore['teamStats']['home']['turnovers'],
            "home_time_of_possession": boxscore['teamStats']['home']['possession'],
            "away_q1_score": boxscore['lineScore']['away']['Q1'],
            "away_q2_score": boxscore['lineScore']['away']['Q2'],
            "away_q3_score": boxscore['lineScore']['away']['Q3'],
            "away_q4_score": boxscore['lineScore']['away']['Q4'],
            "away_ot_score": '', #leave as blank for now to fill in the next step
            "away_total_score": boxscore['lineScore']['away']['totalPts'],
            "away_total_plays": boxscore['teamStats']['away']['totalPlays'],
            "away_total_yards": boxscore['teamStats']['away']['totalYards'],
            "away_passing_yards": boxscore['teamStats']['away']['passingYards'],
            "away_rushing_yards": boxscore['teamStats']['away']['rushingYards'],
            "away_turnovers": boxscore['teamStats']['away']['turnovers'],
            "away_time_of_possession": boxscore['teamStats']['away']['possession'],
            "home_team_win_flag": home_win_flag
        }

        #try to access the 'OT' field and set the value if it exists, if not set OT score to 0
        #away score
        try:
            game_summary["away_ot_score"] = boxscore['lineScore']['away']['OT']
        except KeyError:
            game_summary["away_ot_score"]=0
        
        #home score
        try:
            game_summary["home_ot_score"] = boxscore['lineScore']['home']['OT']
        except KeyError:
            game_summary["home_ot_score"]=0
            
        #append to the boxscore data list
        boxscore_data_list.append(game_summary)  
    
    #create dataframe from list
    boxcore_df = pd.DataFrame(boxscore_data_list)

    #map new datatypes
    boxcore_df = boxcore_df.astype(boxscore_dtype_mapping)

    #convert timestamps
    boxcore_df['away_time_of_possession'] = pd.to_datetime(boxcore_df['away_time_of_possession'], format='%M:%S').dt.time
    boxcore_df['home_time_of_possession'] = pd.to_datetime(boxcore_df['home_time_of_possession'], format='%M:%S').dt.time

    return boxcore_df

def transform_scoring_data(scoring_responses):
    """Take lists from boxscore api responses and process the data into a useable dataframe"""
    #setup empty list to store scoring data rows
    scoring_data_list = []
        
    #loop through scoring events in each boxscore isntance ("game")
    for score in scoring_responses:
        #use mapping to get score period and score_type_id into correct format
        score_period = period_mapping.get(score[1]['scorePeriod'], score[1]['scorePeriod']) #have to add the 1 index to pull the score details, 0 is the game ID
        score_type_id = score_type_dict.get(score[1]['scoreType'], '') #get the ID of the score type or blank if not exists

        #check for incorrect gameID
        #pull all game_ids in database into a set
        unique_game_ids = get_unique_ids(db_parameters=db_params, schema_name='nfl', table_name='dim_game', column_name='game_id')

        #save gameid and updated time as a variable and check if it exists
        game_id = score[0]
        if game_id not in unique_game_ids:
            continue

        #create dictionary for scoring details
        score_detail = {
            "game_id": score[0],
            "team_id": score[1]['teamID'],
            "score_type_id": score_type_id,
            "score_period": score_period,
            "score_time": score[1]['scoreTime'],
            "drive_detail": score[1]['scoreDetails'],
            "score_detail": score[1]['score'],
            "home_team_score": score[1]['homeScore'],
            "away_team_score": score[1]['awayScore']
        }	

        #append to the scoring data list
        scoring_data_list.append(score_detail)

    #convert lists into pandas dataframes
    scoring_df = pd.DataFrame(scoring_data_list)

    #map new datatypes
    scoring_df = scoring_df.astype(scoring_dtype_mapping)

    #convert timestamps
    scoring_df['score_time'] = pd.to_datetime(scoring_df['score_time'], format='%M:%S').dt.time

    #calculate elapsed time within the period
    scoring_df['period_elapsed_time'] = np.where(
        scoring_df.score_period == 'OT',
        (pd.to_datetime('00:10:00', format='%H:%M:%S') - pd.to_datetime(scoring_df.score_time, format='%H:%M:%S')).dt.total_seconds(),
        (pd.to_datetime('00:15:00', format='%H:%M:%S') - pd.to_datetime(scoring_df.score_time, format='%H:%M:%S')).dt.total_seconds()
    )

    #calculate elapsed time in the game using imported function
    scoring_df['game_elapsed_time'] = scoring_df.apply(pandas_game_time_calc, axis=1)

    #convert timestamps for new fields
    scoring_df['period_elapsed_time'] = pd.to_datetime(scoring_df.period_elapsed_time, unit='s').dt.time
    scoring_df['game_elapsed_time'] = pd.to_datetime(scoring_df.game_elapsed_time, unit='s').dt.time

    return scoring_df

#save dataframes to S3
#game_summary.to_csv('s3://nfl-etl-project-brett/cardinals_game_summary_data.csv')
#scoring_details.to_csv('s3://nfl-etl-project-brett/cardinals_game_scoring_details.csv')

# #save game location
# boxscore_response, scoring_response = get_game_data(game_sample)
# save_game_location(boxscore_response, '../data/persist_variables.json')

#test boxscore and scoring data
# boxscore_response, scoring_response = get_game_data(game_sample)
# boxscore_df = transform_boxscore_data(boxscore_response)
# scoring_df = transform_scoring_data(scoring_response)

# boxscore_df.to_csv('../data/Exports/fct_boxscore.csv')
# scoring_df.to_csv('../data/Exports/fct_scoring.csv')