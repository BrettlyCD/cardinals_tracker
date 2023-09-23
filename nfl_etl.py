import pandas as pd
import numpy as np
import os
import json
import requests
from datetime import datetime
import s3fs

from dotenv import load_dotenv
from config.functions.py import pandas_game_time_calc

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

#import game list
from game_variables import game_list_22

#setup empty lists to store game data rows
boxscore_data_list = []
scoring_data_list = []

###API PULLS###
#loop through game list and pull down game data
for game in game_list_22:
    #setup querystring for API endpoint
	querystring = {"gameID":"{game_id}".format(game_id=game),"fantasyPoints":"false"}
    
    #call using predefined headers and querystring
	response = requests.get(url, headers=headers, params=querystring)

    #drill into json to get to boxscore data
	boxscore = response.json()['body']
    
	#create dictionary for game summary
	game_summary = {
        "game_id": boxscore['gameID'],
        "game_type": boxscore['seasonType'],
        "game_date_id": boxscore['gameDate'],
        "game_location": boxscore['gameLocation'],
        "away_team_id": boxscore['lineScore']['away']['teamID'],
        "away_team": boxscore['away'],
        "away_q1_score": boxscore['lineScore']['away']['Q1'],
        "away_q2_score": boxscore['lineScore']['away']['Q2'],
        "away_q3_score": boxscore['lineScore']['away']['Q3'],
        "away_q4_score": boxscore['lineScore']['away']['Q4'],
        "away_ot_score": '',
        "away_total_score": boxscore['lineScore']['away']['totalPts'],
        "away_total_plays": boxscore['teamStats']['away']['totalPlays'],
        "away_total_yards": boxscore['teamStats']['away']['totalYards'],
        "away_passing_yards": boxscore['teamStats']['away']['passingYards'],
        "away_rushing_yards": boxscore['teamStats']['away']['rushingYards'],
        "away_turnovers": boxscore['teamStats']['away']['turnovers'],
        "away_time_of_possession": boxscore['teamStats']['away']['possession'],
        "away_result": boxscore['awayResult'],
        "home_team_id": boxscore['lineScore']['home']['teamID'],
        "home_team": boxscore['home'],
        "home_q1_score": boxscore['lineScore']['home']['Q1'],
        "home_q2_score": boxscore['lineScore']['home']['Q2'],
        "home_q3_score": boxscore['lineScore']['home']['Q3'],
        "home_q4_score": boxscore['lineScore']['home']['Q4'],
        "home_ot_score": '',
        "home_total_score": boxscore['lineScore']['home']['totalPts'],
        "home_total_plays": boxscore['teamStats']['home']['totalPlays'],
        "home_total_yards": boxscore['teamStats']['home']['totalYards'],
        "home_passing_yards": boxscore['teamStats']['home']['passingYards'],
        "home_rushing_yards": boxscore['teamStats']['home']['rushingYards'],
        "home_turnovers": boxscore['teamStats']['home']['turnovers'],
        "home_time_of_possession": boxscore['teamStats']['home']['possession'],
        "home_result": boxscore['homeResult']
    }

    #lry to access the 'OT' field and set the value if it exists, if not set OT score to 0
	try:
		game_summary["away_ot_score"] = boxscore['lineScore']['away']['OT']
	except KeyError:
		game_summary["away_ot_score"]=0

	try:
		game_summary["home_ot_score"] = boxscore['lineScore']['home']['OT']
	except KeyError:
		game_summary["home_ot_score"]=0
        
	#append to the boxscore data list
	boxscore_data_list.append(game_summary)   
        
	#loop through scoring events in each game
	for score in boxscore['scoringPlays']:
            #create dictionary for scoring details
		score_detail = {
            "game_id": game,
            "team_id": score['teamID'],
            "score_type": score['scoreType'],
            "score_period": score['scorePeriod'],
            "score_time": score['scoreTime'],
            "drive_detail": score['scoreDetails'],
            "score_detail": score['score'],
            "away_team_score": score['awayScore'],
            "home_team_score": score['homeScore']
        }	
    
		#append to the scoring data list
		scoring_data_list.append(score_detail)

#convert lists into pandas dataframes
game_summary = pd.DataFrame(boxscore_data_list)
scoring_details = pd.DataFrame(scoring_data_list)

#set mappings for datatypes
summary_dtype_mapping = {
    'game_id': 'object',
    'game_type': 'object',
    'game_date_id': 'object',
    'game_location': 'object',
    'away_team_id': 'object',
    'away_team': 'object',
    'away_q1_score': 'int64',
    'away_q2_score': 'int64',
    'away_q3_score': 'int64',
    'away_q4_score': 'int64',
    'away_ot_score': 'int64',
    'away_total_score': 'int64',
    'away_total_plays': 'int64',
    'away_total_yards': 'int64',
    'away_passing_yards': 'int64',
    'away_rushing_yards': 'int64',
    'away_turnovers': 'int64',
    'away_time_of_possession': 'object',  # Update to datetime after mapping
    'away_result': 'object',
    'home_team_id': 'object',
    'home_team': 'object',
    'home_q1_score': 'int64',
    'home_q2_score': 'int64',
    'home_q3_score': 'int64',
    'home_q4_score': 'int64',
    'home_ot_score': 'int64',
    'home_total_score': 'int64',
    'home_total_plays': 'int64',
    'home_total_yards': 'int64',
    'home_passing_yards': 'int64',
    'home_rushing_yards': 'int64',
    'home_turnovers': 'int64',
    'home_time_of_possession': 'object',  # Update to datetime after mapping
    'away_result': 'object',
    'home_result': 'object',
}

scoring_dtype_mapping = {
    'team_id': 'object',
    'score_type': 'category',  # Make score_type categorical
    'score_period': 'object',
    'score_time': 'object',  # convert to datetime in nextsteyp
    'drive_detail': 'object',
    'score_detail': 'object',
    'away_team_score': 'int64',
    'home_team_score': 'int64',
}

#map new datatypes
game_summary = game_summary.astype(summary_dtype_mapping)
scoring_details = scoring_details.astype(scoring_dtype_mapping)

#calculate elapsed time within the period
scoring_details['period_elapsed_time'] = np.where(
    scoring_details.score_period == 'OT',
    (pd.to_datetime('00:10:00', format='%H:%M:%S') - pd.to_datetime(scoring_details.score_time, format='%H:%M:%S')).dt.total_seconds(),
    (pd.to_datetime('00:15:00', format='%H:%M:%S') - pd.to_datetime(scoring_details.score_time, format='%H:%M:%S')).dt.total_seconds()
)

#calculate elapsed time in the game using imported function
scoring_details['game_elapsed_time'] = scoring_details.apply(pandas_game_time_calc, axis=1)

#convert timestamps
game_summary['away_time_of_possession'] = pd.to_datetime(game_summary['away_time_of_possession'], format='%M:%S').dt.time
game_summary['home_time_of_possession'] = pd.to_datetime(game_summary['home_time_of_possession'], format='%M:%S').dt.time
scoring_details['score_time'] = pd.to_datetime(scoring_details['score_time'], format='%M:%S').dt.time
scoring_details['period_elapsed_time'] = pd.to_datetime(scoring_details.period_elapsed_time, unit='s').dt.time
scoring_details['game_elapsed_time'] = pd.to_datetime(scoring_details.game_elapsed_time, unit='s').dt.time

if __name__ == '__main__':
	print(game_summary.head(5))
	print('-------------------------------------')
	print(scoring_details.head(5))
