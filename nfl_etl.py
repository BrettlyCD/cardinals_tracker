import pandas as pd
import numpy as np
import json
from datetime import datetime
import s3fs

from dotenv import load_dotenv
from config.functions import pandas_game_time_calc

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


#setup empty lists to store game data rows
boxscore_data_list = []
scoring_data_list = []

