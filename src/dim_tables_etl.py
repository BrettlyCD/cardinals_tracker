import pandas as pd
import json
import os
import requests

from dotenv import load_dotenv
from config.dim_tables_static import score_type_dict, sportsbook_dict, game_type_dict #import static dimension data for database update

def create_dim_dataframe(dim_dict):
    """Convert input dictionary for static table into a pandas dataframe"""
    #Convert imported dictionary into a pandas dataframe
    df = pd.DataFrame(dim_dict.items(), columns=[['label', 'ID']])
    #Reorder to put ID first
    df = df[['ID','label']]

    return df

#export dataframes for storage
score_type_df = create_dim_dataframe(score_type_dict)
sportsbook_df = create_dim_dataframe(sportsbook_dict)
game_type_df = create_dim_dataframe(game_type_dict)

score_type_df.to_csv('../data/Exports/dim_score_type.csv')
sportsbook_df.to_csv('../data/Exports/dim_sportsbook.csv')
game_type_df.to_csv('../data/Exports/dim_game_type.csv')