###FILE TO SAVE MAPPINGS###

#period mapping to deal with inconsistent usage of the quarter/period names
period_mapping = {
    '1Q': 'Q1',
    '2Q': 'Q2',
    '3Q': 'Q3',
    '4Q': 'Q4',
    'OT': 'OT'
}

#summary data type mappings
boxscore_dtype_mapping = {
    'game_id': 'object',
    'home_team_id': 'object',
    'away_team_id': 'object',
    # 'game_type': 'object',
    # 'game_date_id': 'object',
    # 'game_location': 'object',
    # 'away_team': 'object',
    # 'home_team': 'object',
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
    'home_team_win_flag': 'int'
    # 'away_result': 'object',
    # 'home_result': 'object',
}

#scoring detail data type mappings
scoring_dtype_mapping = {
    'game_id': 'object',
    'team_id': 'int64',
    'score_type_id': 'int64',  # Make score_type categorical
    'score_period': 'object',
    'score_time': 'object',  # convert to datetime in function
    'drive_detail': 'object',
    'score_detail': 'object',
    'home_team_score': 'int64',
    'away_team_score': 'int64'
}

#static dim table data type mappings - id, label
static_dim_dtype_mapping = {
    'ID': 'int64',
    'label': 'object'
}

#team data type mappings
team_dtype_mapping = {
    'team_id': 'int64',
    'team_name_location': 'object',
    'team_name': 'object',
    'team_abrv': 'object',
    'team_logo_link': 'object'
}

#schedule data type mappings
schedule_dtype_mapping = {
    'game_id': 'object',
    'team_id': 'int64',
    'game_type_id': 'int64',
    'season': 'int64', 
    'game_week': 'object',
    'is_home_team_flag': 'int64',
    'is_complete_flag': 'int64'
}

#game data type mappings
game_dtype_mapping = {
    "game_id": 'object',
    "game_date_id": 'int64',
    "game_type_id": 'int64',
    "home_team_id": 'int64',
    "away_team_id": 'int64',
    "game_start_time": 'object',
    "game_location": 'object',
    "game_arena": 'object',
    "is_neautral_site_flag": 'int64',
    "espn_link": 'object',
    "cbs_link": 'object'
}

#betting data type mappings
betting_dtype_mapping = {
    "game_id": 'object',
    "sportsbook_id": 'int64', 
    "last_update": 'float',  # Update to datetime after mapping
    "total_over_under": 'float',
    "over_odds": 'object',
    "under_odds": 'object',
    "home_spread": 'object',
    "home_odds": 'object',
    "away_spread": 'object',
    "away_odds": 'object',
    "home_ml_odds": 'object',
    "away_ml_odds": 'object'
} 

#record data type mappings
record_dtype_mapping = {
    'team_id': 'int64',
    'updated_datetime': 'object',
    'season': 'int64',
    'wins': 'int64',
    'loses': 'int64',
    'ties': 'int64',
    'points_for': 'int64',
    'points_against': 'int64'
}