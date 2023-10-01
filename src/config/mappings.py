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
summary_dtype_mapping = {
    'game_id': 'object',
    'home_team_id': 'object',
    'away_team_id': 'object',
    # 'game_type': 'object',
    # 'game_date_id': 'object',
    # 'game_location': 'object',
    # 'away_team': 'object',
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
    'away_team_score': 'int64',
    'home_team_score': 'int64'
}

#static dim table data type mappings - id, label
static_dim_dtype_mapping = {
    'ID': 'int64',
    'label': 'object'
}