#create a custom function to apply
def game_time_calc(row):
    """function for pandas dataframe to calculate how much time has passed in the total game using the score time and the period elapsed time"""
    if row['score_period'] == 'OT':
        return (15 * 4 * 60) + row['period_elapsed_time']
    else:
        #try to grab quarter from both 'Q1' and '1Q' format - API changes mid year
        try:
            quarter = int(str(row['score_period'])[0])
        except ValueError:
            quarter = int(str(row['score_period'])[1])
            
        return ((quarter - 1) * 15 * 60) + row['period_elapsed_time']