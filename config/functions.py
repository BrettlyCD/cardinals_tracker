def pandas_game_time_calc(row):
    """function for pandas dataframe to calculate how much time has passed in the total game using the score time and the period elapsed time"""
    if row['score_period'] == 'OT':
        return (15 * 4 * 60) + row['period_elapsed_time'] #if the score is in OT add the elapsed time in OT to a full 60 minutes of clock time
    else:
        first_digit = int(str(row['score_period'])[0])
        return ((first_digit - 1) * 15 * 60) + row['period_elapsed_time'] #if the score isn't in OT add the elapsed time in the current quarter to the clock time completed in prior quarters