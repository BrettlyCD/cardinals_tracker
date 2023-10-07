import psycopg2
import os
import time
from dotenv import load_dotenv

# Load .env to get PostgreSQL user login info
load_dotenv()

# set postgres access variables
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

def execute_sql(connection, sql_statements):
    """Input PostgreSQL connection and sql statements to run against your database."""
    try:
        cursor = connection.cursor()
        for sql in sql_statements:
            cursor.execute(sql)
        connection.commit()
        print("Tables created successfully.")
    except Exception as e:
        print(f"Error: {str(e)}")
        connection.rollback()
    finally:
        cursor.close()

#create list with create statements
create_table_sql = [
    """
    -- Create fct_scoring table in 'nfl' schema
    CREATE TABLE nfl.fct_scoring IF NOT EXISTS (
        score_id serial PRIMARY KEY NOT NULL,
        game_id varchar(16) NOT NULL,
        team_id smallint NOT NULL,
        score_type_id smallint NOT NULL,
        score_period char(2),
        score_time time,
        drive_detail varchar,
        score_detail varchar,
        home_team_score smallint,
        away_team_score smallint,
        period_elapsed_time time,
        game_elapsed_time time
    );
    """,

    """
    -- Create fct_record table in 'nfl' schema
    CREATE TABLE nfl.fct_record IF NOT EXISTS (
        record_id serial PRIMARY KEY NOT NULL,
        team_id smallint NOT NULL,
        updated_datetime timestamp,
        season smallint,
        wins smallint,
        loses smallint,
        ties smallint,
        points_for smallint,
        points_against smallint
    );
    """,

    """
    -- Create fct_boxscore table in 'nfl' schema
    CREATE TABLE nfl.fct_boxscore IF NOT EXISTS (
        boxscore_id serial PRIMARY KEY NOT NULL,
        game_id varchar(16) NOT NULL UNIQUE,
        home_team_id smallint NOT NULL,
        away_team_id smallint NOT NULL,
        home_q1_score smallint,
        home_q2_score smallint,
        home_q3_score smallint,
        home_q4_score smallint,
        home_ot_score smallint,
        home_total_score smallint,
        home_total_plays smallint,
        home_total_yards smallint,
        home_passing_yards smallint,
        home_rushing_yards smallint,
        home_turnovers smallint,
        home_time_of_possession time,
        away_q1_score smallint,
        away_q2_score smallint,
        away_q3_score smallint,
        away_q4_score smallint,
        away_ot_score smallint,
        away_total_score smallint,
        away_total_plays smallint,
        away_total_yards smallint,
        away_passing_yards smallint,
        away_rushing_yards smallint,
        away_turnovers smallint,
        away_time_of_possession time,
        home_team_win_flag smallint
    );
    """,

    """
    -- Create fct_betting table in 'nfl' schema
    CREATE TABLE nfl.fct_betting IF NOT EXISTS (
        odds_id serial PRIMARY KEY NOT NULL,
        game_id varchar(16) NOT NULL,
        sportsbook_id smallint NOT NULL,
        last_update timestamp,
        total_over_under real,
        over_odds varchar(6),
        under_odds varchar(6),
        home_spread varchar(6),
        home_odds varchar(6),
        away_spread varchar(6),
        away_odds varchar(6),
        home_ml_odds varchar(6),
        away_ml_odds varchar(6)
    );
    """,

    """
    -- Create dim_team table in 'nfl' schema
    CREATE TABLE nfl.dim_team IF NOT EXISTS (
        team_id smallint PRIMARY KEY NOT NULL,
        team_name_location varchar(50),
        team_name varchar(50),
        team_abrv varchar(3),
        team_logo_link varchar
    );
    """,

    """
    -- Create dim_sportsbook table in 'nfl' schema
    CREATE TABLE nfl.dim_sportsbook IF NOT EXISTS (
        sportsbook_id smallint PRIMARY KEY NOT NULL,
        sportsbook_name varchar(50) NOT NULL
    );
    """,

    """
    -- Create dim_score_type table in 'nfl' schema
    CREATE TABLE nfl.dim_score_type IF NOT EXISTS (
        score_type_id smallint PRIMARY KEY NOT NULL,
        score_type varchar(50) NOT NULL
    );
    """,

    """
    -- Create dim_schedule table in 'nfl' schema
    CREATE TABLE nfl.dim_schedule IF NOT EXISTS (
        schedule_id serial PRIMARY KEY NOT NULL,
        game_id varchar(16) NOT NULL,
        team_id smallint NOT NULL,
        game_type_id smallint NOT NULL,
        season smallint,
        game_week varchar(50),
        is_home_team_flag smallint,
        is_complete_flag smallint
    );
    """,

    """
    -- Create dim_game table in 'nfl' schema
    CREATE TABLE nfl.dim_game IF NOT EXISTS (
        game_id varchar(16) PRIMARY KEY NOT NULL,
        game_date_id smallint NOT NULL,
        game_type_id smallint NOT NULL,
        home_team_id smallint NOT NULL,
        away_team_id smallint NOT NULL,
        game_start_time varchar(50),
        game_location varchar(50),
        game_arena varchar(50),
        is_neutral_site_flag smallint,
        espn_link varchar,
        cbs_link varchar
    );
    """,

    """
    -- Create dim_game_type table in 'nfl' schema
    CREATE TABLE nfl.dim_game_type IF NOT EXISTS (
        game_type_id smallint PRIMARY KEY NOT NULL,
        game_type varchar(50) NOT NULL
    );
    """,

    """
    -- Create dim_date table in 'nfl' schema
    CREATE TABLE nfl.dim_date IF NOT EXISTS (
        date_id smallint PRIMARY KEY NOT NULL,
        full_date date,
        day_of_week varchar(10),
        day_of_week_num smallint,
        day_of_month smallint,
        month smallint,
        quarter smallint,
        year smallint
    );
    """
]

#create list with alter statements to add foreign keys
add_foreign_keys_sql = [
    """
    -- Add foreign key references to fct_scoring
    ALTER TABLE nfl.fct_scoring
        ADD CONSTRAINT fk_fct_scoring_game FOREIGN KEY (game_id) REFERENCES nfl.dim_game(game_id),
        ADD CONSTRAINT fk_fct_scoring_team FOREIGN KEY (team_id) REFERENCES nfl.dim_team(team_id),
        ADD CONSTRAINT fk_fct_scoring_score_type FOREIGN KEY (score_type_id) REFERENCES nfl.dim_score_type(score_type_id);
    """,

    """
    -- Add foreign key references to fct_record
    ALTER TABLE nfl.fct_record
        ADD CONSTRAINT fk_fct_record_team FOREIGN KEY (team_id) REFERENCES nfl.dim_team(team_id);
    """,

    """
    -- Add foreign key references to fct_boxscore
    ALTER TABLE nfl.fct_boxscore
        ADD CONSTRAINT fk_fct_boxscore_game FOREIGN KEY (game_id) REFERENCES nfl.dim_game(game_id),
        ADD CONSTRAINT fk_fct_boxscore_home_team FOREIGN KEY (home_team_id) REFERENCES nfl.dim_team(team_id),
        ADD CONSTRAINT fk_fct_boxscore_away_team FOREIGN KEY (away_team_id) REFERENCES nfl.dim_team(team_id);
    """,

    """
    -- Add foreign key references to fct_betting
    ALTER TABLE nfl.fct_betting
        ADD CONSTRAINT fk_fct_betting_game FOREIGN KEY (game_id) REFERENCES nfl.dim_game(game_id),
        ADD CONSTRAINT fk_fct_betting_sportsbook FOREIGN KEY (sportsbook_id) REFERENCES nfl.dim_sportsbook(sportsbook_id);
    """,

    """
    -- Add foreign key references to dim_schedule
    ALTER TABLE nfl.dim_schedule
        ADD CONSTRAINT fk_dim_schedule_game FOREIGN KEY (game_id) REFERENCES nfl.dim_game(game_id),
        ADD CONSTRAINT fk_dim_schedule_team FOREIGN KEY (team_id) REFERENCES nfl.dim_team(team_id),
        ADD CONSTRAINT fk_dim_schedule_game_type FOREIGN KEY (game_type_id) REFERENCES nfl.dim_game_type(game_type_id);
    """,

    """
    -- Add foreign key references to dim_game
    ALTER TABLE nfl.dim_game
        ADD CONSTRAINT fk_dim_game_game_date FOREIGN KEY (game_date_id) REFERENCES nfl.dim_date(date_id),
        ADD CONSTRAINT fk_dim_game_game_type FOREIGN KEY (game_type_id) REFERENCES nfl.dim_game_type(game_type_id),
        ADD CONSTRAINT fk_dim_game_home_team FOREIGN KEY (home_team_id) REFERENCES nfl.dim_team(team_id),
        ADD CONSTRAINT fk_dim_game_away_team FOREIGN KEY (away_team_id) REFERENCES nfl.dim_team(team_id);
    """
]

#create tables
try:
    conn1 = psycopg2.connect(**db_params)
    execute_sql(connection=conn1, sql_statements=create_table_sql)
except Exception as e:
    print(f"Database error: {str(e)}")
finally:
    if conn1:
        conn1.close()

#give a 5 second breather
time.sleep(5)

#alter tables to add foreign key references
try:
    conn2 = psycopg2.connect(**db_params)
    execute_sql(connection=conn2, sql_statements=add_foreign_keys_sql)
    
except Exception as e:
    print(f"Database error: {str(e)}")
finally:
    if conn2:
        conn2.close()