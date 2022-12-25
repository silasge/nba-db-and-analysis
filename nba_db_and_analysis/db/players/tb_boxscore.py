from time import sleep

import duckdb
from nba_api.stats.endpoints import BoxScoreTraditionalV2
from tqdm import tqdm

from nba_db_and_analysis.db.utils import connect_to_db, ingest_pandas_df_in_db


def tb_boxscore(conn: duckdb.DuckDBPyConnection) -> None:
    sql = """
    CREATE TABLE IF NOT EXISTS players.tb_boxscore (
        GAME_ID VARCHAR, 
        TEAM_ID VARCHAR, 
        TEAM_ABBREVIATION VARCHAR,
        TEAM_CITY VARCHAR,
        PLAYER_ID VARCHAR,
        PLAYER_NAME VARCHAR,
        NICKNAME VARCHAR,
        START_POSITION VARCHAR, 
        COMMENT VARCHAR, 
        MIN VARCHAR, 
        FGM INTEGER,
        FGA INTEGER,
        FG_PCT FLOAT, 
        FG3M INTEGER, 
        FG3A INTEGER, 
        FG3_PCT FLOAT, 
        FTM INTEGER, 
        FTA INTEGER, 
        FT_PCT FLOAT,
        OREB INTEGER, 
        DREB INTEGER, 
        REB INTEGER, 
        AST INTEGER, 
        STL INTEGER, 
        BLK INTEGER, 
        "TO" INTEGER, 
        PF INTEGER, 
        PTS INTEGER,
        PLUS_MINUS FLOAT
    );
    """
    conn.execute(sql)
    
    
def get_players_tb_score(game_id):
    box = BoxScoreTraditionalV2(
        game_id=game_id
    ).get_data_frames()[0]
    return box


def create_tb_boxscore(season, season_type):
    conn = connect_to_db()
    tb_boxscore(conn=conn)
    game_ids = [
        id_[0] for id_ in conn.execute(
            f"""
            SELECT DISTINCT GAME_ID
            FROM teams.tb_game_logs
            WHERE SEASON_YEAR = '{season}'
            AND SEASON_TYPE = '{season_type}'
            AND GAME_ID NOT IN (
                SELECT DISTINCT GAME_ID
                FROM players.tb_boxscore
            )
            """
        ).fetchall()
    ]
    try:
        with tqdm(game_ids) as pbar:
            for game_id in pbar:
                pbar.set_description(f"Processing {game_id}")
                players_boxsore = get_players_tb_score(game_id=game_id)
                ingest_pandas_df_in_db(
                    table="players.tb_boxscore", df=players_boxsore, conn=conn
                )
                sleep(0.6)
    except Exception as e:
        print(e)
        conn.close()
        
    conn.close()
        