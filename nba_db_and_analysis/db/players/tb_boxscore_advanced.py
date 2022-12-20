import duckdb
from nba_api.stats.endpoints import BoxScoreAdvancedV2
from tqdm import tqdm

from nba_db_and_analysis.db.utils import connect_to_db, ingest_pandas_df_in_db


def players_tb_boxscore_advanced(conn: duckdb.DuckDBPyConnection) -> None:
    sql = """
    CREATE TABLE IF NOT EXISTS players.tb_boxscore_advanced (
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
        E_OFF_RATING FLOAT,
        OFF_RATING FLOAT,
        E_DEF_RATING FLOAT,
        DEF_RATING FLOAT,
        E_NET_RATING FLOAT,
        NET_RATING FLOAT,
        AST_PCT FLOAT,
        AST_TOV FLOAT,
        AST_RATIO FLOAT,
        OREB_PCT FLOAT,
        DREB_PCT FLOAT,
        REB_PCT FLOAT,
        TM_TOV_PCT FLOAT,
        EFG_PCT FLOAT,
        TS_PCT FLOAT,
        USG_PCT FLOAT,
        E_USG_PCT FLOAT,
        E_PACE FLOAT,
        PACE FLOAT,
        PACE_PER40 FLOAT,
        POSS INTEGER,
        PIE FLOAT
    );
    """
    conn.execute(sql)


def get_players_tb_box_score_adv(game_id):
    box = BoxScoreAdvancedV2(
        game_id=game_id
    ).get_data_frames()[0]
    return box


def create_players_tb_boxscore_adv(season, season_type):
    conn = connect_to_db()
    players_tb_boxscore_advanced(conn=conn)
    game_ids = [
        id_[0] for id_ in conn.execute(
            f"""
            SELECT DISTINCT GAME_ID
            FROM teams.tb_game_logs
            WHERE SEASON_YEAR = '{season}'
            AND SEASON_TYPE = '{season_type}'
            AND GAME_ID NOT IN (
                SELECT DISTINCT GAME_ID
                FROM players.tb_boxscore_advanced
            )
            """
        ).fetchall()
    ]
    try:
        with tqdm(game_ids) as pbar:
            for game_id in pbar:
                pbar.set_description(f"Processing {game_id}")
                players_boxsore = get_players_tb_box_score_adv(game_id=game_id)
                ingest_pandas_df_in_db(
                    table="players.tb_boxscore_advanced", df=players_boxsore, conn=conn
                )
    except Exception as e:
        print(e)
        conn.close()
        
    conn.close()
    