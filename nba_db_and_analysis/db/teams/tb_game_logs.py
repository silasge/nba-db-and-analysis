import os
from datetime import date

import duckdb
import pandas as pd
from nba_api.stats.endpoints import TeamGameLogs
from prefect import flow, task

from nba_db_and_analysis.db.utils import connect_to_db, ingest_pandas_df_in_db
from nba_db_and_analysis.db.data_funcs import get_home_away


@task
def tb_game_logs(conn: duckdb.DuckDBPyConnection) -> None:
    sql = """
    CREATE TABLE IF NOT EXISTS teams.tb_game_logs (
        SEASON_YEAR VARCHAR, 
        SEASON_TYPE VARCHAR,
        TEAM_ID VARCHAR, 
        TEAM_ABBREVIATION VARCHAR, 
        TEAM_NAME VARCHAR, 
        GAME_ID VARCHAR,
        GAME_DATE DATE, 
        OPPONENT VARCHAR,
        HOME_AWAY VARCHAR,
        WL VARCHAR, 
        MIN FLOAT, 
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
        TOV INTEGER,
        STL INTEGER, 
        BLK INTEGER, 
        BLKA INTEGER, 
        PF INTEGER, 
        PFD INTEGER, 
        PTS INTEGER, 
        PLUS_MINUS FLOAT
    )
    """
    conn.execute(sql)
    

@task
def get_game_logs(season_nullable, season_type_nullable):
    team_games = TeamGameLogs(
        season_nullable=season_nullable,
        season_type_nullable=season_type_nullable
    ).get_data_frames()[0]
    return team_games


@task
def process_game_logs(
    game: pd.DataFrame,
    season_type_nullable: str,
):
    team_game = game.copy()
    team_game["GAME_DATE"] = pd.to_datetime(team_game["GAME_DATE"])
    team_game = team_game.sort_values("GAME_DATE")
    team_game["SEASON_TYPE"] = season_type_nullable
    team_game["HOME_AWAY"] = team_game["MATCHUP"].apply(lambda x: get_home_away(x))
    team_game["OPPONENT"] = team_game["MATCHUP"].str[-3:]
    return team_game


@task
def select_cols_game_logs(game):
    game = game.loc[
        :,
        [
            "SEASON_YEAR",
            "SEASON_TYPE",
            "TEAM_ID",
            "TEAM_ABBREVIATION",
            "TEAM_NAME",
            "GAME_ID",
            "GAME_DATE",
            "OPPONENT",
            "HOME_AWAY",
            "WL",
            "MIN",
            "FGM",
            "FGA",
            "FG_PCT",
            "FG3M",
            "FG3A",
            "FG3_PCT",
            "FTM",
            "FTA",
            "FT_PCT",
            "OREB",
            "DREB",
            "REB",
            "AST",
            "TOV",
            "STL",
            "BLK",
            "BLKA",
            "PF",
            "PFD",
            "PTS",
            "PLUS_MINUS"
        ]
    ]
    return game


@flow
def create_tb_game_logs(season, season_type):
    conn = connect_to_db()
    tb_game_logs(conn=conn)
    try:
        game = get_game_logs(
            season_nullable=season, season_type_nullable=season_type
        )
        ids_in_db = [
            id_[0] for id_ in conn.execute(
            "SELECT DISTINCT GAME_ID FROM teams.tb_game_logs;"
        ).fetchall()]
        game = game.query("GAME_ID not in @ids_in_db")
        if len(game) > 0:
            game = process_game_logs(game=game, season_type_nullable=season_type)
            game = select_cols_game_logs(game=game)
            ingest_pandas_df_in_db("teams.tb_game_logs", df=game, conn=conn)
    except Exception as e:
        print(e)
        conn.close()
    conn.close()
            
#if __name__ == "__main__":
#    create_tb_game_logs()