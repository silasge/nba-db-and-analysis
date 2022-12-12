import argparse

from prefect import flow, task

from nba_db_and_analysis.db.utils import connect_to_db
from nba_db_and_analysis.db.teams.tb_teams import create_tb_teams
from nba_db_and_analysis.db.teams.tb_game_logs import create_tb_game_logs
from nba_db_and_analysis.db.players.tb_boxscore import create_tb_boxscore


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-season", type=str)
    args = parser.parse_args()
    return args


@flow
def create_schemas():
    conn = connect_to_db()
    sql = """
    CREATE SCHEMA IF NOT EXISTS teams;
    
    CREATE SCHEMA IF NOT EXISTS players;
    """
    conn.execute(sql)
    conn.close()


@flow
def get_game_logs():
    create_schemas()
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-season", type=str)
    parser.add_argument("-season_type", type=str, default="Regular Season")
    args = parser.parse_args()
    create_tb_game_logs(
        season=args.season, season_type=args.season_type
    )
    create_tb_boxscore(
        season=args.season, season_type=args.season_type
    )
