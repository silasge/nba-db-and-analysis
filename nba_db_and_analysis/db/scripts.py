import argparse

from nba_db_and_analysis.db import (
    create_tb_game_logs,
    create_teams_tb_boxscore_adv,
    create_tb_boxscore,
    create_players_tb_boxscore_adv
)

from nba_db_and_analysis.db.utils import connect_to_db

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-season", type=str)
    args = parser.parse_args()
    return args


def create_schemas():
    conn = connect_to_db()
    sql = """
    CREATE SCHEMA IF NOT EXISTS teams;
    
    CREATE SCHEMA IF NOT EXISTS players;
    """
    conn.execute(sql)
    conn.close()


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
    create_teams_tb_boxscore_adv(
        season=args.season, season_type=args.season_type
    )
    create_players_tb_boxscore_adv(
        season=args.season, season_type=args.season_type
    )
