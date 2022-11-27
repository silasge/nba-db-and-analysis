from prefect import flow, task

from nba_db_and_analysis.db.utils import connect_to_db
from nba_db_and_analysis.db.teams.tb_teams import create_tb_teams
from nba_db_and_analysis.db.teams.tb_game_logs import create_tb_game_logs
    
@flow
def create_schemas():
    conn = connect_to_db()
    sql = """
    CREATE SCHEMA IF NOT EXISTS teams;
    """
    conn.execute(sql)
    conn.close()
    

@flow
def create_database():
    create_schemas()
    create_tb_teams()
    create_tb_game_logs()