import duckdb
import pandas as pd
from nba_api.stats.static import teams
from prefect import flow, task

from nba_db_and_analysis.db.utils import connect_to_db, export_table_to_parquet, ingest_parquet_in_db


@task
def tb_teams(conn: duckdb.DuckDBPyConnection) -> None:
    sql = """
    CREATE TABLE IF NOT EXISTS teams.tb_teams (
        id VARCHAR,
        full_name VARCHAR,
        abbreviation VARCHAR,
        nickname VARCHAR,
        city VARCHAR,
        state VARCHAR,
        year_founded INTEGER
    )
    """
    conn.execute(sql)


@task
def get_nba_teams_static():
    nba_teams = teams.get_teams()
    return pd.DataFrame(nba_teams)


@flow
def create_tb_teams():
    nba_teams = get_nba_teams_static()
    parquet_file = export_table_to_parquet(nba_teams, "./data/raw/tb_teams/tb_teams.parquet")
    # ingestion in database
    conn = connect_to_db()
    tb_teams(conn=conn)
    ingest_parquet_in_db("teams.tb_teams", parquet=parquet_file, conn=conn)
    conn.close()
    
    
if __name__ == "__main__":
    create_tb_teams()
    