import duckdb
from prefect import task
import pandas as pd


@task
def connect_to_db(database="./data/processed/nba.duckdb"):
    return duckdb.connect(database=database)


@task
def export_table_to_parquet(
    df: pd.DataFrame,
    save_as: str
) -> str:
    df.to_parquet(path=save_as)
    return save_as


@task
def ingest_parquet_in_db(
    table: str,
    parquet: str,
    conn: duckdb.DuckDBPyConnection
):
    sql = f"""
    INSERT INTO {table}
    SELECT * FROM read_parquet('{parquet}');
    """
    conn.execute(sql)
    conn.commit()
    

@task
def ingest_pandas_df_in_db(
    table: str,
    df: str,
    conn: duckdb.DuckDBPyConnection
):
    df_copy = df.copy()
    sql = f"""
    INSERT INTO {table}
    SELECT * FROM df_copy;
    """
    conn.execute(sql)
    conn.commit()
    