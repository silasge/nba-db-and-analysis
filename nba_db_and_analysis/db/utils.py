import duckdb
import pandas as pd


def connect_to_db(database="./data/processed/nba.duckdb"):
    return duckdb.connect(database=database)


def export_table_to_parquet(
    df: pd.DataFrame,
    save_as: str
) -> str:
    df.to_parquet(path=save_as)
    return save_as


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
    