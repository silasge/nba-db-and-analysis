import os
from datetime import date

import duckdb
import pandas as pd
from nba_api.stats.endpoints import teamgamelog
from prefect import flow, task

from nba_db_and_analysis.db.utils import connect_to_db, export_table_to_parquet, ingest_pandas_df_in_db
from nba_db_and_analysis.db.teams.data_funcs import get_home_away


@task
def tb_game_logs(conn: duckdb.DuckDBPyConnection) -> None:
    sql = """
    CREATE TABLE IF NOT EXISTS teams.tb_game_logs (
        SEASON VARCHAR,
        Team_ID VARCHAR, 
        Game_ID VARCHAR,
        SEASON_TYPE VARCHAR,
        GAME_DATE DATE,
        TEAM VARCHAR, 
        OPPONENT VARCHAR,
        HOME_AWAY VARCHAR,
        NUM_GAME INTEGER,
        WL VARCHAR, 
        W INTEGER, 
        L INTEGER, 
        W_PCT FLOAT,
        MIN INTEGER, 
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
        TOV INTEGER, 
        PF INTEGER,
        PTS INTEGER
    )
    """
    conn.execute(sql)
    

@task
def get_game_logs(team_id, season, season_type_all_star):
    team_games = teamgamelog.TeamGameLog(
        team_id=team_id,
        season=season,
        season_type_all_star=season_type_all_star
    ).get_data_frames()[0]
    return team_games

@task
def process_game_logs(
    game: pd.DataFrame,
    season: str,
    season_type: str,
):
    team_game = game.copy()
    team_game["GAME_DATE"] = pd.to_datetime(team_game["GAME_DATE"])
    team_game = team_game.sort_values("GAME_DATE")
    team_game["SEASON"] = str(season)
    team_game["SEASON_TYPE"] = season_type
    team_game["NUM_GAME"] = range(1, len(team_game)+1)
    team_game["HOME_AWAY"] = team_game["MATCHUP"].apply(lambda x: get_home_away(x))
    team_game["TEAM"] = team_game["MATCHUP"].str[0:3]
    team_game["OPPONENT"] = team_game["MATCHUP"].str[-3:]
    return team_game


@task
def select_cols_game_logs(game):
    game = game.loc[
        :,
        [
            "SEASON", "Team_ID", "Game_ID", "SEASON_TYPE", "GAME_DATE", 
            "TEAM", "OPPONENT", "HOME_AWAY", "NUM_GAME", "WL", "W", 
            "L", "W_PCT", "MIN", "FGM", "FGA", "FG_PCT", "FG3M", "FG3A", 
            "FG3_PCT", "FTM", "FTA", "FT_PCT", "OREB", "DREB", "REB", "AST", 
            "STL", "BLK", "TOV", "PF", "PTS"
        ]
    ]
    return game


@flow
def create_tb_game_logs():
    conn = connect_to_db()
    tb_game_logs(conn=conn)
    id_and_year = conn.execute("SELECT id, year_founded FROM teams.tb_teams;").fetchdf()
    for id_, year_founded in zip(id_and_year["id"], id_and_year["year_founded"]):
        for year in range(year_founded, date.today().year + 1):
            for s_type in ["Pre Season", "Regular Season", "Playoffs"]:
                file_path = f"./data/raw/tb_game_logs/{id_}_{year}_{s_type}.parquet"
                db_fetch = conn.execute(
                    f"""
                    SELECT *
                    FROM teams.tb_game_logs
                    WHERE Team_ID = '{id_}' 
                        and SEASON = '{year}' 
                        and SEASON_TYPE = '{s_type}'
                    """
                ).fetchall()
                try:
                    if len(db_fetch) == 0: # se não existe na base
                        game = get_game_logs(
                            team_id=id_,
                            season=year,
                            season_type_all_star=s_type
                        )
                        if not os.path.exists(file_path): 
                            # exportar o raw para parquet caso não exista
                            export_table_to_parquet(game, file_path)
                        # processa e insere o log na base
                        if len(game) > 0:
                            print("######", file_path, "######")
                            game = process_game_logs(game=game, season=year,    season_type=s_type)
                            game = select_cols_game_logs(game=game)
                            ingest_pandas_df_in_db("teams.tb_game_logs", df=game, conn=conn)
                except Exception as e:
                    print(e)
                    conn.close()                    
    conn.close() 
    
# TODO: Colocar outra regra no flow baseada no seguinte problema:
#   Pode acontecer de casos onde existe o arquivo parquet mas não existe esse arquivo na base.
#   Isso por dois motivos:
#       1) Exceptions antes da ingestão. Por exemplo, se haver um erro em process_game_logs(), ou se
#          eu mandar um CTRL+C na execução, após o export_table_to_parquet rodar, vai exsistir o arquivo e na
#          próxima vez que eu rodar o script ele não irá ser colocado na base
#       2) Parquet não deveria ser colocado deq ualquer forma: Exemplo: Time que não foi no playoff.
#          Vai exsitir o arquivo parquet, que será vazio, mas ele nunca será ingerido na base. Esse é um comportamento ok
#   Ambos são bem parecidos quanto a serem um problema. Então vai ser preciso pensar um pouco em como diferenciar os casos.
#   Possivel solução:
#       if existe na base:
#           if existe arquivo parquet: se len(parquet) > 0: adicionar na base
#           elif não existe arquivo parquet: exportar para parquet. Se len(parquet) > 0: adicionar na base


    
#if __name__ == "__main__":
#    create_tb_game_logs()