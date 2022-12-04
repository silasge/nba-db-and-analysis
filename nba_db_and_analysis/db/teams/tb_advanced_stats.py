import duckdb
from prefect import task, flow
import numpy as np

from nba_db_and_analysis.db.utils import connect_to_db

@task
def tb_advanced_stats(conn: duckdb.DuckDBPyConnection) -> None:
    sql = """
    CREATE TABLE IF NOT EXISTS teams.tb_advanced_stats (
        Team_ID VARCHAR,
        Game_ID VARCHAR,
        POSS INTEGER,
        PACE FLOAT,
        OFF_RATING FLOAT,
        DEF_RATING FLOAT,
        NET_RATING FLOAT
    );
    """
    conn.execute(sql)
    

@task
def populate_tb_advanced_stats_with_team_and_game_id(
    conn: duckdb.DuckDBPyConnection
) -> None:
    sql = """
    INSERT INTO teams.tb_advanced_stats (
    SELECT 
        Team_ID, 
        Game_ID,
        NULL AS POSS,
        NULL AS PACE,
        NULL AS OFF_RATING,
        NULL AS DEF_RATING,
        NULL AS NET_RATING
    FROM teams.tb_game_logs
    WHERE Team_ID NOT IN (
        SELECT DISTINCT Team_ID
        FROM teams.tb_advanced_stats
    )
    AND Game_ID NOT IN (
        SELECT DISTINCT Game_ID
        FROM teams.tb_advanced_stats
    )
    );
    """
    conn.execute(sql)
    

@task
def calc_and_ingest_possession(
    team_id: str,
    game_id: str,
    conn: duckdb.DuckDBPyConnection
) -> None:
    opp_team = conn.execute(
        f"""
        SELECT Team_ID
        FROM teams.tb_game_logs 
        WHERE Team_ID <> '{team_id}'
        AND Game_ID = '{game_id}';
        """
    ).fetchall()
    if len(opp_team) > 0:
        opp_team_id = opp_team[0][0]
        team_stat = conn.execute(
            f"""
            SELECT FGA, FGM, FTA, TOV, OREB, DREB
            FROM teams.tb_game_logs
            WHERE Team_ID = '{team_id}' AND Game_ID = '{game_id}';
            """
        ).fetch_df()
        opp_stat = conn.execute(
            f"""
            SELECT
                FGA AS OPP_FGA, 
                FGM AS OPP_FGM, 
                FTA AS OPP_FTA, 
                TOV AS OPP_TOV, 
                OREB AS OPP_OREB, 
                DREB AS OPP_DREB
            FROM teams.tb_game_logs
            WHERE Team_ID = '{opp_team_id}' AND Game_ID = '{game_id}';
            """
        ).fetch_df()
        fga = team_stat["FGA"][0]
        fta = team_stat["FTA"][0]
        fgm = team_stat["FGM"][0]
        tov = team_stat["TOV"][0]
        oreb = team_stat["OREB"][0]
        dreb = team_stat["DREB"][0]
        o_fga = opp_stat["OPP_FGA"][0]
        o_fta = opp_stat["OPP_FTA"][0]
        o_fgm = opp_stat["OPP_FGM"][0]
        o_tov = opp_stat["OPP_TOV"][0]
        o_oreb = opp_stat["OPP_OREB"][0]
        o_dreb = opp_stat["OPP_DREB"][0]
        team_half = (fga + 0.4 * fta - 1.07 * (oreb / (oreb + o_dreb)) * (fga - fgm) + tov)
        opp_half = (o_fga + 0.4 * o_fta - 1.07 * (o_oreb / (o_oreb + dreb)) * (o_fga - o_fgm) + o_tov)
        poss = np.round(0.5 * (team_half + opp_half))
    else:
        poss = 999
        
    conn.execute(
        f"""
        UPDATE teams.tb_advanced_stats
        SET POSS = {999 if np.isnan(poss) else poss}
        WHERE POSS IS NULL
        AND Team_ID = '{team_id}'
        AND Game_ID = '{game_id}';
        """
    )


@task
def calc_and_ingest_pace(conn):
    sql = """
    WITH temp_pace AS (
    SELECT
    	a.Team_ID, 
    	a.Game_ID, 
        (48 * ((a.POSS::FLOAT*2) / (2*b."MIN"/5))) AS PACE
    FROM teams.tb_advanced_stats AS a
    LEFT JOIN teams.tb_game_logs AS b 
    ON a.Team_ID = b.Team_ID 
    AND a.Game_ID = b.Game_ID
    )
    UPDATE teams.tb_advanced_stats
    SET PACE = (
    SELECT PACE 
    FROM temp_pace 
    WHERE teams.tb_advanced_stats.Team_ID = temp_pace.Team_ID
    AND teams.tb_advanced_stats.Game_ID = temp_pace.Game_ID
    );
    """
    conn.execute(sql)
    
    
@task
def calc_and_ingest_off_rating(conn):
    sql = """
    WITH temp_rating AS (
    SELECT 
    	a.Team_ID,
    	a.Game_ID,
    	a.POSS,
    	b.PTS,
    	(100 * (b.PTS / a.POSS::FLOAT)) AS RATING
    FROM teams.tb_advanced_stats AS a
    LEFT JOIN teams.tb_game_logs AS b
    ON a.Team_ID = b.Team_ID
    AND a.Game_ID = b.Game_ID
    )
    UPDATE teams.tb_advanced_stats 
    SET OFF_RATING = (
    SELECT RATING AS OFF_RATING
    FROM temp_rating
    WHERE teams.tb_advanced_stats.Team_ID = temp_rating.Team_ID
    AND teams.tb_advanced_stats.Game_ID = temp_rating.Game_ID
    );
    """
    conn.execute(sql)
    

@task
def calc_and_ingest_def_rating(conn):
    sql = """
    WITH temp_rating AS (
    SELECT 
    	a.Team_ID,
    	a.Game_ID,
    	a.POSS,
    	b.PTS,
    	(100 * (b.PTS / a.POSS::FLOAT)) AS RATING
    FROM teams.tb_advanced_stats AS a
    LEFT JOIN teams.tb_game_logs AS b
    ON a.Team_ID = b.Team_ID
    AND a.Game_ID = b.Game_ID
    )
    UPDATE teams.tb_advanced_stats 
    SET DEF_RATING = (
    SELECT RATING AS DEF_RATING
    FROM temp_rating
    WHERE teams.tb_advanced_stats.Team_ID <> temp_rating.Team_ID
    AND teams.tb_advanced_stats.Game_ID = temp_rating.Game_ID
    );
    """
    conn.execute(sql)
    

@task
def calc_and_ingest_net_rating(conn):
    sql = """
    WITH temp_net_rating AS (
    SELECT 
    	Team_ID,
    	Game_ID,
    	(OFF_RATING - DEF_RATING) AS NET_RATING
    FROM teams.tb_advanced_stats
    )
    UPDATE teams.tb_advanced_stats
    SET NET_RATING = (
    SELECT NET_RATING
    FROM temp_net_rating
    WHERE teams.tb_advanced_stats.Team_ID = temp_net_rating.Team_ID
    AND teams.tb_advanced_stats.Game_ID = temp_net_rating.Game_ID
    );
    """
    conn.execute(sql)
    

@task
def update_columns_when_poss_999(conn):
    sql = """
    UPDATE teams.tb_advanced_stats
    SET 
        PACE = NULL,
        OFF_RATING = NULL,
        DEF_RATING = NULL,
        NET_RATING = NULL
    WHERE POSS = 999
    """
    conn.execute(sql)


@flow
def get_tb_advanced_stats():
    conn = connect_to_db()
    try:
        tb_advanced_stats(conn=conn)
        populate_tb_advanced_stats_with_team_and_game_id(conn=conn)
        distinct_team_and_game_id = conn.execute(
            """
            SELECT DISTINCT Team_ID, Game_ID
            FROM teams.tb_advanced_stats 
            WHERE POSS IS NULL;
            """
        ).fetchall()
        for team_id, game_id in distinct_team_and_game_id:
            calc_and_ingest_possession(
                team_id=team_id,
                game_id=game_id,
                conn=conn
            )
        calc_and_ingest_pace(conn=conn)
        calc_and_ingest_off_rating(conn=conn)
        calc_and_ingest_def_rating(conn=conn)
        calc_and_ingest_net_rating(conn=conn)
        update_columns_when_poss_999(conn=conn)
    except Exception as e:
        print(e)
        conn.close()
    conn.close()
        

if __name__ == "__main__":
    get_tb_advanced_stats()
    
