rm_parquet:
	del .\data\raw\tb_teams\*.parquet
	del .\data\raw\tb_game_logs\*.parquet

rm_db:
	del .\data\processed\nba.duckdb