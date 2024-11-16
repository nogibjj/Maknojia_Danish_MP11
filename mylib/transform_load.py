import os
import pandas as pd
from databricks import sql
from dotenv import load_dotenv


# Transforms and Loads WR rankings and points data into the specified Databricks database
def load(
    points_dataset="data/WRRankingsWeek5Points.csv",
    ranking_dataset="data/WRRankingsWeek5Ranking.csv",
):

    database_name = "drm85_MP6"
    points_table = "drm85_wr_points"
    ranking_table = "drm85_wr_ranking"

    # Load datasets
    df_points = pd.read_csv(points_dataset)
    df_ranking = pd.read_csv(ranking_dataset)

    load_dotenv()
    server_h = os.getenv("SERVER_HOSTNAME")
    access_token = os.getenv("ACCESS_TOKEN")
    http_path = os.getenv("HTTP_PATH")

    with sql.connect(
        server_hostname=server_h, http_path=http_path, access_token=access_token
    ) as connection:
        cursor = connection.cursor()

        # Create and use the database if it doesn't exist
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        cursor.execute(f"USE {database_name}")

        # Drop the points table if it exists
        cursor.execute(f"DROP TABLE IF EXISTS {points_table}")

        # Create points data table
        cursor.execute(
            f"""
            CREATE TABLE {points_table} (
                RK INTEGER PRIMARY KEY, 
                PLAYER_NAME STRING,                                
                PROJ_FPTS FLOAT
            )
            """
        )

        print("Points table created successfully.")

        # Check if DataFrame is empty
        if df_points.empty:
            print("Points DataFrame is empty.")
            return "Data loading aborted."

        print(f"Inserting {df_points.shape[0]} rows into {points_table}.")

        # Insert points data using executemany for batch insertion
        insert_points_query = f"INSERT INTO {points_table} VALUES (?, ?, ?)"
        for index, row in df_points.iterrows():
            try:
                cursor.execute(insert_points_query, tuple(row))
            except Exception as e:
                print(f"Error inserting row {index}: {e}")

        print("All points data inserted successfully.")

        # Drop the ranking table if it exists
        cursor.execute(f"DROP TABLE IF EXISTS {ranking_table}")

        # Create ranking data table
        cursor.execute(
            f"""
            CREATE TABLE {ranking_table} (
                RK INTEGER PRIMARY KEY, 
                PLAYER_NAME STRING,             
                TEAM STRING, 
                OPP STRING, 
                MATCHUP STRING,
                START_SIT STRING
            )
            """
        )

        print("Ranking table created successfully.")

        # Insert ranking data
        if not df_ranking.empty:
            print(f"Inserting {df_ranking.shape[0]} rows into {ranking_table}.")
            insert_ranking_query = (
                f"INSERT INTO {ranking_table} VALUES (?, ?, ?, ?, ?, ?)"
            )
            for index, row in df_ranking.iterrows():
                try:
                    cursor.execute(insert_ranking_query, tuple(row))
                except Exception as e:
                    print(f"Error inserting row {index}: {e}")

            print("All ranking data inserted successfully.")
    return "Data loaded successfully"


load()