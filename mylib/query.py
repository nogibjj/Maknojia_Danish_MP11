import os
from databricks import sql
from dotenv import load_dotenv

# Define a global variable for the log file
LOG_FILE = "complexQueryLog.md"


def logQuery(query, result="none"):
    """Adds to a query markdown file"""
    with open(LOG_FILE, "a") as file:
        file.write(f"```sql\n{query}\n```\n\n")
        file.write(f"```response from databricks\n{result}\n```\n\n")


def query(query):
    load_dotenv()
    server_h = os.getenv("SERVER_HOSTNAME")
    access_token = os.getenv("ACCESS_TOKEN")
    http_path = os.getenv("HTTP_PATH")

    with sql.connect(
        server_hostname=server_h,
        http_path=http_path,
        access_token=access_token,
    ) as connection:
        cursor = connection.cursor()

        try:
            cursor.execute(query)
            result = cursor.fetchall()
            logQuery(query, result)  # Log the result only after successful execution
            return result  # Optional: return result if needed
        except Exception as e:
            print(f"Error executing query: {e}")
            logQuery(query, str(e))
            raise  # Re-raise the exception after logging
        finally:
            cursor.close()


def run_queries():
    # Query to get player projections and matchups
    player_projections_query = """
    SELECT 
        p.RK AS Rank,
        p.PLAYER_NAME AS PlayerName,
        p.PROJ_FPTS AS ProjectedPoints,
        r.TEAM AS Team,
        r.OPP AS Opponent,
        r.MATCHUP AS Matchup,
        r.START_SIT AS StartSit
    FROM 
        WRRankingsWeek5Points AS p
    JOIN 
        WRRankingsWeek5Ranking AS r 
    ON 
        p.PLAYER_NAME = r.PLAYER_NAME
    ORDER BY 
        p.PROJ_FPTS DESC
    LIMIT 10;
    """

    # Execute the player projections query
    print("Executing Player Projections Query...")
    try:
        results = query(player_projections_query)
        print("Query executed successfully. Results:")
        for row in results:
            print(row)
    except Exception as e:
        print(f"Failed to execute query: {e}")


if __name__ == "__main__":
    run_queries()