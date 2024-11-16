from mylib.etl import query_transform
from mylib.query import spark_sql_query
import os

if __name__ == "__main__":
    current_directory = os.getcwd()

    # Run the query transformation step
    query_transform()

    # Run a SQL query on the transformed dataset
    spark_sql_query("SELECT * FROM WRRankings_delta")
